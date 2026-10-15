package protocol

import (
	"errors"
	"io"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
	"github.com/cirocosta/hearsay/internal/sessionerr"
)

// Role identifies the elected speaker of a stream direction.
type Role uint8

// Elected roles determine the signal grammar at each height.
const (
	Initiator Role = iota
	Responder
)

// Frame validation errors retain their identity through Error.
var (
	ErrStream      = errors.New("invalid logical stream")
	ErrSignal      = errors.New("invalid frame signal")
	ErrPlacement   = errors.New("frame signal is invalid for this speaker and stream")
	ErrArity       = errors.New("frame array length disagrees with signal")
	ErrOverbatched = errors.New("over-target supply run contains multiple records")
	ErrRecord      = errors.New("malformed supply record")
)

// Signal is the V2 state code, including reply and stream boundaries.
type Signal uint8

// Reaction pairs encode continue first, then end-of-reply.
const (
	MatchContinue Signal = iota
	MatchEnd
	QueryEmptyContinue
	QueryEmptyEnd
	QueryContinue
	QueryEnd
	SupplyContinue
	SupplyEnd
	ReplyEnd
	StreamEnd
)

// ValidateSignal checks range and the role-specific stream phase.
func ValidateSignal(role Role, stream uint8, signal Signal) error {
	if stream >= StreamCount {
		return ErrStream
	}
	if signal > StreamEnd {
		return ErrSignal
	}
	if role > Responder {
		return ErrPlacement
	}
	if role == Initiator && stream == 0 && signal < SupplyContinue {
		return ErrPlacement
	}
	if role == Initiator && stream == 16 && (signal == QueryContinue || signal == QueryEnd) {
		return ErrPlacement
	}
	if role == Responder && stream == 16 && signal != SupplyEnd && signal != ReplyEnd &&
		signal != StreamEnd {
		return ErrPlacement
	}
	return nil
}

// StreamAtHeight gives the stream owning height in the speaker's schedule.
// Height zero is the leaf; height 31 is immediately below the root.
func StreamAtHeight(role Role, height uint8) (uint8, error) {
	if role > Responder || height > 31 {
		return 0, ErrStream
	}
	if height == 31 {
		return 0, nil
	}
	if height == 0 {
		return 16, nil
	}
	distance := 31 - height
	if role == Initiator && distance%2 == 1 {
		return distance/2 + 1, nil
	}
	if role == Responder && distance%2 == 0 {
		return distance / 2, nil
	}
	return 0, ErrStream
}

// Label names a session's lazily opened data stream.
type Label struct {
	Epoch  uint8
	Stream uint8
}

// AppendTo appends two canonical unsigned items without an array wrapper.
func (l Label) AppendTo(dst []byte) ([]byte, error) {
	if l.Stream >= StreamCount {
		return dst, ErrStream
	}
	dst = cbor.AppendHead(dst, cbor.MajorUint, uint64(l.Epoch))
	return cbor.AppendHead(dst, cbor.MajorUint, uint64(l.Stream)), nil
}

// ReadLabel consumes exactly one label and rejects an unexpected epoch.
func ReadLabel(r io.Reader, epoch uint8) (Label, error) {
	var label Label
	e, err := cbor.ReadUint(r)
	if err != nil {
		return label, err
	}
	s, err := cbor.ReadUint(r)
	if err != nil {
		return label, err
	}
	if e != uint64(epoch) || s >= StreamCount {
		return label, &LabelError{ExpectedEpoch: epoch, Epoch: e, Stream: s}
	}
	label.Epoch = uint8(e)
	label.Stream = uint8(s)
	return label, nil
}

// LeafRun holds structurally validated encoded records. Its backing bytes are
// borrowed and must remain unchanged while the run or iterator is in use.
type LeafRun struct {
	data  []byte
	count int
}

// ParseRun validates record envelopes without decoding versions or payloads.
func ParseRun(data []byte) (LeafRun, error) {
	run := LeafRun{data: data}
	if len(data) == 0 {
		return LeafRun{}, ErrRecord
	}
	if uint64(len(data)) > MaxRunBytes {
		return LeafRun{}, ErrLimit
	}
	p := parser{data}
	for len(p.data) > 0 {
		if _, err := p.recordBody(); err != nil {
			return LeafRun{}, err
		}
		run.count++
	}
	return run, nil
}

func (p *parser) recordBody() ([]byte, error) {
	if err := p.expect(cbor.MajorTag, cbor.TagSequence); err != nil {
		return nil, invalid("record tag", err)
	}
	b, err := p.body(cbor.MajorBytes)
	if err != nil {
		return nil, invalid("record body", err)
	}
	return b, nil
}

// Bytes exposes the borrowed record envelopes; callers must not mutate them.
func (r LeafRun) Bytes() []byte { return r.data }

// Count returns the number of structurally valid records.
func (r LeafRun) Count() int { return r.count }

// Record is a canonical causal version and one opaque CBOR payload.
// Payload borrows the run's bytes.
type Record struct {
	Version before.Version
	Payload []byte
}

// RecordLimits bounds version decoding. MaxVersionBytes zero admits no version
// bytes. Payload decoding and its typed recursion budget belong to the
// application's codec, after record framing has been validated.
type RecordLimits struct {
	MaxVersionBytes uint64
}

// Records creates an iterator that decodes each version and payload once.
func (r LeafRun) Records(limits RecordLimits) *RecordIterator {
	return &RecordIterator{rest: r.data, limits: limits}
}

// RecordIterator walks borrowed run bytes without materializing a record slice.
type RecordIterator struct {
	rest   []byte
	limits RecordLimits
	failed bool
}

// Next returns false at the run's end. Any error terminates iteration.
func (it *RecordIterator) Next() (Record, bool, error) {
	if it.failed || len(it.rest) == 0 {
		return Record{}, false, nil
	}
	p := parser{it.rest}
	body, err := p.recordBody()
	if err != nil {
		it.failed = true
		return Record{}, false, err
	}
	it.rest = p.data
	record, err := decodeRecord(body, it.limits)
	if err != nil {
		it.failed = true
		return Record{}, false, err
	}
	return record, true, nil
}

func decodeRecord(body []byte, limits RecordLimits) (Record, error) {
	var record Record
	p := parser{body}
	if err := p.expect(cbor.MajorTag, VersionTag); err != nil {
		return record, invalid("record version tag", err)
	}
	v, err := p.body(cbor.MajorBytes)
	if err != nil {
		return record, invalid("record version", err)
	}
	if uint64(len(v)) > limits.MaxVersionBytes {
		return record, invalid("record version", ErrLimit)
	}
	if err := record.Version.UnmarshalBinary(v); err != nil {
		return record, invalid("record version", err)
	}
	if err := cbor.ValidateWellFormed(p.data); err != nil {
		return record, invalid("record payload", err)
	}
	record.Payload = p.data
	return record, nil
}

// Frame is one self-delimiting V2 array. Listing is populated only for nonempty
// queries; Run only for supplies. Scope pairing belongs to the session driver.
type Frame struct {
	Stream  uint8
	Signal  Signal
	Listing []Child
	Run     LeafRun
}

func (f Frame) hasBody() bool { return f.Signal >= QueryContinue && f.Signal <= SupplyEnd }
func validateListing(children []Child) error {
	if len(children) == 0 || len(children) > 256 {
		return ErrStructure
	}
	for i := 1; i < len(children); i++ {
		if children[i-1].Radix >= children[i].Radix {
			return ErrOrder
		}
	}
	return nil
}

func (f Frame) validate(role Role) error {
	if err := ValidateSignal(role, f.Stream, f.Signal); err != nil {
		return err
	}
	if f.Signal == QueryContinue || f.Signal == QueryEnd {
		return validateListing(f.Listing)
	}
	if f.Signal == SupplyContinue || f.Signal == SupplyEnd {
		if f.Run.count == 0 {
			return ErrRecord
		}
		return nil
	}
	return nil
}

// AppendTo appends a canonical frame after checking its representable grammar.
func (f Frame) AppendTo(dst []byte, role Role) ([]byte, error) {
	if err := f.validate(role); err != nil {
		return dst, err
	}
	arity := uint64(2)
	if f.hasBody() {
		arity = 3
	}
	dst = cbor.AppendHead(dst, cbor.MajorArray, arity)
	dst = append(dst, f.Stream, byte(f.Signal))
	switch f.Signal {
	case QueryContinue, QueryEnd:
		dst = cbor.AppendHead(dst, cbor.MajorMap, uint64(len(f.Listing)))
		for _, child := range f.Listing {
			dst = cbor.AppendHead(dst, cbor.MajorUint, uint64(child.Radix))
			dst = appendBytes(dst, cbor.MajorBytes, child.Hash[:])
		}
	case SupplyContinue, SupplyEnd:
		dst = cbor.AppendHead(dst, cbor.MajorTag, cbor.TagSequence)
		dst = appendBytes(dst, cbor.MajorBytes, f.Run.data)
	}
	return dst, nil
}

// FrameLimits controls supply buffering. TargetMessageSize uses the negotiated
// target, including zero's one-record behavior. MaxRunBytes zero uses the wire
// cap; a smaller nonzero cap also bounds individually oversized records.
type FrameLimits struct {
	TargetMessageSize uint64
	MaxRunBytes       uint64
}

// ReadFrame reads one exact frame. It checks over-target batching from the
// first record's heads before buffering the run. It never consumes the next
// frame and never trusts a declared length for an upfront allocation.
func ReadFrame(r io.Reader, role Role, limits FrameLimits) (Frame, error) {
	f, err := readOpener(r, role)
	if err != nil {
		return Frame{}, err
	}
	switch f.Signal {
	case QueryContinue, QueryEnd:
		f.Listing, err = readListing(r)
	case SupplyContinue, SupplyEnd:
		f.Run, err = readRun(r, limits)
	}
	return f, err
}

func readOpener(r io.Reader, role Role) (Frame, error) {
	var f Frame
	h, err := cbor.ReadHead(r)
	if err != nil {
		return f, err
	}
	if h.Major != cbor.MajorArray || h.Value < 2 || h.Value > 3 {
		return f, ErrArity
	}
	stream, err := cbor.ReadUint(r)
	if err != nil {
		return f, err
	}
	if stream >= StreamCount {
		return f, ErrStream
	}
	state, err := cbor.ReadUint(r)
	if err != nil {
		return f, err
	}
	if state > uint64(StreamEnd) {
		return f, ErrSignal
	}
	f.Stream = uint8(stream)
	f.Signal = Signal(state)
	if err := ValidateSignal(role, f.Stream, f.Signal); err != nil {
		return f, err
	}
	expected := uint64(2)
	if f.hasBody() {
		expected = 3
	}
	if h.Value != expected {
		return f, ErrArity
	}
	return f, nil
}

func readListing(r io.Reader) ([]Child, error) {
	h, err := cbor.ReadHead(r)
	if err != nil {
		return nil, err
	}
	if h.Major != cbor.MajorMap || h.Value == 0 || h.Value > 256 {
		return nil, ErrStructure
	}
	children := make([]Child, 0, int(h.Value))
	for i := uint64(0); i < h.Value; i++ {
		child, err := readChild(r)
		if err != nil {
			return nil, err
		}
		if len(children) > 0 && children[len(children)-1].Radix >= child.Radix {
			return nil, ErrOrder
		}
		children = append(children, child)
	}
	return children, nil
}

func readChild(r io.Reader) (Child, error) {
	var child Child
	radix, err := cbor.ReadUint(r)
	if err != nil {
		return child, err
	}
	if radix > 255 {
		return child, ErrStructure
	}
	h, err := cbor.ReadHead(r)
	if err != nil {
		return child, err
	}
	if h.Major != cbor.MajorBytes || h.Value != 24 {
		return child, ErrStructure
	}
	child.Radix = uint8(radix)
	_, err = io.ReadFull(r, child.Hash[:])
	return child, err
}

func runLength(r io.Reader) (uint64, error) {
	h, err := cbor.ReadHead(r)
	if err != nil {
		return 0, err
	}
	if h.Major != cbor.MajorTag || h.Value != cbor.TagSequence {
		return 0, ErrRecord
	}
	h, err = cbor.ReadHead(r)
	if err != nil {
		return 0, err
	}
	if h.Major != cbor.MajorBytes {
		return 0, ErrRecord
	}
	return h.Value, nil
}

func readRun(r io.Reader, limits FrameLimits) (LeafRun, error) {
	length, err := runLength(r)
	if err != nil {
		return LeafRun{}, err
	}
	capBytes := limits.MaxRunBytes
	if capBytes == 0 {
		capBytes = MaxRunBytes
	}
	if length > min(capBytes, MaxRunBytes, uint64(int(^uint(0)>>1))) {
		return LeafRun{}, ErrLimit
	}
	var prefix []byte
	bounded := &io.LimitedReader{R: r, N: int64(length)}
	target := min(limits.TargetMessageSize, MaxTargetMessageSize)
	if length+SupplyOverhead > target {
		prefix, err = loneRecordPrefix(bounded, length)
		if err != nil {
			if bounded.N == 0 {
				return LeafRun{}, sessionerr.Protocol(err)
			}
			return LeafRun{}, err
		}
	}
	data, err := cbor.ResumePayload(bounded, prefix, int(length))
	if err != nil {
		return LeafRun{}, err
	}
	run, err := ParseRun(data)
	return run, sessionerr.Protocol(err)
}

func loneRecordPrefix(r io.Reader, length uint64) ([]byte, error) {
	if length < 3 {
		return nil, ErrOverbatched
	}
	recordLength, err := runLength(r)
	if err != nil {
		return nil, err
	}
	prefix := cbor.AppendHead(nil, cbor.MajorTag, cbor.TagSequence)
	prefix = cbor.AppendHead(prefix, cbor.MajorBytes, recordLength)
	if recordLength > length || uint64(len(prefix)) != length-recordLength {
		return nil, ErrOverbatched
	}
	return prefix, nil
}

// RunBuilder batches records from one supplied subtree. Take ends a run; a
// caller must not combine records from separate protocol supply reactions.
type RunBuilder struct {
	target uint64
	limits RecordLimits
	data   []byte
	count  int
}

// NewRunBuilder uses the smaller session target and the record decoding policy.
func NewRunBuilder(target uint64, limits RecordLimits) *RunBuilder {
	return &RunBuilder{target: min(target, MaxTargetMessageSize), limits: limits}
}

// Add appends a record, or returns false when the current run must be taken
// first. A valid first record is always admitted, even above the target.
func (b *RunBuilder) Add(record Record) (bool, error) {
	v, err := record.Version.MarshalBinary()
	if err != nil {
		return false, err
	}
	if uint64(len(v)) > b.limits.MaxVersionBytes {
		return false, ErrLimit
	}
	if err := cbor.ValidateWellFormed(record.Payload); err != nil {
		return false, err
	}
	body := uint64(
		cbor.HeadLen(VersionTag)+cbor.HeadLen(uint64(len(v)))+len(v),
	) + uint64(
		len(record.Payload),
	)
	item := uint64(cbor.HeadLen(cbor.TagSequence)+cbor.HeadLen(body)) + body
	if item > MaxRunBytes {
		return false, ErrLimit
	}
	if b.count > 0 && (uint64(len(b.data))+item+SupplyOverhead > b.target) {
		return false, nil
	}
	b.data = cbor.AppendHead(b.data, cbor.MajorTag, cbor.TagSequence)
	b.data = cbor.AppendHead(b.data, cbor.MajorBytes, body)
	b.data = cbor.AppendHead(b.data, cbor.MajorTag, VersionTag)
	b.data = appendBytes(b.data, cbor.MajorBytes, v)
	b.data = append(b.data, record.Payload...)
	b.count++
	return true, nil
}

// Take transfers ownership of the current run and resets the builder.
func (b *RunBuilder) Take() LeafRun {
	run := LeafRun{data: b.data, count: b.count}
	b.data = nil
	b.count = 0
	return run
}
