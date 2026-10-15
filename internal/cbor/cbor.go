// Package cbor implements the canonical head grammar used by rumors V2.
//
// Head readers consume exactly one head without buffering subsequent bytes.
// Payload reads grow only as bytes arrive. ScanItem is a separate, bounded
// general CBOR scanner for opaque application payloads; unlike protocol heads,
// those payloads may use noncanonical and indefinite encodings.
package cbor

import (
	"encoding/binary"
	"errors"
	"io"
)

// Canonical protocol head types, tags, and framing sizes.
const (
	MajorUint        uint8  = 0
	MajorNegative    uint8  = 1
	MajorBytes       uint8  = 2
	MajorText        uint8  = 3
	MajorArray       uint8  = 4
	MajorMap         uint8  = 5
	MajorTag         uint8  = 6
	MajorSimple      uint8  = 7
	TagSequence      uint64 = 63
	TagEmbeddedItem  uint64 = 24
	TagSelfDescribed uint64 = 55799
	MaxHeadLen              = 9
	PayloadChunkLen         = 64 * 1024
)

// SelfDescribedHead is the shortest-form head of tag 55799.
const SelfDescribedHead = "\xd9\xd9\xf7"

// Errors distinguish malformed protocol heads, general CBOR, and resource limits.
var (
	ErrIndefinite  = errors.New("indefinite-length CBOR is not canonical")
	ErrReserved    = errors.New("CBOR head uses a reserved additional-information value")
	ErrNotShortest = errors.New("CBOR head is not in shortest form")
	ErrMajor       = errors.New("unexpected CBOR major type")
	ErrLimit       = errors.New("CBOR resource limit exceeded")
	ErrMalformed   = errors.New("malformed CBOR item")
	ErrTrailing    = errors.New("trailing bytes after CBOR item")
)

// Head describes a CBOR item's major type and argument. The argument is a
// value, byte length, element count, or tag number according to Major.
type Head struct {
	Major uint8
	Value uint64
}

// HeadLen returns the number of bytes in a shortest-form head.
func HeadLen(value uint64) int {
	switch {
	case value < 24:
		return 1
	case value <= 0xff:
		return 2
	case value <= 0xffff:
		return 3
	case value <= 0xffffffff:
		return 5
	default:
		return 9
	}
}

// AppendHead appends a shortest-form head. A major type outside 0..7 is a
// programming error and panics. Floating-point encodings are not head arguments.
func AppendHead(dst []byte, major uint8, value uint64) []byte {
	if major > 7 {
		panic("cbor: major type exceeds three bits")
	}
	major <<= 5
	switch HeadLen(value) {
	case 1:
		return append(dst, major|uint8(value))
	case 2:
		return append(dst, major|24, uint8(value))
	case 3:
		return binary.BigEndian.AppendUint16(append(dst, major|25), uint16(value))
	case 5:
		return binary.BigEndian.AppendUint32(append(dst, major|26), uint32(value))
	default:
		return binary.BigEndian.AppendUint64(append(dst, major|27), value)
	}
}

// ParseHead parses one canonical head. On error it returns a zero consumed
// count. The input may contain subsequent items, which remain untouched.
func ParseHead(input []byte) (Head, int, error) {
	if len(input) == 0 {
		return Head{}, 0, io.ErrUnexpectedEOF
	}
	width, err := extensionLen(input[0])
	if err != nil {
		return Head{}, 0, err
	}
	if len(input) < width+1 {
		return Head{}, 0, io.ErrUnexpectedEOF
	}
	value := uint64(input[0] & 31)
	switch width {
	case 1:
		value = uint64(input[1])
	case 2:
		value = uint64(binary.BigEndian.Uint16(input[1:3]))
	case 4:
		value = uint64(binary.BigEndian.Uint32(input[1:5]))
	case 8:
		value = binary.BigEndian.Uint64(input[1:9])
	}
	if HeadLen(value) != width+1 {
		return Head{}, 0, ErrNotShortest
	}
	return Head{input[0] >> 5, value}, width + 1, nil
}

func extensionLen(initial byte) (int, error) {
	switch initial & 31 {
	case 24:
		return 1, nil
	case 25:
		return 2, nil
	case 26:
		return 4, nil
	case 27:
		return 8, nil
	case 28, 29, 30:
		return 0, ErrReserved
	case 31:
		return 0, ErrIndefinite
	default:
		return 0, nil
	}
}

// ReadHead reads one canonical head without reading into its body or the next
// item. EOF before the first byte is io.EOF; EOF inside a head is
// io.ErrUnexpectedEOF. Other transport errors are preserved.
func ReadHead(r io.Reader) (Head, error) {
	var buf [MaxHeadLen]byte
	if _, err := io.ReadFull(r, buf[:1]); err != nil {
		return Head{}, err
	}
	width, err := extensionLen(buf[0])
	if err != nil {
		return Head{}, err
	}
	if width > 0 {
		if _, err := io.ReadFull(r, buf[1:width+1]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return Head{}, err
		}
	}
	head, _, err := ParseHead(buf[:width+1])
	return head, err
}

// ReadByteHead reads one canonical head from an existing byte reader without
// allocating a temporary buffer. It has the same EOF semantics as ReadHead.
func ReadByteHead(r io.ByteReader) (Head, error) {
	initial, err := r.ReadByte()
	if err != nil {
		return Head{}, err
	}
	width, err := extensionLen(initial)
	if err != nil {
		return Head{}, err
	}
	value := uint64(initial & 31)
	if width != 0 {
		value, err = readByteValue(r, width)
		if err != nil {
			return Head{}, err
		}
	}
	if HeadLen(value) != width+1 {
		return Head{}, ErrNotShortest
	}
	return Head{initial >> 5, value}, nil
}

func readByteValue(r io.ByteReader, width int) (uint64, error) {
	var value uint64
	for range width {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		value = value<<8 | uint64(b)
	}
	return value, nil
}

// WriteHead writes a canonical head. A short write without an error is reported
// as io.ErrShortWrite.
func WriteHead(w io.Writer, major uint8, value uint64) error {
	var buf [MaxHeadLen]byte
	encoded := AppendHead(buf[:0], major, value)
	n, err := w.Write(encoded)
	if err == nil && n != len(encoded) {
		return io.ErrShortWrite
	}
	return err
}

// ReadUint reads one canonical unsigned integer.
func ReadUint(r io.Reader) (uint64, error) {
	h, err := ReadHead(r)
	if err != nil {
		return 0, err
	}
	if h.Major != MajorUint {
		return 0, ErrMajor
	}
	return h.Value, nil
}

// ReadPayload reads exactly length bytes. The caller must first apply its
// protocol's maximum length. Allocation grows as bytes arrive, rather than
// trusting a peer-declared length up front. Subsequent bytes remain unread.
func ReadPayload(r io.Reader, length int) ([]byte, error) {
	return ResumePayload(r, nil, length)
}

// ResumePayload continues ReadPayload using a prefix already read from r.
// A prefix exceeding length, or a negative length, fails before reading.
func ResumePayload(r io.Reader, prefix []byte, length int) ([]byte, error) {
	if length < 0 || len(prefix) > length {
		return nil, ErrLimit
	}
	for len(prefix) < length {
		prefix = growPayload(prefix, length)
		end := min(cap(prefix), length)
		n, err := readPayloadChunk(r, prefix[len(prefix):end])
		prefix = prefix[:len(prefix)+n]
		if len(prefix) == length {
			return prefix, nil
		}
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return prefix, nil
}

func growPayload(prefix []byte, length int) []byte {
	if len(prefix) < cap(prefix) {
		return prefix
	}
	target := length
	if cap(prefix) <= (length-1)/2 {
		target = cap(prefix) * 2
	}
	if target < PayloadChunkLen {
		target = min(PayloadChunkLen, length)
	}
	next := make([]byte, len(prefix), target)
	copy(next, prefix)
	return next
}

func readPayloadChunk(r io.Reader, dst []byte) (int, error) {
	for range 100 {
		n, err := r.Read(dst)
		if n < 0 || n > len(dst) {
			return 0, errors.New("cbor: invalid reader count")
		}
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, io.ErrNoProgress
}
