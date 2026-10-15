// Package protocol implements rumors V2 control-stream items.
package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
)

// V2 constants define control and data-stream wire vocabulary.
const (
	Version            = 2
	PreambleLen        = 30
	PartyTag    uint64 = 0xd255
	VersionTag  uint64 = 0xd256
	ClockTag    uint64 = 0xd257
	StreamCount        = 17
	MaxRunBytes uint64 = 1<<32 - 1
	// SupplyOverhead includes the array, two signal items, tag, and widest run head.
	SupplyOverhead           uint64 = 10
	MaxTargetMessageSize            = MaxRunBytes - SupplyOverhead
	DefaultTargetMessageSize uint64 = 256 * (3 + 3 + 24*27 + 232*28)
)

// Wire validation errors can be inspected with errors.Is.
var (
	ErrMagic           = errors.New("not a rumors preamble")
	ErrProtocol        = errors.New("rumors protocol version mismatch")
	ErrNetwork         = errors.New("rumors network mismatch")
	ErrIntent          = errors.New("invalid session intent")
	ErrBootstrapRetire = errors.New("bootstrap cannot retire")
	ErrStructure       = errors.New("malformed rumors item")
	ErrOrder           = errors.New("listing radices are not strictly ascending")
	ErrDepth           = errors.New("payload depth limit mismatch")
	ErrLimit           = errors.New("rumors resource limit exceeded")
)

// Error identifies the rejected wire field and retains its underlying cause.
type Error struct {
	Field string
	Err   error
}

func (e *Error) Error() string              { return fmt.Sprintf("protocol %s: %v", e.Field, e.Err) }
func (e *Error) Unwrap() error              { return e.Err }
func invalid(field string, err error) error { return &Error{field, err} }

// Network is a causal universe identifier. Zero denotes a bootstrap claimant.
type Network [16]byte

// IsBootstrap reports whether the network denotes a bootstrap claimant.
func (n Network) IsBootstrap() bool { return n == Network{} }

// Intent declares whether a session participant will donate its identity.
type Intent uint8

// Session intents are fixed by V2.
const (
	Remain Intent = iota
	Retire
)

// Preamble identifies a session before variable-length traffic is admitted.
type Preamble struct {
	Network Network
	Intent  Intent
}

const preamblePrefix = "\xd9\xd9\xf7\x84\x66rumors"

// Validate rejects undefined or conflicting intent values.
func (p Preamble) Validate() error {
	if p.Intent > Retire {
		return invalid("intent", ErrIntent)
	}
	if p.Network.IsBootstrap() && p.Intent == Retire {
		return invalid("intent", ErrBootstrapRetire)
	}
	return nil
}

// AppendTo appends the fixed-width V2 preamble.
func (p Preamble) AppendTo(dst []byte) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return dst, err
	}
	dst = append(dst, preamblePrefix...)
	dst = append(dst, Version, 0x50)
	dst = append(dst, p.Network[:]...)
	return append(dst, byte(p.Intent)), nil
}

// ParsePreamble validates an exact fixed-width preamble.
func ParsePreamble(data []byte) (Preamble, error) {
	var p Preamble
	if len(data) != PreambleLen {
		return p, invalid("preamble", io.ErrUnexpectedEOF)
	}
	if string(data[:11]) != preamblePrefix {
		return p, invalid("magic", ErrMagic)
	}
	if data[11] != Version {
		h, _, err := cbor.ParseHead(data[11:])
		if err != nil {
			return p, invalid("version", err)
		}
		if h.Major != cbor.MajorUint {
			return p, invalid("version", ErrStructure)
		}
		return p, invalid("version", ErrProtocol)
	}
	if data[12] != 0x50 {
		return p, invalid("network", ErrStructure)
	}
	copy(p.Network[:], data[13:29])
	p.Intent = Intent(data[29])
	return p, p.Validate()
}

// ReadPreamble consumes exactly one preamble without reading following bytes.
func ReadPreamble(r io.Reader) (Preamble, error) {
	var data [PreambleLen]byte
	if _, err := io.ReadFull(r, data[:]); err != nil {
		return Preamble{}, err
	}
	return ParsePreamble(data[:])
}

// ValidateNetwork admits bootstrap claimants or matching established networks.
func ValidateNetwork(local, remote Network) error {
	if !local.IsBootstrap() && !remote.IsBootstrap() && local != remote {
		return ErrNetwork
	}
	return nil
}

// Child is one root-fan listing entry.
type Child struct {
	Radix uint8
	Hash  [24]byte
}

// Greeting declares a replica snapshot and its session parameters.
type Greeting struct {
	Listing           []Child
	SetLen            uint64
	Version           before.Version
	MaxVersionBytes   uint64
	PayloadDepthLimit uint64
	TargetMessageSize uint64
}

var keys = [...]string{
	"listing",
	"set_len",
	"version",
	"max_version_bytes",
	"payload_depth_limit",
	"target_message_size",
}

func appendBytes(dst []byte, major uint8, b []byte) []byte {
	dst = cbor.AppendHead(dst, major, uint64(len(b)))
	return append(dst, b...)
}

func appendKey(
	dst []byte,
	key string,
) []byte {
	return appendBytes(dst, cbor.MajorText, []byte(key))
}

// AppendTo appends the complete tag-24 embedded greeting item.
func (g Greeting) AppendTo(dst []byte) ([]byte, error) {
	if len(g.Listing) > 256 {
		return dst, invalid("listing", ErrLimit)
	}
	for i := 1; i < len(g.Listing); i++ {
		if g.Listing[i-1].Radix >= g.Listing[i].Radix {
			return dst, invalid("listing", ErrOrder)
		}
	}
	v, err := g.Version.MarshalBinary()
	if err != nil {
		return dst, invalid("version", err)
	}
	body := cbor.AppendHead(nil, cbor.MajorMap, 6)
	body = appendKey(body, keys[0])
	body = cbor.AppendHead(body, cbor.MajorMap, uint64(len(g.Listing)))
	for _, child := range g.Listing {
		body = cbor.AppendHead(body, cbor.MajorUint, uint64(child.Radix))
		body = appendBytes(body, cbor.MajorBytes, child.Hash[:])
	}
	body = appendKey(body, keys[1])
	body = cbor.AppendHead(body, cbor.MajorUint, g.SetLen)
	body = appendKey(body, keys[2])
	body = cbor.AppendHead(body, cbor.MajorTag, VersionTag)
	body = appendBytes(body, cbor.MajorBytes, v)
	vals := [...]uint64{g.MaxVersionBytes, g.PayloadDepthLimit, g.TargetMessageSize}
	for i, v := range vals {
		body = appendKey(body, keys[i+3])
		body = cbor.AppendHead(body, cbor.MajorUint, v)
	}
	dst = cbor.AppendHead(dst, cbor.MajorTag, cbor.TagEmbeddedItem)
	return appendBytes(dst, cbor.MajorBytes, body), nil
}

type parser struct{ data []byte }

func (p *parser) head() (cbor.Head, error) {
	h, n, err := cbor.ParseHead(p.data)
	if err == nil {
		p.data = p.data[n:]
	}
	return h, err
}

func (p *parser) expect(major uint8, value uint64) error {
	h, err := p.head()
	if err != nil {
		return err
	}
	if h.Major != major || h.Value != value {
		return ErrStructure
	}
	return nil
}

func (p *parser) body(major uint8) ([]byte, error) {
	h, err := p.head()
	if err != nil {
		return nil, err
	}
	if h.Major != major {
		return nil, ErrStructure
	}
	if h.Value > uint64(len(p.data)) {
		return nil, io.ErrUnexpectedEOF
	}
	b := p.data[:int(h.Value)]
	p.data = p.data[int(h.Value):]
	return b, nil
}

func (p *parser) key(want string) error {
	b, err := p.body(cbor.MajorText)
	if err != nil {
		return err
	}
	if string(b) != want {
		return ErrStructure
	}
	return nil
}

func (p *parser) uint() (uint64, error) {
	h, err := p.head()
	if err != nil {
		return 0, err
	}
	if h.Major != cbor.MajorUint {
		return 0, ErrStructure
	}
	return h.Value, nil
}

// ParseGreeting requires exactly one complete embedded greeting item.
func ParseGreeting(data []byte) (Greeting, error) {
	p := parser{data}
	var g Greeting
	if err := p.expect(cbor.MajorTag, cbor.TagEmbeddedItem); err != nil {
		return g, invalid("greeting tag", err)
	}
	body, err := p.body(cbor.MajorBytes)
	if err != nil {
		return g, invalid("greeting body", err)
	}
	if len(p.data) != 0 {
		return g, invalid("greeting", cbor.ErrTrailing)
	}
	return parseGreetingBody(body)
}

func (p *parser) listing() ([]Child, error) {
	h, err := p.head()
	if err != nil {
		return nil, err
	}
	if h.Major != cbor.MajorMap {
		return nil, ErrStructure
	}
	if h.Value > 256 {
		return nil, ErrLimit
	}
	children := make([]Child, 0, int(h.Value))
	for j := uint64(0); j < h.Value; j++ {
		child, err := p.child()
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

func (p *parser) child() (Child, error) {
	var child Child
	radix, err := p.uint()
	if err != nil {
		return child, err
	}
	if radix > 255 {
		return child, ErrStructure
	}
	b, err := p.body(cbor.MajorBytes)
	if err != nil {
		return child, err
	}
	if len(b) != 24 {
		return child, ErrStructure
	}
	child.Radix = uint8(radix)
	copy(child.Hash[:], b)
	return child, nil
}

func (p *parser) version() (before.Version, error) {
	var v before.Version
	if err := p.expect(cbor.MajorTag, VersionTag); err != nil {
		return v, err
	}
	b, err := p.body(cbor.MajorBytes)
	if err != nil {
		return v, err
	}
	err = v.UnmarshalBinary(b)
	return v, err
}

func (p *parser) greetingField(g *Greeting, i int) error {
	var err error
	switch i {
	case 0:
		g.Listing, err = p.listing()
	case 1:
		g.SetLen, err = p.uint()
	case 2:
		g.Version, err = p.version()
	case 3:
		g.MaxVersionBytes, err = p.uint()
	case 4:
		g.PayloadDepthLimit, err = p.uint()
	case 5:
		g.TargetMessageSize, err = p.uint()
	}
	return err
}

func parseGreetingBody(data []byte) (Greeting, error) {
	var g Greeting
	p := parser{data}
	if err := p.expect(cbor.MajorMap, 6); err != nil {
		return g, invalid("greeting map", err)
	}
	for i, key := range keys {
		if err := p.key(key); err != nil {
			return g, invalid(key, err)
		}
		if err := p.greetingField(&g, i); err != nil {
			return g, invalid(key, err)
		}
	}
	if len(p.data) != 0 {
		return g, invalid("greeting", cbor.ErrTrailing)
	}
	return g, nil
}

// ReadGreeting reads exactly one greeting and bounds its declared body before
// allocating. A zero maximum permits no body bytes.
func ReadGreeting(r io.Reader, maxBodyBytes uint64) (Greeting, error) {
	h, err := cbor.ReadHead(r)
	if err != nil {
		return Greeting{}, err
	}
	if h.Major != cbor.MajorTag || h.Value != cbor.TagEmbeddedItem {
		return Greeting{}, invalid("greeting tag", ErrStructure)
	}
	h, err = cbor.ReadHead(r)
	if err != nil {
		return Greeting{}, err
	}
	if h.Major != cbor.MajorBytes {
		return Greeting{}, invalid("greeting body", ErrStructure)
	}
	if h.Value > maxBodyBytes || h.Value > uint64(int(^uint(0)>>1)) {
		return Greeting{}, invalid("greeting body", ErrLimit)
	}
	body, err := cbor.ReadPayload(r, int(h.Value))
	if err != nil {
		return Greeting{}, err
	}
	return parseGreetingBody(body)
}

// GreetingBodyLimit bounds a full fan, all uint64 declarations, and a version
// of the caller's maximum size. Addition saturates instead of wrapping.
func GreetingBodyLimit(maxVersionBytes uint64) uint64 {
	const overhead uint64 = 8192
	if maxVersionBytes > ^uint64(0)-overhead {
		return ^uint64(0)
	}
	return maxVersionBytes + overhead
}

// Negotiation is symmetric except for the local initiator role.
type Negotiation struct {
	Equal             bool
	Initiator         bool
	TargetMessageSize uint64
}

// Negotiate validates shared configuration before testing equality or electing roles.
func Negotiate(local, remote Greeting) (Negotiation, error) {
	var n Negotiation
	if local.PayloadDepthLimit != remote.PayloadDepthLimit {
		return n, ErrDepth
	}
	n.TargetMessageSize = min(
		local.TargetMessageSize,
		remote.TargetMessageSize,
		MaxTargetMessageSize,
	)
	n.Equal = local.Version.Equal(remote.Version)
	if n.Equal {
		return n, nil
	}
	if local.SetLen != remote.SetLen {
		n.Initiator = local.SetLen < remote.SetLen
		return n, nil
	}
	a, err := local.Version.MarshalBinary()
	if err != nil {
		return n, err
	}
	b, err := remote.Version.MarshalBinary()
	if err != nil {
		return n, err
	}
	n.Initiator = bytes.Compare(a, b) > 0
	return n, nil
}

// CompletionMarker is exchanged after the session's local commit and handoff.
const CompletionMarker = "\x61."
