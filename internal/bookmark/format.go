package bookmark

import (
	"crypto/sha3"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
)

const (
	// FormatVersion is the upstream on-disk bookmark format version.
	FormatVersion uint64 = 6
	partyTag      uint64 = 0xd255
	versionTag    uint64 = 0xd256
)

var (
	// ErrVersion reports an unsupported bookmark format version.
	ErrVersion = errors.New("bookmark: unsupported format version")
	// ErrChecksum reports corrupted or modified record bytes.
	ErrChecksum = errors.New("bookmark: checksum mismatch")
	// ErrRecord reports invalid recovery record structure or atoms.
	ErrRecord = errors.New("bookmark: invalid recovery record")
)

func appendBytes(dst, value []byte) []byte {
	dst = cbor.AppendHead(dst, cbor.MajorBytes, uint64(len(value)))
	return append(dst, value...)
}

func checksum(version uint64, payload []byte) [32]byte {
	input := cbor.AppendHead(nil, cbor.MajorUint, version)
	input = cbor.AppendHead(input, cbor.MajorTag, cbor.TagEmbeddedItem)
	input = appendBytes(input, payload)
	return sha3.Sum256(input)
}

func frame(payload []byte) []byte {
	hash := checksum(FormatVersion, payload)
	out := cbor.AppendHead(nil, cbor.MajorTag, cbor.TagSelfDescribed)
	out = cbor.AppendHead(out, cbor.MajorArray, 3)
	out = cbor.AppendHead(out, cbor.MajorUint, FormatVersion)
	out = appendBytes(out, hash[:])
	out = cbor.AppendHead(out, cbor.MajorTag, cbor.TagEmbeddedItem)
	return appendBytes(out, payload)
}

func encodeNetwork(dst []byte, n *networkRecord) []byte {
	dst = cbor.AppendHead(dst, cbor.MajorArray, 3)
	dst = appendBytes(dst, n.network[:])
	dst = cbor.AppendHead(dst, cbor.MajorTag, versionTag)
	dst = appendBytes(dst, n.written.AppendBinary(nil))
	dst = cbor.AppendHead(dst, cbor.MajorArray, uint64(len(n.identities)))
	for _, raw := range n.identities {
		dst = cbor.AppendHead(dst, cbor.MajorTag, partyTag)
		dst = appendBytes(dst, raw)
	}
	return dst
}

func recordSize(networks, contents int) int {
	payload := cbor.HeadLen(uint64(networks)) + contents
	return cbor.HeadLen(cbor.TagSelfDescribed) + 1 + cbor.HeadLen(FormatVersion) +
		cbor.HeadLen(
			32,
		) + 32 + cbor.HeadLen(cbor.TagEmbeddedItem) + cbor.HeadLen(uint64(payload)) + payload
}

// Encode writes the exact canonical Rust v6 frame without applying retention.
func (r *Record) Encode() []byte {
	payload := cbor.AppendHead(nil, cbor.MajorArray, uint64(len(r.networks)))
	for i := range r.networks {
		payload = encodeNetwork(payload, &r.networks[i])
	}
	return frame(payload)
}

// Decode validates the complete integrity frame before parsing recovery rights.
// Legal noncanonical CBOR envelope spellings do not change the checksum.
func Decode(input []byte) (*Record, error) {
	version, hash, payload, err := envelope(input)
	if err != nil {
		return nil, err
	}
	if version != FormatVersion {
		return nil, fmt.Errorf("%w: %d", ErrVersion, version)
	}
	want := checksum(version, payload)
	if subtle.ConstantTimeCompare(hash, want[:]) != 1 {
		return nil, ErrChecksum
	}
	r, err := decodePayload(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRecord, err)
	}
	return r, nil
}

func envelope(input []byte) (uint64, []byte, []byte, error) {
	if err := cbor.ValidateItem(
		input,
		cbor.Limits{MaxBytes: max(1, len(input)), MaxItems: max(1, len(input))},
	); err != nil {
		return 0, nil, nil, err
	}
	p := parser{input: input}
	if err := p.tag(cbor.TagSelfDescribed); err != nil {
		return 0, nil, nil, err
	}
	outer, err := p.array()
	if err != nil {
		return 0, nil, nil, err
	}
	version, err := p.uint()
	if err != nil {
		return 0, nil, nil, err
	}
	hash, err := p.bytes()
	if err != nil {
		return 0, nil, nil, err
	}
	if err := p.tag(cbor.TagEmbeddedItem); err != nil {
		return 0, nil, nil, err
	}
	payload, err := p.bytes()
	if err != nil {
		return 0, nil, nil, err
	}
	if err := p.end(outer, 3); err != nil {
		return 0, nil, nil, err
	}
	if p.pos != len(input) {
		return 0, nil, nil, cbor.ErrTrailing
	}
	return version, hash, payload, nil
}

func decodePayload(payload []byte) (*Record, error) {
	if err := cbor.ValidateItem(
		payload,
		cbor.Limits{MaxBytes: max(1, len(payload)), MaxItems: max(1, len(payload))},
	); err != nil {
		return nil, err
	}
	p := parser{input: payload}
	count, err := p.array()
	if err != nil {
		return nil, err
	}
	r := new(Record)
	seen := make(map[[16]byte]bool)
	for i := 0; p.more(count, i); i++ {
		n, err := p.network()
		if err != nil {
			return nil, err
		}
		if seen[n.network] {
			return nil, errors.New("duplicate bookmark network")
		}
		seen[n.network] = true
		r.networks = append(r.networks, n)
	}
	if err := p.end(count, len(r.networks)); err != nil {
		return nil, err
	}
	if p.pos != len(payload) {
		return nil, cbor.ErrTrailing
	}
	return r, nil
}

func (p *parser) network() (networkRecord, error) {
	var n networkRecord
	fields, err := p.array()
	if err != nil {
		return n, err
	}
	key, err := p.bytes()
	if err != nil {
		return n, err
	}
	if len(key) != len(n.network) {
		return n, errors.New("network identifier must be 16 bytes")
	}
	copy(n.network[:], key)
	if err := p.tag(versionTag); err != nil {
		return n, err
	}
	raw, err := p.bytes()
	if err != nil {
		return n, err
	}
	if err := n.written.UnmarshalBinary(raw); err != nil {
		return n, err
	}
	n.identities, err = p.identities()
	if err != nil {
		return n, err
	}
	return n, p.end(fields, 3)
}

func (p *parser) identities() ([][]byte, error) {
	count, err := p.array()
	if err != nil {
		return nil, err
	}
	var identities [][]byte
	for i := 0; p.more(count, i); i++ {
		if err := p.tag(partyTag); err != nil {
			return nil, err
		}
		raw, err := p.bytes()
		if err != nil {
			return nil, err
		}
		if _, err := before.DecodeParty(raw); err != nil {
			return nil, err
		}
		identities = append(identities, raw)
	}
	return identities, p.end(count, len(identities))
}
