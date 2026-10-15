package cbor

import (
	"encoding/binary"
	"io"
	"unicode/utf8"
)

// Limits bounds general CBOR scanning. Zero fields use the defaults: nesting
// depth 64, item size 64 MiB, and one million items. Depth counts nested
// containers and tags, including the outermost one. Every string chunk counts
// as an item. No memory proportional to declared lengths is allocated.
type Limits struct {
	MaxDepth int
	MaxBytes int
	MaxItems int
}

// ScanItem returns the length of the first complete CBOR item. This scanner is
// for application payloads, not protocol headers: it accepts non-shortest
// integers, indefinite containers, floating-point values, and all valid tags.
// It checks well-formedness and UTF-8, but not map-key uniqueness or tag-specific
// semantics. Bytes after the first item are not scanned or counted against
// MaxBytes. A break outside an indefinite container is malformed.
func ScanItem(input []byte, limits Limits) (int, error) {
	if limits.MaxDepth == 0 {
		limits.MaxDepth = 64
	}
	if limits.MaxBytes == 0 {
		limits.MaxBytes = 64 << 20
	}
	if limits.MaxItems == 0 {
		limits.MaxItems = 1_000_000
	}
	if limits.MaxDepth < 0 || limits.MaxBytes < 0 || limits.MaxItems < 0 {
		return 0, ErrLimit
	}
	s := scanner{input: input, limits: limits}
	if err := s.item(0); err != nil {
		return 0, err
	}
	return s.pos, nil
}

// ValidateItem verifies that input contains exactly one general CBOR item under
// limits. It deliberately does not enforce canonical protocol-head spelling.
func ValidateItem(input []byte, limits Limits) error {
	n, err := ScanItem(input, limits)
	if err != nil {
		return err
	}
	if n != len(input) {
		return ErrTrailing
	}
	return nil
}

type scanner struct {
	input  []byte
	pos    int
	items  int
	limits Limits
}

func (s *scanner) take(n uint64) ([]byte, error) {
	if n > uint64(s.limits.MaxBytes-s.pos) {
		return nil, ErrLimit
	}
	if n > uint64(len(s.input)-s.pos) {
		return nil, io.ErrUnexpectedEOF
	}
	p := s.input[s.pos : s.pos+int(n)]
	s.pos += int(n)
	return p, nil
}

func (s *scanner) item(depth int) error {
	s.items++
	if s.items > s.limits.MaxItems {
		return ErrLimit
	}
	b, err := s.take(1)
	if err != nil {
		return err
	}
	major, info := b[0]>>5, b[0]&31
	if info == 31 {
		return s.indefinite(major, depth)
	}
	value, err := s.argument(info)
	if err != nil {
		return err
	}
	return s.definite(major, info, value, depth)
}

func (s *scanner) argument(info byte) (uint64, error) {
	if info < 24 {
		return uint64(info), nil
	}
	if info > 27 {
		return 0, ErrReserved
	}
	width := uint64(1) << (info - 24)
	arg, err := s.take(width)
	if err != nil {
		return 0, err
	}
	switch width {
	case 1:
		return uint64(arg[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(arg)), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(arg)), nil
	default:
		return binary.BigEndian.Uint64(arg), nil
	}
}

func (s *scanner) definite(major, info byte, value uint64, depth int) error {
	switch major {
	case MajorUint, MajorNegative:
		return nil
	case MajorBytes, MajorText:
		return s.stringBody(major, value)
	case MajorArray, MajorMap:
		return s.members(major, value, depth)
	case MajorTag:
		return s.tagBody(depth)
	case MajorSimple:
		return validateSimple(info, value)
	default:
		panic("unreachable CBOR major type")
	}
}

func (s *scanner) stringBody(major byte, length uint64) error {
	data, err := s.take(length)
	if err != nil {
		return err
	}
	if major == MajorText && !utf8.Valid(data) {
		return ErrMalformed
	}
	return nil
}

func validateSimple(info byte, value uint64) error {
	// Additional information 24 extends a simple value, not a float. Simple
	// values below 32 in that form are reserved by RFC 8949 section 3.3.
	if info == 24 && value < 32 {
		return ErrMalformed
	}
	return nil
}

func (s *scanner) tagBody(depth int) error {
	if depth >= s.limits.MaxDepth {
		return ErrLimit
	}
	return s.item(depth + 1)
}

func (s *scanner) members(major byte, count uint64, depth int) error {
	if depth >= s.limits.MaxDepth {
		return ErrLimit
	}
	if major == MajorMap {
		if count > ^uint64(0)/2 {
			return ErrLimit
		}
		count *= 2
	}
	// Every member consumes at least one byte and one item. Check these lower
	// bounds before iterating over any peer-controlled count.
	if count > uint64(s.limits.MaxItems-s.items) || count > uint64(s.limits.MaxBytes-s.pos) {
		return ErrLimit
	}
	for range count {
		if err := s.item(depth + 1); err != nil {
			return err
		}
	}
	return nil
}

func (s *scanner) indefinite(major uint8, depth int) error {
	if major < MajorBytes || major > MajorMap {
		return ErrMalformed
	}
	if depth >= s.limits.MaxDepth {
		return ErrLimit
	}
	count := uint64(0)
	for {
		done, err := s.indefiniteBoundary(major)
		if err != nil {
			return err
		}
		if done {
			return validateIndefiniteCount(major, count)
		}
		if err := s.item(depth + 1); err != nil {
			return err
		}
		count++
	}
}

func (s *scanner) indefiniteBoundary(major byte) (bool, error) {
	if s.pos >= s.limits.MaxBytes {
		return false, ErrLimit
	}
	if s.pos == len(s.input) {
		return false, io.ErrUnexpectedEOF
	}
	initial := s.input[s.pos]
	if initial == 0xff {
		s.pos++
		return true, nil
	}
	if major == MajorBytes || major == MajorText {
		if initial>>5 != major || initial&31 == 31 {
			return false, ErrMalformed
		}
	}
	return false, nil
}

func validateIndefiniteCount(major byte, count uint64) error {
	if major == MajorMap && count&1 != 0 {
		return ErrMalformed
	}
	return nil
}
