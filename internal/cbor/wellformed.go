package cbor

import (
	"encoding/binary"
	"io"
)

// ScanWellFormed returns the size of the first complete CBOR item without
// imposing a nesting limit or canonical encoding. Work and storage are bounded
// by the actual input length, never a declared length. Containers use an
// explicit stack; a chain of tags needs constant stack space.
// Syntax follows Ciborium 0.2.2, including its acceptance of nested indefinite
// string segments and extended simple-value spellings below 32.
//
// This function checks syntax and item boundaries. The application's typed
// decoder must validate UTF-8, tag semantics, supported simple values, and its
// own recursion budget. Decoder recursion is type-dependent and cannot be
// inferred from CBOR container depth.
func ScanWellFormed(input []byte) (int, error) {
	s := wellFormedScanner{input: input}
	// A synthetic single-element array describes the top-level item.
	s.stack = append(s.stack, itemScope{kind: MajorArray, remaining: 1})
	for len(s.stack) > 0 {
		if err := s.step(); err != nil {
			return 0, err
		}
	}
	return s.pos, nil
}

// ValidateWellFormed requires exactly one item accepted by ScanWellFormed.
func ValidateWellFormed(input []byte) error {
	n, err := ScanWellFormed(input)
	if err != nil {
		return err
	}
	if n != len(input) {
		return ErrTrailing
	}
	return nil
}

type itemScope struct {
	kind       byte
	indefinite bool
	remaining  uint64
	seen       uint64
}

type wellFormedScanner struct {
	input []byte
	pos   int
	stack []itemScope
}

func (s *wellFormedScanner) step() error {
	top := &s.stack[len(s.stack)-1]
	if !top.indefinite && top.remaining == 0 {
		s.stack = s.stack[:len(s.stack)-1]
		return nil
	}
	if s.pos == len(s.input) {
		return io.ErrUnexpectedEOF
	}
	if top.indefinite && s.input[s.pos] == 0xff {
		return s.endIndefinite(top)
	}
	if top.kind == MajorBytes || top.kind == MajorText {
		return s.stringChunk(top.kind)
	}
	if top.indefinite {
		top.seen++
	} else {
		top.remaining--
	}
	return s.value()
}

func (s *wellFormedScanner) endIndefinite(top *itemScope) error {
	if top.kind == MajorMap && top.seen&1 != 0 {
		return ErrMalformed
	}
	s.pos++
	s.stack = s.stack[:len(s.stack)-1]
	return nil
}

func (s *wellFormedScanner) head() (byte, byte, uint64, error) {
	major, info, value, n, err := parseValueHead(s.input[s.pos:])
	if err == nil {
		s.pos += n
	}
	return major, info, value, err
}

func (s *wellFormedScanner) value() error {
	major, info, value, err := s.head()
	for err == nil && major == MajorTag {
		if info == 31 {
			return ErrMalformed
		}
		major, info, value, err = s.head()
	}
	if err != nil {
		return err
	}
	if info == 31 {
		return s.indefiniteValue(major)
	}
	switch major {
	case MajorUint, MajorNegative:
		return nil
	case MajorBytes, MajorText:
		return s.skip(value)
	case MajorArray, MajorMap:
		return s.container(major, value)
	case MajorSimple:
		return nil
	default:
		return ErrMalformed
	}
}

func (s *wellFormedScanner) indefiniteValue(major byte) error {
	if major < MajorBytes || major > MajorMap {
		return ErrMalformed
	}
	s.stack = append(s.stack, itemScope{kind: major, indefinite: true})
	return nil
}

func (s *wellFormedScanner) container(major byte, count uint64) error {
	available := uint64(len(s.input) - s.pos)
	if major == MajorMap {
		if count > available/2 {
			return io.ErrUnexpectedEOF
		}
		count *= 2
	}
	if count > available {
		return io.ErrUnexpectedEOF
	}
	if count > 0 {
		s.stack = append(s.stack, itemScope{kind: major, remaining: count})
	}
	return nil
}

func (s *wellFormedScanner) skip(length uint64) error {
	if length > uint64(len(s.input)-s.pos) {
		return io.ErrUnexpectedEOF
	}
	s.pos += int(length)
	return nil
}

func (s *wellFormedScanner) stringChunk(expected byte) error {
	major, info, length, err := s.head()
	if err != nil {
		return err
	}
	if major != expected {
		return ErrMalformed
	}
	if info == 31 {
		return s.indefiniteValue(major)
	}
	return s.skip(length)
}

// parseValueHead accepts noncanonical heads and represents a break by major 7,
// info 31. Only callers that know the current container may admit that break.
func parseValueHead(input []byte) (major, info byte, value uint64, n int, err error) {
	if len(input) == 0 {
		return 0, 0, 0, 0, io.ErrUnexpectedEOF
	}
	major, info = input[0]>>5, input[0]&31
	if info < 24 || info == 31 {
		return major, info, uint64(info), 1, nil
	}
	if info > 27 {
		return 0, 0, 0, 0, ErrReserved
	}
	width := 1 << (info - 24)
	if len(input) < 1+width {
		return 0, 0, 0, 0, io.ErrUnexpectedEOF
	}
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
	return major, info, value, 1 + width, nil
}
