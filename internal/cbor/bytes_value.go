package cbor

import (
	"errors"
	"io"
)

// ErrDepth identifies exhaustion of a typed decoder's recursion-step budget.
var ErrDepth = errors.New("CBOR typed decoder recursion limit exceeded")

// DecodeByteString decodes the payload spellings accepted by Ciborium's typed
// byte-buffer decoder: a byte string or an array of unsigned bytes, optionally
// preceded by tags. Noncanonical and indefinite encodings are accepted. The
// input must contain exactly one item. A definite byte string borrows input;
// indefinite strings and arrays allocate their decoded bytes.
//
// A string consumes no Ciborium recursion steps; an array consumes one. Tags
// ignored by this typed decoder consume no steps. Callers enforce their typed
// decode limit independently; this helper has no implicit structural depth cap.
func DecodeByteString(input []byte) ([]byte, error) {
	return DecodeByteStringBounded(input, ^uint64(0))
}

// DecodeByteStringBounded applies Ciborium's typed byte-buffer recursion
// accounting: an array needs one step, a byte string and its ignored tags need
// none. A zero limit therefore accepts byte strings while rejecting arrays.
func DecodeByteStringBounded(input []byte, limit uint64) ([]byte, error) {
	p := byteValueParser{rest: input}
	out, err := p.byteString(limit)
	if err != nil {
		return nil, err
	}
	if len(p.rest) != 0 {
		return nil, ErrTrailing
	}
	return out, nil
}

type byteValueParser struct{ rest []byte }

func (p *byteValueParser) head() (byte, byte, uint64, error) {
	major, info, value, n, err := parseValueHead(p.rest)
	if err == nil {
		p.rest = p.rest[n:]
	}
	return major, info, value, err
}

func (p *byteValueParser) untaggedHead() (byte, byte, uint64, error) {
	major, info, value, err := p.head()
	for err == nil && major == MajorTag {
		if info == 31 {
			return 0, 0, 0, ErrMalformed
		}
		major, info, value, err = p.head()
	}
	return major, info, value, err
}

func (p *byteValueParser) body(length uint64) ([]byte, error) {
	if length > uint64(len(p.rest)) {
		return nil, io.ErrUnexpectedEOF
	}
	value := p.rest[:int(length)]
	p.rest = p.rest[int(length):]
	return value, nil
}

func (p *byteValueParser) byteString(limit uint64) ([]byte, error) {
	major, info, length, err := p.untaggedHead()
	if err != nil {
		return nil, err
	}
	switch major {
	case MajorBytes:
		if info != 31 {
			return p.body(length)
		}
		return p.chunks()
	case MajorArray:
		if limit == 0 {
			return nil, ErrDepth
		}
		return p.array(info, length)
	default:
		return nil, ErrMajor
	}
}

func (p *byteValueParser) chunks() ([]byte, error) {
	var out []byte
	nesting := 1
	for nesting > 0 {
		chunk, delta, err := p.segment()
		if err != nil {
			return nil, err
		}
		nesting += delta
		out = append(out, chunk...)
	}
	return out, nil
}

func (p *byteValueParser) segment() ([]byte, int, error) {
	major, info, length, err := p.head()
	if err != nil {
		return nil, 0, err
	}
	if major == MajorSimple && info == 31 {
		return nil, -1, nil
	}
	if major != MajorBytes {
		return nil, 0, ErrMalformed
	}
	if info == 31 {
		return nil, 1, nil
	}
	data, err := p.body(length)
	return data, 0, err
}

func (p *byteValueParser) array(info byte, count uint64) ([]byte, error) {
	if info != 31 && count > uint64(len(p.rest)) {
		return nil, io.ErrUnexpectedEOF
	}
	var out []byte
	for index := uint64(0); info == 31 || index < count; index++ {
		if info == 31 && len(p.rest) > 0 && p.rest[0] == 0xff {
			p.rest = p.rest[1:]
			return out, nil
		}
		value, err := p.unsignedByte()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func (p *byteValueParser) unsignedByte() (byte, error) {
	major, info, value, err := p.head()
	for err == nil && major == MajorTag {
		if info == 31 {
			return 0, ErrMalformed
		}
		if value == 2 {
			return p.bigByte()
		}
		if value == 3 {
			return 0, ErrMajor
		}
		major, info, value, err = p.head()
	}
	if err != nil {
		return 0, err
	}
	return plainByte(major, info, value)
}

func plainByte(major, info byte, value uint64) (byte, error) {
	if major != MajorUint || info == 31 || value > 255 {
		return 0, ErrMajor
	}
	return byte(value), nil
}

func (p *byteValueParser) bigByte() (byte, error) {
	major, info, length, err := p.head()
	if err != nil {
		return 0, err
	}
	if major != MajorBytes {
		return 0, ErrMajor
	}
	var data []byte
	if info == 31 {
		data, err = p.chunks()
	} else {
		data, err = p.body(length)
	}
	if err != nil {
		return 0, err
	}
	var value byte
	significant := false
	for _, b := range data {
		if significant {
			return 0, ErrMajor
		}
		value = b
		significant = b != 0
	}
	return value, nil
}
