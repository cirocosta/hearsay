package bookmark

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cirocosta/hearsay/internal/cbor"
)

type parser struct {
	input []byte
	pos   int
}

// head accepts the legal alternate spellings accepted by Serde, while the
// encoder always writes shortest-form heads. Validation runs before parsing.
func (p *parser) head() (byte, uint64, bool, error) {
	if p.pos == len(p.input) {
		return 0, 0, false, io.ErrUnexpectedEOF
	}
	initial := p.input[p.pos]
	p.pos++
	major, info := initial>>5, initial&31
	if info < 24 {
		return major, uint64(info), false, nil
	}
	if info == 31 {
		return major, 0, true, nil
	}
	if info > 27 {
		return 0, 0, false, cbor.ErrReserved
	}
	width := 1 << (info - 24)
	if len(p.input)-p.pos < width {
		return 0, 0, false, io.ErrUnexpectedEOF
	}
	data := p.input[p.pos : p.pos+width]
	p.pos += width
	var value uint64
	switch width {
	case 1:
		value = uint64(data[0])
	case 2:
		value = uint64(binary.BigEndian.Uint16(data))
	case 4:
		value = uint64(binary.BigEndian.Uint32(data))
	case 8:
		value = binary.BigEndian.Uint64(data)
	}
	return major, value, false, nil
}

func (p *parser) want(major byte) (uint64, bool, error) {
	found, value, indefinite, err := p.head()
	if err != nil {
		return 0, false, err
	}
	if found != major {
		return 0, false, cbor.ErrMajor
	}
	return value, indefinite, nil
}

func (p *parser) tag(tag uint64) error {
	value, indefinite, err := p.want(cbor.MajorTag)
	if err != nil {
		return err
	}
	if indefinite || value != tag {
		return cbor.ErrMalformed
	}
	return nil
}

func (p *parser) uint() (uint64, error) {
	for {
		major, value, indefinite, err := p.head()
		if err != nil {
			return 0, err
		}
		if indefinite {
			return 0, cbor.ErrMalformed
		}
		switch major {
		case cbor.MajorUint:
			return value, nil
		case cbor.MajorTag:
			switch value {
			case 2:
				return p.bigUint()
			case 3:
				return 0, cbor.ErrMajor
			}
		default:
			return 0, cbor.ErrMajor
		}
	}
}

func (p *parser) array() (int, error) {
	start := p.pos
	major, count, indefinite, err := p.untaggedHead()
	if err != nil {
		return 0, err
	}
	if major == cbor.MajorBytes {
		// Ciborium presents byte strings to sequence visitors as u8 elements.
		// Every element type here is a tagged tuple or atom, so only an empty
		// byte string can represent one of these sequences successfully.
		p.pos = start
		return p.emptyByteSequence()
	}
	if major != cbor.MajorArray {
		return 0, cbor.ErrMajor
	}
	if indefinite {
		return -1, nil
	}
	if count > uint64(len(p.input)-p.pos) {
		return 0, io.ErrUnexpectedEOF
	}
	return int(count), nil
}

func (p *parser) untaggedHead() (byte, uint64, bool, error) {
	major, value, indefinite, err := p.head()
	for err == nil && major == cbor.MajorTag {
		if indefinite {
			return 0, 0, false, cbor.ErrMalformed
		}
		major, value, indefinite, err = p.head()
	}
	return major, value, indefinite, err
}

func (p *parser) bigUint() (uint64, error) {
	start := p.pos
	major, _, _, err := p.head()
	if err != nil {
		return 0, err
	}
	if major != cbor.MajorBytes {
		return 0, cbor.ErrMajor
	}
	p.pos = start
	raw, err := p.bytes()
	if err != nil {
		return 0, err
	}
	raw = bytes.TrimLeft(raw, "\x00")
	if len(raw) > 8 {
		return 0, cbor.ErrMajor
	}
	var value uint64
	for _, digit := range raw {
		value = value<<8 | uint64(digit)
	}
	return value, nil
}

func (p *parser) emptyByteSequence() (int, error) {
	value, err := p.bytes()
	if err != nil {
		return 0, err
	}
	if len(value) != 0 {
		return 0, cbor.ErrMajor
	}
	return 0, nil
}

func (p *parser) more(count, i int) bool {
	if count >= 0 {
		return i < count
	}
	return p.pos < len(p.input) && p.input[p.pos] != 0xff
}

func (p *parser) end(count, actual int) error {
	if count >= 0 {
		if count != actual {
			return cbor.ErrMalformed
		}
		return nil
	}
	if p.pos >= len(p.input) {
		return io.ErrUnexpectedEOF
	}
	if p.input[p.pos] != 0xff {
		return cbor.ErrMalformed
	}
	p.pos++
	return nil
}

func (p *parser) bytes() ([]byte, error) {
	n, err := cbor.ScanWellFormed(p.input[p.pos:])
	if err != nil {
		return nil, err
	}
	value, err := cbor.DecodeByteString(p.input[p.pos : p.pos+n])
	if err != nil {
		return nil, err
	}
	p.pos += n
	return bytes.Clone(value), nil
}
