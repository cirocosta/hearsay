package bookmark

import (
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
	value, indefinite, err := p.want(cbor.MajorUint)
	if indefinite {
		return 0, cbor.ErrMalformed
	}
	return value, err
}

func (p *parser) array() (int, error) {
	count, indefinite, err := p.want(cbor.MajorArray)
	if err != nil {
		return 0, err
	}
	if indefinite {
		return -1, nil
	}
	if count > uint64(len(p.input)-p.pos) {
		return 0, io.ErrUnexpectedEOF
	}
	return int(count), nil
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
	if p.pos == len(p.input) {
		return nil, io.ErrUnexpectedEOF
	}
	if p.input[p.pos]>>5 == cbor.MajorArray {
		return p.byteArray()
	}
	count, indefinite, err := p.want(cbor.MajorBytes)
	if err != nil {
		return nil, err
	}
	if !indefinite {
		return p.take(count)
	}
	return p.byteChunks()
}

func (p *parser) byteArray() ([]byte, error) {
	count, err := p.array()
	if err != nil {
		return nil, err
	}
	out := []byte{}
	for i := 0; p.more(count, i); i++ {
		value, err := p.uint()
		if err != nil {
			return nil, err
		}
		if value > 255 {
			return nil, cbor.ErrMalformed
		}
		out = append(out, byte(value))
	}
	return out, p.end(count, len(out))
}

func (p *parser) byteChunks() ([]byte, error) {
	out := []byte{}
	for p.more(-1, 0) {
		count, indefinite, err := p.want(cbor.MajorBytes)
		if err != nil {
			return nil, err
		}
		if indefinite {
			return nil, cbor.ErrMalformed
		}
		chunk, err := p.take(count)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
	}
	return out, p.end(-1, 0)
}

func (p *parser) take(count uint64) ([]byte, error) {
	if count > uint64(len(p.input)-p.pos) {
		return nil, io.ErrUnexpectedEOF
	}
	out := append([]byte{}, p.input[p.pos:p.pos+int(count)]...)
	p.pos += int(count)
	return out, nil
}
