package before

import (
	"errors"
	"math/big"
	"math/bits"
)

// Decode errors distinguish an incomplete value from an invalid spelling.
var (
	ErrTruncated    = errors.New("before: truncated encoding")
	ErrNonCanonical = errors.New("before: noncanonical encoding")
	ErrTrailingData = errors.New("before: trailing bits or invalid padding")
)

type bitWriter struct {
	data []byte
	pos  int
}

func (w *bitWriter) bit(b bool) {
	if w.pos&7 == 0 {
		w.data = append(w.data, 0)
	}
	if b {
		w.data[w.pos>>3] |= 0x80 >> uint(w.pos&7)
	}
	w.pos++
}

func (w *bitWriter) gamma(n count) {
	if n.wide == nil && n.small != ^uint64(0) {
		v := n.small + 1
		k := bits.Len64(v) - 1
		for i := 0; i < k; i++ {
			w.bit(false)
		}
		for i := k; i >= 0; i-- {
			w.bit(v>>uint(i)&1 != 0)
		}
		return
	}
	v := n.big()
	v.Add(v, big.NewInt(1))
	k := v.BitLen() - 1
	for i := 0; i < k; i++ {
		w.bit(false)
	}
	for i := k; i >= 0; i-- {
		w.bit(v.Bit(i) != 0)
	}
}
func (w *bitWriter) finish() []byte { w.bit(true); return w.data }

type bitReader struct {
	data []byte
	pos  int
}

func (r *bitReader) bit() (bool, error) {
	if r.pos>>3 >= len(r.data) {
		return false, ErrTruncated
	}
	b := r.data[r.pos>>3]>>(7-uint(r.pos&7))&1 != 0
	r.pos++
	return b, nil
}

func (r *bitReader) gamma() (count, error) {
	k := 0
	for {
		b, err := r.bit()
		if err != nil {
			return count{}, err
		}
		if b {
			break
		}
		k++
	}
	// The remaining mantissa must fit before allocating a large integer.
	if k > len(r.data)*8-r.pos {
		return count{}, ErrTruncated
	}
	if k < 64 {
		return count{small: r.mantissaWord(k) - 1}, nil
	}
	v := new(big.Int).SetBit(new(big.Int), k, 1)
	for i := k - 1; i >= 0; i-- {
		b, _ := r.bit()
		if b {
			v.SetBit(v, i, 1)
		}
	}
	v.Sub(v, big.NewInt(1))
	return natural(v), nil
}

func (r *bitReader) mantissaWord(k int) uint64 {
	v := uint64(1)
	for i := 0; i < k; i++ {
		b, _ := r.bit()
		v <<= 1
		if b {
			v |= 1
		}
	}
	return v
}

func (r *bitReader) padding() (int, error) {
	end := r.pos/8 + 1
	if end > len(r.data) {
		return 0, ErrTruncated
	}
	rem := 8 - uint(r.pos&7)
	mask := byte((uint16(1) << rem) - 1)
	if r.data[end-1]&mask != 1<<(rem-1) {
		return 0, ErrTrailingData
	}
	return end, nil
}

// MarshalBinary returns the canonical upstream Party encoding. Restoring a
// party duplicates ownership; callers must retire the original before use.
func (p *Party) MarshalBinary() ([]byte, error) {
	if p == nil || p.root == nil {
		return nil, ErrRetired
	}
	w := bitWriter{}
	pending := []*identity{p.root}
	for len(pending) > 0 {
		n := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if n == owned {
			w.bit(false)
			w.bit(false)
			continue
		}
		w.bit(n.left != nil)
		w.bit(n.right != nil)
		if n.right != nil {
			pending = append(pending, n.right)
		}
		if n.left != nil {
			pending = append(pending, n.left)
		}
	}
	return w.finish(), nil
}

type partyFrame struct {
	tag  uint8
	left *identity
}

func (r *bitReader) partyTag() (uint8, error) {
	a, err := r.bit()
	if err != nil {
		return 0, err
	}
	b, err := r.bit()
	if err != nil {
		return 0, err
	}
	var tag uint8
	if a {
		tag |= 2
	}
	if b {
		tag |= 1
	}
	return tag, nil
}

func attachParty(stack *[]partyFrame, n *identity) (*identity, error) {
	for len(*stack) > 0 {
		f := &(*stack)[len(*stack)-1]
		if f.tag == 3 && f.left == nil {
			f.left = n
			return nil, nil
		}
		switch f.tag {
		case 1:
			n = &identity{right: n}
		case 2:
			n = &identity{left: n}
		case 3:
			if f.left == owned && n == owned {
				return nil, ErrNonCanonical
			}
			n = &identity{left: f.left, right: n}
		}
		*stack = (*stack)[:len(*stack)-1]
	}
	return n, nil
}

func decodePartyPrefix(data []byte) (*Party, int, error) {
	r := bitReader{data: data}
	var stack []partyFrame
	for {
		tag, err := r.partyTag()
		if err != nil {
			return nil, 0, err
		}
		if tag != 0 {
			stack = append(stack, partyFrame{tag: tag})
			continue
		}
		n, err := attachParty(&stack, owned)
		if err != nil {
			return nil, 0, err
		}
		if n != nil {
			end, err := r.padding()
			return &Party{root: n}, end, err
		}
	}
}

// DecodeParty validates exactly one canonical party. Decoding does not prove
// exclusive ownership; callers must prevent concurrent use of duplicate parties.
func DecodeParty(data []byte) (*Party, error) {
	p, end, err := decodePartyPrefix(data)
	if err != nil {
		return nil, err
	}
	if end != len(data) {
		return nil, ErrTrailingData
	}
	return p, nil
}

// UnmarshalBinary replaces p only after complete validation.
func (p *Party) UnmarshalBinary(data []byte) error {
	q, err := DecodeParty(data)
	if err != nil {
		return err
	}
	p.root = q.root
	return nil
}

func deltaCode(current, previous count) count {
	if current.cmp(previous) >= 0 {
		d := current.sub(previous)
		return d.add(d)
	}
	d := previous.sub(current)
	return d.add(d).sub(count{small: 1})
}

// MarshalBinary returns the canonical upstream Version encoding.
func (v Version) MarshalBinary() ([]byte, error) {
	return v.AppendBinary(nil), nil
}

func gammaLen(n count) int {
	if n.wide == nil && n.small != ^uint64(0) {
		return 2*bits.Len64(n.small+1) - 1
	}
	v := n.big()
	v.Add(v, big.NewInt(1))
	return 2*v.BitLen() - 1
}

// EncodedLen returns the canonical byte length without producing the encoding.
func (v Version) EncodedLen() int {
	pending := []*event{v.node()}
	previous := count{}
	first := true
	size := 1
	for len(pending) > 0 {
		n := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		size++
		if n.left != nil {
			pending = append(pending, n.right, n.left)
			continue
		}
		payload := n.low
		if !first {
			payload = deltaCode(n.low, previous)
		}
		size += gammaLen(payload)
		previous = n.low
		first = false
	}
	return (size + 7) / 8
}

// AppendBinary appends the canonical encoding to dst and returns the result.
func (v Version) AppendBinary(dst []byte) []byte {
	w := bitWriter{data: dst, pos: len(dst) * 8}
	pending := []*event{v.node()}
	previous := count{}
	first := true
	for len(pending) > 0 {
		n := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if n.left != nil {
			w.bit(false)
			pending = append(pending, n.right, n.left)
			continue
		}
		w.bit(true)
		payload := n.low
		if !first {
			payload = deltaCode(n.low, previous)
		}
		w.gamma(payload)
		previous = n.low
		first = false
	}
	return w.finish()
}

func applyDelta(previous, code count) (count, error) {
	if code.wide == nil {
		if code.small&1 == 0 {
			return previous.add(count{small: code.small / 2}), nil
		}
		d := count{small: code.small/2 + 1}
		if previous.cmp(d) < 0 {
			return count{}, ErrNonCanonical
		}
		return previous.sub(d), nil
	}
	v := code.big()
	negative := v.Bit(0) != 0
	v.Rsh(v, 1)
	if negative {
		v.Add(v, big.NewInt(1))
	}
	d := natural(v)
	if negative {
		if previous.cmp(d) < 0 {
			return count{}, ErrNonCanonical
		}
		return previous.sub(d), nil
	}
	return previous.add(d), nil
}

type versionFrame struct{ left *event }

func attachVersion(stack *[]versionFrame, n *event) (*event, error) {
	for len(*stack) > 0 {
		f := &(*stack)[len(*stack)-1]
		if f.left == nil {
			f.left = n
			return nil, nil
		}
		if f.left.left == nil && n.left == nil && f.left.low.cmp(n.low) == 0 {
			return nil, ErrNonCanonical
		}
		n = branch(f.left, n)
		*stack = (*stack)[:len(*stack)-1]
	}
	return n, nil
}

type heightReader struct {
	previous count
	first    bool
}

func (h *heightReader) read(r *bitReader) (count, error) {
	code, err := r.gamma()
	if err != nil {
		return count{}, err
	}
	height := code
	if !h.first {
		height, err = applyDelta(h.previous, code)
	}
	h.previous = height
	h.first = false
	return height, err
}

// DecodeVersion validates exactly one canonical version, including arbitrary
// precision counts, nonnegative heights, topology and marker padding.
func DecodeVersion(data []byte) (Version, error) {
	v, end, err := decodeVersionPrefix(data)
	if err != nil {
		return Version{}, err
	}
	if end != len(data) {
		return Version{}, ErrTrailingData
	}
	return v, nil
}

func decodeVersionPrefix(data []byte) (Version, int, error) {
	r := bitReader{data: data}
	var stack []versionFrame
	h := heightReader{first: true}
	for {
		n, err := r.versionLeaf(&stack, &h)
		if err != nil {
			return Version{}, 0, err
		}
		n, err = attachVersion(&stack, n)
		if err != nil {
			return Version{}, 0, err
		}
		if n != nil {
			end, err := r.padding()
			return Version{root: n}, end, err
		}
	}
}

func (r *bitReader) versionLeaf(stack *[]versionFrame, h *heightReader) (*event, error) {
	for {
		isLeaf, err := r.bit()
		if err != nil {
			return nil, err
		}
		if isLeaf {
			break
		}
		*stack = append(*stack, versionFrame{})
	}
	height, err := h.read(r)
	if err != nil {
		return nil, err
	}
	return leaf(height), nil
}

// UnmarshalBinary replaces v only after complete validation.
func (v *Version) UnmarshalBinary(data []byte) error {
	w, err := DecodeVersion(data)
	if err != nil {
		return err
	}
	*v = w
	return nil
}

// MarshalBinary concatenates independently padded Party and Version encodings.
func (c *Clock) MarshalBinary() ([]byte, error) {
	if c == nil {
		return nil, ErrRetired
	}
	p, err := c.party.MarshalBinary()
	if err != nil {
		return nil, err
	}
	v, _ := c.version.MarshalBinary()
	return append(p, v...), nil
}

// DecodeClock validates a Party followed by one Version. The caller must ensure
// that restoring the clock does not duplicate a live participant's identity.
func DecodeClock(data []byte) (*Clock, error) {
	p, end, err := decodePartyPrefix(data)
	if err != nil {
		return nil, err
	}
	v, err := DecodeVersion(data[end:])
	if err != nil {
		return nil, err
	}
	return FromParts(p, v), nil
}

// UnmarshalBinary replaces c only after complete validation.
func (c *Clock) UnmarshalBinary(data []byte) error {
	d, err := DecodeClock(data)
	if err != nil {
		return err
	}
	*c = *d
	return nil
}
