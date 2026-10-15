package before

import "math/big"

// count is an immutable natural number with an allocation-free machine-word path.
type count struct {
	small uint64
	wide  *big.Int
}

func natural(n *big.Int) count {
	if n.Sign() < 0 {
		panic("before: negative count")
	}
	if n.IsUint64() {
		return count{small: n.Uint64()}
	}
	return count{wide: new(big.Int).Set(n)}
}

func (n count) big() *big.Int {
	if n.wide != nil {
		return new(big.Int).Set(n.wide)
	}
	return new(big.Int).SetUint64(n.small)
}

func (n count) cmp(m count) int {
	if n.wide == nil && m.wide == nil {
		if n.small < m.small {
			return -1
		}
		if n.small > m.small {
			return 1
		}
		return 0
	}
	if n.wide == nil {
		return -1
	}
	if m.wide == nil {
		return 1
	}
	return n.wide.Cmp(m.wide)
}

func (n count) add(m count) count {
	if n.wide == nil && m.wide == nil && n.small+m.small >= n.small {
		return count{small: n.small + m.small}
	}
	return natural(new(big.Int).Add(n.big(), m.big()))
}

func (n count) sub(m count) count {
	if n.wide == nil && m.wide == nil {
		if n.small < m.small {
			panic("before: negative count")
		}
		return count{small: n.small - m.small}
	}
	return natural(new(big.Int).Sub(n.big(), m.big()))
}

func maxCount(a, b count) count {
	if a.cmp(b) < 0 {
		return b
	}
	return a
}

func minCount(a, b count) count {
	if a.cmp(b) > 0 {
		return b
	}
	return a
}
