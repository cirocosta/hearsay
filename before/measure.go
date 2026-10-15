package before

import "math/big"

// MinTicks returns the minimum number of tick events that could have produced
// v. It is an exact lower bound on history size, not a bound on leaf height.
func (v Version) MinTicks() *big.Int {
	var leaves, shared count
	pending := []*event{v.node()}
	for len(pending) > 0 {
		n := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if n.left == nil {
			leaves = leaves.add(n.low)
			continue
		}
		shared = shared.add(n.low)
		pending = append(pending, n.right, n.left)
	}
	return leaves.sub(shared).big()
}
