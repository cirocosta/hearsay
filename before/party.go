// Package before implements interval tree clocks. All participants in a causal
// universe must descend from one seed. Fork transfers identity to a new party;
// joining retires the donor. Copying a party or restoring stale bytes can violate
// that rule, which Go's type system cannot enforce. Versions are immutable and
// may be shared freely between goroutines; live parties and clocks require
// external synchronization.
package before

import "errors"

var (
	// ErrOverlap reports an attempted merge of overlapping identities.
	ErrOverlap = errors.New("before: parties overlap")
	// ErrRetired reports use of an identity after ownership was transferred.
	ErrRetired = errors.New("before: party is retired")
)

type identity struct{ left, right *identity }

var owned = &identity{}

// Party owns a nonempty subset of the identity interval. Do not copy Party.
type Party struct{ root *identity }

// SeedParty creates the first participant in a causal universe.
func SeedParty() *Party { return &Party{root: owned} }

// IsSeed reports whether the party owns the complete identity interval.
func (p *Party) IsSeed() bool { return p != nil && p.root == owned }

func (p *Party) alive() *identity {
	if p == nil || p.root == nil {
		panic(ErrRetired)
	}
	return p.root
}

func identityBranch(l, r *identity) *identity {
	if l == nil && r == nil {
		return nil
	}
	if l == owned && r == owned {
		return owned
	}
	return &identity{left: l, right: r}
}

func splitIdentity(p *identity) (*identity, *identity) {
	if p == owned {
		return identityBranch(owned, nil), identityBranch(nil, owned)
	}
	if p.left == nil {
		a, b := splitIdentity(p.right)
		return identityBranch(nil, a), identityBranch(nil, b)
	}
	if p.right == nil {
		a, b := splitIdentity(p.left)
		return identityBranch(a, nil), identityBranch(b, nil)
	}
	return identityBranch(p.left, nil), identityBranch(nil, p.right)
}

// Fork keeps the left share and returns the right share with the same history.
func (p *Party) Fork() *Party { a, b := splitIdentity(p.alive()); p.root = a; return &Party{root: b} }

func disjoint(a, b *identity) bool {
	if a == nil || b == nil {
		return true
	}
	if a == owned || b == owned {
		return false
	}
	return disjoint(a.left, b.left) && disjoint(a.right, b.right)
}

// IsDisjoint reports whether two parties own nonoverlapping regions.
func (p *Party) IsDisjoint(q *Party) bool { return disjoint(p.alive(), q.alive()) }

func unionIdentity(a, b *identity) *identity {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a == owned || b == owned {
		return owned
	}
	return identityBranch(unionIdentity(a.left, b.left), unionIdentity(a.right, b.right))
}

// Join transfers q's identity to p. An unsuccessful join changes neither party.
func (p *Party) Join(q *Party) error {
	a, b := p.alive(), q.alive()
	if !disjoint(a, b) {
		return ErrOverlap
	}
	p.root = unionIdentity(a, b)
	q.root = nil
	return nil
}

func covers(a, b *identity) bool {
	if b == nil || a == owned {
		return true
	}
	if a == nil || b == owned {
		return false
	}
	return covers(a.left, b.left) && covers(a.right, b.right)
}

// Covers reports whether this party owns every region owned by q.
func (p *Party) Covers(q *Party) bool { return covers(p.alive(), q.alive()) }

// Tick records one event in the supplied version.
func (p *Party) Tick(v *Version) { *v = v.Tick(p) }

// Ticks records n events in the supplied version.
func (p *Party) Ticks(v *Version, n uint64) { *v = v.Ticks(p, n) }
