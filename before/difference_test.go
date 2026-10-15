package before

import (
	"math/rand/v2"
	"testing"
)

func TestPartyWithout(t *testing.T) {
	whole := SeedParty()
	exclusion := SeedParty()
	left := exclusion.Fork()
	remainder := whole.Without(left)
	if whole.root != nil || remainder == nil || !remainder.IsDisjoint(left) ||
		!remainder.Covers(exclusion) {
		t.Fatal("identity difference or ownership transfer")
	}
	if !left.IsDisjoint(remainder) {
		t.Fatal("subtraction changed excluded identity")
	}
	if err := remainder.Join(left); err != nil {
		t.Fatal(err)
	}
	if !remainder.IsSeed() {
		t.Fatal("subtraction failed conservation")
	}
}

func TestPartyWithoutEmpty(t *testing.T) {
	p := SeedParty()
	if got := p.Without(p); got != nil || p.root != nil {
		t.Fatal("self subtraction must retire and return nil")
	}
	p, q := SeedParty(), SeedParty()
	if got := p.Without(q); got != nil || p.root != nil || !q.IsSeed() {
		t.Fatal("covered subtraction")
	}
}

func TestPartyWithoutLaws(t *testing.T) {
	rng := rand.New(rand.NewPCG(33, 85))
	roots := []*identity{owned}
	for range 100 {
		p := &Party{root: roots[rng.IntN(len(roots))]}
		q := p.Fork()
		roots = append(roots, p.root, q.root)
	}
	for _, a := range roots {
		for _, b := range roots {
			diff := subtractIdentity(a, b)
			if !disjoint(diff, b) || !covers(a, diff) {
				t.Fatal("difference locality")
			}
			if (diff == nil) != covers(b, a) {
				t.Fatal("difference emptiness")
			}
			if disjoint(a, b) && !covers(diff, a) {
				t.Fatal("disjoint difference changed source")
			}
		}
	}
}
