package before

import (
	"bytes"
	"math/big"
)

// Order describes a causal partial order; Concurrent denotes incomparable values.
type Order int8

// Causal order values returned by Compare.
const (
	Before     Order = -1
	Equal      Order = 0
	After      Order = 1
	Concurrent Order = 2
)

// event stores absolute leaf heights. Subtrees are immutable, canonical and
// shared by snapshots. Cached extrema avoid repeated scans during fill.
type event struct {
	left, right *event
	low, high   count
}

var zeroEvent = &event{}

func leaf(n count) *event {
	if n.cmp(count{}) == 0 {
		return zeroEvent
	}
	return &event{low: n, high: n}
}

func branch(l, r *event) *event {
	if l.left == nil && r.left == nil && l.low.cmp(r.low) == 0 {
		return l
	}
	return &event{left: l, right: r, low: minCount(l.low, r.low), high: maxCount(l.high, r.high)}
}

func children(e *event) (*event, *event) {
	if e.left == nil {
		return e, e
	}
	return e.left, e.right
}

// Version is an immutable causal timestamp. Its zero value is the empty history.
type Version struct{ root *event }

func (v Version) node() *event {
	if v.root == nil {
		return zeroEvent
	}
	return v.root
}

// IsEmpty reports whether the history contains no events.
func (v Version) IsEmpty() bool { return v.node().high.cmp(count{}) == 0 }

// Clone returns an immutable snapshot sharing the same storage.
func (v Version) Clone() Version { return v }

func equalEvents(a, b *event) bool {
	if a == b {
		return true
	}
	if a.low.cmp(b.low) != 0 || a.high.cmp(b.high) != 0 {
		return false
	}
	if a.left == nil || b.left == nil {
		return a.left == nil && b.left == nil
	}
	return equalEvents(a.left, b.left) && equalEvents(a.right, b.right)
}

// Equal reports whether two versions contain identical histories.
func (v Version) Equal(w Version) bool { return equalEvents(v.node(), w.node()) }

func lessEvents(a, b *event) bool {
	if a == b || a.high.cmp(b.low) <= 0 {
		return true
	}
	if a.low.cmp(b.high) > 0 {
		return false
	}
	al, ar := children(a)
	bl, br := children(b)
	return lessEvents(al, bl) && lessEvents(ar, br)
}

// LessEqual reports whether every event in v is contained in w.
func (v Version) LessEqual(w Version) bool { return lessEvents(v.node(), w.node()) }

// Compare returns the causal partial ordering of two histories.
func (v Version) Compare(w Version) Order {
	if v.Equal(w) {
		return Equal
	}
	if v.LessEqual(w) {
		return Before
	}
	if w.LessEqual(v) {
		return After
	}
	return Concurrent
}

// Concurrent reports whether neither history contains the other.
func (v Version) Concurrent(w Version) bool { return v.Compare(w) == Concurrent }

func combine(a, b *event, meet bool) *event {
	if a == b {
		return a
	}
	if a.high.cmp(b.low) <= 0 {
		if meet {
			return a
		}
		return b
	}
	if b.high.cmp(a.low) <= 0 {
		if meet {
			return b
		}
		return a
	}
	al, ar := children(a)
	bl, br := children(b)
	return branch(combine(al, bl, meet), combine(ar, br, meet))
}

// Join returns the least history containing both versions.
func (v Version) Join(
	w Version,
) Version {
	return Version{root: combine(v.node(), w.node(), false)}
}

// Meet returns the greatest history contained in both versions.
func (v Version) Meet(w Version) Version { return Version{root: combine(v.node(), w.node(), true)} }

// JoinAll returns the least history containing all supplied versions and v.
func (v Version) JoinAll(versions ...Version) Version {
	for _, w := range versions {
		v = v.Join(w)
	}
	return v
}

// MeetAll returns the greatest history contained in all supplied versions and v.
func (v Version) MeetAll(versions ...Version) Version {
	for _, w := range versions {
		v = v.Meet(w)
	}
	return v
}

func project(e *event, p *identity) *event {
	if p == nil {
		return zeroEvent
	}
	if p == owned {
		return e
	}
	l, r := children(e)
	return branch(project(l, p.left), project(r, p.right))
}

// Project returns the history within the identity regions owned by p.
func (v Version) Project(p *Party) Version { return Version{root: project(v.node(), p.alive())} }

func fill(e *event, p *identity) *event {
	if p == nil || e.left == nil {
		return e
	}
	if p == owned {
		return leaf(e.high)
	}
	l, r := fill(e.left, p.left), fill(e.right, p.right)
	// fully owned children are leaves; unchanged heights retain their subtree.
	if p.left == owned {
		l = fillOwned(l, r.low)
	}
	if p.right == owned {
		r = fillOwned(r, l.low)
	}
	if l == e.left && r == e.right {
		return e
	}
	return branch(l, r)
}

func fillOwned(e *event, minimum count) *event {
	if e.low.cmp(minimum) < 0 {
		return leaf(minimum)
	}
	return e
}

type growthCost struct{ branches, depth int }

func (a growthCost) less(b growthCost) bool {
	return a.branches < b.branches || a.branches == b.branches && a.depth < b.depth
}

// probe stores route choices as packed indices. Zero encodes the terminal next
// index; the low bit selects right. Unchosen routes remain cheap arena words.
// Fewer new branches win, then smaller depth, with right winning ties.
func probe(e *event, p *identity, routes []uint64) (growthCost, int, []uint64) {
	if p == owned {
		return growthCost{}, -1, routes
	} // fill has already collapsed this case.
	l, r := children(e)
	var cost growthCost
	var next int
	right := false
	if p.left == nil {
		cost, next, routes = probe(r, p.right, routes)
		right = true
	} else if p.right == nil {
		cost, next, routes = probe(l, p.left, routes)
	} else {
		a, an, leftRoutes := probe(l, p.left, routes)
		b, bn, rightRoutes := probe(r, p.right, leftRoutes)
		routes = rightRoutes
		cost, next, right = b, bn, true
		if a.less(b) {
			cost, next, right = a, an, false
		}
	}
	cost.depth++
	if e.left == nil {
		cost.branches++
	}
	index := len(routes)
	return cost, index, appendRoute(routes, next, right)
}

func appendRoute(routes []uint64, next int, right bool) []uint64 {
	word := uint64(next+1) << 1
	if right {
		word |= 1
	}
	return append(routes, word)
}

func grow(e *event, routes []uint64, index int, n count) *event {
	if index < 0 {
		return leaf(e.low.add(n))
	}
	l, r := children(e)
	word := routes[index]
	next := int(word>>1) - 1
	if word&1 != 0 {
		return branch(l, grow(r, routes, next, n))
	}
	return branch(grow(l, routes, next, n), r)
}

// Tick returns the history after one event recorded by p.
func (v Version) Tick(p *Party) Version { return v.Ticks(p, 1) }

// Ticks returns the history after n events recorded by p.
func (v Version) Ticks(p *Party, n uint64) Version { return v.ticks(p, count{small: n}) }

// TicksBig advances by an arbitrary natural count, exactly as repeated Tick calls.
func (v Version) TicksBig(p *Party, n *big.Int) Version { return v.ticks(p, natural(n)) }

func (v Version) ticks(p *Party, n count) Version {
	i := p.alive()
	if n.cmp(count{}) == 0 {
		return v
	}
	e := v.node()
	f := fill(e, i)
	if !equalEvents(e, f) {
		n = n.sub(count{small: 1})
		if n.cmp(count{}) == 0 {
			return Version{root: f}
		}
	}
	if i == owned {
		return Version{root: leaf(f.low.add(n))}
	}
	return Version{root: growPartial(f, i, n)}
}

func growPartial(f *event, i *identity, n count) *event {
	var local [64]uint64
	routes := local[:0]
	_, path, routes := probe(f, i, routes)
	return grow(f, routes, path, n)
}

// Minimum returns the minimum event count across the complete interval.
func (v Version) Minimum() *big.Int { return v.node().low.big() }

// Maximum returns the maximum event count across the complete interval.
func (v Version) Maximum() *big.Int { return v.node().high.big() }

func eventArea(e *event) *big.Rat {
	if e.left == nil {
		return new(big.Rat).SetInt(e.low.big())
	}
	r := new(big.Rat).Add(eventArea(e.left), eventArea(e.right))
	return r.Quo(r, big.NewRat(2, 1))
}

// Rank returns the exact area under the version's event-height function. Every
// causal increase strictly increases rank; concurrent versions may have ties.
func (v Version) Rank() *big.Rat { return eventArea(v.node()) }

// RankedCompare orders causes before effects, breaking equal ranks by the
// canonical version bytes. It returns -1, 0, or 1 and is deterministic across peers.
func (v Version) RankedCompare(w Version) int {
	if v.Equal(w) {
		return 0
	}
	if c := v.Rank().Cmp(w.Rank()); c != 0 {
		return c
	}
	return bytes.Compare(v.AppendBinary(nil), w.AppendBinary(nil))
}
