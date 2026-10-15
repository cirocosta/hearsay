package before

import "errors"

// ErrCrossed reports reversed or concurrent endpoints of a causal span.
var ErrCrossed = errors.New("before: span endpoints are crossed")

// Endpoint identifies the lower endpoint, upper endpoint, or both endpoints.
type Endpoint uint8

// Endpoint values returned by span placement.
const (
	Start Endpoint = iota
	End
	Both
)

// Relation identifies a version's detailed position relative to a causal span.
type Relation uint8

// Relation values returned by span placement.
const (
	PlacementBefore Relation = iota
	PlacementAt
	PlacementBetween
	PlacementConcurrent
	PlacementAfter
)

// Placement describes a causal relation and, for equality or concurrency, the
// endpoints concerned. Endpoint is ignored for other relations.
type Placement struct {
	Relation Relation
	Endpoint Endpoint
}

// Dominance describes how much of a span is contained in a version's history.
type Dominance uint8

// Dominance values classify containment of neither endpoint, the start, or both.
const (
	DominanceBefore Dominance = iota
	DominanceBetween
	DominanceAfter
)

// Precedence describes how much of a span causally contains a version's history.
type Precedence uint8

// Precedence values classify a version before both endpoints, the end, or neither.
const (
	PrecedenceBefore Precedence = iota
	PrecedenceBetween
	PrecedenceAfter
)

// Span is an immutable inclusive causal interval lo <= hi. The zero value is
// the point at the empty history. A span contains all histories between its
// endpoints, including histories that were never emitted by a participant.
type Span struct{ lo, hi Version }

// NewSpan constructs an ordered inclusive span or returns ErrCrossed.
func NewSpan(lo, hi Version) (Span, error) {
	if !lo.LessEqual(hi) {
		return Span{}, ErrCrossed
	}
	return Span{lo: lo, hi: hi}, nil
}

// Point returns the span whose two endpoints are v.
func Point(v Version) Span { return Span{lo: v, hi: v} }

// Span returns the smallest causal span containing v and w.
func (v Version) Span(w Version) Span { return Span{lo: v.Meet(w), hi: v.Join(w)} }

// Lo returns the lower endpoint.
func (s Span) Lo() Version { return s.lo }

// Hi returns the upper endpoint.
func (s Span) Hi() Version { return s.hi }

// IntoParts returns immutable snapshots of the two endpoints.
func (s Span) IntoParts() (Version, Version) { return s.lo, s.hi }

// Equal reports equality of both endpoints.
func (s Span) Equal(other Span) bool { return s.lo.Equal(other.lo) && s.hi.Equal(other.hi) }

// Contains reports whether lo <= v <= hi, including either endpoint.
func (s Span) Contains(v Version) bool { return s.lo.LessEqual(v) && v.LessEqual(s.hi) }

// ContainsSpan reports whether both endpoints of other lie within this span.
func (s Span) ContainsSpan(other Span) bool {
	return s.lo.LessEqual(other.lo) && other.hi.LessEqual(s.hi)
}

// Place classifies v against both endpoints. A concurrent relation to one
// endpoint implies the ordered relation to the other endpoint.
func (s Span) Place(v Version) Placement {
	lo, hi := v.Compare(s.lo), v.Compare(s.hi)
	if lo == Equal {
		if hi == Equal {
			return Placement{PlacementAt, Both}
		}
		return Placement{PlacementAt, Start}
	}
	if hi == Equal {
		return Placement{PlacementAt, End}
	}
	if lo == Before {
		return Placement{Relation: PlacementBefore}
	}
	if hi == After {
		return Placement{Relation: PlacementAfter}
	}
	return classifyMiddle(lo, hi)
}

func classifyMiddle(lo, hi Order) Placement {
	if lo == Concurrent {
		if hi == Concurrent {
			return Placement{PlacementConcurrent, Both}
		}
		return Placement{PlacementConcurrent, Start}
	}
	if hi == Concurrent {
		return Placement{PlacementConcurrent, End}
	}
	return Placement{Relation: PlacementBetween}
}

// Dominance reports whether v contains all of the span, its start, or neither.
func (s Span) Dominance(v Version) Dominance {
	if s.hi.LessEqual(v) {
		return DominanceAfter
	}
	if s.lo.LessEqual(v) {
		return DominanceBetween
	}
	return DominanceBefore
}

// Precedence reports whether v precedes all of the span, its end, or neither.
func (s Span) Precedence(v Version) Precedence {
	if v.LessEqual(s.lo) {
		return PrecedenceBefore
	}
	if v.LessEqual(s.hi) {
		return PrecedenceBetween
	}
	return PrecedenceAfter
}

// Union returns the smallest causal span containing both operands.
func (s Span) Union(
	other Span,
) Span {
	return Span{lo: s.lo.Meet(other.lo), hi: s.hi.Join(other.hi)}
}

// UnionAll returns the smallest causal span containing s and every operand.
func (s Span) UnionAll(others ...Span) Span {
	for _, other := range others {
		s = s.Union(other)
	}
	return s
}

// Intersect returns the exact shared causal interval, or false if it is empty.
func (s Span) Intersect(other Span) (Span, bool) {
	lo, hi := s.lo.Join(other.lo), s.hi.Meet(other.hi)
	if !lo.LessEqual(hi) {
		return Span{}, false
	}
	return Span{lo: lo, hi: hi}, true
}

// IntersectAll returns the shared interval of s and every operand.
func (s Span) IntersectAll(others ...Span) (Span, bool) {
	for _, other := range others {
		var ok bool
		s, ok = s.Intersect(other)
		if !ok {
			return Span{}, false
		}
	}
	return s, true
}

// Join returns the pointwise causal join of corresponding endpoints.
func (s Span) Join(
	other Span,
) Span {
	return Span{lo: s.lo.Join(other.lo), hi: s.hi.Join(other.hi)}
}

// JoinAll joins corresponding endpoints of s and every operand.
func (s Span) JoinAll(others ...Span) Span {
	for _, other := range others {
		s = s.Join(other)
	}
	return s
}

// Meet returns the pointwise causal meet of corresponding endpoints.
func (s Span) Meet(
	other Span,
) Span {
	return Span{lo: s.lo.Meet(other.lo), hi: s.hi.Meet(other.hi)}
}

// MeetAll meets corresponding endpoints of s and every operand.
func (s Span) MeetAll(others ...Span) Span {
	for _, other := range others {
		s = s.Meet(other)
	}
	return s
}

// OwnSpan is an immutable view of a span restricted to an identity snapshot.
// The probe histories passed to its methods are not themselves projected.
type OwnSpan struct {
	span     Span
	identity *identity
}

// Project returns a lazy view restricted to the current identity regions of p.
func (s Span) Project(p *Party) OwnSpan { return OwnSpan{span: s, identity: p.alive()} }

// Lo materializes the projected lower endpoint.
func (s OwnSpan) Lo() Version { return Version{root: project(s.span.lo.node(), s.identity)} }

// Hi materializes the projected upper endpoint.
func (s OwnSpan) Hi() Version { return Version{root: project(s.span.hi.node(), s.identity)} }

// ToSpan materializes both projected endpoints.
func (s OwnSpan) ToSpan() Span { return Span{lo: s.Lo(), hi: s.Hi()} }

// Place classifies an unprojected probe against the projected span.
func (s OwnSpan) Place(v Version) Placement { return s.ToSpan().Place(v) }

// Dominance classifies an unprojected probe's containment of the projected span.
func (s OwnSpan) Dominance(v Version) Dominance { return s.ToSpan().Dominance(v) }

// Precedence classifies an unprojected probe's precedence of the projected span.
func (s OwnSpan) Precedence(v Version) Precedence { return s.ToSpan().Precedence(v) }

// Contains reports whether an unprojected probe lies within the projected span.
func (s OwnSpan) Contains(v Version) bool { return s.ToSpan().Contains(v) }
