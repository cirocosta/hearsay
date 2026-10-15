// Package causally constructs exact causal predicates over versions and spans.
package causally

import (
	"errors"

	"github.com/cirocosta/hearsay/before"
)

// ErrPolarity reports an attempt to combine excluded up-sets and down-sets.
// Opposing holes can require combinatorial search to decide exact span coverage.
var ErrPolarity = errors.New("causally: opposite exclusion polarities")

// Coverage describes how much of an inclusive causal span a query admits.
type Coverage uint8

// Coverage values distinguish no admitted history, some histories, and all.
const (
	Empty Coverage = iota
	Partial
	Full
)

type polarity uint8

const (
	neutral polarity = iota
	down
	up
)

type hole struct {
	at     before.Version
	strict bool
}

// Predicate is a causal query or an atomic inclusive bound. Its closed set of
// implementations preserves the uniform exclusion polarity needed for coverage.
type Predicate interface {
	Contains(before.Version) bool
	Coverage(before.Span) Coverage
	query() Query
}

// Query is an immutable conjunction of causal bounds and same-direction
// exclusions. The zero value admits every version. Query values may be copied
// and shared concurrently; And does not modify either operand.
type Query struct {
	floor, ceiling *before.Version
	holes          []hole
	polarity       polarity
}

func (q Query) query() Query { return q }

// All admits every causal history.
func All() Query { return Query{} }

// FromSpan admits exactly the histories contained in the span.
func FromSpan(s before.Span) Query {
	lo, hi := s.Lo(), s.Hi()
	return Query{floor: &lo, ceiling: &hi}
}

// FromVersion admits exactly one version's history.
func FromVersion(v before.Version) Query { return Query{floor: &v, ceiling: &v} }

// Since admits histories not contained in at: its future and concurrent values.
func Since(at before.Version) Query { return Query{holes: []hole{{at: at}}, polarity: down} }

// Until admits histories that do not yet contain at: its past and concurrent values.
func Until(at before.Version) Query { return Query{holes: []hole{{at: at}}, polarity: up} }

// StrictlyAfter admits at's strict causal future, excluding concurrency.
func StrictlyAfter(at before.Version) Query { q := Since(at); q.floor = &at; return q }

// StrictlyBefore admits at's strict causal past, excluding concurrency.
func StrictlyBefore(at before.Version) Query { q := Until(at); q.ceiling = &at; return q }

// Delta admits histories at or before end, excluding all histories at or before start.
func Delta(start, end before.Version) Query { q := Since(start); q.ceiling = &end; return q }

// Toward admits histories at or after start that do not yet contain end.
func Toward(start, end before.Version) Query { q := Until(end); q.floor = &start; return q }

func holeContains(h hole, p polarity, v before.Version) bool {
	a, b := v, h.at
	if p == up {
		a, b = b, a
	}
	if h.strict {
		return a.Compare(b) == before.Before
	}
	return a.LessEqual(b)
}

func (q Query) excluded(v before.Version) bool {
	for _, h := range q.holes {
		if holeContains(h, q.polarity, v) {
			return true
		}
	}
	return false
}

// Contains reports whether the query admits a concrete version.
func (q Query) Contains(v before.Version) bool {
	if q.floor != nil && !q.floor.LessEqual(v) {
		return false
	}
	if q.ceiling != nil && !v.LessEqual(*q.ceiling) {
		return false
	}
	return !q.excluded(v)
}

func (q Query) admitsAll(s before.Span) bool {
	if q.floor != nil && !q.floor.LessEqual(s.Lo()) {
		return false
	}
	if q.ceiling != nil && !s.Hi().LessEqual(*q.ceiling) {
		return false
	}
	endpoint := s.Lo()
	if q.polarity == up {
		endpoint = s.Hi()
	}
	return !q.excluded(endpoint)
}

func (q Query) clamp(s before.Span) (before.Version, before.Version) {
	lo, hi := s.Lo(), s.Hi()
	if q.floor != nil {
		lo = lo.Join(*q.floor)
	}
	if q.ceiling != nil {
		hi = hi.Meet(*q.ceiling)
	}
	return lo, hi
}

// Coverage returns exact membership over every history between the span's
// endpoints. The query first clamps the interval to its inclusive bounds. A
// union of down-set exclusions covers that interval exactly when it covers the
// clamped top; an up-set union is decided by the clamped bottom.
func (q Query) Coverage(s before.Span) Coverage {
	if q.admitsAll(s) {
		return Full
	}
	lo, hi := q.clamp(s)
	if !lo.LessEqual(hi) {
		return Empty
	}
	endpoint := hi
	if q.polarity == up {
		endpoint = lo
	}
	if q.excluded(endpoint) {
		return Empty
	}
	return Partial
}

func joinFloor(a, b *before.Version) *before.Version {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	joined := a.Join(*b)
	return &joined
}

func meetCeiling(a, b *before.Version) *before.Version {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	meet := a.Meet(*b)
	return &meet
}

// And constructs a conjunction. Opposite exclusion polarities return
// ErrPolarity, matching the combinations excluded by the upstream type system.
func (q Query) And(other Predicate) (Query, error) {
	r := other.query()
	if q.polarity != neutral && r.polarity != neutral && q.polarity != r.polarity {
		return Query{}, ErrPolarity
	}
	p := q.polarity
	if p == neutral {
		p = r.polarity
	}
	holes := make([]hole, 0, len(q.holes)+len(r.holes))
	holes = append(holes, q.holes...)
	holes = append(holes, r.holes...)
	return Query{
		floor:    joinFloor(q.floor, r.floor),
		ceiling:  meetCeiling(q.ceiling, r.ceiling),
		holes:    holes,
		polarity: p,
	}, nil
}
