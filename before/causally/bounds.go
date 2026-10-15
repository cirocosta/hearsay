package causally

import "github.com/cirocosta/hearsay/before"

// Floor admits versions at or after its inclusive causal bound.
type Floor struct{ at before.Version }

// Ceiling admits versions at or before its inclusive causal bound.
type Ceiling struct{ at before.Version }

// After admits at and every history that contains it, excluding concurrency.
func After(at before.Version) Floor { return Floor{at: at} }

// Before admits at and every history contained in it, excluding concurrency.
func Before(at before.Version) Ceiling { return Ceiling{at: at} }

func (f Floor) query() Query   { return Query{floor: &f.at} }
func (c Ceiling) query() Query { return Query{ceiling: &c.at} }

// Contains reports whether v contains the floor's bound.
func (f Floor) Contains(v before.Version) bool { return f.at.LessEqual(v) }

// Contains reports whether the ceiling's bound contains v.
func (c Ceiling) Contains(v before.Version) bool { return v.LessEqual(c.at) }

// Coverage classifies all histories in an inclusive span against this floor.
func (f Floor) Coverage(s before.Span) Coverage { return f.query().Coverage(s) }

// Coverage classifies all histories in an inclusive span against this ceiling.
func (c Ceiling) Coverage(s before.Span) Coverage { return c.query().Coverage(s) }

// Not complements the floor, retaining the strict past and concurrent versions.
func (f Floor) Not() Query { return Until(f.at) }

// Not complements the ceiling, retaining the strict future and concurrent versions.
func (c Ceiling) Not() Query { return Since(c.at) }

// OrConcurrent widens the floor to also admit histories concurrent with its bound.
func (f Floor) OrConcurrent() Query {
	return Query{holes: []hole{{at: f.at, strict: true}}, polarity: down}
}

// OrConcurrent widens the ceiling to also admit histories concurrent with its bound.
func (c Ceiling) OrConcurrent() Query {
	return Query{holes: []hole{{at: c.at, strict: true}}, polarity: up}
}

// And conjoins the floor with another predicate.
func (f Floor) And(other Predicate) (Query, error) { return f.query().And(other) }

// And conjoins the ceiling with another predicate.
func (c Ceiling) And(other Predicate) (Query, error) { return c.query().And(other) }
