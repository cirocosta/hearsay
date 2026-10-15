package hearsay

import (
	"iter"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/before/causally"
)

// Query visits live messages admitted by a causal predicate. Cached subtree
// bounds skip excluded branches and admit whole matching branches at once.
func (s Snapshot[T]) Query(query causally.Predicate) iter.Seq[Message[T]] {
	admit := func(floor, ceiling Version) int {
		span, err := before.NewSpan(floor, ceiling)
		if err != nil {
			panic(err)
		}
		return int(query.Coverage(span)) - 1
	}
	return func(yield func(Message[T]) bool) {
		for version, payload := range s.tree.Range(query.Contains, admit) {
			value, err := s.codec.decode(payload)
			if !yield(Message[T]{Version: version, Payload: value, Err: err}) {
				return
			}
		}
	}
}
