package hearsay

import (
	"iter"
	"sort"

	"github.com/cirocosta/hearsay/internal/tree"
)

// Snapshot is an immutable point-in-time view. Copies share tree structure.
// Every returned payload is decoded into separate owned storage.
type Snapshot[T any] struct {
	network Network
	tree    tree.Tree[[]byte]
	codec   Codec[T]
}

// Message is one versioned payload or its decoding error.
type Message[T any] struct {
	Version Version
	Payload T
	Err     error
}

// Network returns the network captured by this snapshot.
func (s Snapshot[T]) Network() Network { return s.network }

// Latest returns the causal frontier of sends and redactions.
func (s Snapshot[T]) Latest() Version { return s.tree.Latest() }

// Earliest returns the causal floor of live messages, or false when empty.
func (s Snapshot[T]) Earliest() (Version, bool) { return s.tree.Earliest() }

// Len returns the number of live messages.
func (s Snapshot[T]) Len() int { return s.tree.Len() }

// IsEmpty reports whether there are no live messages.
func (s Snapshot[T]) IsEmpty() bool { return s.Len() == 0 }

// Hash returns the Merkle root of live version addresses.
func (s Snapshot[T]) Hash() [24]byte { return s.tree.Hash() }

// Contains reports whether a version is live.
func (s Snapshot[T]) Contains(v Version) bool { _, ok := s.tree.Get(v); return ok }

// Equal compares network, live version addresses, and causal history.
func (s Snapshot[T]) Equal(other Snapshot[T]) bool {
	return s.network == other.network && s.Latest().Equal(other.Latest()) &&
		s.Hash() == other.Hash()
}

// Get returns an independently decoded payload when the version is live.
func (s Snapshot[T]) Get(v Version) (T, bool, error) {
	p, ok := s.tree.Get(v)
	if !ok {
		var zero T
		return zero, false, nil
	}
	value, err := s.codec.decode(p)
	return value, true, err
}

// Versions iterates live versions in unspecified order.
func (s Snapshot[T]) Versions() iter.Seq[Version] {
	return func(yield func(Version) bool) {
		for v := range s.tree.All() {
			if !yield(v) {
				return
			}
		}
	}
}

// All iterates live messages in unspecified order. A codec error is carried on
// its message, so iteration never hides a malformed payload.
func (s Snapshot[T]) All() iter.Seq[Message[T]] {
	return func(yield func(Message[T]) bool) {
		for v, p := range s.tree.All() {
			value, err := s.codec.decode(p)
			if !yield(Message[T]{v, value, err}) {
				return
			}
		}
	}
}

// Range visits live messages admitted by the version predicate.
func (s Snapshot[T]) Range(accept func(Version) bool) iter.Seq[Message[T]] {
	return func(yield func(Message[T]) bool) {
		for v, p := range s.tree.Range(accept, nil) {
			value, err := s.codec.decode(p)
			if !yield(Message[T]{v, value, err}) {
				return
			}
		}
	}
}

// Causal returns a deterministic total order that places causes before effects.
func (s Snapshot[T]) Causal() []Message[T] {
	messages := make([]Message[T], 0, s.Len())
	for m := range s.All() {
		messages = append(messages, m)
	}
	sort.Slice(
		messages,
		func(i, j int) bool { return messages[i].Version.RankedCompare(messages[j].Version) < 0 },
	)
	return messages
}
