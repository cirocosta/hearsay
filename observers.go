package hearsay

import (
	"bytes"
	"context"
	"math/big"
	"sort"

	"github.com/cirocosta/hearsay/internal/tree"
)

// PollState distinguishes an available observation, an idle live replica, and
// a replica whose final state has been drained.
type PollState uint8

// Nonblocking observation states.
const (
	Quiet PollState = iota
	Ready
	Ended
)

// observationSource retains replica content without retaining identity ownership.
type observationSource[T any] struct {
	state *replica[T]
	seen  <-chan struct{}
}

func (s *observationSource[T]) view() (*published, <-chan struct{}, bool) {
	if s.state == nil {
		return nil, nil, true
	}
	return s.state.observation()
}

func (s *observationSource[T]) message(v Version, payload []byte) Message[T] {
	value, err := s.state.codec.decode(payload)
	return Message[T]{Version: v, Payload: value, Err: err}
}

func (s *observationSource[T]) next() (*published, PollState, <-chan struct{}, <-chan struct{}) {
	pub, done, terminal := s.view()
	if pub == nil {
		return nil, Ended, nil, done
	}
	if pub.changed != s.seen {
		s.seen = pub.changed
		return pub, Ready, nil, done
	}
	if terminal {
		return nil, Ended, nil, done
	}
	return nil, Quiet, pub.changed, done
}

// waitObservation waits on the exact snapshot inspected by the preceding poll.
// Closing its change channel cannot be lost between inspection and waiting.
func waitObservation(ctx context.Context, changed, done <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-done:
		return nil
	}
}

// Changes reports coalesced causal frontier advances, including redactions.
// Its first observation is always Ready, even for an empty replica. Observers
// do not retain writable handles. Use one goroutine at a time per observer.
type Changes[T any] struct {
	source observationSource[T]
	seen   Version
	first  bool
}

// Changes observes the current frontier immediately, then coalesced advances.
func (r *Rumors[T]) Changes() *Changes[T] {
	return &Changes[T]{source: observationSource[T]{state: r.state}, first: true}
}

func (c *Changes[T]) poll() (PollState, <-chan struct{}, <-chan struct{}) {
	pub, done, terminal := c.source.view()
	if pub == nil {
		return Ended, nil, done
	}
	latest := pub.tree.Latest()
	if c.first || !c.seen.Equal(latest) {
		c.first = false
		c.seen = latest
		return Ready, nil, done
	}
	if terminal {
		return Ended, nil, done
	}
	return Quiet, pub.changed, done
}

// TryNext takes one nonblocking step. Ready is one change signal, not a count
// of commits. Ended follows the final change when every producer has closed.
func (c *Changes[T]) TryNext() PollState {
	state, _, _ := c.poll()
	return state
}

// Next waits for one signal. False with no error means the replica ended.
// Cancellation consumes no signal and the observer can be used again.
func (c *Changes[T]) Next(ctx context.Context) (bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		state, changed, done := c.poll()
		if state != Quiet {
			return state == Ready, nil
		}
		if err := waitObservation(ctx, changed, done); err != nil {
			return false, err
		}
	}
}

// UnorderedMessages delivers snapshot passes in unspecified order. A captured
// pass remains readable through later redactions. Messages redacted before its
// capture produce no event. Use one goroutine at a time per observer.
type UnorderedMessages[T any] struct {
	source     observationSource[T]
	checkpoint Version
	ceiling    Version
	walk       *tree.Iterator[[]byte]
}

// UnorderedMessages observes all live content and subsequent additions.
func (r *Rumors[T]) UnorderedMessages() *UnorderedMessages[T] {
	return r.UnorderedMessagesSince(Version{})
}

// UnorderedMessagesSince observes live content not covered by since.
func (r *Rumors[T]) UnorderedMessagesSince(since Version) *UnorderedMessages[T] {
	return &UnorderedMessages[T]{source: observationSource[T]{state: r.state}, checkpoint: since}
}

// Checkpoint is the frontier of completed passes. An interrupted pass may replay
// arbitrarily many messages on resume, but never skips an undelivered message.
// The last message in a pass becomes covered on the following poll.
func (u *UnorderedMessages[T]) Checkpoint() Version { return u.checkpoint }

func (u *UnorderedMessages[T]) pop() (Message[T], bool) {
	if u.walk == nil {
		return Message[T]{}, false
	}
	if v, payload, ok := u.walk.Next(); ok {
		return u.source.message(v, payload), true
	}
	u.checkpoint = u.checkpoint.Join(u.ceiling)
	u.walk = nil
	return Message[T]{}, false
}

func (u *UnorderedMessages[T]) poll() (Message[T], PollState, <-chan struct{}, <-chan struct{}) {
	for {
		if message, ok := u.pop(); ok {
			return message, Ready, nil, nil
		}
		pub, state, changed, done := u.source.next()
		if state != Ready {
			return Message[T]{}, state, changed, done
		}
		u.ceiling = pub.tree.Latest()
		u.walk = pub.tree.Since(u.checkpoint)
	}
}

// TryNext takes one nonblocking delivery step.
func (u *UnorderedMessages[T]) TryNext() (Message[T], PollState) {
	message, state, _, _ := u.poll()
	return message, state
}

// Next waits for a message. False with no error means final drain is complete.
// Codec failures remain on Message.Err. Cancellation preserves the pass cursor.
func (u *UnorderedMessages[T]) Next(ctx context.Context) (Message[T], bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Message[T]{}, false, err
		}
		message, state, changed, done := u.poll()
		if state != Quiet {
			return message, state == Ready, nil
		}
		if err := waitObservation(ctx, changed, done); err != nil {
			return Message[T]{}, false, err
		}
	}
}

type causalItem struct {
	version Version
	payload []byte
	rank    *big.Rat
	order   []byte
}

// CausalMessages delivers causes before their effects. Concurrent messages may
// have different relative orders between passes. Sorting a captured pass costs
// O(n log n) time and O(n) references. Use one goroutine at a time per observer.
type CausalMessages[T any] struct {
	source     observationSource[T]
	checkpoint Version
	ingested   Version
	staged     []causalItem
}

// CausalMessages observes all live content and additions in causal order.
func (r *Rumors[T]) CausalMessages() *CausalMessages[T] {
	return r.CausalMessagesSince(Version{})
}

// CausalMessagesSince observes content not covered by since in causal order.
func (r *Rumors[T]) CausalMessagesSince(since Version) *CausalMessages[T] {
	return &CausalMessages[T]{
		source:     observationSource[T]{state: r.state},
		checkpoint: since,
		ingested:   since,
	}
}

// Checkpoint covers completed delivery passes. It trails the staged backlog and
// advances only on the poll after its final message, so crash recovery replays
// the message still in flight instead of skipping it.
func (c *CausalMessages[T]) Checkpoint() Version { return c.checkpoint }

func (c *CausalMessages[T]) pop() (Message[T], bool) {
	if len(c.staged) == 0 {
		c.staged = nil
		return Message[T]{}, false
	}
	item := c.staged[0]
	c.staged[0] = causalItem{}
	c.staged = c.staged[1:]
	return c.source.message(item.version, item.payload), true
}

func (c *CausalMessages[T]) ingest(pub *published) {
	walk := pub.tree.Since(c.ingested)
	for {
		v, payload, ok := walk.Next()
		if !ok {
			break
		}
		c.staged = append(
			c.staged,
			causalItem{version: v, payload: payload, rank: v.Rank(), order: v.AppendBinary(nil)},
		)
	}
	c.ingested = c.ingested.Join(pub.tree.Latest())
	sort.Slice(c.staged, func(i, j int) bool {
		a, b := &c.staged[i], &c.staged[j]
		if order := a.rank.Cmp(b.rank); order != 0 {
			return order < 0
		}
		return bytes.Compare(a.order, b.order) < 0
	})
}

func (c *CausalMessages[T]) poll() (Message[T], PollState, <-chan struct{}, <-chan struct{}) {
	for {
		if message, ok := c.pop(); ok {
			return message, Ready, nil, nil
		}
		c.checkpoint = c.ingested
		pub, state, changed, done := c.source.next()
		if state != Ready {
			return Message[T]{}, state, changed, done
		}
		c.ingest(pub)
	}
}

// TryNext takes one nonblocking causal delivery step.
func (c *CausalMessages[T]) TryNext() (Message[T], PollState) {
	message, state, _, _ := c.poll()
	return message, state
}

// Next waits for a message or final drain. Cancellation preserves staged items.
func (c *CausalMessages[T]) Next(ctx context.Context) (Message[T], bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Message[T]{}, false, err
		}
		message, state, changed, done := c.poll()
		if state != Quiet {
			return message, state == Ready, nil
		}
		if err := waitObservation(ctx, changed, done); err != nil {
			return Message[T]{}, false, err
		}
	}
}
