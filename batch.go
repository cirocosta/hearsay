package hearsay

import (
	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/tree"
)

type action struct {
	insert  bool
	payload []byte
	version Version
}

// Batch collects operations without holding a replica lock. A successful Batch
// callback publishes all actions atomically; returning an error or panicking
// publishes none. Do not use a Batch after its callback returns or concurrently.
type Batch[T any] struct {
	codec   Codec[T]
	actions []action
	closed  bool
	limit   uint64
}

// Send admits and queues a payload without publishing it.
func (b *Batch[T]) Send(payload T) error {
	if b.closed {
		return ErrClosed
	}
	p, err := b.codec.encodeBounded(payload, b.limit)
	if err != nil {
		return err
	}
	b.actions = append(b.actions, action{insert: true, payload: p})
	return nil
}

// Redact queues removal of a version.
func (b *Batch[T]) Redact(version Version) error {
	if b.closed {
		return ErrClosed
	}
	b.actions = append(b.actions, action{version: version})
	return nil
}

// SendAll queues payloads until the first admission failure.
func (b *Batch[T]) SendAll(payloads []T) error {
	for _, p := range payloads {
		if err := b.Send(p); err != nil {
			return err
		}
	}
	return nil
}

// RedactAll queues removal of every supplied version.
func (b *Batch[T]) RedactAll(versions []Version) error {
	for _, v := range versions {
		if err := b.Redact(v); err != nil {
			return err
		}
	}
	return nil
}

// Send commits one admitted payload and returns its unique causal version.
func (r *Replica[T]) Send(payload T) (Version, error) {
	if r == nil || r.state == nil {
		return Version{}, ErrClosed
	}
	r.state.mu.Lock()
	alive := r.aliveLocked()
	codec := r.state.codec
	limit := r.state.settings.PayloadDepthLimit
	r.state.mu.Unlock()
	if !alive {
		return Version{}, ErrClosed
	}
	p, err := codec.encodeBounded(payload, limit)
	if err != nil {
		return Version{}, err
	}
	return r.commit([]action{{insert: true, payload: p}})
}

// Redact removes a live message. An absent version makes no change to history.
func (r *Replica[T]) Redact(version Version) error {
	_, err := r.commit([]action{{version: version}})
	return err
}

// SendAll admits and publishes all payloads in one atomic commit.
func (r *Replica[T]) SendAll(payloads []T) error {
	return r.Batch(func(b *Batch[T]) error { return b.SendAll(payloads) })
}

// RedactAll removes live supplied versions in one atomic commit.
func (r *Replica[T]) RedactAll(versions []Version) error {
	return r.Batch(func(b *Batch[T]) error { return b.RedactAll(versions) })
}

// Batch runs prepare outside replica locks, then atomically commits on success.
// Concurrent operations can commit while prepare runs. Action versions follow
// callback order, including ticks for absent redactions; only effective actions
// contribute to the published frontier.
func (r *Replica[T]) Batch(prepare func(*Batch[T]) error) error {
	if r == nil || r.state == nil {
		return ErrClosed
	}
	s := r.state
	s.mu.Lock()
	alive := r.aliveLocked()
	codec := s.codec
	limit := s.settings.PayloadDepthLimit
	s.mu.Unlock()
	if !alive {
		return ErrClosed
	}
	b := &Batch[T]{codec: codec, limit: limit}
	defer func() { b.closed = true }()
	if err := prepare(b); err != nil {
		return err
	}
	b.closed = true
	_, err := r.commit(b.actions)
	return err
}

func (r *Replica[T]) commit(actions []action) (before.Version, error) {
	if r == nil || r.state == nil {
		return Version{}, ErrClosed
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if !r.aliveLocked() {
		return Version{}, ErrClosed
	}
	candidate := applyBatch(s.current.Load().tree, s.party, actions)
	s.publishLocked(candidate)
	return candidate.Latest(), nil
}

func applyBatch(
	original tree.Tree[[]byte],
	party *before.Party,
	actions []action,
) tree.Tree[[]byte] {
	if len(actions) == 0 {
		return original
	}
	if len(actions) == 1 {
		stamp := original.Latest().Tick(party)
		if actions[0].insert {
			return original.Insert(stamp, actions[0].payload)
		}
		return original.Forget(actions[0].version, stamp)
	}
	return applyEffects(original, batchEffects(original, party, actions))
}

func applyEffects(original tree.Tree[[]byte], effects []batchEffect) tree.Tree[[]byte] {
	active := effectiveKeys(effects)
	if active <= 1 || original.Len() == 0 && active < 4 {
		return applyEffectsSequential(original, effects)
	}
	edits := make([]tree.Edit[[]byte], 0, active)
	for _, effect := range effects {
		if effect.insert || effect.existed {
			edits = append(
				edits,
				tree.Edit[[]byte]{
					Version: effect.version,
					Stamp:   effect.stamp,
					Value:   effect.payload,
					Insert:  effect.insert,
				},
			)
		}
	}
	return original.Apply(edits)
}

func effectiveKeys(effects []batchEffect) int {
	active := 0
	for _, effect := range effects {
		if effect.insert || effect.existed {
			active++
		}
	}
	return active
}

func applyEffectsSequential(original tree.Tree[[]byte], effects []batchEffect) tree.Tree[[]byte] {
	for _, effect := range effects {
		if effect.insert {
			original = original.Insert(effect.version, effect.payload)
		} else if effect.existed {
			original = original.Forget(effect.version, effect.stamp)
		}
	}
	return original
}

type batchEffect struct {
	version Version
	stamp   Version
	payload []byte
	insert  bool
	existed bool
}

func batchEffects(original tree.Tree[[]byte], party *before.Party, actions []action) []batchEffect {
	stamp := original.Latest()
	positions := make(map[tree.Path]int, len(actions))
	effects := make([]batchEffect, 0, len(actions))
	for _, a := range actions {
		stamp = stamp.Tick(party)
		version := a.version
		if a.insert {
			version = stamp
		}
		path := tree.Address(version)
		index, exists := positions[path]
		if !exists {
			_, existed := original.Get(version)
			index = len(effects)
			positions[path] = index
			effects = append(effects, batchEffect{existed: existed})
		}
		effect := &effects[index]
		effect.version = version
		effect.stamp = stamp
		effect.payload = a.payload
		effect.insert = a.insert
	}
	return effects
}
