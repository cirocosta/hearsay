package hearsay

import "github.com/cirocosta/hearsay/before"

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
func (r *Rumors[T]) Send(payload T) (Version, error) {
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
func (r *Rumors[T]) Redact(version Version) error {
	_, err := r.commit([]action{{version: version}})
	return err
}

// SendAll admits and publishes all payloads in one atomic commit.
func (r *Rumors[T]) SendAll(payloads []T) error {
	return r.Batch(func(b *Batch[T]) error { return b.SendAll(payloads) })
}

// RedactAll removes live supplied versions in one atomic commit.
func (r *Rumors[T]) RedactAll(versions []Version) error {
	return r.Batch(func(b *Batch[T]) error { return b.RedactAll(versions) })
}

// Batch runs prepare outside replica locks, then atomically commits on success.
// Concurrent operations can commit while prepare runs. Action versions follow
// callback order, including ticks for absent redactions; only effective actions
// contribute to the published frontier.
func (r *Rumors[T]) Batch(prepare func(*Batch[T]) error) error {
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

func (r *Rumors[T]) commit(actions []action) (before.Version, error) {
	if r == nil || r.state == nil {
		return Version{}, ErrClosed
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if !r.aliveLocked() {
		return Version{}, ErrClosed
	}
	candidate := s.current.Load().tree
	stamp := candidate.Latest()
	for _, a := range actions {
		stamp = stamp.Tick(s.party)
		if a.insert {
			candidate = candidate.Insert(stamp, a.payload)
		} else {
			candidate = candidate.Forget(a.version, stamp)
		}
	}
	s.publishLocked(candidate)
	return candidate.Latest(), nil
}
