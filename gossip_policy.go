package hearsay

import "context"

// GossipWhen creates one connection's request stream from a fresh change
// subscription. The factory must be safe for concurrent calls and return
// promptly. Its goroutines must stop when ctx is canceled. A nil returned
// channel disables local initiation; closing it stops that connection driver.
type GossipWhen[T any] func(ctx context.Context, changes *Changes[T]) <-chan GossipRequest

// WithGossipWhen configures the default initiation factory for this replica's
// connection drivers. Nil restores immediate synchronization followed by local
// or learned changes. Explicit GossipOnce and Retire calls ignore this policy.
func (p *Peer[T]) WithGossipWhen(when GossipWhen[T]) (*Peer[T], error) {
	if p == nil || p.state == nil {
		return nil, ErrClosed
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.token == nil || !p.token.alive {
		return nil, ErrClosed
	}
	s.gossipWhen = when
	return p, nil
}

// WithGossipWhen returns a bootstrap configuration whose joined peer inherits
// the factory. Joining itself ignores the policy. Nil restores the default.
func (b *BootstrapConfig[T]) WithGossipWhen(when GossipWhen[T]) *BootstrapConfig[T] {
	configured := *b
	configured.gossipWhen = when
	return &configured
}

func (s *replica[T]) configuredGossipWhen() GossipWhen[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gossipWhen
}
