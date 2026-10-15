package hearsay

import (
	"context"
	"errors"
	"time"

	"github.com/cirocosta/hearsay/internal/protocol"
)

func copyDeadline(deadline *time.Duration) *time.Duration {
	if deadline == nil {
		return nil
	}
	duration := *deadline
	return &duration
}

// WithSessionDeadline configures the maximum active session duration. Nil
// disables the deadline. Zero expires after the session begins. GossipOnce,
// Retire, and continuous drivers inherit it; idle driver waits are excluded.
func (p *Peer[T]) WithSessionDeadline(deadline *time.Duration) (*Peer[T], error) {
	if p == nil || p.state == nil {
		return nil, ErrClosed
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.token == nil || !p.token.alive {
		return nil, ErrClosed
	}
	s.sessionDeadline = copyDeadline(deadline)
	return p, nil
}

// WithSessionDeadline returns a configuration whose deadline applies during
// joining and to the joined peer's later sessions. Nil disables it; zero starts
// an immediate deadline after the session begins. Bookmark attachment is excluded.
func (b *BootstrapConfig[T]) WithSessionDeadline(deadline *time.Duration) *BootstrapConfig[T] {
	configured := *b
	configured.sessionDeadline = copyDeadline(deadline)
	return &configured
}

// SessionDeadline returns a copy of the configured active session duration.
func (p *Peer[T]) SessionDeadline() *time.Duration { return p.state.configuredDeadline() }

// SessionDeadline returns a copy of this replica's active session duration.
func (r *Rumors[T]) SessionDeadline() *time.Duration { return r.state.configuredDeadline() }

// SessionDeadline returns a copy of this bootstrap's active session duration.
func (b *BootstrapConfig[T]) SessionDeadline() *time.Duration { return copyDeadline(b.sessionDeadline) }

func (s *replica[T]) configuredDeadline() *time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyDeadline(s.sessionDeadline)
}

type deadlineOutcome[R any] struct {
	value R
	err   error
}

func runSessionDeadline[R any](
	ctx context.Context,
	deadline *time.Duration,
	cancel context.CancelCauseFunc,
	operation func() (R, error),
) (R, error) {
	if deadline == nil {
		value, err := operation()
		return activeDeadlineResult(ctx, value, err)
	}
	timer := time.NewTimer(*deadline)
	defer timer.Stop()
	completed := make(chan deadlineOutcome[R], 1)
	go func() { value, err := operation(); completed <- deadlineOutcome[R]{value, err} }()
	select {
	case result := <-completed:
		return activeDeadlineResult(ctx, result.value, result.err)
	case <-timer.C:
	}
	// a ready completion takes precedence over an equally ready deadline.
	select {
	case result := <-completed:
		return activeDeadlineResult(ctx, result.value, result.err)
	default:
	}
	cancel(context.DeadlineExceeded)
	result := <-completed
	if result.err == nil {
		return result.value, nil
	}
	return result.value, phaseError("deadline", errors.Join(context.DeadlineExceeded, result.err))
}

type establishedResult struct {
	gossiped Gossiped
	donated  bool
}

func (s *replica[T]) runConfiguredSession(
	ctx context.Context,
	wire *sessionWire,
	intent protocol.Intent,
	cancel context.CancelCauseFunc,
) (establishedResult, error) {
	return runSessionDeadline(
		ctx,
		s.configuredDeadline(),
		cancel,
		func() (establishedResult, error) {
			result, donated, err := s.session(ctx, wire, intent)
			return establishedResult{result, donated}, err
		},
	)
}

func activeDeadlineResult[R any](ctx context.Context, value R, err error) (R, error) {
	cause := context.Cause(ctx)
	if err != nil && cause != nil && !errors.Is(err, cause) {
		err = errors.Join(cause, err)
	}
	return value, err
}
