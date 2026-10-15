package hearsay

import (
	"context"
	"errors"
	"io"
	"iter"
	"time"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/link"
)

// GossipRequest selects whether a policy request requires local news.
type GossipRequest uint8

// Gossip policies can request conditional synchronization or a heartbeat.
const (
	GossipWhenChanged GossipRequest = iota
	GossipUnconditionally
)

// GossipOptions configures one continuous connection driver.
type GossipOptions[T any] struct {
	// When overrides this connection's initiation factory. Nil inherits the
	// peer's factory, which normally synchronizes immediately, then after local
	// or learned frontier changes.
	// The factory must return promptly; its goroutines must honor ctx. A nil
	// returned channel disables local initiation. Closing the channel ends the
	// driver after its active exchange. Remote initiation remains independent.
	When func(ctx context.Context, changes *Changes[T]) <-chan GossipRequest

	// SessionDeadline bounds active exchanges through completion, excluding idle
	// waits. Nil inherits the peer's default, which normally disables deadlines.
	// A nonnil zero or negative duration expires an active exchange immediately.
	// The duration is copied when iteration starts.
	SessionDeadline *time.Duration
}

// Gossip drives consecutive sessions and yields completed outcomes or one final
// error. Breaking iteration after a success leaves the link idle and reusable.
// Cancellation during an active exchange poisons the link; cancellation during
// an untouched idle wait leaves it reusable. The caller must exclusively own
// carrier until iteration ends, including while the driver waits for a trigger.
//
// The driver retains one writable handle until it ends. The source handle may
// be closed after iteration begins. Sessions suppress their own merge signal;
// concurrent local updates beyond Converged still trigger another exchange.
func (r *Rumors[T]) Gossip(
	ctx context.Context,
	carrier *link.Managed,
	options GossipOptions[T],
) iter.Seq2[Gossiped, error] {
	return func(yield func(Gossiped, error) bool) {
		hold, err := r.Clone()
		if err != nil {
			yield(Gossiped{}, err)
			return
		}
		defer func() { _ = hold.Close() }()
		policyCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		driver := continuousDriver[T]{
			peer:     hold,
			carrier:  carrier,
			changes:  hold.Changes(),
			deadline: hold.state.configuredDeadline(),
		}
		if options.SessionDeadline != nil {
			duration := *options.SessionDeadline
			driver.deadline = &duration
		}
		when := hold.state.configuredGossipWhen()
		if options.When != nil {
			when = options.When
		}
		if when != nil {
			driver.custom = true
			driver.requests = when(policyCtx, driver.changes)
		}
		driver.drive(ctx, yield)
	}
}

type continuousDriver[T any] struct {
	peer      *Rumors[T]
	carrier   *link.Managed
	changes   *Changes[T]
	requests  <-chan GossipRequest
	custom    bool
	converged *Version
	deadline  *time.Duration
}

func (d *continuousDriver[T]) drive(ctx context.Context, yield func(Gossiped, error) bool) {
	for {
		wake := d.wait(ctx)
		if wake.err != nil {
			yield(Gossiped{}, wake.err)
			return
		}
		if wake.end {
			return
		}
		result, err := d.exchange(ctx, wake)
		if err != nil {
			yield(Gossiped{}, err)
			return
		}
		d.converged = &result.Converged
		if !yield(result, nil) {
			return
		}
	}
}

type controlStart struct {
	err error
}

type gossipWake struct {
	led Led
	end bool
	err error
}

func (d *continuousDriver[T]) news() bool {
	return d.converged == nil || !d.peer.Snapshot().Latest().Equal(*d.converged)
}

func (d *continuousDriver[T]) wait(ctx context.Context) gossipWake {
	if d.carrier.State().Poisoned {
		return gossipWake{err: link.ErrLinkPoisoned}
	}
	return d.waitInput(ctx)
}

func (d *continuousDriver[T]) waitInput(ctx context.Context) gossipWake {
	idleCtx, cancel := context.WithCancel(ctx)
	arrivals := make(chan controlStart, 1)
	go func() {
		_, err := d.carrier.PeekControlContext(idleCtx)
		arrivals <- controlStart{err: err}
	}()
	defer cancel()
	stop := func() controlStart {
		cancel()
		return <-arrivals
	}
	for {
		changed, local := d.defaultRequest()
		if local {
			return d.localWake(stop())
		}
		select {
		case <-ctx.Done():
			stop()
			return gossipWake{err: ctx.Err()}
		case arrival := <-arrivals:
			return d.remoteWake(arrival)
		case request, ok := <-d.requests:
			if wake, ready := d.policyWake(request, ok, stop); ready {
				return wake
			}
		case <-changed:
		}
	}
}

func (d *continuousDriver[T]) defaultRequest() (<-chan struct{}, bool) {
	if d.custom {
		return nil, false
	}
	for {
		state, changed, _ := d.changes.poll()
		if state != Ready {
			return changed, false
		}
		if d.news() {
			return nil, true
		}
	}
}

func (d *continuousDriver[T]) policyWake(
	request GossipRequest,
	open bool,
	stop func() controlStart,
) (gossipWake, bool) {
	if !open {
		stop()
		return gossipWake{end: true}, true
	}
	if request != GossipWhenChanged && request != GossipUnconditionally {
		stop()
		return gossipWake{err: errors.New("hearsay: invalid gossip policy request")}, true
	}
	if request == GossipWhenChanged && !d.news() {
		return gossipWake{}, false
	}
	return d.localWake(stop()), true
}

func (d *continuousDriver[T]) remoteWake(start controlStart) gossipWake {
	if start.err == nil {
		return gossipWake{led: LedRemote}
	}
	if errors.Is(start.err, io.EOF) {
		return gossipWake{end: true}
	}
	if errors.Is(start.err, context.Canceled) || errors.Is(start.err, context.DeadlineExceeded) {
		return gossipWake{err: start.err}
	}
	_ = d.carrier.Close()
	return gossipWake{err: phaseError("idle", start.err)}
}

func (d *continuousDriver[T]) localWake(start controlStart) gossipWake {
	if start.err != nil && !errors.Is(start.err, context.Canceled) {
		return d.remoteWake(start)
	}
	return gossipWake{led: LedLocal}
}

func (d *continuousDriver[T]) exchange(ctx context.Context, wake gossipWake) (Gossiped, error) {
	activeCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	wire, err := beginSession(activeCtx, d.carrier)
	if err != nil {
		return Gossiped{}, err
	}
	defer func() { _ = wire.Close() }()
	result, err := d.runActive(activeCtx, wire, cancel)
	if err != nil && ctx.Err() != nil && !errors.Is(err, ctx.Err()) {
		err = errors.Join(ctx.Err(), err)
	}
	result.Led = wake.led
	return result, err
}

func (d *continuousDriver[T]) runActive(
	ctx context.Context,
	wire *sessionWire,
	cancel context.CancelCauseFunc,
) (Gossiped, error) {
	return runSessionDeadline(ctx, d.deadline, cancel, func() (Gossiped, error) {
		result, _, err := d.peer.state.session(ctx, wire, protocol.Remain)
		return result, err
	})
}
