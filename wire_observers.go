package hearsay

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/sessionerr"
	"github.com/cirocosta/hearsay/observe"
)

// AttachObserver appends a wire observer while the peer is exclusively owned.
// Callbacks run synchronously and must follow observe's concurrency contract.
func (p *Peer[T]) AttachObserver(observer observe.Observer) error {
	if p == nil || p.state == nil {
		return ErrClosed
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.token == nil || !p.token.alive {
		return ErrClosed
	}
	s.observers = s.observers.With(observer)
	return nil
}

// WithObserver returns a bootstrap configuration with one more wire observer.
// The joined peer inherits the observer, including after bookmark attachment.
func (b *BootstrapConfig[T]) WithObserver(observer observe.Observer) *BootstrapConfig[T] {
	configured := *b
	configured.observers = b.observers.With(observer)
	return &configured
}

func readObservedItem[T any](carrier *sessionWire, read func(io.Reader) (T, error)) (T, error) {
	if !carrier.observation.ControlAttached(observe.Received) {
		return read(carrier.Control())
	}
	capture := observe.NewCaptureReader(carrier.Control())
	value, err := read(capture)
	if err == nil {
		carrier.observation.ControlReceived(capture.Bytes())
	}
	return value, err
}

func readObservedParty(carrier *sessionWire) (*before.Party, error) {
	return readObservedItem(carrier, readParty)
}

func finishWireObservation(ctx context.Context, carrier *sessionWire, result Gossiped, err error) {
	outcome := observe.Outcome{
		Kind:      observe.Completed,
		Converged: result.Converged,
		Stats:     observe.Stats(result.Stats),
	}
	if err != nil {
		outcome.Kind = observe.Failed
		outcome.Failure = wireFailure(err)
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			outcome.Failure = observe.DeadlineExceeded
		}
		if errors.Is(context.Cause(ctx), context.Canceled) {
			outcome.Kind = observe.Cancelled
		}
	}
	carrier.observation.Finish(outcome)
}

func wireFailure(err error) observe.FailureKind {
	var phase *SessionError
	if errors.As(err, &phase) && (phase.Phase == "bookmark" || phase.Phase == "checkpoint") {
		return observe.BookmarkFailure
	}
	switch sessionerr.Diagnostic(err).(type) {
	case *ProtocolViolation, *sessionerr.ProtocolFailure:
		return observe.ProtocolFailure
	case *TransportError, *sessionerr.IOFailure:
		return observe.TransportFailure
	}
	if errors.Is(err, protocol.ErrNetwork) || errors.Is(err, protocol.ErrDepth) ||
		errors.Is(err, protocol.ErrProtocol) {
		return observe.MismatchFailure
	}
	var transport net.Error
	if errors.As(err, &transport) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) {
		return observe.TransportFailure
	}
	return observe.ProtocolFailure
}
