// Package sessionerr carries session diagnostics across implementation layers.
package sessionerr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/cirocosta/hearsay/observe"
)

// Phase identifies where a session detected failure.
type Phase string

// Session phases include control exchange, reconciliation, and persistence.
const (
	Preamble         Phase = "preamble"
	Greeting         Phase = "greeting"
	Reconciliation   Phase = "reconciliation"
	IdentityTransfer Phase = "identity transfer"
	Completion       Phase = "completion"
	Idle             Phase = "idle"
)

// Operation identifies the transport action that failed.
type Operation string

// Transport operations retain the action observed at the I/O boundary.
const (
	Unknown Operation = ""
	Read    Operation = "read"
	Write   Operation = "write"
	Flush   Operation = "flush"
	Open    Operation = "open"
	Accept  Operation = "accept"
)

// DataStream locates a direction and, when known, its logical stream index.
type DataStream struct {
	Sender observe.Role
	Index  *uint8
}

// Context locates detection without assigning blame to a participant.
// DataStream is nil for control traffic and failures without a known direction.
type Context struct {
	Phase      Phase
	DataStream *DataStream
}

func (c Context) String() string {
	if c.DataStream == nil {
		return string(c.Phase)
	}
	sender := "initiator"
	if c.DataStream.Sender == observe.Responder {
		sender = "responder"
	}
	if c.DataStream.Index == nil {
		return fmt.Sprintf("%s, %s stream", c.Phase, sender)
	}
	return fmt.Sprintf("%s, %s stream %d", c.Phase, sender, *c.DataStream.Index)
}

// TransportError reports a failed transport action and preserves its cause.
type TransportError struct {
	Context   Context
	Operation Operation
	Err       error
}

func (e *TransportError) Error() string {
	operation := string(e.Operation)
	if operation == "" {
		operation = "transport"
	}
	return fmt.Sprintf("%s failed during %s: %v", operation, e.Context, e.Err)
}
func (e *TransportError) Unwrap() error { return e.Err }

// ProtocolViolation reports invalid wire traffic or an implementation invariant.
// Its diagnostic cause is retained for logging rather than grammar matching.
type ProtocolViolation struct {
	Context Context
	Err     error
}

func (e *ProtocolViolation) Error() string {
	return fmt.Sprintf("protocol violation during %s: %v", e.Context, e.Err)
}
func (e *ProtocolViolation) Unwrap() error { return e.Err }

// IOFailure records an operation before its session location is known.
type IOFailure struct {
	Operation Operation
	Err       error
}

// ProtocolFailure marks validation after bytes have already been received.
// In particular, a decoder's EOF or net.Error is not a transport departure.
type ProtocolFailure struct{ Err error }

func (e *ProtocolFailure) Error() string { return e.Err.Error() }
func (e *ProtocolFailure) Unwrap() error { return e.Err }

// Protocol marks a validation failure without assigning its eventual stream.
func Protocol(err error) error {
	if err == nil {
		return nil
	}
	return &ProtocolFailure{Err: err}
}

func (e *IOFailure) Error() string { return fmt.Sprintf("%s: %v", e.Operation, e.Err) }
func (e *IOFailure) Unwrap() error { return e.Err }

// IO marks a transport failure without changing io.Reader's EOF conventions.
func IO(operation Operation, err error) error {
	if err == nil || (operation == Read && err == io.EOF) {
		return err
	}
	return &IOFailure{Operation: operation, Err: err}
}

// At adds a session location while retaining existing directed-stream context.
func At(location Context, err error) error {
	if err == nil {
		return nil
	}
	switch diagnostic := Diagnostic(err).(type) {
	case *ProtocolFailure:
		return &ProtocolViolation{Context: location, Err: err}
	case *IOFailure:
		return &TransportError{Context: location, Operation: diagnostic.Operation, Err: err}
	case *TransportError, *ProtocolViolation:
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if isTransport(err) {
		return &TransportError{Context: location, Operation: Read, Err: err}
	}
	return &ProtocolViolation{Context: location, Err: err}
}

// Diagnostic finds the first classified boundary in unwrap order. A cause's
// own diagnostic cannot override the boundary that received or validated it.
func Diagnostic(err error) error {
	for err != nil {
		switch err.(type) {
		case *ProtocolFailure, *IOFailure, *ProtocolViolation, *TransportError:
			return err
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			return joinedDiagnostic(joined.Unwrap())
		}
		err = errors.Unwrap(err)
	}
	return nil
}

func joinedDiagnostic(causes []error) error {
	for _, cause := range causes {
		if diagnostic := Diagnostic(cause); diagnostic != nil {
			return diagnostic
		}
	}
	return nil
}

func isTransport(err error) bool {
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe)
}

// Stream attributes a reconciliation failure to the known sending direction.
// A negative index means no valid logical index has been decoded.
func Stream(sender observe.Role, index int, err error) error {
	if err == nil {
		return nil
	}
	stream := &DataStream{Sender: sender}
	if index >= 0 {
		value := uint8(index)
		stream.Index = &value
	}
	return At(Context{Phase: Reconciliation, DataStream: stream}, err)
}
