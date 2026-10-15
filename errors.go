package hearsay

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/sessionerr"
	"github.com/cirocosta/hearsay/observe"
)

// SessionPhase identifies where a wire session detected failure.
type SessionPhase = sessionerr.Phase

// Session phases describe detection. Completion can fail after local commit.
const (
	PhasePreamble         = sessionerr.Preamble
	PhaseGreeting         = sessionerr.Greeting
	PhaseReconciliation   = sessionerr.Reconciliation
	PhaseIdentityTransfer = sessionerr.IdentityTransfer
	PhaseCompletion       = sessionerr.Completion
	PhaseIdle             = sessionerr.Idle
)

// TransportOperation identifies a failed transport action.
type TransportOperation = sessionerr.Operation

// Transport operations remain available without parsing an error message.
// TransportUnknown means a backend completion failed without identifying the
// underlying action; it does not infer a read or write from the cause's type.
const (
	TransportUnknown = sessionerr.Unknown
	TransportRead    = sessionerr.Read
	TransportWrite   = sessionerr.Write
	TransportFlush   = sessionerr.Flush
	TransportOpen    = sessionerr.Open
	TransportAccept  = sessionerr.Accept
)

// DataStream locates the elected sender and optional logical stream index.
// Sender uses observe.Initiator or observe.Responder, independent of dialer.
type DataStream = sessionerr.DataStream

// ErrorContext locates failure detection without assigning responsibility.
// DataStream is nil for control traffic and unknown data directions.
type ErrorContext = sessionerr.Context

// TransportError reports a transport operation with its phase and known stream.
// Err preserves the original cause for errors.Is and errors.As.
type TransportError = sessionerr.TransportError

// ProtocolViolation reports invalid traffic or an implementation invariant.
// Its Err is diagnostic; individual wire grammar checks are not a public API.
type ProtocolViolation = sessionerr.ProtocolViolation

// Protocol identifies a wire dialect.
type Protocol = observe.Protocol

// V2 is this build's supported wire dialect.
const V2 = observe.V2

// Mismatch sentinels identify incompatibilities without importing internals.
var (
	ErrProtocolMismatch     = protocol.ErrProtocol
	ErrNetworkMismatch      = protocol.ErrNetwork
	ErrPayloadDepthMismatch = protocol.ErrDepth
)

// Mismatch identifies incompatible peers or settings. Its concrete type
// carries the operands needed to resolve the incompatibility before retrying.
type Mismatch interface {
	error
	mismatch()
}

// ProtocolMismatch reports the local dialect and remote advertised version.
type ProtocolMismatch struct {
	LocalProtocol Protocol
	RemoteVersion uint64
	Err           error
}

func (e *ProtocolMismatch) Error() string {
	return fmt.Sprintf("protocol mismatch: local %d, remote %d", e.LocalProtocol, e.RemoteVersion)
}
func (e *ProtocolMismatch) Unwrap() error { return e.Err }
func (*ProtocolMismatch) mismatch()       {}

// NetworkMismatch reports both universe IDs and exact lower bounds on the
// numbers of events in their opening histories. The bounds are owned snapshots.
type NetworkMismatch struct {
	LocalNetwork    Network
	LocalMinEvents  *big.Int
	RemoteNetwork   Network
	RemoteMinEvents *big.Int
	Err             error
}

func (e *NetworkMismatch) Error() string {
	return fmt.Sprintf(
		"network mismatch: local %s (at least %s events), remote %s (at least %s events)",
		e.LocalNetwork,
		e.LocalMinEvents,
		e.RemoteNetwork,
		e.RemoteMinEvents,
	)
}
func (e *NetworkMismatch) Unwrap() error { return e.Err }
func (*NetworkMismatch) mismatch()       {}

// PayloadDepthMismatch reports the configured traversal limits of both peers.
type PayloadDepthMismatch struct {
	Local, Remote uint64
	Err           error
}

func (e *PayloadDepthMismatch) Error() string {
	return fmt.Sprintf("payload depth mismatch: local %d, remote %d", e.Local, e.Remote)
}
func (e *PayloadDepthMismatch) Unwrap() error { return e.Err }
func (*PayloadDepthMismatch) mismatch()       {}

// SessionError identifies the phase that failed and wraps its typed cause.
// Completion failure does not roll back an existing peer's local commit.
// For wire failures Err holds the primary diagnostic category. Its cause may
// contain a callback's own diagnostic; Context preserves the outer attribution.
type SessionError struct {
	Phase string
	Err   error
}

func (e *SessionError) Error() string { return fmt.Sprintf("hearsay %s: %v", e.Phase, e.Err) }
func (e *SessionError) Unwrap() error { return e.Err }

// Context returns the typed detection context, including a known data stream.
func (e *SessionError) Context() ErrorContext {
	switch diagnostic := sessionerr.Diagnostic(e.Err).(type) {
	case *TransportError:
		return diagnostic.Context
	case *ProtocolViolation:
		return diagnostic.Context
	}
	return ErrorContext{Phase: SessionPhase(e.Phase)}
}

func phaseError(phase string, err error) error {
	if err == nil {
		return nil
	}
	return &SessionError{Phase: phase, Err: sessionCause(phase, err)}
}

func sessionCause(phase string, err error) error {
	var invalid *sessionerr.ProtocolFailure
	if errors.As(err, &invalid) {
		return sessionerr.At(ErrorContext{Phase: SessionPhase(phase)}, err)
	}
	var mismatch Mismatch
	if errors.As(err, &mismatch) {
		return err
	}
	var version *protocol.VersionMismatch
	if errors.As(err, &version) {
		return &ProtocolMismatch{
			LocalProtocol: Protocol(version.Local),
			RemoteVersion: version.Remote,
			Err:           err,
		}
	}
	var depth *protocol.DepthMismatch
	if errors.As(err, &depth) {
		return &PayloadDepthMismatch{Local: depth.Local, Remote: depth.Remote, Err: err}
	}
	if phase == "bookmark" || phase == "checkpoint" {
		return err
	}
	return sessionerr.At(ErrorContext{Phase: SessionPhase(phase)}, err)
}
