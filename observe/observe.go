// Package observe watches complete CBOR wire items exchanged by live sessions.
// These hooks watch the wire; hearsay's content observers watch replicated sets.
//
// Callbacks run synchronously in protocol tasks and must return promptly. They
// must not wait for protocol progress or invoke session lifecycle methods.
// Session and stream factories can run concurrently. Each directed stream's
// messages retain wire order; distinct streams have no common ordering.
//
// Message bytes are borrowed, read-only, and valid only during the callback.
// Copy bytes before retaining them or handing them to another goroutine. Hooks
// see only complete, accepted reads and successfully written and flushed items.
// Transport stream labels and incomplete or rejected items are excluded.
package observe

import (
	"time"

	"github.com/cirocosta/hearsay/before"
)

// Observer creates one optional handler for each session entered by a peer.
// Implementations must support concurrent calls. Return nil to decline a session.
type Observer interface {
	Session(SessionInfo) SessionObserver
}

// SessionObserver creates optional handlers as directed streams begin speaking.
// Implementations must support concurrent calls. It may also implement
// ElectionObserver and FinishObserver to receive lifecycle callbacks.
type SessionObserver interface {
	Stream(StreamInfo) StreamObserver
}

// ElectionObserver optionally receives the role after election, before data
// streams open. Equal-frontier sessions perform no election.
type ElectionObserver interface {
	Elected(Role)
}

// FinishObserver optionally receives exactly one terminal session callback,
// after all stream tasks and preceding callbacks have finished.
type FinishObserver interface {
	Finished(Outcome)
}

// StreamObserver receives one complete item at a time in directed-stream order.
// Message borrows its read-only bytes for the duration of the synchronous call.
type StreamObserver interface {
	Message([]byte)
}

// ObserverFunc adapts a session factory to Observer.
type ObserverFunc func(SessionInfo) SessionObserver

// Session invokes the function.
func (f ObserverFunc) Session(info SessionInfo) SessionObserver { return f(info) }

// SessionKind identifies the operation entered on this side.
type SessionKind uint8

// Lifecycle operations can differ between the two ends of one session.
const (
	Bootstrap SessionKind = iota
	Gossip
	Retire
)

// Protocol identifies a wire dialect.
type Protocol uint8

// V2 is the supported wire dialect.
const V2 Protocol = 2

// SessionInfo identifies an observed session without prescribing numbering.
type SessionInfo struct {
	Kind     SessionKind
	Protocol Protocol
}

// Role identifies one side of the reconciliation descent.
type Role uint8

// Elected roles are independent of which peer triggered a session.
const (
	Initiator Role = iota
	Responder
)

// Direction identifies item flow from this peer's perspective.
type Direction uint8

// Sent and Received name the two independent directions of a stream.
const (
	Sent Direction = iota
	Received
)

// StreamKind distinguishes setup and lifecycle traffic from reconciliation.
type StreamKind uint8

// Control and Data identify the two classes of session streams.
const (
	Control StreamKind = iota
	Data
)

// StreamID identifies a logical stream. Speaker and Index apply only to Data;
// Index is the wire stream index, excluding its transport label bytes.
type StreamID struct {
	Kind    StreamKind
	Speaker Role
	Index   uint8
}

// StreamInfo identifies one directed stream within a session.
type StreamInfo struct {
	ID        StreamID
	Direction Direction
}

// Stats summarizes measured session traffic and reconciliation work.
type Stats struct {
	// Elapsed includes setup, reconciliation, identity transfer, and confirmed
	// completion, excluding an idle continuous-gossip policy wait.
	Elapsed                                    time.Duration
	BytesSent, BytesReceived                   uint64
	FramesSent, FramesReceived                 uint64
	MessagesSent, MessagesGained, MessagesShed uint64
	DisputedScopes, StreamsOpened              uint64
	// WindowGranted reports configured capacity, not observed occupancy.
	WindowGranted uint64
	// WindowStalls is scheduling-dependent full-window pressure on arrival.
	WindowStalls uint64
}

// OutcomeKind identifies a terminal session result.
type OutcomeKind uint8

// Completed requires confirmed peer completion. Cancelled means the driving
// context was cancelled before return; Failed carries a diagnostic category.
const (
	Completed OutcomeKind = iota
	Failed
	Cancelled
)

// FailureKind is a coarse diagnostic category; driver errors retain details.
type FailureKind uint8

// Failure categories do not depend on application storage or transport types.
const (
	TransportFailure FailureKind = iota
	ProtocolFailure
	MismatchFailure
	DeadlineExceeded
	BookmarkFailure
)

// Outcome is a terminal diagnostic summary. Completed uses Converged and Stats;
// Failed uses Failure. Converged excludes concurrent local changes.
type Outcome struct {
	Kind      OutcomeKind
	Converged before.Version
	Stats     Stats
	Failure   FailureKind
}
