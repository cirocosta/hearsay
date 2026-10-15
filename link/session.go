package link

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	// ErrSessionActive means another session already owns this link.
	ErrSessionActive = errors.New("link session already active")
	// ErrLinkPoisoned means an interrupted session made the link unusable.
	ErrLinkPoisoned = errors.New("link poisoned by interrupted session")
	// ErrSessionFinished rejects I/O through a completed session handle.
	ErrSessionFinished = errors.New("link session already finished")
	// ErrSessionAborted means a session was closed without clean completion.
	ErrSessionAborted = errors.New("link session aborted")
)

// SessionState describes a managed link. Epoch labels its next session and
// wraps after 255. Poisoned is true during an active session and remains true
// after failure or cancellation. Only clean completion clears the latch.
type SessionState struct {
	Epoch    uint8
	Poisoned bool
	Active   bool
}

// Managed serializes sessions and owns the link's epoch and poison latch.
// Applications must exclusively use this wrapper after handing it a link.
// A failed or canceled session permanently poisons and closes the transport.
// A clean session releases the transport for its next session.
type Managed struct {
	transport Link
	mu        sync.Mutex
	epoch     uint8
	poisoned  bool
	active    *Session
	closeOnce sync.Once
	closeErr  error
}

// Manage takes ownership of a fresh transport. Both endpoints must start with
// fresh wrappers, so their independently advanced epochs agree.
func Manage(transport Link) *Managed {
	if transport == nil {
		panic("nil link transport")
	}
	return &Managed{transport: transport}
}

// State returns the next epoch and current poison latch.
func (m *Managed) State() SessionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return SessionState{Epoch: m.epoch, Poisoned: m.poisoned, Active: m.active != nil}
}

// Begin exclusively starts a session and advances the epoch before any wire
// I/O. Finish must be called only after the complete session boundary. Every
// other exit must call Abort or Close. Canceling ctx closes the link and wakes
// its outstanding I/O. Cancellation before Begin starts leaves the link idle.
func (m *Managed) Begin(ctx context.Context) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil {
		return nil, ErrSessionActive
	}
	if m.poisoned {
		return nil, ErrLinkPoisoned
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &Session{owner: m, epoch: m.epoch, ctx: ctx}
	s.control.session = s
	m.epoch++
	m.poisoned = true
	m.active = s
	s.stop = context.AfterFunc(ctx, func() { _ = s.Abort(ctx.Err()) })
	return s, nil
}

func (m *Managed) closeTransport() error {
	m.closeOnce.Do(func() { m.closeErr = m.transport.Close() })
	return m.closeErr
}

// Close poisons the managed link, stops its session watcher, and closes the
// transport. It is safe to call concurrently and repeatedly.
func (m *Managed) Close() error {
	m.mu.Lock()
	m.poisoned = true
	if s := m.active; s != nil {
		s.done = true
		s.failure = io.ErrClosedPipe
		s.stop()
		m.active = nil
	}
	m.mu.Unlock()
	return m.closeTransport()
}

// Session lends the transport to exactly one session. It implements Link, so
// protocol code can use it wherever a transport is accepted. The handle must
// not be used for another session after Finish. Deferred Close is safe after
// Finish and cannot affect a later session.
type Session struct {
	owner   *Managed
	epoch   uint8
	ctx     context.Context
	stop    func() bool
	done    bool
	failure error
	control sessionControl
}

var _ Link = (*Session)(nil)

// Epoch is the session's immutable stream-label epoch.
func (s *Session) Epoch() uint8 { return s.epoch }

// Err reports the recorded failure, or nil while active or cleanly finished.
func (s *Session) Err() error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	return s.failure
}

func (s *Session) check() error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.done {
		if s.failure != nil {
			return s.failure
		}
		return ErrSessionFinished
	}
	return s.ctx.Err()
}

// Control returns a session-scoped view of the persistent control stream.
func (s *Session) Control() io.ReadWriteCloser { return &s.control }

// Open opens a data stream while the session remains active. A failed open
// must be reported to Abort by the session's protocol driver.
func (s *Session) Open(ctx context.Context) (io.WriteCloser, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.owner.transport.Open(ctx)
}

// Accept accepts a data stream while active. Canceling this individual accept
// does not poison the session; canceling the context passed to Begin does.
func (s *Session) Accept(ctx context.Context) (io.ReadCloser, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.owner.transport.Accept(ctx)
}

// Finish records a complete protocol boundary and releases the transport.
// It does not undo a local commit. Call it before unrelated post-session work,
// such as bootstrap bookmark attachment. A canceled session cannot finish
// cleanly. Repeated Finish calls preserve the original outcome.
func (s *Session) Finish() error {
	m := s.owner
	m.mu.Lock()
	if s.done {
		err := s.failure
		m.mu.Unlock()
		return err
	}
	s.stop()
	if err := s.ctx.Err(); err != nil {
		s.failLocked(err)
		m.mu.Unlock()
		return errors.Join(err, m.closeTransport())
	}
	s.done = true
	m.active = nil
	m.poisoned = false
	m.mu.Unlock()
	return nil
}

func (s *Session) failLocked(cause error) {
	s.done = true
	s.failure = cause
	s.owner.poisoned = true
	s.owner.active = nil
	s.stop()
}

// Abort records failure and closes the transport, waking all outstanding I/O.
// A nil cause becomes ErrSessionAborted. An already completed session keeps its
// original outcome and cannot poison a subsequent session.
func (s *Session) Abort(cause error) error {
	if cause == nil {
		cause = ErrSessionAborted
	}
	m := s.owner
	m.mu.Lock()
	if s.done {
		err := s.failure
		m.mu.Unlock()
		return err
	}
	s.failLocked(cause)
	m.mu.Unlock()
	return errors.Join(cause, m.closeTransport())
}

// Close aborts an unfinished session. After Finish it is a no-op. Its return
// value reports a transport close failure; Err records the session's cause.
func (s *Session) Close() error {
	m := s.owner
	m.mu.Lock()
	if s.done {
		m.mu.Unlock()
		return nil
	}
	s.failLocked(ErrSessionAborted)
	m.mu.Unlock()
	return m.closeTransport()
}

type sessionControl struct{ session *Session }

func (c *sessionControl) Read(b []byte) (int, error) {
	if err := c.session.check(); err != nil {
		return 0, err
	}
	return c.session.owner.transport.Control().Read(b)
}

func (c *sessionControl) Write(b []byte) (int, error) {
	if err := c.session.check(); err != nil {
		return 0, err
	}
	return c.session.owner.transport.Control().Write(b)
}
func (c *sessionControl) Close() error { return c.session.Close() }

// Completer marks the exact clean end of a data stream. Reusing transports may
// implement it to recover connections without relying on EOF or reading past
// the protocol boundary. Close remains the abort operation for those streams.
type Completer interface{ Complete() error }

// Complete returns a stream at its exact clean boundary. Single-use streams
// use Close; a transport with an explicit completion hook receives Complete.
// Callers must use ordinary Close after a failed or canceled exchange.
func Complete(stream io.Closer) error {
	if done, ok := stream.(Completer); ok {
		return done.Complete()
	}
	return stream.Close()
}
