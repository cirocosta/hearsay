package observe

import (
	"sync"
	"sync/atomic"
)

// Attachment is an immutable collection of observers in attachment order.
// Copying it shares observers. With creates a separate collection.
type Attachment struct{ observers []Observer }

// With appends an observer without changing an existing attachment snapshot.
// A nil observer is ignored.
func (a Attachment) With(observer Observer) Attachment {
	if observer == nil {
		return a
	}
	observers := make([]Observer, len(a.observers)+1)
	copy(observers, a.observers)
	observers[len(a.observers)] = observer
	return Attachment{observers: observers}
}

// Len returns the number of attached peer observers.
func (a Attachment) Len() int { return len(a.observers) }

// Begin starts observation before any wire traffic and opens both control
// directions. It returns nil without allocating when no observer accepts.
func (a Attachment) Begin(kind SessionKind) *Session {
	info := SessionInfo{Kind: kind, Protocol: V2}
	var handlers []SessionObserver
	for _, observer := range a.observers {
		if handler := observer.Session(info); handler != nil {
			handlers = append(handlers, handler)
		}
	}
	if len(handlers) == 0 {
		return nil
	}
	session := &Session{handlers: handlers}
	session.sent = session.newStream(StreamInfo{ID: StreamID{Kind: Control}, Direction: Sent})
	session.received = session.newStream(
		StreamInfo{ID: StreamID{Kind: Control}, Direction: Received},
	)
	return session
}

// Session routes protocol callbacks to accepted observers. Its nil value is
// disabled; nil-aware methods avoid allocations and frame copies. The driver
// must stop and join all protocol tasks before calling Finish, typically with
// a deferred Cancelled fallback followed by an explicit terminal outcome.
type Session struct {
	mu             sync.RWMutex
	closed         bool
	elected        atomic.Bool
	finished       sync.Once
	handlers       []SessionObserver
	sent, received *streamSink
}

// Attached reports whether this session has any accepted observation handlers.
func (s *Session) Attached() bool { return s != nil }

// ControlAttached reports whether a control direction accepts item callbacks.
// Readers can use it to avoid capture allocation when that stream is declined.
func (s *Session) ControlAttached(direction Direction) bool {
	if s == nil {
		return false
	}
	if direction == Sent {
		return s.sent != nil
	}
	return s.received != nil
}

// ControlSent reports an item after its complete write and successful flush.
func (s *Session) ControlSent(item []byte) {
	if s != nil {
		s.sent.Message(item)
	}
}

// ControlReceived reports an exact, completely read and accepted control item.
func (s *Session) ControlReceived(item []byte) {
	if s != nil {
		s.received.Message(item)
	}
}

// Elected reports at most one decided role to interested session handlers.
func (s *Session) Elected(role Role) {
	if s == nil || !s.elected.CompareAndSwap(false, true) {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	for _, handler := range s.handlers {
		if elected, ok := handler.(ElectionObserver); ok {
			elected.Elected(role)
		}
	}
}

// Data opens one directed data stream. Call it only when that direction first
// speaks, excluding transport labels. The returned nil handler declines frames.
func (s *Session) Data(speaker Role, index uint8, direction Direction) StreamObserver {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}
	sink := s.newStream(
		StreamInfo{ID: StreamID{Kind: Data, Speaker: speaker, Index: index}, Direction: direction},
	)
	if sink == nil {
		return nil
	}
	return sink
}

func (s *Session) newStream(info StreamInfo) *streamSink {
	var handlers []StreamObserver
	for _, session := range s.handlers {
		if handler := session.Stream(info); handler != nil {
			handlers = append(handlers, handler)
		}
	}
	if len(handlers) == 0 {
		return nil
	}
	return &streamSink{session: s, handlers: handlers}
}

// Finish delivers at most one terminal result. It waits for synchronous
// callbacks already running, rejects later callbacks, then reports the result.
// The driver remains responsible for draining its protocol goroutines first.
func (s *Session) Finish(outcome Outcome) {
	if s == nil {
		return
	}
	s.finished.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		for _, handler := range s.handlers {
			if finished, ok := handler.(FinishObserver); ok {
				finished.Finished(outcome)
			}
		}
	})
}

type streamSink struct {
	session  *Session
	mu       sync.Mutex
	handlers []StreamObserver
}

func (s *streamSink) Message(item []byte) {
	if s == nil {
		return
	}
	s.session.mu.RLock()
	defer s.session.mu.RUnlock()
	if s.session.closed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, handler := range s.handlers {
		handler.Message(item)
	}
}
