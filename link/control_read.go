package link

import (
	"context"
	"io"
	"sync"
)

// controlReader owns one lazy worker and at most one raw read. A canceled
// caller leaves that read and its eventual bytes available to the next caller.
// The worker and its fixed buffer belong to Managed until the link closes.
type controlReader struct {
	source                   io.Reader
	mu                       sync.Mutex
	buffer                   []byte
	failure                  error
	emptyReads               int
	pending, started, closed bool
	requests                 chan struct{}
	stop                     chan struct{}
	finished                 chan struct{}
	changed                  chan struct{}
}

func newControlReader(source io.Reader) *controlReader {
	return &controlReader{
		source:   source,
		requests: make(chan struct{}, 1),
		stop:     make(chan struct{}),
		finished: make(chan struct{}),
		changed:  make(chan struct{}),
	}
}
func (r *controlReader) signal() { close(r.changed); r.changed = make(chan struct{}) }
func (r *controlReader) startRead() {
	if !r.started {
		r.started = true
		go r.run()
	}
	r.pending = true
	r.requests <- struct{}{}
}

func (r *controlReader) run() {
	defer close(r.finished)
	var buffer [8192]byte
	for {
		select {
		case <-r.stop:
			return
		case <-r.requests:
		}
		n, err := r.source.Read(buffer[:])
		r.mu.Lock()
		r.pending = false
		if r.closed {
			r.mu.Unlock()
			return
		}
		r.publish(buffer[:n], err)
		r.signal()
		r.mu.Unlock()
	}
}

func (r *controlReader) publish(buffer []byte, err error) {
	r.buffer = buffer
	r.failure = err
	if len(buffer) > 0 {
		r.emptyReads = 0
		return
	}
	r.emptyReads++
	if err == nil && r.emptyReads >= 100 {
		r.failure = io.ErrNoProgress
	}
}

func (r *controlReader) tryRead(ctx context.Context, p []byte, peek bool) (int, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, true, err
	}
	if r.closed {
		return 0, true, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, true, nil
	}
	if len(r.buffer) > 0 {
		n := copy(p, r.buffer)
		if !peek {
			r.buffer = r.buffer[n:]
		}
		return n, true, nil
	}
	if r.failure != nil {
		return 0, true, r.failure
	}
	return 0, false, nil
}

func (r *controlReader) read(ctx context.Context, p []byte, peek bool) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if n, ready, err := r.tryRead(ctx, p, peek); ready {
			return n, err
		}
		if !r.pending {
			r.startRead()
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.mu.Lock()
	}
}

func (r *controlReader) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	close(r.stop)
	r.signal()
	if !r.started {
		close(r.finished)
	}
}
func (r *controlReader) wait() { <-r.finished }

func (m *Managed) checkIdleControl() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil {
		return ErrSessionActive
	}
	if m.poisoned {
		return ErrLinkPoisoned
	}
	return nil
}

// ReadControlContext consumes idle control bytes without beginning a session.
// A canceled call leaves any pending raw read and completed bytes cached for a
// later call or session. The caller must stop and join idle reads before Begin.
func (m *Managed) ReadControlContext(ctx context.Context, p []byte) (int, error) {
	if err := m.checkIdleControl(); err != nil {
		return 0, err
	}
	return m.control.read(ctx, p, false)
}

// PeekControlContext waits for an idle control byte without consuming it. A
// canceled idle wait or a policy decision to stop therefore cannot lose the
// next session's first byte. Join the idle wait before starting a session.
func (m *Managed) PeekControlContext(ctx context.Context) (byte, error) {
	if err := m.checkIdleControl(); err != nil {
		return 0, err
	}
	var one [1]byte
	_, err := m.control.read(ctx, one[:], true)
	return one[0], err
}

// ReadControlContext reads session control bytes with a cancelable individual
// wait. Canceling ctx preserves every prefetched byte and keeps the pending raw
// read available across session boundaries; it does not poison the session.
// Canceling the context passed to Begin still aborts the whole session.
func (s *Session) ReadControlContext(ctx context.Context, p []byte) (int, error) {
	if err := s.check(); err != nil {
		return 0, err
	}
	return s.owner.control.read(ctx, p, false)
}

// Flush forwards an optional transport flush. Unbuffered writers need no work.
func Flush(w io.Writer) error {
	if flusher, ok := w.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}
