package link

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countedLink struct {
	Link
	closes     atomic.Int32
	closeError error
}

func (c *countedLink) Close() error {
	c.closes.Add(1)
	return errors.Join(c.Link.Close(), c.closeError)
}

func managedTest(t *testing.T) (*Managed, *MemoryLink, *countedLink) {
	t.Helper()
	a, b := MemoryWithCapacity(1)
	transport := &countedLink{Link: a}
	managed := Manage(transport)
	t.Cleanup(func() { closeTest(t, managed) })
	return managed, b, transport
}

func TestManagedEpochWrapAndCleanReuse(t *testing.T) {
	m, peer, _ := managedTest(t)
	if state := m.State(); state != (SessionState{}) {
		t.Fatal(state)
	}
	for i := range 258 {
		s, err := m.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if s.Epoch() != uint8(i) {
			t.Fatalf("epoch %d at session %d", s.Epoch(), i)
		}
		if state := m.State(); state != (SessionState{Epoch: uint8(i + 1), Poisoned: true, Active: true}) {
			t.Fatal(state)
		}
		done := make(chan error, 1)
		go func() { _, err := s.Control().Write([]byte{byte(i)}); done <- err }()
		var one [1]byte
		if _, err = io.ReadFull(peer.Control(), one[:]); err != nil || one[0] != byte(i) {
			t.Fatalf("control %v %v", one, err)
		}
		waitResult(t, done)
		if err = s.Finish(); err != nil {
			t.Fatal(err)
		}
		if err = s.Finish(); err != nil {
			t.Fatal(err)
		}
		closeTest(t, s)
		if state := m.State(); state != (SessionState{Epoch: uint8(i + 1)}) {
			t.Fatal(state)
		}
	}
}

func TestManagedRejectsOverlapWithoutAdvancingEpoch(t *testing.T) {
	m, _, _ := managedTest(t)
	first, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Begin(context.Background()); !errors.Is(err, ErrSessionActive) {
		t.Fatal(err)
	}
	if state := m.State(); state.Epoch != 1 || !state.Active {
		t.Fatal(state)
	}
	if err = first.Finish(); err != nil {
		t.Fatal(err)
	}
	second, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// a completed handle cannot alter its successor's state.
	closeTest(t, first)
	if err = first.Abort(errors.New("stale failure")); err != nil {
		t.Fatal(err)
	}
	if state := m.State(); state.Epoch != 2 || !state.Active || !state.Poisoned {
		t.Fatal(state)
	}
	if err = second.Finish(); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err = first.Control().Read(one[:]); !errors.Is(err, ErrSessionFinished) {
		t.Fatal(err)
	}
	if _, err = first.Open(context.Background()); !errors.Is(err, ErrSessionFinished) {
		t.Fatal(err)
	}
	if _, err = first.Accept(context.Background()); !errors.Is(err, ErrSessionFinished) {
		t.Fatal(err)
	}
}

func TestManagedFailurePermanentlyPoisons(t *testing.T) {
	m, _, transport := managedTest(t)
	session, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("wire failed after local commit")
	if err = session.Abort(cause); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if !errors.Is(session.Err(), cause) {
		t.Fatal(session.Err())
	}
	if err = session.Finish(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = m.Begin(context.Background()); !errors.Is(err, ErrLinkPoisoned) {
			t.Fatal(err)
		}
	}
	if state := m.State(); state != (SessionState{Epoch: 1, Poisoned: true}) {
		t.Fatal(state)
	}
	closeTest(t, session)
	closeTest(t, m)
	if transport.closes.Load() != 1 {
		t.Fatal(transport.closes.Load())
	}
}

func TestManagedCanceledBeginAndIdleContexts(t *testing.T) {
	m, _, transport := managedTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Begin(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if state := m.State(); state != (SessionState{}) {
		t.Fatal(state)
	}
	ctx, cancel = context.WithCancel(context.Background())
	first, err := m.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = first.Finish(); err != nil {
		t.Fatal(err)
	}
	cancel()
	// synchronizing with a new session must not revive the finished context's
	// watcher, even though the next session now owns the same byte stream.
	next, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = next.Finish(); err != nil {
		t.Fatal(err)
	}
	if state := m.State(); state != (SessionState{Epoch: 2}) {
		t.Fatal(state)
	}
	if transport.closes.Load() != 0 {
		t.Fatal("idle cancellation closed transport")
	}
}

func TestManagedCancellationWakesAllIO(t *testing.T) {
	m, peer, transport := managedTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := m.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := peer.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, r)
	if _, err = w.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	for range StreamCount {
		if _, err = s.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 5)
	check := func(err error) {
		if err != nil {
			err = nil
		} else {
			err = errors.New("operation succeeded after cancellation")
		}
		done <- err
	}
	go func() { _, err := w.Write([]byte{2}); check(err) }()
	go func() { _, err := s.Open(context.Background()); check(err) }()
	go func() { _, err := s.Accept(context.Background()); check(err) }()
	go func() { var one [1]byte; _, err := s.Control().Read(one[:]); check(err) }()
	go func() { _, err := s.Control().Write([]byte{1, 2}); check(err) }()
	cancel()
	for range 5 {
		waitResult(t, done)
	}
	if err = s.Finish(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = m.Begin(context.Background()); !errors.Is(err, ErrLinkPoisoned) {
		t.Fatal(err)
	}
	if transport.closes.Load() != 1 {
		t.Fatal(transport.closes.Load())
	}
}

func TestManagedIndividualAcceptCancelDoesNotPoison(t *testing.T) {
	m, _, _ := managedTest(t)
	s, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.Accept(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = s.Finish(); err != nil {
		t.Fatal(err)
	}
	if m.State().Poisoned {
		t.Fatal("individual accept cancellation poisoned session")
	}
}

func TestManagedFinishCancellationRace(t *testing.T) {
	for range 300 {
		m, _, transport := managedTest(t)
		ctx, cancel := context.WithCancel(context.Background())
		s, err := m.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var workers sync.WaitGroup
		var finishErr error
		workers.Go(func() { <-start; cancel() })
		workers.Go(func() { <-start; finishErr = s.Finish() })
		close(start)
		workers.Wait()
		if finishErr == nil {
			if m.State().Poisoned {
				t.Fatal("clean finish was later poisoned")
			}
			next, err := m.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err = next.Finish(); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(finishErr, context.Canceled) {
			t.Fatal(finishErr)
		}
		closeTest(t, m)
		if transport.closes.Load() != 1 {
			t.Fatal(transport.closes.Load())
		}
	}
}

func TestManagedConcurrentAbortClosesExactlyOnce(t *testing.T) {
	m, _, transport := managedTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	s, err := m.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 20 {
		workers.Go(func() { _ = s.Abort(errors.New("failed")) })
		workers.Go(func() { closeTest(t, m) })
		workers.Go(func() { closeTest(t, s) })
	}
	cancel()
	workers.Wait()
	if transport.closes.Load() != 1 {
		t.Fatal(transport.closes.Load())
	}
	if !m.State().Poisoned {
		t.Fatal("abort cleared poison")
	}
}

type completionTest struct {
	completed, closed int
	failure           error
}

func (c *completionTest) Complete() error { c.completed++; return c.failure }
func (c *completionTest) Close() error    { c.closed++; return nil }
func TestCompleteUsesExplicitHook(t *testing.T) {
	cause := errors.New("completion failed")
	stream := &completionTest{failure: cause}
	if err := Complete(stream); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if stream.completed != 1 || stream.closed != 0 {
		t.Fatalf("complete %d, close %d", stream.completed, stream.closed)
	}
	var onlyClose io.Closer = io.NopCloser(&emptyReader{})
	if err := Complete(onlyClose); err != nil {
		t.Fatal(err)
	}
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

func TestManagedCloseErrorPreservesCause(t *testing.T) {
	a, _ := Memory()
	closeCause := errors.New("close failed")
	transport := &countedLink{Link: a, closeError: closeCause}
	m := Manage(transport)
	s, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("wire failed")
	err = s.Abort(failure)
	if !errors.Is(err, failure) || !errors.Is(err, closeCause) {
		t.Fatal(err)
	}
	if err = m.Close(); !errors.Is(err, closeCause) {
		t.Fatal(err)
	}
}

func TestManagedCloseStopsSessionDeadline(t *testing.T) {
	m, _, transport := managedTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	s, err := m.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	closeTest(t, m)
	if !errors.Is(s.Err(), io.ErrClosedPipe) {
		t.Fatal(s.Err())
	}
	cancel()
	closeTest(t, m)
	if transport.closes.Load() != 1 {
		t.Fatal(transport.closes.Load())
	}
}
