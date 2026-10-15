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

type controlResponse struct {
	data []byte
	err  error
}
type gatedControl struct {
	responses                       chan controlResponse
	started                         chan struct{}
	closed                          chan struct{}
	once                            sync.Once
	reads, active, maximum, flushes atomic.Int32
	flushError                      error
}

func newGatedControl() *gatedControl {
	return &gatedControl{
		responses: make(chan controlResponse, 128),
		started:   make(chan struct{}, 128),
		closed:    make(chan struct{}),
	}
}

func (c *gatedControl) Read(p []byte) (int, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for old := c.maximum.Load(); active > old; old = c.maximum.Load() {
		if c.maximum.CompareAndSwap(old, active) {
			break
		}
	}
	c.reads.Add(1)
	c.started <- struct{}{}
	select {
	case response := <-c.responses:
		return copy(p, response.data), response.err
	case <-c.closed:
		return 0, io.ErrClosedPipe
	}
}
func (*gatedControl) Write(p []byte) (int, error) { return len(p), nil }

func (c *gatedControl) Close() error { c.once.Do(func() { close(c.closed) }); return nil }
func (c *gatedControl) Flush() error { c.flushes.Add(1); return c.flushError }

type controlTestLink struct {
	Link
	control *gatedControl
}

func (l *controlTestLink) Control() io.ReadWriteCloser { return l.control }

func (l *controlTestLink) Close() error { return errors.Join(l.control.Close(), l.Link.Close()) }

func controlManagedTest(t *testing.T) (*Managed, *gatedControl) {
	t.Helper()
	a, _ := Memory()
	c := newGatedControl()
	m := Manage(&controlTestLink{Link: a, control: c})
	t.Cleanup(func() { closeTest(t, m) })
	return m, c
}

func readStarted(t *testing.T, c *gatedControl) {
	t.Helper()
	select {
	case <-c.started:
	case <-time.After(5 * time.Second):
		t.Fatal("raw control read did not start")
	}
}

func canceledRead(t *testing.T, m *Managed, c *gatedControl) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		var one [1]byte
		_, err := m.ReadControlContext(ctx, one[:])
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		result <- err
	}()
	readStarted(t, c)
	cancel()
	waitResult(t, result)
}

func TestControlPendingReadSurvivesCallerCancellation(t *testing.T) {
	m, c := controlManagedTest(t)
	canceledRead(t, m, c)
	if m.State().Poisoned {
		t.Fatal("individual idle read poisoned link")
	}
	session, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c.responses <- controlResponse{data: []byte("abc"), err: io.EOF}
	var got [3]byte
	if _, err = io.ReadFull(session.Control(), got[:]); err != nil || string(got[:]) != "abc" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err = session.Control().Read(got[:]); err != io.EOF {
		t.Fatal(err)
	}
	if c.reads.Load() != 1 || c.maximum.Load() != 1 {
		t.Fatalf("raw reads %d, simultaneous %d", c.reads.Load(), c.maximum.Load())
	}
	if err = session.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestControlPeekAndCanceledReadPreserveBufferedBytes(t *testing.T) {
	m, c := controlManagedTest(t)
	c.responses <- controlResponse{data: []byte("first")}
	for range 2 {
		first, err := m.PeekControlContext(context.Background())
		if err != nil || first != 'f' {
			t.Fatalf("peek %q %v", first, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var got [5]byte
	if n, err := m.ReadControlContext(ctx, got[:]); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read %d %v", n, err)
	}
	session, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadFull(session.Control(), got[:]); err != nil || string(got[:]) != "first" {
		t.Fatalf("%q %v", got, err)
	}
	if c.reads.Load() != 1 {
		t.Fatal(c.reads.Load())
	}
	if err = session.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestControlPendingReadSurvivesSessionBoundary(t *testing.T) {
	m, c := controlManagedTest(t)
	first, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var p [32]byte
		_, err := first.ReadControlContext(ctx, p[:])
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		done <- err
	}()
	readStarted(t, c)
	cancel()
	waitResult(t, done)
	if err = first.Finish(); err != nil {
		t.Fatal(err)
	}
	next, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c.responses <- controlResponse{data: []byte("next")}
	var p [4]byte
	if _, err = io.ReadFull(next.Control(), p[:]); err != nil || string(p[:]) != "next" {
		t.Fatalf("%q %v", p, err)
	}
	if c.reads.Load() != 1 {
		t.Fatal("session boundary restarted pending raw read")
	}
	if err = next.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestControlPartialPrefixThenEOF(t *testing.T) {
	m, c := controlManagedTest(t)
	session, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c.responses <- controlResponse{data: []byte(".")}
	var prefix [32]byte
	n, err := session.ReadControlContext(context.Background(), prefix[:])
	if n != 1 || err != nil || prefix[0] != '.' {
		t.Fatalf("prefix %q %v", prefix[:n], err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := session.ReadControlContext(ctx, prefix[:])
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		done <- err
	}()
	readStarted(t, c)
	readStarted(t, c)
	cancel()
	waitResult(t, done)
	c.responses <- controlResponse{err: io.EOF}
	if _, err = session.ReadControlContext(context.Background(), prefix[:]); err != io.EOF {
		t.Fatal(err)
	}
	if c.reads.Load() != 2 {
		t.Fatal(c.reads.Load())
	}
}

func TestControlConcurrentCanceledWaitersShareOneRawRead(t *testing.T) {
	m, c := controlManagedTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	var failures atomic.Int32
	for range 32 {
		workers.Go(func() {
			var one [1]byte
			_, err := m.ReadControlContext(ctx, one[:])
			if !errors.Is(err, context.Canceled) {
				failures.Add(1)
			}
		})
	}
	readStarted(t, c)
	cancel()
	workers.Wait()
	if failures.Load() != 0 {
		t.Fatal(failures.Load())
	}
	if c.reads.Load() != 1 || c.maximum.Load() != 1 {
		t.Fatalf("raw reads %d, simultaneous %d", c.reads.Load(), c.maximum.Load())
	}
	c.responses <- controlResponse{data: []byte("z")}
	first, err := m.PeekControlContext(context.Background())
	if err != nil || first != 'z' {
		t.Fatalf("%q %v", first, err)
	}
}

func TestControlCloseJoinsPendingWorker(t *testing.T) {
	m, c := controlManagedTest(t)
	canceledRead(t, m, c)
	closeTest(t, m)
	select {
	case <-m.control.finished:
	default:
		t.Fatal("managed close left raw reader running")
	}
	if c.active.Load() != 0 {
		t.Fatal("raw read still active after close")
	}
}

func TestControlEmptyReadsAndZeroLength(t *testing.T) {
	m, c := controlManagedTest(t)
	if n, err := m.ReadControlContext(context.Background(), nil); n != 0 || err != nil {
		t.Fatalf("empty read %d %v", n, err)
	}
	if c.reads.Load() != 0 {
		t.Fatal("zero-length read started worker")
	}
	for range 100 {
		c.responses <- controlResponse{}
	}
	var one [1]byte
	if _, err := m.ReadControlContext(context.Background(), one[:]); err != io.ErrNoProgress {
		t.Fatal(err)
	}
}

func TestControlFlushForwarding(t *testing.T) {
	m, c := controlManagedTest(t)
	session, err := m.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = Flush(session.Control()); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("flush failed")
	c.flushError = cause
	if err = Flush(session.Control()); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if c.flushes.Load() != 2 {
		t.Fatal(c.flushes.Load())
	}
	if err = session.Finish(); err != nil {
		t.Fatal(err)
	}
	if err = Flush(session.Control()); !errors.Is(err, ErrSessionFinished) {
		t.Fatal(err)
	}
	if err = Flush(io.Discard); err != nil {
		t.Fatal(err)
	}
}
