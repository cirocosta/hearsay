package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/link"
)

// LinkPair is a fresh connected pair. Cleanup releases factory-owned resources
// after both links have been closed and every test operation has exited.
type LinkPair struct {
	Near, Far link.Link
	Cleanup   func() error
}

// LinkFactory must honor its context while constructing a fresh pair.
type LinkFactory func(context.Context) (LinkPair, error)

// LinkOptions configures focused deadlines and optional exact buffer probes.
type LinkOptions struct {
	Timeout time.Duration
	// StreamCapacity, when positive, declares the exact per-data-stream byte
	// capacity. The suite verifies backpressure at this bound. Leave zero when
	// the transport cannot expose an exact window; pressure then remains sampled.
	StreamCapacity int
}

type linkFixture struct {
	*tasks
	pair              LinkPair
	nearOnce, farOnce sync.Once
	nearErr, farErr   error
}

func (f *linkFixture) closeNear() error {
	f.nearOnce.Do(func() { f.nearErr = f.pair.Near.Close() })
	return f.nearErr
}

func (f *linkFixture) closeFar() error {
	f.farOnce.Do(func() { f.farErr = f.pair.Far.Close() })
	return f.farErr
}
func (f *linkFixture) close() error { return errors.Join(f.closeNear(), f.closeFar()) }

func freshLink(t *testing.T, factory LinkFactory, options LinkOptions) *linkFixture {
	t.Helper()
	g := newTasks(options.Timeout)
	pair, err := factory(g.ctx)
	if err != nil {
		g.cancel()
		t.Fatal(err)
	}
	if pair.Near == nil || pair.Far == nil {
		g.cancel()
		t.Fatal("factory returned a nil link")
	}
	f := &linkFixture{tasks: g, pair: pair}
	stop := context.AfterFunc(g.ctx, func() { _ = f.close() })
	t.Cleanup(func() {
		stop()
		g.cleanup(t, f.close)
		if pair.Cleanup != nil {
			if err := pair.Cleanup(); err != nil {
				t.Error(err)
			}
		}
	})
	return f
}

// CheckLink runs independent checks on fresh pairs, in both directions. Control
// duplex exchanges use 32 KiB per direction. Unknown buffer sizes can hide
// coupling; use CheckLinkCapacities for exact-window transports. Factories and
// transport Close must cooperate with cancellation for bounded cleanup.
func CheckLink(t *testing.T, factory LinkFactory, options ...LinkOptions) {
	t.Helper()
	var config LinkOptions
	if len(options) > 0 {
		config = options[0]
	}
	checks := []struct {
		name  string
		check func(*testing.T, *linkFixture)
	}{
		{"control-duplex", checkControl},
		{"stream-completion-and-abort", checkStreamEnds},
		{"delayed-half-completion", checkDelayedCompletion},
		{"concurrent-streams", checkConcurrentStreams},
		{
			"independence",
			func(t *testing.T, f *linkFixture) { checkIndependence(t, f, config.StreamCapacity) },
		},
		{"accept-cancellation", checkAcceptCancellation},
		{"close-unblocks", checkLinkClose},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) { check.check(t, freshLink(t, factory, config)) })
	}
}

// CheckLinkCapacities runs the entire suite at each exact stream capacity.
// Passing capacity 1 checks the smallest buffered window allowed by the link
// contract. The factory must apply that capacity independently to every stream.
func CheckLinkCapacities(
	t *testing.T,
	factory func(context.Context, int) (LinkPair, error),
	capacities ...int,
) {
	t.Helper()
	for _, capacity := range capacities {
		t.Run(fmt.Sprintf("capacity-%d", capacity), func(t *testing.T) {
			if capacity < 1 {
				t.Fatal("stream capacity must be positive")
			}
			CheckLink(
				t,
				func(ctx context.Context) (LinkPair, error) { return factory(ctx, capacity) },
				LinkOptions{StreamCapacity: capacity},
			)
		})
	}
}

func contents(n int, tag byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31) ^ tag
	}
	return b
}

func writeExact(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func readExact(r io.Reader, want []byte) error {
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return errors.New("stream bytes changed or reached the wrong endpoint")
	}
	return nil
}

func controlTasks(f *linkFixture) {
	ab, ba := contents(32*1024, 17), contents(32*1024, 91)
	f.run(func() error { return writeExact(f.pair.Near.Control(), ab) })
	f.run(func() error { return readExact(f.pair.Far.Control(), ab) })
	f.run(func() error { return writeExact(f.pair.Far.Control(), ba) })
	f.run(func() error { return readExact(f.pair.Near.Control(), ba) })
}

func checkControl(t *testing.T, f *linkFixture) {
	controlTasks(f)
	f.wait(t)
	controlTasks(f)
	f.wait(t)
}

func openStream(t *testing.T, f *linkFixture, near, far link.Link) (io.WriteCloser, io.ReadCloser) {
	t.Helper()
	writer, err := near.Open(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := far.Accept(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return writer, reader
}

func writeCompleted(w io.WriteCloser, payload []byte, abort bool) error {
	if err := writeExact(w, payload); err != nil {
		return err
	}
	if abort {
		return w.Close()
	}
	return link.Complete(w)
}

func readCompleted(r io.ReadCloser, payload []byte, abort bool) error {
	if err := readExact(r, payload); err != nil {
		return err
	}
	if abort {
		var one [1]byte
		if _, err := r.Read(one[:]); err != io.EOF {
			return fmt.Errorf("aborted stream must deliver EOF: %w", err)
		}
		return r.Close()
	}
	return link.Complete(r)
}

func checkStreamEnds(t *testing.T, f *linkFixture) {
	for _, ends := range [][2]link.Link{{f.pair.Near, f.pair.Far}, {f.pair.Far, f.pair.Near}} {
		for _, abort := range []bool{false, true} {
			w, r := openStream(t, f, ends[0], ends[1])
			payload := contents(8193, 52)
			f.run(func() error { return writeCompleted(w, payload, abort) })
			f.run(func() error { return readCompleted(r, payload, abort) })
			f.wait(t)
		}
	}
}

func sendStream(f *linkFixture, near link.Link, index byte) error {
	w, err := near.Open(f.ctx)
	if err != nil {
		return err
	}
	payload := contents(129, index)
	payload[0] = index
	if err = writeExact(w, payload); err != nil {
		_ = w.Close()
		return err
	}
	return link.Complete(w)
}

func readLabeledStream(r io.Reader, seen []bool) error {
	var label [1]byte
	if _, err := io.ReadFull(r, label[:]); err != nil {
		return err
	}
	index := int(label[0])
	if index >= len(seen) || seen[index] {
		return errors.New("invalid or duplicate stream label")
	}
	seen[index] = true
	payload := contents(129, label[0])
	return readExact(r, payload[1:])
}

func receiveStreams(f *linkFixture, far link.Link, count int) error {
	seen := make([]bool, link.StreamCount)
	for range count {
		r, err := far.Accept(f.ctx)
		if err != nil {
			return err
		}
		if err = readLabeledStream(r, seen); err != nil {
			_ = r.Close()
			return err
		}
		if err = link.Complete(r); err != nil {
			return err
		}
	}
	return nil
}

func streamTasks(f *linkFixture, near, far link.Link, count int) {
	for i := range count {
		f.run(func() error { return sendStream(f, near, byte(i)) })
	}
	f.run(func() error { return receiveStreams(f, far, count) })
}

func checkConcurrentStreams(t *testing.T, f *linkFixture) {
	streamTasks(f, f.pair.Near, f.pair.Far, link.StreamCount)
	streamTasks(f, f.pair.Far, f.pair.Near, link.StreamCount)
	f.wait(t)
}

func pressureStream(
	t *testing.T,
	f *linkFixture,
	near, far link.Link,
	capacity int,
) (io.ReadCloser, <-chan error, <-chan error) {
	t.Helper()
	w, r := openStream(t, f, near, far)
	if capacity > 0 {
		if err := writeExact(w, contents(capacity, 71)); err != nil {
			t.Fatal(err)
		}
	}
	stopped := make(chan error, 1)
	firstWrite := make(chan error, 1)
	started := make(chan struct{})
	f.pending.Add(1)
	go func() {
		defer f.pending.Done()
		payload := contents(512, 11)
		close(started)
		err := writeExact(w, []byte{1})
		firstWrite <- err
		if err != nil {
			stopped <- err
			_ = w.Close()
			return
		}
		for {
			if err := writeExact(w, payload); err != nil {
				stopped <- err
				_ = w.Close()
				return
			}
		}
	}()
	<-started
	return r, stopped, firstWrite
}

func checkIndependence(t *testing.T, f *linkFixture, capacity int) {
	for _, ends := range [][2]link.Link{{f.pair.Near, f.pair.Far}, {f.pair.Far, f.pair.Near}} {
		stalled, stopped, firstWrite := pressureStream(t, f, ends[0], ends[1], capacity)
		streamTasks(f, ends[0], ends[1], link.StreamCount-1)
		streamTasks(f, ends[1], ends[0], link.StreamCount)
		controlTasks(f)
		f.wait(t)
		if capacity > 0 {
			select {
			case err := <-firstWrite:
				t.Fatalf("write exceeded declared stream capacity: %v", err)
			default:
			}
		}
		// the other streams and control must complete while this receiver is idle.
		select {
		case err := <-stopped:
			t.Fatalf("stalled stream failed while link healthy: %v", err)
		default:
		}
		checkClose(t, stalled)
		select {
		case <-stopped:
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
	}
}

type acceptedStream struct {
	reader io.ReadCloser
	err    error
}

func collectCanceledAccept(
	t *testing.T,
	f *linkFixture,
	far link.Link,
	accepted <-chan acceptedStream,
) io.ReadCloser {
	t.Helper()
	var result acceptedStream
	select {
	case result = <-accepted:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	if result.err != nil {
		if !errors.Is(result.err, context.Canceled) {
			t.Fatal(result.err)
		}
		var err error
		result.reader, err = far.Accept(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	if result.reader == nil {
		t.Fatal("successful accept returned nil stream")
	}
	return result.reader
}

func canceledAccept(t *testing.T, f *linkFixture, near, far link.Link, cancelFirst bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	accepted := make(chan acceptedStream, 1)
	f.pending.Add(1)
	go func() { defer f.pending.Done(); r, err := far.Accept(ctx); accepted <- acceptedStream{r, err} }()
	if cancelFirst {
		cancel()
	}
	w, err := near.Open(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	reader := collectCanceledAccept(t, f, far, accepted)
	payload := contents(257, 82)
	f.run(func() error { return writeCompleted(w, payload, false) })
	f.run(func() error { return readCompleted(reader, payload, false) })
	f.wait(t)
}

func checkAcceptCancellation(t *testing.T, f *linkFixture) {
	for _, ends := range [][2]link.Link{{f.pair.Near, f.pair.Far}, {f.pair.Far, f.pair.Near}} {
		for i := range 32 {
			canceledAccept(t, f, ends[0], ends[1], i%2 == 0)
		}
	}
}

func checkLinkClose(t *testing.T, f *linkFixture) {
	w, r := openStream(t, f, f.pair.Near, f.pair.Far)
	f.run(func() error {
		return expectFailure(
			func() error { var one [1]byte; _, err := f.pair.Near.Control().Read(one[:]); return err },
		)
	})
	f.run(
		func() error { return expectFailure(func() error { _, err := f.pair.Near.Accept(f.ctx); return err }) },
	)
	f.run(func() error { return expectFailure(func() error { return writeUntilError(w) }) })
	if err := f.closeNear(); err != nil {
		t.Fatal(err)
	}
	f.wait(t)
	checkClose(t, r)
	f.run(func() error {
		return expectFailure(
			func() error { var one [1]byte; _, err := f.pair.Far.Control().Read(one[:]); return err },
		)
	})
	f.wait(t)
}

func writeUntilError(w io.Writer) error {
	payload := contents(512, 1)
	for {
		if err := writeExact(w, payload); err != nil {
			return err
		}
	}
}

func checkDelayedCompletion(t *testing.T, f *linkFixture) {
	for _, ends := range [][2]link.Link{{f.pair.Near, f.pair.Far}, {f.pair.Far, f.pair.Near}} {
		for _, receiverFirst := range []bool{false, true} {
			delayedCompletion(t, f, ends[0], ends[1], receiverFirst)
		}
	}
}

func delayedCompletion(t *testing.T, f *linkFixture, near, far link.Link, receiverFirst bool) {
	t.Helper()
	w, r := openStream(t, f, near, far)
	f.run(func() error { return writeExact(w, []byte{1}) })
	f.run(func() error { return readExact(r, []byte{1}) })
	f.wait(t)
	completed, held := io.Closer(w), io.Closer(r)
	if receiverFirst {
		completed, held = r, w
	}
	if err := link.Complete(completed); err != nil {
		t.Fatal(err)
	}
	for range link.StreamCount + 1 {
		nextWriter, nextReader := openStream(t, f, near, far)
		f.run(func() error { return writeCompleted(nextWriter, []byte{2}, false) })
		f.run(func() error { return readCompleted(nextReader, []byte{2}, false) })
		f.wait(t)
	}
	if err := link.Complete(held); err != nil {
		t.Fatal(err)
	}
}
