package link

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func closeTest(t *testing.T, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func waitResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transport operation did not finish")
	}
}

func TestControlDuplex(t *testing.T) {
	for _, capacity := range []int{1, 7, 8192} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			a, b := MemoryWithCapacity(capacity)
			defer closeTest(t, a)
			payload := bytes.Repeat([]byte("control"), 4096)
			done := make(chan error, 4)
			for _, stream := range []io.ReadWriteCloser{a.Control(), b.Control()} {
				go func() { _, err := stream.Write(payload); done <- err }()
				go func() {
					got := make([]byte, len(payload))
					_, err := io.ReadFull(stream, got)
					if err == nil && !bytes.Equal(got, payload) {
						err = errors.New("control bytes changed")
					}
					done <- err
				}()
			}
			for range 4 {
				waitResult(t, done)
			}
			// the same control stream survives subsequent sessions.
			go func() { _, err := a.Control().Write([]byte{42}); done <- err }()
			var one [1]byte
			if _, err := io.ReadFull(b.Control(), one[:]); err != nil || one[0] != 42 {
				t.Fatalf("control reuse: %v, %v", one, err)
			}
			waitResult(t, done)
		})
	}
}

func TestIndependentStreams(t *testing.T) {
	for _, capacity := range []int{1, 7, 8192} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			a, b := MemoryWithCapacity(capacity)
			defer closeTest(t, a)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			writers := make([]io.WriteCloser, StreamCount)
			readers := make([]io.ReadCloser, StreamCount)
			// every stream can open without the peer accepting or consuming any.
			for i := range StreamCount {
				var err error
				writers[i], err = a.Open(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			for i := range StreamCount {
				var err error
				readers[i], err = b.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			// fill one stream and leave its next write blocked while all other streams
			// and both control directions continue independently.
			full := bytes.Repeat([]byte{5}, capacity)
			if _, err := writers[0].Write(full); err != nil {
				t.Fatal(err)
			}
			blocked := make(chan error, 1)
			go func() { _, err := writers[0].Write([]byte{6}); blocked <- err }()
			done := make(chan error, StreamCount)
			for i := 1; i < StreamCount; i++ {
				go func() {
					_, err := writers[i].Write([]byte{byte(i)})
					if err == nil {
						err = writers[i].Close()
					}
					done <- err
				}()
				var one [1]byte
				if _, err := io.ReadFull(readers[i], one[:]); err != nil || one[0] != byte(i) {
					t.Fatalf("stream %d: %v %v", i, one, err)
				}
				if _, err := readers[i].Read(one[:]); err != io.EOF {
					t.Fatalf("stream %d EOF: %v", i, err)
				}
				closeTest(t, readers[i])
			}
			for range StreamCount - 1 {
				waitResult(t, done)
			}
			select {
			case err := <-blocked:
				t.Fatalf("full stream write completed: %v", err)
			default:
			}
			got := make([]byte, capacity+1)
			if _, err := io.ReadFull(readers[0], got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got[:capacity], full) || got[capacity] != 6 {
				t.Fatal("buffered bytes changed")
			}
			waitResult(t, blocked)
			closeTest(t, writers[0])
			closeTest(t, readers[0])
			p := a.pair
			p.mu.Lock()
			remaining := len(p.pipes)
			p.mu.Unlock()
			if remaining != 2 {
				t.Fatalf("completed pipes retained: %d", remaining)
			}
		})
	}
}

func TestAcceptCancellationRetainsStream(t *testing.T) {
	a, b := Memory()
	defer closeTest(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w, err := a.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, w)
	if _, err = b.Accept(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r, err := b.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closeTest(t, r)
	// cancel an accept that is already waiting and then supply its stream.
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.Accept(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		done <- err
	}()
	cancel()
	waitResult(t, done)
	w, err = a.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, w)
	r, err = b.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closeTest(t, r)
}

func TestOpenCancellationAndBacklog(t *testing.T) {
	a, b := Memory()
	defer closeTest(t, a)
	for range StreamCount {
		if _, err := a.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := a.Open(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		done <- err
	}()
	cancel()
	waitResult(t, done)
	r, err := b.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closeTest(t, r)
	if _, err := a.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWakesOperations(t *testing.T) {
	a, b := MemoryWithCapacity(1)
	ctx := context.Background()
	w, err := a.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := b.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	for range StreamCount {
		if _, err := a.Open(ctx); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 5)
	checkClosed := func(err error) {
		if errors.Is(err, io.ErrClosedPipe) {
			err = nil
		}
		done <- err
	}
	go func() { _, err := w.Write([]byte{2}); checkClosed(err) }()
	go func() { _, err := a.Accept(ctx); checkClosed(err) }()
	go func() { _, err := a.Open(ctx); checkClosed(err) }()
	go func() { var one [1]byte; _, err := a.Control().Read(one[:]); checkClosed(err) }()
	go func() { _, err := b.Control().Write([]byte{1, 2}); checkClosed(err) }()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		waitResult(t, done)
	}
	if _, err = r.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	closeTest(t, a)
	closeTest(t, b)
}

func TestConcurrentOpenAndClose(t *testing.T) {
	for range 20 {
		a, b := MemoryWithCapacity(1)
		var workers sync.WaitGroup
		for range StreamCount {
			workers.Go(func() {
				w, err := a.Open(context.Background())
				if err == nil {
					_, _ = w.Write([]byte{1, 2})
					closeTest(t, w)
				}
			})
		}
		workers.Go(func() {
			for range StreamCount {
				r, err := b.Accept(context.Background())
				if err != nil {
					return
				}
				closeTest(t, r)
			}
		})
		workers.Go(func() { closeTest(t, a) })
		workers.Wait()
		closeTest(t, b)
	}
}

func TestWriterCloseDrainsAndReaderCloseAborts(t *testing.T) {
	a, b := MemoryWithCapacity(7)
	defer closeTest(t, a)
	w, _ := a.Open(context.Background())
	r, _ := b.Accept(context.Background())
	if _, err := w.Write([]byte("buffer")); err != nil {
		t.Fatal(err)
	}
	closeTest(t, w)
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "buffer" {
		t.Fatalf("%q %v", got, err)
	}
	closeTest(t, r)
	w, _ = a.Open(context.Background())
	r, _ = b.Accept(context.Background())
	closeTest(t, r)
	if _, err = w.Write([]byte{1}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	closeTest(t, w)
}
