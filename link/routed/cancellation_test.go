package routed

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancellationDisarmJoinsStartedClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	disarm := closeOnCancellation(ctx, func() { close(entered); <-release; close(finished) })
	cancel()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation close did not start")
	}
	disarmed := make(chan struct{})
	go func() { disarm(); close(disarmed) }()
	// a stop-only implementation returns immediately while Close remains held.
	select {
	case <-disarmed:
		t.Fatal("disarm returned before in-flight connection Close finished")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-disarmed:
	case <-time.After(5 * time.Second):
		t.Fatal("disarm did not join connection Close")
	}
	select {
	case <-finished:
	default:
		t.Fatal("connection Close still running after disarm")
	}
}

func TestCancellationDisarmPreventsLateClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closed := make(chan struct{})
	disarm := closeOnCancellation(ctx, func() { close(closed) })
	disarm()
	cancel()
	select {
	case <-closed:
		t.Fatal("disarmed cancellation closed handed-off connection")
	case <-time.After(20 * time.Millisecond):
	}
}

type handoffCloseConn struct {
	net.Conn
	cancel           context.CancelFunc
	entered, release chan struct{}
	first            atomic.Bool
}

func (c *handoffCloseConn) Flush() error {
	c.cancel()
	<-c.entered
	return nil
}

func (c *handoffCloseConn) Close() error {
	// repeated Close can return while the first caller still owns cleanup.
	if !c.first.CompareAndSwap(false, true) {
		return nil
	}
	close(c.entered)
	<-c.release
	return c.Conn.Close()
}

func TestCanceledRoutingJoinsConnectionCloseBeforeReturn(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "link"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			near, far := endpointTest(t, Config{}), endpointTest(t, Config{})
			var carrier *Link
			if stream {
				carrier, _ = pairTest(t, near, far)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			dial := near.dial
			near.dial = func(ctx context.Context, address []byte) (net.Conn, error) {
				conn, err := dial(ctx, address)
				if err != nil {
					return nil, err
				}
				return &handoffCloseConn{
					Conn:    conn,
					cancel:  cancel,
					entered: entered,
					release: release,
				}, nil
			}
			done := make(chan error, 1)
			go func() {
				if stream {
					writer, err := carrier.Open(ctx)
					if err == nil {
						_ = writer.Close()
					}
					done <- err
					return
				}
				_, opened, err := near.Link(ctx, far.address)
				if err == nil {
					_ = opened.Close()
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation close did not start")
			}
			select {
			case err := <-done:
				t.Fatalf("routing returned before cancellation cleanup finished: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("routing did not finish after cancellation close")
			}
		})
	}
}
