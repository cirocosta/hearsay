package routed

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/link"
)

func poolCondition(t *testing.T, name string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal(name)
		}
	}
}

func readyCount(l *Link) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, outgoing := range l.idle {
		if outgoing.ready && !outgoing.dead {
			count++
		}
	}
	return count
}
func poolCount(l *Link) int       { l.mu.Lock(); defer l.mu.Unlock(); return len(l.idle) }
func connectionCount(l *Link) int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.conns) }
func countedPair(t *testing.T, config Config) (*Link, *Link, *atomic.Int32) {
	t.Helper()
	near, far := endpointTest(t, config), endpointTest(t, config)
	dials := new(atomic.Int32)
	dial := near.dial
	near.dial = func(ctx context.Context, address []byte) (net.Conn, error) { dials.Add(1); return dial(ctx, address) }
	left, right := pairTest(t, near, far)
	dials.Store(0)
	return left, right, dials
}

func poolStream(t *testing.T, left, right *Link) (*writer, *reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outgoing, err := left.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := right.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return outgoing.(*writer), incoming.(*reader)
}

func completeTest(t *testing.T, stream io.Closer) {
	t.Helper()
	if err := link.Complete(stream); err != nil {
		t.Fatal(err)
	}
}

func exchangePoolByte(t *testing.T, w io.Writer, r io.Reader) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- writeRouting(w, []byte{42}) }()
	var one [1]byte
	if _, err := io.ReadFull(r, one[:]); err != nil || one[0] != 42 {
		t.Fatalf("payload %v %v", one, err)
	}
	resultTest(t, done)
}

func TestPoolReusesOnlyAfterBothHalvesComplete(t *testing.T) {
	for _, receiverFirst := range []bool{false, true} {
		name := "producer-first"
		if receiverFirst {
			name = "receiver-first"
		}
		t.Run(name, func(t *testing.T) {
			left, right, dials := countedPair(t, Config{})
			w, r := poolStream(t, left, right)
			exchangePoolByte(t, w, r)
			first, held := io.Closer(w), io.Closer(r)
			if receiverFirst {
				first, held = r, w
			}
			completeTest(t, first)
			// completion of only one half must not make Open wait for the other.
			nextWriter, nextReader := poolStream(t, left, right)
			if dials.Load() != 2 {
				t.Fatalf("fresh dials before both completions: %d", dials.Load())
			}
			closeTest(t, nextWriter)
			closeTest(t, nextReader)
			completeTest(t, held)
			poolCondition(
				t,
				"both completions did not produce READY",
				func() bool { return readyCount(left) == 1 },
			)
			reusedWriter, reusedReader := poolStream(t, left, right)
			if dials.Load() != 2 {
				t.Fatalf("ready stream dialed again: %d", dials.Load())
			}
			exchangePoolByte(t, reusedWriter, reusedReader)
			completeTest(t, reusedWriter)
			completeTest(t, reusedReader)
			// deferred abort cleanup after Complete cannot close a later generation.
			closeTest(t, w)
			closeTest(t, r)
			poolCondition(
				t,
				"reused stream did not return",
				func() bool { return readyCount(left) == 1 },
			)
		})
	}
}

func TestDisablePoolingDialsEveryOutgoingStream(t *testing.T) {
	left, right, dials := countedPair(t, Config{DisablePooling: true})
	for range 3 {
		w, r := poolStream(t, left, right)
		exchangePoolByte(t, w, r)
		completeTest(t, w)
		completeTest(t, r)
	}
	if dials.Load() != 3 {
		t.Fatal(dials.Load())
	}
	if poolCount(left) != 0 {
		t.Fatal("disabled outgoing pool retained connections")
	}
}

func TestPoolRejectsIdleEOFAndUnexpectedBytes(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		name := "EOF"
		if invalid {
			name = "unexpected-byte"
		}
		t.Run(name, func(t *testing.T) {
			left, right, dials := countedPair(t, Config{})
			w, r := poolStream(t, left, right)
			completeTest(t, w)
			completeTest(t, r)
			poolCondition(t, "stream not ready", func() bool { return readyCount(left) == 1 })
			if invalid {
				if err := writeRouting(r.Conn, []byte{99}); err != nil {
					t.Fatal(err)
				}
			} else {
				closeTest(t, r.Conn)
			}
			poolCondition(
				t,
				"dead pool entry retained",
				func() bool { return poolCount(left) == 0 && connectionCount(left) == 1 },
			)
			nextWriter, nextReader := poolStream(t, left, right)
			if dials.Load() != 2 {
				t.Fatal(dials.Load())
			}
			closeTest(t, nextWriter)
			closeTest(t, nextReader)
		})
	}
}

func TestPoolRejectsReusedWrongTokenAndKind(t *testing.T) {
	for _, wrongKind := range []bool{false, true} {
		name := "token"
		if wrongKind {
			name = "kind"
		}
		t.Run(name, func(t *testing.T) {
			left, right, _ := countedPair(t, Config{})
			w, r := poolStream(t, left, right)
			completeTest(t, w)
			completeTest(t, r)
			poolCondition(t, "stream not ready", func() bool { return readyCount(left) == 1 })
			outgoing := left.takeReady()
			if outgoing == nil {
				t.Fatal("missing ready connection")
			}
			token, kind, address := Token{99}, kindStream, []byte(nil)
			if wrongKind {
				token, kind, address = left.token, kindLink, []byte{1}
			}
			if err := writeHeader(outgoing.conn, kind, token, address); err != nil {
				t.Fatal(err)
			}
			poolCondition(
				t,
				"invalid reuse header did not release connection",
				func() bool { return connectionCount(left) == 1 },
			)
			if right.endpoint.Stats().RoutingFailures != 1 {
				t.Fatal(right.endpoint.Stats())
			}
			nextWriter, nextReader := poolStream(t, left, right)
			closeTest(t, nextWriter)
			closeTest(t, nextReader)
		})
	}
}

func TestPoolsBoundRetainedConnections(t *testing.T) {
	left, right, _ := countedPair(t, Config{})
	writers := make([]*writer, 0, link.StreamCount+1)
	readers := make([]*reader, 0, link.StreamCount+1)
	for range link.StreamCount + 1 {
		w, r := poolStream(t, left, right)
		writers = append(writers, w)
		readers = append(readers, r)
	}
	for _, w := range writers {
		completeTest(t, w)
	}
	if poolCount(left) != link.StreamCount {
		t.Fatalf("outgoing pool size %d", poolCount(left))
	}
	for _, r := range readers {
		completeTest(t, r)
	}
	poolCondition(
		t,
		"retained pool did not become ready",
		func() bool { return readyCount(left) == link.StreamCount },
	)
	right.mu.Lock()
	incomingIdle := right.incomingIdle
	right.mu.Unlock()
	if incomingIdle > link.StreamCount {
		t.Fatalf("incoming idle count %d", incomingIdle)
	}
	closeTest(t, left)
	closeTest(t, right)
	left.mu.Lock()
	outgoingRetained := len(left.conns) + len(left.idle)
	left.mu.Unlock()
	right.mu.Lock()
	incomingRetained := len(right.conns) + right.incomingIdle
	right.mu.Unlock()
	if outgoingRetained != 0 || incomingRetained != 0 {
		t.Fatalf(
			"retained after close: outgoing %d, incoming %d",
			outgoingRetained,
			incomingRetained,
		)
	}
}

type stalledHeaderConn struct {
	net.Conn
	block   atomic.Bool
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (c *stalledHeaderConn) Write(p []byte) (int, error) {
	if c.block.Load() {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}

func (c *stalledHeaderConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestPoolCanceledReuseHeaderClosesConnection(t *testing.T) {
	near, far := endpointTest(t, Config{}), endpointTest(t, Config{})
	original := near.dial
	near.dial = func(ctx context.Context, address []byte) (net.Conn, error) {
		conn, err := original(ctx, address)
		if err != nil {
			return nil, err
		}
		return &stalledHeaderConn{
			Conn:    conn,
			entered: make(chan struct{}, 1),
			closed:  make(chan struct{}),
		}, nil
	}
	left, right := pairTest(t, near, far)
	w, r := poolStream(t, left, right)
	completeTest(t, w)
	completeTest(t, r)
	poolCondition(t, "stream not ready", func() bool { return readyCount(left) == 1 })
	gated := w.Conn.(*trackedConn).Conn.(*stalledHeaderConn)
	gated.block.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := left.Open(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		} else if err == nil {
			err = errors.New("canceled reuse succeeded")
		}
		done <- err
	}()
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reuse header did not start")
	}
	cancel()
	resultTest(t, done)
	poolCondition(
		t,
		"canceled reused connection retained",
		func() bool { return connectionCount(left) == 1 },
	)
	nextWriter, nextReader := poolStream(t, left, right)
	closeTest(t, nextWriter)
	closeTest(t, nextReader)
}

func TestPoolReuseDoesNotConsumeFreshHeaderAdmission(t *testing.T) {
	near, far := endpointTest(t, Config{}), endpointTest(t, Config{PendingHeaders: 1})
	left, right := pairTest(t, near, far)
	w, r := poolStream(t, left, right)
	completeTest(t, w)
	completeTest(t, r)
	poolCondition(t, "stream not ready", func() bool { return readyCount(left) == 1 })
	stalled, err := TCPDial(context.Background(), far.address)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, stalled)
	poolCondition(
		t,
		"stalled fresh header not admitted",
		func() bool { far.mu.Lock(); defer far.mu.Unlock(); return len(far.conns) == 3 },
	)
	nextWriter, nextReader := poolStream(t, left, right)
	exchangePoolByte(t, nextWriter, nextReader)
	completeTest(t, nextWriter)
	completeTest(t, nextReader)
}

func TestPoolREADYFlushesBufferedConnections(t *testing.T) {
	near, far := flushEndpointTest(t), flushEndpointTest(t)
	left, right := pairTest(t, near, far)
	w, r := poolStream(t, left, right)
	exchangePoolByte(t, w, r)
	completeTest(t, w)
	completeTest(t, r)
	poolCondition(t, "buffered READY not flushed", func() bool { return readyCount(left) == 1 })
	nextWriter, nextReader := poolStream(t, left, right)
	exchangePoolByte(t, nextWriter, nextReader)
	completeTest(t, nextWriter)
	completeTest(t, nextReader)
}

func TestPoolConcurrentCompletionAndClose(t *testing.T) {
	for range 20 {
		left, right, _ := countedPair(t, Config{})
		w, r := poolStream(t, left, right)
		var workers sync.WaitGroup
		workers.Go(func() { completeTest(t, w) })
		workers.Go(func() { completeTest(t, r) })
		workers.Go(func() { closeTest(t, left) })
		workers.Go(func() { closeTest(t, right) })
		workers.Wait()
	}
}

type readyEOFConn struct{ net.Conn }

func (readyEOFConn) Read(p []byte) (int, error) { p[0] = ready; return 1, io.EOF }
func TestPoolRejectsREADYWithTerminalError(t *testing.T) {
	if _, err := readReady(readyEOFConn{}); err != io.EOF {
		t.Fatal(err)
	}
}
