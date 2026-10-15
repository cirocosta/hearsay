package routed

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/link"
)

func rawStream(t *testing.T, destination *Endpoint, token Token) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := TCPDial(ctx, destination.address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, conn) })
	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = writeHeader(conn, kindStream, token, nil); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestOverflowDrainsSupplyAndPreservesOtherDirections(t *testing.T) {
	for _, reused := range []bool{false, true} {
		name := "fresh"
		if reused {
			name = "reused"
		}
		t.Run(name, func(t *testing.T) {
			near, far := endpointTest(t, Config{}), endpointTest(t, Config{})
			left, right := pairTest(t, near, far)
			other, otherRemote := pairTest(t, near, far)
			activeWriter, activeReader := poolStream(t, left, right)
			pooledWriter, pooledReader := poolStream(t, left, right)
			completeTest(t, pooledWriter)
			completeTest(t, pooledReader)
			poolCondition(t, "READY missing", func() bool { return readyCount(left) == 1 })
			for range link.StreamCount {
				conn := rawStream(t, far, left.token)
				if _, err := conn.Write([]byte{42}); err != nil {
					t.Fatal(err)
				}
			}
			poolCondition(
				t,
				"queued headers missing",
				func() bool { right.mu.Lock(); defer right.mu.Unlock(); return len(right.queue) == link.StreamCount },
			)
			if reused {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				w, err := left.Open(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer closeTest(t, w)
			} else {
				rawStream(t, far, left.token)
			}
			poolCondition(
				t,
				"overflow did not revoke supply",
				func() bool { return far.Stats().StreamOverflows == 1 },
			)
			poolCondition(
				t,
				"reuse wait retained",
				func() bool { right.mu.Lock(); defer right.mu.Unlock(); return right.incomingIdle == 0 },
			)
			far.mu.Lock()
			registered, detached := far.links[left.token] != nil, len(far.detached)
			far.mu.Unlock()
			if registered || detached != 1 {
				t.Fatalf("revocation: registered %v, detached %d", registered, detached)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for range link.StreamCount {
				stream, err := right.Accept(ctx)
				if err != nil {
					t.Fatalf("accepted stream lost: %v", err)
				}
				var one [1]byte
				if _, err = io.ReadFull(stream, one[:]); err != nil || one[0] != 42 {
					t.Fatalf("queued payload %v: %v", one, err)
				}
				completeTest(t, stream)
			}
			if _, err := right.Accept(ctx); !errors.Is(err, errStreamOverflow) {
				t.Fatalf("drained supply: %v", err)
			}
			exchangePoolByte(t, activeWriter, activeReader)
			completeTest(t, activeWriter)
			completeTest(t, activeReader)
			exchangePoolByte(t, left.Control(), right.Control())
			exchangePoolByte(t, other.Control(), otherRemote.Control())
			reverseWriter, reverseReader := poolStream(t, right, left)
			exchangePoolByte(t, reverseWriter, reverseReader)
			closeTest(t, reverseWriter)
			closeTest(t, reverseReader)
			rejected := rawStream(t, far, left.token)
			var one [1]byte
			if _, err := rejected.Read(one[:]); err != io.EOF {
				t.Fatalf("revoked token accepted: %v", err)
			}
			closeTest(t, far)
			right.mu.Lock()
			closed, retained := right.closed, len(right.conns)
			right.mu.Unlock()
			far.mu.Lock()
			remaining := len(far.detached)
			far.mu.Unlock()
			if !closed || retained != 0 || remaining != 0 {
				t.Fatalf(
					"endpoint close: closed %v, conns %d, detached %d",
					closed,
					retained,
					remaining,
				)
			}
		})
	}
}
