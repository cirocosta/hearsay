package routed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/link"
)

func closeTest(t *testing.T, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func endpointTest(t *testing.T, config Config) *Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address, err := EncodeTCPAddr(listener.Addr().(*net.TCPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(listener, address, TCPDial, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, e) })
	return e
}

func resultTest(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not finish")
	}
}

func pairTest(t *testing.T, a, b *Endpoint) (*Link, *Link) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, left, err := a.Link(ctx, b.address)
	if err != nil {
		t.Fatal(err)
	}
	incoming, right, err := b.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Token != incoming.Token || !bytes.Equal(info.Peer, b.address) ||
		!bytes.Equal(incoming.Peer, a.address) {
		t.Fatal("routing identity mismatch")
	}
	return left, right
}

func TestRoutedDuplexSparseAndRepeated(t *testing.T) {
	a, b := endpointTest(t, Config{}), endpointTest(t, Config{})
	left, right := pairTest(t, a, b)
	payload := bytes.Repeat([]byte("control"), 1<<15)
	done := make(chan error, 4)
	for _, l := range []*Link{left, right} {
		go func() { _, err := l.Control().Write(payload); done <- err }()
		go func() {
			got := make([]byte, len(payload))
			_, err := io.ReadFull(l.Control(), got)
			if err == nil && !bytes.Equal(got, payload) {
				err = errors.New("control bytes changed")
			}
			done <- err
		}()
	}
	for range 4 {
		resultTest(t, done)
	}
	for range 3 {
		for _, direction := range [][2]*Link{{left, right}, {right, left}} {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			writers := make([]io.WriteCloser, link.StreamCount)
			for i := range writers {
				var err error
				writers[i], err = direction[0].Open(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			for i, w := range writers {
				go func() {
					_, err := w.Write([]byte{byte(i)})
					if err == nil {
						err = w.Close()
					}
					done <- err
				}()
			}
			seen := make(map[byte]bool)
			for range writers {
				r, err := direction[1].Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				if err != nil || len(got) != 1 {
					t.Fatalf("stream data %v %v", got, err)
				}
				if seen[got[0]] || int(got[0]) >= len(writers) {
					t.Fatalf("duplicate or invalid stream label %v", got)
				}
				seen[got[0]] = true
				// router arrival order is independent of open order; labels belong to
				// the session and are not interpreted by this transport.
				closeTest(t, r)
				resultTest(t, done)
			}
			cancel()
		}
	}
	left.mu.Lock()
	remaining := len(left.conns)
	left.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("completed connections retained: %d", remaining)
	}
}

func TestAcceptCancellationAndIsolation(t *testing.T) {
	a, b := endpointTest(t, Config{}), endpointTest(t, Config{})
	first, remote := pairTest(t, a, b)
	other, otherRemote := pairTest(t, a, b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := remote.Accept(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := first.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := remote.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	closeTest(t, w)
	closeTest(t, r)
	closeTest(t, first)
	done := make(chan error, 1)
	go func() { _, err := other.Control().Write([]byte{8}); done <- err }()
	var one [1]byte
	if _, err = io.ReadFull(otherRemote.Control(), one[:]); err != nil || one[0] != 8 {
		t.Fatalf("unrelated control: %v %v", one, err)
	}
	resultTest(t, done)
}

func TestCloseCancelsPendingAndPreservesOtherEndpoint(t *testing.T) {
	a, b := endpointTest(t, Config{}), endpointTest(t, Config{})
	left, right := pairTest(t, a, b)
	done := make(chan error, 2)
	go func() {
		_, err := left.Accept(context.Background())
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		done <- err
	}()
	go func() {
		var one [1]byte
		_, err := left.Control().Read(one[:])
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		done <- err
	}()
	closeTest(t, left)
	resultTest(t, done)
	resultTest(t, done)
	var one [1]byte
	if _, err := right.Control().Read(one[:]); err != io.EOF {
		t.Fatalf("peer departure: %v", err)
	}
	_, _ = pairTest(t, a, b)
}

func TestRoutingAdmissionDeadlineAndMalformedHeaders(t *testing.T) {
	e := endpointTest(t, Config{PendingHeaders: 1, RoutingTimeout: 30 * time.Millisecond})
	stalled, err := TCPDial(context.Background(), e.address)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, stalled)
	if err = stalled.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err = stalled.Read(one[:]); err != io.EOF {
		t.Fatalf("stalled route: %v", err)
	}
	bad, err := TCPDial(context.Background(), e.address)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, bad)
	if err = writeAll(bad, make([]byte, prefixLen)); err != nil {
		t.Fatal(err)
	}
	if _, err = bad.Read(one[:]); err != io.EOF {
		t.Fatalf("bad route: %v", err)
	}
	a := endpointTest(t, Config{})
	_, _ = pairTest(t, a, e)
	if got := e.Stats().RoutingFailures; got != 2 {
		t.Fatalf("routing failures %d", got)
	}
}

func TestConcurrentEndpointClose(t *testing.T) {
	for range 10 {
		a, b := endpointTest(t, Config{}), endpointTest(t, Config{})
		var workers sync.WaitGroup
		workers.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, l, err := a.Link(ctx, b.address)
			if err == nil {
				closeTest(t, l)
			}
		})
		workers.Go(func() { closeTest(t, a) })
		workers.Go(func() { closeTest(t, b) })
		workers.Wait()
	}
}

func TestAddressCodec(t *testing.T) {
	for _, text := range []string{"127.0.0.1:42", "[::1]:65535", "[::ffff:192.0.2.1]:1"} {
		addr := netip.MustParseAddrPort(text)
		b, err := EncodeTCPAddr(addr)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeTCPAddr(b)
		if err != nil {
			t.Fatal(err)
		}
		if got != netip.AddrPortFrom(addr.Addr().Unmap(), addr.Port()) {
			t.Fatalf("%v != %v", got, addr)
		}
	}
	if _, err := EncodeTCPAddr(netip.MustParseAddrPort("[fe80::1%eth0]:1")); err == nil {
		t.Fatal("scoped address accepted")
	}
}

func TestUnregisteredLinkCloseKeepsTokenOwner(t *testing.T) {
	e := endpointTest(t, Config{})
	control, remote := net.Pipe()
	defer closeTest(t, remote)
	owner := newLink(e, control, Token{7}, []byte{1})
	if !e.register(owner) {
		t.Fatal("owner registration failed")
	}
	duplicateControl, duplicateRemote := net.Pipe()
	defer closeTest(t, duplicateRemote)
	duplicate := newLink(e, duplicateControl, owner.token, []byte{2})
	if e.register(duplicate) {
		t.Fatal("duplicate token registered")
	}
	closeTest(t, duplicate)
	e.mu.Lock()
	registered := e.links[owner.token]
	e.mu.Unlock()
	if registered != owner {
		t.Fatal("duplicate close removed token owner")
	}
	closeTest(t, owner)
}

func TestIncomingQueueLimitAndCancellation(t *testing.T) {
	a, b := endpointTest(t, Config{}), endpointTest(t, Config{PendingLinks: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, first, err := a.Link(ctx, b.address)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, first)
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, _, err = b.Accept(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, extra, err := a.Link(ctx, b.address); err == nil {
		closeTest(t, extra)
		t.Fatal("full incoming queue admitted a link")
	}
	incoming, accepted, err := b.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, accepted)
	if incoming.Token != info.Token {
		t.Fatal("canceled accept lost queued link")
	}
	_, _ = pairTest(t, a, b)
}

func TestUnknownStreamTokenCannotReachOtherLink(t *testing.T) {
	a, b := endpointTest(t, Config{}), endpointTest(t, Config{})
	_, _ = pairTest(t, a, b)
	conn, err := TCPDial(context.Background(), b.address)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, conn)
	if err = writeHeader(conn, kindStream, Token{99}, nil); err != nil {
		t.Fatal(err)
	}
	if err = conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err = conn.Read(one[:]); err != io.EOF {
		t.Fatalf("unknown token: %v", err)
	}
	if got := b.Stats().RoutingFailures; got != 1 {
		t.Fatalf("unknown-token count %d", got)
	}
	_, _ = pairTest(t, a, b)
}

func TestCanceledEstablishmentClosesPendingACK(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	readHeaderDone := make(chan error, 1)
	readPeerEOF := make(chan error, 1)
	dial := func(context.Context, []byte) (net.Conn, error) {
		local, remote := net.Pipe()
		go func() {
			defer closeTest(t, remote)
			_, headerErr := readHeader(remote)
			readHeaderDone <- headerErr
			var one [1]byte
			_, readErr := remote.Read(one[:])
			if readErr == io.EOF {
				readErr = nil
			}
			readPeerEOF <- readErr
		}()
		return local, nil
	}
	endpoint, err := New(listener, []byte{1}, dial, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTest(t, endpoint)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, linkErr := endpoint.Link(ctx, []byte{2})
		if errors.Is(linkErr, context.Canceled) {
			linkErr = nil
		} else {
			linkErr = fmt.Errorf("canceled establishment: %w", linkErr)
		}
		done <- linkErr
	}()
	resultTest(t, readHeaderDone)
	cancel()
	resultTest(t, done)
	resultTest(t, readPeerEOF)
	endpoint.mu.Lock()
	connections, links := len(endpoint.conns), len(endpoint.links)
	endpoint.mu.Unlock()
	if connections != 0 || links != 0 {
		t.Fatalf("canceled establishment retained %d connections, %d links", connections, links)
	}
}

type noncomparableConn struct {
	net.Conn
	extra []byte
}

func TestCallerConnectionNeedNotBeComparable(t *testing.T) {
	e := endpointTest(t, Config{})
	local, remote := net.Pipe()
	defer closeTest(t, remote)
	conn, ok := e.track(noncomparableConn{Conn: local, extra: []byte{1}})
	if !ok {
		t.Fatal("connection rejected")
	}
	e.closeConn(conn)
}
