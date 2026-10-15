package routed

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/link"
)

type flushRequiredConn struct {
	net.Conn
	writer *bufio.Writer
}

func newFlushRequiredConn(conn net.Conn) *flushRequiredConn {
	return &flushRequiredConn{Conn: conn, writer: bufio.NewWriter(conn)}
}
func (c *flushRequiredConn) Write(p []byte) (int, error) { return c.writer.Write(p) }
func (c *flushRequiredConn) Flush() error                { return c.writer.Flush() }

type flushListener struct{ net.Listener }

func (l *flushListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newFlushRequiredConn(conn), nil
}

func flushEndpointTest(t *testing.T) *Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address, err := EncodeTCPAddr(listener.Addr().(*net.TCPAddr).AddrPort())
	if err != nil {
		t.Fatal(err)
	}
	dial := func(ctx context.Context, address []byte) (net.Conn, error) {
		conn, err := TCPDial(ctx, address)
		if err != nil {
			return nil, err
		}
		return newFlushRequiredConn(conn), nil
	}
	endpoint, err := New(
		&flushListener{Listener: listener},
		address,
		dial,
		Config{RoutingTimeout: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTest(t, endpoint) })
	return endpoint
}

func TestRoutingAndStreamFlushForwarding(t *testing.T) {
	near, far := flushEndpointTest(t), flushEndpointTest(t)
	left, right := pairTest(t, near, far)
	// establishment requires both the outgoing LINK header and incoming ACK
	// to cross buffered writers before either side can await its next read.
	payload := []byte("control")
	if err := writeRouting(left.Control(), payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(right.Control(), got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("control %q %v", got, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer, err := left.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := right.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeRouting(writer, []byte("data")); err != nil {
		t.Fatal(err)
	}
	got = make([]byte, 4)
	if _, err = io.ReadFull(reader, got); err != nil || string(got) != "data" {
		t.Fatalf("stream %q %v", got, err)
	}
	closeTest(t, writer)
	closeTest(t, reader)
}

type failingFlushBuffer struct {
	bytes.Buffer
	cause error
	calls int
}

func (b *failingFlushBuffer) Flush() error { b.calls++; return b.cause }
func TestHeaderReturnsFlushFailure(t *testing.T) {
	cause := errors.New("flush failed")
	writer := &failingFlushBuffer{cause: cause}
	if err := writeHeader(writer, kindStream, Token{}, nil); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if writer.calls != 1 || writer.Len() != prefixLen {
		t.Fatalf("flushes %d, bytes %d", writer.calls, writer.Len())
	}
	if err := link.Flush(io.Discard); err != nil {
		t.Fatal(err)
	}
}
