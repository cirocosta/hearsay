package routed

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/cirocosta/hearsay/link"
)

// Link is an independently routed control connection and stream supply.
// Closing its control stream closes the link. Close terminates its pending
// opens, accepts, and all active streams without affecting other links.
type Link struct {
	endpoint *Endpoint
	token    Token
	peer     []byte
	control  *control
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	changed  chan struct{}
	queue    []net.Conn
	conns    map[net.Conn]struct{}
}

var _ link.Link = (*Link)(nil)

func newLink(e *Endpoint, conn net.Conn, token Token, peer []byte) *Link {
	ctx, cancel := context.WithCancel(e.ctx)
	l := &Link{
		endpoint: e,
		token:    token,
		peer:     append([]byte(nil), peer...),
		ctx:      ctx,
		cancel:   cancel,
		changed:  make(chan struct{}),
		conns:    map[net.Conn]struct{}{conn: {}},
	}
	l.control = &control{Conn: conn, link: l}
	return l
}

// Info returns a copy of the link's identity and advertised peer address.
func (l *Link) Info() Info { return Info{Token: l.token, Peer: append([]byte(nil), l.peer...)} }

// Control returns the persistent duplex control stream.
func (l *Link) Control() io.ReadWriteCloser { return l.control }

func (l *Link) add(conn net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.conns[conn] = struct{}{}
	return true
}

var errStreamOverflow = errors.New("incoming stream limit exceeded")

func (l *Link) deliver(conn net.Conn) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return net.ErrClosed
	}
	if len(l.queue) >= link.StreamCount {
		return errStreamOverflow
	}
	l.conns[conn] = struct{}{}
	l.queue = append(l.queue, conn)
	close(l.changed)
	l.changed = make(chan struct{})
	return nil
}

// Open dials a fresh outgoing connection. Closing the returned writer delivers
// accepted bytes followed by EOF; it never returns a connection to a pool.
func (l *Link) Open(ctx context.Context) (io.WriteCloser, error) {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	stopLink := context.AfterFunc(l.ctx, cancel)
	defer stopLink()
	if err := child.Err(); err != nil {
		return nil, err
	}
	conn, err := l.endpoint.dial(child, append([]byte(nil), l.peer...))
	if err != nil {
		return nil, err
	}
	tracked, ok := l.endpoint.track(conn)
	if !ok {
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	conn = tracked
	stop := context.AfterFunc(child, func() { l.endpoint.closeConn(conn) })
	defer stop()
	if !l.add(conn) {
		l.endpoint.closeConn(conn)
		return nil, net.ErrClosed
	}
	err = writeHeader(conn, kindStream, l.token, nil)
	if ctxErr := child.Err(); ctxErr != nil {
		err = ctxErr
	}
	if err != nil {
		l.release(conn)
		return nil, err
	}
	return &writer{stream: &stream{Conn: conn, link: l}}, nil
}

// Accept yields a peer stream without assuming any ordering across streams.
// A canceled wait leaves its next incoming stream available to later calls.
func (l *Link) Accept(ctx context.Context) (io.ReadCloser, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if l.closed {
			return nil, net.ErrClosed
		}
		if len(l.queue) > 0 {
			conn := l.queue[0]
			copy(l.queue, l.queue[1:])
			l.queue[len(l.queue)-1] = nil
			l.queue = l.queue[:len(l.queue)-1]
			return &reader{stream: &stream{Conn: conn, link: l}}, nil
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		l.mu.Lock()
	}
}

func (l *Link) release(conn net.Conn) {
	l.endpoint.closeConn(conn)
	l.mu.Lock()
	delete(l.conns, conn)
	l.mu.Unlock()
}

// Close terminates the link and removes its routing registration.
func (l *Link) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.cancel()
	close(l.changed)
	conns := make([]net.Conn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.conns = nil
	l.queue = nil
	l.mu.Unlock()
	l.endpoint.mu.Lock()
	if l.endpoint.links[l.token] == l {
		delete(l.endpoint.links, l.token)
	}
	l.endpoint.mu.Unlock()
	for _, c := range conns {
		l.endpoint.closeConn(c)
	}
	return nil
}

type control struct {
	net.Conn
	link *Link
}

func (c *control) Close() error { return c.link.Close() }

type stream struct {
	net.Conn
	link *Link
	once sync.Once
}

func (s *stream) Close() error { s.once.Do(func() { s.link.release(s.Conn) }); return nil }

type (
	reader struct{ *stream }
	writer struct{ *stream }
)
