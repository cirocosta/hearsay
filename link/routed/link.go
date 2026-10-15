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
	endpoint     *Endpoint
	token        Token
	peer         []byte
	control      *control
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	closed       bool
	supplyFailed bool
	changed      chan struct{}
	queue        []net.Conn
	conns        map[net.Conn]struct{}
	idle         []*outgoingConn
	incomingIdle int
	reusing      []net.Conn
	workers      sync.WaitGroup
	shutdownDone chan struct{}
}

var _ link.Link = (*Link)(nil)

func newLink(e *Endpoint, conn net.Conn, token Token, peer []byte) *Link {
	ctx, cancel := context.WithCancel(e.ctx)
	l := &Link{
		endpoint:     e,
		token:        token,
		peer:         append([]byte(nil), peer...),
		ctx:          ctx,
		cancel:       cancel,
		changed:      make(chan struct{}),
		conns:        map[net.Conn]struct{}{conn: {}},
		shutdownDone: make(chan struct{}),
	}
	l.control = &control{Conn: conn, link: l}
	return l
}

// Info returns a copy of the link's identity and advertised peer address.
func (l *Link) Info() Info { return Info{Token: l.token, Peer: append([]byte(nil), l.peer...)} }

// Control returns the persistent duplex control stream.
func (l *Link) Control() io.ReadWriteCloser { return l.control }

var errStreamOverflow = errors.New("incoming stream limit exceeded")

func (l *Link) deliver(conn net.Conn) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return net.ErrClosed
	}
	if l.supplyFailed {
		return io.ErrClosedPipe
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

// Open takes a READY-confirmed connection without waiting, or dials a fresh
// independent connection. Complete allows reuse; Close aborts the stream.
func (l *Link) Open(ctx context.Context) (io.WriteCloser, error) {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	stopLink := context.AfterFunc(l.ctx, cancel)
	defer stopLink()
	if err := child.Err(); err != nil {
		return nil, err
	}
	conn, outgoing, err := l.openConnection(child)
	if err != nil {
		return nil, err
	}
	disarm := closeOnCancellation(child, func() { l.endpoint.closeConn(conn) })
	err = writeHeader(conn, kindStream, l.token, nil)
	disarm()
	if ctxErr := child.Err(); ctxErr != nil {
		err = ctxErr
	}
	if err != nil {
		l.release(conn)
		return nil, err
	}
	return &writer{stream: &stream{Conn: conn, link: l}, outgoing: outgoing}, nil
}

func (l *Link) openConnection(ctx context.Context) (net.Conn, *outgoingConn, error) {
	if outgoing := l.takeReady(); outgoing != nil {
		return outgoing.conn, outgoing, nil
	}
	conn, err := l.endpoint.dial(ctx, append([]byte(nil), l.peer...))
	if err != nil {
		return nil, nil, err
	}
	tracked, ok := l.endpoint.track(conn)
	if !ok {
		_ = conn.Close()
		return nil, nil, net.ErrClosed
	}
	outgoing, ok := l.prepareOutgoing(tracked)
	if !ok {
		l.endpoint.closeConn(tracked)
		return nil, nil, net.ErrClosed
	}
	return tracked, outgoing, nil
}

// Accept yields a peer stream without assuming any ordering across streams.
// A canceled wait leaves its next incoming stream available to later calls.
// A peer that exceeds the incoming queue limit loses its stream supply. Already
// queued streams drain before Accept reports that failure; control stays open.
func (l *Link) Accept(ctx context.Context) (io.ReadCloser, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if stream, err := l.pollIncoming(ctx); stream != nil || err != nil {
			return stream, err
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

// pollIncoming checks the supply with Link.mu held. A nil stream and error
// mean the caller must wait for another routing event.
func (l *Link) pollIncoming(ctx context.Context) (io.ReadCloser, error) {
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
	if l.supplyFailed {
		return nil, errStreamOverflow
	}
	return nil, nil
}

func (l *Link) release(conn net.Conn) {
	l.endpoint.closeConn(conn)
	l.mu.Lock()
	delete(l.conns, conn)
	l.removeIdle(conn)
	l.mu.Unlock()
}

// failIncoming revokes only the incoming supply. The endpoint retains ownership
// of the detached link so its eventual Close still interrupts all link I/O.
func (l *Link) failIncoming() {
	l.mu.Lock()
	if l.closed || l.supplyFailed {
		l.mu.Unlock()
		return
	}
	l.supplyFailed = true
	idle := append([]net.Conn(nil), l.reusing...)
	close(l.changed)
	l.changed = make(chan struct{})
	l.mu.Unlock()
	l.endpoint.mu.Lock()
	if l.endpoint.links[l.token] == l {
		delete(l.endpoint.links, l.token)
		if !l.endpoint.closed {
			if l.endpoint.detached == nil {
				l.endpoint.detached = make(map[*Link]struct{})
			}
			l.endpoint.detached[l] = struct{}{}
		}
	}
	l.endpoint.mu.Unlock()
	for _, conn := range idle {
		l.release(conn)
	}
	l.endpoint.streamOverflows.Add(1)
}

// shutdown revokes routing and interrupts every connection without joining.
func (l *Link) shutdown() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
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
	l.idle = nil
	l.reusing = nil
	l.mu.Unlock()
	l.endpoint.mu.Lock()
	if l.endpoint.links[l.token] == l {
		delete(l.endpoint.links, l.token)
	}
	delete(l.endpoint.detached, l)
	l.endpoint.mu.Unlock()
	for _, c := range conns {
		l.endpoint.closeConn(c)
	}
	close(l.shutdownDone)
}

// Close terminates the link and joins its bounded reuse workers.
func (l *Link) Close() error {
	l.shutdown()
	<-l.shutdownDone
	l.workers.Wait()
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
	writer struct {
		*stream
		outgoing *outgoingConn
	}
)

func flushConnection(conn net.Conn) error { return link.Flush(conn) }
func (c *control) Flush() error           { return flushConnection(c.Conn) }
func (s *stream) Flush() error            { return flushConnection(s.Conn) }
