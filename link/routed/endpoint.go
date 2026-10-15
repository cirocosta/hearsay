package routed

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Dial opens an independent, fully duplex connection to an opaque address.
// Calls may run concurrently. The implementation must honor cancellation.
type Dial func(context.Context, []byte) (net.Conn, error)

// Config bounds routing work. Zero queue limits select the defaults.
// RoutingTimeout applies only to initial headers and acknowledgements.
// A zero timeout leaves routing attempts untimed, bounded by PendingHeaders.
type Config struct {
	PendingHeaders int
	PendingLinks   int
	RoutingTimeout time.Duration
}

// Info describes a routed link. Peer is a copy of the remote advertised name.
type Info struct {
	Token Token
	Peer  []byte
}

// Stats reports cumulative invalid routing attempts and stream overflows.
type Stats struct {
	RoutingFailures uint64
	StreamOverflows uint64
}

// Endpoint owns a listener and routes each accepted connection by link token.
// New starts its bounded router. Close terminates all links and waits for the
// router to exit. Established data connections are not pooled or reused.
type Endpoint struct {
	listener        net.Listener
	dial            Dial
	address         []byte
	config          Config
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	closed          bool
	failure         error
	links           map[Token]*Link
	conns           map[net.Conn]struct{}
	incoming        []*Link
	reserved        int
	changed         chan struct{}
	workers         sync.WaitGroup
	routingFailures atomic.Uint64
	streamOverflows atomic.Uint64
}

// New takes ownership of listener on success. Advertised names contain between
// 1 and MaxAddressLen opaque bytes and must be dialable by the other endpoint.
func New(listener net.Listener, advertised []byte, dial Dial, config Config) (*Endpoint, error) {
	if listener == nil || dial == nil {
		return nil, errors.New("listener and dial are required")
	}
	if len(advertised) < 1 || len(advertised) > MaxAddressLen {
		return nil, errors.New("advertised address length must be 1..255")
	}
	if config.PendingHeaders < 0 || config.PendingLinks < 0 || config.RoutingTimeout < 0 {
		return nil, errors.New("negative routing limit")
	}
	if config.PendingHeaders == 0 {
		config.PendingHeaders = 64
	}
	if config.PendingLinks == 0 {
		config.PendingLinks = 64
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Endpoint{
		listener: listener,
		dial:     dial,
		address:  append([]byte(nil), advertised...),
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
		links:    make(map[Token]*Link),
		conns:    make(map[net.Conn]struct{}),
		changed:  make(chan struct{}),
	}
	e.workers.Add(1)
	go e.run()
	return e, nil
}

func (e *Endpoint) signal() { close(e.changed); e.changed = make(chan struct{}) }

func (e *Endpoint) run() {
	defer e.workers.Done()
	slots := make(chan struct{}, e.config.PendingHeaders)
	for {
		select {
		case slots <- struct{}{}:
		case <-e.ctx.Done():
			return
		}
		conn, err := e.listener.Accept()
		if err != nil {
			<-slots
			e.shutdown(err)
			return
		}
		tracked, ok := e.track(conn)
		if !ok {
			_ = conn.Close()
			<-slots
			return
		}
		e.workers.Add(1)
		go func() { defer e.workers.Done(); defer func() { <-slots }(); e.route(tracked) }()
	}
}

func (e *Endpoint) track(conn net.Conn) (net.Conn, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, false
	}
	tracked := &trackedConn{Conn: conn}
	e.conns[tracked] = struct{}{}
	return tracked, true
}

func (e *Endpoint) closeConn(conn net.Conn) {
	_ = conn.Close()
	e.mu.Lock()
	delete(e.conns, conn)
	e.mu.Unlock()
}

func (e *Endpoint) route(conn net.Conn) {
	if e.config.RoutingTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(e.config.RoutingTimeout)); err != nil {
			e.reject(conn)
			return
		}
	}
	h, err := readHeader(conn)
	if err != nil {
		e.reject(conn)
		return
	}
	if h.kind == kindLink {
		e.routeLink(conn, h)
		return
	}
	if err = conn.SetDeadline(time.Time{}); err != nil {
		e.reject(conn)
		return
	}
	e.mu.Lock()
	l := e.links[h.token]
	e.mu.Unlock()
	if l == nil {
		e.reject(conn)
		return
	}
	if err := l.deliver(conn); err != nil {
		e.closeConn(conn)
		if errors.Is(err, errStreamOverflow) {
			e.streamOverflows.Add(1)
			_ = l.Close()
		}
	}
}
func (e *Endpoint) reject(conn net.Conn) { e.routingFailures.Add(1); e.closeConn(conn) }

func (e *Endpoint) reserve(conn net.Conn, h header) *Link {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.links[h.token] != nil || len(e.incoming)+e.reserved >= e.config.PendingLinks {
		return nil
	}
	l := newLink(e, conn, h.token, h.peer)
	e.links[h.token] = l
	e.reserved++
	return l
}

func (e *Endpoint) routeLink(conn net.Conn, h header) {
	l := e.reserve(conn, h)
	if l == nil {
		e.reject(conn)
		return
	}
	success := false
	defer func() {
		if !success {
			e.mu.Lock()
			e.reserved--
			e.signal()
			e.mu.Unlock()
			_ = l.Close()
		}
	}()
	if err := writeAll(conn, []byte{ack}); err != nil {
		e.routingFailures.Add(1)
		return
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		e.routingFailures.Add(1)
		return
	}
	e.mu.Lock()
	if !e.closed {
		e.reserved--
		e.incoming = append(e.incoming, l)
		e.signal()
		success = true
	}
	e.mu.Unlock()
}

// Accept returns the next established incoming link. Cancellation never removes
// a queued link. Listener failure terminates the endpoint and its live links.
func (e *Endpoint) Accept(ctx context.Context) (Info, *Link, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return Info{}, nil, err
		}
		if e.closed {
			return Info{}, nil, e.failure
		}
		if len(e.incoming) > 0 {
			l := e.incoming[0]
			copy(e.incoming, e.incoming[1:])
			e.incoming[len(e.incoming)-1] = nil
			e.incoming = e.incoming[:len(e.incoming)-1]
			return l.Info(), l, nil
		}
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		e.mu.Lock()
	}
}

// Link establishes a new link. Both endpoints register its random token before
// this method returns, so either may immediately open sparse data streams.
func (e *Endpoint) Link(ctx context.Context, peer []byte) (Info, *Link, error) {
	if len(peer) < 1 || len(peer) > MaxAddressLen {
		return Info{}, nil, errors.New("peer address length must be 1..255")
	}
	var token Token
	if _, err := rand.Read(token[:]); err != nil {
		return Info{}, nil, err
	}
	ctx, cancel := e.operationContext(ctx)
	defer cancel()
	conn, err := e.dial(ctx, append([]byte(nil), peer...))
	if err != nil {
		return Info{}, nil, err
	}
	tracked, ok := e.track(conn)
	if !ok {
		_ = conn.Close()
		return Info{}, nil, net.ErrClosed
	}
	conn = tracked
	stop := context.AfterFunc(ctx, func() { e.closeConn(conn) })
	defer stop()
	l := newLink(e, conn, token, peer)
	if !e.register(l) {
		_ = l.Close()
		return Info{}, nil, net.ErrClosed
	}
	if err = writeHeader(conn, kindLink, token, e.address); err == nil {
		err = readACK(conn)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
	}
	if err != nil {
		_ = l.Close()
		return Info{}, nil, err
	}
	return l.Info(), l, nil
}

func readACK(r io.Reader) error {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	if b[0] != ack {
		return errors.New("invalid routing acknowledgement")
	}
	return nil
}

func (e *Endpoint) register(l *Link) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.links[l.token] != nil {
		return false
	}
	e.links[l.token] = l
	return true
}

func (e *Endpoint) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(e.ctx, cancel)
	return child, func() { stop(); cancel() }
}

// Stats returns routing counters without blocking active connections.
func (e *Endpoint) Stats() Stats {
	return Stats{
		RoutingFailures: e.routingFailures.Load(),
		StreamOverflows: e.streamOverflows.Load(),
	}
}

func (e *Endpoint) shutdown(err error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	e.failure = err
	e.cancel()
	e.signal()
	conns := make([]net.Conn, 0, len(e.conns))
	for c := range e.conns {
		conns = append(conns, c)
	}
	links := make([]*Link, 0, len(e.links))
	for _, l := range e.links {
		links = append(links, l)
	}
	e.incoming = nil
	e.mu.Unlock()
	_ = e.listener.Close()
	for _, c := range conns {
		e.closeConn(c)
	}
	for _, l := range links {
		_ = l.Close()
	}
}

// Close stops the router and all links. It is idempotent.
func (e *Endpoint) Close() error { e.shutdown(net.ErrClosed); e.workers.Wait(); return nil }

// trackedConn gives every admitted connection a comparable registry identity,
// including caller-supplied net.Conn values with noncomparable fields.
type trackedConn struct{ net.Conn }
