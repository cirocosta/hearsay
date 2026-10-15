package routed

import (
	"io"
	"net"

	"github.com/cirocosta/hearsay/link"
)

const ready byte = 2

// outgoingConn owns the reverse READY channel across all of a connection's
// generations. Only its monitor reads that direction. Link.mu protects flags.
type outgoingConn struct {
	conn        net.Conn
	owner       *Link
	ready, dead bool
}

func (l *Link) prepareOutgoing(conn net.Conn) (*outgoingConn, bool) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, false
	}
	l.conns[conn] = struct{}{}
	if l.endpoint.config.DisablePooling {
		l.mu.Unlock()
		return nil, true
	}
	outgoing := &outgoingConn{conn: conn, owner: l}
	l.workers.Add(1)
	l.mu.Unlock()
	go outgoing.monitor()
	return outgoing, true
}

func (o *outgoingConn) monitor() {
	defer o.owner.workers.Done()
	for {
		byteRead, err := readReady(o.conn)
		if err != nil {
			break
		}
		if byteRead != ready || !o.markReady() {
			break
		}
	}
	o.owner.mu.Lock()
	o.dead = true
	o.owner.mu.Unlock()
	o.owner.release(o.conn)
}

func readReady(conn net.Conn) (byte, error) {
	var one [1]byte
	for range 100 {
		n, err := conn.Read(one[:])
		if err != nil {
			return 0, err
		}
		if n == 1 {
			return one[0], nil
		}
	}
	return 0, io.ErrNoProgress
}

func (o *outgoingConn) markReady() bool {
	l := o.owner
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || o.dead || o.ready {
		return false
	}
	o.ready = true
	return true
}

func (l *Link) takeReady() *outgoingConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	attempts := len(l.idle)
	for range attempts {
		outgoing := l.idle[0]
		copy(l.idle, l.idle[1:])
		l.idle[len(l.idle)-1] = nil
		l.idle = l.idle[:len(l.idle)-1]
		if outgoing.dead {
			continue
		}
		if outgoing.ready {
			outgoing.ready = false
			return outgoing
		}
		l.idle = append(l.idle, outgoing)
	}
	return nil
}

func (l *Link) putOutgoing(outgoing *outgoingConn) {
	l.mu.Lock()
	_, tracked := l.conns[outgoing.conn]
	if !l.closed && !outgoing.dead && tracked && len(l.idle) < link.StreamCount {
		l.idle = append(l.idle, outgoing)
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	l.release(outgoing.conn)
}

func (l *Link) removeIdle(conn net.Conn) {
	for i, outgoing := range l.idle {
		if outgoing.conn == conn {
			outgoing.dead = true
			copy(l.idle[i:], l.idle[i+1:])
			l.idle[len(l.idle)-1] = nil
			l.idle = l.idle[:len(l.idle)-1]
			return
		}
	}
}

// completeIncoming returns immediately. A bounded worker owns READY and the
// next header, independently of fresh-header admission and other streams.
func (l *Link) completeIncoming(conn net.Conn) {
	l.mu.Lock()
	_, tracked := l.conns[conn]
	if l.closed || l.supplyFailed || !tracked || l.incomingIdle >= link.StreamCount {
		l.mu.Unlock()
		l.release(conn)
		return
	}
	l.incomingIdle++
	l.reusing = append(l.reusing, conn)
	l.workers.Add(1)
	l.mu.Unlock()
	go l.awaitReuse(conn)
}

func (l *Link) awaitReuse(conn net.Conn) {
	defer l.workers.Done()
	h, err := reuseHeader(conn)
	closed := l.finishReuse(conn)
	if err != nil || closed {
		l.release(conn)
		return
	}
	if h.kind != kindStream || h.token != l.token {
		l.endpoint.routingFailures.Add(1)
		l.release(conn)
		return
	}
	if err = l.deliver(conn); err != nil {
		l.release(conn)
		if err == errStreamOverflow {
			l.failIncoming()
		}
	}
}

func (l *Link) finishReuse(conn net.Conn) bool {
	l.mu.Lock()
	l.incomingIdle--
	for i, waiting := range l.reusing {
		if waiting == conn {
			copy(l.reusing[i:], l.reusing[i+1:])
			l.reusing[len(l.reusing)-1] = nil
			l.reusing = l.reusing[:len(l.reusing)-1]
			break
		}
	}
	closed := l.closed || l.supplyFailed
	l.mu.Unlock()
	return closed
}

func reuseHeader(conn net.Conn) (header, error) {
	if err := writeRouting(conn, []byte{ready}); err != nil {
		return header{}, err
	}
	return readHeader(conn)
}

// Complete returns an outgoing connection only at an explicit clean boundary.
// A connection without READY remains pooled but cannot delay another Open.
func (w *writer) Complete() error {
	w.once.Do(func() {
		if w.outgoing == nil {
			w.link.release(w.Conn)
		} else {
			w.link.putOutgoing(w.outgoing)
		}
	})
	return nil
}

// Complete hands the incoming connection to its link's bounded reuse worker.
// Cleanup or handshake failure does not change the completed protocol outcome.
func (r *reader) Complete() error {
	r.once.Do(func() { r.link.completeIncoming(r.Conn) })
	return nil
}
