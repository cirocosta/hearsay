package link

import (
	"context"
	"io"
	"sync"
)

const defaultCapacity = 8 * 1024

// MemoryLink is one endpoint of an in-process link. It starts no goroutines.
// Closing either endpoint closes the pair and wakes every pending operation.
type MemoryLink struct {
	pair    *memoryPair
	side    int
	control *controlStream
}

type memoryPair struct {
	mu       sync.Mutex
	closed   bool
	changed  chan struct{}
	queue    [2][]*pipe
	pipes    map[*pipe]struct{}
	capacity int
}

// Memory returns a connected pair with 8 KiB of buffering per stream.
func Memory() (*MemoryLink, *MemoryLink) { return MemoryWithCapacity(defaultCapacity) }

// MemoryWithCapacity returns a pair with capacity bytes per stream and
// direction. It panics when capacity is less than one. Each direction queues
// up to StreamCount opened streams awaiting acceptance.
func MemoryWithCapacity(capacity int) (*MemoryLink, *MemoryLink) {
	if capacity < 1 {
		panic("a link stream must buffer at least one byte")
	}
	p := &memoryPair{
		capacity: capacity,
		changed:  make(chan struct{}),
		pipes:    make(map[*pipe]struct{}),
	}
	ab, ba := p.newPipe(), p.newPipe()
	a, b := &MemoryLink{pair: p, side: 0}, &MemoryLink{pair: p, side: 1}
	a.control = &controlStream{read: ba, write: ab, close: a.Close}
	b.control = &controlStream{read: ab, write: ba, close: b.Close}
	return a, b
}

// Control returns the persistent duplex control stream.
func (l *MemoryLink) Control() io.ReadWriteCloser { return l.control }

func (p *memoryPair) newPipe() *pipe {
	s := newPipe(p.capacity)
	p.pipes[s] = struct{}{}
	s.release = func() { p.mu.Lock(); delete(p.pipes, s); p.mu.Unlock() }
	return s
}

func (p *memoryPair) signal() { close(p.changed); p.changed = make(chan struct{}) }

// Open creates an independent outgoing stream. Closing the writer delivers
// buffered bytes before EOF. Open fails if ctx is already canceled.
func (l *MemoryLink) Open(ctx context.Context) (io.WriteCloser, error) {
	p := l.pair
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.closed {
			return nil, io.ErrClosedPipe
		}
		target := 1 - l.side
		if len(p.queue[target]) < StreamCount {
			s := p.newPipe()
			p.queue[target] = append(p.queue[target], s)
			p.signal()
			return &pipeWriter{s}, nil
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		p.mu.Lock()
	}
}

// Accept waits for the next incoming stream. Cancellation leaves queued
// streams available to a later call. No ordering across streams is required.
func (l *MemoryLink) Accept(ctx context.Context) (io.ReadCloser, error) {
	p := l.pair
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.closed {
			return nil, io.ErrClosedPipe
		}
		q := p.queue[l.side]
		if len(q) > 0 {
			s := q[0]
			copy(q, q[1:])
			q[len(q)-1] = nil
			p.queue[l.side] = q[:len(q)-1]
			p.signal()
			return &pipeReader{s}, nil
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		p.mu.Lock()
	}
}

// Close closes both endpoints and all streams. It is safe to call repeatedly.
func (l *MemoryLink) Close() error {
	p := l.pair
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	all := p.pipes
	p.pipes = nil
	p.queue = [2][]*pipe{}
	p.signal()
	p.mu.Unlock()
	for s := range all {
		s.abort()
	}
	return nil
}

type controlStream struct {
	read, write *pipe
	close       func() error
}

func (c *controlStream) Read(b []byte) (int, error)  { return c.read.Read(b) }
func (c *controlStream) Write(b []byte) (int, error) { return c.write.Write(b) }
func (c *controlStream) Close() error                { return c.close() }

type pipeReader struct{ *pipe }

func (r *pipeReader) Close() error { r.closeRead(); return nil }

type pipeWriter struct{ *pipe }

func (w *pipeWriter) Close() error { w.closeWrite(); return nil }

// pipe uses a bounded ring. Reader and writer wait independently; no helper
// goroutine or per-chunk allocation is needed.
type pipe struct {
	mu                      sync.Mutex
	readable, writable      *sync.Cond
	buffer                  []byte
	head, size              int
	readClosed, writeClosed bool
	release                 func()
	released                bool
}

func newPipe(capacity int) *pipe {
	p := &pipe{buffer: make([]byte, capacity)}
	p.readable = sync.NewCond(&p.mu)
	p.writable = sync.NewCond(&p.mu)
	return p
}

func (p *pipe) releaseIfDone() {
	if !p.released && (p.readClosed || p.writeClosed && p.size == 0) {
		p.released = true
		if p.release != nil {
			p.release()
		}
	}
}

func (p *pipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(b) == 0 {
		return 0, nil
	}
	for p.size == 0 && !p.writeClosed && !p.readClosed {
		p.readable.Wait()
	}
	if p.readClosed {
		return 0, io.ErrClosedPipe
	}
	if p.size == 0 {
		return 0, io.EOF
	}
	n := min(len(b), p.size)
	first := min(n, len(p.buffer)-p.head)
	copy(b, p.buffer[p.head:p.head+first])
	copy(b[first:n], p.buffer[:n-first])
	p.head = (p.head + n) % len(p.buffer)
	p.size -= n
	p.writable.Broadcast()
	p.releaseIfDone()
	return n, nil
}

func (p *pipe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for len(b) > 0 {
		for p.size == len(p.buffer) && !p.readClosed && !p.writeClosed {
			p.writable.Wait()
		}
		if p.readClosed || p.writeClosed {
			return total, io.ErrClosedPipe
		}
		tail := (p.head + p.size) % len(p.buffer)
		n := min(len(b), len(p.buffer)-p.size, len(p.buffer)-tail)
		copy(p.buffer[tail:tail+n], b[:n])
		p.size += n
		total += n
		b = b[n:]
		p.readable.Signal()
	}
	return total, nil
}

func (p *pipe) closeRead() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readClosed = true
	p.writable.Broadcast()
	p.readable.Broadcast()
	p.releaseIfDone()
}

func (p *pipe) closeWrite() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writeClosed = true
	p.readable.Broadcast()
	p.writable.Broadcast()
	p.releaseIfDone()
}

func (p *pipe) abort() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readClosed = true
	p.writeClosed = true
	p.readable.Broadcast()
	p.writable.Broadcast()
}
