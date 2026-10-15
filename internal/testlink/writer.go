package testlink

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/cirocosta/hearsay/link"
)

type packet struct {
	due  time.Time
	data []byte
}

type writer struct {
	owner    *pair
	side     int
	base     io.WriteCloser
	mu       sync.Mutex
	changed  chan struct{}
	done     chan struct{}
	queue    []packet
	head     int
	used     int
	closing  bool
	complete bool
	err      error
}

func (w *writer) signal() { close(w.changed); w.changed = make(chan struct{}) }

func (w *writer) Write(data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		n := min(len(data), w.owner.config.PacketSize)
		if err := w.enqueue(data[:n]); err != nil {
			return total, err
		}
		data = data[n:]
		total += n
	}
	if err := w.Flush(); err != nil {
		return total, err
	}
	return total, nil
}

func (w *writer) enqueue(data []byte) error {
	for {
		if w.owner.ctx.Err() != nil {
			return io.ErrClosedPipe
		}
		changed, err := w.offer(data)
		if err != nil {
			return err
		}
		if changed == nil {
			return nil
		}
		select {
		case <-changed:
		case <-w.owner.ctx.Done():
			return io.ErrClosedPipe
		}
	}
}

func (w *writer) offer(data []byte) (<-chan struct{}, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return nil, w.err
	}
	if w.closing {
		return nil, io.ErrClosedPipe
	}
	if len(data) > w.owner.config.StreamWindow-w.used {
		return w.changed, nil
	}
	copied := bytes.Clone(data)
	due := w.owner.directions[w.side].reserve(time.Now(), len(copied))
	w.queue = append(w.queue, packet{due: due, data: copied})
	w.used += len(copied)
	w.owner.stats[w.side].add(len(copied))
	w.signal()
	return nil, nil
}

// Flush reports accepted queue state. Packets already belong to the transport;
// flushing does not wait for propagation or receiver reads.
func (w *writer) Flush() error {
	if w.owner.ctx.Err() != nil {
		return io.ErrClosedPipe
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Close accepts an end-of-stream behind queued packets. Delivery drains those
// packets and closes the base writer asynchronously, like a socket write close.
func (w *writer) Close() error { return w.finish(false) }

// Complete accepts a clean boundary behind queued packets. The delivery worker
// delegates that boundary only after draining all previously accepted bytes.
func (w *writer) Complete() error { return w.finish(true) }

func (w *writer) finish(complete bool) error {
	w.mu.Lock()
	if !w.closing {
		w.closing = true
		w.complete = complete
		w.signal()
	}
	defer w.mu.Unlock()
	return w.err
}

func (w *writer) deliver() {
	defer w.owner.workers.Done()
	err := w.runDelivery()
	if err != nil {
		w.owner.abort()
	}
	w.releaseAll(err)
	close(w.done)
}

func (w *writer) runDelivery() error {
	for {
		next, ok, nextErr := w.nextPacket()
		if nextErr != nil {
			return nextErr
		}
		if !ok {
			return w.finishBase()
		}
		if err := w.deliverPacket(next); err != nil {
			return err
		}
	}
}

func (w *writer) deliverPacket(next packet) error {
	if err := waitUntil(w.owner, next.due); err != nil {
		return err
	}
	n, err := w.base.Write(next.data)
	if n > 0 {
		w.owner.stats[w.side].delivered.Add(uint64(n))
	}
	if err == nil && n != len(next.data) {
		err = io.ErrShortWrite
	}
	w.releaseOne()
	return err
}

func (w *writer) nextPacket() (packet, bool, error) {
	for {
		if w.owner.ctx.Err() != nil {
			return packet{}, false, io.ErrClosedPipe
		}
		w.mu.Lock()
		if w.head < len(w.queue) {
			next := w.queue[w.head]
			w.mu.Unlock()
			return next, true, nil
		}
		if w.closing {
			w.mu.Unlock()
			return packet{}, false, nil
		}
		changed := w.changed
		w.mu.Unlock()
		select {
		case <-changed:
		case <-w.owner.ctx.Done():
			return packet{}, false, io.ErrClosedPipe
		}
	}
}

func (w *writer) finishBase() error {
	if w.owner.ctx.Err() != nil {
		return io.ErrClosedPipe
	}
	w.mu.Lock()
	complete := w.complete
	w.mu.Unlock()
	if complete {
		return link.Complete(w.base)
	}
	return w.base.Close()
}

func (w *writer) releaseOne() {
	w.mu.Lock()
	defer w.mu.Unlock()
	size := len(w.queue[w.head].data)
	w.queue[w.head] = packet{}
	w.head++
	w.used -= size
	w.owner.stats[w.side].release(size)
	if w.head == len(w.queue) {
		w.queue = w.queue[:0]
		w.head = 0
	}
	if w.head > 64 && w.head >= len(w.queue)/2 {
		remaining := copy(w.queue, w.queue[w.head:])
		clear(w.queue[remaining:])
		w.queue = w.queue[:remaining]
		w.head = 0
	}
	w.signal()
}

func (w *writer) releaseAll(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.err = err
	for _, item := range w.queue[w.head:] {
		w.owner.stats[w.side].release(len(item.data))
	}
	w.queue = nil
	w.head = 0
	w.used = 0
	w.signal()
}

func waitUntil(p *pair, due time.Time) error {
	remaining := time.Until(due)
	if remaining <= 0 {
		return nil
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-p.ctx.Done():
		return io.ErrClosedPipe
	}
}
