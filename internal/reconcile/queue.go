package reconcile

import (
	"context"
	"errors"
	"sync"
)

var errQueueClosed = errors.New("reconcile: pending queue is closed")

// pendingHandle keeps the minimum window on the native channel path. Wider
// windows reserve ring storage only as work arrives. Handles live inline in
// the engine's depth array, without one wrapper allocation per queue.
type pendingHandle struct {
	direct chan pending
	lazy   *pendingQueue
}

func newPendingHandle(limit int) pendingHandle {
	if limit <= 1 {
		return pendingHandle{direct: make(chan pending, 1)}
	}
	return pendingHandle{lazy: newPendingQueue(limit)}
}

func (q *pendingHandle) initialized() bool { return q.direct != nil || q.lazy != nil }

func (q *pendingHandle) push(ctx context.Context, item pending) error {
	if q.direct == nil {
		return q.lazy.push(ctx, item)
	}
	select {
	case q.direct <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *pendingHandle) pop(ctx context.Context) (pending, bool, error) {
	if q.direct == nil {
		return q.lazy.pop(ctx)
	}
	select {
	case item, ok := <-q.direct:
		return item, ok, nil
	case <-ctx.Done():
		return pending{}, false, ctx.Err()
	}
}

func (q *pendingHandle) close() {
	if q.direct == nil {
		q.lazy.close()
		return
	}
	close(q.direct)
}

// pendingQueue is a bounded single-producer, single-consumer queue. Storage
// grows with actual occupancy, while limit retains the session's fixed window.
// Readiness tokens coalesce and allocate no memory on individual operations.
type pendingQueue struct {
	mu                 sync.Mutex
	buffer             []pending
	head, size, limit  int
	closed             bool
	readable, writable chan struct{}
}

func newPendingQueue(limit int) *pendingQueue {
	return &pendingQueue{
		limit:    max(1, limit),
		readable: make(chan struct{}, 1),
		writable: make(chan struct{}, 1),
	}
}

func queueNotify(ready chan struct{}) {
	select {
	case ready <- struct{}{}:
	default:
	}
}

func (q *pendingQueue) push(ctx context.Context, item pending) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return errQueueClosed
		}
		if q.size < q.limit {
			q.insert(item)
			q.mu.Unlock()
			queueNotify(q.readable)
			return nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.writable:
		}
	}
}

func (q *pendingQueue) insert(item pending) {
	if q.size == len(q.buffer) {
		q.grow()
	}
	q.buffer[(q.head+q.size)%len(q.buffer)] = item
	q.size++
}

func (q *pendingQueue) grow() {
	capacity := q.limit
	if len(q.buffer) <= q.limit/2 {
		capacity = max(1, 2*len(q.buffer))
	}
	buffer := make([]pending, capacity)
	for i := 0; i < q.size; i++ {
		buffer[i] = q.buffer[(q.head+i)%len(q.buffer)]
	}
	q.buffer = buffer
	q.head = 0
}

func (q *pendingQueue) pop(ctx context.Context) (pending, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return pending{}, false, err
		}
		q.mu.Lock()
		if q.size > 0 {
			item := q.remove()
			q.mu.Unlock()
			queueNotify(q.writable)
			return item, true, nil
		}
		if q.closed {
			q.mu.Unlock()
			return pending{}, false, nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return pending{}, false, ctx.Err()
		case <-q.readable:
		}
	}
}

func (q *pendingQueue) remove() pending {
	item := q.buffer[q.head]
	q.buffer[q.head] = pending{}
	q.head = (q.head + 1) % len(q.buffer)
	q.size--
	return item
}

func (q *pendingQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	queueNotify(q.readable)
	queueNotify(q.writable)
}
