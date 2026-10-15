package reconcile

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var errQueueClosed = errors.New("reconcile: pending queue is closed")

// pendingHandle keeps the minimum window on the native channel path. Wider
// windows reserve ring storage only as work arrives. Handles live inline in
// the engine's depth array, without one wrapper allocation per queue.
type pendingHandle struct {
	direct chan pending
	lazy   *pendingQueue
}

func newPendingHandle(limit int, credit ...uint64) pendingHandle {
	if limit <= 1 {
		return pendingHandle{direct: make(chan pending, 1)}
	}
	return pendingHandle{lazy: newPendingQueue(limit, credit...)}
}

func (q *pendingHandle) initialized() bool { return q.direct != nil || q.lazy != nil }

func (q *pendingHandle) push(ctx context.Context, item pending) error {
	return q.pushMeasured(ctx, item, nil)
}

func (q *pendingHandle) pushMeasured(
	ctx context.Context,
	item pending,
	stalls *atomic.Uint64,
) error {
	if q.direct == nil {
		return q.lazy.pushMeasured(ctx, item, stalls)
	}
	if stalls != nil && len(q.direct) == cap(q.direct) {
		stalls.Add(1)
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

// pendingQueue is a bounded single-producer, single-consumer queue. With byte
// credit, it charges allocated slot capacity plus detached queued fan capacity.
// A single item can exceed the share only when empty. Slot high-water remains
// charged after pop; admission can replace an excessive ring with a smaller one.
// During replacement old+new arrays cost at most two shares plus
// that one fan. This is a reachable-array envelope, excluding allocator rounding,
// GC headroom, shared opening nodes, and separately bounded reply/run custody.
// Readiness tokens coalesce and allocate no memory on individual operations.
type pendingQueue struct {
	mu                  sync.Mutex
	buffer              []pending
	head, size, limit   int
	closed              bool
	readable, writable  chan struct{}
	byteLimit, fanBytes uint64
}

func newPendingQueue(limit int, credit ...uint64) *pendingQueue {
	q := &pendingQueue{
		limit:    max(1, limit),
		readable: make(chan struct{}, 1),
		writable: make(chan struct{}, 1),
	}
	if len(credit) > 0 {
		q.byteLimit = credit[0]
	}
	return q
}

func queueNotify(ready chan struct{}) {
	select {
	case ready <- struct{}{}:
	default:
	}
}

func (q *pendingQueue) push(ctx context.Context, item pending) error {
	return q.pushMeasured(ctx, item, nil)
}

func (q *pendingQueue) pushMeasured(
	ctx context.Context,
	item pending,
	stalls *atomic.Uint64,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := q.tryPush(item, stalls)
		stalls = nil
		if done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.writable:
		}
	}
}

func (q *pendingQueue) tryPush(item pending, stalls *atomic.Uint64) (bool, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false, errQueueClosed
	}
	capacity, admitted := q.admissionCapacity(item)
	if q.size == q.limit || !admitted {
		if stalls != nil {
			stalls.Add(1)
		}
		q.mu.Unlock()
		return false, nil
	}
	q.insert(item, capacity)
	q.mu.Unlock()
	queueNotify(q.readable)
	return true, nil
}

func (q *pendingQueue) insert(item pending, capacity int) {
	if capacity != len(q.buffer) {
		q.grow(capacity)
	}
	q.buffer[(q.head+q.size)%len(q.buffer)] = item
	q.size++
	q.fanBytes += pendingFanBytes(item)
}

func (q *pendingQueue) growthCapacity() int {
	capacity := q.limit
	if len(q.buffer) <= q.limit/2 {
		capacity = max(1, 2*len(q.buffer))
	}
	return capacity
}

// admissionCapacity charges the ring's retained high-water capacity, even after
// pop. The one-item exception preserves progress when a fan exceeds its share.
// During resize both arrays remain reachable, adding at most one extra share.
func (q *pendingQueue) admissionCapacity(item pending) (int, bool) {
	capacity := len(q.buffer)
	if q.size == capacity {
		capacity = q.growthCapacity()
	}
	if q.byteLimit == 0 || q.size == 0 {
		return capacity, true
	}
	fan := pendingFanBytes(item)
	if fan > q.byteLimit || q.fanBytes > q.byteLimit-fan {
		return 0, false
	}
	slots := (q.byteLimit - q.fanBytes - fan) / pendingSlotBytes
	if uint64(len(q.buffer)) > slots {
		capacity = len(q.buffer) / 2
		if capacity <= q.size {
			return 0, false
		}
		// a cheap wave can fill the share with inline slots. Recover fan
		// credit only with a geometric shrink that can amortize its copy.
		capacity = min(capacity, int(slots))
	}
	if uint64(capacity) > slots {
		// trimming growth to one affordable extra slot would repeatedly
		// copy a nearly full ring. Wait for a pop and reuse existing slots.
		return 0, false
	}
	return capacity, capacity > q.size
}

func (q *pendingQueue) grow(capacity int) {
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
	q.fanBytes -= pendingFanBytes(item)
	return item
}

func (q *pendingQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	queueNotify(q.readable)
	queueNotify(q.writable)
}
