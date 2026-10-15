package reconcile

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"testing"
	"time"
)

func queueItem(index uint64) pending {
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], index)
	return pending{scope: emptyScope(prefix[:])}
}
func queueIndex(item pending) uint64 { return binary.BigEndian.Uint64(item.scope.Prefix()) }

func TestPendingHandleSelectsStorage(t *testing.T) {
	ctx := context.Background()
	for _, limit := range []int{0, 1, 2, 17, 4096} {
		q := newPendingHandle(limit)
		if !q.initialized() {
			t.Fatal("new queue has no storage path")
		}
		if limit <= 1 {
			if cap(q.direct) != 1 || q.lazy != nil {
				t.Fatal("minimum window bypassed native channel")
			}
		} else if q.direct != nil || q.lazy.limit != limit || len(q.lazy.buffer) != 0 {
			t.Fatal("wider window changed its limit or eagerly reserved slots")
		}
		if err := q.push(ctx, queueItem(9)); err != nil {
			t.Fatal(err)
		}
		q.close()
		item, ok, err := q.pop(ctx)
		if err != nil || !ok || queueIndex(item) != 9 {
			t.Fatalf("drain: %v %t", err, ok)
		}
		if _, ok, err := q.pop(ctx); err != nil || ok {
			t.Fatalf("closed: %v %t", err, ok)
		}
	}
}

func TestPendingHandleCancellation(t *testing.T) {
	for _, limit := range []int{1, 17} {
		q := newPendingHandle(limit)
		ctx, cancel := context.WithCancel(context.Background())
		finished := make(chan error, 1)
		go func() { _, _, err := q.pop(ctx); finished <- err }()
		cancel()
		if err := <-finished; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		q.close()
	}
}

func TestPendingQueueLazyStorage(t *testing.T) {
	q := newPendingQueue(1 << 20)
	ctx := context.Background()
	if len(q.buffer) != 0 {
		t.Fatal("queue eagerly allocated its window")
	}
	if err := q.push(ctx, queueItem(1)); err != nil {
		t.Fatal(err)
	}
	if len(q.buffer) != 1 {
		t.Fatalf("first item reserved %d slots", len(q.buffer))
	}
	q.close()
	got, ok, err := q.pop(ctx)
	if err != nil || !ok || queueIndex(got) != 1 {
		t.Fatalf("drain: %v %t", err, ok)
	}
	if _, ok, err := q.pop(ctx); err != nil || ok {
		t.Fatalf("closed drain: %v %t", err, ok)
	}
	if err := q.push(ctx, queueItem(2)); !errors.Is(err, errQueueClosed) {
		t.Fatal(err)
	}
	if q.buffer[0].scope.Depth() != 0 || q.buffer[0].fan != nil {
		t.Fatal("popped item retained references")
	}
}

func TestPendingQueueWrapAndGrowth(t *testing.T) {
	q := newPendingQueue(17)
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(9, 41))
	var sent, received uint64
	for range 10_000 {
		if q.size == 0 || (q.size < q.limit && rng.IntN(2) == 0) {
			if err := q.push(ctx, queueItem(sent)); err != nil {
				t.Fatal(err)
			}
			sent++
		} else {
			got, ok, err := q.pop(ctx)
			if err != nil || !ok || queueIndex(got) != received {
				t.Fatalf("order %d: %v %t", received, err, ok)
			}
			received++
		}
		if len(q.buffer) > 17 {
			t.Fatal("queue exceeded its fixed window")
		}
	}
	q.close()
	for received < sent {
		got, ok, err := q.pop(ctx)
		if err != nil || !ok || queueIndex(got) != received {
			t.Fatal("drain order")
		}
		received++
	}
}

func TestPendingQueueConcurrentOneSlot(t *testing.T) {
	q := newPendingQueue(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		defer q.close()
		for i := uint64(0); i < 10_000; i++ {
			if err := q.push(ctx, queueItem(i)); err != nil {
				finished <- err
				return
			}
		}
		finished <- nil
	}()
	for i := uint64(0); i < 10_000; i++ {
		got, ok, err := q.pop(ctx)
		if err != nil || !ok || queueIndex(got) != i {
			t.Fatalf("item %d: %v %t", i, err, ok)
		}
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.pop(ctx); err != nil || ok {
		t.Fatalf("closed: %v %t", err, ok)
	}
}

func TestPendingQueueCancellationAndClose(t *testing.T) {
	for _, writer := range []bool{false, true} {
		q := newPendingQueue(1)
		if writer {
			if err := q.push(context.Background(), queueItem(1)); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			if writer {
				done <- q.push(ctx, queueItem(2))
				return
			}
			_, _, err := q.pop(ctx)
			done <- err
		}()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		go func() {
			if writer {
				done <- q.push(ctx, queueItem(2))
				return
			}
			_, _, err := q.pop(ctx)
			done <- err
		}()
		q.close()
		err := <-done
		cancel()
		if writer && !errors.Is(err, errQueueClosed) {
			t.Fatal(err)
		}
		if !writer && err != nil {
			t.Fatal(err)
		}
	}
}
