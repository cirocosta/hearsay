package reconcile

import (
	"context"
	"os"
	"runtime"
	"testing"
	"unsafe"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
)

// TestWindowQueueLiveMemory is an opt-in ownership diagnostic, not a complete
// session peak measurement. Immutable trees predate the baseline; only actual
// queued scope views, detached fans, and queue storage remain at the barrier.
func TestWindowQueueLiveMemory(t *testing.T) {
	if os.Getenv("HEARSAY_WINDOW_MEMORY") != "1" {
		t.Skip("set HEARSAY_WINDOW_MEMORY=1 for the untimed live-heap diagnostic")
	}
	t.Logf("layout pending=%d scope=%d child=%d listing-child=%d reaction=%d",
		unsafe.Sizeof(pending{}), unsafe.Sizeof(tree.Scope[[]byte]{}),
		unsafe.Sizeof(tree.Child[[]byte]{}), unsafe.Sizeof(protocol.Child{}),
		unsafe.Sizeof(reaction{}))
	for _, count := range []int{1000, 10_000, 100_000} {
		fixture := reconciliationFixture(count, 0, 32, false)
		for _, budget := range []uint64{0, 16 << 20, 512 << 20} {
			measureWindowQueueMemory(t, fixture.left, budget)
		}
		runtime.KeepAlive(fixture)
	}
}

func measureWindowQueueMemory(t *testing.T, snapshot tree.Tree[[]byte], budget uint64) {
	t.Helper()
	runtime.GC()
	var before, held runtime.MemStats
	runtime.ReadMemStats(&before)
	e := &engine{
		ctx:     context.Background(),
		options: Options{MemoryBudget: budget},
		role:    protocol.Initiator,
	}
	e.initQueues()
	var items, fanBytes, ringBytes uint64
	for depth := 0; depth <= 32; depth += 2 {
		q := &e.queues[depth+1]
		var previous tree.Path
		seen := false
		used := uint64(0)
		for version := range snapshot.All() {
			path := tree.Address(version)
			if seen && sameMemoryPrefix(path, previous, depth) {
				continue
			}
			scope, err := snapshot.Scope(path[:depth])
			if err != nil {
				t.Fatal(err)
			}
			fan := scope.Children()
			if err := q.push(context.Background(), pending{scope: scope, fan: fan}); err != nil {
				t.Fatal(err)
			}
			fanBytes += uint64(cap(fan)) * uint64(unsafe.Sizeof(tree.Child[[]byte]{}))
			items++
			previous, seen = path, true
			used++
			if used == e.windowGranted {
				break
			}
		}
		if q.direct != nil {
			ringBytes += uint64(cap(q.direct)) * uint64(unsafe.Sizeof(pending{}))
		} else {
			ringBytes += uint64(cap(q.lazy.buffer)) * uint64(unsafe.Sizeof(pending{}))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&held)
	t.Logf("N=%d budget=%d grant=%d queued=%d fan-B=%d allocated-slot-B=%d live-heap-delta-B=%d",
		snapshot.Len(), budget, e.windowGranted, items, fanBytes, ringBytes,
		int64(held.HeapAlloc)-int64(before.HeapAlloc))
	for i := range e.queues {
		q := &e.queues[i]
		if !q.initialized() {
			continue
		}
		q.close()
		for {
			_, ok, err := q.pop(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
		}
		if q.lazy != nil {
			for _, item := range q.lazy.buffer {
				if item.fan != nil || item.scope.Len() != 0 {
					t.Fatal("drained queue retained a pending scope")
				}
			}
		}
	}
	runtime.GC()
	var drained runtime.MemStats
	runtime.ReadMemStats(&drained)
	t.Logf("N=%d budget=%d drained-with-slot-highwater-live-B=%d", snapshot.Len(), budget,
		int64(drained.HeapAlloc)-int64(before.HeapAlloc))
	runtime.KeepAlive(e)
	runtime.KeepAlive(snapshot)
}

func sameMemoryPrefix(a, b tree.Path, depth int) bool {
	for i := 0; i < depth; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
