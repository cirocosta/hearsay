package reconcile

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

func waitForStall(t *testing.T, stalls *atomic.Uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for stalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("full-window arrival was not observed")
		}
		runtime.Gosched()
	}
}

func TestWindowStallCountsArrivalOnce(t *testing.T) {
	for _, width := range []int{1, 4} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		q := newPendingHandle(width)
		var stalls atomic.Uint64
		for i := 0; i < width; i++ {
			if err := q.pushMeasured(ctx, queueItem(uint64(i)), &stalls); err != nil {
				t.Fatal(err)
			}
		}
		if stalls.Load() != 0 {
			t.Fatal("available capacity reported a stall")
		}
		finished := make(chan error, 1)
		go func() { finished <- q.pushMeasured(ctx, queueItem(uint64(width)), &stalls) }()
		waitForStall(t, &stalls)
		if q.lazy != nil {
			// scheduling wakeups without newly available capacity must not count
			// the same arriving push more than once.
			for range 20 {
				queueNotify(q.lazy.writable)
				runtime.Gosched()
			}
		}
		if _, ok, err := q.pop(ctx); err != nil || !ok {
			t.Fatalf("release: %v %t", err, ok)
		}
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if stalls.Load() != 1 {
			t.Fatalf("one full-window arrival reported %d stalls", stalls.Load())
		}
		q.close()
		cancel()
	}
}

func TestEqualAndBootstrapWindowGrants(t *testing.T) {
	empty := tree.Tree[[]byte]{}
	a, b := runPair(t, empty, empty, 1, 0)
	if a.Stats.WindowGranted != 0 || b.Stats.WindowGranted != 0 || a.Stats.WindowStalls != 0 ||
		b.Stats.WindowStalls != 0 {
		t.Fatal("equal-frontier shortcut derived a window")
	}
	clock := before.Seed()
	full := empty.Insert(clock.Tick(), []byte{0xf6})
	a, b = runPair(t, empty, full, 1, 0)
	// upstream derives a floor window when unequal frontiers open a session,
	// including a one-sided bootstrap, even if no subtree ends up disputed.
	if a.Stats.WindowGranted != 1 || b.Stats.WindowGranted != 1 {
		t.Fatalf("bootstrap grants: %d %d", a.Stats.WindowGranted, b.Stats.WindowGranted)
	}
	if a.Stats.WindowStalls != 0 || b.Stats.WindowStalls != 0 {
		t.Fatal("one-sided bootstrap reported comparison pressure")
	}
}

func TestWindowGrantedReportsCapacity(t *testing.T) {
	clock := before.Seed()
	base := tree.Tree[[]byte]{}.Insert(clock.Tick(), []byte{0xf6})
	remote := base.Insert(clock.Tick(), []byte{0xf6})
	left, right := link.MemoryWithCapacity(1)
	defer func() { _ = left.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// use the unchanged current pricing formula to grant seven slots even
	// though this sparse difference cannot occupy seven in-flight scopes.
	const budget = 7 * 17 * (256*128 + 256)
	type outcome struct {
		result Result
		err    error
	}
	joined := make(chan outcome, 1)
	localGreeting, remoteGreeting := greeting(base, 0), greeting(remote, 0)
	go func() {
		result, err := Run(
			ctx,
			left,
			0,
			base,
			localGreeting,
			remoteGreeting,
			Options{MemoryBudget: budget},
		)
		joined <- outcome{result, err}
	}()
	b, err := Run(
		ctx,
		right,
		0,
		remote,
		remoteGreeting,
		localGreeting,
		Options{MemoryBudget: budget},
	)
	a := <-joined
	if err != nil || a.err != nil {
		t.Fatalf("session: %v %v", a.err, err)
	}
	if a.result.Stats.WindowGranted != 7 || b.Stats.WindowGranted != 7 {
		t.Fatalf("capacity grants: %d %d", a.result.Stats.WindowGranted, b.Stats.WindowGranted)
	}
}
