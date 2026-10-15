package hearsay

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/testlink"
	"github.com/cirocosta/hearsay/link"
)

// TestSessionTransientMemory is deliberately separate from timing benchmarks.
// The sampled heap includes result custody and transport; the final held-result
// baseline and transport ledger make those costs visible. Forced GC perturbs
// scheduling; the sample is not the natural peak or a hard ownership bound.
func TestSessionTransientMemory(t *testing.T) {
	if os.Getenv("HEARSAY_WINDOW_MEMORY") != "1" {
		t.Skip("set HEARSAY_WINDOW_MEMORY=1 for the untimed session heap diagnostic")
	}
	for _, count := range []int{1000, 10_000, 100_000} {
		fixture := makeNetworkFixture(t, networkCase{shared: count, delta: count / 10, payload: 32})
		for _, budget := range []uint64{0, 16 << 20} {
			measureSessionMemory(t, fixture, budget)
		}
		runtime.KeepAlive(fixture)
	}
}

func measureSessionMemory(t *testing.T, fixture networkFixture, budget uint64) {
	t.Helper()
	party := before.SeedParty()
	a := networkActor(t, party, fixture.left, budget)
	b := networkActor(t, party.Fork(), fixture.right, budget)
	left, right, err := testlink.Pair(testlink.Config{
		RTT: 2 * time.Millisecond, BytesPerSecond: 125_000_000, StreamWindow: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	leftManaged, rightManaged := link.Manage(left), link.Manage(right)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	runtime.GC()
	var initial runtime.MemStats
	runtime.ReadMemStats(&initial)
	var peak, samples atomic.Uint64
	peak.Store(initial.HeapAlloc)
	stop, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				runtime.GC()
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				peak.Store(max(peak.Load(), current.HeapAlloc))
				samples.Add(1)
			}
		}
	}()
	type outcome struct {
		result Gossiped
		err    error
	}
	joined := make(chan outcome, 1)
	go func() { result, err := a.GossipOnce(ctx, leftManaged); joined <- outcome{result, err} }()
	rightResult, rightErr := b.GossipOnce(ctx, rightManaged)
	leftResult := <-joined
	_ = leftManaged.Close()
	_ = rightManaged.Close()
	close(stop)
	<-sampled
	if leftResult.err != nil || rightErr != nil {
		t.Fatalf("instrumented session: %v %v", leftResult.err, rightErr)
	}
	if a.Snapshot().tree.Hash() != fixture.want || b.Snapshot().tree.Hash() != fixture.want {
		t.Fatal("instrumented session differs from Join oracle")
	}
	runtime.GC()
	var custody runtime.MemStats
	runtime.ReadMemStats(&custody)
	maximum := max(peak.Load(), custody.HeapAlloc)
	baseline := max(initial.HeapAlloc, custody.HeapAlloc)
	t.Logf(
		"N=%d budget=%d samples=%d initial-heap-B=%d sampled-increment-B=%d final-custody-increment-B=%d sampled-excess-over-both-baselines-B=%d transport-peak-B=%d window=%d stalls=%d",
		fixture.left.Len(),
		budget,
		samples.Load(),
		initial.HeapAlloc,
		int64(maximum)-int64(initial.HeapAlloc),
		int64(custody.HeapAlloc)-int64(initial.HeapAlloc),
		maximum-baseline,
		max(left.Stats().PeakQueuedBytes, right.Stats().PeakQueuedBytes),
		max(leftResult.result.Stats.WindowGranted, rightResult.Stats.WindowGranted),
		leftResult.result.Stats.WindowStalls+rightResult.Stats.WindowStalls,
	)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	a, b = nil, nil
	runtime.GC()
	var released runtime.MemStats
	runtime.ReadMemStats(&released)
	t.Logf("N=%d budget=%d after-result-release-increment-B=%d", fixture.left.Len(), budget,
		int64(released.HeapAlloc)-int64(initial.HeapAlloc))
	runtime.KeepAlive(fixture)
}
