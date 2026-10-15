package hearsay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/testlink"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

type networkCase struct {
	name                   string
	shared, delta, payload int
	redactions, sparse     bool
}
type networkFixture struct {
	left, right tree.Tree[[]byte]
	want        tree.Hash
	count       int
}
type networkProfile struct {
	name   string
	config testlink.Config
}

func TestCalibratedNetworkSession(t *testing.T) {
	fixture := makeNetworkFixture(t, networkCase{shared: 32, delta: 8, payload: 64})
	for _, budget := range []uint64{0, DefaultSyncMemoryBudget} {
		party := before.SeedParty()
		other := party.Fork()
		a := networkActor(t, party, fixture.left, budget)
		b := networkActor(t, other, fixture.right, budget)
		left, right, err := testlink.Pair(
			testlink.Config{
				RTT:            2 * time.Millisecond,
				BytesPerSecond: 125_000,
				StreamWindow:   64 << 10,
				PacketSize:     4096,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		leftManaged, rightManaged := link.Manage(left), link.Manage(right)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		cancel()
		if leftResult.err != nil || rightErr != nil {
			t.Fatalf("session: %v %v", leftResult.err, rightErr)
		}
		if a.Snapshot().tree.Hash() != fixture.want || b.Snapshot().tree.Hash() != fixture.want {
			t.Fatal("network session differs from Join oracle")
		}
		if leftResult.result.Stats.BytesSent != rightResult.Stats.BytesReceived ||
			rightResult.Stats.BytesSent != leftResult.result.Stats.BytesReceived {
			t.Fatal("data byte conservation failed")
		}
		if leftResult.result.Elapsed < 2*time.Millisecond ||
			rightResult.Elapsed < 2*time.Millisecond {
			t.Fatal("full-session latency omitted modeled propagation")
		}
		if left.Stats().QueuedBytes != 0 || right.Stats().QueuedBytes != 0 {
			t.Fatal("model retained wire bytes after worker shutdown")
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func makeNetworkFixture(b testing.TB, c networkCase) networkFixture {
	b.Helper()
	clock := before.Seed()
	payload, err := BytesCodec().encode(make([]byte, c.payload))
	if err != nil {
		b.Fatal(err)
	}
	var shared tree.Tree[[]byte]
	versions := make([]Version, 0, 2*c.delta)
	for range c.shared {
		v := clock.Tick()
		shared = shared.Insert(v, payload)
		if len(versions) < 2*c.delta {
			versions = append(versions, v)
		}
	}
	rightClock := clock.Fork()
	left, right := shared, shared
	for i := 0; i < c.delta; i++ {
		if c.redactions {
			left = left.Forget(versions[i], clock.Tick())
			right = right.Forget(versions[c.delta+i], rightClock.Tick())
		} else {
			left = left.Insert(clock.Tick(), payload)
			if !c.sparse {
				right = right.Insert(rightClock.Tick(), payload)
			}
		}
	}
	want := left.Join(right)
	return networkFixture{left: left, right: right, want: want.Hash(), count: want.Len()}
}

// BenchmarkSessionNetwork includes negotiation, reconciliation, and confirmed
// completion. Fixture and actor construction are excluded. Every operation
// closes the model link and joins its delivery workers before timing ends.
func BenchmarkSessionNetwork(b *testing.B) {
	profiles := []networkProfile{
		{name: "CPU"},
		{
			name:   "LAN1ms100G",
			config: testlink.Config{RTT: time.Millisecond, BytesPerSecond: 12_500_000_000},
		},
		{
			name:   "WAN100ms1G",
			config: testlink.Config{RTT: 100 * time.Millisecond, BytesPerSecond: 125_000_000},
		},
		{
			name:   "WAN20ms100M",
			config: testlink.Config{RTT: 20 * time.Millisecond, BytesPerSecond: 12_500_000},
		},
	}
	cases := []networkCase{
		{name: "divergent100k", shared: 100_000, delta: 10_000, payload: 1024},
		{name: "sparse100k", shared: 100_000, delta: 1, payload: 1024, sparse: true},
		{name: "disjoint10k", delta: 10_000, payload: 1024},
		{name: "redactions100k", shared: 100_000, delta: 10_000, payload: 1024, redactions: true},
		{name: "bulk10k", shared: 10_000, delta: 1000, payload: 16 << 10},
		{name: "small100k", shared: 100_000, delta: 10_000, payload: 32},
		{name: "equal100k", shared: 100_000, payload: 1024},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			fixture := makeNetworkFixture(b, c)
			for _, profile := range profiles {
				var budgets []uint64
				for _, budget := range []uint64{0, 1 << 20, 16 << 20, DefaultSyncMemoryBudget} {
					if includeNetworkCase(c, profile, budget) {
						budgets = append(budgets, budget)
					}
				}
				if len(budgets) == 0 {
					continue
				}
				b.Run(profile.name, func(b *testing.B) {
					for _, budget := range budgets {
						b.Run(
							fmt.Sprintf("budget%d", budget),
							func(b *testing.B) { benchmarkNetworkFixture(b, fixture, profile.config, budget) },
						)
					}
				})
			}
		})
	}
}

func includeNetworkCase(c networkCase, p networkProfile, budget uint64) bool {
	if c.name == "divergent100k" {
		return true
	}
	if budget != 0 && budget != DefaultSyncMemoryBudget {
		return false
	}
	if c.name == "equal100k" {
		return budget == DefaultSyncMemoryBudget && (p.name == "CPU" || p.name == "WAN100ms1G")
	}
	if c.sparse {
		return p.name == "CPU" || p.name == "WAN100ms1G"
	}
	return p.name == "WAN100ms1G"
}

func networkActor(
	b testing.TB,
	party *before.Party,
	t tree.Tree[[]byte],
	budget uint64,
) *Rumors[[]byte] {
	b.Helper()
	p := newPeer(Network{1}, party, t, BytesCodec())
	settings := p.SynchronizationSettings()
	settings.SyncMemoryBudget = budget
	if _, err := p.WithSynchronizationSettings(settings); err != nil {
		b.Fatal(err)
	}
	r, err := p.IntoRumors()
	if err != nil {
		b.Fatal(err)
	}
	return r
}

func benchmarkNetworkFixture(
	b *testing.B,
	fixture networkFixture,
	config testlink.Config,
	budget uint64,
) {
	b.Helper()
	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()
	var wire, stalls, granted uint64
	var elapsed time.Duration
	var peakTransport uint64
	for range b.N {
		// each operation is an independent universe. Peers do not send new
		// events, and both exclusive parties are discarded after the session.
		party := before.SeedParty()
		other := party.Fork()
		a := networkActor(b, party, fixture.left, budget)
		c := networkActor(b, other, fixture.right, budget)
		left, right, err := testlink.Pair(config)
		if err != nil {
			b.Fatal(err)
		}
		leftManaged, rightManaged := link.Manage(left), link.Manage(right)
		ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
		type outcome struct {
			result Gossiped
			err    error
		}
		joined := make(chan outcome, 1)
		b.StartTimer()
		go func() { result, err := a.GossipOnce(ctx, leftManaged); joined <- outcome{result, err} }()
		rightResult, rightErr := c.GossipOnce(ctx, rightManaged)
		leftResult := <-joined
		_ = leftManaged.Close()
		_ = rightManaged.Close()
		b.StopTimer()
		cancel()
		if leftResult.err != nil || rightErr != nil {
			b.Fatalf("session: %v %v", leftResult.err, rightErr)
		}
		if a.Snapshot().tree.Hash() != fixture.want || c.Snapshot().tree.Hash() != fixture.want ||
			a.Snapshot().Len() != fixture.count ||
			c.Snapshot().Len() != fixture.count {
			b.Fatal("session differs from causal Join oracle")
		}
		wire += leftResult.result.Stats.BytesSent + rightResult.Stats.BytesSent
		stalls += leftResult.result.Stats.WindowStalls + rightResult.Stats.WindowStalls
		granted = max(
			granted,
			leftResult.result.Stats.WindowGranted,
			rightResult.Stats.WindowGranted,
		)
		elapsed += max(leftResult.result.Elapsed, rightResult.Elapsed)
		peakTransport = max(
			peakTransport,
			left.Stats().PeakQueuedBytes,
			right.Stats().PeakQueuedBytes,
		)
		if err := a.Close(); err != nil {
			b.Fatal(err)
		}
		if err := c.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(wire)/float64(b.N), "data-B/op")
	b.ReportMetric(float64(stalls)/float64(b.N), "window-stalls/op")
	b.ReportMetric(float64(granted), "window-granted")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "session-ns/op")
	b.ReportMetric(float64(peakTransport), "transport-peak-B")
}
