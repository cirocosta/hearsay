package hearsay

import (
	"fmt"
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/tree"
)

type atomicBatchFixture struct {
	original tree.Tree[[]byte]
	party    *before.Party
	actions  []action
	want     tree.Tree[[]byte]
}

var atomicBatchResult tree.Tree[[]byte]

func makeAtomicBatchFixture(size, delta int, kind string) atomicBatchFixture {
	party := before.SeedParty()
	var original tree.Tree[[]byte]
	var version Version
	versions := make([]Version, size)
	payload := []byte{0x41, 1}
	for i := range size {
		version = version.Tick(party)
		versions[i] = version
		original = original.Insert(version, payload)
	}
	fixture := atomicBatchFixture{original: original, party: party, want: original}
	for i := range delta {
		insert := kind == "send" || kind == "mixed" && i%2 == 0
		next := action{insert: insert, payload: payload}
		if !insert {
			next.version = versions[i*size/delta]
		}
		fixture.actions = append(fixture.actions, next)
		version = version.Tick(party)
		if insert {
			fixture.want = fixture.want.Insert(version, payload)
		} else {
			fixture.want = fixture.want.Forget(next.version, version)
		}
	}
	return fixture
}

// BenchmarkApplyBatch isolates immutable tree mutation from payload admission,
// locks, and publication. Fixtures and the sequential survivor oracle are cold
// setup; each operation starts from the same immutable tree and causal history.
func BenchmarkApplyBatch(b *testing.B) {
	for _, size := range []int{0, 1000, 10000} {
		for _, delta := range []int{1, 2, 4, 7, 8, 64, 512} {
			for _, kind := range []string{"send", "redact", "mixed"} {
				if size == 0 && kind != "send" {
					continue
				}
				name := fmt.Sprintf("%s/N%d/D%d", kind, size, delta)
				b.Run(name, func(b *testing.B) {
					fixture := makeAtomicBatchFixture(size, delta, kind)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						atomicBatchResult = applyBatch(
							fixture.original,
							fixture.party,
							fixture.actions,
						)
					}
					b.StopTimer()
					if atomicBatchResult.Hash() != fixture.want.Hash() ||
						atomicBatchResult.Len() != fixture.want.Len() ||
						!atomicBatchResult.Latest().Equal(fixture.want.Latest()) {
						b.Fatal("atomic batch differs from survivor oracle")
					}
					b.ReportMetric(float64(delta), "actions/op")
				})
			}
		}
	}
}

// BenchmarkAtomicBatchAPI includes admission, locking, and one publication.
// Snapshot resets are outside the timer in bounded groups; no observers or
// sessions see the benchmark's repeated, isolated starting histories.
func BenchmarkAtomicBatchAPI(b *testing.B) {
	for _, size := range []int{0, 10000} {
		for _, delta := range []int{1, 64, 512} {
			for _, kind := range []string{"send", "redact", "mixed"} {
				if size == 0 && kind != "send" {
					continue
				}
				b.Run(
					fmt.Sprintf("%s/N%d/D%d", kind, size, delta),
					func(b *testing.B) { benchmarkAtomicAPI(b, size, delta, kind) },
				)
			}
		}
	}
}

func benchmarkAtomicAPI(b *testing.B, size, delta int, kind string) {
	b.Helper()
	fixture := makeAtomicBatchFixture(size, delta, kind)
	const width = 16
	peers := make([]*Replica[[]byte], width)
	for i := range peers {
		peer := newPeer(Network{1}, before.SeedParty(), fixture.original, BytesCodec())
		var err error
		peers[i], err = peer.IntoReplica()
		if err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		for _, peer := range peers {
			if err := peer.Close(); err != nil {
				b.Error(err)
			}
		}
	})
	payloads := make([][]byte, delta)
	versions := make([]Version, delta)
	for i, a := range fixture.actions {
		payloads[i] = []byte{1}
		versions[i] = a.version
	}
	b.ReportAllocs()
	b.ResetTimer()
	for completed := 0; completed < b.N; {
		b.StopTimer()
		count := min(width, b.N-completed)
		for _, peer := range peers[:count] {
			peer.state.current.Store(
				&published{tree: fixture.original, changed: make(chan struct{})},
			)
		}
		b.StartTimer()
		for _, peer := range peers[:count] {
			if err := runAtomicAPI(peer, payloads, versions, fixture.actions, kind); err != nil {
				b.Fatal(err)
			}
		}
		completed += count
	}
	b.StopTimer()
	got := peers[0].Snapshot()
	if got.Hash() != [24]byte(fixture.want.Hash()) || !got.Latest().Equal(fixture.want.Latest()) {
		b.Fatal("public atomic batch differs from oracle")
	}
}

func runAtomicAPI(
	replica *Replica[[]byte],
	payloads [][]byte,
	versions []Version,
	actions []action,
	kind string,
) error {
	switch kind {
	case "send":
		return replica.SendAll(payloads)
	case "redact":
		return replica.RedactAll(versions)
	default:
		return replica.Batch(func(batch *Batch[[]byte]) error {
			for _, a := range actions {
				var err error
				if a.insert {
					err = batch.Send([]byte{1})
				} else {
					err = batch.Redact(a.version)
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
}
