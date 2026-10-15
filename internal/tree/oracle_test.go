package tree_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/tree"
)

// modelReplica tracks observed send identities independently of causal clocks.
// An observed but absent send has been removed. No timestamp comparison enters
// the model's survivor rule.
type modelReplica struct {
	known map[int]bool
	live  map[int]bool
}

func emptyModel() modelReplica {
	return modelReplica{known: make(map[int]bool), live: make(map[int]bool)}
}

func modelJoin(a, b modelReplica) modelReplica {
	out := emptyModel()
	for id := range a.known {
		out.known[id] = true
	}
	for id := range b.known {
		out.known[id] = true
	}
	for id := range a.live {
		if b.live[id] || !b.known[id] {
			out.live[id] = true
		}
	}
	for id := range b.live {
		if a.live[id] || !a.known[id] {
			out.live[id] = true
		}
	}
	return out
}

func assertModel(
	t *testing.T,
	got tree.Tree[int],
	want modelReplica,
	versions map[int]before.Version,
) {
	t.Helper()
	if got.Len() != len(want.live) {
		t.Fatalf("live count: got %d want %d", got.Len(), len(want.live))
	}
	seen := make(map[int]bool)
	for v, id := range got.All() {
		if !want.live[id] || seen[id] {
			t.Fatalf("unexpected or duplicate live send %d", id)
		}
		if !v.Equal(versions[id]) {
			t.Fatalf("send %d changed version", id)
		}
		if !v.LessEqual(got.Latest()) {
			t.Fatalf("live send %d lies outside history", id)
		}
		seen[id] = true
	}
	for id := range want.known {
		value, ok := got.Get(versions[id])
		if ok != want.live[id] || ok && value != id {
			t.Fatalf("lookup send %d: got (%d,%t), want presence %t", id, value, ok, want.live[id])
		}
	}
}

func TestIndependentObservedSetOracle(t *testing.T) {
	for seed := uint64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			const peers = 7
			rng := rand.New(rand.NewPCG(seed, seed*31))
			clocks := []*before.Clock{before.Seed()}
			for len(clocks) < peers {
				clocks = append(clocks, clocks[rng.IntN(len(clocks))].Fork())
			}
			replicas := make([]tree.Tree[int], peers)
			models := make([]modelReplica, peers)
			for i := range models {
				models[i] = emptyModel()
			}
			versions := make(map[int]before.Version)
			type held struct {
				value tree.Tree[int]
				model modelReplica
			}
			var snapshots []held
			nextID := 0
			for step := 0; step < 1600; step++ {
				i, j := rng.IntN(peers), rng.IntN(peers)
				switch rng.IntN(10) {
				case 0, 1, 2, 3:
					clocks[i].Absorb(replicas[i].Latest())
					v := clocks[i].Tick()
					versions[nextID] = v
					replicas[i] = replicas[i].Insert(v, nextID)
					models[i] = modelJoin(models[i], emptyModel())
					models[i].known[nextID], models[i].live[nextID] = true, true
					nextID++
				case 4, 5:
					if len(models[i].live) == 0 {
						break
					}
					// Select by stable send identity so the corpus is reproducible.
					start := rng.IntN(nextID)
					id := start
					for !models[i].live[id] {
						id = (id + 1) % nextID
					}
					clocks[i].Absorb(replicas[i].Latest())
					deletion := clocks[i].Tick()
					replicas[i] = replicas[i].Forget(versions[id], deletion)
					models[i] = modelJoin(models[i], emptyModel())
					delete(models[i].live, id)
				default:
					// One-way learning allows stale and partitioned peers to persist.
					replicas[i] = replicas[i].Join(replicas[j])
					models[i] = modelJoin(models[i], models[j])
				}
				assertModel(t, replicas[i], models[i], versions)
				if step%100 == 0 {
					snapshots = append(
						snapshots,
						held{replicas[i], modelJoin(models[i], emptyModel())},
					)
					a, b, c := replicas[i], replicas[j], replicas[rng.IntN(peers)]
					if a.Join(a).Hash() != a.Hash() || !a.Join(a).Latest().Equal(a.Latest()) {
						t.Fatal("join is not idempotent")
					}
					ab, ba := a.Join(b), b.Join(a)
					if ab.Hash() != ba.Hash() || !ab.Latest().Equal(ba.Latest()) {
						t.Fatal("join is not commutative")
					}
					left, right := ab.Join(c), a.Join(b.Join(c))
					if left.Hash() != right.Hash() || !left.Latest().Equal(right.Latest()) {
						t.Fatal("join is not associative")
					}
				}
			}
			// A chain carries deletions transitively before broadcasting the result.
			for i := 1; i < peers; i++ {
				replicas[i] = replicas[i].Join(replicas[i-1])
				models[i] = modelJoin(models[i], models[i-1])
			}
			for i := 0; i < peers; i++ {
				replicas[i] = replicas[i].Join(replicas[peers-1])
				models[i] = modelJoin(models[i], models[peers-1])
				assertModel(t, replicas[i], models[i], versions)
				if replicas[i].Hash() != replicas[peers-1].Hash() {
					t.Fatal("reunion did not converge")
				}
			}
			for _, snapshot := range snapshots {
				assertModel(t, snapshot.value, snapshot.model, versions)
			}
		})
	}
}

func TestTransitiveDeletionPreservesConcurrentUnseenSend(t *testing.T) {
	a := before.Seed()
	b, c := a.Fork(), a.Fork()
	original := a.Tick()
	initial := tree.Tree[int]{}.Insert(original, 1)
	left, stale := initial, initial
	a.Absorb(left.Latest())
	left = left.Forget(original, a.Tick())
	// This author still knows the old message, but has never seen its deletion.
	b.Absorb(stale.Latest())
	concurrent := b.Tick()
	stale = stale.Insert(concurrent, 2)
	bridge := tree.Tree[int]{}.Join(left)
	c.Absorb(bridge.Latest())
	later := c.Tick()
	bridge = bridge.Insert(later, 3)
	for _, result := range []tree.Tree[int]{bridge.Join(stale), stale.Join(bridge)} {
		if _, ok := result.Get(original); ok {
			t.Fatal("transitively learned deletion resurrected")
		}
		if _, ok := result.Get(concurrent); !ok {
			t.Fatal("unseen concurrent send was mistaken for a deletion")
		}
		if _, ok := result.Get(later); !ok || result.Len() != 2 {
			t.Fatal("bridge send did not survive")
		}
	}
}
