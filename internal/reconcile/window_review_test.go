package reconcile

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/cirocosta/hearsay/internal/tree"
)

// TestByteCreditAdmissionOracle checks independent necessary admission bounds
// and mandatory admission when existing slots fit. Geometric growth may apply
// conservative backpressure even when a smaller replacement would fit.
func TestByteCreditAdmissionOracle(t *testing.T) {
	for seed := uint64(1); seed <= 16; seed++ {
		rng := rand.New(rand.NewPCG(seed, seed*17))
		for round := 0; round < 12; round++ {
			share := pendingSlotBytes + uint64(rng.IntN(40_000))
			limit := 1 + rng.IntN(int(share/pendingSlotBytes))
			q := newPendingQueue(limit, share)
			var model []pending
			var fanBytes uint64
			next := uint64(0)
			for step := 0; step < 1600; step++ {
				oldCapacity := len(q.buffer)
				if len(model) == 0 || rng.IntN(5) < 3 {
					fans := []int{0, 0, 0, 1, 1, 2, 3, 16, 64, 256}[rng.IntN(10)]
					item := queueItem(next)
					next++
					item.fan = make([]tree.Child[[]byte], fans)
					itemBytes := uint64(fans) * pendingChildBytes
					possible := len(model) < limit &&
						(len(model) == 0 || (uint64(len(model)+1)*pendingSlotBytes+fanBytes+itemBytes <= share))
					mustAdmit := len(model) < limit && (len(model) == 0 ||
						(len(model) < oldCapacity && uint64(oldCapacity)*pendingSlotBytes+fanBytes+itemBytes <= share))
					got, err := q.tryPush(item, nil)
					if err != nil || (got && !possible) || (!got && mustAdmit) {
						t.Fatalf(
							"seed%d round%d step%d share%d size%d cap%d fan%d: admitted%v possible%v mandatory%v err%v",
							seed,
							round,
							step,
							share,
							len(model),
							oldCapacity,
							itemBytes,
							got,
							possible,
							mustAdmit,
							err,
						)
					}
					if got {
						newCapacity := len(q.buffer)
						if newCapacity > oldCapacity &&
							newCapacity != min(limit, max(1, 2*oldCapacity)) {
							t.Fatal("non-geometric growth")
						}
						if newCapacity < oldCapacity &&
							(newCapacity > oldCapacity/2 || newCapacity <= len(model)) {
							t.Fatal(
								"shrink did not halve or preserve live items plus the incoming slot",
							)
						}
						model = append(model, item)
						fanBytes += itemBytes
					}
				} else {
					item, ok, err := q.pop(context.Background())
					if err != nil || !ok || queueIndex(item) != queueIndex(model[0]) {
						t.Fatalf("FIFO seed%d round%d step%d: %v", seed, round, step, err)
					}
					fanBytes -= uint64(cap(model[0].fan)) * pendingChildBytes
					model[0] = pending{}
					model = model[1:]
				}
				if q.size != len(model) || q.fanBytes != fanBytes || len(q.buffer) > limit {
					t.Fatal("ledger/count mismatch")
				}
				owned := uint64(len(q.buffer))*pendingSlotBytes + fanBytes
				extra := uint64(0)
				if len(model) == 1 {
					extra = uint64(cap(model[0].fan)) * pendingChildBytes
				}
				if owned > share+extra {
					t.Fatalf("live envelope exceeded: %d>%d+%d", owned, share, extra)
				}
				if oldCapacity != len(q.buffer) &&
					owned+uint64(oldCapacity)*pendingSlotBytes > 2*share+extra {
					t.Fatal("resize reachability envelope exceeded")
				}
				for slot, item := range q.buffer {
					occupied := false
					for offset := 0; offset < q.size; offset++ {
						if (q.head+offset)%len(q.buffer) == slot {
							occupied = true
							break
						}
					}
					if !occupied && (item.fan != nil || item.scope.Depth() != 0) {
						t.Fatal("unused ring slot retains an item")
					}
				}
			}
			q.close()
			for _, want := range model {
				got, ok, err := q.pop(context.Background())
				if err != nil || !ok || queueIndex(got) != queueIndex(want) {
					t.Fatal("close drain changed FIFO")
				}
			}
			if _, ok, err := q.pop(context.Background()); err != nil || ok || q.fanBytes != 0 {
				t.Fatal("closed queue retained fan ownership")
			}
		}
	}
}
