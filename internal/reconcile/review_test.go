package reconcile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

// reviewCollisionPair finds real version-derived paths sharing two bytes. This
// exercises overlapping alternating-height queues beyond ordinary root disputes.
func reviewCollisionPair(t *testing.T) (tree.Tree[[]byte], tree.Tree[[]byte]) {
	t.Helper()
	root := before.Seed()
	a, b := root, root.Fork()
	known := make(map[uint16]before.Version)
	var left, right tree.Tree[[]byte]
	for i := 0; i < 2400; i++ {
		v := a.Tick()
		p := tree.Address(v)
		key := uint16(p[0])<<8 | uint16(p[1])
		known[key] = v
		if i < 100 {
			left = left.Insert(v, []byte{0xf6})
		}
	}
	found := 0
	for i := 0; i < 2400; i++ {
		v := b.Tick()
		p := tree.Address(v)
		key := uint16(p[0])<<8 | uint16(p[1])
		if match, ok := known[key]; ok {
			left = left.Insert(match, []byte{0xf6})
			right = right.Insert(v, []byte{0xf6})
			found++
		}
		if i < 100 {
			right = right.Insert(v, []byte{0xf6})
		}
	}
	if found < 10 {
		t.Fatalf("insufficient deterministic collisions: %d", found)
	}
	return left, right
}

func TestReviewDeepDisputesAsymmetricTargets(t *testing.T) {
	a, b := reviewCollisionPair(t)
	want := a.Join(b)
	for _, targets := range [][2]uint64{{0, 4096}, {31, 0}, {128, 4096}} {
		t.Run(fmt.Sprint(targets), func(t *testing.T) {
			left, right := link.MemoryWithCapacity(1)
			t.Cleanup(func() { _ = left.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ga, gb := greeting(a, targets[0]), greeting(b, targets[1])
			type outcome struct {
				result Result
				err    error
			}
			result := make(chan outcome, 1)
			go func() { r, err := Run(ctx, left, 19, a, ga, gb, Options{MemoryBudget: 0}); result <- outcome{r, err} }()
			rb, err := Run(ctx, right, 19, b, gb, ga, Options{MemoryBudget: 17 << 20})
			ra := <-result
			if err != nil || ra.err != nil {
				t.Fatalf("asymmetric one-byte reconciliation: %v / %v", ra.err, err)
			}
			sameTree(t, ra.result.Tree, want)
			sameTree(t, rb.Tree, want)
		})
	}
}

func TestReviewEarlySupplyMustBelongToAnnouncedRadix(t *testing.T) {
	clock := before.Seed()
	version := clock.Tick()
	path := tree.Address(version)
	e := &engine{remote: protocol.Greeting{Listing: []protocol.Child{{Radix: path[0] ^ 1}}}}
	if err := e.validateEarlyPath(path); err == nil {
		t.Fatal("accepted a root radix absent from the sender's greeting")
	}
}
