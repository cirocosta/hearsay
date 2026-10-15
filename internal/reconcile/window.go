package reconcile

import (
	"unsafe"

	"github.com/cirocosta/hearsay/internal/tree"
)

const (
	pendingSlotBytes  = uint64(unsafe.Sizeof(pending{}))
	pendingChildBytes = uint64(unsafe.Sizeof(tree.Child[[]byte]{}))
	comparisonQueues  = 17
)

// comparisonWindow returns a count ceiling and a byte share for one queue.
// Queue d retains children at depth d and names scopes at prefix depth d-1.
// Scope views share immutable nodes, clocks, and payloads with the opening tree;
// only the inline slots and detached fan arrays belong to this queue's ledger.
// Consequently the greeting's version-size bound has no queue-owned term.
func comparisonWindow(budget, peerMessages uint64, depth int) (int, uint64) {
	share := budget / comparisonQueues
	population := occupiedPrefixes(peerMessages, depth-1)
	if depth == 1 {
		population = 1
	}
	maxInt := uint64(^uint(0) >> 1)
	count := max(1, min(population, share/pendingSlotBytes, maxInt))
	return int(count), share
}

// occupiedPrefixes saturates before radix exponentiation can overflow.
func occupiedPrefixes(messages uint64, depth int) uint64 {
	population := uint64(1)
	for range depth {
		if population >= messages || population > ^uint64(0)/256 {
			return messages
		}
		population *= 256
	}
	return min(population, messages)
}

func pendingFanBytes(item pending) uint64 {
	if uint64(cap(item.fan)) > ^uint64(0)/pendingChildBytes {
		return ^uint64(0)
	}
	return uint64(cap(item.fan)) * pendingChildBytes
}
