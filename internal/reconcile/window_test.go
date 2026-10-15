package reconcile

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

func TestWindowPopulationAndArithmetic(t *testing.T) {
	for _, c := range []struct {
		messages uint64
		depth    int
		want     uint64
	}{
		{0, 32, 0},
		{1000, 0, 1},
		{1000, 1, 256},
		{1000, 2, 1000},
		{math.MaxUint64, 7, 1 << 56},
		{math.MaxUint64, 8, math.MaxUint64},
		{math.MaxUint64, 32, math.MaxUint64},
	} {
		if got := occupiedPrefixes(c.messages, c.depth); got != c.want {
			t.Fatalf("population(%d,%d)=%d want%d", c.messages, c.depth, got, c.want)
		}
	}
	for _, budget := range []uint64{0, 1, 1359, 1360, 1 << 20, math.MaxUint64} {
		for _, messages := range []uint64{0, 1, 7, 1 << 32, math.MaxUint64} {
			previous := 0
			for depth := 1; depth <= 33; depth++ {
				count, share := comparisonWindow(budget, messages, depth)
				if count < 1 || share != budget/comparisonQueues {
					t.Fatal("lost floor or byte share")
				}
				if depth == 1 && count != 1 {
					t.Fatal("root query has more than one scope")
				}
				if count > 1 &&
					(uint64(count) > messages || uint64(count) > share/pendingSlotBytes) {
					t.Fatal("count exceeds population or affordable inline storage")
				}
				if count < previous {
					t.Fatal("deeper prefix population reduced the capacity")
				}
				previous = count
			}
		}
	}
}

func TestWindowRoleAndDepthMapping(t *testing.T) {
	for _, role := range []protocol.Role{protocol.Initiator, protocol.Responder} {
		e := &engine{
			role:    role,
			local:   protocol.Greeting{SetLen: 1000},
			remote:  protocol.Greeting{SetLen: 1000},
			options: Options{MemoryBudget: 1000 * comparisonQueues * pendingSlotBytes},
		}
		e.initQueues()
		for depth := 0; depth < len(e.queues); depth++ {
			q := &e.queues[depth]
			wanted := depth > 0 && depth%2 == 1
			if role == protocol.Responder {
				wanted = depth > 0 && depth%2 == 0
			}
			if q.initialized() != wanted {
				t.Fatalf(
					"role%d queue%d initialized=%v want%v",
					role,
					depth,
					q.initialized(),
					wanted,
				)
			}
			if wanted {
				count, _ := comparisonWindow(e.options.MemoryBudget, 1000, depth)
				got := 1
				if q.lazy != nil {
					got = q.lazy.limit
				}
				if got != count {
					t.Fatalf("queue%d count%d want%d", depth, got, count)
				}
			}
		}
		if e.windowGranted != 1000 {
			t.Fatalf("grant%d want1000", e.windowGranted)
		}
	}
}

func TestRealQuestionsEnterTheirScopeDepthQueue(t *testing.T) {
	fixture := reconciliationFixture(2, 0, 1, false)
	var path tree.Path
	for version := range fixture.left.All() {
		path = tree.Address(version)
		break
	}
	for depth := 0; depth < 32; depth++ {
		role := protocol.Responder
		if depth%2 == 1 {
			role = protocol.Initiator
		}
		e := &engine{
			ctx: context.Background(), role: role, local: fixture.local, remote: fixture.remote,
			options: Options{MemoryBudget: 1 << 20},
		}
		e.initQueues()
		scope, err := fixture.left.Scope(path[:depth])
		if err != nil {
			t.Fatal(err)
		}
		child := scope.Children()[0]
		pair := fanPair{ours: &child, theirs: &protocol.Child{Radix: child.Radix}}
		if depth == 31 {
			// a peer-only leaf produces the terminal empty query. Identical
			// leaf paths otherwise match without consulting their digest.
			pair.ours = nil
		}
		action, _, err := e.comparePair(scope, pair, depth)
		if err != nil || action == nil || action.question == nil {
			t.Fatalf("question at depth%d: %v", depth, err)
		}
		action.flushed = make(chan struct{})
		close(action.flushed)
		if err := e.publishQuestion(*action, depth); err != nil {
			t.Fatal(err)
		}
		index := action.question.scope.Depth() + 1
		if index != depth+2 || !e.queues[index].initialized() {
			t.Fatal("real scope entered wrong retained-child depth")
		}
		item, ok, err := e.queues[index].pop(e.ctx)
		if err != nil || !ok || item.scope.Depth()+1 != index {
			t.Fatal("published scope depth changed")
		}
		for _, held := range item.fan {
			if held.Scope.Depth() != index {
				t.Fatal("fan child depth disagrees with queue depth")
			}
		}
	}
}

func fanItem(children int) pending {
	return pending{fan: make([]tree.Child[[]byte], children)}
}

func TestCreditLedgerGrowthHighwaterAndFloor(t *testing.T) {
	const share = 4*pendingSlotBytes + 2*pendingChildBytes
	q := newPendingQueue(10, share)
	for i := 0; i < 2; i++ {
		oldBytes := uint64(cap(q.buffer)) * pendingSlotBytes
		accepted, err := q.tryPush(fanItem(1), nil)
		if err != nil || !accepted {
			t.Fatalf("push%d: %v %v", i, accepted, err)
		}
		owned := uint64(cap(q.buffer))*pendingSlotBytes + q.fanBytes
		if owned > share || owned+oldBytes > 2*share {
			t.Fatal("growth exceeded reachable ownership envelope")
		}
	}
	if cap(q.buffer) != 2 || q.fanBytes != 2*pendingChildBytes {
		t.Fatal("geometric ring or fan ledger changed")
	}
	if ok, err := q.tryPush(fanItem(1), nil); err != nil || ok {
		t.Fatal("accepted over-credit item")
	}
	for q.size > 0 {
		q.remove()
	}
	if cap(q.buffer) != 2 || q.fanBytes != 0 {
		t.Fatal("drain lost high-water or retained fan charges")
	}
	if ok, err := q.tryPush(fanItem(256), nil); err != nil || !ok {
		t.Fatal("retained high-water prevented one-item progress")
	}
	if ok, err := q.tryPush(pending{}, nil); err != nil || ok {
		t.Fatal("oversized progress item admitted another item")
	}
	q.remove()
	for _, item := range q.buffer {
		if item.fan != nil {
			t.Fatal("pop retained detached fan")
		}
	}
	// pop retains the two-slot ring and releases only fan ownership.
	if ok, _ := q.tryPush(fanItem(2), nil); !ok {
		t.Fatal("empty progress failed")
	}
	if ok, _ := q.tryPush(fanItem(2), nil); !ok || cap(q.buffer) != 2 {
		t.Fatal("available byte credit prevented a two-item window")
	}
}

func TestCreditCheapWaveThenFanWave(t *testing.T) {
	const share = 17 * pendingSlotBytes
	q := newPendingQueue(17, share)
	for range 17 {
		if ok, err := q.tryPush(pending{}, nil); err != nil || !ok {
			t.Fatal("cheap wave did not fill count ceiling")
		}
	}
	for q.size > 0 {
		q.remove()
	}
	if cap(q.buffer) != 17 {
		t.Fatal("pop discarded charged high-water")
	}
	for {
		ok, err := q.tryPush(fanItem(1), nil)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	// geometric growth waits rather than repeatedly copying to the exact
	// affordable count (ten). Eight slots still recover useful overlap.
	want := 8
	if q.size != want || q.fanBytes+uint64(cap(q.buffer))*pendingSlotBytes > share {
		t.Fatalf("positive-fan wave collapsed to%d items, want%d", q.size, want)
	}
}

func TestCreditWaitWakeCancelCloseAndStall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q := newPendingQueue(100, 2*pendingSlotBytes+2*pendingChildBytes)
	if err := q.push(ctx, fanItem(1)); err != nil {
		t.Fatal(err)
	}
	if err := q.push(ctx, fanItem(1)); err != nil {
		t.Fatal(err)
	}
	var stalls atomic.Uint64
	joined := make(chan error, 1)
	go func() { joined <- q.pushMeasured(ctx, fanItem(1), &stalls) }()
	waitForStall(t, &stalls)
	for range 5 {
		queueNotify(q.writable)
	}
	if _, ok, err := q.pop(ctx); err != nil || !ok {
		t.Fatal("pop failed")
	}
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	if stalls.Load() != 1 {
		t.Fatal("credit-blocked arrival counted more than once")
	}
	blocked, abort := context.WithCancel(ctx)
	go func() { joined <- q.push(blocked, fanItem(1)) }()
	abort()
	if err := <-joined; err != context.Canceled {
		t.Fatalf("cancel: %v", err)
	}
	go func() { joined <- q.push(ctx, fanItem(1)) }()
	q.close()
	if err := <-joined; err != errQueueClosed {
		t.Fatalf("close: %v", err)
	}
	for {
		_, ok, err := q.pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	if q.fanBytes != 0 {
		t.Fatal("close/drain retained fan credit")
	}
}

func TestBootstrapWindowAtPositiveBudgets(t *testing.T) {
	fixture := reconciliationFixture(8, 0, 1, false)
	empty := tree.Tree[[]byte]{}
	for _, budget := range []uint64{0, 1 << 20, 512 << 20} {
		for _, reverse := range []bool{false, true} {
			a, b := empty, fixture.left
			if reverse {
				a, b = b, a
			}
			left, right := link.MemoryWithCapacity(1)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			type outcome struct {
				result Result
				err    error
			}
			joined := make(chan outcome, 1)
			ga, gb := greeting(a, 0), greeting(b, 0)
			go func() {
				result, err := Run(ctx, left, 0, a, ga, gb, Options{MemoryBudget: budget})
				joined <- outcome{result, err}
			}()
			other, err := Run(ctx, right, 0, b, gb, ga, Options{MemoryBudget: budget})
			first := <-joined
			_ = left.Close()
			_ = right.Close()
			cancel()
			if err != nil || first.err != nil {
				t.Fatalf("bootstrap budget%d reverse%v: %v %v", budget, reverse, first.err, err)
			}
			if first.result.Stats.WindowGranted != 1 || other.Stats.WindowGranted != 1 {
				t.Fatalf(
					"bootstrap budget%d grants%d/%d",
					budget,
					first.result.Stats.WindowGranted,
					other.Stats.WindowGranted,
				)
			}
			sameTree(t, first.result.Tree, fixture.left)
			sameTree(t, other.Tree, fixture.left)
		}
	}
}
