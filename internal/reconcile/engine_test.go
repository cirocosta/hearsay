package reconcile

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

func greeting(t tree.Tree[[]byte], target uint64) protocol.Greeting {
	root, _ := t.Scope(nil)
	return protocol.Greeting{
		Listing:           listing(root.Children()),
		SetLen:            uint64(t.Len()),
		Version:           t.Latest(),
		MaxVersionBytes:   uint64(t.MaxVersionBytes()),
		PayloadDepthLimit: 64,
		TargetMessageSize: target,
	}
}

func runPair(t *testing.T, a, b tree.Tree[[]byte], capacity int, target uint64) (Result, Result) {
	t.Helper()
	left, right := link.MemoryWithCapacity(capacity)
	defer func() { _ = left.Close() }()
	return runOnPair(t, left, right, 0, a, b, target)
}

func runOnPair(
	t *testing.T,
	left, right link.Link,
	epoch uint8,
	a, b tree.Tree[[]byte],
	target uint64,
) (Result, Result) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ga, gb := greeting(a, target), greeting(b, target)
	type outcome struct {
		result Result
		err    error
	}
	ch := make(chan outcome, 1)
	go func() { result, err := Run(ctx, left, epoch, a, ga, gb, Options{}); ch <- outcome{result, err} }()
	rb, err := Run(ctx, right, epoch, b, gb, ga, Options{})
	ra := <-ch
	if err != nil || ra.err != nil {
		t.Fatalf("session failed: left %v; right %v", ra.err, err)
	}
	return ra.result, rb
}

func sameTree(t *testing.T, got, want tree.Tree[[]byte]) {
	t.Helper()
	if got.Len() != want.Len() || got.Hash() != want.Hash() || !got.Latest().Equal(want.Latest()) {
		t.Fatalf(
			"tree mismatch: got len %d hash %x; want len %d hash %x",
			got.Len(),
			got.Hash(),
			want.Len(),
			want.Hash(),
		)
	}
	for v, value := range want.All() {
		payload, ok := got.Get(v)
		if !ok || !bytes.Equal(payload, value) {
			t.Fatal("payload mismatch")
		}
	}
}

func TestOneByteSessions(t *testing.T) {
	for _, target := range []uint64{0, 28, 4096} {
		aClock := before.Seed()
		bClock := aClock.Fork()
		var a, b tree.Tree[[]byte]
		for i := 0; i < 80; i++ {
			a = a.Insert(aClock.Tick(), cbor.AppendHead(nil, cbor.MajorUint, uint64(i)))
			b = b.Insert(bClock.Tick(), []byte{0xf6})
		}
		want := a.Join(b)
		ra, rb := runPair(t, a, b, 1, target)
		sameTree(t, ra.Tree, want)
		sameTree(t, rb.Tree, want)
		if ra.Stats.MessagesGained != 80 || rb.Stats.MessagesGained != 80 ||
			ra.Stats.MessagesSent != 80 ||
			rb.Stats.MessagesSent != 80 {
			t.Fatalf("unexpected stats: %+v %+v", ra.Stats, rb.Stats)
		}
	}
}

func TestBootstrapRedactionAndReuse(t *testing.T) {
	clock := before.Seed()
	var full tree.Tree[[]byte]
	for i := 0; i < 300; i++ {
		full = full.Insert(clock.Tick(), []byte{0xf6})
	}
	empty := tree.Tree[[]byte]{}
	left, right := link.MemoryWithCapacity(1)
	defer func() { _ = left.Close() }()
	ra, rb := runOnPair(t, left, right, 0, empty, full, 4096)
	sameTree(t, ra.Tree, full)
	sameTree(t, rb.Tree, full)
	if ra.Stats.StreamsOpened != 0 || rb.Stats.StreamsOpened != 1 {
		t.Fatalf("bootstrap opened excessive levels: %+v %+v", ra.Stats, rb.Stats)
	}
	convergedA, convergedB := runOnPair(t, left, right, 1, ra.Tree, rb.Tree, 4096)
	if convergedA.Stats.StreamsOpened != 0 || convergedB.Stats.StreamsOpened != 0 {
		t.Fatal("equal session opened streams")
	}
	var removed []before.Version
	for version := range full.All() {
		if len(removed) < 150 {
			removed = append(removed, version)
		}
	}
	redacted := full
	for _, version := range removed {
		redacted = redacted.Forget(version, clock.Tick())
	}
	ra, rb = runOnPair(t, left, right, 2, full, redacted, 0)
	want := full.Join(redacted)
	sameTree(t, ra.Tree, want)
	sameTree(t, rb.Tree, want)
	if ra.Stats.MessagesShed != 150 {
		t.Fatalf("shed count: %+v", ra.Stats)
	}
}

func TestRandomHistories(t *testing.T) {
	rng := rand.New(rand.NewPCG(721, 919))
	root := before.Seed()
	clocks := []*before.Clock{root, root.Fork()}
	clocks = append(clocks, clocks[0].Fork(), clocks[1].Fork())
	states := make([]tree.Tree[[]byte], 4)
	for step := 0; step < 90; step++ {
		who := rng.IntN(4)
		clocks[who].Absorb(states[who].Latest())
		switch rng.IntN(3) {
		case 0, 1:
			states[who] = states[who].Insert(
				clocks[who].Tick(),
				cbor.AppendHead(nil, cbor.MajorUint, uint64(step)),
			)
		case 2:
			for version := range states[who].All() {
				states[who] = states[who].Forget(version, clocks[who].Tick())
				break
			}
		}
		if step%3 != 0 {
			continue
		}
		other := (who + 1 + rng.IntN(3)) % 4
		want := states[who].Join(states[other])
		target := []uint64{0, 31, 1024}[rng.IntN(3)]
		ra, rb := runPair(t, states[who], states[other], 1, target)
		sameTree(t, ra.Tree, want)
		sameTree(t, rb.Tree, want)
		states[who], states[other] = ra.Tree, rb.Tree
	}
}

func TestCancellationAndAdmissionFailure(t *testing.T) {
	c := before.Seed()
	full := tree.Tree[[]byte]{}.Insert(c.Tick(), []byte{0xf6})
	empty := tree.Tree[[]byte]{}
	left, _ := link.MemoryWithCapacity(1)
	defer func() { _ = left.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		_, err := Run(ctx, left, 0, empty, greeting(empty, 0), greeting(full, 0), Options{})
		finished <- err
	}()
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("canceled session succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation leaked session")
	}
	left, right := link.MemoryWithCapacity(1)
	defer func() { _ = left.Close() }()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sentinel := errors.New("application rejected payload")
	go func() {
		_, err := Run(
			ctx,
			left,
			0,
			empty,
			greeting(empty, 0),
			greeting(full, 0),
			Options{Admit: func([]byte) error { return sentinel }},
		)
		finished <- err
	}()
	_, remoteErr := Run(ctx, right, 0, full, greeting(full, 0), greeting(empty, 0), Options{})
	if err := <-finished; !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if remoteErr == nil {
		t.Fatal("peer ignored rejected supply")
	}
}

func TestWidePayloadHasNoImplicitItemCap(t *testing.T) {
	const elements = 1_000_001
	payload := cbor.AppendHead(nil, cbor.MajorArray, elements)
	payload = append(payload, make([]byte, elements)...)
	clock := before.Seed()
	full := tree.Tree[[]byte]{}.Insert(clock.Tick(), payload)
	empty := tree.Tree[[]byte]{}
	a, b := runPair(t, empty, full, 8192, 2<<20)
	sameTree(t, a.Tree, full)
	sameTree(t, b.Tree, full)
}

func TestTerminalLeafExchange(t *testing.T) {
	for _, redacted := range []bool{false, true} {
		clock := before.Seed()
		version := clock.Tick()
		full := tree.Tree[[]byte]{}.Insert(version, []byte{0xf6})
		empty := tree.Tree[[]byte]{}
		if redacted {
			empty = empty.WithLatest(clock.Tick())
		}
		left, right := link.MemoryWithCapacity(1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		a := &engine{
			ctx:      ctx,
			original: empty,
			local:    greeting(empty, 0),
			remote:   greeting(full, 0),
			role:     protocol.Initiator,
			options:  Options{},
			edits:    make(chan tree.Scope[[]byte], 2),
		}
		b := &engine{
			ctx:      ctx,
			original: full,
			local:    greeting(full, 0),
			remote:   greeting(empty, 0),
			role:     protocol.Responder,
			options:  Options{},
			edits:    make(chan tree.Scope[[]byte], 2),
		}
		a.streams = newStreamSet(ctx, left, 0, a.role, protocol.FrameLimits{})
		b.streams = newStreamSet(ctx, right, 0, b.role, protocol.FrameLimits{})
		b.startOutput(16)
		path := tree.Address(version)
		held, _ := full.Scope(path[:])
		request := pending{scope: emptyScope(path[:]), terminal: true}
		finished := make(chan error, 1)
		go func() {
			err := b.answerLeaf(held, nil)
			if err == nil {
				err = b.finishOutput(16)
			}
			finished <- err
		}()
		if err := a.resolve(request, 33, 16); err != nil {
			t.Fatal(err)
		}
		if err := a.streams.finishReceive(16); err != nil {
			t.Fatal(err)
		}
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if redacted {
			if len(a.edits) != 0 || b.shed.Load() != 1 {
				t.Fatal("terminal deletion was not honored")
			}
		} else {
			if len(a.edits) != 1 {
				t.Fatal("terminal supply missing")
			}
			leaf := <-a.edits
			got, payload, ok := leaf.Leaf()
			if !ok || !got.Equal(version) || !bytes.Equal(payload, []byte{0xf6}) {
				t.Fatal("wrong terminal leaf")
			}
		}
		if err := a.streams.shutdown(true); err != nil {
			t.Fatal(err)
		}
		if err := b.streams.shutdown(true); err != nil {
			t.Fatal(err)
		}
		cancel()
		_ = left.Close()
	}
}

func TestReplyValidation(t *testing.T) {
	clock := before.Seed()
	version := clock.Tick()
	full := tree.Tree[[]byte]{}.Insert(version, []byte{0xf6})
	root, _ := full.Scope(nil)
	empty := tree.Tree[[]byte]{}
	emptyRoot, _ := empty.Scope(nil)
	e := &engine{
		ctx:     context.Background(),
		remote:  greeting(full, 0),
		options: Options{},
		edits:   make(chan tree.Scope[[]byte], 4),
	}
	state := newReplyState(pending{scope: emptyRoot})
	path := tree.Address(version)
	if err := e.validateEarlyPath(path); err != nil {
		t.Fatal(err)
	}
	announced := e.remote.Listing
	e.remote.Listing = nil
	if err := e.validateEarlyPath(path); !errors.Is(err, ErrSupply) {
		t.Fatal(err)
	}
	e.remote.Listing = announced
	if _, err := e.react(
		state,
		protocol.Frame{Signal: protocol.MatchEnd},
		1,
	); !errors.Is(
		err,
		ErrReply,
	) {
		t.Fatal(err)
	}
	state = newReplyState(pending{scope: root, fan: root.Children()})
	if err := state.observe(tree.Address(version)); !errors.Is(err, ErrSupply) {
		t.Fatal(err)
	}
	state = newReplyState(pending{scope: emptyRoot})
	e.remote.Version = before.Version{}
	if err := e.absorbRecord(
		state,
		protocol.Record{Version: version, Payload: []byte{0xf6}},
	); !errors.Is(
		err,
		ErrContainment,
	) {
		t.Fatal(err)
	}
	e.remote = greeting(full, 0)
	e.remote.SetLen = 0
	state = newReplyState(pending{scope: emptyRoot})
	if err := e.absorbRecord(
		state,
		protocol.Record{Version: version, Payload: []byte{0xf6}},
	); !errors.Is(
		err,
		ErrSetLength,
	) {
		t.Fatal(err)
	}
	state = newReplyState(pending{scope: emptyRoot})
	path = tree.Address(version)
	if err := state.observe(path); err != nil {
		t.Fatal(err)
	}
	if err := state.observe(path); !errors.Is(err, ErrSupply) {
		t.Fatal(err)
	}
	state = newReplyState(pending{scope: emptyRoot})
	state.reacted = true
	if _, err := e.react(
		state,
		protocol.Frame{Signal: protocol.ReplyEnd},
		1,
	); !errors.Is(
		err,
		ErrReply,
	) {
		t.Fatal(err)
	}
	if _, err := e.react(
		newReplyState(pending{scope: emptyRoot}),
		protocol.Frame{Signal: protocol.StreamEnd},
		1,
	); !errors.Is(
		err,
		ErrReply,
	) {
		t.Fatal(err)
	}
}
