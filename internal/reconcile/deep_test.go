package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

func TestLeafParentComparisonUsesVersionAddress(t *testing.T) {
	clock := before.Seed()
	version := clock.Tick()
	full := tree.Tree[[]byte]{}.Insert(version, []byte{0xf6})
	path := tree.Address(version)
	parent, err := full.Scope(path[:31])
	if err != nil {
		t.Fatal(err)
	}
	peerListing := listing(parent.Children())
	// At the leaf parent the full address identifies the version. Upstream
	// answer::leaf_parent ignores the peer's digest for an already-held radix.
	peerListing[0].Hash[0] ^= 0xff
	e := &engine{}
	actions, err := e.compare(parent, peerListing, 31)
	if err != nil || len(actions) != 1 || actions[0].signal != protocol.MatchContinue ||
		actions[0].question != nil {
		t.Fatalf("leaf identity produced a dispute: %+v %v", actions, err)
	}
	if e.disputed.Load() != 0 {
		t.Fatal("matching leaf counted as disputed")
	}
	actions, err = e.compare(emptyScope(path[:31]), listing(parent.Children()), 31)
	if err != nil || len(actions) != 1 || actions[0].signal != protocol.QueryEmptyContinue {
		t.Fatal(actions, err)
	}
	question := actions[0].question
	if question == nil || !question.terminal || question.scope.Depth() != 32 ||
		!bytes.Equal(question.scope.Prefix(), path[:]) {
		t.Fatal("leaf-parent request did not retain the complete real address")
	}
}

func newDeepTestEngine(
	ctx context.Context,
	cancel context.CancelFunc,
	carrier link.Link,
	local, remote tree.Tree[[]byte],
	role protocol.Role,
) *engine {
	e := &engine{
		ctx: ctx, cancel: cancel, carrier: carrier, original: local,
		local: greeting(local, 0), remote: greeting(remote, 0), role: role,
		edits: make(chan tree.Scope[[]byte], 1),
	}
	e.initQueues()
	e.streams = newStreamSet(ctx, carrier, 0, role, protocol.FrameLimits{})
	return e
}

func waitDeepWorkers(t *testing.T, engines ...*engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		for _, e := range engines {
			e.workers.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deep stage workers did not stop")
	}
}

func TestLastLevelQuestionAndReplyWithOneByteWindows(t *testing.T) {
	for _, redacted := range []bool{false, true} {
		name := "supply"
		if redacted {
			name = "redaction"
		}
		t.Run(name, func(t *testing.T) {
			clock := before.Seed()
			version := clock.Tick()
			full := tree.Tree[[]byte]{}.Insert(version, []byte{0xf6})
			empty := tree.Tree[[]byte]{}
			if redacted {
				empty = empty.WithLatest(clock.Tick())
			}
			path := tree.Address(version)
			held, err := full.Scope(path[:31])
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			left, right := link.MemoryWithCapacity(1)
			defer func() { _ = left.Close() }()
			a := newDeepTestEngine(ctx, cancel, left, empty, full, protocol.Initiator)
			b := newDeepTestEngine(ctx, cancel, right, full, empty, protocol.Responder)
			defer func() { _ = a.streams.shutdown(false); _ = b.streams.shutdown(false) }()
			a.startOutput(16)
			b.startOutput(16)
			a.launch(func() error { return a.stage(33) })
			// A real singleton exposes valid virtual scopes at every depth. Inject
			// the final pending scope through the existing stage seam: reaching it
			// from two distinct honest trees would require an infeasible 31-byte
			// address-prefix collision. No path or production hash is replaced.
			a.launch(func() error {
				defer a.queues[33].close()
				actions, err := a.compare(emptyScope(path[:31]), listing(held.Children()), 31)
				if err != nil {
					return err
				}
				if err := a.emit(actions, 31, 16); err != nil {
					return err
				}
				return a.finishOutput(16)
			})
			b.launch(func() error {
				if err := b.resolve(
					pending{scope: held, fan: held.Children()},
					32,
					16,
				); err != nil {
					return err
				}
				if err := b.streams.finishReceive(16); err != nil {
					return err
				}
				return b.finishOutput(16)
			})
			waitDeepWorkers(t, a, b)
			if a.failure() != nil || b.failure() != nil {
				t.Fatalf("last-level exchange: %v / %v", a.failure(), b.failure())
			}
			if err := a.streams.shutdown(true); err != nil {
				t.Fatal(err)
			}
			if err := b.streams.shutdown(true); err != nil {
				t.Fatal(err)
			}
			if a.windowGranted != 1 || b.windowGranted != 1 {
				t.Fatal("deep fixture did not use one-slot queues")
			}
			if a.streams.snapshotStats().StreamsOpened != 1 ||
				b.streams.snapshotStats().StreamsOpened != 1 {
				t.Fatal("terminal exchange opened unrelated streams")
			}
			if redacted {
				if len(a.edits) != 0 || b.shed.Load() != 1 {
					t.Fatal("last-level causal deletion was not honored")
				}
				return
			}
			if len(a.edits) != 1 || a.gained.Load() != 1 {
				t.Fatal("last-level supply did not publish exactly one leaf")
			}
			result := <-a.edits
			got, payload, ok := result.Leaf()
			if !ok || !got.Equal(version) || !bytes.Equal(payload, []byte{0xf6}) {
				t.Fatal("terminal reply changed leaf identity")
			}
		})
	}
}

type deepWriteLink struct {
	link.Link
	frameStarted chan struct{}
	once         sync.Once
}

func (l *deepWriteLink) Open(ctx context.Context) (io.WriteCloser, error) {
	writer, err := l.Link.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &deepWriteStream{WriteCloser: writer, owner: l}, nil
}

type deepWriteStream struct {
	io.WriteCloser
	owner  *deepWriteLink
	writes int
}

func (w *deepWriteStream) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 2 {
		w.owner.once.Do(func() { close(w.owner.frameStarted) })
	}
	return w.WriteCloser.Write(p)
}

func TestCancellationStopsBlockedLastLevelQuestion(t *testing.T) {
	clock := before.Seed()
	version := clock.Tick()
	full := tree.Tree[[]byte]{}.Insert(version, []byte{0xf6})
	path := tree.Address(version)
	held, err := full.Scope(path[:31])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	left, right := link.MemoryWithCapacity(1)
	defer func() { _ = left.Close() }()
	watched := &deepWriteLink{Link: left, frameStarted: make(chan struct{})}
	a := newDeepTestEngine(ctx, cancel, watched, tree.Tree[[]byte]{}, full, protocol.Initiator)
	peer := newStreamSet(ctx, right, 0, protocol.Responder, protocol.FrameLimits{})
	defer func() { _ = a.streams.shutdown(false); _ = peer.shutdown(false) }()
	a.startOutput(16)
	a.launch(func() error { return a.stage(33) })
	a.launch(func() error {
		defer a.queues[33].close()
		actions, err := a.compare(emptyScope(path[:31]), listing(held.Children()), 31)
		if err != nil {
			return err
		}
		return a.emit(actions, 31, 16)
	})
	// The peer accepts the label but never consumes the frame. The final
	// question therefore blocks on its one-byte stream before publication,
	// while stage 33 waits on its one-slot queue.
	select {
	case <-watched.frameStarted:
	case <-time.After(time.Second):
		t.Fatal("terminal question did not reach the wire")
	}
	cancel()
	waitDeepWorkers(t, a)
	if a.failure() == nil {
		t.Fatal("canceled terminal question succeeded")
	}
	if q, ok, err := a.queues[33].pop(context.Background()); err != nil || ok {
		t.Fatalf("unflushed terminal question was published: %+v %t %v", q, ok, err)
	}
}

func TestTerminalScopeValidationBounds(t *testing.T) {
	clock := before.Seed()
	version := clock.Tick()
	path := tree.Address(version)
	request := pending{scope: emptyScope(path[:]), terminal: true}
	state := newReplyState(request)
	if err := state.observe(path); err != nil {
		t.Fatal(err)
	}
	wrong := path
	wrong[31] ^= 1
	if err := newReplyState(request).observe(wrong); !errors.Is(err, ErrSupply) {
		t.Fatal("wrong full terminal address accepted", err)
	}
	e := &engine{ctx: context.Background(), role: protocol.Initiator}
	for _, signal := range []protocol.Signal{protocol.MatchEnd, protocol.QueryEmptyEnd, protocol.QueryEnd} {
		if _, err := e.react(
			newReplyState(request),
			protocol.Frame{Signal: signal},
			33,
		); !errors.Is(
			err,
			ErrReply,
		) {
			t.Fatalf("terminal positional reaction %d accepted: %v", signal, err)
		}
	}
	if err := e.answerLeaf(
		tree.LeafScope(version, []byte{0xf6}),
		[]protocol.Child{{Radix: 1}},
	); !errors.Is(
		err,
		ErrReply,
	) {
		t.Fatal("nonempty terminal question accepted", err)
	}
	e.initQueues()
	for _, depth := range []int{34, 35} {
		if err := e.enqueue(depth, request); !errors.Is(err, ErrReply) {
			t.Fatalf("question depth%d accepted: %v", depth, err)
		}
	}
}
