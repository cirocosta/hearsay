// Package reconcile implements the bounded, pipelined rumors V2 data phase.
// Control negotiation, identity transfer, and publishing the final snapshot
// belong to the caller.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/observe"
)

// Session errors identify incoherent premises and invalid positional replies.
var (
	ErrPremise     = errors.New("reconcile: greeting disagrees with opening snapshot")
	ErrReply       = errors.New("reconcile: invalid positional reply")
	ErrSupply      = errors.New("reconcile: invalid supplied scope or order")
	ErrContainment = errors.New("reconcile: supplied version exceeds peer history")
	ErrSetLength   = errors.New("reconcile: supplied leaves exceed peer set length")
)

// Options bounds transient queues and admits application payloads. Admit runs
// before a received payload enters the result tree and must not retain or mutate
// its bytes. The callback owns the negotiated typed recursion policy.
type Options struct {
	MemoryBudget uint64
	Admit        func([]byte) error
	Observer     *observe.Session
}

// Stats counts data frames, excluding transport labels and control traffic.
type Stats struct {
	// Elapsed covers the full session when populated by the session driver.
	Elapsed                                    time.Duration
	BytesSent, BytesReceived                   uint64
	FramesSent, FramesReceived                 uint64
	MessagesSent, MessagesGained, MessagesShed uint64
	DisputedScopes, StreamsOpened              uint64
	// WindowGranted is the largest configured scope-count ceiling. Byte credit
	// may apply earlier. Equal frontiers skip window derivation and report zero.
	WindowGranted uint64
	// WindowStalls counts pushes that observed a full comparison queue on
	// arrival. It measures scheduling-dependent pressure, not wait duration.
	WindowStalls uint64
}

// Result contains a coherent reconciled snapshot and its data-phase statistics.
type Result struct {
	Tree  tree.Tree[[]byte]
	Stats Stats
}

type pending struct {
	scope    tree.Scope[[]byte]
	fan      []tree.Child[[]byte]
	terminal bool
}
type engine struct {
	ctx                                    context.Context
	cancel                                 context.CancelFunc
	carrier                                link.Link
	streams                                *streamSet
	original                               tree.Tree[[]byte]
	local, remote                          protocol.Greeting
	role                                   protocol.Role
	options                                Options
	queues                                 [34]pendingHandle
	outputs                                [17]chan replyBatch
	outputDone                             [17]chan struct{}
	edits                                  chan tree.Scope[[]byte]
	errOnce                                sync.Once
	errMu                                  sync.Mutex
	err                                    error
	workers                                sync.WaitGroup
	sent, gained, shed, disputed, received atomic.Uint64
	windowGranted                          uint64
	windowStalls                           atomic.Uint64
}

// Run reconciles immutable opening snapshots over independently flow-controlled
// data streams. On failure or cancellation it closes the carrier to unblock I/O.
// Success leaves the persistent control stream ready for identity/completion.
// The caller owns the control reader and must cancel ctx on control departure,
// retaining any control bytes received before identity transfer or completion.
func Run(
	ctx context.Context,
	carrier link.Link,
	epoch uint8,
	snapshot tree.Tree[[]byte],
	local, remote protocol.Greeting,
	options Options,
) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("reconcile: nil context")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := validatePremise(snapshot, local); err != nil {
		return Result{}, err
	}
	negotiation, err := protocol.Negotiate(local, remote)
	if err != nil {
		return Result{}, err
	}
	if negotiation.Equal {
		return Result{Tree: snapshot.WithLatest(remote.Version)}, nil
	}
	role := protocol.Responder
	if negotiation.Initiator {
		role = protocol.Initiator
	}
	options.Observer.Elected(observe.Role(role))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	e := &engine{
		ctx:      runCtx,
		cancel:   cancel,
		carrier:  carrier,
		original: snapshot,
		local:    local,
		remote:   remote,
		role:     role,
		options:  options,
		edits:    make(chan tree.Scope[[]byte], 1),
	}
	e.streams = newStreamSet(
		runCtx,
		carrier,
		epoch,
		role,
		protocol.FrameLimits{TargetMessageSize: negotiation.TargetMessageSize},
		options.Observer,
	)
	e.initQueues()
	return e.run()
}

func validatePremise(t tree.Tree[[]byte], g protocol.Greeting) error {
	if uint64(t.Len()) != g.SetLen || !t.Latest().Equal(g.Version) {
		return ErrPremise
	}
	root, err := t.Scope(nil)
	if err != nil {
		return err
	}
	fan := root.Children()
	if len(fan) != len(g.Listing) {
		return ErrPremise
	}
	for i, child := range fan {
		if child.Radix != g.Listing[i].Radix || [24]byte(child.Scope.Hash()) != g.Listing[i].Hash {
			return ErrPremise
		}
	}
	return nil
}

func (e *engine) initQueues() {
	population := e.remote.SetLen
	if e.local.SetLen == 0 {
		// an empty opening is elected initiator. Its root query has no
		// held children, so no positional reply can trigger deeper descent.
		population = 0
	}
	first := 1
	if e.role == protocol.Responder {
		first = 2
	}
	for depth := first; depth < 34; depth += 2 {
		count, share := comparisonWindow(e.options.MemoryBudget, population, depth)
		e.windowGranted = max(e.windowGranted, uint64(count))
		e.queues[depth] = newPendingHandle(count, share)
	}
}

func (e *engine) fail(err error) {
	if err == nil {
		return
	}
	e.errOnce.Do(
		func() { e.errMu.Lock(); e.err = err; e.errMu.Unlock(); e.cancel(); _ = e.carrier.Close() },
	)
}
func (e *engine) failure() error { e.errMu.Lock(); defer e.errMu.Unlock(); return e.err }
func (e *engine) launch(fn func() error) {
	e.workers.Add(1)
	go func() { defer e.workers.Done(); e.fail(fn()) }()
}

func (e *engine) run() (Result, error) {
	result := e.original.WithLatest(e.remote.Version)
	aggregated := make(chan struct{})
	go func() { defer close(aggregated); result = e.aggregate(result) }()
	watched := make(chan struct{})
	stopWatch := make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case err := <-e.streams.failures():
			e.fail(err)
		case <-e.ctx.Done():
			e.fail(e.ctx.Err())
		case <-stopWatch:
		}
	}()
	e.startOutputs()
	if e.role == protocol.Initiator {
		e.startInitiator()
	} else {
		e.startResponder()
	}
	e.workers.Wait()
	close(e.edits)
	<-aggregated
	close(stopWatch)
	<-watched
	err := e.failure()
	if closeErr := e.streams.shutdown(err == nil); closeErr != nil && err == nil {
		err = closeErr
	}
	e.cancel()
	if err != nil {
		return Result{}, err
	}
	counts := e.streams.snapshotStats()
	stats := Stats{
		BytesSent:      counts.BytesSent,
		BytesReceived:  counts.BytesReceived,
		FramesSent:     counts.FramesSent,
		FramesReceived: counts.FramesReceived,
		StreamsOpened:  counts.StreamsOpened,
		MessagesSent:   e.sent.Load(),
		MessagesGained: e.gained.Load(),
		MessagesShed:   e.shed.Load(),
		DisputedScopes: e.disputed.Load(),
		WindowGranted:  e.windowGranted,
		WindowStalls:   e.windowStalls.Load(),
	}
	return Result{Tree: result, Stats: stats}, nil
}

func (e *engine) aggregate(result tree.Tree[[]byte]) tree.Tree[[]byte] {
	for {
		select {
		case <-e.ctx.Done():
			return result
		case scope, ok := <-e.edits:
			if !ok {
				return result
			}
			next, err := result.ReplaceScope(scope)
			if err != nil {
				e.fail(err)
				return result
			}
			result = next
		}
	}
}

func (e *engine) publish(scope tree.Scope[[]byte]) error {
	select {
	case e.edits <- scope:
		return nil
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
}

func (e *engine) enqueue(depth int, q pending) error {
	if depth >= 34 || !e.queues[depth].initialized() {
		return fmt.Errorf("%w: question beyond terminal height", ErrReply)
	}
	return e.queues[depth].pushMeasured(e.ctx, q, &e.windowStalls)
}

func (e *engine) startInitiator() {
	for depth := 1; depth <= 33; depth += 2 {
		d := depth
		e.launch(func() error { return e.stage(d) })
	}
	root, _ := e.original.Scope(nil)
	e.launch(func() error {
		defer e.queues[1].close()
		return e.enqueue(1, pending{scope: root, fan: root.Children()})
	})
	e.launch(func() error {
		if err := e.sendEarly(); err != nil {
			return err
		}
		return e.finishOutput(0)
	})
}

func (e *engine) startResponder() {
	for depth := 2; depth <= 32; depth += 2 {
		d := depth
		e.launch(func() error { return e.stage(d) })
	}
	e.launch(func() error {
		defer e.queues[2].close()
		root, _ := e.original.Scope(nil)
		if err := e.answer(root, e.remote.Listing, 0); err != nil {
			return err
		}
		return e.finishOutput(0)
	})
	e.launch(e.receiveEarly)
}

func (e *engine) stage(depth int) error {
	next := depth + 2
	if next < 34 && e.queues[next].initialized() {
		defer e.queues[next].close()
	}
	incoming := incomingStream(depth, e.role)
	for {
		q, ok, err := e.queues[depth].pop(e.ctx)
		if err != nil {
			return err
		}
		if !ok {
			return e.finishStage(depth, incoming)
		}
		if err := e.resolve(q, depth, incoming); err != nil {
			return err
		}
	}
}

func incomingStream(depth int, role protocol.Role) uint8 {
	if role == protocol.Responder {
		return uint8(depth / 2)
	}
	return uint8((depth - 1) / 2)
}

func (e *engine) finishStage(depth int, incoming uint8) error {
	if err := e.streams.finishReceive(incoming); err != nil {
		return err
	}
	if depth == 33 {
		return nil
	}
	return e.finishOutput(outgoingStream(depth, e.role))
}

func outgoingStream(depth int, role protocol.Role) uint8 {
	if role == protocol.Initiator {
		return uint8((depth + 1) / 2)
	}
	return uint8(depth / 2)
}

func listing(fan []tree.Child[[]byte]) []protocol.Child {
	out := make([]protocol.Child, len(fan))
	for i, child := range fan {
		out[i] = protocol.Child{Radix: child.Radix, Hash: [24]byte(child.Scope.Hash())}
	}
	return out
}

func emptyScope(prefix []byte) tree.Scope[[]byte] {
	scope, _ := (tree.Tree[[]byte]{}).Scope(prefix)
	return scope
}

func (e *engine) filter(scope tree.Scope[[]byte]) (tree.Scope[[]byte], error) {
	if scope.Len() == 0 {
		return scope, nil
	}
	if scope.Ceiling().LessEqual(e.remote.Version) {
		e.shed.Add(uint64(scope.Len()))
		return emptyScope(scope.Prefix()), nil
	}
	if !scope.Floor().LessEqual(e.remote.Version) {
		return scope, nil
	}
	fan := scope.Children()
	survivors := make([]tree.Child[[]byte], 0, len(fan))
	for _, child := range fan {
		filtered, err := e.filter(child.Scope)
		if err != nil {
			return scope, err
		}
		if filtered.Len() > 0 {
			survivors = append(survivors, tree.Child[[]byte]{Radix: child.Radix, Scope: filtered})
		}
	}
	return scope.WithChildren(survivors)
}
