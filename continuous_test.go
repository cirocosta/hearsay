package hearsay

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/observe"
)

type continuousEvent struct {
	result Gossiped
	err    error
}

func startContinuous(
	ctx context.Context,
	r *Rumors[[]byte],
	carrier *link.Managed,
	options GossipOptions[[]byte],
) (<-chan continuousEvent, <-chan struct{}) {
	events := make(chan continuousEvent, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for result, err := range r.Gossip(ctx, carrier, options) {
			events <- continuousEvent{result: result, err: err}
		}
	}()
	return events, done
}

func nextContinuous(t *testing.T, events <-chan continuousEvent) Gossiped {
	t.Helper()
	select {
	case event := <-events:
		if event.err != nil {
			t.Fatal(event.err)
		}
		return event.result
	case <-time.After(5 * time.Second):
		t.Fatal("continuous driver did not complete a session")
		return Gossiped{}
	}
}

func noContinuousEvent(t *testing.T, events <-chan continuousEvent) {
	t.Helper()
	synctest.Wait()
	select {
	case event := <-events:
		t.Fatalf("unexpected extra session: %+v, %v", event.result, event.err)
	default:
	}
}

func continuousPeers(t *testing.T) (*Rumors[[]byte], *Rumors[[]byte]) {
	t.Helper()
	a := seededBytes(t)
	peer := bootstrapBytes(t, a)
	b, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return a, b
}

func TestContinuousDefaultChangesAndMergeSuppression(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := continuousPeers(t)
		near, far := managedPair(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		left, leftDone := startContinuous(ctx, a, near, GossipOptions[[]byte]{})
		right, rightDone := startContinuous(ctx, b, far, GossipOptions[[]byte]{})
		nextContinuous(t, left)
		nextContinuous(t, right)
		noContinuousEvent(t, left)
		noContinuousEvent(t, right)
		version := observerSend(t, a, "push")
		l, r := nextContinuous(t, left), nextContinuous(t, right)
		if l.Led != LedLocal || r.Led != LedRemote || !b.Snapshot().Contains(version) {
			t.Fatalf("push directions or content: %v %v", l.Led, r.Led)
		}
		noContinuousEvent(t, left)
		noContinuousEvent(t, right)
		if err := a.Redact(version); err != nil {
			t.Fatal(err)
		}
		nextContinuous(t, left)
		nextContinuous(t, right)
		if !b.Snapshot().IsEmpty() {
			t.Fatal("continuous driver did not propagate redaction")
		}
		noContinuousEvent(t, left)
		noContinuousEvent(t, right)
		cancel()
		<-leftDone
		<-rightDone
		if near.State().Poisoned || far.State().Poisoned {
			t.Fatal("idle cancellation poisoned a healthy link")
		}
	})
}

func TestContinuousPolicyRequestsAndGracefulReuse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := continuousPeers(t)
		near, far := managedPair(t)
		leftRequests, rightRequests := make(chan GossipRequest), make(chan GossipRequest)
		leftOptions := GossipOptions[[]byte]{
			When: func(context.Context, *Changes[[]byte]) <-chan GossipRequest { return leftRequests },
		}
		rightOptions := GossipOptions[[]byte]{
			When: func(context.Context, *Changes[[]byte]) <-chan GossipRequest { return rightRequests },
		}
		left, leftDone := startContinuous(t.Context(), a, near, leftOptions)
		right, rightDone := startContinuous(t.Context(), b, far, rightOptions)
		synctest.Wait()
		if near.State().Epoch != 0 || far.State().Epoch != 0 {
			t.Fatal("custom policy initiated without a request")
		}
		leftRequests <- GossipUnconditionally
		l, r := nextContinuous(t, left), nextContinuous(t, right)
		if l.Led != LedLocal || r.Led != LedRemote {
			t.Fatal("remote requests were blocked by custom policy")
		}
		rightRequests <- GossipWhenChanged
		noContinuousEvent(t, left)
		noContinuousEvent(t, right)
		leftRequests <- GossipUnconditionally
		nextContinuous(t, left)
		nextContinuous(t, right)
		version := observerSend(t, a, "conditional")
		noContinuousEvent(t, left)
		leftRequests <- GossipWhenChanged
		nextContinuous(t, left)
		nextContinuous(t, right)
		if !b.Snapshot().Contains(version) {
			t.Fatal("conditional request skipped local news")
		}
		close(leftRequests)
		close(rightRequests)
		<-leftDone
		<-rightDone
		if near.State().Poisoned || far.State().Poisoned {
			t.Fatal("policy exhaustion poisoned idle transport")
		}
		gossipPair(t, a, b, near, far)
	})
}

func TestContinuousDeadlineExcludesIdleAndIncludesPartialPreamble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := seededBytes(t)
		leftRaw, rightRaw := link.MemoryWithCapacity(1)
		near := link.Manage(leftRaw)
		t.Cleanup(func() { _ = near.Close() })
		deadline := 20 * time.Millisecond
		options := GossipOptions[[]byte]{
			When:            func(context.Context, *Changes[[]byte]) <-chan GossipRequest { return nil },
			SessionDeadline: &deadline,
		}
		events, done := startContinuous(t.Context(), r, near, options)
		synctest.Wait()
		time.Sleep(10 * deadline)
		noContinuousEvent(t, events)
		if near.State().Epoch != 0 || near.State().Poisoned {
			t.Fatal("idle deadline changed session state")
		}
		if _, err := rightRaw.Control().Write([]byte{0xd9}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(deadline)
		select {
		case event := <-events:
			if !errors.Is(event.err, context.DeadlineExceeded) {
				t.Fatal("partial preamble did not start active deadline:", event.err)
			}
		case <-time.After(time.Second):
			t.Fatal("partial preamble remained idle")
		}
		<-done
		if state := near.State(); !state.Poisoned || state.Epoch != 1 {
			t.Fatal(state)
		}
	})
}

func TestContinuousZeroDeadlinePoisonsActiveSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := seededBytes(t)
		near, _ := managedPair(t)
		zero := time.Duration(0)
		events, done := startContinuous(
			t.Context(),
			r,
			near,
			GossipOptions[[]byte]{SessionDeadline: &zero},
		)
		event := <-events
		if !errors.Is(event.err, context.DeadlineExceeded) {
			t.Fatal(event.err)
		}
		<-done
		if state := near.State(); !state.Poisoned || state.Epoch != 1 {
			t.Fatal("zero deadline did not enter and poison active session:", state)
		}
	})
}

func TestContinuousConcurrentLocalCommitRetriggers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := continuousPeers(t)
		aRaw, bRaw := link.MemoryWithCapacity(1)
		gate := &greetingGateControl{
			ReadWriteCloser: aRaw.Control(),
			entered:         make(chan struct{}),
			release:         make(chan struct{}),
		}
		near, far := link.Manage(&greetingGateLink{Link: aRaw, control: gate}), link.Manage(bRaw)
		t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		left, leftDone := startContinuous(ctx, a, near, GossipOptions[[]byte]{})
		right, rightDone := startContinuous(ctx, b, far, GossipOptions[[]byte]{})
		<-gate.entered
		version := observerSend(t, a, "during exchange")
		close(gate.release)
		first := nextContinuous(t, left)
		nextContinuous(t, right)
		if version.LessEqual(first.Converged) {
			t.Fatal("opening snapshot claimed a concurrent commit")
		}
		nextContinuous(t, left)
		nextContinuous(t, right)
		if !b.Snapshot().Contains(version) || !a.Snapshot().Equal(b.Snapshot()) {
			t.Fatal("unsent concurrent commit failed to trigger another exchange")
		}
		noContinuousEvent(t, left)
		noContinuousEvent(t, right)
		cancel()
		<-leftDone
		<-rightDone
	})
}

func TestContinuousBreakReleasesHandleAndKeepsLinkReusable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := continuousPeers(t)
		near, far := managedPair(t)
		served := make(chan error, 1)
		go func() { _, err := b.GossipOnce(t.Context(), far); served <- err }()
		for _, err := range a.Gossip(t.Context(), near, GossipOptions[[]byte]{}) {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if err := <-served; err != nil {
			t.Fatal(err)
		}
		peer, err := a.TryIntoPeer()
		if err != nil {
			t.Fatal("driver retained writable handle after break:", err)
		}
		live, err := peer.IntoRumors()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = live.Close() })
		gossipPair(t, live, b, near, far)
	})
}

func TestContinuousClosedPolicyCancelsFactoryAndReleasesIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := seededBytes(t)
		near, _ := managedPair(t)
		requests := make(chan GossipRequest)
		close(requests)
		stopped := make(chan struct{})
		calls := 0
		options := GossipOptions[[]byte]{
			When: func(ctx context.Context, _ *Changes[[]byte]) <-chan GossipRequest {
				calls++
				context.AfterFunc(ctx, func() { close(stopped) })
				return requests
			},
		}
		for _, err := range r.Gossip(t.Context(), near, options) {
			t.Fatal("closed untouched policy yielded an outcome:", err)
		}
		<-stopped
		if calls != 1 || near.State().Epoch != 0 || near.State().Poisoned {
			t.Fatal("closed policy changed the transport or recreated its factory")
		}
		peer, err := r.TryIntoPeer()
		if err != nil {
			t.Fatal("closed policy retained its writable handle:", err)
		}
		_ = peer.Close()
	})
}

func TestContinuousPoisonedLinkFailsBeforeIdleWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := seededBytes(t)
		near, _ := managedPair(t)
		_ = near.Close()
		options := GossipOptions[[]byte]{
			When: func(context.Context, *Changes[[]byte]) <-chan GossipRequest { return nil },
		}
		events, done := startContinuous(t.Context(), r, near, options)
		event := <-events
		if !errors.Is(event.err, link.ErrLinkPoisoned) {
			t.Fatal(event.err)
		}
		<-done
	})
}

type continuousDeadlineObserver struct{ outcomes chan observe.Outcome }

func (o *continuousDeadlineObserver) Session(
	observe.SessionInfo,
) observe.SessionObserver {
	return o
}

func (*continuousDeadlineObserver) Stream(observe.StreamInfo) observe.StreamObserver { return nil }

func (o *continuousDeadlineObserver) Finished(outcome observe.Outcome) {
	o.outcomes <- outcome
}

func TestContinuousDeadlineObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peer, err := Seed(BytesCodec())
		if err != nil {
			t.Fatal(err)
		}
		observer := &continuousDeadlineObserver{outcomes: make(chan observe.Outcome, 2)}
		if err := peer.AttachObserver(observer); err != nil {
			t.Fatal(err)
		}
		r, err := peer.IntoRumors()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		near, _ := managedPair(t)
		zero := time.Duration(0)
		events, done := startContinuous(
			t.Context(),
			r,
			near,
			GossipOptions[[]byte]{SessionDeadline: &zero},
		)
		if event := <-events; !errors.Is(event.err, context.DeadlineExceeded) {
			t.Fatal(event.err)
		}
		<-done
		outcome := <-observer.outcomes
		if outcome.Kind != observe.Failed || outcome.Failure != observe.DeadlineExceeded {
			t.Fatalf("deadline observation: %+v", outcome)
		}
		select {
		case extra := <-observer.outcomes:
			t.Fatalf("duplicate terminal observation: %+v", extra)
		default:
		}
	})
}

type (
	continuousEOFLink    struct{ link.Link }
	continuousEOFControl struct{ io.ReadWriteCloser }
)

func (c continuousEOFControl) Read([]byte) (int, error) { return 0, io.EOF }
func (l continuousEOFLink) Control() io.ReadWriteCloser {
	return continuousEOFControl{ReadWriteCloser: l.Link.Control()}
}

func TestContinuousIdleEOFEndsCleanly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := seededBytes(t)
		raw, _ := link.MemoryWithCapacity(1)
		near := link.Manage(continuousEOFLink{Link: raw})
		t.Cleanup(func() { _ = near.Close() })
		options := GossipOptions[[]byte]{
			When: func(context.Context, *Changes[[]byte]) <-chan GossipRequest { return nil },
		}
		for _, err := range r.Gossip(t.Context(), near, options) {
			t.Fatal("untouched remote EOF yielded an outcome:", err)
		}
		if state := near.State(); state.Epoch != 0 || state.Poisoned {
			t.Fatal("idle EOF entered a session:", state)
		}
	})
}

func TestContinuousActiveCancellationPoisonsAndReleasesIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := seededBytes(t)
		leftRaw, rightRaw := link.MemoryWithCapacity(1)
		near := link.Manage(leftRaw)
		t.Cleanup(func() { _ = near.Close() })
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		options := GossipOptions[[]byte]{
			When: func(context.Context, *Changes[[]byte]) <-chan GossipRequest { return nil },
		}
		events, done := startContinuous(ctx, r, near, options)
		if _, err := rightRaw.Control().Write([]byte{0xd9}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !near.State().Active {
			t.Fatal("partial preamble did not enter an active exchange")
		}
		cancel()
		event := <-events
		if !errors.Is(event.err, context.Canceled) {
			t.Fatal("active cancellation lost its context cause:", event.err)
		}
		<-done
		if state := near.State(); state.Active || !state.Poisoned {
			t.Fatal(state)
		}
		peer, err := r.TryIntoPeer()
		if err != nil {
			t.Fatal("cancelled active driver retained identity ownership:", err)
		}
		_ = peer.Close()
	})
}

func TestContinuousPolicyCloseFinishesActiveExchange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := continuousPeers(t)
		aRaw, bRaw := link.MemoryWithCapacity(1)
		gate := &greetingGateControl{
			ReadWriteCloser: aRaw.Control(),
			entered:         make(chan struct{}),
			release:         make(chan struct{}),
		}
		near, far := link.Manage(&greetingGateLink{Link: aRaw, control: gate}), link.Manage(bRaw)
		t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
		requests := make(chan GossipRequest, 1)
		requests <- GossipUnconditionally
		options := GossipOptions[[]byte]{
			When: func(context.Context, *Changes[[]byte]) <-chan GossipRequest { return requests },
		}
		left, done := startContinuous(t.Context(), a, near, options)
		served := make(chan error, 1)
		go func() { _, err := b.GossipOnce(t.Context(), far); served <- err }()
		<-gate.entered
		close(requests)
		close(gate.release)
		nextContinuous(t, left)
		<-done
		if err := <-served; err != nil {
			t.Fatal(err)
		}
		if near.State().Poisoned || far.State().Poisoned {
			t.Fatal("closing an active policy cancelled its exchange")
		}
		gossipPair(t, a, b, near, far)
	})
}

func TestContinuousInheritsPeerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peer, err := Seed(BytesCodec())
		if err != nil {
			t.Fatal(err)
		}
		zero := time.Duration(0)
		peer, err = peer.WithSessionDeadline(&zero)
		if err != nil {
			t.Fatal(err)
		}
		r, err := peer.IntoRumors()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		near, _ := managedPair(t)
		events, done := startContinuous(t.Context(), r, near, GossipOptions[[]byte]{})
		if event := <-events; !errors.Is(event.err, context.DeadlineExceeded) {
			t.Fatal("continuous driver did not inherit its peer deadline:", event.err)
		}
		<-done
	})
}

func TestContinuousOptionsOverridePeerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := continuousPeers(t)
		peer, err := a.TryIntoPeer()
		if err != nil {
			t.Fatal(err)
		}
		zero := time.Duration(0)
		peer, err = peer.WithSessionDeadline(&zero)
		if err != nil {
			t.Fatal(err)
		}
		a, err = peer.IntoRumors()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = a.Close() })
		near, far := managedPair(t)
		deadline := time.Second
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		left, leftDone := startContinuous(
			ctx,
			a,
			near,
			GossipOptions[[]byte]{SessionDeadline: &deadline},
		)
		right, rightDone := startContinuous(ctx, b, far, GossipOptions[[]byte]{})
		nextContinuous(t, left)
		nextContinuous(t, right)
		noContinuousEvent(t, left)
		noContinuousEvent(t, right)
		cancel()
		<-leftDone
		<-rightDone
		if near.State().Poisoned || far.State().Poisoned {
			t.Fatal("explicit continuous deadline did not override immediate peer deadline")
		}
	})
}
