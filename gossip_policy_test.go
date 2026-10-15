package hearsay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/bookmark"
)

type policyInvocation struct {
	ctx     context.Context
	changes *Changes[[]byte]
	first   PollState
}

func closedGossipPolicy(calls chan<- policyInvocation) GossipWhen[[]byte] {
	return func(ctx context.Context, changes *Changes[[]byte]) <-chan GossipRequest {
		calls <- policyInvocation{ctx, changes, changes.TryNext()}
		ended := make(chan GossipRequest)
		close(ended)
		return ended
	}
}

func driveEndedPolicy(t *testing.T, peer *Rumors[[]byte]) {
	t.Helper()
	near, _ := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, err := range peer.Gossip(ctx, near, GossipOptions[[]byte]{}) {
		t.Fatalf("ended inherited policy yielded an exchange: %v", err)
	}
	if state := near.State(); state.Epoch != 0 || state.Poisoned {
		t.Fatalf("ended policy touched wire: %+v", state)
	}
}

func oneGossipPolicyExchange(
	t *testing.T,
	source, target *Rumors[[]byte],
	options GossipOptions[[]byte],
) Gossiped {
	t.Helper()
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	var completed Gossiped
	yielded := false
	for result, err := range target.Gossip(ctx, near, options) {
		if err != nil {
			t.Fatal(err)
		}
		completed = result
		yielded = true
		break
	}
	if !yielded {
		t.Fatal("connection policy ended before an exchange")
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	return completed
}

func TestGossipFactorySurvivesCloneBookmarkAndReunion(t *testing.T) {
	calls := make(chan policyInvocation, 3)
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.WithGossipWhen(closedGossipPolicy(calls)); err != nil {
		t.Fatal(err)
	}
	if err := peer.AttachBookmark(t.Context(), new(bookmark.Memory)); err != nil {
		t.Fatal(err)
	}
	live, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	clone, err := live.Clone()
	if err != nil {
		t.Fatal(err)
	}
	driveEndedPolicy(t, live)
	driveEndedPolicy(t, clone)
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := live.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := recovered.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	driveEndedPolicy(t, restored)
	close(calls)
	seen := make(map[*Changes[[]byte]]bool)
	for call := range calls {
		if call.first != Ready || seen[call.changes] {
			t.Fatal("connections shared a consumed Changes subscription")
		}
		seen[call.changes] = true
		select {
		case <-call.ctx.Done():
		default:
			t.Fatal("ended connection did not stop factory context")
		}
	}
	if len(seen) != 3 {
		t.Fatalf("connection factory calls: %d", len(seen))
	}
	if _, err := recovered.WithGossipWhen(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("consumed peer changed policy: %v", err)
	}
}

func TestBootstrapFactorySurvivesConfigurationAndAttachment(t *testing.T) {
	calls := make(chan policyInvocation, 2)
	initial := Bootstrap(BytesCodec()).WithGossipWhen(closedGossipPolicy(calls))
	config := initial.WithSessionDeadline(nil).WithObserver(nil).WithBookmark(new(bookmark.Memory))
	config, err := config.WithSynchronizationSettings(config.SynchronizationSettings())
	if err != nil {
		t.Fatal(err)
	}
	source := seededBytes(t)
	if _, err := source.Send([]byte("joined")); err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	joined, err := config.Join(ctx, near)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if joined.Peer == nil || joined.BookmarkError != nil {
		t.Fatalf("joined bookmark: %+v", joined)
	}
	if len(calls) != 0 {
		t.Fatal("bootstrap invoked continuous initiation factory")
	}
	live, err := joined.Peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	driveEndedPolicy(t, live)
	if len(calls) != 1 {
		t.Fatal("joined peer lost configured initiation factory")
	}
	if !source.Snapshot().Equal(live.Snapshot()) {
		t.Fatal("factory configuration affected bootstrap content")
	}
}

func TestPerConnectionGossipFactoryOverridesInheritedPolicy(t *testing.T) {
	source := seededBytes(t)
	peer := bootstrapBytes(t, source)
	inherited := make(chan policyInvocation, 1)
	if _, err := peer.WithGossipWhen(closedGossipPolicy(inherited)); err != nil {
		t.Fatal(err)
	}
	live, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	overrides := make(chan policyInvocation, 1)
	when := func(ctx context.Context, changes *Changes[[]byte]) <-chan GossipRequest {
		overrides <- policyInvocation{ctx, changes, changes.TryNext()}
		requests := make(chan GossipRequest, 1)
		requests <- GossipUnconditionally
		close(requests)
		return requests
	}
	oneGossipPolicyExchange(t, source, live, GossipOptions[[]byte]{When: when})
	if len(inherited) != 0 || len(overrides) != 1 {
		t.Fatal("connection override also invoked inherited factory")
	}
	if call := <-overrides; call.first != Ready {
		t.Fatal("override did not receive a fresh change subscription")
	}
}

func TestInheritedPassiveGossipPolicyStillServesRemoteAndExplicitCalls(t *testing.T) {
	source := seededBytes(t)
	peer := bootstrapBytes(t, source)
	calls := make(chan policyInvocation, 2)
	when := func(ctx context.Context, changes *Changes[[]byte]) <-chan GossipRequest {
		calls <- policyInvocation{ctx, changes, changes.TryNext()}
		return nil
	}
	if _, err := peer.WithGossipWhen(when); err != nil {
		t.Fatal(err)
	}
	live, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if _, err := source.Send([]byte("remote")); err != nil {
		t.Fatal(err)
	}
	result := oneGossipPolicyExchange(t, source, live, GossipOptions[[]byte]{})
	if result.Led != LedRemote || !source.Snapshot().Equal(live.Snapshot()) {
		t.Fatal("passive inherited policy blocked remote initiation")
	}
	near, far := managedPair(t)
	gossipPair(t, source, live, near, far)
	if len(calls) != 1 {
		t.Fatal("explicit gossip invoked continuous factory")
	}
}

func TestNilGossipFactoryRestoresDefault(t *testing.T) {
	source := seededBytes(t)
	peer := bootstrapBytes(t, source)
	calls := make(chan policyInvocation, 1)
	if _, err := peer.WithGossipWhen(closedGossipPolicy(calls)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.WithGossipWhen(nil); err != nil {
		t.Fatal(err)
	}
	live, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	oneGossipPolicyExchange(t, source, live, GossipOptions[[]byte]{})
	if len(calls) != 0 {
		t.Fatal("cleared initiation factory was invoked")
	}
}
