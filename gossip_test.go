package hearsay

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
	"github.com/cirocosta/hearsay/link"
)

func managedPair(t *testing.T) (*link.Managed, *link.Managed) {
	t.Helper()
	a, b := link.MemoryWithCapacity(1)
	near, far := link.Manage(a), link.Manage(b)
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	return near, far
}

func bootstrapBytes(t *testing.T, source *Rumors[[]byte]) *Peer[[]byte] {
	t.Helper()
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	joined, err := Bootstrap(BytesCodec()).Join(ctx, near)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if joined.Peer == nil || joined.Retry != nil {
		t.Fatal("join did not create peer")
	}
	return joined.Peer
}

func gossipPair(t *testing.T, a, b *Rumors[[]byte], near, far *link.Managed) (Gossiped, Gossiped) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type outcome struct {
		result Gossiped
		err    error
	}
	finished := make(chan outcome, 1)
	go func() { result, err := a.GossipOnce(ctx, near); finished <- outcome{result, err} }()
	right, err := b.GossipOnce(ctx, far)
	left := <-finished
	if err != nil {
		t.Fatal(err)
	}
	if left.err != nil {
		t.Fatal(left.err)
	}
	return left.result, right
}

func TestBootstrapGossipRedactionAndReuse(t *testing.T) {
	a := seededBytes(t)
	first, err := a.Send([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	peer := bootstrapBytes(t, a)
	b, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	if !a.Snapshot().Equal(b.Snapshot()) {
		t.Fatal("bootstrap snapshot differs")
	}
	if !a.state.party.IsDisjoint(b.state.party) {
		t.Fatal("bootstrap duplicated identity")
	}
	near, far := managedPair(t)
	left, right := gossipPair(t, a, b, near, far)
	for _, result := range []Gossiped{left, right} {
		stats := result.Stats
		stats.Elapsed = 0
		if stats != (SessionStats{}) {
			t.Fatalf("equal frontier counted data work: %+v", result.Stats)
		}
	}
	if near.State().Poisoned || far.State().Poisoned {
		t.Fatal("completed session poisoned link")
	}
	if _, err := b.Send([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := a.Redact(first); err != nil {
		t.Fatal(err)
	}
	left, right = gossipPair(t, a, b, near, far)
	if !left.Converged.Equal(right.Converged) || !a.Snapshot().Equal(b.Snapshot()) {
		t.Fatal("diverged after gossip")
	}
	if a.Snapshot().Contains(first) || a.Snapshot().Len() != 1 {
		t.Fatal("deleted message resurrected")
	}
	gossipPair(t, a, b, near, far)
	if near.State().Epoch != 3 || far.State().Epoch != 3 {
		t.Fatal("session boundary not reusable")
	}
}

func TestMutualBootstrapBailsCleanly(t *testing.T) {
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type outcome struct {
		joined Joined[[]byte]
		err    error
	}
	done := make(chan outcome, 1)
	go func() { joined, err := Bootstrap(BytesCodec()).Join(ctx, near); done <- outcome{joined, err} }()
	right, err := Bootstrap(BytesCodec()).Join(ctx, far)
	left := <-done
	if err != nil || left.err != nil {
		t.Fatalf("mutual bootstrap: %v %v", left.err, err)
	}
	if right.Peer != nil || left.joined.Peer != nil || right.Retry == nil ||
		left.joined.Retry == nil {
		t.Fatal("mutual bootstrap fabricated identity")
	}
	if near.State().Poisoned || far.State().Poisoned {
		t.Fatal("mutual bootstrap did not finish boundary")
	}
}

func TestNetworkMismatchPoisonsWithoutChangingState(t *testing.T) {
	a, b := seededBytes(t), seededBytes(t)
	_, _ = a.Send([]byte("a"))
	_, _ = b.Send([]byte("b"))
	beforeA, beforeB := a.Snapshot(), b.Snapshot()
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := a.GossipOnce(ctx, near); done <- err }()
	_, err := b.GossipOnce(ctx, far)
	leftErr := <-done
	if leftErr == nil || err == nil ||
		(!errors.Is(leftErr, protocol.ErrNetwork) && !errors.Is(err, protocol.ErrNetwork)) {
		t.Fatalf("network mismatch: %v %v", leftErr, err)
	}
	if !beforeA.Equal(a.Snapshot()) || !beforeB.Equal(b.Snapshot()) {
		t.Fatal("network mismatch changed state")
	}
	if !near.State().Poisoned || !far.State().Poisoned {
		t.Fatal("failed links not poisoned")
	}
}

func TestRetireTransfersExclusiveIdentity(t *testing.T) {
	a := seededBytes(t)
	peer := bootstrapBytes(t, a)
	b, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send([]byte("departing")); err != nil {
		t.Fatal(err)
	}
	retiring, err := b.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := a.GossipOnce(ctx, far); served <- err }()
	outcome := retiring.Retire(ctx, near)
	if outcome.Status != Retired || outcome.Peer != nil || outcome.Err != nil {
		t.Fatalf("retirement: %+v", outcome)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if !a.state.party.IsSeed() {
		t.Fatal("retirement did not reunite identity")
	}
	if a.Snapshot().Len() != 1 {
		t.Fatal("retirement lost departing content")
	}
	if _, err := retiring.IntoRumors(); !errors.Is(err, ErrClosed) {
		t.Fatal("retiree reused identity")
	}
}

func TestMutualRetirementDeclinesWithoutMutation(t *testing.T) {
	a := seededBytes(t)
	peer := bootstrapBytes(t, a)
	b, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = a.Send([]byte("a"))
	_, _ = b.Send([]byte("b"))
	snapA, snapB := a.Snapshot(), b.Snapshot()
	pa, err := a.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	pb, err := b.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan RetireResult[[]byte], 1)
	go func() { finished <- pa.Retire(ctx, near) }()
	right, left := pb.Retire(ctx, far), <-finished
	for _, outcome := range []RetireResult[[]byte]{left, right} {
		if outcome.Status != RetirementDeclined || outcome.Peer == nil || outcome.Err != nil {
			t.Fatalf("mutual retire: %+v", outcome)
		}
		defer func() { _ = outcome.Peer.Close() }()
	}
	if !snapA.Equal(a.Snapshot()) || !snapB.Equal(b.Snapshot()) {
		t.Fatal("mutual retirement reconciled content")
	}
}

func TestRetirementFailureBeforeHandoffRecovers(t *testing.T) {
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	_ = far.Close()
	outcome := peer.Retire(t.Context(), near)
	if outcome.Status != RetirementRecovered || outcome.Peer == nil || outcome.Err == nil {
		t.Fatalf("failed retirement: %+v", outcome)
	}
	live, err := outcome.Peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if _, err := live.Send([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
}

type failIdentityLink struct {
	link.Link
	control io.ReadWriteCloser
}

func (l *failIdentityLink) Control() io.ReadWriteCloser { return l.control }

type failIdentityControl struct{ io.ReadWriteCloser }

func (c *failIdentityControl) Write(p []byte) (int, error) {
	n, err := c.ReadWriteCloser.Write(p)
	if bytes.HasPrefix(p, []byte{0xd9, 0xd2, 0x55}) && err == nil {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func TestRetirementFailureAfterHandoffIsUncertain(t *testing.T) {
	a := seededBytes(t)
	peer := bootstrapBytes(t, a)
	nearRaw, farRaw := link.MemoryWithCapacity(1)
	near := link.Manage(
		&failIdentityLink{Link: nearRaw, control: &failIdentityControl{nearRaw.Control()}},
	)
	far := link.Manage(farRaw)
	defer func() { _ = near.Close(); _ = far.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := a.GossipOnce(ctx, far); served <- err }()
	outcome := peer.Retire(ctx, near)
	<-served
	if outcome.Status != RetirementUncertain || outcome.Peer != nil || outcome.Err == nil {
		t.Fatalf("uncertain retirement: %+v", outcome)
	}
	if _, err := peer.IntoRumors(); !errors.Is(err, ErrClosed) {
		t.Fatal("uncertain retiree resumed")
	}
}

func TestConcurrentBootstrapDonationsRemainDisjoint(t *testing.T) {
	a := seededBytes(t)
	const count = 4
	peers := make(chan *Peer[[]byte], count)
	var wg sync.WaitGroup
	wg.Add(count)
	for range count {
		go func() { defer wg.Done(); peers <- bootstrapBytes(t, a) }()
	}
	wg.Wait()
	close(peers)
	all := []*before.Party{a.state.party}
	for p := range peers {
		all = append(all, p.state.party)
		_ = p.Close()
	}
	for i, p := range all {
		for _, q := range all[i+1:] {
			if !p.IsDisjoint(q) {
				t.Fatal("concurrent donations overlap")
			}
		}
	}
}

type greetingGateLink struct {
	link.Link
	control io.ReadWriteCloser
}

func (l *greetingGateLink) Control() io.ReadWriteCloser { return l.control }

type greetingGateControl struct {
	io.ReadWriteCloser
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *greetingGateControl) Write(p []byte) (int, error) {
	if bytes.HasPrefix(p, []byte{0xd8, 24}) {
		c.once.Do(func() { close(c.entered); <-c.release })
	}
	return c.ReadWriteCloser.Write(p)
}

func TestGossipPreservesConcurrentLocalCommits(t *testing.T) {
	a := seededBytes(t)
	old, err := a.Send([]byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	peer := bootstrapBytes(t, a)
	b, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	aRaw, bRaw := link.MemoryWithCapacity(1)
	gate := &greetingGateControl{
		ReadWriteCloser: aRaw.Control(),
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	near, far := link.Manage(&greetingGateLink{Link: aRaw, control: gate}), link.Manage(bRaw)
	defer func() { _ = near.Close(); _ = far.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type outcome struct {
		result Gossiped
		err    error
	}
	leftDone, rightDone := make(chan outcome, 1), make(chan outcome, 1)
	go func() { result, err := a.GossipOnce(ctx, near); leftDone <- outcome{result, err} }()
	go func() { result, err := b.GossipOnce(ctx, far); rightDone <- outcome{result, err} }()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := a.TryIntoPeer(); !errors.Is(err, ErrBusy) {
		t.Fatalf("active session did not retain identity claim: %v", err)
	}
	added, err := a.Send([]byte("concurrent"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Redact(old); err != nil {
		t.Fatal(err)
	}
	close(gate.release)
	left, right := <-leftDone, <-rightDone
	if left.err != nil || right.err != nil {
		t.Fatalf("gossip: %v %v", left.err, right.err)
	}
	if !a.Snapshot().Contains(added) || a.Snapshot().Contains(old) {
		t.Fatal("session publication lost concurrent local changes")
	}
	if left.result.Converged.Equal(a.Snapshot().Latest()) {
		t.Fatal("session outcome claimed concurrent frontier")
	}
	gossipPair(t, a, b, near, far)
	if !a.Snapshot().Equal(b.Snapshot()) || b.Snapshot().Contains(old) ||
		!b.Snapshot().Contains(added) {
		t.Fatal("next session did not converge concurrent changes")
	}
}

type failCompletionControl struct{ io.ReadWriteCloser }

func (c *failCompletionControl) Write(p []byte) (int, error) {
	n, err := c.ReadWriteCloser.Write(p)
	if string(p) == protocol.CompletionMarker && err == nil {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func TestCompletionFailureRetainsCommittedGossip(t *testing.T) {
	a := seededBytes(t)
	peer := bootstrapBytes(t, a)
	b, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	va, err := a.Send([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	vb, err := b.Send([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	aRaw, bRaw := link.MemoryWithCapacity(1)
	near := link.Manage(
		&failIdentityLink{Link: aRaw, control: &failCompletionControl{aRaw.Control()}},
	)
	far := link.Manage(bRaw)
	defer func() { _ = near.Close(); _ = far.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := b.GossipOnce(ctx, far); finished <- err }()
	_, err = a.GossipOnce(ctx, near)
	<-finished
	var sessionErr *SessionError
	if !errors.As(err, &sessionErr) || sessionErr.Phase != "completion" {
		t.Fatalf("completion failure: %v", err)
	}
	if !a.Snapshot().Contains(va) || !a.Snapshot().Contains(vb) {
		t.Fatal("completion failure rolled back local commit")
	}
	if _, err := a.Send([]byte("still live")); err != nil {
		t.Fatal(err)
	}
	if !near.State().Poisoned {
		t.Fatal("failed completion link remained reusable")
	}
}

func TestRetireIntoBootstrapTransfersWholeIdentity(t *testing.T) {
	source := seededBytes(t)
	if _, err := source.Send([]byte("carried")); err != nil {
		t.Fatal(err)
	}
	peer, err := source.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	retired := make(chan RetireResult[[]byte], 1)
	go func() { retired <- peer.Retire(ctx, near) }()
	joined, err := Bootstrap(BytesCodec()).Join(ctx, far)
	if err != nil {
		t.Fatal(err)
	}
	result := <-retired
	if result.Status != Retired || result.Err != nil {
		t.Fatalf("retirement: %+v", result)
	}
	if joined.Peer == nil || !joined.Peer.state.party.IsSeed() {
		t.Fatal("retiring provider did not transfer whole identity")
	}
	live, err := joined.Peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if !live.Snapshot().Equal(source.Snapshot()) {
		t.Fatal("retiring provider content lost")
	}
}
