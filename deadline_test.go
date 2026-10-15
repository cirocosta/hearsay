package hearsay

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/observe"
)

func TestInheritedZeroDeadlineStartsActiveGossip(t *testing.T) {
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	zero := time.Duration(0)
	if _, err := peer.WithSessionDeadline(&zero); err != nil {
		t.Fatal(err)
	}
	zero = time.Hour
	recorder := &wireRecorder{}
	if err := peer.AttachObserver(recorder); err != nil {
		t.Fatal(err)
	}
	live, err := peer.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if configured := live.SessionDeadline(); configured == nil || *configured != 0 {
		t.Fatal("configured duration was not copied")
	}
	near, _ := managedPair(t)
	_, err = live.GossipOnce(t.Context(), near)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("immediate gossip deadline: %v", err)
	}
	if state := near.State(); state.Epoch != 1 || !state.Poisoned || state.Active {
		t.Fatalf("zero deadline bypassed active session: %+v", state)
	}
	session := recorder.last(t)
	session.mu.Lock()
	defer session.mu.Unlock()
	if len(session.outcomes) != 1 || session.outcomes[0].Kind != observe.Failed ||
		session.outcomes[0].Failure != observe.DeadlineExceeded {
		t.Fatalf("deadline observation: %+v", session.outcomes)
	}
	if _, err := live.Send([]byte("still live")); err != nil {
		t.Fatal(err)
	}
}

func TestInheritedZeroBootstrapDeadlinePreservesRetry(t *testing.T) {
	zero := time.Duration(0)
	config := Bootstrap(BytesCodec()).WithSessionDeadline(&zero)
	near, _ := managedPair(t)
	joined, err := config.Join(t.Context(), near)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("immediate join deadline: %v", err)
	}
	if joined.Peer != nil || joined.Retry != config {
		t.Fatal("failed bootstrap lost retry configuration")
	}
	if state := near.State(); state.Epoch != 1 || !state.Poisoned {
		t.Fatalf("join deadline bypassed active session: %+v", state)
	}
}

func TestInheritedZeroRetirementDeadlineRecoversIdentity(t *testing.T) {
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	zero := time.Duration(0)
	if _, err := peer.WithSessionDeadline(&zero); err != nil {
		t.Fatal(err)
	}
	near, _ := managedPair(t)
	result := peer.Retire(t.Context(), near)
	if result.Status != RetirementRecovered || result.Peer == nil ||
		!errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("retirement deadline: %+v", result)
	}
	if _, err := peer.IntoReplica(); !errors.Is(err, ErrClosed) {
		t.Fatal("consumed retirement token became usable")
	}
	if _, err := result.Peer.WithSessionDeadline(nil); err != nil {
		t.Fatal(err)
	}
	if result.Peer.SessionDeadline() != nil {
		t.Fatal("nil did not disable deadline")
	}
	live, err := result.Peer.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if _, err := live.Send([]byte("recovered")); err != nil {
		t.Fatal(err)
	}
}

type deadlineCompletionLink struct {
	link.Link
	control *deadlineCompletionControl
	once    sync.Once
}
type deadlineCompletionControl struct {
	io.ReadWriteCloser
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (l *deadlineCompletionLink) Control() io.ReadWriteCloser { return l.control }
func (l *deadlineCompletionLink) Close() error {
	l.once.Do(func() { close(l.control.closed) })
	return l.Link.Close()
}

func (c *deadlineCompletionControl) Write(p []byte) (int, error) {
	if string(p) == "\x61." {
		c.once.Do(func() { close(c.entered) })
		<-c.closed
		return 0, io.ErrClosedPipe
	}
	return c.ReadWriteCloser.Write(p)
}

func TestInheritedRetirementDeadlineAfterHandoffIsUncertain(t *testing.T) {
	source := seededBytes(t)
	peer := bootstrapBytes(t, source)
	duration := 500 * time.Millisecond
	if _, err := peer.WithSessionDeadline(&duration); err != nil {
		t.Fatal(err)
	}
	a, b := link.MemoryWithCapacity(1)
	control := &deadlineCompletionControl{
		ReadWriteCloser: a.Control(),
		entered:         make(chan struct{}),
		closed:          make(chan struct{}),
	}
	near, far := link.Manage(&deadlineCompletionLink{Link: a, control: control}), link.Manage(b)
	defer func() { _ = near.Close(); _ = far.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	outcome := peer.Retire(ctx, near)
	<-served
	select {
	case <-control.entered:
	default:
		t.Fatal("retirement deadline expired before handoff acceptance test")
	}
	if outcome.Status != RetirementUncertain || outcome.Peer != nil ||
		!errors.Is(outcome.Err, context.DeadlineExceeded) {
		t.Fatalf("retirement completion deadline: %+v", outcome)
	}
	if !source.state.party.IsSeed() {
		t.Fatal("recipient lost accepted identity after unconfirmed completion")
	}
}

func TestBootstrapDeadlineStopsBeforeBookmarkAttachment(t *testing.T) {
	source := seededBytes(t)
	near, far := managedPair(t)
	duration := 500 * time.Millisecond
	storage := &faultBookmark{started: make(chan struct{}), release: make(chan struct{})}
	entered := storage.started
	config := Bootstrap(BytesCodec()).WithSessionDeadline(&duration).WithBookmark(storage)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	type outcome struct {
		joined Joined[[]byte]
		err    error
	}
	finished := make(chan outcome, 1)
	go func() { joined, err := config.Join(ctx, near); finished <- outcome{joined, err} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if near.State().Poisoned || far.State().Poisoned {
		t.Fatal("bookmark attachment retained active session")
	}
	select {
	case early := <-finished:
		t.Fatalf("bookmark attachment did not wait for storage: %+v", early)
	case <-time.After(duration + 50*time.Millisecond):
	}
	close(storage.release)
	result := <-finished
	if result.err != nil || result.joined.BookmarkError != nil || result.joined.Peer == nil {
		t.Fatalf("post-session bookmark attachment: %+v %v", result.joined, result.err)
	}
	joinedPeer := result.joined.Peer
	defer func() { _ = joinedPeer.Close() }()
	if inherited := joinedPeer.SessionDeadline(); inherited == nil || *inherited != duration {
		t.Fatal("joined peer did not inherit deadline")
	}
}
