package hearsay

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/bookmark"
	record "github.com/cirocosta/hearsay/internal/bookmark"
	"github.com/cirocosta/hearsay/internal/tree"
)

type faultBookmark struct {
	data          bookmark.Memory
	mu            sync.Mutex
	loads, stores int
	fail          error
	started       chan struct{}
	release       chan struct{}
}

func (b *faultBookmark) Load(ctx context.Context) ([]byte, error) {
	b.mu.Lock()
	b.loads++
	b.mu.Unlock()
	return b.data.Load(ctx)
}

func (b *faultBookmark) Store(ctx context.Context, value []byte) error {
	b.mu.Lock()
	b.stores++
	failure, started, release := b.fail, b.started, b.release
	b.started = nil
	b.mu.Unlock()
	if started != nil {
		close(started)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if failure != nil {
		return failure
	}
	return b.data.Store(ctx, value)
}

func attachedBytes(t *testing.T, storage bookmark.Storage) *Rumors[[]byte] {
	t.Helper()
	p, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.AttachBookmark(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	r, err := p.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func TestBookmarkPristineSeedDefersIO(t *testing.T) {
	storage := new(faultBookmark)
	r := attachedBytes(t, storage)
	if storage.loads != 0 || storage.stores != 0 {
		t.Fatal("pristine seed performed storage I/O")
	}
	if _, _, err := r.state.prepareSession(t.Context(), false, false); err != nil {
		t.Fatal(err)
	}
	if storage.loads != 1 || storage.stores != 1 {
		t.Fatal("first session did not checkpoint")
	}
	if _, _, err := r.state.prepareSession(t.Context(), false, false); err != nil {
		t.Fatal(err)
	}
	if storage.loads != 1 || storage.stores != 1 {
		t.Fatal("unchanged own progress repeated checkpoint")
	}
	p, err := r.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetBookmarkSizeLimit(0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.state.prepareSession(t.Context(), false, false); err != nil {
		t.Fatal(err)
	}
	encoded, err := storage.data.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, new(record.Record).Encode()) {
		t.Fatal("minimum limit did not evict rights")
	}
}

func TestBookmarkAttachFailurePreservesPeer(t *testing.T) {
	r := seededBytes(t)
	if _, err := r.Send([]byte("before attachment")); err != nil {
		t.Fatal(err)
	}
	p, err := r.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	storage := new(faultBookmark)
	if err := storage.data.Store(t.Context(), []byte{0x00}); err != nil {
		t.Fatal(err)
	}
	attachmentErr := p.AttachBookmark(t.Context(), storage)
	var formatErr *bookmark.FormatError
	if !errors.Is(attachmentErr, bookmark.ErrNotBookmark) || !errors.As(attachmentErr, &formatErr) {
		t.Fatalf("attachment lost public format diagnostics: %v", attachmentErr)
	}
	if p.state.bookmark != nil || !p.token.alive {
		t.Fatal("attachment failure consumed peer")
	}
	if err := storage.data.Clear(t.Context()); err != nil {
		t.Fatal(err)
	}
	storage.fail = errors.New("disk failure")
	if err := p.AttachBookmark(t.Context(), storage); !errors.Is(err, storage.fail) {
		t.Fatal(err)
	}
	if p.state.bookmark != nil || !p.token.alive {
		t.Fatal("failed store attached bookmark")
	}
	storage.fail = nil
	if err := p.AttachBookmark(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	if err := p.AttachBookmark(t.Context(), storage); !errors.Is(err, ErrBookmarkAttached) {
		t.Fatal(err)
	}
	handle, err := p.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handle.Close() }()
	if _, err := handle.Send([]byte("after attachment")); err != nil {
		t.Fatal(err)
	}
}

func TestBookmarkCheckpointCapturesExactSnapshotWithoutBlockingWrites(t *testing.T) {
	storage := new(faultBookmark)
	r := attachedBytes(t, storage)
	first, err := r.Send([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	storage.started, storage.release = make(chan struct{}), make(chan struct{})
	started, release := storage.started, storage.release
	type result struct {
		prior tree.Tree[[]byte]
		err   error
	}
	done := make(chan result, 1)
	go func() { prior, _, err := r.state.prepareSession(t.Context(), false, false); done <- result{prior, err} }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("store did not start")
	}
	sent := make(chan Version, 1)
	go func() {
		version, err := r.Send([]byte("while storing"))
		if err != nil {
			t.Error(err)
		}
		sent <- version
	}()
	var later Version
	select {
	case later = <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("storage held replica lock")
	}
	close(release)
	out := <-done
	if out.err != nil {
		t.Fatal(out.err)
	}
	if !out.prior.Latest().Equal(first) || out.prior.Latest().Equal(later) {
		t.Fatal("session snapshot advanced beyond durable checkpoint")
	}
	expected := new(record.Record)
	if err := expected.Record([16]byte(r.Network()), r.state.party, first); err != nil {
		t.Fatal(err)
	}
	stored, err := storage.data.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, expected.Encode()) {
		t.Fatal("stored frontier differs from snapshot shared")
	}
}

func TestBookmarkFailedCheckpointReloadsAndRecoversFork(t *testing.T) {
	storage := new(faultBookmark)
	r := attachedBytes(t, storage)
	storage.fail = errors.New("store interrupted")
	if _, _, err := r.state.prepareSession(
		t.Context(),
		true,
		false,
	); !errors.Is(
		err,
		storage.fail,
	) {
		t.Fatal(err)
	}
	if r.state.reservations != 0 || !r.state.party.IsSeed() {
		t.Fatal("failed checkpoint leaked reserved fork")
	}
	if r.state.bookmark.cache != nil {
		t.Fatal("failed store retained unconfirmed cache")
	}
	storage.fail = nil
	if _, _, err := r.state.prepareSession(t.Context(), false, false); err != nil {
		t.Fatal(err)
	}
	if storage.loads != 2 {
		t.Fatal("retry did not reload actual durable bytes")
	}
}

func TestBookmarkCheckpointCancellation(t *testing.T) {
	storage := new(faultBookmark)
	r := attachedBytes(t, storage)
	storage.started, storage.release = make(chan struct{}), make(chan struct{})
	started := storage.started
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, _, err := r.state.prepareSession(ctx, true, false); done <- err }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if r.state.reservations != 0 || !r.state.party.IsSeed() || r.state.bookmark.cache != nil {
		t.Fatal("cancelled checkpoint retained speculative state")
	}
	storage.mu.Lock()
	storage.release = nil
	storage.mu.Unlock()
	if _, _, err := r.state.prepareSession(t.Context(), false, false); err != nil {
		t.Fatal(err)
	}
}

func TestBookmarkRestartReclaimsCaughtUpIdentity(t *testing.T) {
	storage := new(bookmark.Memory)
	old := attachedBytes(t, storage)
	remotePeer := bootstrapBytes(t, old)
	remote, err := remotePeer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = remote.Close() }()
	if _, err := old.Send([]byte("durable own write")); err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	gossipPair(t, old, remote, near, far)
	oldRaw, err := old.state.party.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	joinNear, joinFar := managedPair(t)
	done := make(chan error, 1)
	go func() { _, err := remote.GossipOnce(t.Context(), joinFar); done <- err }()
	joined, err := Bootstrap(BytesCodec()).WithBookmark(storage).Join(t.Context(), joinNear)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if joined.BookmarkError != nil {
		t.Fatal(joined.BookmarkError)
	}
	if _, _, err := joined.Peer.state.prepareSession(t.Context(), false, false); err != nil {
		t.Fatal(err)
	}
	prior, err := before.DecodeParty(oldRaw)
	if err != nil {
		t.Fatal(err)
	}
	if !joined.Peer.state.party.Covers(prior) {
		t.Fatal("caught-up restart did not reclaim old identity")
	}
	if !joined.Peer.state.party.IsDisjoint(remote.state.party) {
		t.Fatal("reclaimed donated identity")
	}
}
