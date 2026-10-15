package hearsay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/bookmark"
	record "github.com/cirocosta/hearsay/internal/bookmark"
	"github.com/cirocosta/hearsay/link"
)

type bookmarkReviewLink struct {
	link.Link
	control io.ReadWriteCloser
}

func (l *bookmarkReviewLink) Control() io.ReadWriteCloser { return l.control }

type bookmarkReviewControl struct {
	io.ReadWriteCloser
	check func([]byte) error
}

func (c *bookmarkReviewControl) Write(p []byte) (int, error) {
	if err := c.check(p); err != nil {
		return 0, err
	}
	return c.ReadWriteCloser.Write(p)
}

type bookmarkReviewStorage struct {
	bookmark.Memory
	stores atomic.Int64
	loads  atomic.Int64
	fail   atomic.Bool
	pause  atomic.Pointer[bookmarkReviewPause]
}

type bookmarkReviewPause struct {
	entered chan struct{}
	replace bool
}

var errBookmarkReviewStore = errors.New("injected bookmark review store failure")

func (s *bookmarkReviewStorage) Store(ctx context.Context, p []byte) error {
	s.stores.Add(1)
	if s.fail.Load() {
		return errBookmarkReviewStore
	}
	if pause := s.pause.Swap(nil); pause != nil {
		if pause.replace {
			if err := s.Memory.Store(ctx, p); err != nil {
				return err
			}
		}
		close(pause.entered)
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Memory.Store(ctx, p)
}

func (s *bookmarkReviewStorage) Load(ctx context.Context) ([]byte, error) {
	s.loads.Add(1)
	return s.Memory.Load(ctx)
}

func TestBookmarkReviewDonationRemovedBeforeIdentityWrite(t *testing.T) {
	storage := new(bookmarkReviewStorage)
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.AttachBookmark(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	source, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	if _, err := source.Send([]byte("durable before sharing")); err != nil {
		t.Fatal(err)
	}
	a, b := link.MemoryWithCapacity(1)
	inspection := make(chan error, 1)
	control := &bookmarkReviewControl{ReadWriteCloser: a.Control(), check: func(p []byte) error {
		if !bytes.HasPrefix(p, []byte{0xd9, 0xd2, 0x55}) {
			return nil
		}
		err := reviewDonationRecord(source, storage, p)
		inspection <- err
		return err
	}}
	near, far := link.Manage(&bookmarkReviewLink{Link: a, control: control}), link.Manage(b)
	defer func() { _ = near.Close(); _ = far.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, near); served <- err }()
	joined, joinErr := Bootstrap(BytesCodec()).Join(ctx, far)
	serveErr := <-served
	if joinErr != nil || serveErr != nil {
		t.Fatalf("bootstrap: %v %v", joinErr, serveErr)
	}
	if err := <-inspection; err != nil {
		t.Fatal(err)
	}
	if joined.Peer == nil {
		t.Fatal("bootstrap returned no peer")
	}
	defer func() { _ = joined.Peer.Close() }()
	if !source.state.party.IsDisjoint(joined.Peer.state.party) {
		t.Fatal("donation duplicated identity")
	}
}

func reviewDonationRecord(
	source *Rumors[[]byte],
	storage *bookmarkReviewStorage,
	wire []byte,
) error {
	if storage.stores.Load() < 2 {
		return errors.New("identity write preceded durable checkpoint and donation removal")
	}
	donated, err := readParty(bytes.NewReader(wire))
	if err != nil {
		return err
	}
	encoded, err := storage.Load(context.Background())
	if err != nil {
		return err
	}
	durable, err := record.Decode(encoded)
	if err != nil {
		return err
	}
	source.state.mu.Lock()
	raw, err := source.state.party.MarshalBinary()
	frontier := source.state.current.Load().tree.Latest()
	network := [16]byte(source.state.network)
	source.state.mu.Unlock()
	if err != nil {
		return err
	}
	probe, err := before.DecodeParty(raw)
	if err != nil {
		return err
	}
	if err := durable.Reclaim(network, probe, frontier, true); err != nil {
		return err
	}
	if !probe.IsDisjoint(donated) {
		return errors.New("durable recovery still contains outgoing donation")
	}
	return nil
}

func TestBookmarkReviewPendingReservationBlocksOverlappingReclaim(t *testing.T) {
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	if err := peer.AttachBookmark(t.Context(), new(bookmark.Memory)); err != nil {
		t.Fatal(err)
	}
	_, fork, err := peer.state.prepareSession(t.Context(), true, false)
	if err != nil || fork == nil {
		t.Fatalf("reserve: %v", err)
	}
	tx := &sessionTransaction[[]byte]{state: peer.state, fork: fork, reserved: true}
	defer func() {
		if tx.reserved {
			tx.recoverFork()
			tx.reserved = false
		}
	}()
	if err := peer.state.checkpointBookmark(t.Context()); err != nil {
		t.Fatal(err)
	}
	peer.state.mu.Lock()
	disjoint := peer.state.party.IsDisjoint(fork)
	reservations := peer.state.reservations
	peer.state.mu.Unlock()
	if !disjoint || reservations != 1 {
		t.Fatal("checkpoint reclaimed an outstanding speculative donation")
	}
	tx.recoverFork()
	tx.reserved = false
	if !peer.state.party.IsSeed() || peer.state.reservations != 0 {
		t.Fatal("failed donation did not restore exclusive seed identity")
	}
}

func TestBookmarkReviewBootstrapAttachmentFailurePreservesCustody(t *testing.T) {
	source := seededBytes(t)
	storage := new(bookmarkReviewStorage)
	storage.fail.Store(true)
	config := Bootstrap(BytesCodec()).WithBookmark(storage)
	copyConfig := *config
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, near); served <- err }()
	joined, err := config.Join(ctx, far)
	if serveErr := <-served; err != nil || serveErr != nil {
		t.Fatalf("wire bootstrap: %v %v", err, serveErr)
	}
	if joined.Peer == nil || !errors.Is(joined.BookmarkError, errBookmarkReviewStore) {
		t.Fatalf("attachment failure lost peer: %+v", joined)
	}
	if near.State().Poisoned || far.State().Poisoned {
		t.Fatal("post-completion storage failure poisoned clean link")
	}
	otherNear, _ := managedPair(t)
	if _, err := copyConfig.Join(ctx, otherNear); !errors.Is(err, ErrClaimed) {
		t.Fatalf("copied builder reacquired consumed custody: %v", err)
	}
	storage.fail.Store(false)
	if err := joined.Peer.AttachBookmark(ctx, storage); err != nil {
		t.Fatal(err)
	}
	live, err := joined.Peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if _, err := live.Send([]byte("usable after attachment retry")); err != nil {
		t.Fatal(err)
	}
	gossipPair(t, source, live, near, far)
}

func TestBookmarkReviewBootstrapClaimRecoversAfterCancellation(t *testing.T) {
	config := Bootstrap(BytesCodec()).WithBookmark(new(bookmark.Memory))
	copyConfig := *config
	a, b := link.MemoryWithCapacity(1)
	entered := make(chan struct{})
	var once sync.Once
	control := &bookmarkReviewControl{
		ReadWriteCloser: a.Control(),
		check:           func([]byte) error { once.Do(func() { close(entered) }); return nil },
	}
	near, far := link.Manage(&bookmarkReviewLink{Link: a, control: control}), link.Manage(b)
	defer func() { _ = near.Close(); _ = far.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := config.Join(ctx, near); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	otherNear, _ := managedPair(t)
	if _, err := copyConfig.Join(ctx, otherNear); !errors.Is(err, ErrBusy) {
		t.Fatalf("copied builder bypassed in-flight custody: %v", err)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled bootstrap succeeded without provider")
	}
	source := seededBytes(t)
	retryNear, retryFar := managedPair(t)
	retryCtx, retryCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer retryCancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(retryCtx, retryNear); served <- err }()
	joined, err := copyConfig.Join(retryCtx, retryFar)
	if serveErr := <-served; err != nil || serveErr != nil {
		t.Fatalf("retry: %v %v", err, serveErr)
	}
	if joined.Peer == nil || joined.BookmarkError != nil {
		t.Fatalf("retry failed to own bookmarked peer: %v", joined.BookmarkError)
	}
	_ = joined.Peer.Close()
}

func TestBookmarkReviewCancelledDonationReloadsBeforeRetry(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "before replacement", true: "after replacement"}[after],
			func(t *testing.T) {
				storage := new(bookmarkReviewStorage)
				peer, err := Seed(BytesCodec())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = peer.Close() }()
				if err := peer.AttachBookmark(t.Context(), storage); err != nil {
					t.Fatal(err)
				}
				_, fork, err := peer.state.prepareSession(t.Context(), true, false)
				if err != nil || fork == nil {
					t.Fatalf("prepare donation: %v", err)
				}
				tx := &sessionTransaction[[]byte]{state: peer.state, fork: fork, reserved: true}
				defer tx.recoverFork()
				prior, err := storage.Memory.Load(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				pause := &bookmarkReviewPause{entered: make(chan struct{}), replace: after}
				storage.pause.Store(pause)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- peer.state.donateBookmark(ctx, fork) }()
				select {
				case <-pause.entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled donation: %v", err)
				}
				stored, err := storage.Memory.Load(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Equal(stored, prior) == after {
					t.Fatal("injected storage boundary did not take effect")
				}
				loads := storage.loads.Load()
				if err := peer.state.donateBookmark(t.Context(), fork); err != nil {
					t.Fatal(err)
				}
				if storage.loads.Load() != loads+1 {
					t.Fatal("cancelled donation reused speculative record cache")
				}
				wire, err := encodeParty(fork)
				if err != nil {
					t.Fatal(err)
				}
				probeSource := &Rumors[[]byte]{state: peer.state}
				if err := reviewDonationRecord(probeSource, storage, wire); err != nil {
					t.Fatal(err)
				}
				if peer.state.reservations != 1 {
					t.Fatal("storage cancellation released live donation reservation")
				}
			},
		)
	}
}
