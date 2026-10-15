package hearsay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/bookmark"
	record "github.com/cirocosta/hearsay/internal/bookmark"
	"github.com/cirocosta/hearsay/internal/tree"
)

// DefaultBookmarkSizeLimit bounds encoded restart bookkeeping, including its frame.
const DefaultBookmarkSizeLimit = record.DefaultSizeLimit

// ErrBookmarkAttached prevents replacing storage belonging to a live peer.
var ErrBookmarkAttached = errors.New("hearsay: bookmark already attached")

type peerBookmark struct {
	gate         chan struct{}
	storage      bookmark.Storage
	cache        *record.Record
	durableParty []byte
	durableOwn   Version
}

func newPeerBookmark(storage bookmark.Storage) *peerBookmark {
	return &peerBookmark{gate: make(chan struct{}, 1), storage: storage}
}

func (b *peerBookmark) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case b.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-b.gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *peerBookmark) unlock() { <-b.gate }

func (b *peerBookmark) load(ctx context.Context) error {
	if b.cache != nil {
		return nil
	}
	encoded, err := b.storage.Load(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if encoded == nil {
		b.cache = new(record.Record)
		return nil
	}
	b.cache, err = record.Decode(encoded)
	return err
}

func (b *peerBookmark) store(ctx context.Context, limit int, party []byte, own Version) error {
	encoded := b.cache.EncodeBounded(limit)
	if err := b.storage.Store(ctx, encoded); err != nil {
		b.cache = nil
		b.durableParty = nil
		return err
	}
	b.durableParty = party
	b.durableOwn = own
	return nil
}

// AttachBookmark attaches restart storage to an exclusively owned peer. On
// failure the peer remains usable without storage and attachment may be retried.
// A pristine seed postpones storage I/O until its first synchronization.
// Never share storage between concurrently live peers or roll back its records.
func (p *Peer[T]) AttachBookmark(ctx context.Context, storage bookmark.Storage) error {
	if p == nil || p.state == nil || storage == nil {
		return ErrClosed
	}
	pristine, limit, err := p.beginBookmarkAttachment()
	if err != nil {
		return err
	}
	s := p.state
	defer func() { s.mu.Lock(); p.token.alive = true; s.mu.Unlock() }()
	b := newPeerBookmark(storage)
	if err := s.persistBookmarkAttachment(ctx, b, limit, pristine); err != nil {
		return fmt.Errorf("hearsay: attach bookmark: %w", err)
	}
	s.mu.Lock()
	s.bookmark = b
	s.mu.Unlock()
	return nil
}

func (p *Peer[T]) beginBookmarkAttachment() (bool, int, error) {
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.token == nil || !p.token.alive {
		return false, 0, ErrClosed
	}
	if s.bookmark != nil {
		return false, 0, ErrBookmarkAttached
	}
	p.token.alive = false
	pristine := s.party.IsSeed() && s.current.Load().tree.Latest().IsEmpty()
	return pristine, s.bookmarkLimit, nil
}

func (s *replica[T]) persistBookmarkAttachment(
	ctx context.Context,
	b *peerBookmark,
	limit int,
	pristine bool,
) error {
	if pristine {
		return ctx.Err()
	}
	if err := b.load(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	err := b.cache.Record([16]byte(s.network), s.party, s.current.Load().tree.Latest())
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return b.store(ctx, limit, nil, Version{})
}

// SetBookmarkSizeLimit sets retention for the next store. It invalidates the
// cached checkpoint so even an unchanged peer applies the new limit.
func (p *Peer[T]) SetBookmarkSizeLimit(limit int) error {
	if p == nil || p.state == nil {
		return ErrClosed
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.token == nil || !p.token.alive {
		return ErrClosed
	}
	if limit < 0 {
		return errors.New("hearsay: negative bookmark size limit")
	}
	s.bookmarkLimit = limit
	if s.bookmark != nil {
		s.bookmark.durableParty = nil
	}
	return nil
}

func (s *replica[T]) bookmarkState() (*peerBookmark, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bookmark, s.bookmarkLimit
}

// prepareSession checkpoints, snapshots, and reserves any new donation fork
// under one replica lock. Existing reservations prevent reclamation.
func (s *replica[T]) prepareSession(
	ctx context.Context,
	peerBootstrap, selfRetiring bool,
) (tree.Tree[[]byte], *before.Party, error) {
	b, limit, err := s.loadSessionBookmark(ctx)
	if err != nil {
		return tree.Tree[[]byte]{}, nil, err
	}
	if b != nil {
		defer b.unlock()
	}
	s.mu.Lock()
	prior := s.current.Load().tree
	var fork *before.Party
	var encoded []byte
	var own Version
	var changed bool
	if b != nil {
		encoded, own, changed = s.recordCheckpointLocked(b, prior.Latest())
	}
	if peerBootstrap && !selfRetiring {
		fork = s.party.Fork()
		s.reservations++
	}
	s.mu.Unlock()
	if b != nil && changed {
		if err := b.store(ctx, limit, encoded, own); err != nil {
			s.recoverBookmarkFork(fork)
			return tree.Tree[[]byte]{}, nil, err
		}
	}
	return prior, fork, nil
}

func (s *replica[T]) loadSessionBookmark(ctx context.Context) (*peerBookmark, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	b, limit := s.bookmarkState()
	if b == nil {
		return nil, limit, nil
	}
	if err := b.lock(ctx); err != nil {
		return nil, 0, err
	}
	if err := b.load(ctx); err != nil {
		b.unlock()
		return nil, 0, err
	}
	return b, limit, nil
}

func (s *replica[T]) recoverBookmarkFork(fork *before.Party) {
	if fork == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.party.Join(fork); err != nil {
		panic(err)
	}
	s.reservations--
}

func (s *replica[T]) recordCheckpointLocked(
	b *peerBookmark,
	frontier Version,
) ([]byte, Version, bool) {
	encoded, err := s.party.MarshalBinary()
	if err != nil {
		panic(err)
	}
	own := frontier.Project(s.party)
	if bytes.Equal(encoded, b.durableParty) && own.Equal(b.durableOwn) {
		return nil, Version{}, false
	}
	if s.reservations == 0 {
		if err := b.cache.Reclaim([16]byte(s.network), s.party, frontier, true); err != nil {
			panic(err)
		}
	}
	if err := b.cache.Record([16]byte(s.network), s.party, frontier); err != nil {
		panic(err)
	}
	encoded, err = s.party.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return encoded, frontier.Project(s.party), true
}

func (s *replica[T]) donateBookmark(ctx context.Context, party *before.Party) error {
	b, limit := s.bookmarkState()
	if b == nil {
		return nil
	}
	if err := b.lock(ctx); err != nil {
		return err
	}
	defer b.unlock()
	if err := b.load(ctx); err != nil {
		return err
	}
	b.cache.Donate([16]byte(s.network), party)
	return b.store(ctx, limit, nil, Version{})
}

func (s *replica[T]) checkpointBookmark(ctx context.Context) error {
	_, _, err := s.prepareSession(ctx, false, false)
	return err
}

type bootstrapBookmark struct {
	mu       sync.Mutex
	storage  bookmark.Storage
	inFlight bool
	consumed bool
}

// WithBookmark selects restart storage for a bootstrap. Copies of the returned
// builder share one custody claim and cannot produce multiple live peers.
func (b *BootstrapConfig[T]) WithBookmark(storage bookmark.Storage) *BootstrapConfig[T] {
	configured := *b
	configured.bookmark = &bootstrapBookmark{storage: storage}
	return &configured
}

// WithBookmarkSizeLimit selects the joined peer's retention limit.
func (b *BootstrapConfig[T]) WithBookmarkSizeLimit(limit int) *BootstrapConfig[T] {
	configured := *b
	configured.bookmarkLimit = limit
	return &configured
}

func (b *BootstrapConfig[T]) beginBookmarkJoin() error {
	if b.bookmarkLimit < 0 {
		return errors.New("hearsay: negative bookmark size limit")
	}
	if b.bookmark == nil {
		return nil
	}
	claim := b.bookmark
	if claim.storage == nil {
		return ErrClosed
	}
	claim.mu.Lock()
	defer claim.mu.Unlock()
	if claim.consumed {
		return ErrClaimed
	}
	if claim.inFlight {
		return ErrBusy
	}
	claim.inFlight = true
	return nil
}

func (b *BootstrapConfig[T]) finishBookmarkJoin(success bool) {
	if b.bookmark == nil {
		return
	}
	claim := b.bookmark
	claim.mu.Lock()
	defer claim.mu.Unlock()
	claim.inFlight = false
	claim.consumed = claim.consumed || success
}

func (b *BootstrapConfig[T]) attachJoinedBookmark(ctx context.Context, p *Peer[T]) error {
	if err := p.SetBookmarkSizeLimit(b.bookmarkLimit); err != nil {
		return err
	}
	if b.bookmark == nil {
		return nil
	}
	return p.AttachBookmark(ctx, b.bookmark.storage)
}
