// Package hearsay provides a causally versioned replicated message set.
package hearsay

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
	"github.com/cirocosta/hearsay/internal/tree"
)

// Version identifies an event and its causal past. Its zero value is empty.
type Version = before.Version

// Network identifies a causal universe. Independently seeded networks cannot merge.
type Network [16]byte

func (n Network) String() string { return hex.EncodeToString(n[:]) }

// Errors report handle ownership and payload admission failures.
var (
	ErrClosed     = errors.New("hearsay: handle is closed or consumed")
	ErrBusy       = errors.New("hearsay: other handles remain")
	ErrClaimed    = errors.New("hearsay: peer already reclaimed")
	ErrCodec      = errors.New("hearsay: invalid payload codec")
	ErrUnfaithful = errors.New("hearsay: payload does not survive codec round trip")
)

// DefaultPayloadDepthLimit bounds typed payload decoder recursion at admission.
const DefaultPayloadDepthLimit = 256

// Codec defines the owned CBOR representation of a payload. Functions must be
// safe for concurrent calls. Encode and DecodeBounded handle exactly one CBOR
// item. DecodeBounded enforces typed recursion under the supplied limit; tags
// skipped by the type do not consume it. Equal defaults to reflect.DeepEqual.
// The replica copies buffers and gives DecodeBounded an independent copy.
// Readback uses an unlimited bound because stored payloads are already admitted.
type Codec[T any] struct {
	Encode            func(T) ([]byte, error)
	DecodeBounded     func([]byte, uint64) (T, error)
	Equal             func(T, T) bool
	PayloadDepthLimit uint64
}

// BytesCodec stores opaque binary messages.
func BytesCodec() Codec[[]byte] {
	return Codec[[]byte]{Encode: func(p []byte) ([]byte, error) {
		return append(cbor.AppendHead(nil, cbor.MajorBytes, uint64(len(p))), p...), nil
	}, DecodeBounded: decodeBytesBounded, Equal: bytes.Equal}
}

func decodeBytesBounded(p []byte, limit uint64) ([]byte, error) {
	return cbor.DecodeByteStringBounded(p, limit)
}

// JSONCodec stores JSON inside a CBOR byte string and rejects trailing data.
// Its wire payload type is a byte string containing JSON, rather than a CBOR map.
func JSONCodec[T any]() Codec[T] {
	return Codec[T]{Encode: encodeJSON[T], DecodeBounded: decodeJSON[T]}
}

func encodeJSON[T any](v T) ([]byte, error) {
	p, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(cbor.AppendHead(nil, cbor.MajorBytes, uint64(len(p))), p...), nil
}

func decodeJSON[T any](p []byte, limit uint64) (T, error) {
	var v T
	raw, err := decodeBytesBounded(p, limit)
	if err != nil {
		return v, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := d.Decode(&v); err != nil {
		return v, err
	}
	var extra any
	err = d.Decode(&extra)
	if err == io.EOF {
		return v, nil
	}
	if err == nil {
		err = errors.New("multiple JSON values")
	}
	return v, err
}

func (c Codec[T]) encode(v T) ([]byte, error) {
	limit := c.PayloadDepthLimit
	if limit == 0 {
		limit = DefaultPayloadDepthLimit
	}
	return c.encodeBounded(v, limit)
}

func (c Codec[T]) encodeBounded(v T, limit uint64) ([]byte, error) {
	if c.Encode == nil || c.DecodeBounded == nil {
		return nil, ErrCodec
	}
	p, err := c.Encode(v)
	if err != nil {
		return nil, fmt.Errorf("hearsay: encode payload: %w", err)
	}
	owned := bytes.Clone(p)
	if err := cbor.ValidateWellFormed(owned); err != nil {
		return nil, fmt.Errorf("hearsay: admit CBOR payload: %w", err)
	}
	decoded, err := c.DecodeBounded(bytes.Clone(owned), limit)
	if err != nil {
		return nil, fmt.Errorf("hearsay: admit payload: %w", err)
	}
	faithful := false
	if c.Equal != nil {
		faithful = c.Equal(v, decoded)
	} else {
		faithful = reflect.DeepEqual(v, decoded)
	}
	if !faithful {
		return nil, ErrUnfaithful
	}
	return owned, nil
}

func (c Codec[T]) decode(p []byte) (T, error) { return c.DecodeBounded(bytes.Clone(p), ^uint64(0)) }

type published struct {
	tree    tree.Tree[[]byte]
	changed chan struct{}
}

type replica[T any] struct {
	mu           sync.Mutex
	network      Network
	party        *before.Party
	codec        Codec[T]
	current      atomic.Pointer[published]
	generation   *generation
	settings     SynchronizationSettings
	reservations uint64
	done         chan struct{}
	ended        bool
}

type generation struct {
	refs    int
	done    chan struct{}
	claimed bool
	waiters int
}
type (
	peerToken   struct{ alive bool }
	handleToken struct{ alive bool }
)

// Peer holds exclusive identity ownership. Copies share one consumption token.
// IntoRumors consumes the peer; TryIntoPeer or IntoPeer recovers it later.
type Peer[T any] struct {
	state *replica[T]
	token *peerToken
}

// Rumors is a synchronized writable replica handle. Ordinary Go copies share
// one lifetime token. Clone creates an independently closable handle; Close
// releases it. Snapshots retain content without retaining identity ownership.
type Rumors[T any] struct {
	state      *replica[T]
	token      *handleToken
	generation *generation
}

// Seed creates the first peer of a new randomly identified network.
func Seed[T any](codec Codec[T]) (*Peer[T], error) { return SeedFrom(rand.Reader, codec) }

// SeedFrom reads the network identifier from entropy. Use a cryptographically
// secure source in production; repeated identifiers violate network isolation.
func SeedFrom[T any](entropy io.Reader, codec Codec[T]) (*Peer[T], error) {
	if codec.Encode == nil || codec.DecodeBounded == nil {
		return nil, ErrCodec
	}
	var network Network
	for range 2 {
		if _, err := io.ReadFull(entropy, network[:]); err != nil {
			return nil, fmt.Errorf("hearsay: network entropy: %w", err)
		}
		if network != (Network{}) {
			return newPeer(network, before.SeedParty(), tree.Tree[[]byte]{}, codec), nil
		}
	}
	return nil, errors.New("hearsay: entropy repeatedly produced reserved zero network")
}

func newPeer[T any](
	network Network,
	party *before.Party,
	t tree.Tree[[]byte],
	codec Codec[T],
) *Peer[T] {
	settings := defaultSettings(codec.PayloadDepthLimit)
	codec.PayloadDepthLimit = settings.PayloadDepthLimit
	s := &replica[T]{
		network:  network,
		party:    party,
		codec:    codec,
		settings: settings,
		done:     make(chan struct{}),
	}
	s.current.Store(&published{tree: t, changed: make(chan struct{})})
	return &Peer[T]{state: s, token: &peerToken{alive: true}}
}

// Network returns the causal universe of this peer.
func (p *Peer[T]) Network() Network { return p.state.network }

// IntoRumors consumes exclusive peer ownership and creates one live handle.
func (p *Peer[T]) IntoRumors() (*Rumors[T], error) {
	if p == nil || p.state == nil || p.token == nil {
		return nil, ErrClosed
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if !p.token.alive {
		return nil, ErrClosed
	}
	p.token.alive = false
	g := &generation{refs: 1, done: make(chan struct{})}
	s.generation = g
	return &Rumors[T]{state: s, token: &handleToken{alive: true}, generation: g}, nil
}

func (r *Rumors[T]) aliveLocked() bool {
	return r != nil && r.token != nil && r.token.alive && r.generation == r.state.generation
}

// Network returns the causal universe shared by this replica.
func (r *Rumors[T]) Network() Network { return r.state.network }

// Clone creates another live handle. Close each independently cloned handle.
func (r *Rumors[T]) Clone() (*Rumors[T], error) {
	if r == nil || r.state == nil {
		return nil, ErrClosed
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if !r.aliveLocked() {
		return nil, ErrClosed
	}
	r.generation.refs++
	return &Rumors[T]{
		state:      r.state,
		token:      &handleToken{alive: true},
		generation: r.generation,
	}, nil
}

func (r *Rumors[T]) releaseLocked() {
	r.token.alive = false
	r.generation.refs--
	if r.generation.refs == 0 {
		close(r.generation.done)
	}
}

// Close releases this handle. Repeated calls and closes through copies are safe.
func (r *Rumors[T]) Close() error {
	if r == nil || r.state == nil {
		return nil
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if r.aliveLocked() {
		r.releaseLocked()
		if r.generation.refs == 0 && r.generation.waiters == 0 && !r.generation.claimed {
			r.state.finishLocked()
		}
	}
	return nil
}

func (r *Rumors[T]) claimLocked() (*Peer[T], error) {
	if r.generation.claimed {
		return nil, ErrClaimed
	}
	r.generation.claimed = true
	return &Peer[T]{state: r.state, token: &peerToken{alive: true}}, nil
}

// TryIntoPeer consumes the handle only if no other live handle remains.
// ErrBusy leaves this handle usable. Exactly one caller can recover the peer.
func (r *Rumors[T]) TryIntoPeer() (*Peer[T], error) {
	if r == nil || r.state == nil {
		return nil, ErrClosed
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if !r.aliveLocked() {
		return nil, ErrClosed
	}
	if r.generation.refs != 1 {
		return nil, ErrBusy
	}
	r.releaseLocked()
	return r.claimLocked()
}

// IntoPeer consumes this handle and waits for all independently cloned handles
// to close or enter IntoPeer. Cancellation abandons this caller's claim; the
// consumed handle remains closed. Among waiting callers exactly one succeeds.
func (r *Rumors[T]) IntoPeer(ctx context.Context) (*Peer[T], error) {
	if r == nil || r.state == nil {
		return nil, ErrClosed
	}
	s := r.state
	s.mu.Lock()
	if !r.aliveLocked() {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	r.generation.waiters++
	r.releaseLocked()
	done := r.generation.done
	s.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r.generation.waiters--
	if err := ctx.Err(); err != nil {
		if r.generation.refs == 0 && r.generation.waiters == 0 && !r.generation.claimed {
			s.finishLocked()
		}
		return nil, err
	}
	return r.claimLocked()
}

// Snapshot captures a consistent immutable view in constant time. It remains
// readable after this handle closes and after the peer is reclaimed.
func (r *Rumors[T]) Snapshot() Snapshot[T] {
	return Snapshot[T]{
		network: r.state.network,
		tree:    r.state.current.Load().tree,
		codec:   r.state.codec,
	}
}

func (s *replica[T]) publishLocked(t tree.Tree[[]byte]) {
	prior := s.current.Load()
	if prior.tree.SameRoot(t) {
		return
	}
	next := &published{tree: t, changed: make(chan struct{})}
	s.current.Store(next)
	close(prior.changed)
}

// Close abandons this exclusive peer and ends its observers. A consumed peer
// cannot close the replica now owned by Rumors handles or another peer token.
func (p *Peer[T]) Close() error {
	if p == nil || p.state == nil {
		return nil
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.token != nil && p.token.alive {
		p.token.alive = false
		p.state.finishLocked()
	}
	return nil
}

func (s *replica[T]) finishLocked() {
	if !s.ended {
		s.ended = true
		close(s.done)
	}
}

func (s *replica[T]) observation() (*published, <-chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.Load(), s.done, s.ended
}
