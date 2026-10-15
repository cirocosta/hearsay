package hearsay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/reconcile"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/observe"
)

// SessionStats records reconciliation traffic and work on this endpoint.
type SessionStats = reconcile.Stats

type replayControl struct {
	io.ReadWriteCloser
	prefix []byte
}

type controlArrival struct {
	ahead []byte
	err   error
}

func (c *replayControl) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}

	return c.ReadWriteCloser.Read(p)
}

func (c *replayControl) watch(ctx context.Context, carrier *sessionWire) func() error {
	watchCtx, cancel := context.WithCancel(ctx)
	ready := make(chan controlArrival, 1)
	go func() {
		var ahead []byte
		var buffer [64]byte
		for {
			n, err := carrier.ReadControlContext(watchCtx, buffer[:])
			ahead = append(ahead, buffer[:n]...)
			if err != nil {
				if watchCtx.Err() == nil {
					_ = carrier.Abort(err)
				} else {
					err = nil
				}
				ready <- controlArrival{ahead, err}
				return
			}
		}
	}()
	return func() error {
		cancel()
		arrival := <-ready
		c.prefix = append(c.prefix, arrival.ahead...)
		return arrival.err
	}
}

type sessionWire struct {
	*link.Session
	control     *replayControl
	observation *observe.Session
}

func (s *sessionWire) Control() io.ReadWriteCloser { return s.control }

func beginSession(ctx context.Context, carrier *link.Managed) (*sessionWire, error) {
	s, err := carrier.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &sessionWire{Session: s, control: &replayControl{ReadWriteCloser: s.Control()}}, nil
}

// Led identifies the session trigger on this endpoint.
type Led uint8

// Session trigger directions do not affect protocol role election.
const (
	LedLocal Led = iota
	LedRemote
)

// Gossiped confirms both endpoints committed the reconciled snapshot. Local
// writes made during the exchange may advance beyond Converged before return.
type Gossiped struct {
	Converged Version
	Led       Led
	Stats     SessionStats
	// Elapsed mirrors Stats.Elapsed for the full session.
	Elapsed time.Duration
}

// SessionError identifies the phase that failed. Completion errors can follow
// a successful local commit; they do not roll that commit back.
type SessionError struct {
	Phase string
	Err   error
}

func (e *SessionError) Error() string { return fmt.Sprintf("hearsay %s: %v", e.Phase, e.Err) }
func (e *SessionError) Unwrap() error { return e.Err }
func phaseError(phase string, err error) error {
	if err == nil {
		return nil
	}
	return &SessionError{phase, err}
}

// BootstrapConfig retains payload and synchronization settings across retries.
// Copies can make independent attempts when no bookmark storage is attached.
type BootstrapConfig[T any] struct {
	codec           Codec[T]
	settings        SynchronizationSettings
	bookmark        *bootstrapBookmark
	bookmarkLimit   int
	observers       observe.Attachment
	sessionDeadline *time.Duration
}

// Bootstrap configures joining a network through any established peer.
func Bootstrap[T any](codec Codec[T]) *BootstrapConfig[T] {
	return &BootstrapConfig[T]{
		codec:         codec,
		settings:      defaultSettings(codec.PayloadDepthLimit),
		bookmarkLimit: DefaultBookmarkSizeLimit,
	}
}

// Joined returns either a live peer or the configuration for another attempt.
// A nil error with Retry set means both endpoints were bootstrapping.
type Joined[T any] struct {
	Peer          *Peer[T]
	Retry         *BootstrapConfig[T]
	BookmarkError error
}

// Join receives the provider's content and identity. It exposes a new peer only
// after both endpoints confirm completion. Failure poisons the managed link;
// Retry retains the configuration for an attempt through a fresh link.
func (b *BootstrapConfig[T]) Join(ctx context.Context, carrier *link.Managed) (Joined[T], error) {
	retry := Joined[T]{Retry: b}
	if !b.validCodec() {
		return retry, ErrCodec
	}
	if err := b.beginBookmarkJoin(); err != nil {
		return retry, err
	}
	joinedSuccessfully := false
	defer func() { b.finishBookmarkJoin(joinedSuccessfully) }()
	activeCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	session, err := beginSession(activeCtx, carrier)
	if err != nil {
		return retry, err
	}
	defer func() { _ = session.Close() }()
	peer, err := runSessionDeadline(
		activeCtx,
		b.sessionDeadline,
		cancel,
		func() (*Peer[T], error) { return b.joinSession(activeCtx, session) },
	)
	if err != nil {
		return retry, err
	}
	if peer == nil {
		return retry, nil
	}
	joinedSuccessfully = true
	bookmarkErr := b.attachJoinedBookmark(ctx, peer)
	return Joined[T]{Peer: peer, BookmarkError: bookmarkErr}, nil
}

func (b *BootstrapConfig[T]) joinSession(
	ctx context.Context,
	session *sessionWire,
) (peer *Peer[T], err error) {
	started := time.Now()
	session.observation = b.observers.Begin(observe.Bootstrap)
	var content reconcile.Result
	defer func() {
		stats := content.Stats
		stats.Elapsed = time.Since(started)
		finishWireObservation(
			ctx,
			session,
			Gossiped{Converged: content.Tree.Latest(), Stats: stats},
			err,
		)
	}()
	remote, err := exchangePreamble(session, protocol.Preamble{})
	if err != nil {
		return nil, phaseError("preamble", err)
	}
	content, err = runContent(
		ctx,
		session,
		tree.Tree[[]byte]{},
		b.codec,
		b.settings,
		protocol.Network{},
		remote.Network,
	)
	if err != nil {
		return nil, err
	}
	donated, err := receiveBootstrapParty(session, remote.Network)
	if err != nil {
		return nil, phaseError("identity transfer", err)
	}
	if err = completeSession(session); err != nil {
		return nil, phaseError("completion", err)
	}
	if donated == nil {
		return nil, nil
	}
	codec := b.codec
	codec.PayloadDepthLimit = b.settings.PayloadDepthLimit
	peer = newPeer(Network(remote.Network), donated, content.Tree, codec)
	peer.state.settings = b.settings
	peer.state.observers = b.observers
	peer.state.sessionDeadline = b.sessionDeadline
	return peer, nil
}

// RetirementStatus determines whether identity can be used after an attempt.
type RetirementStatus uint8

// Retirement outcomes distinguish a safe retry from an uncertain handoff.
const (
	Retired RetirementStatus = iota
	RetirementDeclined
	RetirementRecovered
	RetirementUncertain
)

// RetireResult returns a peer only when handoff did not begin. Uncertain means
// the remote may own its identity, so callers must never restore or reuse it.
type RetireResult[T any] struct {
	Status RetirementStatus
	Peer   *Peer[T]
	Err    error
}

// GossipOnce runs one complete session. Established handles also serve joiners
// and accept retiring peers. Each active session holds its own producer claim.
func (r *Rumors[T]) GossipOnce(ctx context.Context, carrier *link.Managed) (Gossiped, error) {
	hold, err := r.Clone()
	if err != nil {
		return Gossiped{}, err
	}
	defer func() { _ = hold.Close() }()
	activeCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	session, err := beginSession(activeCtx, carrier)
	if err != nil {
		return Gossiped{}, err
	}
	defer func() { _ = session.Close() }()
	outcome, err := hold.state.runConfiguredSession(activeCtx, session, protocol.Remain, cancel)
	result := outcome.gossiped
	return result, err
}

// Retire consumes exclusive ownership for the duration of the attempt. A
// recovered or declined outcome returns a fresh peer token. Once identity
// transmission begins every failure is uncertain and permanently ends this
// replica, including failure while waiting for completion confirmation.
func (p *Peer[T]) Retire(ctx context.Context, carrier *link.Managed) RetireResult[T] {
	if p == nil || p.state == nil {
		return RetireResult[T]{Status: RetirementRecovered, Err: ErrClosed}
	}
	s := p.state
	s.mu.Lock()
	if p.token == nil || !p.token.alive {
		s.mu.Unlock()
		return RetireResult[T]{Status: RetirementRecovered, Err: ErrClosed}
	}
	p.token.alive = false
	s.mu.Unlock()
	recoverPeer := func(err error, status RetirementStatus) RetireResult[T] {
		return RetireResult[T]{
			Status: status,
			Peer:   &Peer[T]{state: s, token: &peerToken{alive: true}},
			Err:    err,
		}
	}
	activeCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	session, err := beginSession(activeCtx, carrier)
	if err != nil {
		return recoverPeer(err, RetirementRecovered)
	}
	defer func() { _ = session.Close() }()
	outcome, err := s.runConfiguredSession(activeCtx, session, protocol.Retire, cancel)
	donated := outcome.donated
	if donated {
		s.mu.Lock()
		s.finishLocked()
		s.mu.Unlock()
		if err != nil {
			return RetireResult[T]{Status: RetirementUncertain, Err: err}
		}
		return RetireResult[T]{Status: Retired}
	}
	if err != nil {
		return recoverPeer(err, RetirementRecovered)
	}
	return recoverPeer(nil, RetirementDeclined)
}

type sessionTransaction[T any] struct {
	state    *replica[T]
	carrier  *sessionWire
	remote   protocol.Preamble
	intent   protocol.Intent
	prior    tree.Tree[[]byte]
	fork     *before.Party
	reserved bool
	donated  bool
}

func (s *replica[T]) session(
	ctx context.Context,
	carrier *sessionWire,
	intent protocol.Intent,
) (result Gossiped, donated bool, err error) {
	started := time.Now()
	kind := observe.Gossip
	if intent == protocol.Retire {
		kind = observe.Retire
	}
	carrier.observation = s.observers.Begin(kind)
	defer func() {
		result.Elapsed = time.Since(started)
		result.Stats.Elapsed = result.Elapsed
		finishWireObservation(ctx, carrier, result, err)
	}()
	remote, err := exchangePreamble(
		carrier,
		protocol.Preamble{Network: protocol.Network(s.network), Intent: intent},
	)
	if err != nil {
		return Gossiped{}, false, phaseError("preamble", err)
	}
	if intent == protocol.Retire && remote.Intent == protocol.Retire {
		err = completeSession(carrier)
		return Gossiped{
			Converged: s.current.Load().tree.Latest(),
		}, false, phaseError(
			"completion",
			err,
		)
	}
	tx := &sessionTransaction[T]{state: s, carrier: carrier, intent: intent, remote: remote}
	if err = tx.prepare(ctx); err != nil {
		return Gossiped{}, false, phaseError("checkpoint", err)
	}
	defer tx.recoverFork()
	result, err = tx.reconcile(ctx)
	return result, tx.donated, err
}

func (tx *sessionTransaction[T]) prepare(ctx context.Context) error {
	prior, fork, err := tx.state.prepareSession(
		ctx,
		tx.remote.Network.IsBootstrap(),
		tx.intent == protocol.Retire,
	)
	if err != nil {
		return err
	}
	tx.prior = prior
	tx.fork = fork
	tx.reserved = fork != nil
	return nil
}

func (tx *sessionTransaction[T]) recoverFork() {
	if !tx.reserved {
		return
	}
	s := tx.state
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservations--
	if tx.fork != nil {
		_ = s.party.Join(tx.fork)
	}
}

func (tx *sessionTransaction[T]) reconcile(ctx context.Context) (Gossiped, error) {
	s := tx.state
	content, err := runContent(
		ctx,
		tx.carrier,
		tx.prior,
		s.codec,
		s.settings,
		protocol.Network(s.network),
		tx.remote.Network,
	)
	if err != nil {
		return Gossiped{}, err
	}
	absorbed, err := tx.transferIdentity(ctx)
	if err != nil {
		return Gossiped{}, phaseError("identity transfer", err)
	}
	converged := content.Tree.Latest()
	if err = tx.publish(content.Tree, absorbed); err != nil {
		return Gossiped{}, phaseError("identity transfer", err)
	}
	if err = tx.complete(ctx); err != nil {
		return Gossiped{}, err
	}
	return Gossiped{Converged: converged, Led: LedLocal, Stats: content.Stats}, nil
}

func (tx *sessionTransaction[T]) transferIdentity(ctx context.Context) (*before.Party, error) {
	if tx.remote.Intent == protocol.Retire {
		return readObservedParty(tx.carrier)
	}
	if tx.intent == protocol.Retire || tx.fork != nil {
		return nil, tx.donateIdentity(ctx)
	}
	return nil, nil
}

func (tx *sessionTransaction[T]) donateIdentity(ctx context.Context) error {
	s := tx.state
	s.mu.Lock()
	party := s.party
	if tx.fork != nil {
		party = tx.fork
	}
	s.mu.Unlock()
	if err := s.donateBookmark(ctx, party); err != nil {
		return err
	}
	wire, err := encodeParty(party)
	if err != nil {
		return err
	}
	if tx.intent == protocol.Retire {
		tx.donated = true
	} else {
		tx.fork = nil
		s.mu.Lock()
		s.reservations--
		s.mu.Unlock()
		tx.reserved = false
	}
	err = writeItem(tx.carrier.Control(), wire)
	if err == nil {
		tx.carrier.observation.ControlSent(wire)
	}
	return err
}

func (tx *sessionTransaction[T]) publish(content tree.Tree[[]byte], absorbed *before.Party) error {
	s := tx.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if absorbed != nil {
		if err := s.party.Join(absorbed); err != nil {
			return err
		}
	}
	s.publishLocked(s.current.Load().tree.Join(content))
	return nil
}

func (tx *sessionTransaction[T]) complete(ctx context.Context) error {
	if tx.remote.Intent == protocol.Retire {
		if err := tx.state.checkpointBookmark(ctx); err != nil {
			return phaseError("bookmark", err)
		}
	}
	return phaseError("completion", completeSession(tx.carrier))
}

func runContent[T any](
	ctx context.Context,
	carrier *sessionWire,
	snapshot tree.Tree[[]byte],
	codec Codec[T],
	settings SynchronizationSettings,
	localNetwork, remoteNetwork protocol.Network,
) (reconcile.Result, error) {
	local := makeGreeting(snapshot, settings)
	encoded, err := local.AppendTo(nil)
	if err != nil {
		return reconcile.Result{}, phaseError("greeting", err)
	}
	remote, err := exchangeItem(carrier, encoded, readGreeting)
	if err != nil {
		return reconcile.Result{}, phaseError("greeting", err)
	}
	if err = protocol.ValidateNetwork(localNetwork, remoteNetwork); err != nil {
		return reconcile.Result{}, phaseError("greeting", err)
	}
	if remoteNetwork.IsBootstrap() && !remote.Version.Equal(Version{}) {
		return reconcile.Result{}, phaseError(
			"greeting",
			errors.New("bootstrap claimant has causal history"),
		)
	}

	stopWatch := carrier.control.watch(ctx, carrier)
	result, err := reconcile.Run(
		ctx,
		carrier,
		carrier.Epoch(),
		snapshot,
		local,
		remote,
		reconcile.Options{
			MemoryBudget: settings.SyncMemoryBudget,
			Observer:     carrier.observation,
			Admit: func(p []byte) error {
				_, err := codec.DecodeBounded(bytes.Clone(p), settings.PayloadDepthLimit)
				return err
			},
		},
	)
	if err != nil {
		_ = carrier.Abort(err)
	}
	watchErr := stopWatch()
	if err == nil {
		err = watchErr
	}
	return result, phaseError("reconciliation", err)
}

func makeGreeting(snapshot tree.Tree[[]byte], settings SynchronizationSettings) protocol.Greeting {
	root, _ := snapshot.Scope(nil)
	children := root.Children()
	listing := make([]protocol.Child, len(children))
	for i, c := range children {
		listing[i] = protocol.Child{Radix: c.Radix, Hash: c.Scope.Hash()}
	}
	return protocol.Greeting{
		Listing:           listing,
		SetLen:            uint64(snapshot.Len()),
		Version:           snapshot.Latest(),
		MaxVersionBytes:   uint64(snapshot.MaxVersionBytes()),
		PayloadDepthLimit: settings.PayloadDepthLimit,
		TargetMessageSize: settings.TargetMessageSize,
	}
}

func exchangePreamble(carrier *sessionWire, p protocol.Preamble) (protocol.Preamble, error) {
	encoded, err := p.AppendTo(nil)
	if err != nil {
		return protocol.Preamble{}, err
	}
	return exchangeItem(carrier, encoded, protocol.ReadPreamble)
}

func exchangeItem[T any](
	carrier *sessionWire,
	out []byte,
	read func(io.Reader) (T, error),
) (T, error) {
	writeDone := make(chan error, 1)
	go func() {
		err := writeItem(carrier.Control(), out)
		if err == nil {
			carrier.observation.ControlSent(out)
		}
		if err != nil {
			_ = carrier.Abort(err)
		}
		writeDone <- err
	}()
	value, readErr := readObservedItem(carrier, read)
	if readErr != nil {
		_ = carrier.Abort(readErr)
	}
	writeErr := <-writeDone
	if readErr != nil {
		return value, readErr
	}
	return value, writeErr
}

func writeItem(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	if flush, ok := w.(interface{ Flush() error }); ok {
		return flush.Flush()
	}
	return nil
}

func (c *replayControl) Flush() error {
	if flush, ok := c.ReadWriteCloser.(interface{ Flush() error }); ok {
		return flush.Flush()
	}
	return nil
}

func completeSession(carrier *sessionWire) error {
	_, err := exchangeItem(
		carrier,
		[]byte(protocol.CompletionMarker),
		func(r io.Reader) (struct{}, error) {
			var got [2]byte
			_, err := io.ReadFull(r, got[:])
			if err == nil && string(got[:]) != protocol.CompletionMarker {
				err = protocol.ErrStructure
			}
			return struct{}{}, err
		},
	)
	if err != nil {
		return err
	}
	return carrier.Finish()
}

func encodeParty(party *before.Party) ([]byte, error) {
	raw, err := party.MarshalBinary()
	if err != nil {
		return nil, err
	}
	item := cbor.AppendHead(nil, cbor.MajorTag, protocol.PartyTag)
	item = cbor.AppendHead(item, cbor.MajorBytes, uint64(len(raw)))
	return append(item, raw...), nil
}

func readParty(r io.Reader) (*before.Party, error) {
	tag, err := cbor.ReadHead(r)
	if err != nil {
		return nil, err
	}
	if tag.Major != cbor.MajorTag || tag.Value != protocol.PartyTag {
		return nil, protocol.ErrStructure
	}
	raw, err := cbor.ReadBytes(r, int(^uint(0)>>1))
	if err != nil {
		return nil, err
	}
	return before.DecodeParty(raw)
}

func receiveBootstrapParty(carrier *sessionWire, network protocol.Network) (*before.Party, error) {
	if network.IsBootstrap() {
		return nil, nil
	}
	return readObservedParty(carrier)
}

func (b *BootstrapConfig[T]) validCodec() bool {
	return b != nil && b.codec.Encode != nil && b.codec.DecodeBounded != nil
}

func readGreeting(r io.Reader) (protocol.Greeting, error) {
	return protocol.ReadGreeting(r, uint64(int(^uint(0)>>1)))
}
