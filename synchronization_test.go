package hearsay

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/internal/protocol"
)

func TestTypedDecoderReceivesExactConfiguredBound(t *testing.T) {
	codec := BytesCodec()
	decode := codec.DecodeBounded
	var bound atomic.Uint64
	codec.DecodeBounded = func(p []byte, limit uint64) ([]byte, error) { bound.Store(limit); return decode(p, limit) }
	peer, err := Seed(codec)
	if err != nil {
		t.Fatal(err)
	}
	settings := peer.SynchronizationSettings()
	settings.PayloadDepthLimit = 0
	if _, err := peer.WithSynchronizationSettings(settings); err != nil {
		t.Fatal(err)
	}
	r, err := peer.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	v, err := r.Send([]byte("zero-depth bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if got := bound.Load(); got != 0 {
		t.Fatalf("zero bound became %d", got)
	}
	if _, _, err := r.Snapshot().Get(v); err != nil {
		t.Fatal(err)
	}
	if got := bound.Load(); got != math.MaxUint64 {
		t.Fatalf("readback unnecessarily readmitted at %d", got)
	}
	recovered, err := r.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	settings.PayloadDepthLimit = math.MaxUint64
	if _, err := recovered.WithSynchronizationSettings(settings); err != nil {
		t.Fatal(err)
	}
	live, err := recovered.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if _, err := live.Send([]byte("unbounded")); err != nil {
		t.Fatal(err)
	}
	if got := bound.Load(); got != math.MaxUint64 {
		t.Fatalf("large bound truncated to %d", got)
	}
}

func TestTypedDecoderCanSkipTagsAtZeroDepth(t *testing.T) {
	codec := BytesCodec()
	encode := codec.Encode
	codec.Encode = func(p []byte) ([]byte, error) {
		item, err := encode(p)
		if err != nil {
			return nil, err
		}
		return append(bytes.Repeat([]byte{0xc0}, 512), item...), nil
	}
	peer, err := Seed(codec)
	if err != nil {
		t.Fatal(err)
	}
	settings := peer.SynchronizationSettings()
	settings.PayloadDepthLimit = 0
	if _, err := peer.WithSynchronizationSettings(settings); err != nil {
		t.Fatal(err)
	}
	source, err := peer.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	if _, err := source.Send([]byte("tagged")); err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	joiner, err := Bootstrap(BytesCodec()).WithSynchronizationSettings(settings)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := joiner.Join(ctx, near)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	live, err := joined.Peer.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if live.Snapshot().Len() != 1 {
		t.Fatal("typed decoder lost tagged payload")
	}
	for message := range live.Snapshot().All() {
		if message.Err != nil || string(message.Payload) != "tagged" {
			t.Fatalf("message: %+v", message)
		}
	}
}

func TestDepthMismatchReturnsBootstrapReservation(t *testing.T) {
	source := seededBytes(t)
	near, far := managedPair(t)
	settings := Bootstrap(BytesCodec()).SynchronizationSettings()
	settings.PayloadDepthLimit = 0
	joiner, err := Bootstrap(BytesCodec()).WithSynchronizationSettings(settings)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	joined, joinErr := joiner.Join(ctx, near)
	serveErr := <-served
	if joinErr == nil || serveErr == nil ||
		(!errors.Is(joinErr, protocol.ErrDepth) && !errors.Is(serveErr, protocol.ErrDepth)) {
		t.Fatalf("depth mismatch: %v %v", joinErr, serveErr)
	}
	if joined.Peer != nil || joined.Retry != joiner {
		t.Fatal("failed join did not preserve builder")
	}
	source.state.mu.Lock()
	defer source.state.mu.Unlock()
	if source.state.reservations != 0 || !source.state.party.IsSeed() {
		t.Fatal("failed bootstrap leaked reserved identity")
	}
}
