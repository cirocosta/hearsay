package hearsay

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func seededBytes(t *testing.T) *Rumors[[]byte] {
	t.Helper()
	p, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func TestSnapshotOwnsPayloads(t *testing.T) {
	r := seededBytes(t)
	input := []byte("message")
	v, err := r.Send(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 'X'
	snap := r.Snapshot()
	got, ok, err := snap.Get(v)
	if err != nil || !ok || string(got) != "message" {
		t.Fatalf("get: %q %v %v", got, ok, err)
	}
	got[0] = 'Y'
	again, _, _ := snap.Get(v)
	if string(again) != "message" {
		t.Fatalf("mutated snapshot: %q", again)
	}
	if err := r.Redact(v); err != nil {
		t.Fatal(err)
	}
	if r.Snapshot().Contains(v) || !snap.Contains(v) {
		t.Fatal("snapshot changed through redaction")
	}
	if !v.LessEqual(r.Snapshot().Latest()) || v.Equal(r.Snapshot().Latest()) {
		t.Fatal("redaction did not advance history")
	}
}

func TestBatchRollbackAndAtomicPublish(t *testing.T) {
	r := seededBytes(t)
	original := r.Snapshot()
	reject := errors.New("reject")
	if err := r.Batch(func(b *Batch[[]byte]) error {
		if err := b.Send([]byte("a")); err != nil {
			t.Fatal(err)
		}
		return reject
	}); !errors.Is(err, reject) {
		t.Fatal(err)
	}
	if !original.Equal(r.Snapshot()) {
		t.Fatal("failed batch published")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("missing panic")
			}
		}()
		_ = r.Batch(func(b *Batch[[]byte]) error { _ = b.Send([]byte("a")); panic("abort") })
	}()
	if !original.Equal(r.Snapshot()) {
		t.Fatal("panicking batch published")
	}
	if err := r.SendAll([][]byte{[]byte("a"), []byte("b"), []byte("c")}); err != nil {
		t.Fatal(err)
	}
	snap := r.Snapshot()
	if snap.Len() != 3 {
		t.Fatal(snap.Len())
	}
	var last Version
	for _, m := range snap.Causal() {
		if !last.LessEqual(m.Version) || last.Equal(m.Version) {
			t.Fatal("unordered batch versions")
		}
		last = m.Version
	}
	before := snap.Latest()
	if err := r.Redact(Version{}); err != nil {
		t.Fatal(err)
	}
	if !before.Equal(r.Snapshot().Latest()) {
		t.Fatal("missing redaction advanced history")
	}
}

func TestCodecAdmissionRollback(t *testing.T) {
	codec := JSONCodec[string]()
	decode := codec.Decode
	codec.Decode = func(p []byte) (string, error) {
		s, err := decode(p)
		if s == "reject" {
			return "", ErrCodec
		}
		return s, err
	}

	p, err := Seed(codec)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := p.IntoRumors()
	defer func() { _ = r.Close() }()
	if err := r.SendAll([]string{"accepted", "reject"}); !errors.Is(err, ErrCodec) {
		t.Fatal(err)
	}
	if !r.Snapshot().IsEmpty() {
		t.Fatal("partial admission published")
	}
}

func TestPeerCopiesCannotDuplicateIdentity(t *testing.T) {
	p, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	copyPeer := *p
	r, err := p.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyPeer.IntoRumors(); !errors.Is(err, ErrClosed) {
		t.Fatalf("copied peer consumed: %v", err)
	}
	clone, err := r.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.TryIntoPeer(); !errors.Is(err, ErrBusy) {
		t.Fatalf("busy: %v", err)
	}
	copyHandle := *clone
	_ = clone.Close()
	_ = copyHandle.Close()
	recovered, err := r.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Send(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("consumed handle send: %v", err)
	}
	live, err := recovered.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	if _, err := copyHandle.Clone(); !errors.Is(err, ErrClosed) {
		t.Fatalf("old generation reused: %v", err)
	}
}

func TestConcurrentReunionOneWinner(t *testing.T) {
	r := seededBytes(t)
	other, err := r.Clone()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	for _, h := range []*Rumors[[]byte]{r, other} {
		go func() { defer wg.Done(); _, err := h.IntoPeer(context.Background()); results <- err }()
	}
	wg.Wait()
	close(results)
	wins, claims := 0, 0
	for err := range results {
		if err == nil {
			wins++
		} else if errors.Is(err, ErrClaimed) {
			claims++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || claims != 1 {
		t.Fatalf("wins=%d claimed=%d", wins, claims)
	}
}

func TestConcurrentSnapshotAndWrites(t *testing.T) {
	r := seededBytes(t)
	const workers = 8
	const count = 30
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range count {
				if _, err := r.Send([]byte("x")); err != nil {
					t.Error(err)
					return
				}
				snap := r.Snapshot()
				if snap.Len() == 0 {
					t.Error("empty snapshot")
				}
			}
		}()
	}
	wg.Wait()
	if got := r.Snapshot().Len(); got != workers*count {
		t.Fatal(got)
	}
}

func TestUnfaithfulCodecRejected(t *testing.T) {
	codec := JSONCodec[string]()
	codec.Decode = func([]byte) (string, error) { return "other", nil }
	peer, err := Seed(codec)
	if err != nil {
		t.Fatal(err)
	}
	r, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if _, err := r.Send("original"); !errors.Is(err, ErrUnfaithful) {
		t.Fatal(err)
	}
	if !r.Snapshot().IsEmpty() {
		t.Fatal("lossy payload published")
	}
}

func TestMalformedCodecRejected(t *testing.T) {
	codec := Codec[string]{
		Encode: func(string) ([]byte, error) { return []byte{1, 2}, nil },
		Decode: func([]byte) (string, error) { return "value", nil },
	}
	peer, err := Seed(codec)
	if err != nil {
		t.Fatal(err)
	}
	r, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if _, err := r.Send("value"); err == nil {
		t.Fatal("accepted multiple CBOR items")
	}
	if !r.Snapshot().IsEmpty() {
		t.Fatal("malformed payload published")
	}
}

func TestIntoPeerCancellationConsumesHandle(t *testing.T) {
	r := seededBytes(t)
	other, err := r.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.IntoPeer(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := r.Send(nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	peer, err := other.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	live, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	_ = live.Close()
}
