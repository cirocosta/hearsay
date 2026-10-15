package hearsay

import (
	"bytes"
	"errors"
	"math"
	"sync/atomic"
	"testing"
)

func TestCodecRequiresBoundedDecoder(t *testing.T) {
	codec := Codec[[]byte]{Encode: func(p []byte) ([]byte, error) { return p, nil }}
	if _, err := Seed(codec); !errors.Is(err, ErrCodec) {
		t.Fatalf("missing bounded decoder: %v", err)
	}
}

func TestCodecBoundedAdmissionAndUnlimitedReadback(t *testing.T) {
	codec := BytesCodec()
	decode := codec.DecodeBounded
	var seen atomic.Uint64
	codec.DecodeBounded = func(p []byte, limit uint64) ([]byte, error) { seen.Store(limit); return decode(p, limit) }
	for _, limit := range []uint64{0, 1, math.MaxUint64} {
		if _, err := codec.encodeBounded([]byte("value"), limit); err != nil {
			t.Fatal(err)
		}
		if got := seen.Load(); got != limit {
			t.Fatalf("bound %d became %d", limit, got)
		}
	}
	codec.PayloadDepthLimit = 1
	peer, err := Seed(codec)
	if err != nil {
		t.Fatal(err)
	}
	r, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	version, err := r.Send([]byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if got := seen.Load(); got != 1 {
		t.Fatalf("admission bound: %d", got)
	}
	if _, _, err := r.Snapshot().Get(version); err != nil {
		t.Fatal(err)
	}
	if got := seen.Load(); got != math.MaxUint64 {
		t.Fatalf("readback bound: %d", got)
	}
}

func TestBytesCodecTypedArrayDepth(t *testing.T) {
	codec := BytesCodec()
	if _, err := codec.DecodeBounded([]byte{0x80}, 0); err == nil {
		t.Fatal("byte array ignored typed recursion at zero")
	}
	if got, err := codec.DecodeBounded(
		[]byte{0x81, 0x18, 0xff},
		1,
	); err != nil ||
		!bytes.Equal(got, []byte{255}) {
		t.Fatalf("byte array at one: %x %v", got, err)
	}
	if got, err := codec.DecodeBounded(
		[]byte{0xc0, 0x5f, 0x41, 'x', 0xff},
		0,
	); err != nil ||
		string(got) != "x" {
		t.Fatalf("tagged indefinite byte string at zero: %x %v", got, err)
	}
}
