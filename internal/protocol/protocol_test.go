package protocol

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
)

func TestPreambleGolden(t *testing.T) {
	// upstream preamble.rs: 55799(["rumors", 2, h'0102...10', 1]).
	p := Preamble{Intent: Retire}
	for i := range p.Network {
		p.Network[i] = byte(i + 1)
	}
	got, err := p.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("d9d9f7846672756d6f727302500102030405060708090a0b0c0d0e0f1001")
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x want %x", got, want)
	}
	parsed, err := ParsePreamble(want)
	if err != nil || parsed != p {
		t.Fatalf("%+v %v", parsed, err)
	}
	r := bytes.NewReader(append(append([]byte{}, want...), 99))
	if _, err := ReadPreamble(r); err != nil {
		t.Fatal(err)
	}
	if r.Len() != 1 {
		t.Fatal("consumed following item")
	}
	for i := 0; i < len(want); i++ {
		if _, err := ReadPreamble(bytes.NewReader(want[:i])); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
}

func TestPreambleRejections(t *testing.T) {
	p := Preamble{Network: Network{1}}
	valid, _ := p.AppendTo(nil)
	for _, tc := range []struct {
		offset int
		value  byte
		want   error
	}{{0, 0, ErrMagic}, {11, 3, ErrProtocol}, {12, 0x51, ErrStructure}, {29, 2, ErrIntent}} {
		data := bytes.Clone(valid)
		data[tc.offset] = tc.value
		if _, err := ParsePreamble(data); !errors.Is(err, tc.want) {
			t.Fatalf("%x: %v", data, err)
		}
	}
	if _, err := (Preamble{Intent: Retire}).AppendTo(nil); !errors.Is(err, ErrBootstrapRetire) {
		t.Fatal(err)
	}
	if err := ValidateNetwork(Network{1}, Network{2}); !errors.Is(err, ErrNetwork) {
		t.Fatal(err)
	}
	if err := ValidateNetwork(Network{}, Network{2}); err != nil {
		t.Fatal(err)
	}
}

func TestGreetingGolden(t *testing.T) {
	// upstream greeting.rs deterministic key order, before zero-version e0.
	body := []byte(
		"\xa6\x67listing\xa0\x67set_len\x00\x67version\xd9\xd2\x56\x41\xe0\x71max_version_bytes\x00\x73payload_depth_limit\x00\x73target_message_size\x00",
	)
	want := cbor.AppendHead(nil, cbor.MajorTag, 24)
	want = cbor.AppendHead(want, cbor.MajorBytes, uint64(len(body)))
	want = append(want, body...)
	got, err := (Greeting{}).AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x want %x", got, want)
	}
	parsed, err := ParseGreeting(want)
	if err != nil || !parsed.Version.IsEmpty() || len(parsed.Listing) != 0 {
		t.Fatalf("%+v %v", parsed, err)
	}
	r := bytes.NewReader(append(bytes.Clone(want), 99))
	if _, err := ReadGreeting(r, uint64(len(body))); err != nil {
		t.Fatal(err)
	}
	if r.Len() != 1 {
		t.Fatal("consumed following item")
	}
	if _, err := ReadGreeting(
		bytes.NewReader(want),
		uint64(len(body)-1),
	); !errors.Is(
		err,
		ErrLimit,
	) {
		t.Fatal(err)
	}
	for i := 0; i < len(want); i++ {
		if _, err := ParseGreeting(want[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	if _, err := ParseGreeting(append(bytes.Clone(want), 0)); !errors.Is(err, cbor.ErrTrailing) {
		t.Fatal(err)
	}
}

func TestFullGreeting(t *testing.T) {
	clock := before.Seed()
	g := Greeting{
		Version:           clock.Tick(),
		SetLen:            math.MaxUint64,
		MaxVersionBytes:   math.MaxUint64,
		PayloadDepthLimit: math.MaxUint64,
		TargetMessageSize: math.MaxUint64,
	}
	for i := 0; i < 256; i++ {
		child := Child{Radix: uint8(i)}
		for j := range child.Hash {
			child.Hash[j] = byte(i + j)
		}
		g.Listing = append(g.Listing, child)
	}
	encoded, err := g.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseGreeting(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Version.Equal(g.Version) || got.SetLen != g.SetLen ||
		got.MaxVersionBytes != g.MaxVersionBytes ||
		got.PayloadDepthLimit != g.PayloadDepthLimit ||
		got.TargetMessageSize != g.TargetMessageSize ||
		len(got.Listing) != 256 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
	for i := range g.Listing {
		if g.Listing[i] != got.Listing[i] {
			t.Fatalf("child %d", i)
		}
	}
	g.Listing[255].Radix = 254
	if _, err := g.AppendTo(nil); !errors.Is(err, ErrOrder) {
		t.Fatal(err)
	}
}

func TestGreetingMalformed(t *testing.T) {
	valid, _ := (Greeting{Listing: []Child{{Radix: 1}, {Radix: 2}}}).AppendTo(nil)
	// find the second radix after the first full digest.
	bad := bytes.Clone(valid)
	at := bytes.Index(bad, []byte("listing")) + len("listing") + 1 + 1 + 2 + 24
	bad[at] = 1
	if _, err := ParseGreeting(bad); !errors.Is(err, ErrOrder) {
		t.Fatalf("duplicate radix: %v", err)
	}
	bad = bytes.Clone(valid)
	bad[bytes.Index(bad, []byte("set_len"))] = 'x'
	if _, err := ParseGreeting(bad); !errors.Is(err, ErrStructure) {
		t.Fatal(err)
	}
	huge := cbor.AppendHead(nil, cbor.MajorTag, 24)
	huge = cbor.AppendHead(huge, cbor.MajorBytes, math.MaxUint64)
	if _, err := ReadGreeting(bytes.NewReader(huge), math.MaxUint64); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := ReadGreeting(
		bytes.NewReader(valid[:len(valid)-1]),
		math.MaxUint64,
	); !errors.Is(
		err,
		io.ErrUnexpectedEOF,
	) {
		t.Fatal(err)
	}
}

func TestNegotiate(t *testing.T) {
	c := before.Seed()
	a := Greeting{Version: c.Tick(), SetLen: 1, PayloadDepthLimit: 64, TargetMessageSize: 1024}
	b := a
	b.Version = c.Tick()
	b.SetLen = 2
	b.TargetMessageSize = 512
	n, err := Negotiate(a, b)
	if err != nil || !n.Initiator || n.Equal || n.TargetMessageSize != 512 {
		t.Fatalf("%+v %v", n, err)
	}
	inverse, err := Negotiate(b, a)
	if err != nil || inverse.Initiator {
		t.Fatalf("%+v %v", inverse, err)
	}
	b.SetLen = 1
	n, _ = Negotiate(a, b)
	inverse, _ = Negotiate(b, a)
	if n.Initiator == inverse.Initiator {
		t.Fatal("tie election did not choose one initiator")
	}
	b = a
	n, err = Negotiate(a, b)
	if err != nil || !n.Equal || n.Initiator {
		t.Fatalf("%+v %v", n, err)
	}
	b.PayloadDepthLimit++
	if _, err := Negotiate(a, b); !errors.Is(err, ErrDepth) {
		t.Fatal("depth mismatch hidden by equal versions")
	}
	if GreetingBodyLimit(math.MaxUint64) != math.MaxUint64 {
		t.Fatal("bound overflow")
	}
}
