package bookmark

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
)

// These exact frames come from the upstream committed Insta snapshots.
const (
	emptyFixture  = "d9d9f783065820c24fdfe00b190b6f8fac2dbefba12b38eb3f0f6922a2d85764565911e8736bb0d8184180"
	sampleFixture = "d9d9f7830658206f5f0acf87a69322bc7c7bfb3354101768b429bd31bc20619a5a3e04adbaad5bd818582a8183505a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5ad9d256432aa58083d9d25541a2d9d2554148d9d2554192"
)

func unhex(t *testing.T, text string) []byte {
	t.Helper()
	value, err := hex.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sample(t *testing.T) *Record {
	t.Helper()
	clock := before.Seed()
	first := clock.Fork()
	second := clock.Fork()
	clock.Tick()
	first.Tick()
	first.Tick()
	r := new(Record)
	var network [16]byte
	for i := range network {
		network[i] = 0x5a
	}
	for _, c := range []*before.Clock{clock, first, second} {
		p, v := c.IntoParts()
		if err := r.Record(network, p, v); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func TestRustSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name    string
		r       *Record
		fixture string
	}{
		{"empty", new(Record), emptyFixture}, {"nontrivial", sample(t), sampleFixture},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := unhex(t, tc.fixture)
			if got := tc.r.Encode(); !bytes.Equal(got, want) {
				t.Fatalf("encode: %x; want %x", got, want)
			}
			decoded, err := Decode(want)
			if err != nil {
				t.Fatal(err)
			}
			if got := decoded.Encode(); !bytes.Equal(got, want) {
				t.Fatalf("round trip: %x", got)
			}
		})
	}
}

func TestMalformedFrame(t *testing.T) {
	valid := unhex(t, sampleFixture)
	for i := 0; i < len(valid); i++ {
		if _, err := Decode(valid[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	badHash := bytes.Clone(valid)
	badHash[10] ^= 1
	if _, err := Decode(badHash); !errors.Is(err, ErrChecksum) {
		t.Fatal(err)
	}
	badVersion := bytes.Clone(valid)
	badVersion[4] = 5
	if _, err := Decode(badVersion); !errors.Is(err, ErrVersion) {
		t.Fatal(err)
	}
	if _, err := Decode(append(bytes.Clone(valid), 0)); err == nil {
		t.Fatal("accepted trailing frame")
	}
	for _, payload := range [][]byte{{0x80, 0}, {0x81, 0x80}, {0x81, 0x84}, {0xff}, {0x00}} {
		if _, err := Decode(frame(payload)); !errors.Is(err, ErrRecord) {
			t.Fatalf("payload %x: %v", payload, err)
		}
	}
	r := sample(t)
	r.networks = append(r.networks, r.networks[0])
	if _, err := Decode(r.Encode()); !errors.Is(err, ErrRecord) {
		t.Fatalf("duplicate network: %v", err)
	}
	r = sample(t)
	r.networks[0].identities[0] = []byte{0xff}
	if _, err := Decode(r.Encode()); !errors.Is(err, ErrRecord) {
		t.Fatalf("bad party: %v", err)
	}
}

func TestAlternateEnvelope(t *testing.T) {
	payload := []byte{0x80}
	hash := checksum(FormatVersion, payload)
	out := []byte{0xda, 0, 0, 0xd9, 0xf7, 0x9f, 0x18, 6}
	out = append(out, 0x5f, 0x50)
	out = append(out, hash[:16]...)
	out = append(out, 0x50)
	out = append(out, hash[16:]...)
	out = append(out, 0xff, 0xd8, 24, 0x5f, 0x41, 0x80, 0xff, 0xff)
	r, err := Decode(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Encode(), unhex(t, emptyFixture)) {
		t.Fatal("alternate envelope changed record")
	}
	// Payload spelling itself is integrity protected, including indefinite arrays.
	r, err = Decode(frame([]byte{0x9f, 0xff}))
	if err != nil || len(r.networks) != 0 {
		t.Fatalf("indefinite payload: %v", err)
	}
}

func TestTupleBoundaries(t *testing.T) {
	r := sample(t)
	payload := cbor.AppendHead(nil, cbor.MajorArray, 1)
	entry := encodeNetwork(nil, &r.networks[0])
	// An indefinite tuple must consume its own break, not the parent's break.
	entry[0] = 0x9f
	payload = append(payload, entry...)
	payload = append(payload, 0xff)
	if _, err := Decode(frame(payload)); err != nil {
		t.Fatal(err)
	}
	bad := append(bytes.Clone(payload[:len(payload)-1]), 0x00, 0xff)
	if _, err := Decode(frame(bad)); err == nil {
		t.Fatal("accepted extra tuple field")
	}
}

func FuzzDecode(f *testing.F) {
	for _, text := range []string{emptyFixture, sampleFixture} {
		data, _ := hex.DecodeString(text)
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		r, err := Decode(input)
		if err != nil {
			return
		}
		encoded := r.Encode()
		if _, err := Decode(encoded); err != nil {
			t.Fatal(err)
		}
	})
}
