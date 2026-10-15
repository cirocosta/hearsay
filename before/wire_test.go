package before

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"testing"
)

// The digests pin every acceptance verdict for all one- and two-byte strings.
// The upstream Rust oracle hashes accepted records as length followed by bytes,
// visiting each big-endian uint16 and its one-byte suffix when below 256.
func TestWireRustExhaustiveAcceptance(t *testing.T) {
	for _, tt := range []struct {
		name, want string
		decode     func([]byte) error
	}{
		{"party", "73fb87ec7dcd2d19342c7127eb3fc24cdd715705f41c8ad6eaa0f0f673809ff5", func(b []byte) error { _, e := DecodeParty(b); return e }},
		{"version", "d523727179cc3bd4f4257dd122f12fae346657467a4d9c5985b85f9c6bebf9de", func(b []byte) error { _, e := DecodeVersion(b); return e }},
		{"clock", "f3578d16af1502dffbb217a52c2e01bfa300b12e372871bb9ea69664f94a5e73", func(b []byte) error { _, e := DecodeClock(b); return e }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := sha256.New()
			for n := 0; n <= 65535; n++ {
				data := [2]byte{byte(n >> 8), byte(n)}
				for _, b := range [][]byte{data[1:], data[:]} {
					if len(b) == 1 && n > 255 {
						continue
					}
					if tt.decode(b) == nil {
						h.Write([]byte{byte(len(b))})
						h.Write(b)
					}
				}
			}
			if got := fmt.Sprintf("%x", h.Sum(nil)); got != tt.want {
				t.Fatalf("acceptance digest %s; want Rust %s", got, tt.want)
			}
		})
	}
}

// these vectors were emitted by the upstream before crate. Clock encodings
// pin the byte boundary between fields.
func TestWireRustGolden(t *testing.T) {
	c := Seed()
	check := func(name, party, version, clock string) {
		t.Helper()
		wantP, _ := hex.DecodeString(party)
		wantV, _ := hex.DecodeString(version)
		wantC, _ := hex.DecodeString(clock)
		gotP, err := c.Party().MarshalBinary()
		if err != nil || !bytes.Equal(gotP, wantP) {
			t.Fatalf("%s party %x, %v; want %x", name, gotP, err, wantP)
		}
		gotV, err := c.Version().MarshalBinary()
		if err != nil || !bytes.Equal(gotV, wantV) {
			t.Fatalf("%s version %x, %v; want %x", name, gotV, err, wantV)
		}
		if got := c.Version().EncodedLen(); got != len(wantV) {
			t.Fatalf("%s encoded length %d; want %d", name, got, len(wantV))
		}
		gotC, err := c.MarshalBinary()
		if err != nil || !bytes.Equal(gotC, wantC) {
			t.Fatalf("%s clock %x, %v; want %x", name, gotC, err, wantC)
		}
		decoded, err := DecodeClock(wantC)
		if err != nil || !decoded.Version().Equal(c.Version()) {
			t.Fatalf("%s decode: %v", name, err)
		}
		p, err := DecodeParty(wantP)
		if err != nil || !covers(p.root, c.party.root) || !covers(c.party.root, p.root) {
			t.Fatalf("%s party decode: %v", name, err)
		}
		v, err := DecodeVersion(wantV)
		if err != nil || !v.Equal(c.Version()) {
			t.Fatalf("%s version decode: %v", name, err)
		}
		prefix := []byte{0x12, 0x34}
		appended := c.Version().AppendBinary(append([]byte(nil), prefix...))
		if !bytes.Equal(appended, append(prefix, wantV...)) {
			t.Fatalf("%s append changed prefix", name)
		}
	}
	check("seed", "20", "e0", "20e0")
	c.Tick()
	check("tick", "20", "a8", "20a8")
	d := c.Fork()
	check("fork-left", "88", "a8", "88a8")
	c, d = d, c
	check("fork-right", "48", "a8", "48a8")
	c, d = d, c
	c.Tick()
	check("left-tick", "88", "5d40", "885d40")
	d.Ticks(3)
	c, d = d, c
	check("right-ticks", "48", "54f0", "4854f0")
	c, d = d, c
	if err := c.Join(d); err != nil {
		t.Fatal(err)
	}
	check("join", "20", "5cb0", "205cb0")
	c.Fork()
	c.version = c.version.TicksBig(c.Party(), new(big.Int).Lsh(big.NewInt(1), 127))
	wide := "4000000000000000000000000000000040000000000000000000000000000002400000000000000000000000000000007fffffffffffffffffffffffffffffff40"
	check("wide", "88", wide, "88"+wide)
}

func TestWireRejectsMalformed(t *testing.T) {
	for _, tt := range []struct {
		name   string
		data   []byte
		decode func([]byte) error
		want   error
	}{
		{"empty version", nil, func(b []byte) error { _, e := DecodeVersion(b); return e }, ErrTruncated},
		{"negative height", []byte{0x75}, func(b []byte) error { _, e := DecodeVersion(b); return e }, ErrNonCanonical},
		{"equal leaves", []byte{0x7c}, func(b []byte) error { _, e := DecodeVersion(b); return e }, ErrNonCanonical},
		{"version padding", []byte{0xe1}, func(b []byte) error { _, e := DecodeVersion(b); return e }, ErrTrailingData},
		{"version trailing", []byte{0xe0, 0}, func(b []byte) error { _, e := DecodeVersion(b); return e }, ErrTrailingData},
		{"equal owned siblings", []byte{0xc2}, func(b []byte) error { _, e := DecodeParty(b); return e }, ErrNonCanonical},
		{"party padding", []byte{0x21}, func(b []byte) error { _, e := DecodeParty(b); return e }, ErrTrailingData},
		{"party trailing", []byte{0x20, 0}, func(b []byte) error { _, e := DecodeParty(b); return e }, ErrTrailingData},
		{"clock missing version", []byte{0x20}, func(b []byte) error { _, e := DecodeClock(b); return e }, ErrTruncated},
		{"clock malformed party padding", []byte{0x21, 0xe0}, func(b []byte) error { _, e := DecodeClock(b); return e }, ErrTrailingData},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.decode(tt.data); !errors.Is(err, tt.want) {
				t.Fatalf("got %v; want %v", err, tt.want)
			}
		})
	}
}

func TestWireDecodeTransactional(t *testing.T) {
	c := Seed()
	c.Tick()
	before, _ := c.MarshalBinary()
	if err := c.UnmarshalBinary([]byte{0x20, 0xe1}); err == nil {
		t.Fatal("accepted malformed clock")
	}
	after, _ := c.MarshalBinary()
	if !bytes.Equal(before, after) {
		t.Fatal("failed decode changed clock")
	}
	v := c.Version()
	if err := v.UnmarshalBinary([]byte{0x7c}); err == nil || !v.Equal(c.Version()) {
		t.Fatal("failed decode changed version")
	}
	p := SeedParty()
	if err := p.UnmarshalBinary([]byte{0xc2}); err == nil || !p.IsSeed() {
		t.Fatal("failed decode changed party")
	}
}

func TestWireWideBoundary(t *testing.T) {
	p := SeedParty()
	p.Fork()
	for _, width := range []uint{63, 64, 65, 127, 128, 129, 511} {
		for _, offset := range []int64{-1, 0, 1} {
			n := new(big.Int).Lsh(big.NewInt(1), width)
			n.Add(n, big.NewInt(offset))
			v := Version{}.TicksBig(p, n)
			data, _ := v.MarshalBinary()
			w, err := DecodeVersion(data)
			if err != nil || !v.Equal(w) {
				t.Fatalf("width %d offset %d: %v", width, offset, err)
			}
		}
	}
}

func TestWireDeepDecode(t *testing.T) {
	const depth = 10000
	p := bitWriter{}
	v := bitWriter{}
	for range depth {
		p.bit(true)
		p.bit(false)
		v.bit(false)
	}
	p.bit(false)
	p.bit(false)
	v.bit(true)
	v.gamma(count{})
	for range depth {
		v.bit(true)
		v.gamma(count{small: 2})
	}
	party, err := DecodeParty(p.finish())
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := party.MarshalBinary()
	if !bytes.Equal(encoded, p.data) {
		t.Fatal("deep party changed")
	}
	version, err := DecodeVersion(v.finish())
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ = version.MarshalBinary()
	if !bytes.Equal(encoded, v.data) {
		t.Fatal("deep version changed")
	}
}

func FuzzWireCanonical(f *testing.F) {
	for _, data := range [][]byte{{0x20}, {0xe0}, {0x20, 0xe0}, {0x88, 0x5d, 0x40}, {0xc2}, {0x75}, {}} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if v, err := DecodeVersion(data); err == nil {
			got, _ := v.MarshalBinary()
			if v.EncodedLen() != len(got) {
				t.Fatal("version encoded length differs")
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("version accepted noncanonical %x => %x", data, got)
			}
		}
		if p, err := DecodeParty(data); err == nil {
			got, _ := p.MarshalBinary()
			if !bytes.Equal(got, data) {
				t.Fatalf("party accepted noncanonical %x => %x", data, got)
			}
		}
		if c, err := DecodeClock(data); err == nil {
			got, _ := c.MarshalBinary()
			if !bytes.Equal(got, data) {
				t.Fatalf("clock accepted noncanonical %x => %x", data, got)
			}
		}
	})
}

func BenchmarkWireVersion(b *testing.B) {
	c := Seed()
	clocks := []*Clock{c}
	for range 31 {
		clocks = append(clocks, clocks[len(clocks)-1].Fork())
	}
	for i, c := range clocks {
		c.Ticks(uint64(i + 1))
	}
	v := Version{}
	for _, c := range clocks {
		v = v.Join(c.Version())
	}
	data, _ := v.MarshalBinary()
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = v.MarshalBinary()
		}
	})
	b.Run("append", func(b *testing.B) {
		b.ReportAllocs()
		buf := make([]byte, 0, len(data))
		for b.Loop() {
			buf = v.AppendBinary(buf[:0])
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = DecodeVersion(data)
		}
	})
}
