package before

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
)

// These canonical encodings and the exhaustive acceptance digest were emitted
// by the pinned upstream Rust Span implementation.
func TestSpanWireRustGolden(t *testing.T) {
	c := Seed()
	zero := c.Version()
	one := c.Tick()
	two := c.Tick()
	for _, tt := range []struct {
		name, hex string
		lo, hi    Version
	}{
		{"zero-point", "e0e0", zero, zero},
		{"one-point", "a8a8", one, one},
		{"zero-two", "e0b8", zero, two},
		{"one-two", "a8b8", one, two},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSpan(tt.lo, tt.hi)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := hex.DecodeString(tt.hex)
			got, err := s.MarshalBinary()
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("encoded %x, %v; want %x", got, err, want)
			}
			decoded, err := DecodeSpan(want)
			if err != nil || !decoded.Equal(s) {
				t.Fatalf("decoded span %v", err)
			}
			if s.EncodedLen() != len(got) {
				t.Fatal("encoded length differs")
			}
			prefix := []byte{0x12, 0x34}
			appended := s.AppendBinary(append([]byte(nil), prefix...))
			if !bytes.Equal(appended, append(prefix, want...)) {
				t.Fatal("append changed prefix")
			}
			if tt.lo.Equal(tt.hi) && decoded.lo.root != decoded.hi.root {
				t.Fatal("point endpoints do not share storage")
			}
		})
	}
}

func TestSpanWireRustExhaustiveAcceptance(t *testing.T) {
	h := sha256.New()
	for n := 0; n <= 65535; n++ {
		data := [2]byte{byte(n >> 8), byte(n)}
		s, err := DecodeSpan(data[:])
		if err != nil {
			continue
		}
		h.Write(data[:])
		got, _ := s.MarshalBinary()
		if !bytes.Equal(got, data[:]) {
			t.Fatalf("accepted noncanonical %x", data)
		}
	}
	const want = "bd11fe835fbb5838305cbab0eba340e95501da543c6ce4094e07412786ef7622"
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != want {
		t.Fatalf("acceptance digest %s; want Rust %s", got, want)
	}
}

func TestSpanWireMalformedPrecedesCrossed(t *testing.T) {
	for _, tt := range []struct {
		name string
		data []byte
		want error
	}{
		{"missing lower", nil, ErrTruncated},
		{"missing upper", []byte{0xa8}, ErrTruncated},
		{"lower padding", []byte{0xa9, 0xe0}, ErrTrailingData},
		{"crossed", []byte{0xa8, 0xe0}, ErrNonCanonical},
		{"crossed upper padding", []byte{0xa8, 0xe1}, ErrTrailingData},
		{"crossed trailing", []byte{0xa8, 0xe0, 0}, ErrTrailingData},
		{"upper collapse", []byte{0xa8, 0x7c}, ErrNonCanonical},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeSpan(tt.data)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v; want %v", err, tt.want)
			}
		})
	}
	c := Seed()
	d := c.Fork()
	left := c.Tick()
	right := d.Tick()
	a, _ := left.MarshalBinary()
	b, _ := right.MarshalBinary()
	if _, err := DecodeSpan(append(a, b...)); !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("concurrent endpoints: %v", err)
	}
}

func TestSpanWireTransactional(t *testing.T) {
	c := Seed()
	v := c.Tick()
	s := Point(v)
	old := s
	if err := s.UnmarshalBinary([]byte{0xa8, 0xe0}); err == nil || !s.Equal(old) {
		t.Fatal("failed decode changed span")
	}
}

func FuzzSpanWireCanonical(f *testing.F) {
	for _, b := range [][]byte{{0xe0, 0xe0}, {0xa8, 0xb8}, {0xa8, 0xe0}, {0xa8, 0xe1}, {0x20}, {}} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := DecodeSpan(data)
		if err != nil {
			return
		}
		encoded, _ := s.MarshalBinary()
		if !bytes.Equal(encoded, data) {
			t.Fatalf("accepted noncanonical %x => %x", data, encoded)
		}
		if !s.lo.LessEqual(s.hi) {
			t.Fatal("accepted crossed span")
		}
		if s.EncodedLen() != len(data) {
			t.Fatal("encoded length differs")
		}
	})
}
