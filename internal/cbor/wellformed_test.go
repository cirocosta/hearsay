package cbor

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestWellFormedItems(t *testing.T) {
	for _, input := range [][]byte{
		{0},
		{0x18, 0},
		{0x80},
		{0xa0},
		{0x81, 0},
		{0xa1, 0, 1},
		{0xc0, 0xc0, 0},
		{0x81, 0x5f, 0x41, 0, 0xff},
		{0x7f, 0x61, 'a', 0xff},
		{0x5f, 0x5f, 0x41, 1, 0xff, 0xff},
		{0x7f, 0x7f, 0x61, 'a', 0xff, 0xff},
		{0x9f, 0x81, 0, 0xff},
		{0xbf, 0, 0, 0xff},
		{0xf8, 20},
		{0xf8, 32},
		{0xf9, 0, 0},
		{0xfa, 0, 0, 0, 0},
		{0xfb, 0, 0, 0, 0, 0, 0, 0, 0},
	} {
		if err := ValidateWellFormed(input); err != nil {
			t.Fatalf("%x: %v", input, err)
		}
		n, err := ScanWellFormed(append(bytes.Clone(input), 0xff))
		if err != nil || n != len(input) {
			t.Fatalf("%x consumed %d: %v", input, n, err)
		}
		if err := ValidateWellFormed(append(bytes.Clone(input), 0)); !errors.Is(err, ErrTrailing) {
			t.Fatal(err)
		}
		for cut := 0; cut < len(input); cut++ {
			if _, err := ScanWellFormed(input[:cut]); err == nil {
				t.Fatalf("accepted truncated %x", input[:cut])
			}
		}
	}
	// UTF-8 and supported simple values are typed decoder obligations. The
	// scanner only confirms their CBOR envelope.
	if err := ValidateWellFormed([]byte{0x61, 0xff}); err != nil {
		t.Fatal(err)
	}
}

func TestWellFormedMalformed(t *testing.T) {
	for _, input := range [][]byte{
		{0xff},
		{0x1f},
		{0x3f},
		{0xdf},
		{0xc0, 0xff},
		{0xfc},
		{0xfd},
		{0xfe},
		{0x5f, 0x60, 0xff},
		{0x7f, 0x40, 0xff},
		{0x5f, 0xc0, 0x40, 0xff},
		{0xbf, 0, 0xff},
		{0x81, 0xff},
		{0xa1, 0, 0xff},
	} {
		if err := ValidateWellFormed(input); err == nil {
			t.Fatalf("malformed accepted %x", input)
		}
	}
	for _, input := range [][]byte{
		{0x1b, 0},
		{0x5b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		{0xbb, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	} {
		if err := ValidateWellFormed(input); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("%x: %v", input, err)
		}
	}
}

func TestWellFormedDeepInputsUseExplicitStack(t *testing.T) {
	const depth = 100_000
	for _, head := range []byte{0xc0, 0x81} {
		input := append(bytes.Repeat([]byte{head}, depth), 0)
		if err := ValidateWellFormed(input); err != nil {
			t.Fatal(err)
		}
	}
	input := append(bytes.Repeat([]byte{0x9f}, depth), 0)
	input = append(input, bytes.Repeat([]byte{0xff}, depth)...)
	if err := ValidateWellFormed(input); err != nil {
		t.Fatal(err)
	}
}

func FuzzWellFormed(f *testing.F) {
	for _, input := range [][]byte{{0}, {0xc0, 0xc0, 0}, {0x9f, 0, 0xff}, {0xbf, 0, 0, 0xff}, {0x5f, 0x41, 0, 0xff}, {0xff}} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		n, err := ScanWellFormed(input)
		if len(input) > 0 {
			_, oldErr := ScanItem(
				input,
				Limits{MaxDepth: len(input), MaxBytes: len(input), MaxItems: len(input)},
			)
			if oldErr == nil && err != nil {
				t.Fatal("rejected bounded scanner input", err)
			}
		}
		if err != nil {
			return
		}
		if n < 1 || n > len(input) {
			t.Fatalf("invalid span %d", n)
		}
		if err := ValidateWellFormed(input[:n]); err != nil {
			t.Fatal(err)
		}
	})
}
