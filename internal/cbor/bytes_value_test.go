package cbor

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestDecodeByteStringCiboriumCorpus(t *testing.T) {
	// These verdicts are pinned by the exact upstream Ciborium 0.2.2,
	// bytes 1.11.1, and serde 1.0.228 oracle in .agi.local/payload-depth.
	tests := []struct{ hex, want string }{
		{"40", ""},
		{"4100", "00"},
		{"5801ff", "ff"},
		{"c0c04101", "01"},
		{"c24101", "01"},
		{"5f580101ff", "01"},
		{"80", ""},
		{"8100", "00"},
		{"9f000118ffff", "0001ff"},
		{"c081c001", "01"},
		{"81c241ff", "ff"},
		{"81c25f4200004101ff", "01"},
		{"81c2420001", "01"},
		{"5f5f4101ffff", "01"},
		{"5f5f40ffff", ""},
	}
	for _, tt := range tests {
		input, _ := hex.DecodeString(tt.hex)
		want, _ := hex.DecodeString(tt.want)
		got, err := DecodeByteString(input)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %x %v", tt.hex, got, err)
		}
		if _, err := DecodeByteString(append(bytes.Clone(input), 0)); err == nil {
			t.Fatalf("trailing accepted %s", tt.hex)
		}
		for cut := 0; cut < len(input); cut++ {
			if _, err := DecodeByteString(input[:cut]); err == nil {
				t.Fatalf("truncation accepted %x", input[:cut])
			}
		}
	}
	for _, encoded := range []string{
		"00", "6100", "8120", "81f90000", "8180", "81190100", "81c34100",
		"81c2420100", "81c2c04101", "5fc040ff", "9f00", "ff", "df40",
	} {
		input, _ := hex.DecodeString(encoded)
		if got, err := DecodeByteString(input); err == nil {
			t.Fatalf("%s accepted: %x", encoded, got)
		}
	}
}

func TestDecodeByteStringLongTagChain(t *testing.T) {
	input := append(bytes.Repeat([]byte{0xc0}, 100_000), 0x41, 9)
	got, err := DecodeByteString(input)
	if err != nil || !bytes.Equal(got, []byte{9}) {
		t.Fatal(got, err)
	}
}

func FuzzDecodeByteString(f *testing.F) {
	for _, input := range [][]byte{{0x40}, {0xc0, 0x41, 0}, {0x9f, 0, 0xff}, {0x5f, 0x41, 0, 0xff}, {0xff}} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if _, err := DecodeByteString(input); err == nil {
			if err := ValidateWellFormed(input); err != nil {
				t.Fatal("typed byte decoder accepted malformed CBOR", err)
			}
		}
	})
}

func TestDecodeByteStringZeroDepth(t *testing.T) {
	for _, encoded := range []string{"40", "c0c04101", "5f4101ff", "c24101", "5f5f4101ffff"} {
		input, _ := hex.DecodeString(encoded)
		if _, err := DecodeByteStringBounded(input, 0); err != nil {
			t.Fatalf("%s: %v", encoded, err)
		}
	}
	for _, encoded := range []string{"80", "8100", "c09f00ff"} {
		input, _ := hex.DecodeString(encoded)
		if _, err := DecodeByteStringBounded(input, 0); err != ErrDepth {
			t.Fatalf("%s: %v", encoded, err)
		}
		if _, err := DecodeByteStringBounded(input, 1); err != nil {
			t.Fatalf("%s: %v", encoded, err)
		}
	}
}
