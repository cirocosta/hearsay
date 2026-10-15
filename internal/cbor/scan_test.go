package cbor

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestScanGeneralCBOR(t *testing.T) {
	valid := [][]byte{
		{0},
		{0x18, 0},
		{0x38, 0},
		{0xf4},
		{0xf6},
		{0xf7},
		{0xf8, 32},
		{0xf9, 0x7e, 0},
		{0xfa, 0x7f, 0x80, 0, 0},
		{0xfb, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0},
		{0x40},
		{0x60},
		{0x62, 0xc3, 0xa9},
		{0x80},
		{0xa0},
		{0x81, 0},
		{0xa1, 0, 1},
		{0xc0, 0},
		{0xd9, 0xd9, 0xf7, 0x80},
		{0x5f, 0x41, 1, 0x42, 2, 3, 0xff},
		{0x7f, 0x61, 'a', 0x62, 0xc3, 0xa9, 0xff},
		{0x9f, 0, 0x9f, 1, 0xff, 0xff},
		{0xbf, 0, 1, 0xff},
	}
	for _, item := range valid {
		if err := ValidateItem(item, Limits{}); err != nil {
			t.Fatalf("valid %x: %v", item, err)
		}
		n, err := ScanItem(append(bytes.Clone(item), 0xff), Limits{MaxBytes: len(item)})
		if err != nil || n != len(item) {
			t.Fatalf("sequence %x: %d %v", item, n, err)
		}
		if err := ValidateItem(
			append(bytes.Clone(item), 0),
			Limits{},
		); !errors.Is(
			err,
			ErrTrailing,
		) {
			t.Fatalf("trailing %x: %v", item, err)
		}
		for cut := 0; cut < len(item); cut++ {
			if _, err := ScanItem(item[:cut], Limits{}); err == nil {
				t.Fatalf("truncated accepted %x", item[:cut])
			}
		}
	}
}

func TestScanMalformed(t *testing.T) {
	invalid := [][]byte{
		{0xff},
		{0x1f},
		{0x3f},
		{0xdf},
		{0xfc},
		{0xfd},
		{0xfe},
		{0xf8, 0},
		{0xf8, 23},
		{0xf8, 24},
		{0xf8, 31},
		{0x61, 0xff},
		{0x7f, 0x61, 0xc3, 0x61, 0xa9, 0xff},
		{0x5f, 0x60, 0xff},
		{0x7f, 0x40, 0xff},
		{0x5f, 0x5f, 0xff, 0xff},
		{0xbf, 0, 0xff},
		{0x81, 0xff},
		{0xa1, 0, 0xff},
	}
	for _, item := range invalid {
		if err := ValidateItem(item, Limits{}); err == nil {
			t.Fatalf("malformed accepted %x", item)
		}
	}
	if err := ValidateItem([]byte{0x18}, Limits{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

func TestScanLimits(t *testing.T) {
	tests := []struct {
		input  []byte
		limits Limits
	}{
		{[]byte{0x81, 0x81, 0}, Limits{MaxDepth: 1}},
		{[]byte{0xc0, 0xc0, 0}, Limits{MaxDepth: 1}},
		{[]byte{0x9f, 0x9f, 0xff, 0xff}, Limits{MaxDepth: 1}},
		{[]byte{0x43, 1, 2, 3}, Limits{MaxBytes: 3}},
		{[]byte{0x82, 0, 0}, Limits{MaxItems: 2}},
		{[]byte{0x9f, 0, 0, 0xff}, Limits{MaxItems: 2}},
		{[]byte{0x5f, 0x40, 0x40, 0xff}, Limits{MaxItems: 2}},
		{[]byte{0x9f, 0, 0xff}, Limits{MaxBytes: 2}},
		{[]byte{0x5b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, Limits{}},
		{[]byte{0xbb, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, Limits{}},
	}
	for _, tt := range tests {
		if err := ValidateItem(tt.input, tt.limits); !errors.Is(err, ErrLimit) {
			t.Fatalf("%x %+v: %v", tt.input, tt.limits, err)
		}
	}
	if err := ValidateItem([]byte{0x81, 0}, Limits{MaxDepth: 1}); err != nil {
		t.Fatal(err)
	}
}

func FuzzScanItem(f *testing.F) {
	for _, b := range [][]byte{{0}, {0x9f, 0, 0xff}, {0xf9, 0x7e, 0}, {0xbf, 0, 1, 0xff}, {0xff}, {0x5b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		n, err := ScanItem(b, Limits{MaxDepth: 16, MaxBytes: 4096, MaxItems: 1024})
		if err != nil {
			return
		}
		if n < 1 || n > len(b) || n > 4096 {
			t.Fatalf("invalid span %d", n)
		}
		if err := ValidateItem(
			b[:n],
			Limits{MaxDepth: 16, MaxBytes: 4096, MaxItems: 1024},
		); err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkScanItem(b *testing.B) {
	item := append([]byte{0x98, 100}, bytes.Repeat([]byte{0}, 100)...)
	b.ReportAllocs()
	b.SetBytes(int64(len(item)))
	for i := 0; i < b.N; i++ {
		_, _ = ScanItem(item, Limits{})
	}
}
