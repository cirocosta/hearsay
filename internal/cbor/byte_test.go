package cbor

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
)

func TestByteHeadMatchesReader(t *testing.T) {
	for n := 0; n <= math.MaxUint16; n++ {
		input := []byte{byte(n >> 8), byte(n)}
		wantReader, gotReader := bytes.NewReader(input), bytes.NewReader(input)
		want, wantErr := ReadHead(wantReader)
		got, gotErr := ReadByteHead(gotReader)
		if got != want || !errors.Is(gotErr, wantErr) || gotReader.Len() != wantReader.Len() {
			t.Fatalf("%x: got %+v %v; want %+v %v", input, got, gotErr, want, wantErr)
		}
	}
}

func TestByteHeadBoundaries(t *testing.T) {
	for _, value := range []uint64{0, 23, 24, 255, 256, 65535, 65536, math.MaxUint32, math.MaxUint32 + 1, math.MaxUint64} {
		encoded := AppendHead(nil, MajorUint, value)
		for cut := 0; cut <= len(encoded); cut++ {
			got, err := ReadByteHead(bytes.NewReader(encoded[:cut]))
			switch {
			case cut == len(encoded):
				if err != nil || got != (Head{MajorUint, value}) {
					t.Fatalf("value %d: %+v %v", value, got, err)
				}
			case cut == 0:
				if !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
			default:
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal(err)
				}
			}
		}
		r := bufio.NewReader(bytes.NewReader(append(encoded, 7)))
		if _, err := ReadByteHead(r); err != nil {
			t.Fatal(err)
		}
		if next, err := r.ReadByte(); err != nil || next != 7 {
			t.Fatalf("lost following item: %d %v", next, err)
		}
	}
}

type failingByteReader struct {
	remaining int
	err       error
}

func (r *failingByteReader) ReadByte() (byte, error) {
	if r.remaining == 0 {
		return 0, r.err
	}
	r.remaining--
	return 0x1b, nil
}

func TestByteHeadPreservesTransportError(t *testing.T) {
	want := errors.New("transport failure")
	for _, remaining := range []int{0, 1, 4, 8} {
		_, err := ReadByteHead(&failingByteReader{remaining: remaining, err: want})
		if !errors.Is(err, want) {
			t.Fatalf("after %d bytes: %v", remaining, err)
		}
	}
}

func BenchmarkReadByteHead(b *testing.B) {
	for _, value := range []uint64{23, 24, 256, 65536, math.MaxUint64} {
		encoded := AppendHead(nil, MajorUint, value)
		b.Run(fmt.Sprint(len(encoded)), func(b *testing.B) {
			r := bytes.NewReader(encoded)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Reset(encoded)
				if _, err := ReadByteHead(r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
