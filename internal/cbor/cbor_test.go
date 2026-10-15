package cbor

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
)

func TestHeadBoundaries(t *testing.T) {
	values := []uint64{
		0,
		1,
		23,
		24,
		25,
		255,
		256,
		65535,
		65536,
		math.MaxUint32,
		math.MaxUint32 + 1,
		math.MaxUint64,
	}
	for major := uint8(0); major < 8; major++ {
		for _, value := range values {
			encoded := AppendHead(nil, major, value)
			if len(encoded) != HeadLen(value) {
				t.Fatalf("head length %d", value)
			}
			h, n, err := ParseHead(append(encoded, 0))
			if err != nil || h != (Head{major, value}) || n != len(encoded) {
				t.Fatalf("%x: %+v %d %v", encoded, h, n, err)
			}
			r := bytes.NewReader(append(encoded, 0))
			got, err := ReadHead(r)
			if err != nil || got != h || r.Len() != 1 {
				t.Fatalf("stream %x: %+v %v trailing %d", encoded, got, err, r.Len())
			}
			var out bytes.Buffer
			if err := WriteHead(
				&out,
				major,
				value,
			); err != nil ||
				!bytes.Equal(out.Bytes(), encoded) {
				t.Fatal("writer disagrees", err)
			}
			for cut := 0; cut < len(encoded); cut++ {
				if _, n, err := ParseHead(
					encoded[:cut],
				); !errors.Is(err, io.ErrUnexpectedEOF) ||
					n != 0 {
					t.Fatalf("cut %x: %d %v", encoded[:cut], n, err)
				}
				_, err := ReadHead(bytes.NewReader(encoded[:cut]))
				expected := io.ErrUnexpectedEOF
				if cut == 0 {
					expected = io.EOF
				}
				if !errors.Is(err, expected) {
					t.Fatalf("stream cut %x: %v", encoded[:cut], err)
				}
			}
		}
	}
	if got := AppendHead(nil, MajorTag, TagSelfDescribed); string(got) != SelfDescribedHead {
		t.Fatalf("self describe %x", got)
	}
}

func TestMalformedHeads(t *testing.T) {
	tests := []struct {
		data []byte
		err  error
	}{
		{[]byte{0x18, 23}, ErrNotShortest},
		{[]byte{0x19, 0, 255}, ErrNotShortest},
		{[]byte{0x1a, 0, 0, 255, 255}, ErrNotShortest},
		{[]byte{0x1b, 0, 0, 0, 0, 255, 255, 255, 255}, ErrNotShortest},
		{[]byte{0x1c}, ErrReserved},
		{[]byte{0x1d}, ErrReserved},
		{[]byte{0x1e}, ErrReserved},
		{[]byte{0x1f}, ErrIndefinite},
	}
	for _, tt := range tests {
		_, n, err := ParseHead(tt.data)
		if !errors.Is(err, tt.err) || n != 0 {
			t.Fatalf("%x: %d %v", tt.data, n, err)
		}
		_, err = ReadHead(bytes.NewReader(tt.data))
		if !errors.Is(err, tt.err) {
			t.Fatalf("stream %x: %v", tt.data, err)
		}
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestWriterAndStringErrors(t *testing.T) {
	if err := WriteHead(shortWriter{}, MajorUint, 24); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := WriteText(&out, "\xff"); !errors.Is(err, ErrMalformed) || out.Len() != 0 {
		t.Fatal(err)
	}
	if _, err := ReadText(bytes.NewReader([]byte{0x61, 0xff}), 1); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	if _, err := ReadBytes(bytes.NewReader([]byte{0x43}), 2); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := ReadUint(bytes.NewReader([]byte{0x40})); !errors.Is(err, ErrMajor) {
		t.Fatal(err)
	}
	if _, err := ReadArray(bytes.NewReader([]byte{0})); !errors.Is(err, ErrMajor) {
		t.Fatal(err)
	}
	if _, err := ReadBytes(bytes.NewReader([]byte{0}), 1); !errors.Is(err, ErrMajor) {
		t.Fatal(err)
	}
}

func TestStringIO(t *testing.T) {
	var out bytes.Buffer
	if err := WriteBytes(&out, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := WriteText(&out, "caf\u00e9"); err != nil {
		t.Fatal(err)
	}
	if err := WriteHead(&out, MajorArray, 2); err != nil {
		t.Fatal(err)
	}
	b, err := ReadBytes(&out, 3)
	if err != nil || string(b) != "abc" {
		t.Fatal(b, err)
	}
	text, err := ReadText(&out, 5)
	if err != nil || text != "caf\u00e9" {
		t.Fatal(text, err)
	}
	n, err := ReadArray(&out)
	if err != nil || n != 2 || out.Len() != 0 {
		t.Fatal(n, err)
	}
}

type trackingReader struct {
	input   []byte
	largest int
}

func (r *trackingReader) Read(p []byte) (int, error) {
	r.largest = max(r.largest, len(p))
	if len(r.input) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.input)
	r.input = r.input[n:]
	return n, nil
}

func TestPayloadGrowthAndExactness(t *testing.T) {
	for _, size := range []int{0, 1, PayloadChunkLen - 1, PayloadChunkLen, PayloadChunkLen + 1, 3*PayloadChunkLen + 1} {
		data := bytes.Repeat([]byte{42}, size)
		r := bytes.NewReader(append(data, 99))
		got, err := ReadPayload(r, size)
		if err != nil || !bytes.Equal(got, data) || r.Len() != 1 {
			t.Fatalf("size %d: %v trailing %d", size, err, r.Len())
		}
		for _, cut := range []int{0, 1, PayloadChunkLen - 1, PayloadChunkLen, PayloadChunkLen + 1, size - 1} {
			if cut < 0 || cut >= size {
				continue
			}
			_, err := ReadPayload(bytes.NewReader(data[:cut]), size)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("size %d cut %d: %v", size, cut, err)
			}
		}
	}
	r := &trackingReader{}
	_, err := ReadPayload(r, math.MaxInt)
	if !errors.Is(err, io.ErrUnexpectedEOF) || r.largest > PayloadChunkLen {
		t.Fatalf("untrusted length reserved %d: %v", r.largest, err)
	}
	prefix := make([]byte, 1, 100)
	prefix[0] = 1
	r = &trackingReader{input: []byte{2, 3, 4}}
	got, err := ResumePayload(r, prefix, 3)
	if err != nil || !bytes.Equal(got, []byte{1, 2, 3}) || len(r.input) != 1 {
		t.Fatal(got, err, r.input)
	}
	if _, err := ResumePayload(r, prefix, 0); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := ReadPayload(r, -1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func FuzzHead(f *testing.F) {
	for _, v := range []uint64{0, 23, 24, 255, 256, 65536, math.MaxUint64} {
		f.Add(uint8(0), v)
	}
	f.Fuzz(func(t *testing.T, major uint8, value uint64) {
		major &= 7
		encoded := AppendHead(nil, major, value)
		got, n, err := ParseHead(encoded)
		if err != nil || n != len(encoded) || got != (Head{major, value}) {
			t.Fatalf("%x: %+v %d %v", encoded, got, n, err)
		}
	})
}

func FuzzParseHead(f *testing.F) {
	for _, b := range [][]byte{{}, {0}, {0x18}, {0x18, 23}, {0xff}, {0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		h, n, err := ParseHead(b)
		if err != nil {
			if n != 0 {
				t.Fatal("advanced on error")
			}
			return
		}
		if n < 1 || n > len(b) || !bytes.Equal(AppendHead(nil, h.Major, h.Value), b[:n]) {
			t.Fatalf("noncanonical acceptance %x", b)
		}
	})
}

func BenchmarkAppendHead(b *testing.B) {
	var buf [MaxHeadLen]byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = AppendHead(buf[:0], MajorBytes, uint64(i))
	}
}

func BenchmarkParseHead(b *testing.B) {
	encoded := AppendHead(nil, MajorUint, math.MaxUint64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = ParseHead(encoded)
	}
}

type emptyReadThenData struct{ empty int }

func (r *emptyReadThenData) Read(p []byte) (int, error) {
	if r.empty > 0 {
		r.empty--
		return 0, nil
	}
	p[0] = 7
	return 1, nil
}

func TestPayloadTransientEmptyReads(t *testing.T) {
	got, err := ReadPayload(&emptyReadThenData{empty: 2}, 1)
	if err != nil || !bytes.Equal(got, []byte{7}) {
		t.Fatal(got, err)
	}
	if _, err := ReadPayload(&emptyReadThenData{empty: 100}, 1); !errors.Is(err, io.ErrNoProgress) {
		t.Fatal(err)
	}
}

type readerOnly struct{ reader *bytes.Reader }

func (r readerOnly) Read(p []byte) (int, error) { return r.reader.Read(p) }

func BenchmarkReadHead(b *testing.B) {
	values := []struct {
		name  string
		value uint64
	}{
		{"1", 23}, {"2", 24}, {"3", 256}, {"5", 65536}, {"9", math.MaxUint64},
	}
	for _, value := range values {
		encoded := AppendHead(nil, MajorUint, value.value)
		b.Run("bytes.Reader/"+value.name, func(b *testing.B) {
			reader := bytes.NewReader(encoded)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader.Reset(encoded)
				if _, err := ReadHead(reader); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("readerOnly/"+value.name, func(b *testing.B) {
			reader := bytes.NewReader(encoded)
			wrapped := readerOnly{reader}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader.Reset(encoded)
				if _, err := ReadHead(wrapped); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
