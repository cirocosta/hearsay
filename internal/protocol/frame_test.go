package protocol

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
)

func testRecordLimits() RecordLimits {
	return RecordLimits{
		MaxVersionBytes: 4096,
	}
}

func nullRun(t *testing.T) LeafRun {
	t.Helper()
	run, err := ParseRun([]byte{0xd8, 0x3f, 0x46, 0xd9, 0xd2, 0x56, 0x41, 0xe0, 0xf6})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestUpstreamFrameAtlas(t *testing.T) {
	// exact bytes copied from upstream canonical_frame_atlas_snapshot.snap,
	// initiator stream 01, all ten signal states.
	golden := []string{
		"820100", "820101", "820102", "820103",
		"830104a1005818000000000000000000000000000000000000000000000000",
		"830105a1005818000000000000000000000000000000000000000000000000",
		"830106d83f49d83f46d9d25641e0f6", "830107d83f49d83f46d9d25641e0f6",
		"820108", "820109",
	}
	run := nullRun(t)
	for state, wantHex := range golden {
		f := Frame{Stream: 1, Signal: Signal(state)}
		if f.Signal == QueryContinue || f.Signal == QueryEnd {
			f.Listing = []Child{{}}
		}
		if f.Signal == SupplyContinue || f.Signal == SupplyEnd {
			f.Run = run
		}
		encoded, err := f.AppendTo(nil, Initiator)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := hex.DecodeString(wantHex)
		if !bytes.Equal(encoded, want) {
			t.Fatalf("state %d got %x want %x", state, encoded, want)
		}
		r := bytes.NewReader(append(bytes.Clone(want), 0xff))
		got, err := ReadFrame(r, Initiator, FrameLimits{TargetMessageSize: 1024})
		if err != nil {
			t.Fatalf("state %d: %v", state, err)
		}
		if got.Stream != 1 || got.Signal != f.Signal || r.Len() != 1 {
			t.Fatalf("state %d mismatch or read ahead", state)
		}
		reencoded, err := got.AppendTo(nil, Initiator)
		if err != nil || !bytes.Equal(reencoded, want) {
			t.Fatalf("state %d roundtrip: %x %v", state, reencoded, err)
		}
		for cut := 0; cut < len(want); cut++ {
			if _, err := ReadFrame(
				bytes.NewReader(want[:cut]),
				Initiator,
				FrameLimits{TargetMessageSize: 1024},
			); err == nil {
				t.Fatalf("state %d accepted cut %d", state, cut)
			}
		}
	}
}

func TestSignalAtlas(t *testing.T) {
	// upstream atlas phase masks: all states at interior streams; special
	// opening supplies, leaf-parent replies, and terminal leaf replies.
	for role := Initiator; role <= Responder; role++ {
		for stream := uint8(0); stream < 17; stream++ {
			mask := uint16(0x3ff)
			if role == Initiator && stream == 0 {
				mask = 0x3c0
			}
			if role == Initiator && stream == 16 {
				mask = 0x3cf
			}
			if role == Responder && stream == 16 {
				mask = 0x380
			}
			for signal := Signal(0); signal <= 9; signal++ {
				err := ValidateSignal(role, stream, signal)
				if (err == nil) != (mask&(1<<signal) != 0) {
					t.Fatalf("role %d stream %d signal %d: %v", role, stream, signal, err)
				}
			}
		}
	}
	if err := ValidateSignal(Initiator, 17, 0); !errors.Is(err, ErrStream) {
		t.Fatal(err)
	}
	if err := ValidateSignal(Initiator, 1, 10); !errors.Is(err, ErrSignal) {
		t.Fatal(err)
	}
	if err := ValidateSignal(Role(2), 1, 0); !errors.Is(err, ErrPlacement) {
		t.Fatal(err)
	}
}

func TestHeightScheduleAndLabels(t *testing.T) {
	for role := Initiator; role <= Responder; role++ {
		for height := uint8(1); height < 31; height++ {
			stream, err := StreamAtHeight(role, height)
			expected := (role == Initiator && (31-height)%2 == 1) ||
				(role == Responder && (31-height)%2 == 0)
			if (err == nil) != expected {
				t.Fatalf("role %d height %d: %d %v", role, height, stream, err)
			}
		}
		if stream, err := StreamAtHeight(role, 31); err != nil || stream != 0 {
			t.Fatalf("%d %v", stream, err)
		}
		if stream, err := StreamAtHeight(role, 0); err != nil || stream != 16 {
			t.Fatalf("%d %v", stream, err)
		}
	}
	for _, epoch := range []uint8{0, 23, 24, 255} {
		for stream := uint8(0); stream < 17; stream++ {
			want := Label{Epoch: epoch, Stream: stream}
			encoded, err := want.AppendTo(nil)
			if err != nil {
				t.Fatal(err)
			}
			r := bytes.NewReader(append(encoded, 0xff))
			got, err := ReadLabel(r, epoch)
			if err != nil || got != want || r.Len() != 1 {
				t.Fatalf("%+v %v", got, err)
			}
			if _, err := ReadLabel(bytes.NewReader(encoded), epoch+1); !errors.Is(err, ErrStream) {
				t.Fatal(err)
			}
		}
	}
	if _, err := (Label{Stream: 17}).AppendTo(nil); !errors.Is(err, ErrStream) {
		t.Fatal(err)
	}
}

func TestSupplyBatching(t *testing.T) {
	record := Record{Payload: []byte{0xf6}}
	builder := NewRunBuilder(28, testRecordLimits())
	// null records occupy nine bytes; the conservative overhead is ten.
	for i := 0; i < 2; i++ {
		if admitted, err := builder.Add(record); err != nil || !admitted {
			t.Fatalf("%t %v", admitted, err)
		}
	}
	if admitted, err := builder.Add(record); err != nil || admitted {
		t.Fatalf("third admitted: %t %v", admitted, err)
	}
	run := builder.Take()
	if run.Count() != 2 || len(run.Bytes()) != 18 {
		t.Fatalf("run: %+v", run)
	}
	f := Frame{Stream: 1, Signal: SupplyEnd, Run: run}
	encoded, err := f.AppendTo(nil, Initiator)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(
		bytes.NewReader(encoded),
		Initiator,
		FrameLimits{TargetMessageSize: 28},
	); err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(encoded)
	if _, err := ReadFrame(
		r,
		Initiator,
		FrameLimits{TargetMessageSize: 27},
	); !errors.Is(
		err,
		ErrOverbatched,
	) {
		t.Fatal(err)
	}
	if r.Len() != 15 {
		t.Fatalf("overbatched reader consumed payload: %d bytes remain", r.Len())
	}
	zero := NewRunBuilder(0, testRecordLimits())
	if ok, err := zero.Add(record); err != nil || !ok {
		t.Fatalf("%t %v", ok, err)
	}
	if ok, err := zero.Add(record); err != nil || ok {
		t.Fatalf("%t %v", ok, err)
	}
	f.Run = zero.Take()
	encoded, err = f.AppendTo(nil, Initiator)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(bytes.NewReader(encoded), Initiator, FrameLimits{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(
		bytes.NewReader(encoded),
		Initiator,
		FrameLimits{MaxRunBytes: 8},
	); !errors.Is(
		err,
		ErrLimit,
	) {
		t.Fatal(err)
	}
}

func TestRunRecordIterator(t *testing.T) {
	clock := before.Seed()
	builder := NewRunBuilder(4096, testRecordLimits())
	records := []Record{
		{Version: clock.Tick(), Payload: []byte{0x18, 0x01}},
		{Version: clock.Tick(), Payload: []byte{0x9f, 0xf6, 0xff}},
	}
	for _, record := range records {
		if ok, err := builder.Add(record); !ok || err != nil {
			t.Fatalf("%t %v", ok, err)
		}
	}
	run := builder.Take()
	it := run.Records(testRecordLimits())
	for _, want := range records {
		got, ok, err := it.Next()
		if !ok || err != nil || !got.Version.Equal(want.Version) ||
			!bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("%+v %t %v", got, ok, err)
		}
	}
	if _, ok, err := it.Next(); ok || err != nil {
		t.Fatalf("%t %v", ok, err)
	}
	it = run.Records(RecordLimits{})
	if _, _, err := it.Next(); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, ok, err := it.Next(); ok || err != nil {
		t.Fatal("iterator resumed after error")
	}
}

func TestMalformedFramesAndRuns(t *testing.T) {
	for _, data := range [][]byte{{0x82, 1, 4}, {0x83, 1, 0}, {0x82, 17, 0}, {0x82, 1, 10}, {0x83, 1, 4, 0xa0}, {0x83, 1, 7, 0xd8, 0x3f, 0x40}} {
		if _, err := ReadFrame(bytes.NewReader(data), Initiator, FrameLimits{}); err == nil {
			t.Fatalf("accepted %x", data)
		}
	}
	run := nullRun(t)
	for cut := 0; cut < len(run.Bytes()); cut++ {
		if _, err := ParseRun(run.Bytes()[:cut]); err == nil {
			t.Fatalf("accepted cut %d", cut)
		}
	}
	bad := bytes.Clone(run.Bytes())
	bad[0] = 0xd9
	if _, err := ParseRun(bad); err == nil {
		t.Fatal("accepted wrong record tag")
	}
	for _, payload := range [][]byte{{}, {0xf6, 0xf6}, {0x81}} {
		raw := append([]byte{0xd9, 0xd2, 0x56, 0x41, 0xe0}, payload...)
		envelope := cbor.AppendHead(nil, cbor.MajorTag, 63)
		envelope = appendBytes(envelope, cbor.MajorBytes, raw)
		parsed, err := ParseRun(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := parsed.Records(testRecordLimits()).Next(); err == nil {
			t.Fatalf("accepted payload %x", payload)
		}
	}
	// a malformed first-record head cannot read beyond its declared run.
	encoded := []byte{0x83, 1, 7, 0xd8, 0x3f, 0x43, 0xd8, 0x3f, 0x5b, 0, 0, 0, 0, 0, 0, 0, 0}
	r := bytes.NewReader(encoded)
	if _, err := ReadFrame(r, Initiator, FrameLimits{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if r.Len() != 8 {
		t.Fatalf("read beyond run: %d remain", r.Len())
	}
}

func TestRecordsLeavePayloadRecursionToTypedCodec(t *testing.T) {
	clock := before.Seed()
	payload := append(bytes.Repeat([]byte{0xc0}, 100_000), 0)
	limits := RecordLimits{MaxVersionBytes: 4096}
	builder := NewRunBuilder(1<<20, limits)
	record := Record{Version: clock.Tick(), Payload: payload}
	if ok, err := builder.Add(record); err != nil || !ok {
		t.Fatalf("builder imposed structural payload policy: %v", err)
	}
	decoded, ok, err := builder.Take().Records(limits).Next()
	if err != nil || !ok || !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("record imposed structural payload policy: %v", err)
	}
}

func TestRecordsLeavePayloadSemanticsToTypedCodec(t *testing.T) {
	clock := before.Seed()
	limits := testRecordLimits()
	// A typed decoder may accept the noncanonical false spelling. UTF-8
	// validity belongs to that same decoder, rather than record framing.
	for _, payload := range [][]byte{{0xf8, 20}, {0x61, 0xff}} {
		builder := NewRunBuilder(4096, limits)
		record := Record{Version: clock.Tick(), Payload: payload}
		if ok, err := builder.Add(record); err != nil || !ok {
			t.Fatal(ok, err)
		}
		got, ok, err := builder.Take().Records(limits).Next()
		if err != nil || !ok || !bytes.Equal(got.Payload, record.Payload) {
			t.Fatal(got, ok, err)
		}
	}
}
