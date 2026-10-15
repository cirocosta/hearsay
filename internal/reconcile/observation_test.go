package reconcile

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/cirocosta/hearsay/internal/cbor"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/observe"
)

type wireRecording struct {
	mu        sync.Mutex
	streams   []observe.StreamInfo
	items     map[observe.StreamInfo][][]byte
	onMessage func([]byte)
}

func (r *wireRecording) Session(observe.SessionInfo) observe.SessionObserver { return r }
func (r *wireRecording) Stream(info observe.StreamInfo) observe.StreamObserver {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.streams = append(r.streams, info)
	return &wireRecordingStream{record: r, info: info}
}

type wireRecordingStream struct {
	record *wireRecording
	info   observe.StreamInfo
}

func (s *wireRecordingStream) Message(item []byte) {
	s.record.mu.Lock()
	defer s.record.mu.Unlock()
	if s.record.items == nil {
		s.record.items = make(map[observe.StreamInfo][][]byte)
	}
	s.record.items[s.info] = append(s.record.items[s.info], bytes.Clone(item))
	if s.record.onMessage != nil {
		s.record.onMessage(item)
	}
}

func (r *wireRecording) begin() *observe.Session {
	return (observe.Attachment{}).With(r).Begin(observe.Gossip)
}

func TestStreamObservationSentAfterFlushExcludesLabel(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	writer := new(completionWriter)
	flushes := uint64(0)
	record := &wireRecording{onMessage: func(item []byte) {
		flushes++
		if writer.flushed.Load() != flushes {
			t.Error("frame observed before successful flush")
		}
		if err := cbor.ValidateItem(item, cbor.Limits{}); err != nil {
			t.Error("observed bytes are not one complete item", err)
		}
	}}
	observer := record.begin()
	s := newStreamSet(
		ctx,
		writerLink{writer},
		3,
		protocol.Initiator,
		protocol.FrameLimits{},
		observer,
	)
	defer func() { _ = s.shutdown(false); observer.Finish(observe.Outcome{Kind: observe.Cancelled}) }()
	if err := s.send(1, protocol.Frame{Stream: 1, Signal: protocol.QueryEmptyEnd}); err != nil {
		t.Fatal(err)
	}
	if err := s.finishSend(1); err != nil {
		t.Fatal(err)
	}
	if err := s.shutdown(true); err != nil {
		t.Fatal(err)
	}
	info := observe.StreamInfo{
		ID:        observe.StreamID{Kind: observe.Data, Speaker: observe.Initiator, Index: 1},
		Direction: observe.Sent,
	}
	items := record.items[info]
	if len(items) != 2 {
		t.Fatalf("sent callback count %d", len(items))
	}
	var expected []byte
	for i, signal := range []protocol.Signal{protocol.QueryEmptyEnd, protocol.StreamEnd} {
		encoded, err := (protocol.Frame{Stream: 1, Signal: signal}).AppendTo(
			nil,
			protocol.Initiator,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(items[i], encoded) {
			t.Fatal("sent observation changed frame bytes")
		}
		expected = append(expected, encoded...)
	}
	actual := bytes.NewReader(writer.Bytes())
	if _, err := protocol.ReadLabel(actual, 3); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(writer.Bytes()[len(writer.Bytes())-actual.Len():], expected) {
		t.Fatal("frame callbacks included label bytes")
	}
	if len(record.streams) != 3 {
		t.Fatal("data direction factory did not run exactly once")
	}
}

func TestStreamObservationExcludesFailedFlush(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	failure := errors.New("injected observation flush failure")
	writer := &completionWriter{flushErr: failure}
	record := new(wireRecording)
	observer := record.begin()
	s := newStreamSet(
		ctx,
		writerLink{writer},
		0,
		protocol.Initiator,
		protocol.FrameLimits{},
		observer,
	)
	defer func() { _ = s.shutdown(false); observer.Finish(observe.Outcome{Kind: observe.Failed}) }()
	if err := s.send(
		1,
		protocol.Frame{Stream: 1, Signal: protocol.ReplyEnd},
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatal(err)
	}
	if len(record.items) != 0 {
		t.Fatal("failed flush emitted a frame callback")
	}
	if len(record.streams) != 3 {
		t.Fatal("failed write did not open data observer exactly once")
	}
}

func TestStreamObservationReceivedExactCompleteItems(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	data, err := (protocol.Label{Epoch: 5, Stream: 1}).AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	var expected [][]byte
	for _, signal := range []protocol.Signal{protocol.ReplyEnd, protocol.StreamEnd} {
		frame, err := (protocol.Frame{Stream: 1, Signal: signal}).AppendTo(nil, protocol.Responder)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, frame)
		data = append(data, frame...)
	}
	reader := &completionReader{Reader: bytes.NewReader(data)}
	record := new(wireRecording)
	observer := record.begin()
	s := newStreamSet(
		ctx,
		&completionLink{reader: reader},
		5,
		protocol.Initiator,
		protocol.FrameLimits{},
		observer,
	)
	defer func() { _ = s.shutdown(false); observer.Finish(observe.Outcome{Kind: observe.Cancelled}) }()
	if _, err := s.receive(1); err != nil {
		t.Fatal(err)
	}
	if err := s.finishReceive(1); err != nil {
		t.Fatal(err)
	}
	if err := s.shutdown(true); err != nil {
		t.Fatal(err)
	}
	info := observe.StreamInfo{
		ID:        observe.StreamID{Kind: observe.Data, Speaker: observe.Responder, Index: 1},
		Direction: observe.Received,
	}
	items := record.items[info]
	if len(items) != 2 {
		t.Fatalf("received callbacks %d", len(items))
	}
	for i, item := range items {
		if !bytes.Equal(item, expected[i]) {
			t.Fatal("received callback changed bytes or included label")
		}
	}
	if len(record.streams) != 3 {
		t.Fatal("receive direction factory did not run exactly once")
	}
}

func TestStreamObservationExcludesFragmentsAndRejectedItems(t *testing.T) {
	for _, defect := range []string{"truncated", "wrong stream", "invalid signal"} {
		t.Run(defect, func(t *testing.T) {
			ctx, cancel := streamContext(t)
			defer cancel()
			data, err := (protocol.Label{Stream: 1}).AppendTo(nil)
			if err != nil {
				t.Fatal(err)
			}
			switch defect {
			case "truncated":
				data = append(data, 0x82, 1)
			case "wrong stream":
				data, err = (protocol.Frame{Stream: 2, Signal: protocol.ReplyEnd}).AppendTo(
					data,
					protocol.Responder,
				)
			case "invalid signal":
				data = append(data, 0x82, 1, 99)
			}
			if err != nil {
				t.Fatal(err)
			}
			record := new(wireRecording)
			observer := record.begin()
			s := newStreamSet(
				ctx,
				&completionLink{reader: &completionReader{Reader: bytes.NewReader(data)}},
				0,
				protocol.Initiator,
				protocol.FrameLimits{},
				observer,
			)
			defer func() { _ = s.shutdown(false); observer.Finish(observe.Outcome{Kind: observe.Failed}) }()
			if _, err := s.receive(1); err == nil {
				t.Fatal("malformed received stream accepted")
			}
			if len(record.items) != 0 {
				t.Fatal("incomplete or rejected item emitted callback")
			}
		})
	}
}
