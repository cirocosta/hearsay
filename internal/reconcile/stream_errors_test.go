package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/sessionerr"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/observe"
)

type diagnosticCarrier struct {
	link.Link
	openErr, acceptErr error
}

func (l diagnosticCarrier) Open(ctx context.Context) (io.WriteCloser, error) {
	if l.openErr != nil {
		return nil, l.openErr
	}
	return l.Link.Open(ctx)
}

func (l diagnosticCarrier) Accept(ctx context.Context) (io.ReadCloser, error) {
	if l.acceptErr != nil {
		return nil, l.acceptErr
	}
	return l.Link.Accept(ctx)
}

type diagnosticReader struct{ err error }

func (r diagnosticReader) Read([]byte) (int, error) { return 0, r.err }

func TestDataTransportDiagnosticsRetainOpenAndAccept(t *testing.T) {
	failure := errors.New("carrier operation failed")
	for _, operation := range []sessionerr.Operation{sessionerr.Open, sessionerr.Accept} {
		t.Run(string(operation), func(t *testing.T) {
			left, right := link.Memory()
			defer func() { _ = right.Close() }()
			carrier := diagnosticCarrier{Link: left}
			if operation == sessionerr.Open {
				carrier.openErr = failure
			} else {
				carrier.acceptErr = failure
			}
			streams := newStreamSet(
				t.Context(),
				carrier,
				3,
				protocol.Initiator,
				protocol.FrameLimits{},
			)
			defer func() { _ = streams.shutdown(false) }()
			var err error
			if operation == sessionerr.Open {
				err = streams.send(1, protocol.Frame{Stream: 1, Signal: protocol.ReplyEnd})
			} else {
				err = <-streams.failures()
			}
			var transport *sessionerr.TransportError
			if !errors.As(err, &transport) || !errors.Is(err, failure) {
				t.Fatalf("lost transport cause: %v", err)
			}
			if transport.Operation != operation ||
				transport.Context.Phase != sessionerr.Reconciliation ||
				transport.Context.DataStream == nil {
				t.Fatalf("wrong transport context: %+v", transport)
			}
			stream := transport.Context.DataStream
			if operation == sessionerr.Open &&
				(stream.Sender != observe.Initiator || stream.Index == nil || *stream.Index != 1) {
				t.Fatalf("wrong open direction: %+v", stream)
			}
			if operation == sessionerr.Accept &&
				(stream.Sender != observe.Responder || stream.Index != nil) {
				t.Fatalf("invented accepted index: %+v", stream)
			}
		})
	}
}

func TestDataReadDiagnosticsDistinguishWireAndTransport(t *testing.T) {
	streams := &streamSet{local: protocol.Initiator, remote: protocol.Responder}
	failure := errors.New("reader failed")
	_, err := streams.readFrame(1, diagnosticReader{failure})
	var transport *sessionerr.TransportError
	if !errors.As(err, &transport) || !errors.Is(err, failure) ||
		transport.Operation != sessionerr.Read {
		t.Fatalf("lost data read cause: %v", err)
	}
	if stream := transport.Context.DataStream; stream == nil ||
		stream.Sender != observe.Responder ||
		stream.Index == nil ||
		*stream.Index != 1 {
		t.Fatalf("wrong data read stream: %+v", stream)
	}
	_, err = streams.readFrame(1, bytes.NewReader([]byte{0x82, 1, 0x18, 99}))
	var violation *sessionerr.ProtocolViolation
	if !errors.As(err, &violation) || !errors.Is(err, protocol.ErrSignal) {
		t.Fatalf("lost protocol cause: %v", err)
	}
	if stream := violation.Context.DataStream; stream == nil ||
		stream.Sender != observe.Responder ||
		stream.Index == nil ||
		*stream.Index != 1 {
		t.Fatalf("wrong protocol stream: %+v", stream)
	}
}

func TestRejectedLabelRetainsOnlyValidKnownIndex(t *testing.T) {
	streams := &streamSet{remote: protocol.Responder}
	for _, index := range []uint64{0, 16, 17, 999} {
		err := streams.labelError(&protocol.LabelError{ExpectedEpoch: 1, Epoch: 2, Stream: index})
		var violation *sessionerr.ProtocolViolation
		if !errors.As(err, &violation) || !errors.Is(err, protocol.ErrStream) {
			t.Fatalf("lost label diagnostic: %v", err)
		}
		known := violation.Context.DataStream.Index
		if index < protocol.StreamCount {
			if known == nil || uint64(*known) != index {
				t.Fatalf("lost valid label index %d", index)
			}
		} else if known != nil {
			t.Fatalf("invented invalid index %d", index)
		}
	}
}
