package hearsay_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	h "github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/observe"
)

type diagnosticControl struct {
	reader                      *bytes.Reader
	readErr, writeErr, flushErr error
	afterRead                   <-chan struct{}
	blockRead                   bool
	closed                      chan struct{}
	closeOnce                   sync.Once
}

func (c *diagnosticControl) Read(p []byte) (int, error) {
	if c.blockRead {
		<-c.closed
		return 0, io.EOF
	}
	if c.readErr != nil {
		return 0, c.readErr
	}
	return c.reader.Read(p)
}

func (c *diagnosticControl) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		if c.afterRead != nil {
			<-c.afterRead
		}
		return 0, c.writeErr
	}
	return len(p), nil
}

func (c *diagnosticControl) Flush() error {
	if c.flushErr != nil && c.afterRead != nil {
		<-c.afterRead
	}
	return c.flushErr
}

func (c *diagnosticControl) Close() error {
	if c.closed != nil {
		c.closeOnce.Do(func() { close(c.closed) })
	}
	return nil
}

type diagnosticLink struct{ control *diagnosticControl }

func (l diagnosticLink) Control() io.ReadWriteCloser { return l.control }
func (diagnosticLink) Open(context.Context) (io.WriteCloser, error) {
	return nil, errors.New("unexpected data open")
}

func (diagnosticLink) Accept(ctx context.Context) (io.ReadCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (l diagnosticLink) Close() error { return l.control.Close() }

func diagnosticPreamble(version byte) []byte {
	data := []byte{0xd9, 0xd9, 0xf7, 0x84, 0x66, 'r', 'u', 'm', 'o', 'r', 's', version, 0x50}
	data = append(data, make([]byte, 16)...)
	return append(data, 0)
}

type diagnosticObserver struct {
	ready chan struct{}
	once  sync.Once
}

func (o *diagnosticObserver) Session(observe.SessionInfo) observe.SessionObserver { return o }
func (o *diagnosticObserver) Stream(info observe.StreamInfo) observe.StreamObserver {
	if info.ID.Kind == observe.Control && info.Direction == observe.Received {
		return o
	}
	return nil
}
func (o *diagnosticObserver) Message([]byte) { o.once.Do(func() { close(o.ready) }) }

func publicDiagnosticPeer(t *testing.T, observers ...observe.Observer) *h.Rumors[[]byte] {
	t.Helper()
	peer, err := h.Seed(h.BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	for _, observer := range observers {
		if err := peer.AttachObserver(observer); err != nil {
			t.Fatal(err)
		}
	}
	rumors, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rumors.Close() })
	return rumors
}

func TestPublicProtocolMismatchOperands(t *testing.T) {
	peer := publicDiagnosticPeer(t)
	carrier := link.Manage(
		diagnosticLink{&diagnosticControl{reader: bytes.NewReader(diagnosticPreamble(3))}},
	)
	defer func() { _ = carrier.Close() }()
	_, err := peer.GossipOnce(t.Context(), carrier)
	var mismatch *h.ProtocolMismatch
	if !errors.As(err, &mismatch) || !errors.Is(err, h.ErrProtocolMismatch) {
		t.Fatalf("expected public protocol mismatch; got %v", err)
	}
	if mismatch.LocalProtocol != h.V2 || mismatch.RemoteVersion != 3 {
		t.Fatalf("mismatch operands: %+v", mismatch)
	}
	var category h.Mismatch
	if !errors.As(err, &category) {
		t.Fatal("public mismatch category unavailable")
	}
}

func TestPublicControlTransportOperationAndCause(t *testing.T) {
	failure := errors.New("scripted transport failure")
	for _, tt := range []struct {
		name               string
		read, write, flush error
		operation          h.TransportOperation
	}{
		{"read", failure, nil, nil, h.TransportRead},
		{"write", nil, failure, nil, h.TransportWrite},
		{"flush", nil, nil, failure, h.TransportFlush},
		{"write EOF", nil, io.EOF, nil, h.TransportWrite},
		{"flush EOF", nil, nil, io.EOF, h.TransportFlush},
	} {
		t.Run(tt.name, func(t *testing.T) {
			observer := &diagnosticObserver{ready: make(chan struct{})}
			peer := publicDiagnosticPeer(t, observer)
			control := &diagnosticControl{
				reader:   bytes.NewReader(diagnosticPreamble(2)),
				readErr:  tt.read,
				writeErr: tt.write,
				flushErr: tt.flush,
			}
			if tt.read == nil {
				control.afterRead = observer.ready
			}
			carrier := link.Manage(diagnosticLink{control})
			defer func() { _ = carrier.Close() }()
			_, err := peer.GossipOnce(t.Context(), carrier)
			var transport *h.TransportError
			if !errors.As(err, &transport) {
				t.Fatalf("expected public transport error; got %v", err)
			}
			if transport.Operation != tt.operation || transport.Context.Phase != h.PhasePreamble ||
				transport.Context.DataStream != nil {
				t.Fatalf("wrong transport context: %+v", transport)
			}
			cause := tt.read
			if cause == nil {
				cause = tt.write
			}
			if cause == nil {
				cause = tt.flush
			}
			if !errors.Is(err, cause) {
				t.Fatalf("lost transport cause: %v", err)
			}
			var session *h.SessionError
			if !errors.As(err, &session) || session.Context().Phase != h.PhasePreamble {
				t.Fatal("lost session context")
			}
		})
	}
}

func TestPublicPayloadDepthMismatchOperands(t *testing.T) {
	source := publicDiagnosticPeer(t)
	builder := h.Bootstrap(h.BytesCodec())
	settings := builder.SynchronizationSettings()
	settings.PayloadDepthLimit--
	builder, err := builder.WithSynchronizationSettings(settings)
	if err != nil {
		t.Fatal(err)
	}
	near, far := link.Memory()
	a, b := link.Manage(near), link.Manage(far)
	defer func() { _ = a.Close(); _ = b.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, b); served <- err }()
	_, joinErr := builder.Join(ctx, a)
	serveErr := <-served
	matched := 0
	for i, err := range []error{joinErr, serveErr} {
		var mismatch *h.PayloadDepthMismatch
		if !errors.As(err, &mismatch) {
			if err == nil {
				t.Fatal("mismatched session succeeded")
			}
			continue
		}
		matched++
		if !errors.Is(err, h.ErrPayloadDepthMismatch) {
			t.Fatalf("lost mismatch sentinel: %v", err)
		}
		wantLocal, wantRemote := settings.PayloadDepthLimit, source.SynchronizationSettings().PayloadDepthLimit
		if i == 1 {
			wantLocal, wantRemote = wantRemote, wantLocal
		}
		if mismatch.Local != wantLocal || mismatch.Remote != wantRemote {
			t.Fatalf("wrong depth operands: %+v", mismatch)
		}
	}
	if matched == 0 {
		t.Fatalf("no public depth mismatch: %v / %v", joinErr, serveErr)
	}
}

func TestPublicNetworkMismatchEventBounds(t *testing.T) {
	left, right := publicDiagnosticPeer(t), publicDiagnosticPeer(t)
	if err := left.SendAll([][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Send([]byte("c")); err != nil {
		t.Fatal(err)
	}
	a, b := link.Memory()
	near, far := link.Manage(a), link.Manage(b)
	defer func() { _ = near.Close(); _ = far.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := left.GossipOnce(ctx, near); done <- err }()
	_, rightErr := right.GossipOnce(ctx, far)
	leftErr := <-done
	matched := 0
	for i, err := range []error{leftErr, rightErr} {
		var mismatch *h.NetworkMismatch
		if !errors.As(err, &mismatch) {
			if err == nil {
				t.Fatal("mismatched session succeeded")
			}
			continue
		}
		matched++
		if !errors.Is(err, h.ErrNetworkMismatch) {
			t.Fatalf("lost mismatch sentinel: %v", err)
		}
		wantLocal, wantRemote := int64(2), int64(1)
		if i == 1 {
			wantLocal, wantRemote = wantRemote, wantLocal
		}
		if mismatch.LocalMinEvents.Int64() != wantLocal ||
			mismatch.RemoteMinEvents.Int64() != wantRemote ||
			mismatch.LocalNetwork == mismatch.RemoteNetwork {
			t.Fatalf("wrong network operands: %+v", mismatch)
		}
	}
	if matched == 0 {
		t.Fatalf("no public network mismatch: %v / %v", leftErr, rightErr)
	}
}

func TestPublicMalformedGreetingIsProtocolFailure(t *testing.T) {
	peer := publicDiagnosticPeer(t)
	input := append(diagnosticPreamble(2), 0xd8, 0x18, 0x41, 0xa6)
	carrier := link.Manage(diagnosticLink{&diagnosticControl{reader: bytes.NewReader(input)}})
	defer func() { _ = carrier.Close() }()
	_, err := peer.GossipOnce(t.Context(), carrier)
	var violation *h.ProtocolViolation
	if !errors.As(err, &violation) || violation.Context.Phase != h.PhaseGreeting {
		t.Fatalf("malformed complete greeting misclassified: %v", err)
	}
	var transport *h.TransportError
	if errors.As(err, &transport) {
		t.Fatalf("decoder EOF became transport departure: %v", err)
	}
}

func TestPublicPayloadCallbackFailureKeepsDataContext(t *testing.T) {
	source := publicDiagnosticPeer(t)
	if _, err := source.Send([]byte("bad")); err != nil {
		t.Fatal(err)
	}
	codec := h.BytesCodec()
	decode := codec.DecodeBounded
	rejection := &net.DNSError{Err: "typed payload rejected"}
	codec.DecodeBounded = func(data []byte, limit uint64) ([]byte, error) {
		payload, err := decode(data, limit)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(payload, []byte("bad")) {
			return nil, rejection
		}
		return payload, nil
	}
	near, far := link.Memory()
	a, b := link.Manage(near), link.Manage(far)
	defer func() { _ = a.Close(); _ = b.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, b); served <- err }()
	_, err := h.Bootstrap(codec).Join(ctx, a)
	<-served
	var violation *h.ProtocolViolation
	if !errors.As(err, &violation) || !errors.Is(err, rejection) {
		t.Fatalf("payload callback cause misclassified: %v", err)
	}
	stream := violation.Context.DataStream
	if violation.Context.Phase != h.PhaseReconciliation || stream == nil || stream.Index == nil {
		t.Fatalf("lost payload stream context: %+v", violation.Context)
	}
	var transport *h.TransportError
	if errors.As(err, &transport) {
		t.Fatalf("typed decoder error became transport: %v", err)
	}
}

func TestPublicWriterAbortKeepsDecisiveOperation(t *testing.T) {
	failure := errors.New("decisive outgoing transport failure")
	for _, operation := range []h.TransportOperation{h.TransportWrite, h.TransportFlush} {
		t.Run(string(operation), func(t *testing.T) {
			peer := publicDiagnosticPeer(t)
			control := &diagnosticControl{blockRead: true, closed: make(chan struct{})}
			if operation == h.TransportWrite {
				control.writeErr = failure
			} else {
				control.flushErr = failure
			}
			carrier := link.Manage(diagnosticLink{control})
			defer func() { _ = carrier.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := peer.GossipOnce(ctx, carrier)
			var transport *h.TransportError
			if !errors.As(err, &transport) || !errors.Is(err, failure) ||
				transport.Operation != operation {
				t.Fatalf("abort-induced read hid original operation: %v", err)
			}
			if transport.Context.Phase != h.PhasePreamble {
				t.Fatalf("wrong phase: %+v", transport.Context)
			}
		})
	}
}

func TestPublicSessionContextUsesOutermostDiagnostic(t *testing.T) {
	index := uint8(5)
	want := h.ErrorContext{
		Phase:      h.PhaseReconciliation,
		DataStream: &h.DataStream{Sender: observe.Responder, Index: &index},
	}
	nested := &h.TransportError{
		Context:   h.ErrorContext{Phase: h.PhaseGreeting},
		Operation: h.TransportRead,
		Err:       io.EOF,
	}
	cause := &h.ProtocolViolation{Context: want, Err: nested}
	failure := &h.SessionError{Phase: "reconciliation", Err: cause}
	got := failure.Context()
	if got.Phase != want.Phase || got.DataStream != want.DataStream {
		t.Fatalf("nested codec diagnostic overrode receiving context: %+v", got)
	}
	if !errors.Is(failure, io.EOF) {
		t.Fatal("lost nested original cause")
	}
}

func TestPublicIdleDiagnosticsPreserveCleanEOF(t *testing.T) {
	failure := errors.New("idle control read failed")
	for _, cause := range []error{io.EOF, failure} {
		peer := publicDiagnosticPeer(t)
		carrier := link.Manage(diagnosticLink{&diagnosticControl{readErr: cause}})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		options := h.GossipOptions[[]byte]{
			When: func(context.Context, *h.Changes[[]byte]) <-chan h.GossipRequest { return nil },
		}
		count := 0
		for _, err := range peer.Gossip(ctx, carrier, options) {
			count++
			var transport *h.TransportError
			if !errors.As(err, &transport) || !errors.Is(err, failure) ||
				transport.Operation != h.TransportRead ||
				transport.Context.Phase != h.PhaseIdle {
				t.Fatalf("wrong idle diagnostic: %v", err)
			}
		}
		cancel()
		_ = carrier.Close()
		if cause == io.EOF && count != 0 {
			t.Fatal("clean idle EOF became a session failure")
		}
		if cause != io.EOF && count != 1 {
			t.Fatalf("idle failure yielded %d errors", count)
		}
	}
}
