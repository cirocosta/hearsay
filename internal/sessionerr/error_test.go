package sessionerr

import (
	"errors"
	"io"
	"net"
	"testing"
)

func TestValidationFailureDoesNotBecomeTransport(t *testing.T) {
	for _, cause := range []error{io.EOF, io.ErrUnexpectedEOF, &net.DNSError{Err: "typed decoder failed"}, &TransportError{Operation: Read, Err: io.EOF}} {
		err := At(Context{Phase: Greeting}, Protocol(cause))
		var violation *ProtocolViolation
		if !errors.As(err, &violation) || violation.Context.Phase != Greeting ||
			!errors.Is(err, cause) {
			t.Fatalf("validation cause misclassified: %v", err)
		}
	}
}

func TestEOFOperationPreservesReaderContractOnly(t *testing.T) {
	if IO(Read, io.EOF) != io.EOF {
		t.Fatal("reader EOF contract changed")
	}
	for _, operation := range []Operation{Write, Flush, Open, Accept} {
		err := At(Context{Phase: Preamble}, IO(operation, io.EOF))
		var transport *TransportError
		if !errors.As(err, &transport) || transport.Operation != operation ||
			!errors.Is(err, io.EOF) {
			t.Fatalf("EOF operation lost: %v", err)
		}
	}
}

func TestControlBoundaryPreservesLocatedValidationStream(t *testing.T) {
	cause := io.ErrUnexpectedEOF
	first := Stream(1, 5, Protocol(cause))
	err := At(Context{Phase: Reconciliation}, first)
	var violation *ProtocolViolation
	if !errors.As(err, &violation) || !errors.Is(err, cause) {
		t.Fatalf("lost validation cause: %v", err)
	}
	stream := violation.Context.DataStream
	if stream == nil || stream.Index == nil || *stream.Index != 5 || stream.Sender != 1 {
		t.Fatalf("control boundary lost data context: %+v", stream)
	}
}

func TestTransportBoundaryOverridesCauseDiagnostic(t *testing.T) {
	nested := &ProtocolViolation{Context: Context{Phase: Greeting}, Err: io.EOF}
	err := At(Context{Phase: Completion}, IO(Write, nested))
	var transport *TransportError
	if !errors.As(err, &transport) || transport.Operation != Write ||
		transport.Context.Phase != Completion ||
		!errors.Is(err, io.EOF) {
		t.Fatalf("cause overrode actual transport boundary: %v", err)
	}
}
