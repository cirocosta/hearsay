package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/link"
)

func streamContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func TestStreamSetIndependentStreams(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	left, right := link.MemoryWithCapacity(1)
	a := newStreamSet(ctx, left, 7, protocol.Initiator, protocol.FrameLimits{})
	b := newStreamSet(ctx, right, 7, protocol.Responder, protocol.FrameLimits{})
	defer func() { _ = a.shutdown(false) }()
	defer func() { _ = b.shutdown(false) }()
	failures := make(chan error, link.StreamCount)
	for index := link.StreamCount - 1; index >= 0; index-- {
		go func(index uint8) {
			err := a.send(index, protocol.Frame{Stream: index, Signal: protocol.ReplyEnd})
			if err == nil {
				err = a.finishSend(index)
			}
			failures <- err
		}(uint8(index))
	}
	for index := uint8(0); index < link.StreamCount; index++ {
		frame, err := b.receive(index)
		if err != nil || frame.Stream != index || frame.Signal != protocol.ReplyEnd {
			t.Fatal(frame, err)
		}
		if err := b.finishReceive(index); err != nil {
			t.Fatal(err)
		}
	}
	for range link.StreamCount {
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	if err := a.shutdown(true); err != nil {
		t.Fatal(err)
	}
	if err := b.shutdown(true); err != nil {
		t.Fatal(err)
	}
	sent, received := a.snapshotStats(), b.snapshotStats()
	if sent.StreamsOpened != 17 || sent.FramesSent != 34 || sent.BytesSent != 102 {
		t.Fatal(sent)
	}
	if received.FramesReceived != 34 || received.BytesReceived != 102 {
		t.Fatal(received)
	}
	// A successful data phase leaves the persistent control stream usable.
	written := make(chan error, 1)
	go func() { _, err := left.Control().Write([]byte{9}); written <- err }()
	var p [1]byte
	if _, err := io.ReadFull(right.Control(), p[:]); err != nil || p[0] != 9 {
		t.Fatal(p, err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	_ = left.Close()
}

func TestStreamSetUnusedDirectionsStayLazy(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	left, _ := link.Memory()
	s := newStreamSet(ctx, left, 0, protocol.Initiator, protocol.FrameLimits{})
	for index := uint8(0); index < link.StreamCount; index++ {
		if err := s.finishSend(index); err != nil {
			t.Fatal(err)
		}
		if err := s.finishReceive(index); err != nil {
			t.Fatal(err)
		}
	}
	if s.snapshotStats().StreamsOpened != 0 {
		t.Fatal(s.snapshotStats())
	}
	if err := s.shutdown(true); err != nil {
		t.Fatal(err)
	}
	_ = left.Close()
}

func writeRawStream(ctx context.Context, transport link.Link, data []byte) error {
	writer, err := transport.Open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = writer.Close() }()
	return writeAll(writer, data, nil)
}

func TestStreamSetRejectsDuplicateAndEpoch(t *testing.T) {
	for _, wrongEpoch := range []bool{false, true} {
		t.Run(map[bool]string{false: "duplicate", true: "epoch"}[wrongEpoch], func(t *testing.T) {
			ctx, cancel := streamContext(t)
			defer cancel()
			left, right := link.Memory()
			s := newStreamSet(ctx, left, 8, protocol.Initiator, protocol.FrameLimits{})
			defer func() { _ = s.shutdown(false) }()
			label, _ := (protocol.Label{Epoch: 8, Stream: 1}).AppendTo(nil)
			if wrongEpoch {
				label[0] = 7
			}
			if err := writeRawStream(ctx, right, label); err != nil {
				t.Fatal(err)
			}
			if !wrongEpoch {
				if err := writeRawStream(ctx, right, label); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-s.failures():
				expected := errDuplicateStream
				if wrongEpoch {
					expected = protocol.ErrStream
				}
				if !errors.Is(err, expected) {
					t.Fatal(err)
				}
				if err := s.shutdown(true); !errors.Is(err, expected) {
					t.Fatalf("shutdown lost accept failure: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("accept failure did not propagate")
			}
		})
	}
}

func TestStreamSetFrameLabelMismatch(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	left, right := link.Memory()
	s := newStreamSet(ctx, left, 0, protocol.Initiator, protocol.FrameLimits{})
	defer func() { _ = s.shutdown(false) }()
	data, _ := (protocol.Label{Epoch: 0, Stream: 1}).AppendTo(nil)
	data, _ = (protocol.Frame{Stream: 2, Signal: protocol.MatchEnd}).AppendTo(
		data,
		protocol.Responder,
	)
	if err := writeRawStream(ctx, right, data); err != nil {
		t.Fatal(err)
	}
	if _, err := s.receive(1); !errors.Is(err, protocol.ErrStream) {
		t.Fatal(err)
	}
}

func TestStreamSetRequiresStreamEnd(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	left, right := link.Memory()
	s := newStreamSet(ctx, left, 0, protocol.Initiator, protocol.FrameLimits{})
	defer func() { _ = s.shutdown(false) }()
	data, _ := (protocol.Label{Epoch: 0, Stream: 1}).AppendTo(nil)
	for range 2 {
		data, _ = (protocol.Frame{Stream: 1, Signal: protocol.ReplyEnd}).AppendTo(
			data,
			protocol.Responder,
		)
	}
	if err := writeRawStream(ctx, right, data); err != nil {
		t.Fatal(err)
	}
	if _, err := s.receive(1); err != nil {
		t.Fatal(err)
	}
	if err := s.finishReceive(1); !errors.Is(err, errExpectedStreamEnd) {
		t.Fatal(err)
	}
}

type observedAccept struct {
	link.Link
	accepted chan struct{}
	once     sync.Once
}

func (l *observedAccept) Accept(ctx context.Context) (io.ReadCloser, error) {
	r, err := l.Link.Accept(ctx)
	if err == nil {
		l.once.Do(func() { close(l.accepted) })
	}
	return r, err
}

func TestStreamSetCancellationUnblocksUnlabeledStream(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	left, right := link.MemoryWithCapacity(1)
	observed := &observedAccept{Link: left, accepted: make(chan struct{})}
	s := newStreamSet(ctx, observed, 0, protocol.Initiator, protocol.FrameLimits{})
	writer, err := right.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	<-observed.accepted
	receiveDone := make(chan error, 1)
	go func() { _, err := s.receive(1); receiveDone <- err }()
	cancel()
	if err := s.shutdown(false); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-receiveDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte{0}); err == nil {
		t.Fatal("cancellation left writer open")
	}
}

type chunkWriter struct {
	bytes.Buffer
	limit int
}

func (w *chunkWriter) Write(
	p []byte,
) (int, error) {
	return w.Buffer.Write(p[:min(w.limit, len(p))])
}

func TestStreamWriteAll(t *testing.T) {
	writer := &chunkWriter{limit: 2}
	var count atomic.Uint64
	if err := writeAll(writer, []byte("abcdef"), &count); err != nil {
		t.Fatal(err)
	}
	if writer.String() != "abcdef" || count.Load() != 6 {
		t.Fatal(writer.String(), count.Load())
	}
	writer.limit = 0
	if err := writeAll(writer, []byte{1}, nil); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}

type completionReader struct {
	*bytes.Reader
	closed, completed atomic.Uint64
}

func (r *completionReader) Close() error    { r.closed.Add(1); return nil }
func (r *completionReader) Complete() error { r.completed.Add(1); return nil }

type completionLink struct {
	reader   *completionReader
	accepted atomic.Bool
}

func (l *completionLink) Control() io.ReadWriteCloser { panic("unexpected control access") }
func (l *completionLink) Open(context.Context) (io.WriteCloser, error) {
	return nil, errors.New("unexpected open")
}
func (l *completionLink) Close() error { return nil }
func (l *completionLink) Accept(ctx context.Context) (io.ReadCloser, error) {
	if l.accepted.CompareAndSwap(false, true) {
		return l.reader, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestStreamSetCompletedReadersAreNotClosedAgain(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	data, _ := (protocol.Label{Stream: 1}).AppendTo(nil)
	for _, signal := range []protocol.Signal{protocol.ReplyEnd, protocol.StreamEnd} {
		data, _ = (protocol.Frame{Stream: 1, Signal: signal}).AppendTo(data, protocol.Responder)
	}
	reader := &completionReader{Reader: bytes.NewReader(data)}
	transport := &completionLink{reader: reader}
	s := newStreamSet(ctx, transport, 0, protocol.Initiator, protocol.FrameLimits{})
	if _, err := s.receive(1); err != nil {
		t.Fatal(err)
	}
	if err := s.finishReceive(1); err != nil {
		t.Fatal(err)
	}
	if err := s.shutdown(true); err != nil {
		t.Fatal(err)
	}
	if reader.completed.Load() != 1 || reader.closed.Load() != 0 {
		t.Fatal(reader.completed.Load(), reader.closed.Load())
	}
}

type completionWriter struct {
	bytes.Buffer
	closed, completed, flushed atomic.Uint64
	flushErr                   error
}

func (w *completionWriter) Close() error    { w.closed.Add(1); return nil }
func (w *completionWriter) Complete() error { w.completed.Add(1); return nil }
func (w *completionWriter) Flush() error    { w.flushed.Add(1); return w.flushErr }

type writerLink struct{ writer io.WriteCloser }

func (l writerLink) Control() io.ReadWriteCloser                  { panic("unexpected control access") }
func (l writerLink) Open(context.Context) (io.WriteCloser, error) { return l.writer, nil }
func (l writerLink) Accept(ctx context.Context) (io.ReadCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (l writerLink) Close() error { return nil }

func TestStreamSetCompletedWritersAreNotClosedAgain(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	writer := &completionWriter{}
	s := newStreamSet(ctx, writerLink{writer}, 4, protocol.Initiator, protocol.FrameLimits{})
	if err := s.send(1, protocol.Frame{Stream: 1, Signal: protocol.QueryEmptyEnd}); err != nil {
		t.Fatal(err)
	}
	if err := s.finishSend(1); err != nil {
		t.Fatal(err)
	}
	if err := s.shutdown(true); err != nil {
		t.Fatal(err)
	}
	if writer.completed.Load() != 1 || writer.closed.Load() != 0 || writer.flushed.Load() != 2 {
		t.Fatal(writer.completed.Load(), writer.closed.Load(), writer.flushed.Load())
	}
	reader := bytes.NewReader(writer.Bytes())
	if _, err := protocol.ReadLabel(reader, 4); err != nil {
		t.Fatal(err)
	}
	for _, signal := range []protocol.Signal{protocol.QueryEmptyEnd, protocol.StreamEnd} {
		frame, err := protocol.ReadFrame(reader, protocol.Initiator, protocol.FrameLimits{})
		if err != nil || frame.Signal != signal {
			t.Fatal(frame, err)
		}
	}
	if reader.Len() != 0 {
		t.Fatal("trailing stream bytes")
	}
}

func TestStreamSetPropagatesFlushError(t *testing.T) {
	ctx, cancel := streamContext(t)
	defer cancel()
	expected := errors.New("flush failed")
	writer := &completionWriter{flushErr: expected}
	s := newStreamSet(ctx, writerLink{writer}, 0, protocol.Initiator, protocol.FrameLimits{})
	defer func() { _ = s.shutdown(false) }()
	if err := s.send(
		1,
		protocol.Frame{Stream: 1, Signal: protocol.ReplyEnd},
	); !errors.Is(
		err,
		expected,
	) {
		t.Fatal(err)
	}
	if s.snapshotStats().FramesSent != 0 {
		t.Fatal(s.snapshotStats())
	}
}
