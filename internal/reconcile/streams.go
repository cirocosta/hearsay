// Package reconcile drives the streaming V2 tree reconciliation protocol.
package reconcile

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/link"
)

var (
	errDuplicateStream   = errors.New("duplicate logical data stream")
	errStreamFinished    = errors.New("logical data stream already finished")
	errExpectedStreamEnd = errors.New("expected logical stream end")
	errUnusedStream      = errors.New("unexpected unused logical data stream")
)

type streamCounters struct {
	BytesSent      uint64
	BytesReceived  uint64
	FramesSent     uint64
	FramesReceived uint64
	StreamsOpened  uint64
}

type streamStats struct {
	bytesSent      atomic.Uint64
	bytesReceived  atomic.Uint64
	framesSent     atomic.Uint64
	framesReceived atomic.Uint64
	streamsOpened  atomic.Uint64
}

type streamWriter struct {
	mu     sync.Mutex
	writer io.WriteCloser
	done   bool
	buf    []byte
}

type streamReader struct {
	mu     sync.Mutex
	ready  chan struct{}
	reader io.ReadCloser
	used   bool
	done   bool
	seen   bool
}

// streamSet routes independent session data streams. Each logical stream has
// one sequential producer and one sequential consumer; different streams may
// progress concurrently. The transport control stream is never borrowed here.
type streamSet struct {
	ctx           context.Context
	cancel        context.CancelFunc
	transport     link.Link
	epoch         uint8
	local         protocol.Role
	remote        protocol.Role
	limits        protocol.FrameLimits
	outgoing      [link.StreamCount]streamWriter
	incoming      [link.StreamCount]streamReader
	stats         streamStats
	fatal         chan error
	firstFailure  sync.Once
	shutdownOnce  sync.Once
	shutdownErr   error
	stopWatch     func() bool
	acceptDone    chan struct{}
	mu            sync.Mutex
	pending       io.ReadCloser
	activeReaders [link.StreamCount]io.ReadCloser
	activeWriters [link.StreamCount]io.WriteCloser
	closing       bool
	failure       error
}

func newStreamSet(
	ctx context.Context,
	transport link.Link,
	epoch uint8,
	localRole protocol.Role,
	limits protocol.FrameLimits,
) *streamSet {
	runCtx, cancel := context.WithCancel(ctx)
	s := &streamSet{
		ctx:        runCtx,
		cancel:     cancel,
		transport:  transport,
		epoch:      epoch,
		local:      localRole,
		remote:     localRole ^ 1,
		limits:     limits,
		fatal:      make(chan error, 1),
		acceptDone: make(chan struct{}),
	}
	for i := range s.incoming {
		s.incoming[i].ready = make(chan struct{})
	}
	s.stopWatch = context.AfterFunc(ctx, func() { s.fail(ctx.Err()) })
	go s.acceptLoop()
	return s
}

func (s *streamSet) fail(err error) {
	if err == nil {
		return
	}
	s.firstFailure.Do(func() {
		s.mu.Lock()
		s.failure = err
		s.mu.Unlock()
		s.fatal <- err
		s.cancel()
		_ = s.transport.Close()
	})
}

func (s *streamSet) failures() <-chan error { return s.fatal }

func (s *streamSet) acceptLoop() {
	defer close(s.acceptDone)
	for {
		reader, err := s.transport.Accept(s.ctx)
		if err != nil {
			s.acceptError(err)
			return
		}
		if !s.setPending(reader) {
			_ = reader.Close()
			return
		}
		label, err := protocol.ReadLabel(reader, s.epoch)
		if err != nil {
			s.acceptError(err)
			return
		}
		if err = s.route(reader, label.Stream); err != nil {
			s.acceptError(err)
			return
		}
	}
}

func (s *streamSet) acceptError(err error) {
	if s.ctx.Err() == nil {
		s.fail(err)
	}
}

func (s *streamSet) setPending(reader io.ReadCloser) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.pending = reader
	return true
}

func (s *streamSet) route(reader io.ReadCloser, index uint8) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return context.Canceled
	}
	slot := &s.incoming[index]
	if slot.seen {
		return errDuplicateStream
	}
	slot.seen = true
	slot.reader = reader
	s.activeReaders[index] = reader
	s.pending = nil
	close(slot.ready)
	return nil
}

func (s *streamSet) send(index uint8, frame protocol.Frame) error {
	if index >= link.StreamCount || frame.Stream != index {
		return protocol.ErrStream
	}
	slot := &s.outgoing[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.done {
		return errStreamFinished
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	encoded, err := frame.AppendTo(slot.buf[:0], s.local)
	if err != nil {
		return err
	}
	slot.buf = encoded
	if err = s.openWriter(index, slot); err != nil {
		return err
	}
	if err = writeAll(slot.writer, encoded, &s.stats.bytesSent); err != nil {
		return err
	}
	if err = flushStream(slot.writer); err != nil {
		return err
	}
	s.stats.framesSent.Add(1)
	return nil
}

func (s *streamSet) openWriter(index uint8, slot *streamWriter) error {
	if slot.writer != nil {
		return nil
	}
	writer, err := s.transport.Open(s.ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = writer.Close()
		return context.Canceled
	}
	slot.writer = writer
	s.activeWriters[index] = writer
	s.mu.Unlock()
	s.stats.streamsOpened.Add(1)
	label, _ := (protocol.Label{Epoch: s.epoch, Stream: index}).AppendTo(nil)
	return writeAll(writer, label, nil)
}

func writeAll(writer io.Writer, data []byte, counter *atomic.Uint64) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		if counter != nil {
			counter.Add(uint64(n))
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func flushStream(writer io.Writer) error {
	if flusher, ok := writer.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

func (s *streamSet) receive(index uint8) (protocol.Frame, error) {
	if index >= link.StreamCount {
		return protocol.Frame{}, protocol.ErrStream
	}
	slot := &s.incoming[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.done {
		return protocol.Frame{}, errStreamFinished
	}
	select {
	case <-s.ctx.Done():
		return protocol.Frame{}, s.ctx.Err()
	case <-slot.ready:
	}
	slot.used = true
	return s.readFrame(index, slot.reader)
}

type countedReader struct {
	reader  io.Reader
	counter *atomic.Uint64
}

func (r countedReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.counter.Add(uint64(n))
	}
	return n, err
}

func (s *streamSet) readFrame(index uint8, reader io.Reader) (protocol.Frame, error) {
	frame, err := protocol.ReadFrame(
		countedReader{reader, &s.stats.bytesReceived},
		s.remote,
		s.limits,
	)
	if err != nil {
		return protocol.Frame{}, err
	}
	if frame.Stream != index {
		return protocol.Frame{}, protocol.ErrStream
	}
	s.stats.framesReceived.Add(1)
	return frame, nil
}

func (s *streamSet) finishSend(index uint8) error {
	if index >= link.StreamCount {
		return protocol.ErrStream
	}
	slot := &s.outgoing[index]
	// No stream is created for a direction that sent no replies.
	slot.mu.Lock()
	if slot.writer == nil || slot.done {
		slot.mu.Unlock()
		return nil
	}
	slot.mu.Unlock()
	if err := s.send(index, protocol.Frame{Stream: index, Signal: protocol.StreamEnd}); err != nil {
		return err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.done = true
	if err := link.Complete(slot.writer); err != nil {
		return err
	}
	s.mu.Lock()
	s.activeWriters[index] = nil
	s.mu.Unlock()
	return nil
}

func (s *streamSet) finishReceive(index uint8) error {
	if index >= link.StreamCount {
		return protocol.ErrStream
	}
	slot := &s.incoming[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if !slot.used || slot.done {
		return nil
	}
	frame, err := s.readFrame(index, slot.reader)
	if err != nil {
		return err
	}
	if frame.Signal != protocol.StreamEnd {
		return errExpectedStreamEnd
	}
	slot.done = true
	if err := link.Complete(slot.reader); err != nil {
		return err
	}
	s.mu.Lock()
	s.activeReaders[index] = nil
	s.mu.Unlock()
	return nil
}

func (s *streamSet) shutdown(success bool) error {
	s.shutdownOnce.Do(func() { s.shutdownErr = s.stop(success) })
	return s.shutdownErr
}

func (s *streamSet) stop(success bool) error {
	s.stopWatch()
	s.cancel()
	var err error
	if !success {
		err = s.transport.Close()
	}
	err = errors.Join(err, s.closeActive())
	<-s.acceptDone
	s.mu.Lock()
	err = errors.Join(err, s.failure)
	s.mu.Unlock()
	if success {
		err = errors.Join(err, s.unfinishedStreams())
	}
	return err
}

func (s *streamSet) closeActive() error {
	s.mu.Lock()
	s.closing = true
	var closers []io.Closer
	if s.pending != nil {
		closers = append(closers, s.pending)
	}
	for _, reader := range s.activeReaders {
		if reader != nil {
			closers = append(closers, reader)
		}
	}
	for _, writer := range s.activeWriters {
		if writer != nil {
			closers = append(closers, writer)
		}
	}
	s.mu.Unlock()
	var err error
	for _, closer := range closers {
		err = errors.Join(err, closer.Close())
	}
	return err
}

func (s *streamSet) unfinishedStreams() error {
	if s.pending != nil {
		return errUnusedStream
	}
	for i := range s.incoming {
		slot := &s.incoming[i]
		slot.mu.Lock()
		unfinished := slot.seen && (!slot.used || !slot.done)
		slot.mu.Unlock()
		if unfinished {
			return errUnusedStream
		}
		writer := &s.outgoing[i]
		writer.mu.Lock()
		unfinished = writer.writer != nil && !writer.done
		writer.mu.Unlock()
		if unfinished {
			return errUnusedStream
		}
	}
	return nil
}

func (s *streamSet) snapshotStats() streamCounters {
	return streamCounters{
		BytesSent: s.stats.bytesSent.Load(), BytesReceived: s.stats.bytesReceived.Load(),
		FramesSent: s.stats.framesSent.Load(), FramesReceived: s.stats.framesReceived.Load(),
		StreamsOpened: s.stats.streamsOpened.Load(),
	}
}
