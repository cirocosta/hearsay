package testlink

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/link"
)

func testPair(t *testing.T, config Config) (*Link, *Link) {
	t.Helper()
	a, b, err := Pair(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	return a, b
}

func testStream(t *testing.T, a, b *Link) (io.WriteCloser, io.ReadCloser) {
	t.Helper()
	writer, err := a.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := b.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	return writer, reader
}

func TestConfigurationValidation(t *testing.T) {
	for _, config := range []Config{{RTT: -1}, {StreamWindow: -1}, {PacketSize: -1}, {StreamWindow: 1, PacketSize: 2}} {
		if a, b, err := Pair(config); err == nil {
			_ = a.Close()
			_ = b.Close()
			t.Fatalf("accepted %+v", config)
		}
	}
	a, _ := testPair(t, Config{StreamWindow: 1})
	if a.pair.config.PacketSize != 1 {
		t.Fatal("default packet does not fit window")
	}
}

func TestObservedPacketAndTrainLatency(t *testing.T) {
	for _, count := range []int{1, 20} {
		t.Run(map[int]string{1: "packet", 20: "train"}[count], func(t *testing.T) {
			const packetSize = 100
			const rate = 20_000
			const rtt = 80 * time.Millisecond
			a, b := testPair(
				t,
				Config{RTT: rtt, BytesPerSecond: rate, StreamWindow: 4096, PacketSize: packetSize},
			)
			writer, reader := testStream(t, a, b)
			payload := bytes.Repeat([]byte{7}, count*packetSize)
			start := time.Now()
			if _, err := writer.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(reader)
			elapsed := time.Since(start)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatal(len(got), err)
			}
			expected := rtt/2 + time.Duration(len(payload))*time.Second/rate
			if elapsed < expected-5*time.Millisecond || elapsed > expected+200*time.Millisecond {
				t.Fatalf("%d packet delivery took%s; schedule requires%s", count, elapsed, expected)
			}
			t.Logf("packets=%d observed=%s scheduled=%s", count, elapsed, expected)
		})
	}
}

func TestStreamsShareAggregateSerializationRate(t *testing.T) {
	const rate = 10_000
	const size = 1000
	a, b := testPair(
		t,
		Config{
			RTT:            20 * time.Millisecond,
			BytesPerSecond: rate,
			StreamWindow:   4096,
			PacketSize:     100,
		},
	)
	aw, ar := testStream(t, a, b)
	bw, br := testStream(t, a, b)
	start := time.Now()
	for _, writer := range []io.WriteCloser{aw, bw} {
		if _, err := writer.Write(make([]byte, size)); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, reader := range []io.Reader{ar, br} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload, err := io.ReadAll(reader)
			if err == nil && len(payload) != size {
				err = io.ErrUnexpectedEOF
			}
			results <- err
		}()
	}
	wg.Wait()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	expected := 20*time.Millisecond/2 + 2*size*time.Second/rate
	if elapsed < expected-10*time.Millisecond || elapsed > expected+200*time.Millisecond {
		t.Fatalf("aggregate delivery took%s; shared rate requires%s", elapsed, expected)
	}
	if got := a.Stats().DeliveredBytes; got != 2*size {
		t.Fatal("wrong aggregate bytes", got)
	}
}

func awaitCondition(t *testing.T, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("condition did not become true")
		}
	}
}

func TestBlockedReaderDoesNotBlockOtherStream(t *testing.T) {
	const window = 4096
	a, b := testPair(t, Config{
		RTT: 2 * time.Millisecond, BytesPerSecond: 1_000_000,
		StreamWindow: window, PacketSize: 1024,
	})
	blocked, _ := testStream(t, a, b)
	outgoing, incoming := testStream(t, a, b)
	blockedResult := make(chan error, 1)
	go func() { _, err := blocked.Write(make([]byte, 1<<20)); blockedResult <- err }()
	awaitCondition(
		t,
		func() bool { return a.Stats().DeliveredBytes >= 8192 && a.Stats().QueuedBytes == window },
	)
	if _, err := outgoing.Write([]byte("independent")); err != nil {
		t.Fatal(err)
	}
	if err := outgoing.Close(); err != nil {
		t.Fatal(err)
	}
	received := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(incoming)
		if err == nil && string(got) != "independent" {
			err = errors.New("wrong independent payload")
		}
		received <- err
	}()
	select {
	case err := <-received:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated blocked reader stopped delivery")
	}
	if _, err := a.Control().Write([]byte{9}); err != nil {
		t.Fatal(err)
	}
	var controlByte [1]byte
	if _, err := io.ReadFull(b.Control(), controlByte[:]); err != nil || controlByte[0] != 9 {
		t.Fatal("blocked data stream consumed control credit", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-blockedResult; err == nil {
		t.Fatal("aborted blocked write succeeded")
	}
	if stats := a.Stats(); stats.QueuedBytes != 0 || stats.PeakQueuedBytes > 2*window {
		t.Fatal("queue credit did not remain bounded", stats)
	}
}

func TestControlIsDuplexAndUsesOwnCredit(t *testing.T) {
	a, b := testPair(t, Config{StreamWindow: 1, PacketSize: 1})
	writeDone := make(chan error, 2)
	for _, endpoint := range []*Link{a, b} {
		go func() { _, err := endpoint.Control().Write(bytes.Repeat([]byte{3}, 128)); writeDone <- err }()
	}
	for _, endpoint := range []*Link{a, b} {
		data := make([]byte, 128)
		if _, err := io.ReadFull(endpoint.Control(), data); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := <-writeDone; err != nil {
			t.Fatal(err)
		}
	}
}

func TestExactLabelsFramesAndCleanDrain(t *testing.T) {
	a, b := testPair(t, Config{RTT: 10 * time.Millisecond, StreamWindow: 1024, PacketSize: 1})
	outgoing, reader := testStream(t, a, b)
	label := protocol.Label{Epoch: 201, Stream: 16}
	data, err := label.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, signal := range []protocol.Signal{protocol.QueryEmptyEnd, protocol.StreamEnd} {
		data, err = (protocol.Frame{Stream: 16, Signal: signal}).AppendTo(data, protocol.Initiator)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := outgoing.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := link.Complete(outgoing); err != nil {
		t.Fatal(err)
	}
	if got, err := protocol.ReadLabel(reader, 201); err != nil || got != label {
		t.Fatal(got, err)
	}
	for _, signal := range []protocol.Signal{protocol.QueryEmptyEnd, protocol.StreamEnd} {
		got, err := protocol.ReadFrame(reader, protocol.Initiator, protocol.FrameLimits{})
		if err != nil || got.Signal != signal {
			t.Fatal(got, err)
		}
	}
	var trailing [1]byte
	if _, err := reader.Read(trailing[:]); err != io.EOF {
		t.Fatal("clean stream did not drain before EOF", err)
	}
	<-outgoing.(*writer).done
	if stats := a.Stats(); stats.QueuedBytes != 0 || stats.DeliveredBytes != uint64(len(data)) {
		t.Fatal(stats)
	}
	if _, err := outgoing.Write([]byte{0}); err == nil {
		t.Fatal("completed writer accepted new bytes")
	}
}

func TestCloseAbortsTimersAndJoinsWorkers(t *testing.T) {
	a, b := testPair(t, Config{RTT: time.Hour, StreamWindow: 4, PacketSize: 1})
	outgoing, _ := testStream(t, a, b)
	if _, err := outgoing.Write([]byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- a.Close() }()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pair close leaked delivery worker")
	}
	for _, worker := range []*writer{a.control.writer, b.control.writer, outgoing.(*writer)} {
		select {
		case <-worker.done:
		default:
			t.Fatal("Close returned before joining worker")
		}
	}
	if a.Stats().QueuedBytes != 0 {
		t.Fatal("cancellation retained packets", a.Stats())
	}
	if _, err := b.Open(context.Background()); err == nil {
		t.Fatal("closed pair opened stream")
	}
}

func TestAcceptCancellationPreservesQueuedStream(t *testing.T) {
	a, b := testPair(t, Config{})
	writer, err := a.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Accept(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	reader, err := b.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if _, err := writer.Write([]byte{9}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, []byte{9}) {
		t.Fatal(got, err)
	}
}

func TestConcurrentOpenAndPairCloseJoinsEveryWorker(t *testing.T) {
	a, _ := testPair(t, Config{RTT: time.Hour, StreamWindow: 1, PacketSize: 1})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var producers sync.WaitGroup
	started := make(chan struct{})
	for range 64 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			<-started
			stream, err := a.Open(ctx)
			if err != nil {
				return
			}
			_, _ = stream.Write([]byte{1})
			_ = stream.Close()
		}()
	}
	close(started)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	joined := make(chan struct{})
	go func() { producers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("concurrent open/close leaked producer")
	}
	if a.Stats().QueuedBytes != 0 {
		t.Fatal("closed pair retained queued bytes")
	}
}
