package observe

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

type recording struct {
	mu       sync.Mutex
	info     []SessionInfo
	streams  []StreamInfo
	roles    []Role
	items    map[StreamInfo][][]byte
	outcomes []Outcome
	calls    []string
}

func (r *recording) Session(info SessionInfo) SessionObserver {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.info = append(r.info, info)
	r.calls = append(r.calls, "session")
	return r
}

func (r *recording) Stream(info StreamInfo) StreamObserver {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.streams = append(r.streams, info)
	r.calls = append(r.calls, "stream")
	return &recordingStream{record: r, info: info}
}

func (r *recording) Elected(role Role) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.roles = append(r.roles, role)
	r.calls = append(r.calls, "elected")
}

func (r *recording) Finished(outcome Outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, outcome)
	r.calls = append(r.calls, "finished")
}

type recordingStream struct {
	record *recording
	info   StreamInfo
}

func (s *recordingStream) Message(item []byte) {
	s.record.mu.Lock()
	defer s.record.mu.Unlock()
	if s.record.items == nil {
		s.record.items = make(map[StreamInfo][][]byte)
	}
	s.record.items[s.info] = append(s.record.items[s.info], bytes.Clone(item))
	s.record.calls = append(s.record.calls, "message")
}

func TestDisabledAndDeclinedSessionsDoNotAllocate(t *testing.T) {
	var none Attachment
	declined := none.With(ObserverFunc(func(SessionInfo) SessionObserver { return nil }))
	for _, attachment := range []Attachment{none, declined} {
		allocations := testing.AllocsPerRun(100, func() {
			session := attachment.Begin(Gossip)
			if session.Attached() || session.ControlAttached(Sent) ||
				session.ControlAttached(Received) {
				panic("disabled session attached")
			}
			session.ControlSent(nil)
			session.ControlReceived(nil)
			session.Elected(Initiator)
			if session.Data(Initiator, 0, Sent) != nil {
				panic("disabled data attached")
			}
			session.Finish(Outcome{Kind: Cancelled})
		})
		if allocations != 0 {
			t.Fatalf("disabled hooks allocated %g times", allocations)
		}
	}
}

func TestBeginCreatesDirectedControlBeforeAnyItems(t *testing.T) {
	r := new(recording)
	session := (Attachment{}).With(r).Begin(Bootstrap)
	if !session.Attached() || !session.ControlAttached(Sent) || !session.ControlAttached(Received) {
		t.Fatal("accepted session disabled")
	}
	if !reflect.DeepEqual(r.info, []SessionInfo{{Kind: Bootstrap, Protocol: V2}}) {
		t.Fatal("incorrect session metadata")
	}
	want := []StreamInfo{
		{ID: StreamID{Kind: Control}, Direction: Sent},
		{ID: StreamID{Kind: Control}, Direction: Received},
	}
	if !reflect.DeepEqual(r.streams, want) ||
		!reflect.DeepEqual(r.calls, []string{"session", "stream", "stream"}) {
		t.Fatal("control handlers did not open before traffic")
	}
	item := []byte{0x18, 1}
	session.ControlSent(item)
	session.ControlReceived(item)
	item[1] = 99
	if r.items[want[0]][0][1] != 1 {
		t.Fatal("recorder failed to own borrowed bytes")
	}
	session.Finish(Outcome{Kind: Completed})
	if len(r.roles) != 0 {
		t.Fatal("control-only session elected role")
	}
}

type orderedObserver struct {
	index int
	calls *[]int
}

func (o orderedObserver) Session(SessionInfo) SessionObserver {
	*o.calls = append(*o.calls, o.index)
	return o
}

func (o orderedObserver) Stream(StreamInfo) StreamObserver {
	*o.calls = append(*o.calls, o.index)
	return o
}
func (o orderedObserver) Message([]byte)   { *o.calls = append(*o.calls, o.index) }
func (o orderedObserver) Elected(Role)     { *o.calls = append(*o.calls, o.index) }
func (o orderedObserver) Finished(Outcome) { *o.calls = append(*o.calls, o.index) }

func TestAttachmentOrderAndPersistentConfiguration(t *testing.T) {
	var attachment Attachment
	var calls []int
	for i := 0; i < 5; i++ {
		prior := attachment
		attachment = attachment.With(orderedObserver{index: i, calls: &calls})
		if prior.Len() != i {
			t.Fatal("attachment modified prior snapshot")
		}
	}
	session := attachment.Begin(Gossip)
	session.Elected(Responder)
	session.ControlSent(nil)
	session.ControlReceived(nil)
	session.Data(Responder, 3, Sent).Message(nil)
	session.Finish(Outcome{Kind: Completed})
	var want []int
	for pass := 0; pass < 9; pass++ {
		for i := 0; i < 5; i++ {
			want = append(want, i)
		}
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("attachment call order %v", calls)
	}
}

type declinedStreams struct{ finished int }

func (*declinedStreams) Stream(StreamInfo) StreamObserver { return nil }
func (d *declinedStreams) Finished(Outcome)               { d.finished++ }

func TestDeclinedStreamsStillFinishSession(t *testing.T) {
	handler := new(declinedStreams)
	session := (Attachment{}).With(ObserverFunc(func(SessionInfo) SessionObserver { return handler })).
		Begin(Retire)
	if session.ControlAttached(Sent) || session.ControlAttached(Received) ||
		session.Data(Initiator, 1, Received) != nil {
		t.Fatal("declined stream enabled capture")
	}
	session.ControlSent(nil)
	session.ControlReceived(nil)
	session.Finish(Outcome{Kind: Failed, Failure: BookmarkFailure})
	session.Finish(Outcome{Kind: Cancelled})
	if handler.finished != 1 {
		t.Fatal("declined streams changed terminal callback count")
	}
}

func TestConcurrentStreamsKeepIndependentOrder(t *testing.T) {
	r := new(recording)
	session := (Attachment{}).With(r).Begin(Gossip)
	session.Elected(Initiator)
	session.Elected(Responder)
	var wg sync.WaitGroup
	for index := uint8(0); index < 8; index++ {
		wg.Go(func() {
			stream := session.Data(Initiator, index, Sent)
			for item := 0; item < 100; item++ {
				stream.Message([]byte{byte(item)})
			}
		})
	}
	wg.Wait()
	frontier := before.Version{}.Tick(before.SeedParty())
	outcome := Outcome{
		Kind:      Completed,
		Converged: frontier,
		Stats:     Stats{FramesSent: 800, StreamsOpened: 8},
	}
	session.Finish(outcome)
	if len(r.roles) != 1 || r.roles[0] != Initiator {
		t.Fatal("role callback not at-most-once")
	}
	for index := uint8(0); index < 8; index++ {
		info := StreamInfo{
			ID:        StreamID{Kind: Data, Speaker: Initiator, Index: index},
			Direction: Sent,
		}
		if len(r.items[info]) != 100 {
			t.Fatal("stream lost messages")
		}
		for i, item := range r.items[info] {
			if !bytes.Equal(item, []byte{byte(i)}) {
				t.Fatal("directed stream order changed")
			}
		}
	}
	if len(r.outcomes) != 1 || r.outcomes[0].Stats != outcome.Stats ||
		!r.outcomes[0].Converged.Equal(frontier) {
		t.Fatal("terminal outcome lost diagnostics")
	}
	if r.calls[len(r.calls)-1] != "finished" {
		t.Fatal("terminal callback preceded stream callbacks")
	}
}

type blockedMessage struct {
	entered  chan struct{}
	release  chan struct{}
	finished chan struct{}
	mu       sync.Mutex
	calls    []string
}

func (b *blockedMessage) Stream(StreamInfo) StreamObserver { return b }
func (b *blockedMessage) Message([]byte) {
	close(b.entered)
	<-b.release
	b.mu.Lock()
	b.calls = append(b.calls, "message returned")
	b.mu.Unlock()
}

func (b *blockedMessage) Finished(Outcome) {
	b.mu.Lock()
	b.calls = append(b.calls, "finished")
	b.mu.Unlock()
	close(b.finished)
}

func TestFinishDrainsCallbacksAndSuppressesLateEvents(t *testing.T) {
	handler := &blockedMessage{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	session := (Attachment{}).With(ObserverFunc(func(SessionInfo) SessionObserver { return handler })).
		Begin(Gossip)
	stream := session.Data(Initiator, 2, Sent)
	messageDone := make(chan struct{})
	go func() { stream.Message(nil); close(messageDone) }()
	<-handler.entered
	var finishes sync.WaitGroup
	for i := 0; i < 10; i++ {
		finishes.Go(func() { session.Finish(Outcome{Kind: Cancelled}) })
	}
	close(handler.release)
	<-messageDone
	finishes.Wait()
	<-handler.finished
	if !reflect.DeepEqual(handler.calls, []string{"message returned", "finished"}) {
		t.Fatal("finish did not drain in-flight callback")
	}
	stream.Message(nil)
	session.ControlSent(nil)
	session.ControlReceived(nil)
	session.Elected(Initiator)
	if session.Data(Initiator, 3, Sent) != nil {
		t.Fatal("finished session accepted new stream")
	}
	if len(handler.calls) != 2 {
		t.Fatal("late callback followed terminal outcome")
	}
}

func TestConcurrentSessionFactories(t *testing.T) {
	r := new(recording)
	attachment := (Attachment{}).With(r)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			session := attachment.Begin(Gossip)
			session.ControlSent([]byte{0})
			session.Finish(Outcome{Kind: Completed})
		})
	}
	wg.Wait()
	if len(r.info) != 20 || len(r.outcomes) != 20 {
		t.Fatal("concurrent sessions lost lifecycle callbacks")
	}
}

type dataAndErrorReader struct{ used bool }

func (r *dataAndErrorReader) Read(p []byte) (int, error) {
	if r.used {
		return 0, io.EOF
	}
	r.used = true
	return copy(p, []byte{0x18, 1}), io.EOF
}

func TestCaptureReaderExactWireBytes(t *testing.T) {
	item := []byte{0x18, 1}
	input := bytes.NewReader(append(bytes.Clone(item), 0x00))
	capture := NewCaptureReader(input)
	for range item {
		var one [1]byte
		if _, err := io.ReadFull(capture, one[:]); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(capture.Bytes(), item) || input.Len() != 1 {
		t.Fatal("capture read ahead or changed legal alternate wire spelling")
	}
	withError := NewCaptureReader(new(dataAndErrorReader))
	var buffer [4]byte
	n, err := withError.Read(buffer[:])
	if n != 2 || !errors.Is(err, io.EOF) || !bytes.Equal(withError.Bytes(), item) {
		t.Fatalf("capture lost bytes returned alongside error: %d %v", n, err)
	}
}
