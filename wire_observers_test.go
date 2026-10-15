package hearsay

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/observe"
)

type recordedItem struct {
	info  observe.StreamInfo
	bytes []byte
}
type recordedSession struct {
	mu        sync.Mutex
	info      observe.SessionInfo
	items     []recordedItem
	elections int
	outcomes  []observe.Outcome
}
type recordedStream struct {
	session *recordedSession
	info    observe.StreamInfo
}
type wireRecorder struct {
	mu       sync.Mutex
	sessions []*recordedSession
}

func (r *wireRecorder) Session(info observe.SessionInfo) observe.SessionObserver {
	s := &recordedSession{info: info}
	r.mu.Lock()
	r.sessions = append(r.sessions, s)
	r.mu.Unlock()
	return s
}

func (s *recordedSession) Stream(info observe.StreamInfo) observe.StreamObserver {
	return &recordedStream{s, info}
}
func (s *recordedSession) Elected(observe.Role) { s.mu.Lock(); s.elections++; s.mu.Unlock() }
func (s *recordedSession) Finished(outcome observe.Outcome) {
	s.mu.Lock()
	s.outcomes = append(s.outcomes, outcome)
	s.mu.Unlock()
}

func (s *recordedStream) Message(p []byte) {
	s.session.mu.Lock()
	s.session.items = append(s.session.items, recordedItem{s.info, bytes.Clone(p)})
	s.session.mu.Unlock()
}

func (r *wireRecorder) last(t *testing.T) *recordedSession {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sessions) == 0 {
		t.Fatal("no observed sessions")
	}
	return r.sessions[len(r.sessions)-1]
}

func TestWireObserversCaptureCompleteControlAndData(t *testing.T) {
	recorderA, recorderB := &wireRecorder{}, &wireRecorder{}
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.AttachObserver(recorderA); err != nil {
		t.Fatal(err)
	}
	source, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	if _, err := source.Send([]byte("observed")); err != nil {
		t.Fatal(err)
	}
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	joined, err := Bootstrap(BytesCodec()).WithObserver(recorderB).Join(ctx, near)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	live, err := joined.Peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	bootstrapStats := make([]observe.Stats, 0, 2)
	for _, recorder := range []*wireRecorder{recorderA, recorderB} {
		session := recorder.last(t)
		session.mu.Lock()
		if session.elections != 1 || len(session.outcomes) != 1 ||
			session.outcomes[0].Kind != observe.Completed {
			t.Fatalf(
				"bootstrap observation: elections=%d outcomes=%+v",
				session.elections,
				session.outcomes,
			)
		}
		sent, received, framesSent, framesReceived := uint64(0), uint64(0), uint64(0), uint64(0)
		controls := 0
		for _, item := range session.items {
			if item.info.ID.Kind == observe.Control {
				controls++
				continue
			}
			if item.info.Direction == observe.Sent {
				sent += uint64(len(item.bytes))
				framesSent++
			} else {
				received += uint64(len(item.bytes))
				framesReceived++
			}
		}
		if controls == 0 || framesSent+framesReceived == 0 {
			t.Fatal("bootstrap omitted observed control or data items")
		}
		stats := session.outcomes[0].Stats
		if stats.Elapsed <= 0 {
			t.Fatal("bootstrap elapsed omitted from observer statistics")
		}
		if stats.BytesSent != sent || stats.BytesReceived != received ||
			stats.FramesSent != framesSent ||
			stats.FramesReceived != framesReceived {
			t.Fatalf(
				"observed data %d/%d bytes %d/%d frames differ from stats %+v",
				sent,
				received,
				framesSent,
				framesReceived,
				stats,
			)
		}
		bootstrapStats = append(bootstrapStats, stats)
		session.mu.Unlock()
	}
	if a, b := bootstrapStats[0], bootstrapStats[1]; a.BytesSent != b.BytesReceived ||
		a.BytesReceived != b.BytesSent ||
		a.FramesSent != b.FramesReceived ||
		a.FramesReceived != b.FramesSent {
		t.Fatalf("paired bootstrap data accounting differs: %+v %+v", a, b)
	}
	left, right := gossipPair(t, source, live, near, far)
	results := []Gossiped{left, right}
	for index, recorder := range []*wireRecorder{recorderA, recorderB} {
		session := recorder.last(t)
		session.mu.Lock()
		if session.info.Kind != observe.Gossip || session.info.Protocol != observe.V2 ||
			session.elections != 0 ||
			len(session.outcomes) != 1 {
			t.Fatal("equal session lifecycle observation")
		}
		sent, received := 0, 0
		for _, item := range session.items {
			if item.info.ID.Kind != observe.Control {
				t.Fatal("equal session opened observed data")
			}
			if item.info.Direction == observe.Sent {
				sent++
			} else {
				received++
			}
		}
		if sent != 3 || received != 3 {
			t.Fatalf("equal control items: sent=%d received=%d", sent, received)
		}
		returned := results[index]
		if returned.Elapsed <= 0 || returned.Elapsed != returned.Stats.Elapsed ||
			observe.Stats(returned.Stats) != session.outcomes[0].Stats {
			t.Fatalf(
				"full-session statistics differ: elapsed=%v returned=%+v observed=%+v",
				returned.Elapsed,
				returned.Stats,
				session.outcomes[0].Stats,
			)
		}
		session.mu.Unlock()
	}
}

func TestRejectedPreambleIsNotObservedAsReceivedItem(t *testing.T) {
	recorder := &wireRecorder{}
	peer, err := Seed(BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.AttachObserver(recorder); err != nil {
		t.Fatal(err)
	}
	source, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	carrier := link.Manage(
		&departingLink{
			control: &departingControl{bytes.NewReader(make([]byte, 30))},
			done:    make(chan struct{}),
		},
	)
	defer func() { _ = carrier.Close() }()
	if _, err := source.GossipOnce(t.Context(), carrier); err == nil {
		t.Fatal("invalid preamble accepted")
	}
	session := recorder.last(t)
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, item := range session.items {
		if item.info.Direction == observe.Received {
			t.Fatal("rejected preamble emitted received callback")
		}
	}
	if len(session.outcomes) != 1 || session.outcomes[0].Kind != observe.Failed {
		t.Fatalf("failed session outcomes: %+v", session.outcomes)
	}
}

func TestContinuousWireObserverElapsedMatchesReturnedStatistics(t *testing.T) {
	source := seededBytes(t)
	peer := bootstrapBytes(t, source)
	recorder := &wireRecorder{}
	if err := peer.AttachObserver(recorder); err != nil {
		t.Fatal(err)
	}
	live, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	near, far := managedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := source.GossipOnce(ctx, far); served <- err }()
	yielded := false
	for result, err := range live.Gossip(ctx, near, GossipOptions[[]byte]{}) {
		if err != nil {
			t.Fatal(err)
		}
		yielded = true
		session := recorder.last(t)
		session.mu.Lock()
		if len(session.outcomes) != 1 {
			t.Fatalf("terminal observations: %+v", session.outcomes)
		}
		if result.Elapsed <= 0 || result.Elapsed != result.Stats.Elapsed ||
			observe.Stats(result.Stats) != session.outcomes[0].Stats {
			t.Fatalf(
				"continuous elapsed differs: elapsed=%v returned=%+v observed=%+v",
				result.Elapsed,
				result.Stats,
				session.outcomes[0].Stats,
			)
		}
		session.mu.Unlock()
		break
	}
	if !yielded {
		t.Fatal("continuous driver omitted completed result")
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
