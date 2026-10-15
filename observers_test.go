package hearsay

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/tree"
)

type messageObserver interface {
	TryNext() (Message[[]byte], PollState)
	Next(context.Context) (Message[[]byte], bool, error)
	Checkpoint() Version
}

type observerFactory struct {
	name string
	make func(*Rumors[[]byte], Version) messageObserver
}

var observerFactories = []observerFactory{
	{
		"unordered",
		func(r *Rumors[[]byte], since Version) messageObserver { return r.UnorderedMessagesSince(since) },
	},
	{
		"causal",
		func(r *Rumors[[]byte], since Version) messageObserver { return r.CausalMessagesSince(since) },
	},
}

func observerSend(t *testing.T, r *Rumors[[]byte], value string) Version {
	t.Helper()
	v, err := r.Send([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func drainObserver(t *testing.T, observer messageObserver) ([]Message[[]byte], PollState) {
	t.Helper()
	var out []Message[[]byte]
	for {
		m, state := observer.TryNext()
		if state != Ready {
			return out, state
		}
		if m.Err != nil {
			t.Fatal(m.Err)
		}
		out = append(out, m)
	}
}

func TestObserverCheckpointCompletedPass(t *testing.T) {
	for _, factory := range observerFactories {
		t.Run(factory.name, func(t *testing.T) {
			r := seededBytes(t)
			for i := 0; i < 3; i++ {
				observerSend(t, r, fmt.Sprint(i))
			}
			observer := factory.make(r, Version{})
			for i := 0; i < 3; i++ {
				m, state := observer.TryNext()
				if state != Ready || m.Err != nil {
					t.Fatalf("message %d: %v %v", i, state, m.Err)
				}
				if !observer.Checkpoint().IsEmpty() {
					t.Fatal("checkpoint covered a partially delivered pass")
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, _, err := observer.Next(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if !observer.Checkpoint().IsEmpty() {
				t.Fatal("cancelled call changed checkpoint")
			}
			replayed, state := drainObserver(t, factory.make(r, observer.Checkpoint()))
			if len(replayed) != 3 || state != Quiet {
				t.Fatal("interrupted pass did not replay safely")
			}
			if _, state := observer.TryNext(); state != Quiet {
				t.Fatal(state)
			}
			if !observer.Checkpoint().Equal(r.Snapshot().Latest()) {
				t.Fatal("completed pass did not advance checkpoint")
			}
			resumed := factory.make(r, observer.Checkpoint())
			if messages, state := drainObserver(t, resumed); len(messages) != 0 || state != Quiet {
				t.Fatal("completed checkpoint replayed covered messages")
			}
			observerSend(t, r, "new")
			if messages, _ := drainObserver(
				t,
				resumed,
			); len(messages) != 1 ||
				string(messages[0].Payload) != "new" {
				t.Fatal("resume skipped new message")
			}
		})
	}
}

func TestObserverCapturedPassSurvivesRedaction(t *testing.T) {
	for _, factory := range observerFactories {
		t.Run(factory.name, func(t *testing.T) {
			r := seededBytes(t)
			versions := []Version{
				observerSend(t, r, "a"),
				observerSend(t, r, "b"),
				observerSend(t, r, "c"),
			}
			observer := factory.make(r, Version{})
			if _, state := observer.TryNext(); state != Ready {
				t.Fatal(state)
			}
			if err := r.RedactAll(versions); err != nil {
				t.Fatal(err)
			}
			messages, state := drainObserver(t, observer)
			if len(messages) != 2 || state != Quiet {
				t.Fatalf("captured pass: %d remaining, %v", len(messages), state)
			}
			if !observer.Checkpoint().Equal(r.Snapshot().Latest()) {
				t.Fatal("history-only redaction frontier was not checkpointed")
			}
			if messages, state := drainObserver(
				t,
				factory.make(r, Version{}),
			); len(messages) != 0 ||
				state != Quiet {
				t.Fatal("redacted content produced an event")
			}
		})
	}
}

func TestObserverOwnershipRoundTripAndFinalDrain(t *testing.T) {
	for _, factory := range observerFactories {
		t.Run(factory.name, func(t *testing.T) {
			r := seededBytes(t)
			observer := factory.make(r, Version{})
			peer, err := r.TryIntoPeer()
			if err != nil {
				t.Fatal("observer retained identity ownership:", err)
			}
			if _, state := observer.TryNext(); state != Quiet {
				t.Fatal("recovered peer did not keep observer alive:", state)
			}
			live, err := peer.IntoRumors()
			if err != nil {
				t.Fatal(err)
			}
			observerSend(t, live, "after recovery")
			last := live.Snapshot().Latest()
			if err := live.Close(); err != nil {
				t.Fatal(err)
			}
			messages, state := drainObserver(t, observer)
			if len(messages) != 1 || state != Ended ||
				string(messages[0].Payload) != "after recovery" {
				t.Fatalf("final drain: %v %v", messages, state)
			}
			if !observer.Checkpoint().Equal(last) {
				t.Fatal("final checkpoint was not complete")
			}
			if _, state := observer.TryNext(); state != Ended {
				t.Fatal("ended was not terminal")
			}
		})
	}
}

func TestObserverBlockingCancellationAndWakeup(t *testing.T) {
	for _, factory := range observerFactories {
		t.Run(factory.name, func(t *testing.T) {
			r := seededBytes(t)
			observer := factory.make(r, Version{})
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { _, _, err := observer.Next(ctx); result <- err }()
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled observation did not return")
			}
			go func() {
				m, ok, err := observer.Next(context.Background())
				if err == nil && (!ok || string(m.Payload) != "wake") {
					err = fmt.Errorf("unexpected wake result: %q %t", m.Payload, ok)
				}
				result <- err
			}()
			observerSend(t, r, "wake")
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("publication wakeup was lost")
			}
		})
	}
}

func TestObserverConcurrentProducerFinalDrain(t *testing.T) {
	for _, factory := range observerFactories {
		t.Run(factory.name, func(t *testing.T) {
			r := seededBytes(t)
			observer := factory.make(r, Version{})
			producer, err := r.Clone()
			if err != nil {
				t.Fatal(err)
			}
			_ = r.Close()
			producerErr := make(chan error, 1)
			go func() {
				defer func() { _ = producer.Close() }()
				for i := 0; i < 160; i++ {
					if _, err := producer.Send([]byte(fmt.Sprint(i))); err != nil {
						producerErr <- err
						return
					}
				}
				producerErr <- nil
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			seen := make(map[string]bool)
			for {
				m, ok, err := observer.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				key := string(m.Payload)
				if m.Err != nil || seen[key] {
					t.Fatal("decode failure or duplicate:", key, m.Err)
				}
				seen[key] = true
			}
			if err := <-producerErr; err != nil {
				t.Fatal(err)
			}
			if len(seen) != 160 {
				t.Fatalf("delivered %d, want 160", len(seen))
			}
		})
	}
}

func TestCausalObserverConcurrentVersionsAndPortableCheckpoint(t *testing.T) {
	party := before.SeedParty()
	other := party.Fork()
	a := Version{}.Tick(party)
	b := Version{}.Tick(other)
	later := a.Join(b).Tick(party)
	r := seededBytes(t)
	r.state.mu.Lock()
	var content tree.Tree[[]byte]
	for _, item := range []struct {
		version Version
		value   string
	}{{later, "later"}, {b, "b"}, {a, "a"}} {
		payload, err := r.state.codec.encode([]byte(item.value))
		if err != nil {
			t.Fatal(err)
		}
		content = content.Insert(item.version, payload)
	}
	r.state.publishLocked(content)
	r.state.mu.Unlock()
	observer := r.CausalMessages()
	messages, state := drainObserver(t, observer)
	if len(messages) != 3 || state != Quiet || string(messages[2].Payload) != "later" {
		t.Fatal("causal effects preceded their causes")
	}
	for i, m := range messages {
		for _, earlier := range messages[:i] {
			if m.Version.Compare(earlier.Version) == before.Before {
				t.Fatal("causal order reversed")
			}
		}
	}
	// A checkpoint is a causal frontier and does not depend on trie address order.
	for _, factory := range observerFactories {
		if replay, _ := drainObserver(t, factory.make(r, observer.Checkpoint())); len(replay) != 0 {
			t.Fatal("causal checkpoint did not port to another observer")
		}
	}
}

func TestChangesCoalescingNoopsAndFinalDrain(t *testing.T) {
	r := seededBytes(t)
	changes := r.Changes()
	if changes.TryNext() != Ready || changes.TryNext() != Quiet {
		t.Fatal("initial change signal must be immediate exactly once")
	}
	v := observerSend(t, r, "removed")
	if err := r.Redact(v); err != nil {
		t.Fatal(err)
	}
	if changes.TryNext() != Ready || changes.TryNext() != Quiet {
		t.Fatal("history-only changes did not coalesce")
	}
	_ = r.Redact(v)
	_ = r.SendAll(nil)
	if changes.TryNext() != Quiet {
		t.Fatal("no-op mutation emitted a change")
	}
	peer, err := r.TryIntoPeer()
	if err != nil {
		t.Fatal(err)
	}
	if changes.TryNext() != Quiet {
		t.Fatal("ownership recovery ended Changes")
	}
	live, err := peer.IntoRumors()
	if err != nil {
		t.Fatal(err)
	}
	observerSend(t, live, "final")
	_ = live.Close()
	if changes.TryNext() != Ready || changes.TryNext() != Ended {
		t.Fatal("Changes did not report final state then end")
	}
}

func TestChangesCancellationThenClose(t *testing.T) {
	r := seededBytes(t)
	changes := r.Changes()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := changes.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ok, err := changes.Next(context.Background()); !ok || err != nil {
		t.Fatal("cancellation consumed initial change")
	}
	result := make(chan error, 1)
	go func() {
		ok, err := changes.Next(context.Background())
		if err == nil && ok {
			err = errors.New("closed Changes yielded another signal")
		}
		result <- err
	}()
	_ = r.Close()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closure failed to wake Changes")
	}
}

func TestObserverCheckpointNeverSkipsUndeliveredLiveMessage(t *testing.T) {
	for _, factory := range observerFactories {
		t.Run(factory.name, func(t *testing.T) {
			r := seededBytes(t)
			observer := factory.make(r, Version{})
			delivered := make(map[string]bool)
			var versions []Version
			for step := 0; step < 180; step++ {
				versions = append(versions, observerSend(t, r, fmt.Sprint(step)))
				if step%4 == 0 {
					_ = r.Redact(versions[step/2])
				}
				for poll := 0; poll < step%5; poll++ {
					m, state := observer.TryNext()
					if state == Ready {
						if m.Err != nil {
							t.Fatal(m.Err)
						}
						delivered[string(m.Payload)] = true
					}
				}
				checkpoint := observer.Checkpoint()
				for m := range r.Snapshot().All() {
					if m.Version.LessEqual(checkpoint) && !delivered[string(m.Payload)] {
						t.Fatalf("step %d: checkpoint skipped live message %q", step, m.Payload)
					}
				}
				if step%17 == 0 {
					// Simulate abandoning a partly delivered pass and restarting.
					observer = factory.make(r, checkpoint)
				}
			}
			_ = r.Close()
			messages, state := drainObserver(t, observer)
			if state != Ended {
				t.Fatal(state)
			}
			for _, m := range messages {
				delivered[string(m.Payload)] = true
			}
			for m := range r.Snapshot().All() {
				if !delivered[string(m.Payload)] {
					t.Fatal("final drain skipped", string(m.Payload))
				}
			}
		})
	}
}
