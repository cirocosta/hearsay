package main

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cirocosta/hearsay"
)

type assignee struct {
	Name        string `json:"name"`
	Address     string `json:"address"`
	Generation  uint64 `json:"generation"`
	Incarnation string `json:"incarnation"`
}

type card struct {
	ID       string    `json:"id"`
	Title    string    `json:"title"`
	Body     string    `json:"body"`
	Assignee *assignee `json:"assignee,omitzero"`
	Result   string    `json:"result,omitzero"`
}

func (c card) validate(kind string) error {
	if c.ID == "" || c.Title == "" {
		return errors.New("card requires id and title")
	}
	if kind == "todo" {
		if c.Assignee != nil || c.Result != "" {
			return errors.New("todo card cannot have assignee or result")
		}
		return nil
	}
	if c.Assignee == nil || c.Assignee.Address == "" || c.Assignee.Incarnation == "" ||
		c.Assignee.Generation == 0 {
		return errors.New("assigned card requires worker address, generation and incarnation")
	}
	if kind == "done" && c.Result == "" {
		return errors.New("done card requires result")
	}
	return nil
}

type cardState struct {
	Kind     string
	Card     card
	Version  hearsay.Version
	Versions []hearsay.Version
}

type board struct {
	Todo       []card `json:"todo"`
	InProgress []card `json:"in_progress"`
	Done       []card `json:"done"`
}

// projectCards selects one deterministic view without discarding unseen updates.
// ranked order places a later causal update after the versions it replaces.
func projectCards(snapshot hearsay.Snapshot[envelope]) ([]cardState, error) {
	selected := make(map[string]cardState)
	for message := range snapshot.All() {
		if message.Err != nil {
			return nil, fmt.Errorf("read board: %w", message.Err)
		}
		if message.Payload.Kind == "member" {
			continue
		}
		value := message.Payload
		previous, exists := selected[value.Card.ID]
		observed := append(previous.Versions, message.Version)
		if !exists || message.Version.RankedCompare(previous.Version) > 0 {
			previous = cardState{Kind: value.Kind, Card: value.Card, Version: message.Version}
		}
		previous.Versions = observed
		selected[value.Card.ID] = previous
	}
	states := make([]cardState, 0, len(selected))
	for _, state := range selected {
		states = append(states, state)
	}
	slices.SortFunc(states, func(a, b cardState) int { return cmp.Compare(a.Card.ID, b.Card.ID) })
	return states, nil
}

func readBoard(replica *hearsay.Replica[envelope]) (board, error) {
	view := board{Todo: []card{}, InProgress: []card{}, Done: []card{}}
	states, err := projectCards(replica.Snapshot())
	if err != nil {
		return board{}, err
	}
	for _, state := range states {
		switch state.Kind {
		case "todo":
			view.Todo = append(view.Todo, state.Card)
		case "in_progress":
			view.InProgress = append(view.InProgress, state.Card)
		case "done":
			view.Done = append(view.Done, state.Card)
		}
	}
	return view, nil
}

// replaceCard redacts only observed versions of this card, never member records.
// a concurrent remote claim may survive; projectCards resolves its visible value.
func replaceCard(
	replica *hearsay.Replica[envelope],
	kind string,
	value card,
	observed ...hearsay.Version,
) error {
	return replica.Batch(func(batch *hearsay.Batch[envelope]) error {
		if err := batch.RedactAll(observed); err != nil {
			return err
		}
		return batch.Send(envelope{Kind: kind, Card: value})
	})
}

func (n *node) startBoard(ctx context.Context) {
	n.appWorkers.Go(func() { n.claimLoop(ctx) })
	n.appWorkers.Go(func() { n.executeLoop(ctx) })
}

func (n *node) claimLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n.mu.Lock()
			if err := n.claim(now); err != nil {
				n.failure = err
			}
			n.mu.Unlock()
		}
	}
}

// claim runs under n.mu, serializing local HTTP writes and both worker loops.
// gossip offers eventual claims, not exclusive ownership or exactly-once work.
func (n *node) claim(now time.Time) error {
	if n.leaving {
		return nil
	}
	states, err := projectCards(n.replica.Snapshot())
	if err != nil {
		return err
	}
	if _, assigned := n.assignedCard(states); assigned {
		return nil
	}
	for _, state := range states {
		if state.Kind == "in_progress" && n.abandoned(state.Card, now) {
			value := state.Card
			value.Assignee = nil
			value.Result = ""
			return replaceCard(n.replica, "todo", value, state.Versions...)
		}
		if state.Kind == "todo" {
			value := state.Card
			value.Assignee = &assignee{
				Name:        n.self.Name,
				Address:     n.self.Address,
				Generation:  n.self.Generation,
				Incarnation: n.self.Incarnation,
			}
			return replaceCard(n.replica, "in_progress", value, state.Versions...)
		}
	}
	return nil
}

func (n *node) owns(value card) bool {
	owner := value.Assignee
	return owner != nil && owner.Address == n.self.Address &&
		owner.Generation == n.self.Generation &&
		owner.Incarnation == n.self.Incarnation
}

// missing membership is not failure evidence: the record may still be in flight.
func (n *node) abandoned(value card, now time.Time) bool {
	owner := value.Assignee
	if owner == nil {
		return false
	}
	m, known := n.members[owner.Address]
	if !known {
		return false
	}
	assigned := record{Generation: owner.Generation, Incarnation: owner.Incarnation}
	if order := compareIncarnation(m.Record, assigned); order != 0 {
		return order > 0
	}
	return m.Record.Left ||
		m.status(now, n.config.SuspectAfter, n.config.UnreachableAfter) == "unreachable"
}

func (n *node) nextAssigned() (cardState, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	states, err := projectCards(n.replica.Snapshot())
	if err != nil {
		return cardState{}, false, err
	}
	state, assigned := n.assignedCard(states)
	return state, assigned && !n.leaving, nil
}

func (n *node) assignedCard(states []cardState) (cardState, bool) {
	for _, state := range states {
		if state.Kind == "in_progress" && n.owns(state.Card) {
			return state, true
		}
	}
	return cardState{}, false
}

func (n *node) executeLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := n.executeNext(ctx); err != nil && ctx.Err() == nil {
				n.mu.Lock()
				n.failure = err
				n.mu.Unlock()
			}
		}
	}
}

func (n *node) executeNext(ctx context.Context) error {
	state, found, err := n.nextAssigned()
	if err != nil || !found {
		return err
	}
	// this pause lets competing claims arrive; it is not a consensus barrier.
	if err := waitFor(ctx, 200*time.Millisecond); err != nil {
		return err
	}
	if !n.stillAssigned(state) {
		return nil
	}
	result, err := demoWork(ctx, state.Card, n.config.JobDuration)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	current, assigned := n.currentAssignmentLocked(state)
	if ctx.Err() != nil || !assigned {
		return nil
	}
	value := state.Card
	value.Result = result
	return replaceCard(n.replica, "done", value, current.Versions...)
}

func (n *node) stillAssigned(state cardState) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, assigned := n.currentAssignmentLocked(state)
	return assigned
}

func (n *node) currentAssignmentLocked(state cardState) (cardState, bool) {
	states, err := projectCards(n.replica.Snapshot())
	if err != nil {
		n.failure = err
		return cardState{}, false
	}
	for _, current := range states {
		if current.Card.ID == state.Card.ID {
			return current, !n.leaving && current.Kind == "in_progress" && n.owns(current.Card) &&
				current.Version.Equal(state.Version)
		}
	}
	return cardState{}, false
}

// demoWork is deliberately idempotent: partitions can cause duplicate execution.
// a future real worker can replace this function without changing board gossip.
func demoWork(ctx context.Context, value card, duration time.Duration) (string, error) {
	if err := waitFor(ctx, duration); err != nil {
		return "", err
	}
	checksum := sha256.Sum256([]byte(value.Title + "\n" + value.Body))
	return fmt.Sprintf("%x", checksum), nil
}

func waitFor(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (n *node) addIssue(title, body string) (card, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaving {
		return card{}, hearsay.ErrClosed
	}
	value := card{ID: rand.Text(), Title: title, Body: body}
	if err := replaceCard(n.replica, "todo", value); err != nil {
		return card{}, fmt.Errorf("submit issue: %w", err)
	}
	return value, nil
}
