package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link"
)

func boardNode(t *testing.T) *node {
	t.Helper()
	seed, err := hearsay.Seed(payloadCodec())
	if err != nil {
		t.Fatal(err)
	}
	replica, err := seed.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	self := record{
		Name:        "worker",
		Address:     "127.0.0.1:9000",
		Generation:  1,
		Incarnation: "self",
		Counter:     1,
	}
	if err := publish(replica, &self); err != nil {
		t.Fatal(err)
	}
	return &node{config: defaults(), replica: replica, self: self, members: make(map[string]member)}
}

func TestAutonomousBoardFlowAndPayloadCoexistence(t *testing.T) {
	c := testConfig()
	c.Neighbors = 1
	c.ProbeEvery = time.Hour
	c.JobDuration = 500 * time.Millisecond
	a := testNode(t, c, nil)
	c.Join = a.address
	b := testNode(t, c, nil)
	c.Join = b.address
	d := testNode(t, c, nil)
	eventually(t, "membership before card", func() bool { return allActive([]*node{a, b, d}) })
	value, err := a.addIssue("demo issue", "the human copies this text from GitHub")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "assigned card visible through an intermediate peer", func() bool {
		view, err := readBoard(d.replica)
		return err == nil && len(view.InProgress) == 1 && view.InProgress[0].Assignee != nil
	})
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(value.Title+"\n"+value.Body)))
	eventually(t, "all nodes see completion", func() bool {
		for _, n := range []*node{a, b, d} {
			view, err := readBoard(n.replica)
			if err != nil || len(view.Done) != 1 || view.Done[0].ID != value.ID ||
				view.Done[0].Result != checksum ||
				len(view.Todo)+len(view.InProgress) != 0 {
				return false
			}
		}
		return true
	})
	if !allActive([]*node{a, b, d}) {
		t.Fatal("card updates removed member records")
	}
	a.mu.Lock()
	_, direct := a.links[d.address]
	a.mu.Unlock()
	if direct {
		t.Fatal("chain end nodes formed a direct connection")
	}
}

func TestExecutionCancellation(t *testing.T) {
	n := boardNode(t)
	if _, err := n.addIssue("cancel this job", "body"); err != nil {
		t.Fatal(err)
	}
	if err := n.claim(time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := n.executeNext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution error = %v, want context.Canceled", err)
	}
	states, err := projectCards(n.replica.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Kind != "in_progress" {
		t.Fatalf("canceled board = %+v, want one in_progress", states)
	}
}

func TestExecutionSkipsLostClaim(t *testing.T) {
	n := boardNode(t)
	value, err := n.addIssue("lose this claim", "body")
	if err != nil {
		t.Fatal(err)
	}
	if err := n.claim(time.Now()); err != nil {
		t.Fatal(err)
	}
	states, err := projectCards(n.replica.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	prior := states[0]
	value.Assignee = &assignee{
		Name:        "other",
		Address:     "127.0.0.1:9001",
		Generation:  1,
		Incarnation: "other",
	}
	if err := replaceCard(n.replica, "in_progress", value, prior.Versions...); err != nil {
		t.Fatal(err)
	}
	if n.stillAssigned(prior) {
		t.Fatal("worker retained another worker's selected claim")
	}
	if err := n.executeNext(t.Context()); err != nil {
		t.Fatal(err)
	}
	view, err := readBoard(n.replica)
	if err != nil || len(view.Done) != 0 || len(view.InProgress) != 1 ||
		view.InProgress[0].Assignee.Name != "other" {
		t.Fatalf("board after lost claim = %+v, error=%v", view, err)
	}
}

func TestFailedWorkerRequeuesAndCompletes(t *testing.T) {
	n := boardNode(t)
	n.config.JobDuration = time.Millisecond
	owner := record{Address: "127.0.0.1:9001", Generation: 1, Incarnation: "failed", Left: true}
	value := card{
		ID:    "orphan",
		Title: "recover work",
		Assignee: &assignee{
			Address:     owner.Address,
			Generation:  owner.Generation,
			Incarnation: owner.Incarnation,
		},
	}
	n.members[owner.Address] = member{Record: owner, Seen: time.Now()}
	if err := replaceCard(n.replica, "in_progress", value); err != nil {
		t.Fatal(err)
	}
	if err := n.claim(time.Now()); err != nil {
		t.Fatal(err)
	}
	view, err := readBoard(n.replica)
	if err != nil || len(view.Todo) != 1 || view.Todo[0].Assignee != nil {
		t.Fatalf("requeued board = %+v, error=%v", view, err)
	}
	if err := n.claim(time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := n.executeNext(t.Context()); err != nil {
		t.Fatal(err)
	}
	view, err = readBoard(n.replica)
	if err != nil || len(view.Done) != 1 ||
		view.Done[0].Assignee.Incarnation != n.self.Incarnation {
		t.Fatalf("recovered board = %+v, error=%v", view, err)
	}
}

func TestAbandonmentRequiresCurrentIncarnationEvidence(t *testing.T) {
	n := boardNode(t)
	now := time.Now()
	owner := assignee{Address: "127.0.0.1:9001", Generation: 2, Incarnation: "new"}
	value := card{Assignee: &owner}
	if n.abandoned(value, now) {
		t.Fatal("unknown member considered abandoned")
	}
	for _, left := range []bool{false, true} {
		n.members[owner.Address] = member{
			Record: record{Generation: 1, Incarnation: "old", Left: left},
			Seen:   now.Add(-time.Hour),
		}
		if n.abandoned(value, now) {
			t.Errorf("older failed incarnation left=%t abandoned a newer assignment", left)
		}
	}
	n.members[owner.Address] = member{
		Record: record{Generation: owner.Generation, Incarnation: owner.Incarnation},
		Seen:   now.Add(-time.Hour),
	}
	if !n.abandoned(value, now) {
		t.Fatal("current unreachable owner not considered abandoned")
	}
}

func boardPeers(t *testing.T) (*node, *node, *link.Managed, *link.Managed) {
	t.Helper()
	a := boardNode(t)
	near, far := link.Memory()
	left, right := link.Manage(near), link.Manage(far)
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	served := make(chan error, 1)
	go func() { _, err := a.replica.GossipOnce(t.Context(), left); served <- err }()
	joined, err := hearsay.Bootstrap(payloadCodec()).Join(t.Context(), right)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	replica, err := joined.Peer.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	return a, &node{
		config:  defaults(),
		replica: replica,
		self: record{
			Name:        "other",
			Address:     "127.0.0.1:9001",
			Generation:  1,
			Incarnation: "other",
		},
		members: make(map[string]member),
	}, left, right
}

func syncBoard(t *testing.T, a, b *node, left, right *link.Managed) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	served := make(chan error, 1)
	go func() { _, err := a.replica.GossipOnce(ctx, left); served <- err }()
	_, err := b.replica.GossipOnce(ctx, right)
	if remote := <-served; err != nil || remote != nil {
		t.Fatalf("board sync errors = %v, %v", err, remote)
	}
}

func TestConcurrentClaimsConverge(t *testing.T) {
	a, b, left, right := boardPeers(t)
	if _, err := a.addIssue("shared task", "body"); err != nil {
		t.Fatal(err)
	}
	syncBoard(t, a, b, left, right)
	for _, n := range []*node{a, b} {
		if err := n.claim(time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	syncBoard(t, a, b, left, right)
	first, err := projectCards(a.replica.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	second, err := projectCards(b.replica.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 1 || !first[0].Version.Equal(second[0].Version) ||
		*first[0].Card.Assignee != *second[0].Card.Assignee {
		t.Fatalf("concurrent views differ: %+v versus %+v", first, second)
	}
	if len(first[0].Versions) != 2 {
		t.Fatalf("observed versions = %d, want both concurrent claims", len(first[0].Versions))
	}
}

func TestStaleTransitionPreservesUnobservedRemoteState(t *testing.T) {
	a, b, left, right := boardPeers(t)
	value, err := a.addIssue("race with gossip", "body")
	if err != nil {
		t.Fatal(err)
	}
	syncBoard(t, a, b, left, right)
	decision, err := projectCards(a.replica.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.claim(time.Now()); err != nil {
		t.Fatal(err)
	}
	remote, err := projectCards(b.replica.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	syncBoard(t, a, b, left, right)
	value.Assignee = &assignee{
		Name:        a.self.Name,
		Address:     a.self.Address,
		Generation:  a.self.Generation,
		Incarnation: a.self.Incarnation,
	}
	if err := replaceCard(a.replica, "in_progress", value, decision[0].Versions...); err != nil {
		t.Fatal(err)
	}
	if !a.replica.Snapshot().Contains(remote[0].Version) {
		t.Fatal("stale decision redacted a remote claim it had not evaluated")
	}
}
