package main

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link/routed"
)

func TestHeartbeatFreshness(t *testing.T) {
	start := time.Unix(100, 0)
	members := make(map[string]member)
	original := record{Address: "a", Generation: 1, Incarnation: "old", Counter: 1}
	observe(members, original, start)
	observe(members, original, start.Add(14*time.Second))
	if got := members["a"].status(
		start.Add(6*time.Second),
		5*time.Second,
		15*time.Second,
	); got != "suspected" {
		t.Fatalf("replayed heartbeat status = %s, want suspected", got)
	}
	if got := members["a"].status(
		start.Add(16*time.Second),
		5*time.Second,
		15*time.Second,
	); got != "unreachable" {
		t.Fatalf("timed out heartbeat status = %s, want unreachable", got)
	}
	newer := original
	newer.Counter++
	observe(members, newer, start.Add(17*time.Second))
	if got := members["a"].status(
		start.Add(18*time.Second),
		5*time.Second,
		15*time.Second,
	); got != "active" {
		t.Fatalf("new heartbeat status = %s, want active", got)
	}
	departure := newer
	departure.Counter++
	departure.Left = true
	observe(members, departure, start.Add(19*time.Second))
	newer.Counter = 100
	observe(members, newer, start.Add(20*time.Second))
	if !members["a"].Record.Left {
		t.Fatal("old incarnation revived after departure")
	}
	restarted := record{Address: "a", Generation: 2, Incarnation: "new", Counter: 1}
	observe(members, restarted, start.Add(21*time.Second))
	observe(members, departure, start.Add(22*time.Second))
	if got := members["a"].Record; got != restarted {
		t.Fatalf("restart record = %+v, want %+v", got, restarted)
	}
}

func TestPublishBoundsRecordsAndRestart(t *testing.T) {
	replica := testReplica(t, hearsay.JSONCodec[envelope]())
	self := newRecord(replica, "127.0.0.1:9000")
	for range 100 {
		self.Counter++
		if err := publish(replica, &self); err != nil {
			t.Fatal(err)
		}
	}
	restarted := newRecord(replica, self.Address)
	if restarted.Generation != self.Generation+1 || restarted.Incarnation == self.Incarnation {
		t.Fatalf("restart = %+v, prior = %+v", restarted, self)
	}
	restarted.Counter = 1
	if err := publish(replica, &restarted); err != nil {
		t.Fatal(err)
	}
	if got := replica.Snapshot().Len(); got != 1 {
		t.Fatalf("live records = %d, want 1", got)
	}
}

func testConfig() config {
	c := defaults()
	c.Listen = "127.0.0.1:0"
	c.HTTP = ""
	c.Heartbeat = 40 * time.Millisecond
	c.SuspectAfter = 300 * time.Millisecond
	c.UnreachableAfter = 700 * time.Millisecond
	c.LeaveTimeout = 200 * time.Millisecond
	c.RetryMin = 30 * time.Millisecond
	c.RetryMax = 150 * time.Millisecond
	c.ProbeEvery = 150 * time.Millisecond
	c.IOTimeout = time.Second
	return c
}

func testNode(t *testing.T, c config, dial routed.Dial) *node {
	t.Helper()
	n, err := startNode(t.Context(), c, io.Discard, dial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.close)
	return n
}

func eventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out: %s", description)
		case <-tick.C:
		}
	}
}

func hasStatus(n *node, address, status string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	m, ok := n.members[address]
	return ok && m.status(time.Now(), n.config.SuspectAfter, n.config.UnreachableAfter) == status
}

func TestMultiHopJoinLeaveCrashAndRestart(t *testing.T) {
	c := testConfig()
	c.Neighbors = 1
	c.ProbeEvery = time.Hour
	a := testNode(t, c, nil)
	c.Join = a.address
	b := testNode(t, c, nil)
	c.Join = b.address
	d := testNode(t, c, nil)
	eventually(
		t,
		"multi-hop membership",
		func() bool { return hasStatus(a, d.address, "active") && hasStatus(d, a.address, "active") },
	)
	a.mu.Lock()
	_, direct := a.links[d.address]
	a.mu.Unlock()
	if direct {
		t.Fatal("chain end nodes retained a direct connection")
	}
	shutdown, cancel := context.WithTimeout(t.Context(), c.LeaveTimeout)
	if err := d.leave(shutdown); err != nil {
		t.Fatal(err)
	}
	cancel()
	eventually(t, "multi-hop departure", func() bool { return hasStatus(a, d.address, "left") })
	// closing b without leave models an unexpected process exit.
	b.close()
	eventually(t, "crash detection", func() bool { return hasStatus(a, b.address, "unreachable") })
	c.Listen = b.address
	c.Join = a.address
	restarted := testNode(t, c, nil)
	eventually(t, "restart recovery", func() bool { return hasStatus(a, b.address, "active") })
	a.mu.Lock()
	got := a.members[b.address].Record
	a.mu.Unlock()
	restarted.mu.Lock()
	want := restarted.self
	restarted.mu.Unlock()
	if got.Incarnation != want.Incarnation || got.Generation <= b.self.Generation {
		t.Fatalf("restart view = %+v, restarted self = %+v", got, want)
	}
}

func TestRetryBounds(t *testing.T) {
	for failures := range 20 {
		for range 20 {
			delay := retryDelay(failures, time.Second, 10*time.Second)
			if delay < time.Second || delay > 10*time.Second {
				t.Fatalf("retry delay(%d) = %s", failures, delay)
			}
		}
	}
}

func TestCandidateSelection(t *testing.T) {
	now := time.Now()
	n := &node{
		config:  testConfig(),
		address: "self",
		members: map[string]member{
			"self":    {Record: record{Address: "self"}},
			"dead":    {Record: record{Address: "dead"}},
			"left":    {Record: record{Left: true}},
			"backoff": {Record: record{}},
		},
		links:   make(map[string]*connection),
		dialing: make(map[string]bool),
		retries: map[string]retry{"backoff": {after: now.Add(time.Hour)}},
	}
	address, probe := n.candidate(now)
	if address != "dead" || probe {
		t.Fatalf("underfull candidate = %q, probe=%t, want dead/false", address, probe)
	}
	delete(n.dialing, "dead")
	n.config.Neighbors = 1
	n.links["neighbor"] = &connection{}
	address, probe = n.candidate(now)
	if address != "dead" || !probe {
		t.Fatalf("at-target candidate = %q, probe=%t, want dead/true", address, probe)
	}
	delete(n.dialing, "dead")
	if address, _ := n.candidate(now.Add(time.Millisecond)); address != "" {
		t.Fatalf("probe ignored interval: %q", address)
	}
}

// firewall drops new TCP attempts; closing routed endpoints' known links below
// also removes pooled streams, so the partition covers both transport paths.
type firewall struct {
	mu      sync.Mutex
	blocked map[string]bool
}

func (f *firewall) dial(ctx context.Context, address []byte) (net.Conn, error) {
	f.mu.Lock()
	blocked := f.blocked[string(address)]
	f.mu.Unlock()
	if blocked {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: net.ErrClosed}
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", string(address))
}

func allActive(nodes []*node) bool {
	for _, n := range nodes {
		for _, other := range nodes {
			if !hasStatus(n, other.address, "active") {
				return false
			}
		}
	}
	return true
}

func degreeWithin(nodes []*node, minimum, maximum int) bool {
	for _, n := range nodes {
		n.mu.Lock()
		count := len(n.links)
		n.mu.Unlock()
		if count < minimum || count > maximum {
			return false
		}
	}
	return true
}

func partition(nodes []*node, firewalls []*firewall) {
	for i, n := range nodes {
		firewalls[i].mu.Lock()
		for j, other := range nodes {
			firewalls[i].blocked[other.address] = (i < 2) != (j < 2)
		}
		firewalls[i].mu.Unlock()
		disconnectOtherComponent(n, nodes, i)
	}
}

func disconnectOtherComponent(n *node, nodes []*node, index int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for address, connection := range n.links {
		j := slices.IndexFunc(nodes, func(other *node) bool { return other.address == address })
		if (index < 2) != (j < 2) {
			_ = connection.managed.Close()
			delete(n.links, address)
		}
	}
}

func TestSparsePartitionRecoveryAtTarget(t *testing.T) {
	c := testConfig()
	c.Neighbors = 1
	firewalls := make([]*firewall, 4)
	nodes := make([]*node, 4)
	for i := range nodes {
		firewalls[i] = &firewall{blocked: make(map[string]bool)}
		if i > 0 {
			c.Join = nodes[i-1].address
		}
		nodes[i] = testNode(t, c, firewalls[i].dial)
	}
	eventually(t, "initial convergence", func() bool { return allActive(nodes) })
	partition(nodes, firewalls)
	eventually(t, "partition-local timeout", func() bool {
		return hasStatus(nodes[0], nodes[3].address, "unreachable") &&
			hasStatus(nodes[3], nodes[0].address, "unreachable")
	})
	eventually(
		t,
		"each component has a retained local neighbor",
		func() bool { return degreeWithin(nodes, 1, 2) },
	)
	for _, f := range firewalls {
		f.mu.Lock()
		clear(f.blocked)
		f.mu.Unlock()
	}
	var stableSince time.Time
	eventually(t, "at-target recovery remains active beyond the failure timeout", func() bool {
		if !allActive(nodes) {
			stableSince = time.Time{}
			return false
		}
		if stableSince.IsZero() {
			stableSince = time.Now()
		}
		return time.Since(stableSince) > c.UnreachableAfter*2
	})
}

func TestSeedAdmissionRemainsSparse(t *testing.T) {
	c := testConfig()
	c.Neighbors = 2
	seed := testNode(t, c, nil)
	nodes := []*node{seed}
	c.Join = seed.address
	for range 6 {
		nodes = append(nodes, testNode(t, c, nil))
	}
	eventually(t, "all members visible", func() bool { return allActive(nodes) })
	eventually(t, "seed star trimmed", func() bool { return degreeWithin(nodes, 1, c.Neighbors+1) })
	for _, n := range nodes {
		if got := n.replica.Snapshot().Len(); got > len(nodes)*2 {
			t.Fatalf("replicated records = %d, members = %d", got, len(nodes))
		}
	}
}

func TestPayloadValidation(t *testing.T) {
	codec := payloadCodec()
	for _, invalid := range []envelope{{Kind: "unknown"}, {Kind: "todo", Card: card{Title: "missing id"}}, {Kind: "member", Member: record{Address: "invalid"}}} {
		if _, err := codec.Encode(invalid); err == nil {
			t.Errorf("Encode(%+v) accepted invalid payload", invalid)
		}
	}
}

func TestHeartbeatPreservesCard(t *testing.T) {
	codec := payloadCodec()
	replica := testReplica(t, codec)
	task := envelope{Kind: "todo", Card: card{ID: "task-1", Title: "preserve this card"}}
	version, err := replica.Send(task)
	if err != nil {
		t.Fatal(err)
	}
	self := newRecord(replica, "127.0.0.1:9000")
	for range 20 {
		self.Counter++
		if err := publish(replica, &self); err != nil {
			t.Fatal(err)
		}
	}
	if !replica.Snapshot().Contains(version) {
		t.Fatal("member heartbeat redacted a card")
	}
	if got := replica.Snapshot().Len(); got != 2 {
		t.Fatalf("records = %d, want member + card", got)
	}
}

func TestSimultaneousDialsDeduplicate(t *testing.T) {
	c := testConfig()
	c.Neighbors = 1
	c.ProbeEvery = time.Hour
	a := testNode(t, c, nil)
	c.Join = a.address
	b := testNode(t, c, nil)
	for _, pair := range [][2]*node{{a, b}, {b, a}} {
		n, other := pair[0], pair[1]
		n.mu.Lock()
		n.dialing[other.address] = true
		if connection := n.links[other.address]; connection != nil {
			_ = connection.managed.Close()
			delete(n.links, other.address)
		}
		n.mu.Unlock()
	}
	var workers sync.WaitGroup
	for _, pair := range [][2]*node{{a, b}, {b, a}} {
		n, other := pair[0], pair[1]
		workers.Go(func() {
			managed, err := n.open(t.Context(), other.address, hello{})
			if err != nil {
				t.Errorf("crossed dial to %s: %v", other.address, err)
				return
			}
			n.launch(t.Context(), other.address, managed, true, false)
		})
	}
	workers.Wait()
	eventually(t, "crossed dials choose one canonical connection", func() bool {
		return canonicalLink(a, b) && canonicalLink(b, a)
	})
}

func canonicalLink(n, other *node) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	c := n.links[other.address]
	return len(n.links) == 1 && c != nil && c.outgoing == (n.address < other.address) &&
		!c.converged.IsEmpty()
}

func TestRestartThroughStalePeerRefutesPriorIncarnation(t *testing.T) {
	replica := testReplica(t, payloadCodec())
	address := "127.0.0.1:9000"
	prior := record{Address: address, Generation: 2, Incarnation: "z-prior", Counter: 100}
	observer := make(map[string]member)
	observe(observer, prior, time.Now())
	// the restarted writer chose generation 2 via a stale generation-1 peer.
	restarted := record{Address: address, Generation: 2, Incarnation: "a-restarted", Counter: 1}
	if _, err := replica.Send(envelope{Kind: "member", Member: prior}); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Send(envelope{Kind: "member", Member: restarted}); err != nil {
		t.Fatal(err)
	}
	restarted.Counter++
	if err := publish(replica, &restarted); err != nil {
		t.Fatal(err)
	}
	observe(observer, restarted, time.Now())
	observe(observer, prior, time.Now())
	if got := observer[address].Record; got.Incarnation != restarted.Incarnation ||
		got.Generation <= prior.Generation {
		t.Fatalf("post-repair observer = %+v, prior = %+v", got, prior)
	}
	if got := replica.Snapshot().Len(); got != 1 {
		t.Fatalf("restart records = %d, want 1", got)
	}
}

func testReplica(t *testing.T, codec hearsay.Codec[envelope]) *hearsay.Replica[envelope] {
	t.Helper()
	seed, err := hearsay.Seed(codec)
	if err != nil {
		t.Fatal(err)
	}
	replica, err := seed.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close() })
	return replica
}

func TestLatePriorIncarnationSurvivesUntilRebase(t *testing.T) {
	codec := payloadCodec()
	encode := codec.Encode
	var inject func()
	codec.Encode = func(e envelope) ([]byte, error) {
		if inject != nil {
			f := inject
			inject = nil
			f()
		}
		return encode(e)
	}
	replica := testReplica(t, codec)
	prior := record{Address: "127.0.0.1:9000", Generation: 2, Incarnation: "z-prior", Counter: 100}
	restarted := record{
		Address:     prior.Address,
		Generation:  2,
		Incarnation: "a-restarted",
		Counter:     1,
	}
	if err := publish(replica, &restarted); err != nil {
		t.Fatal(err)
	}
	inject = func() {
		if _, err := replica.Send(envelope{Kind: "member", Member: prior}); err != nil {
			t.Fatal(err)
		}
	}
	restarted.Counter++
	if err := publish(replica, &restarted); err != nil {
		t.Fatal(err)
	}
	if got := replica.Snapshot().Len(); got != 2 {
		t.Fatalf("late competing record was lost: records = %d, want 2", got)
	}
	restarted.Counter++
	if err := publish(replica, &restarted); err != nil {
		t.Fatal(err)
	}
	if restarted.Generation <= prior.Generation {
		t.Fatalf("generation = %d, want > %d", restarted.Generation, prior.Generation)
	}
	if got := replica.Snapshot().Len(); got != 1 {
		t.Fatalf("post-rebase records = %d, want 1", got)
	}
}

func TestCanonicalJoin(t *testing.T) {
	got, err := canonicalJoin("[0:0:0:0:0:0:0:1]:9000")
	if err != nil || got != "[::1]:9000" {
		t.Fatalf("canonical IPv6 join = %q, %v", got, err)
	}
	for _, address := range []string{"0.0.0.0:9000", "127.0.0.1:0", "hostname:9000"} {
		if _, err := canonicalJoin(address); err == nil {
			t.Errorf("canonicalJoin(%q) accepted invalid address", address)
		}
	}
}

func TestHTTPStartupFailureClosesBootstrapLink(t *testing.T) {
	seed := testNode(t, testConfig(), nil)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	c := testConfig()
	c.Join = seed.address
	c.HTTP = occupied.Addr().String()
	joined, err := startNode(t.Context(), c, io.Discard, nil)
	if err == nil {
		joined.close()
		t.Fatal("HTTP bind unexpectedly succeeded on occupied address")
	}
	eventually(
		t,
		"failed startup closes initial bootstrap connection",
		func() bool { return degreeWithin([]*node{seed}, 0, 0) },
	)
}
