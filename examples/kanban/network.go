package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/link/routed"
)

type connection struct {
	managed   *link.Managed
	outgoing  bool
	started   time.Time
	converged hearsay.Version
}

type retry struct {
	failures int
	after    time.Time
}

type node struct {
	config     config
	address    string
	endpoint   *routed.Endpoint
	replica    *hearsay.Replica[envelope]
	cancel     context.CancelFunc
	workers    sync.WaitGroup
	appCancel  context.CancelFunc
	appWorkers sync.WaitGroup
	slots      chan struct{}
	mu         sync.Mutex
	self       record
	leaving    bool
	members    map[string]member
	links      map[string]*connection
	dialing    map[string]bool
	retries    map[string]retry
	lastProbe  time.Time
	output     io.Writer
	rendered   string
	failure    error
}

// hello is example-owned admission metadata, before the Hearsay wire protocol.
type hello struct {
	Probe bool
}

func startNode(ctx context.Context, c config, output io.Writer, dial routed.Dial) (*node, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	join, err := canonicalJoin(c.Join)
	if err != nil {
		return nil, err
	}
	c.Join = join
	endpoint, advertised, err := listenEndpoint(c, dial)
	if err != nil {
		return nil, err
	}
	n := &node{
		config:   c,
		address:  advertised,
		endpoint: endpoint,
		output:   output,
		members:  make(map[string]member),
		links:    make(map[string]*connection),
		dialing:  make(map[string]bool),
		retries:  make(map[string]retry),
		slots:    make(chan struct{}, 32),
	}
	replica, initial, err := n.initialize(ctx)
	if err != nil {
		_ = endpoint.Close()
		return nil, err
	}
	defer func() {
		if initial != nil {
			_ = initial.Close()
		}
	}()
	n.replica = replica
	n.self = newRecord(n.replica, n.address)
	n.self.Name = c.Name
	if n.self.Name == "" {
		n.self.Name = n.address
	}
	n.self.Counter = 1
	if err = publish(n.replica, &n.self); err != nil {
		_ = n.replica.Close()
		_ = endpoint.Close()
		return nil, err
	}
	// workers survive the signal context only for bounded graceful synchronization.
	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.cancel = cancel
	appCtx, appCancel := context.WithCancel(workerCtx)
	n.appCancel = appCancel
	if err := n.startHTTP(appCtx); err != nil {
		n.close()
		return nil, err
	}
	n.startBoard(appCtx)
	n.workers.Go(func() { n.accept(workerCtx) })
	if initial != nil {
		n.launch(workerCtx, c.Join, initial, true, false)
		initial = nil
	}
	n.workers.Go(func() { n.pulse(workerCtx) })
	return n, nil
}

func canonicalJoin(address string) (string, error) {
	if address == "" {
		return "", nil
	}
	parsed, err := netip.ParseAddrPort(address)
	if err != nil || parsed.Addr().IsUnspecified() || parsed.Port() == 0 {
		return "", errors.New("join must be a dialable numeric IP:port with a nonzero port")
	}
	return parsed.String(), nil
}

func listenEndpoint(c config, dial routed.Dial) (*routed.Endpoint, string, error) {
	address, err := netip.ParseAddrPort(c.Listen)
	if err != nil || address.Addr().IsUnspecified() {
		return nil, "", errors.New("listen must be a dialable numeric IP:port")
	}
	listener, err := net.Listen("tcp", address.String())
	if err != nil {
		return nil, "", fmt.Errorf("listen: %w", err)
	}
	advertised := listener.Addr().String()
	if c.Join == advertised {
		_ = listener.Close()
		return nil, "", errors.New("cannot join this worker's own gossip address")
	}
	if dial == nil {
		dial = func(ctx context.Context, address []byte) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", string(address))
		}
	}
	endpoint, err := routed.New(
		listener,
		[]byte(advertised),
		dial,
		routed.Config{RoutingTimeout: c.IOTimeout},
	)
	if err != nil {
		_ = listener.Close()
		return nil, "", err
	}
	return endpoint, advertised, nil
}

func (n *node) initialize(ctx context.Context) (*hearsay.Replica[envelope], *link.Managed, error) {
	if n.config.Join != "" {
		return n.bootstrap(ctx, n.config.Join)
	}
	seed, err := hearsay.Seed(payloadCodec())
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = seed.Close() }()
	replica, err := seed.IntoReplica()
	return replica, nil, err
}

func (n *node) bootstrap(
	ctx context.Context,
	address string,
) (*hearsay.Replica[envelope], *link.Managed, error) {
	if _, err := netip.ParseAddrPort(address); err != nil {
		return nil, nil, fmt.Errorf("join address: %w", err)
	}
	for failures := 0; ; failures++ {
		replica, managed, err := n.joinOnce(ctx, address)
		if err == nil {
			return replica, managed, nil
		}
		timer := time.NewTimer(retryDelay(failures, n.config.RetryMin, n.config.RetryMax))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (n *node) joinOnce(
	ctx context.Context,
	address string,
) (*hearsay.Replica[envelope], *link.Managed, error) {
	managed, err := n.open(ctx, address, hello{})
	if err != nil {
		return nil, nil, err
	}
	attempt, cancel := context.WithTimeout(ctx, n.config.IOTimeout)
	defer cancel()
	joined, err := hearsay.Bootstrap(payloadCodec()).Join(attempt, managed)
	if err != nil {
		_ = managed.Close()
		return nil, nil, err
	}
	if joined.Peer == nil {
		_ = managed.Close()
		return nil, nil, errors.New("remote peer was also bootstrapping")
	}
	replica, err := joined.Peer.IntoReplica()
	if err != nil {
		_ = joined.Peer.Close()
		_ = managed.Close()
		return nil, nil, err
	}
	return replica, managed, nil
}

// frame reads exactly its payload; buffering here could consume gossip bytes.
func writeHello(writer io.Writer, h hello) error {
	payload, err := json.Marshal(h)
	if err != nil {
		return err
	}
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	frame = append(frame, payload...)
	for len(frame) > 0 {
		count, err := writer.Write(frame)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		frame = frame[count:]
	}
	return nil
}

func readHello(reader io.Reader) (hello, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return hello{}, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size > 1024 {
		return hello{}, errors.New("admission frame exceeds 1024 bytes")
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return hello{}, err
	}
	var h hello
	err := json.Unmarshal(payload, &h)
	return h, err
}

func (n *node) open(ctx context.Context, address string, h hello) (*link.Managed, error) {
	attempt, cancel := context.WithTimeout(ctx, n.config.IOTimeout)
	defer cancel()
	_, raw, err := n.endpoint.Link(attempt, []byte(address))
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(attempt, func() { _ = raw.Close() })
	err = writeHello(raw.Control(), h)
	if err == nil {
		_, err = readHello(raw.Control())
	}
	stop()
	if err != nil || attempt.Err() != nil {
		_ = raw.Close()
		return nil, errors.Join(err, attempt.Err())
	}
	return link.Manage(raw), nil
}

func (n *node) accept(ctx context.Context) {
	for {
		info, raw, err := n.endpoint.Accept(ctx)
		if err != nil {
			return
		}
		select {
		case n.slots <- struct{}{}:
			n.workers.Go(func() {
				defer func() { <-n.slots }()
				n.incoming(ctx, string(info.Peer), raw)
			})
		default:
			_ = raw.Close()
		}
	}
}

func (n *node) incoming(ctx context.Context, address string, raw *routed.Link) {
	h, err := n.admit(ctx, raw)
	if err != nil {
		_ = raw.Close()
		return
	}
	n.serve(ctx, address, link.Manage(raw), false, h.Probe)
}

func (n *node) admit(ctx context.Context, raw *routed.Link) (hello, error) {
	attempt, cancel := context.WithTimeout(ctx, n.config.IOTimeout)
	defer cancel()
	stop := context.AfterFunc(attempt, func() { _ = raw.Close() })
	defer stop()
	h, err := readHello(raw.Control())
	if err != nil {
		return hello{}, err
	}
	err = writeHello(raw.Control(), hello{})
	return h, err
}

func (n *node) launch(
	ctx context.Context,
	address string,
	managed *link.Managed,
	outgoing, probe bool,
) {
	n.workers.Go(func() { n.serve(ctx, address, managed, outgoing, probe) })
}

// register resolves simultaneous dials identically at both endpoints.
func (n *node) register(address string, c *connection) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if address == n.address || n.leaving {
		return false
	}
	if old := n.links[address]; old != nil {
		preferOutgoing := n.address < address
		if old.outgoing == preferOutgoing || c.outgoing != preferOutgoing {
			return false
		}
		_ = old.managed.Close()
	}
	n.links[address] = c
	return true
}

func (n *node) serve(
	ctx context.Context,
	address string,
	managed *link.Managed,
	outgoing, probe bool,
) {
	defer func() { _ = managed.Close() }()
	if probe {
		attempt, cancel := context.WithTimeout(ctx, n.config.IOTimeout)
		defer cancel()
		_, _ = n.replica.GossipOnce(attempt, managed)
		return
	}
	c := &connection{managed: managed, outgoing: outgoing, started: time.Now()}
	if !n.register(address, c) {
		return
	}
	defer func() {
		n.mu.Lock()
		if n.links[address] == c {
			delete(n.links, address)
		}
		n.mu.Unlock()
	}()
	for result, err := range n.replica.Gossip(ctx, managed, hearsay.GossipOptions[envelope]{SessionDeadline: &n.config.IOTimeout}) {
		if err != nil {
			return
		}
		n.mu.Lock()
		c.converged = result.Converged
		n.trim(address)
		n.mu.Unlock()
	}
}

// trim admits joiners at capacity and keeps at most target+1 established links.
// the oldest other link is replaced; this does not guarantee graph connectivity.
func (n *node) trim(keep string) {
	if len(n.links) <= n.config.Neighbors+1 {
		return
	}
	var victim string
	for address, c := range n.links {
		if address == keep {
			continue
		}
		if victim == "" || c.started.Before(n.links[victim].started) {
			victim = address
		}
	}
	if victim != "" {
		_ = n.links[victim].managed.Close()
		delete(n.links, victim)
	}
}

func retryDelay(failures int, minimum, maximum time.Duration) time.Duration {
	ceiling := minimum
	for range min(failures, 30) {
		ceiling = min(ceiling*2, maximum)
	}
	return minimum + time.Duration(rand.Int64N(int64(ceiling-minimum)+1))
}

func (n *node) candidate(now time.Time) (string, bool) {
	probe := len(n.links) >= n.config.Neighbors
	if probe && now.Sub(n.lastProbe) < n.config.ProbeEvery {
		return "", false
	}
	var candidates []string
	for address, m := range n.members {
		if address != n.address && !m.Record.Left && n.links[address] == nil &&
			!n.dialing[address] &&
			!now.Before(n.retries[address].after) {
			candidates = append(candidates, address)
		}
	}
	if len(candidates) == 0 {
		return "", false
	}
	address := candidates[rand.IntN(len(candidates))]
	n.dialing[address] = true
	if probe {
		n.lastProbe = now
	}
	return address, probe
}

func (n *node) connect(ctx context.Context, address string, probe bool) {
	n.mu.Lock()
	// a stale member may be across a partition; retain a recovered bridge so
	// heartbeat freshness does not depend on occasional one-shot probes.
	probe = probe &&
		n.members[address].status(
			time.Now(),
			n.config.SuspectAfter,
			n.config.UnreachableAfter,
		) == "active"
	n.mu.Unlock()
	defer func() { n.mu.Lock(); delete(n.dialing, address); n.mu.Unlock() }()
	managed, err := n.open(ctx, address, hello{Probe: probe})
	n.mu.Lock()
	r := n.retries[address]
	if err == nil {
		r.failures = 0
	} else {
		r.failures++
	}
	r.after = time.Now().Add(retryDelay(r.failures, n.config.RetryMin, n.config.RetryMax))
	n.retries[address] = r
	n.mu.Unlock()
	if err == nil {
		n.serve(ctx, address, managed, true, probe)
	}
}

func (n *node) pulse(ctx context.Context) {
	// the short scan tick only updates local views; heartbeats remain independent.
	ticker := time.NewTicker(min(n.config.Heartbeat, 250*time.Millisecond))
	defer ticker.Stop()
	nextHeartbeat := time.Now().Add(n.config.Heartbeat)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if !now.Before(nextHeartbeat) {
				n.heartbeat()
				nextHeartbeat = now.Add(n.config.Heartbeat)
			}
			n.maintain(ctx, now)
		}
	}
}

func (n *node) heartbeat() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaving {
		return
	}
	n.self.Counter++
	if err := publish(n.replica, &n.self); err != nil {
		n.failure = err
	}
}

func (n *node) maintain(ctx context.Context, now time.Time) {
	n.mu.Lock()
	n.refresh(now)
	address, probe := "", false
	if !n.leaving {
		address, probe = n.candidate(now)
	}
	n.mu.Unlock()
	if address == "" {
		return
	}
	select {
	case n.slots <- struct{}{}:
		n.workers.Go(func() { defer func() { <-n.slots }(); n.connect(ctx, address, probe) })
	default:
		n.mu.Lock()
		delete(n.dialing, address)
		n.mu.Unlock()
	}
}

func (n *node) refresh(now time.Time) {
	for m := range n.replica.Snapshot().All() {
		if m.Err != nil {
			n.failure = m.Err
			continue
		}
		if m.Payload.Kind == "member" {
			observe(n.members, m.Payload.Member, now)
		}
	}
	var text strings.Builder
	addresses := make([]string, 0, len(n.links))
	for address := range n.links {
		addresses = append(addresses, address)
	}
	slices.Sort(addresses)
	fmt.Fprintf(
		&text,
		"node %s neighbors=%d [%s]\n",
		n.address,
		len(addresses),
		strings.Join(addresses, ", "),
	)
	for _, address := range sortedAddresses(n.members) {
		m := n.members[address]
		fmt.Fprintf(
			&text,
			"  %s name=%q incarnation=%d/%s %s\n",
			address,
			m.Record.Name,
			m.Record.Generation,
			m.Record.Incarnation[:min(8, len(m.Record.Incarnation))],
			m.status(now, n.config.SuspectAfter, n.config.UnreachableAfter),
		)
	}
	view := text.String()
	if view != n.rendered {
		if _, err := io.WriteString(n.output, view+"\n"); err != nil {
			n.failure = err
		}
		n.rendered = view
	}
}

func (n *node) run(ctx context.Context) error {
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), n.config.LeaveTimeout)
	defer cancel()
	return n.leave(shutdown)
}

func (n *node) leave(ctx context.Context) error {
	n.appCancel()
	n.appWorkers.Wait()
	n.mu.Lock()
	n.leaving = true
	n.self.Counter++
	n.self.Left = true
	err := publish(n.replica, &n.self)
	departure := n.replica.Snapshot().Latest()
	n.mu.Unlock()
	if err != nil {
		return fmt.Errorf("publish departure: %w", err)
	}
	<-ctx.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	var acknowledged int
	for _, c := range n.links {
		if departure.LessEqual(c.converged) {
			acknowledged++
		}
	}
	_, err = fmt.Fprintf(
		n.output,
		"departure %s synchronized with %d current direct neighbors; global delivery unconfirmed\n",
		n.address,
		acknowledged,
	)
	return errors.Join(n.failure, err)
}

func (n *node) close() {
	n.cancel()
	n.appCancel()
	_ = n.endpoint.Close()
	n.appWorkers.Wait()
	n.workers.Wait()
	_ = n.replica.Close()
}
