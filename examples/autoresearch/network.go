package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link"
)

type edge struct {
	left, right int
	a, b        *link.Managed
}

type network struct {
	peers    []*hearsay.Replica[note]
	edges    []edge
	workers  sync.WaitGroup
	sessions atomic.Uint64
	bytes    atomic.Uint64
}

func newNetwork(ctx context.Context, count int, goal string) (*network, error) {
	n := &network{}
	if err := n.bootstrap(ctx, count, goal); err != nil {
		n.close()
		return nil, err
	}
	if count > 2 {
		n.edges = append(n.edges, newEdge(count-1, 0))
	}
	return n, nil
}

func newEdge(left, right int) edge {
	a, b := link.Memory()
	return edge{left: left, right: right, a: link.Manage(a), b: link.Manage(b)}
}

func (n *network) bootstrap(ctx context.Context, count int, goal string) error {
	seed, err := hearsay.Seed(hearsay.JSONCodec[note]())
	if err != nil {
		return err
	}
	first, err := seed.IntoReplica()
	if err != nil {
		_ = seed.Close()
		return err
	}
	n.peers = append(n.peers, first)
	if _, err := first.Send(
		note{ID: "goal", Author: "user", Kind: "goal", Body: goal},
	); err != nil {
		return err
	}
	for i := 1; i < count; i++ {
		if err := n.join(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

func (n *network) join(ctx context.Context, index int) error {
	e := newEdge(index-1, index)
	n.edges = append(n.edges, e)
	served := make(chan error, 1)
	go func() { _, err := n.peers[index-1].GossipOnce(ctx, e.a); served <- err }()
	joined, joinErr := hearsay.Bootstrap(hearsay.JSONCodec[note]()).Join(ctx, e.b)
	if joinErr != nil {
		_ = e.b.Close()
	}
	serveErr := <-served
	if err := errors.Join(joinErr, serveErr); err != nil {
		if joined.Peer != nil {
			_ = joined.Peer.Close()
		}
		return err
	}
	replica, err := joined.Peer.IntoReplica()
	if err == nil {
		n.peers = append(n.peers, replica)
	}
	return err
}

func (n *network) start(ctx context.Context) <-chan error {
	failures := make(chan error, 2*len(n.edges))
	for _, e := range n.edges {
		n.drive(ctx, e.left, e.a, failures)
		n.drive(ctx, e.right, e.b, failures)
	}
	return failures
}

func (n *network) drive(
	ctx context.Context,
	index int,
	carrier *link.Managed,
	failures chan<- error,
) {
	n.workers.Go(func() {
		for result, err := range n.peers[index].Gossip(ctx, carrier, hearsay.GossipOptions[note]{}) {
			if err != nil {
				if ctx.Err() == nil {
					failures <- fmt.Errorf("agent-%02d gossip: %w", index, err)
				}
				return
			}
			n.sessions.Add(1)
			n.bytes.Add(result.Stats.BytesSent)
		}
	})
}

func (n *network) close() {
	for _, e := range n.edges {
		_ = e.a.Close()
		_ = e.b.Close()
	}
	n.workers.Wait()
	for _, peer := range n.peers {
		_ = peer.Close()
	}
}
