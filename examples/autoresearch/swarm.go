package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func createRun(cfg config) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.Directory), 0o700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(cfg.Directory, 0o700); err != nil {
		return nil, fmt.Errorf("run directory must be new: %w", err)
	}
	if err := os.WriteFile(
		filepath.Join(cfg.Directory, "run.json"),
		jsonItem(cfg),
		0o600,
	); err != nil {
		return nil, err
	}
	return os.OpenFile(
		filepath.Join(cfg.Directory, "events.jsonl"),
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		0o600,
	)
}

func runSwarm(ctx context.Context, cfg config, provider model, output io.Writer) error {
	events, err := createRun(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = events.Close() }()
	log := &logger{output: output, events: events}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	setup, finish := context.WithTimeout(ctx, 30*time.Second)
	n, err := newNetwork(setup, cfg.Agents, cfg.Goal)
	finish()
	if err != nil {
		return err
	}
	defer n.close()
	defer cancel()
	agents, err := prepareAgents(cfg, n, provider, log)
	if err != nil {
		return err
	}
	defer closeAgents(agents)
	log.print(
		"swarm",
		"started",
		fmt.Sprintf("%d %s peers, gossip ring, %s", cfg.Agents, cfg.Model, cfg.Directory),
	)
	err = collaborate(ctx, cancel, agents, n.start(ctx), cfg.Turns)
	if err == nil {
		err = drainGossip(ctx, n)
	}
	if err == nil {
		err = checkFinalAgreement(n, agents)
	}
	cancel()
	if saveErr := saveBoards(cfg.Directory, n); saveErr != nil {
		err = errors.Join(err, saveErr)
	}
	log.print(
		"swarm",
		"traffic",
		fmt.Sprintf(
			"%d peer-session completions, %d data bytes sent",
			n.sessions.Load(),
			n.bytes.Load(),
		),
	)
	if err != nil {
		log.print("swarm", "limited", err.Error())
		return err
	}
	log.print("swarm", "agreed", "all agents observed unanimous stop; boards converged")
	return nil
}

func prepareAgents(cfg config, n *network, provider model, log *logger) ([]*agent, error) {
	members := make([]string, cfg.Agents)
	for i := range members {
		members[i] = fmt.Sprintf("agent-%02d", i)
	}
	commands := &executor{lane: make(chan struct{}, 1), timeout: cfg.CommandTimeout}
	var agents []*agent
	for i, id := range members {
		directory := filepath.Join(cfg.Directory, id)
		if err := os.Mkdir(directory, 0o700); err != nil {
			closeAgents(agents)
			return nil, err
		}
		root, err := os.OpenRoot(directory)
		if err != nil {
			closeAgents(agents)
			return nil, err
		}
		agents = append(agents, &agent{
			id: id, members: members, peer: n.peers[i], model: provider,
			root: root, executor: commands, log: log,
		})
	}
	return agents, nil
}

func closeAgents(agents []*agent) {
	for _, a := range agents {
		_ = a.root.Close()
	}
}

func checkFinalAgreement(n *network, agents []*agent) error {
	expected := agents[0].finished
	for _, a := range agents {
		if expected == "" || a.finished != expected {
			return errors.New("agents observed different stop decisions")
		}
	}
	notes, err := readBoard(n.peers[0])
	if err != nil {
		return err
	}
	if stopAgreement(notes, agents[0].members) != expected {
		return errors.New("stop agreement was withdrawn before final convergence")
	}
	return nil
}

func collaborate(
	ctx context.Context,
	cancel context.CancelFunc,
	agents []*agent,
	failures <-chan error,
	turns int,
) error {
	var workers sync.WaitGroup
	budget := &turnBudget{}
	budget.remaining.Store(int64(turns))
	results := make(chan error, len(agents))
	for _, a := range agents {
		workers.Go(func() { results <- a.run(ctx, budget) })
	}
	defer workers.Wait()
	for range agents {
		select {
		case err := <-results:
			if err != nil {
				cancel()
				return err
			}
		case err := <-failures:
			cancel()
			return err
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		}
	}
	return nil
}

func drainGossip(ctx context.Context, n *network) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if converged(n) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("final gossip drain: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func converged(n *network) bool {
	first := n.peers[0].Snapshot()
	for _, peer := range n.peers[1:] {
		if !first.Equal(peer.Snapshot()) {
			return false
		}
	}
	return true
}

func saveBoards(directory string, n *network) error {
	directory = filepath.Join(directory, "boards")
	if err := os.Mkdir(directory, 0o700); err != nil {
		return err
	}
	for i, peer := range n.peers {
		notes, err := readBoard(peer)
		if err != nil {
			return err
		}
		path := filepath.Join(directory, fmt.Sprintf("agent-%02d.json", i))
		if err := os.WriteFile(path, jsonItem(notes), 0o600); err != nil {
			return err
		}
	}
	return nil
}
