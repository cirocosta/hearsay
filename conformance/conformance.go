// Package conformance provides live-process contract checks for caller-owned
// transports and bookmark storage. Checks use fresh fixtures, bounded waits,
// opaque records, and protocol-independent stream traffic. Passing demonstrates
// the sampled sizes and schedules; it does not prove security, crash durability,
// power-loss durability, every failure point, or absence of future background
// writes. Transport buffer bounds and shared-credit sizing still need review.
package conformance

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

const defaultTimeout = 5 * time.Second

func timeoutValue(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return defaultTimeout
	}
	return timeout
}

type tasks struct {
	ctx     context.Context
	cancel  context.CancelFunc
	timeout time.Duration
	pending sync.WaitGroup
	results chan error
	count   int
}

func newTasks(timeout time.Duration) *tasks {
	timeout = timeoutValue(timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &tasks{ctx: ctx, cancel: cancel, timeout: timeout, results: make(chan error, 256)}
}

func (g *tasks) run(task func() error) {
	g.count++
	g.pending.Add(1)
	go func() { defer g.pending.Done(); g.results <- task() }()
}

func (g *tasks) wait(t *testing.T) {
	t.Helper()
	for range g.count {
		select {
		case err := <-g.results:
			if err != nil {
				t.Fatal(err)
			}
		case <-g.ctx.Done():
			t.Fatalf("conformance deadline: %v", g.ctx.Err())
		}
	}
	g.count = 0
}

func (g *tasks) cleanup(t *testing.T, closeFixture func() error) {
	t.Helper()
	g.cancel()
	if err := closeFixture(); err != nil {
		t.Errorf("fixture cleanup: %v", err)
	}
	done := make(chan struct{})
	go func() { g.pending.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(g.timeout):
		t.Errorf("transport operations did not exit after close; fixture may leak goroutines")
	}
}

func checkClose(t *testing.T, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func expectFailure(operation func() error) error {
	if err := operation(); err != nil {
		return nil
	}
	return errors.New("operation succeeded after transport close")
}
