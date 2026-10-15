//go:build unix

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCommandsAreSerializedAndCancelledAsGroups(t *testing.T) {
	directory := t.TempDir()
	e := &executor{lane: make(chan struct{}, 1), timeout: time.Second}
	var workers sync.WaitGroup
	for range 3 {
		workers.Go(func() {
			result, err := e.run(
				context.Background(),
				directory,
				"printf 'start\\n' >> trace; sleep .02; printf 'end\\n' >> trace",
			)
			if err != nil || result.Exit != 0 {
				t.Errorf("%+v %v", result, err)
			}
		})
	}
	workers.Wait()
	trace, err := os.ReadFile(filepath.Join(directory, "trace"))
	if err != nil || string(trace) != strings.Repeat("start\nend\n", 3) {
		t.Fatalf("commands overlapped: %q %v", trace, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	result, err := e.run(ctx, directory, "sleep 30 & wait")
	if err != nil || result.Exit == 0 || time.Since(start) > 2*time.Second {
		t.Fatalf("descendant did not terminate: %+v %v", result, err)
	}
}

func TestCommandOutputAndEnvironmentAreBounded(t *testing.T) {
	t.Setenv("DEMO_API_KEY", "hidden")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result, err := executeCommand(
		ctx,
		t.TempDir(),
		"test -z \"$DEMO_API_KEY\" || exit 42; yes output",
	)
	if err != nil || result.Exit == 42 || !result.Truncated || len(result.Output) > (24<<10)+200 {
		t.Fatalf("output/environment bounds: %+v %v", result, err)
	}
}
