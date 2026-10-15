// Command runner verifies live V2 sessions against the pinned Rust oracle.
package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

func main() {
	if os.Getenv("HEARSAY_INTEROP_DEBUG") == "1" {
		time.AfterFunc(5*time.Second, dumpStacks)
	}
	for _, provider := range []string{"rust", "go"} {
		if selected := os.Getenv(
			"HEARSAY_INTEROP_PROVIDER",
		); selected != "" &&
			selected != provider {
			continue
		}
		if err := runScenario(provider); err != nil {
			fmt.Fprintln(os.Stderr, provider, err)
			os.Exit(1)
		}
	}
}

func dumpStacks() {
	path := os.Getenv("HEARSAY_INTEROP_STACK_FILE")
	if path == "" {
		path = ".agi.local/interop-artifacts/go-stack.txt"
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer func() { _ = f.Close() }()
	if err := pprof.Lookup("goroutine").WriteTo(f, 2); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
