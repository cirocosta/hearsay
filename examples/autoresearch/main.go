// Command autoresearch gives a goal to independent agents linked by Hearsay.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/cirocosta/hearsay/examples/autoresearch/internal/codex"
)

const defaultGoal = `Make parsing Hyperliquid's websocket trades and l2Book JSON streams as fast as you can on this host. Deliver runnable source, a representative corpus, correctness checks, reproducible benchmarks, and evidence for the best candidate. SIMD, assembly, specialized layouts, and any available language/toolchain are allowed. As a swarm, agree on the parsing contract (including numeric precision, ownership and malformed inputs), workload, baseline, metrics, division of work, progress checks, fair repeated comparisons, and stopping criteria. Investigate rather than assuming a particular optimization wins. Distinguish measured improvements from claims about universal optimality. Start with the official websocket schemas: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions.md .`

type config struct {
	Agents         int           `json:"agents"`
	Model          string        `json:"model"`
	Goal           string        `json:"goal"`
	Directory      string        `json:"directory"`
	Duration       time.Duration `json:"duration"`
	Turns          int           `json:"turns"`
	CommandTimeout time.Duration `json:"command_timeout"`
	AuthFile       string        `json:"-"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := runCLI(ctx, os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseConfig(args []string, output io.Writer) (config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return config{}, err
	}
	var cfg config
	f := flag.NewFlagSet("autoresearch", flag.ContinueOnError)
	f.SetOutput(output)
	f.IntVar(&cfg.Agents, "agents", 10, "independent Hearsay peers")
	f.StringVar(&cfg.Model, "model", "gpt-6-luna", "Codex model used by every agent")
	f.StringVar(&cfg.Goal, "goal", defaultGoal, "the swarm's shared goal")
	f.StringVar(
		&cfg.Directory,
		"dir",
		filepath.Join(
			".agi.local",
			"autoresearch",
			time.Now().UTC().Format("20060102-150405.000000000"),
		),
		"new directory for this run",
	)
	f.StringVar(
		&cfg.AuthFile,
		"auth",
		filepath.Join(home, ".codex", "auth.json"),
		"read-only Codex or gx credential file",
	)
	f.DurationVar(
		&cfg.Duration,
		"duration",
		20*time.Minute,
		"hard run ceiling, independent of agent stopping decisions",
	)
	f.IntVar(&cfg.Turns, "turns", 640, "maximum model turns across the entire swarm")
	f.DurationVar(
		&cfg.CommandTimeout,
		"command-timeout",
		90*time.Second,
		"maximum duration of a command",
	)
	if err := f.Parse(args); err != nil {
		return cfg, err
	}
	if f.NArg() != 0 || cfg.Agents < 2 || cfg.Agents > 32 || cfg.Turns < 1 || cfg.Duration <= 0 ||
		cfg.CommandTimeout <= 0 ||
		cfg.Goal == "" ||
		cfg.Model == "" {
		return cfg, errors.New(
			"require 2-32 agents, positive limits, a goal/model, and no positional arguments",
		)
	}
	cfg.Directory, err = filepath.Abs(cfg.Directory)
	return cfg, err
}

func runCLI(ctx context.Context, args []string, output io.Writer) error {
	cfg, err := parseConfig(args, output)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	provider, err := codex.New(cfg.AuthFile, cfg.Model, filepath.Base(cfg.Directory))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	return runSwarm(
		ctx,
		cfg,
		codexModel{client: provider},
		output,
	)
}
