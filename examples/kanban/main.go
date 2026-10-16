// the kanban command demonstrates a shared worker board over sparse gossip.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	Listen           string
	Name             string
	HTTP             string
	JobDuration      time.Duration
	Join             string
	Neighbors        int
	Heartbeat        time.Duration
	SuspectAfter     time.Duration
	UnreachableAfter time.Duration
	LeaveTimeout     time.Duration
	RetryMin         time.Duration
	RetryMax         time.Duration
	ProbeEvery       time.Duration
	IOTimeout        time.Duration
}

func defaults() config {
	return config{
		Listen:           "127.0.0.1:9000",
		HTTP:             "127.0.0.1:0",
		JobDuration:      2 * time.Second,
		Neighbors:        3,
		Heartbeat:        time.Second,
		SuspectAfter:     5 * time.Second,
		UnreachableAfter: 15 * time.Second,
		LeaveTimeout:     5 * time.Second,
		RetryMin:         time.Second,
		RetryMax:         10 * time.Second,
		ProbeEvery:       10 * time.Second,
		IOTimeout:        3 * time.Second,
	}
}

func (c config) validate() error {
	if c.Neighbors < 1 {
		return errors.New("neighbors must be positive")
	}
	if c.JobDuration <= 0 {
		return errors.New("job-duration must be positive")
	}
	if c.Heartbeat <= 0 || c.SuspectAfter <= c.Heartbeat || c.UnreachableAfter <= c.SuspectAfter {
		return errors.New("require 0 < heartbeat < suspect-after < unreachable-after")
	}
	if c.LeaveTimeout <= 0 || c.RetryMin <= 0 || c.RetryMax < c.RetryMin || c.ProbeEvery <= 0 ||
		c.IOTimeout <= 0 {
		return errors.New("timeouts must be positive and retry-max >= retry-min")
	}
	return nil
}

func run(ctx context.Context, args []string, output io.Writer) error {
	c := defaults()
	flags := flag.NewFlagSet("kanban", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&c.Listen, "listen", c.Listen, "dialable IP:port to listen on")
	flags.StringVar(&c.Name, "name", "", "worker name (defaults to gossip address)")
	flags.StringVar(&c.HTTP, "http", c.HTTP, "HTTP API listen address (empty disables HTTP)")
	flags.DurationVar(
		&c.JobDuration,
		"job-duration",
		c.JobDuration,
		"duration of the built-in checksum demo job",
	)
	flags.StringVar(&c.Join, "join", "", "known peer IP:port (omit to seed a new network)")
	flags.IntVar(
		&c.Neighbors,
		"neighbors",
		c.Neighbors,
		"target direct neighbors, including incoming links",
	)
	flags.DurationVar(&c.Heartbeat, "heartbeat", c.Heartbeat, "self heartbeat interval")
	flags.DurationVar(
		&c.SuspectAfter,
		"suspect-after",
		c.SuspectAfter,
		"time without a newer heartbeat before suspicion",
	)
	flags.DurationVar(
		&c.UnreachableAfter,
		"unreachable-after",
		c.UnreachableAfter,
		"time without a newer heartbeat before unreachable",
	)
	flags.DurationVar(
		&c.LeaveTimeout,
		"leave-timeout",
		c.LeaveTimeout,
		"maximum graceful departure synchronization wait",
	)
	flags.DurationVar(
		&c.ProbeEvery,
		"probe-every",
		c.ProbeEvery,
		"recovery probe interval, even at neighbor target",
	)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if err := c.validate(); err != nil {
		return err
	}
	n, err := startNode(ctx, c, output, nil)
	if err != nil {
		return err
	}
	defer n.close()
	return n.run(ctx)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
