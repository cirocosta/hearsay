package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type executor struct {
	lane    chan struct{}
	timeout time.Duration
}

type commandResult struct {
	Exit      int    `json:"exit"`
	Output    string `json:"output"`
	Truncated bool   `json:"truncated"`
	Elapsed   string `json:"elapsed"`
}

// limitedOutput keeps draining a noisy command without unbounded allocation.
type limitedOutput struct {
	data      []byte
	truncated bool
}

func (b *limitedOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (24 << 10) - len(b.data)
	if len(p) > remaining {
		b.truncated = true
		p = p[:remaining]
	}
	b.data = append(b.data, p...)
	return n, nil
}

func (e *executor) run(ctx context.Context, directory, command string) (commandResult, error) {
	select {
	case e.lane <- struct{}{}:
		defer func() { <-e.lane }()
	case <-ctx.Done():
		return commandResult{}, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	return executeCommand(ctx, directory, command)
}

func commandEnvironment(directory string) []string {
	var result []string
	for _, variable := range os.Environ() {
		key, _, _ := strings.Cut(variable, "=")
		if strings.Contains(key, "TOKEN") || strings.Contains(key, "SECRET") ||
			strings.Contains(key, "API_KEY") ||
			key == "GOWORK" {
			continue
		}
		result = append(result, variable)
	}
	return append(result, "PWD="+directory, "GOWORK=off")
}

func (a *agent) runCommand(ctx context.Context, command string) (string, error) {
	if strings.TrimSpace(command) == "" {
		return "", errors.New("command cannot be empty")
	}
	a.log.print(a.id, "command", command)
	result, err := a.executor.run(ctx, a.root.Name(), command)
	if err != nil {
		result.Exit = -1
		result.Output += "\nrunner error: " + err.Error()
	}
	body := string(jsonItem(struct {
		Command string        `json:"command"`
		Result  commandResult `json:"result"`
	}{Command: command, Result: result}))
	id, err := a.publish(note{Kind: "result", Body: body})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("result %s\n%s", id, body), nil
}
