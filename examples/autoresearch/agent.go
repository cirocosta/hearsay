package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cirocosta/hearsay"
)

var errLimit = errors.New("research budget reached")

type agent struct {
	id       string
	members  []string
	peer     *hearsay.Replica[note]
	model    model
	root     *os.Root
	executor *executor
	log      *logger
	seq      int
	active   string
}

func instructions(actor string) string {
	return `You are ` + actor + `, an independent parser researcher.
Your private Go module imports your own research/parser package. Other agents
have separate modules. Your only shared knowledge comes from your local Hearsay
experiment log. No agent assigns work or controls another agent.
Read contract/contract.go, parser/parser.go and parser_test.go to learn the fixed
interface, synthetic workload and encoding/json oracle. Optimize Parse while
preserving output, errors, exact numeric strings and input ownership. The fixed
suite is a demonstration, not proof of production compatibility.
Read new peer entries before choosing your next experiment. Learn from failures
and build on promising candidates. Announce a hypothesis with try, edit only
parser/ source files, then evaluate. Evaluate freezes your package, runs trusted
checks and five benchmark samples against the current best package, and gossips
source, hashes, measurements and failures. A median improvement greater than
3 percent promotes a candidate. Benchmark evidence remains subject to host noise.
Use adopt to copy a visible experiment's parser into your own package and try
an improvement or independent reproduction. Failed approaches are useful evidence.
Use post for observations or questions. Use run for local investigation; its
results are shared automatically. All commands and evaluations share one lane.
Use status(exhausted=true, body=reason) when you have no useful ideas left.
This is revisable: new peer evidence can inspire another try, which clears your
exhaustion status. Do not declare exhaustion with an unfinished experiment.
The run ends when everyone locally sees all members exhausted and logs converge,
or at a hard budget. Exhaustion is not a claim of optimality. No votes are needed.
Use wait for new knowledge; avoid spending turns on acknowledgments or plans.
Commands run as the local user, not in a sandbox. Do not alter the harness,
credentials, host settings, Git repositories or other agents' directories.
Only modify parser/ files using file tools; never modify common checks or contract.
Use uv run for Python. Keep output small. SIMD and assembly are allowed.
`
}

type turnBudget struct{ remaining atomic.Int64 }

func (b *turnBudget) take() bool {
	for available := b.remaining.Load(); available > 0; available = b.remaining.Load() {
		if b.remaining.CompareAndSwap(available, available-1) {
			return true
		}
	}
	return false
}

func (a *agent) run(ctx context.Context, budget *turnBudget) error {
	history := []json.RawMessage{userItem("Members: " + strings.Join(a.members, ", "))}
	if a.active != "" {
		history = append(history, userItem("Continuing saved experiment "+a.active+
			". Your parser package is retained. Evaluate this attempt before starting another; prior notes arrive through gossip."))
	}
	seen := make(map[string]bool)
	for round := 1; ; round++ {
		notes, err := readBoard(a.peer)
		if err != nil {
			return err
		}
		if allExhausted(notes, a.members) {
			a.log.print(a.id, "exhausted", "all peers have exhausted their ideas")
			return nil
		}
		if !budget.take() {
			return fmt.Errorf("%s: %w", a.id, errLimit)
		}
		history = append(
			history,
			userItem(boardDelta(notes, seen, a.limits(ctx, round, budget))),
		)
		if err := a.step(ctx, &history, round); err != nil {
			return err
		}
	}
}

func (a *agent) limits(ctx context.Context, round int, budget *turnBudget) string {
	remaining := "no deadline"
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline).Truncate(time.Second).String() + " wall time left"
	}
	return fmt.Sprintf(
		"your model turn %d; %d turns left across the entire swarm; %s; commands limited to %s",
		round,
		budget.remaining.Load(),
		remaining,
		a.executor.timeout,
	)
}

func (a *agent) step(ctx context.Context, history *[]json.RawMessage, round int) error {
	if len(jsonItem(*history)) > 2<<20 {
		return fmt.Errorf("%s context: %w", a.id, errLimit)
	}
	a.log.print(a.id, "turn", fmt.Sprint(round))
	result, err := a.model.Respond(ctx, a.id, *history)
	if err != nil {
		return fmt.Errorf("%s provider: %w", a.id, err)
	}
	*history = append(*history, result.Items...)
	if result.Text != "" {
		if _, err := a.publish(note{Kind: "note", Body: result.Text}); err != nil {
			return err
		}
	}
	if len(result.Calls) > 16 {
		return errors.New("provider returned more than 16 tool calls")
	}
	for _, tool := range result.Calls {
		output, toolErr := a.use(ctx, tool)
		if toolErr != nil {
			output = "error: " + toolErr.Error()
		}
		*history = append(*history, jsonItem(map[string]string{
			"type": "function_call_output", "call_id": tool.ID, "output": output,
		}))
	}
	return ctx.Err()
}

func (a *agent) publish(n note) (string, error) {
	if len(n.Body) > 32<<10 {
		return "", errors.New("message body exceeds 32 KiB")
	}
	a.seq++
	n.ID, n.Author, n.Seq = fmt.Sprintf("%s:%d", a.id, a.seq), a.id, a.seq
	if _, err := a.peer.Send(n); err != nil {
		return "", err
	}
	a.log.message(n)
	return n.ID, nil
}

func (a *agent) wait(ctx context.Context, seconds int) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(max(1, min(seconds, 30)))*time.Second)
	defer cancel()
	changes := a.peer.Changes()
	_, _ = changes.Next(ctx)
	_, err := changes.Next(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}
