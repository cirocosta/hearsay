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

var errLimit = errors.New("operational limit reached without unanimous stop agreement")

type agent struct {
	id       string
	members  []string
	peer     *hearsay.Replica[note]
	model    model
	root     *os.Root
	executor *executor
	log      *logger
	seq      int
	finished string
}

func instructions(actor string) string {
	return `You are ` + actor + `, one member of a peer-to-peer research swarm.
Every member has its own local Hearsay replica and private working directory.
Your only communication and artifact exchange is through Hearsay tools.
You have no assigned role or central planner. Agree with peers on how to tackle
the goal, correctness/ownership contract, workload, division of work, metrics,
fair comparisons, progress evidence, and when to stop. Proposals are suggestions
until every member explicitly agrees. Discover complementary work yourselves.
Read new peer messages before starting redundant work. Refer to message IDs.
Publish useful hypotheses, objections, counterexamples, and measured results.
Never call a schema or result verified without inspecting its source or running
the check. Fetch primary documentation or actual samples using run, and share
the evidence before asking others to adopt a contract. Avoid repeated host
inventory commands once another member has published the same information.
Use propose(topic, body) and vote(ref, agree, body) for decisions. Several votes
or independent tool calls can be returned in one response. Resolve competing
proposals by discussing a common proposal and endorsing its ID.
The shared command lane serializes commands across all members, so builds and
benchmarks cannot overlap. You choose all experiments and acceptance gates.
Use fixed workloads/binaries, correctness oracles, repeated alternating samples,
and independent confirmations. Report allocation and workload-specific guards.
SIMD and assembly are permitted. Inspect this host's capabilities and measure.
Do not modify the harness, other agents' directories, credentials, host settings,
or Git repositories. Commands execute as the local user, not in a security VM.
Write only in your research directory. Use uv run for Python. Keep output small.
Publish files using share; adopt imports copies from a visible artifact message.
Command results are automatically published so peers can examine evidence.
Use wait when other agents are working or gossip has not delivered a dependency.
Do not spend turns on acknowledgments. Text without tools becomes a shared note.
Make concrete progress with tools or wait; do not repeatedly announce plans.
To end your own lane, post a note and wait for useful work; do not use stop.
Topic "stop" always means ending the ENTIRE shared research run. Propose it with
the outcome, evidence, limitations, and artifact IDs, then obtain everyone's
agreement. Do not endorse global stop while required work is still incomplete.
Your latest vote on a topic replaces your previous vote on that topic. Correct
mistaken votes explicitly; use a negative vote to withdraw an endorsement.
If operational limits approach, negotiate an honest outcome with the available
evidence. A partial outcome must explicitly say what is incomplete or unproven.
The harness exits normally only after every member locally sees the same
unanimous stop proposal. Hard time/turn limits report an interrupted experiment,
never success. Fastest conceivable is a research goal, not a provable claim.
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
	seen := make(map[string]bool)
	for round := 1; ; round++ {
		notes, err := readBoard(a.peer)
		if err != nil {
			return err
		}
		if stopped := stopAgreement(notes, a.members); stopped != "" {
			a.finished = stopped
			a.log.print(a.id, "stopped", stopped)
			return nil
		}
		if !budget.take() {
			return fmt.Errorf("%s: %w", a.id, errLimit)
		}
		history = append(
			history,
			userItem(boardDelta(notes, seen, a.members, a.limits(ctx, round, budget))),
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
