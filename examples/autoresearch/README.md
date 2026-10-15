# A shared research notebook carried by gossip

Ten independent agents try to make a Go JSON parser faster. Each has its own
parser package, working directory, model conversation, and Hearsay replica.
There is no research coordinator. Agents learn what others tried through their
local copy of the experiment log.

The loop is small:

1. Read new entries from the local replica.
2. Choose an idea and announce it with `try`.
3. Edit the local `parser/` package.
4. Call `evaluate` to check and measure a frozen copy.
5. Read what peers learned and choose the next idea.

For example, A tests a different decoder and publishes its source and results.
B sees the result through gossip, uses `adopt` to copy that candidate, and tries
an improvement. C reads a failed attempt and explores a different approach.
Independent reproduction is useful; accidental repetition can still happen
before gossip delivers an entry.

## What the agents share

Entries carry intent, completed experiments, observations, or exhaustion status.
An evaluated experiment includes its source, source and workload hashes, command
output, correctness outcome, and benchmark samples. Failures stay in the log.
Entries link to the earlier `try` so readers can follow an attempt.

Agents use `post` for questions and findings, `read_file`/`write_file` for local
source, `run` for investigation, and `wait` for new knowledge. Command results
are shared automatically. `adopt` replaces the local parser package with the
source from a visible experiment. File writes are limited to Go, assembly, and
header files directly under `parser/`; external dependencies and additional
packages are outside this example's fixed module.

All peers run in one process with memory links in a ring. Only an agent's local
replica supplies messages to its conversation. The runner connects peers and
supplies tools; it does not choose hypotheses or assign work.

## A common check and a mechanical winner

Every private module starts with the same runnable main program, parser
interface, `encoding/json` baseline, correctness checks, and synthetic fixtures
for Hyperliquid-style `trades` and `l2Book` messages. Numeric strings remain
strings, timestamps and trade IDs remain integers, and output owns its storage.
The supplied baseline defines decoding behavior, including duplicate JSON fields.
This is a teaching workload, not a representative production corpus.

`evaluate` copies only candidate source into a fresh module and restores the
common checks and contract. It runs the checks, then five benchmark samples of
the candidate and current winner. All shell commands and evaluations share one
execution lane to prevent overlapping benchmarks. A different candidate replaces
the winner only when its median time improves by more than 3 percent. An
identical source hash cannot win again through timing noise.

This ranking rule does not direct research. The agents choose what to try and
use the log to decide what to adopt. Passing the fixed checks is evidence, not
proof of correctness on all inputs. Host noise still affects measurements.

At exit, `winner/` contains a runnable Go module whose main imports the selected
`research/parser` package. `selection.json` identifies the selected experiment.
If nothing clears the gates, the module uses the baseline. This is a source
selection for the next build; it does not change imports in a running Go binary.

## Knowing when to stop

An agent calls `status(exhausted=true, body=reason)` when it has no useful ideas
left. Its latest status replaces its earlier status. Starting another `try`
clears exhaustion, so new peer findings can restart exploration. Agents can
also revise their status explicitly. No proposals or votes are needed.

Normal completion requires every agent to see all members exhausted, followed
by a final gossip drain that checks converged replicas and the latest statuses.
Exhaustion describes the agents' judgment; it does not prove an optimal parser.
A time, turn, or context limit ends with a nonzero status. The log, private
workspaces, and best candidate are still saved after a research limit.

## Run and inspect

From the repository root, with Go 1.27.1 and an existing Codex CLI login:

```sh
go run ./examples/autoresearch
```

The example uses `gpt-6-luna` through a small Responses/SSE client that reads
`~/.codex/auth.json` without modifying it. Use `-auth` for another credential
file. Defaults are ten agents, 20 minutes, and 5,000 model turns shared across
the swarm. Override with `-agents`, `-model`, `-duration`, `-turns`, or `-goal`.
`-goal` changes the research prompt; the parser contract and workload stay fixed.
Use `-resume /path/to/previous-run` to continue with its notebook, parser packages,
and selected candidate in a new run directory. Keep the same agent count. Each
continuation has a fresh turn/time budget and model conversations; research
knowledge comes back through the gossiped notebook. A changed workload resets
winner selection.

Interrupted model response streams retry up to four attempts. Partial tool calls
are discarded until a complete response arrives.

Commands run as your local user in the requesting agent's directory, with
bounded duration/output and filtered credential variables. This is not a sandbox.

Runs live in `.agi.local/autoresearch/<timestamp>/`:

- `run.json`: settings.
- `events.jsonl`: the experiment notebook, including shared source and evidence.
- `agent-*/`: private modules.
- `boards/`: final local replica logs.
- `winner/`: the selected runnable module.

To use the selected parser, run `go run .` inside `winner/` and supply one JSON
message on standard input. Start reading the harness at `agent.go` (research
loop), `network.go` (gossip ring), and `experiment.go` (checks and selection).
The `workspace/` templates define the common module supplied to every agent.

`go test -race ./examples/autoresearch/...` exercises ten-peer experiment
sharing, candidate adoption, independent reproduction, failed attempts,
revisable exhaustion, and budget exits with a deterministic model, without
calling the provider.
