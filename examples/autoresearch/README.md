# A research swarm connected by Hearsay

Ten `gpt-6-luna` agents receive one goal: make Hyperliquid websocket `trades`
and `l2Book` JSON parsing as fast as possible. They decide the parsing contract,
workload, metrics, division of work, experiments, acceptance gates, and stopping
criteria by exchanging proposals, objections, evidence, and votes.

There are no assigned specialist roles or prewritten parser candidates. The
same small agent loop runs for every member. Each has its own bootstrapped
`Replica[note]`, model conversation, and research directory. Memory links form
a ring; messages and files reach nonadjacent agents through gossip.

See [the collaboration diagrams](collaboration.md) for artifact review and stopping.

## Run

From the repository root, with Go 1.27.1 and an existing Codex CLI login:

```sh
go run ./examples/autoresearch
```

The provider defaults to `gpt-6-luna` and reads `~/.codex/auth.json` without
modifying it. `-auth /path/to/codex-auth.json` also accepts a gx credential file.
Expired credentials require refreshing the login outside this example. The
small Responses/SSE client follows gx's provider code; no agent SDK is needed.

Replace the goal or operational ceilings:

```sh
go run ./examples/autoresearch \
  -agents 10 -model gpt-6-luna -duration 20m -turns 640 \
  -goal 'Optimize Hyperliquid trades and l2Book websocket JSON parsing. Agree on correctness, benchmark methodology, progress checks, and when to stop. Deliver runnable source and independently reproduced evidence. SIMD and assembly are welcome.'
```

The default goal links to Hyperliquid's [WebSocket schemas](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions).
The agents can collect data and choose Go, C, assembly, or another installed
toolchain. Required compilers and data tools depend on their chosen experiments.

## How collaboration works

- `post` publishes findings, counterexamples, questions, and progress.
- `propose` and `vote` negotiate any decision. Agreement requires every member.
- `read_file` and `write_file` work in each agent's own directory.
- `run` executes shell commands and automatically gossips their output/results.
- `share` publishes file contents through Hearsay; `adopt` imports local copies.
- `wait` yields until a replica change or a short timeout.

Only the agent's own replica supplies its model context. There is no shared
in-memory message list or central planner. The logger and final convergence
check observe the run without making research decisions.

All commands share one lane so builds, tests, and benchmark processes cannot
overlap each other. Agents choose the actual measurement protocol. The lane
does not eliminate provider traffic, OS noise, or unrelated host activity;
performance claims still need repeated controls and independent confirmation.

Agents execute generated code as your local user. Research directories and file
tools constrain ordinary edits; command execution is not a security sandbox.
The runner filters credential environment variables, bounds command duration
and output, and terminates command process groups on Unix. It does not provide
a VM or container boundary.

## Results and termination

Runs go under `.agi.local/autoresearch/<timestamp>/` by default:

- `run.json`: goal, model, membership, and operational limits.
- `events.jsonl`: authored Hearsay messages, measurements, and shared artifacts.
- `agent-00/` through `agent-09/`: independent research files.
- `boards/`: final local message sets for inspecting agreement and convergence.

A normal exit requires all agents to endorse the same `stop` proposal, observe
that unanimous agreement locally, and drain gossip to converged boards. The
latest vote on a topic replaces the previous vote, allowing corrections. The
runner rejects differing or withdrawn stop decisions during final convergence.
This is a cooperative fixed-membership voting convention, not a general
distributed consensus or failure-recovery protocol.

Time, turn, and context ceilings end the run with a nonzero status. They never
count as negotiated completion or proof of the fastest possible parser. The
agents' evidence determines which workload-specific gains they can support.
The turn budget is shared by the whole swarm; transient provider errors get at
most four request attempts per turn with bounded backoff.

`go test -race ./examples/autoresearch/...` exercises ten real Hearsay peers
with a deterministic model: a rejected/revised plan, evaluation agreement,
artifact exchange, independent command checks, and unanimous termination.
Those tests need no credentials or provider access and make no parser speed claim.
