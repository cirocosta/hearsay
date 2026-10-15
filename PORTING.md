# Port requirements and acceptance gates

Hearsay targets a complete, clean slate Go port of
[oxidecomputer/rumors](https://github.com/oxidecomputer/rumors), pinned to upstream
revision `bf2ba264e2360eab1ba5f06148ea3ef3050825c8`.

The target includes causal message replication with tombstone-free redaction,
compact recyclable identities, immutable snapshots, bounded streaming
reconciliation, lifecycle operations, transports, observers, and bookmarks.
A partial in-process replica or full-state exchange does not satisfy this target.
Go APIs may express ownership and cancellation differently, but must preserve
the upstream guarantees.

## Naming and compatibility

Hearsay's shared writable handle is `Replica[T]`. `Peer.IntoReplica` transfers
exclusive identity ownership into that handle; `TryIntoPeer` and `IntoPeer`
recover it. Examples, diagnostics, and the `hearsay-interop` verifier use Hearsay's
names. The original `Rumors` Go type and `IntoRumors` method have been removed;
callers must use `Replica` and `IntoReplica`.

The V2 preamble still contains the literal `rumors`. That text identifies the
wire protocol and must remain byte-compatible. The Rust dependency, pinned clone
path, fixture generators, and upstream attribution also retain their actual
names. The Go API rename does not change wire or bookmark formats.

## Required behavior and evidence

| Requirement                | Acceptance evidence                                                                                                                                                                                                                                                                                                        |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Interval tree clocks       | Canonical Party/Version bytes match upstream fixtures. Tick, fork, identity join, causal comparison, lattice operations, rank and causal queries pass independent model and differential tests. Arbitrary natural counts remain exact.                                                                                     |
| Network and lifecycle      | Separate seeds reject gossip. Bootstrap transfers a unique identity and full replica; abandoned unsent forks return safely. Mutual bootstrap bails. Retirement distinguishes declined, safely recovered, and uncertain identity transfer. Identity conservation holds under cancellation and overlapping sessions.         |
| Payload admission          | CBOR round trips equal before committing. Lossy encodings and depth excess fail without mutation. Default depth is 256; the handshake requires exact depth compatibility. A Rust-Go corpus proves matching admission decisions, including upstream Ciborium 0.2.2 recursion accounting.                                    |
| Replica and redaction      | Arbitrary gossip schedules, partitions and reunion converge. Absent causally observed leaves remain deleted without per-message tombstones. Concurrent unseen sends survive. Independent flat survivor rules agree with trie join and wire reconciliation.                                                                 |
| Immutable radix trie       | Canonical compressed 256-way trie uses SHA3-256 of canonical version bytes for 32-byte paths and 24-byte comparison hashes. Payloads do not enter hashes. Hash fixtures, insertion-order independence, deletion compression, causal aggregates and retained-snapshot tests pass.                                           |
| Atomic API                 | Send, redact, bulk operations and scoped batches publish complete commits. Batch error or panic commits nothing. Nested operations retain upstream semantics. Concurrent handles and lifecycle ownership gates pass race and quiescence tests.                                                                             |
| Snapshots                  | Constant-time immutable snapshot capture; lookup, count, hash, earliest/latest, iteration, and causal ranges agree with the model and remain isolated from later changes. Held readers do not block mutation.                                                                                                              |
| Content observers          | Unordered and causal delivery, safe resumable checkpoints, coalesced changes, quiet/end distinctions, and final drain match upstream behavior. Resume may replay but never skips. Causal predecessors arrive first; redactions are not content events.                                                                     |
| Link contract              | Persistent duplex control plus up to 17 independent bounded unidirectional streams per direction; lazy opens and arbitrary arrival order. Conformance passes at one-byte capacity. Cancelled accepts preserve delivery. Stream completion supports reuse.                                                                  |
| Link session state         | One active session per link, epoch labels, stream index validation and poisoned failed/cancelled exchanges. Successful exchanges return reusable transport halves. Idle EOF, truncated preambles and active failures remain distinct.                                                                                      |
| Routed transport           | Generic dial/listen router binds per-link tokens and peer addresses, supports concurrent links and stream reuse, bounded queues, fairness, teardown and statistics. Adapter conformance and real socket integration pass.                                                                                                  |
| Exact V2 setup             | Fixed 30-byte preamble, greeting, tagged causal atoms and completion epilogue match byte fixtures. Network/protocol/settings mismatches fail before descent. Equal frontiers avoid data streams; smaller replica initiates with canonical-byte tie breaking.                                                               |
| Streaming protocol         | Exact phase-specific frame grammar, separate reply/stream ends, ordered query maps, embedded CBOR leaf runs, alternating-height streams, opening listings and early supplies. Decoder rejects malformed signals, lengths, scope, order and version bounds before unsafe buffering. Rust-Go sessions interoperate.          |
| Bounded reconciliation     | Per-height bounded queues and minimum progress floor, declared version-size accounting, negotiated minimum supply target, one-record overhang and oversized multi-record rejection. Memory scaling, backpressure and latency tests cover sparse and deep disputes.                                                         |
| Session driver             | Gossip once and continuous conditional/unconditional policies; suppress self-triggered repeat exchanges. Remote initiation remains responsive. Deadlines bound active exchanges, including partial preambles, while excluding idle time. Concurrent local changes survive commit.                                          |
| Wire observation and stats | Exact complete accepted/read and flushed/written CBOR items, stream order, optional election and exactly one terminal callback after tasks end. Disabled hooks remain cheap. Stats preserve upstream measurement boundaries and typed errors preserve operation/phase/stream context.                                      |
| Bookmark rights            | Store local write progress before sharing; durably remove rights before identity donation; suppress reclamation while a fork is reserved. Recovery waits for recorded own writes. Storage failure/cancellation cannot duplicate identities or roll back later durable state.                                               |
| Bookmark format and files  | Upstream v6 checksum envelope and ordered retention records pass golden and malformed fixtures. Encoded retention limit defaults to 16 MiB. Atomic file replacement syncs contents and parent directory; cancelled operations cannot reorder stores. Bookmark storage contains no messages.                                |
| Performance                | Reproducible benchmarks cover causal operations, hashes, trie mutations/joins, observer delivery, codecs, and complete sessions across set size, overlap, deletion, payload, RTT, bandwidth and budget. Record baselines, repeated candidate trials, allocations and memory; keep gains only after correctness gates pass. |

The upstream test suites, wire snapshots, property regressions, conformance
suites, and formal streaming model define verification scenarios. Equivalent Go
coverage is required; translating Rust tooling literally is not required.

## Dependency order and parallel work

1. Pin upstream and extract deterministic causal, hash, frame, bookmark and session fixtures.
1. Implement the causal kernel and canonical codecs, with independent oracles.
1. Build the immutable trie, causal deletion join and snapshot queries.
1. Add concurrent replica handles, atomic batches and content observers.
1. Build Link conformance and exact V2 codecs in parallel; follow with routed transport.
1. Implement the local streaming mirror, wire proxy, bounded windows and run batching.
1. Integrate bootstrap, gossip policies, deadlines, retirement and completion ownership.
1. Add bookmark recovery, atomic file storage, wire hooks and session statistics.
1. Prove Rust-Go interoperability, failure behavior, race freedom and workload performance.

Independent lanes can overlap once their shared contracts stabilize. Run
measured hillclimbing on meaningful causal, tree and codec benchmarks as these
kernels become available; retain full-session regression gates before accepting
optimizations that affect reconciliation.

## Current state

The core, transport, lifecycle, and recovery implementations are present. The
following checks provide direct evidence rather than relying on source presence:

- `before/testdata` records an exact upstream clock replay. Wire tests compare
  canonical bytes and exhaustive short-input acceptance. Causal query fixtures
  compare 51,030 Rust membership and coverage verdicts.
- `internal/tree/oracle_test.go` compares joins against a separate flat survivor
  model across send, redact, partition, and reunion schedules. Snapshot, logical
  scope, hash, compression, and iterator tests cover retained immutable state.
- `internal/reconcile` tests exercise bounded queues, independent writer pumps,
  one-byte streams, malformed replies, causal deletion, and final protocol
  stages at genuine leaf addresses. Honest full sessions cannot practically
  generate 31-byte SHA3 prefix collisions; final-level tests enter the existing
  stage interfaces without changing hashing.
- Root tests cover local ownership, atomic batches, observer checkpoints,
  bootstrap, retirement, concurrent mutation, deadlines, inherited policies,
  bookmark write ordering, recovery, and cancellation. `testdata/batch_semantics`
  records upstream final-effect behavior for repeated and canceled actions.
- Public external-package tests check typed mismatch operands, error provenance,
  operation/phase/directed-stream context, and preservation of the first causal
  write or flush failure. Wire observation and statistics tests verify exact
  item boundaries, data-only counters, elapsed time, and terminal callbacks.
- `conformance` supplies reusable link and bookmark checks. `link/routed` tests
  real sockets, stream completion, cancellation callback ownership, reuse,
  bounded admission, and teardown.
- `interop/run.sh` runs both provider languages against the pinned Rust crate.
  It checks large payloads, concurrent sends, cross-peer and transitive deletion,
  minimum budgets, data-bearing epoch wrap, READY reuse, retirement, and
  bootstrap again. Its source, compiler pin, and dependency lock are tracked;
  ordinary Go tests do not require Rust or network access.
- [PERFORMANCE.md](PERFORMANCE.md) records retained and rejected trials, repeated
  measurements, allocation changes, workload limits, and reproducible commands.
- Calibrated full-session fixtures cover RTT, bandwidth, overlap, payloads, and
  budgets. Independent byte-credit/FIFO tests cover 307,200 operations. Opt-in
  queue and session memory diagnostics distinguish queue ownership, result
  custody, transport storage, and the progress floor from total resident memory.

The final requirements audit found no remaining blocker. On 2026-10-14,
`make check` passed all Go tests, race checks, vet, and lint. The real Rust
matrix passed default, each endpoint at minimum, both endpoints at minimum with
the Go race detector, Go budgets of 16 MiB and 16 KiB (the latter with race),
and empty providers. All seven cases ran both provider languages and the full
lifecycle, 270 sessions with data-bearing epoch wrap, and READY reuse with no
fresh data dials. Tracked Go and interoperability source checksums remained
identical throughout that final matrix.

These checks establish the recorded acceptance evidence; they do not prove
equivalence for every possible schedule or payload type. Custom codecs must
implement their typed recursion contract. Memory and performance limits are
documented in [PERFORMANCE.md](PERFORMANCE.md). The requirements above remain
the regression contract for later changes.
