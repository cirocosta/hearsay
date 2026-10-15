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

## Required behavior and evidence

| Requirement | Acceptance evidence |
| --- | --- |
| Interval tree clocks | Canonical Party/Version bytes match upstream fixtures. Tick, fork, identity join, causal comparison, lattice operations, rank and causal queries pass independent model and differential tests. Arbitrary natural counts remain exact. |
| Network and lifecycle | Separate seeds reject gossip. Bootstrap transfers a unique identity and full replica; abandoned unsent forks return safely. Mutual bootstrap bails. Retirement distinguishes declined, safely recovered, and uncertain identity transfer. Identity conservation holds under cancellation and overlapping sessions. |
| Payload admission | CBOR round trips equal before committing. Lossy encodings and depth excess fail without mutation. Default depth is 256; the handshake requires exact depth compatibility. A Rust-Go corpus proves matching admission decisions, including upstream Ciborium 0.2.2 recursion accounting. |
| Replica and redaction | Arbitrary gossip schedules, partitions and reunion converge. Absent causally observed leaves remain deleted without per-message tombstones. Concurrent unseen sends survive. Independent flat survivor rules agree with trie join and wire reconciliation. |
| Immutable radix trie | Canonical compressed 256-way trie uses SHA3-256 of canonical version bytes for 32-byte paths and 24-byte comparison hashes. Payloads do not enter hashes. Hash fixtures, insertion-order independence, deletion compression, causal aggregates and retained-snapshot tests pass. |
| Atomic API | Send, redact, bulk operations and scoped batches publish complete commits. Batch error or panic commits nothing. Nested operations retain upstream semantics. Concurrent handles and lifecycle ownership gates pass race and quiescence tests. |
| Snapshots | Constant-time immutable snapshot capture; lookup, count, hash, earliest/latest, iteration, and causal ranges agree with the model and remain isolated from later changes. Held readers do not block mutation. |
| Content observers | Unordered and causal delivery, safe resumable checkpoints, coalesced changes, quiet/end distinctions, and final drain match upstream behavior. Resume may replay but never skips. Causal predecessors arrive first; redactions are not content events. |
| Link contract | Persistent duplex control plus up to 17 independent bounded unidirectional streams per direction; lazy opens and arbitrary arrival order. Conformance passes at one-byte capacity. Cancelled accepts preserve delivery. Stream completion supports reuse. |
| Link session state | One active session per link, epoch labels, stream index validation and poisoned failed/cancelled exchanges. Successful exchanges return reusable transport halves. Idle EOF, truncated preambles and active failures remain distinct. |
| Routed transport | Generic dial/listen router binds per-link tokens and peer addresses, supports concurrent links and stream reuse, bounded queues, fairness, teardown and statistics. Adapter conformance and real socket integration pass. |
| Exact V2 setup | Fixed 30-byte preamble, greeting, tagged causal atoms and completion epilogue match byte fixtures. Network/protocol/settings mismatches fail before descent. Equal frontiers avoid data streams; smaller replica initiates with canonical-byte tie breaking. |
| Streaming protocol | Exact phase-specific frame grammar, separate reply/stream ends, ordered query maps, embedded CBOR leaf runs, alternating-height streams, opening listings and early supplies. Decoder rejects malformed signals, lengths, scope, order and version bounds before unsafe buffering. Rust-Go sessions interoperate. |
| Bounded reconciliation | Per-height bounded queues and minimum progress floor, declared version-size accounting, negotiated minimum supply target, one-record overhang and oversized multi-record rejection. Memory scaling, backpressure and latency tests cover sparse and deep disputes. |
| Session driver | Gossip once and continuous conditional/unconditional policies; suppress self-triggered repeat exchanges. Remote initiation remains responsive. Deadlines bound active exchanges, including partial preambles, while excluding idle time. Concurrent local changes survive commit. |
| Wire observation and stats | Exact complete accepted/read and flushed/written CBOR items, stream order, optional election and exactly one terminal callback after tasks end. Disabled hooks remain cheap. Stats preserve upstream measurement boundaries and typed errors preserve operation/phase/stream context. |
| Bookmark rights | Store local write progress before sharing; durably remove rights before identity donation; suppress reclamation while a fork is reserved. Recovery waits for recorded own writes. Storage failure/cancellation cannot duplicate identities or roll back later durable state. |
| Bookmark format and files | Upstream v6 checksum envelope and ordered retention records pass golden and malformed fixtures. Encoded retention limit defaults to 16 MiB. Atomic file replacement syncs contents and parent directory; cancelled operations cannot reorder stores. Bookmark storage contains no messages. |
| Performance | Reproducible benchmarks cover causal operations, hashes, trie mutations/joins, observer delivery, codecs, and complete sessions across set size, overlap, deletion, payload, RTT, bandwidth and budget. Record baselines, repeated candidate trials, allocations and memory; keep gains only after correctness gates pass. |

The upstream test suites, wire snapshots, property regressions, conformance
suites, and formal streaming model define verification scenarios. Equivalent Go
coverage is required; translating Rust tooling literally is not required.

## Dependency order and parallel work

1. Pin upstream and extract deterministic causal, hash, frame, bookmark and session fixtures.
2. Implement the causal kernel and canonical codecs, with independent oracles.
3. Build the immutable trie, causal deletion join and snapshot queries.
4. Add concurrent replica handles, atomic batches and content observers.
5. Build Link conformance and exact V2 codecs in parallel; follow with routed transport.
6. Implement the local streaming mirror, wire proxy, bounded windows and run batching.
7. Integrate bootstrap, gossip policies, deadlines, retirement and completion ownership.
8. Add bookmark recovery, atomic file storage, wire hooks and session statistics.
9. Prove Rust-Go interoperability, failure behavior, race freedom and workload performance.

Independent lanes can overlap once their shared contracts stabilize. Run
measured hillclimbing on meaningful causal, tree and codec benchmarks as these
kernels become available; retain full-session regression gates before accepting
optimizations that affect reconciliation.

## Current state

The checkout contains initial Go sources for causal clocks (`before/`), a
persistent trie (`internal/tree/`), CBOR structure (`internal/cbor/`), bookmark
storage (`bookmark/`), and memory transport (`link/`). These are active
implementation slices. Source presence alone does not prove correctness,
compatibility, or performance.

The full port is incomplete. No completion claim follows from this document.
Wire interoperability, streaming reconciliation, full lifecycle, observer
semantics, bookmark recovery integration and representative performance gains
must each have direct acceptance evidence before the goal is complete.
