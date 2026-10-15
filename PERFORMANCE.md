# Performance experiments

Keep correctness fixtures fixed while comparing candidates. Record the revision,
compiler, CPU affinity, runtime worker count, platform, allocations, and repeated
raw samples. Alternate variants and confirm promising results independently.
Rejected trials remain in the local `.agi.local/perf/` research archive.

## CBOR head reads

On an Intel i9-9900K, Linux amd64, Go 1.27.1, CPU 2, and `GOMAXPROCS=1`, an
explicit `ReadByteHead` path removed the temporary buffer allocation for callers
that already own a byte reader. The existing `ReadHead(io.Reader)` path remains
unchanged. Independent confirmation medians over four samples per case:

| Encoded head bytes | ReadHead ns/op | ReadByteHead ns/op | Reduction |
| ------------------ | -------------: | -----------------: | --------: |
| 1                  |         25.415 |              4.922 |     80.6% |
| 2                  |         31.765 |              7.983 |     74.9% |
| 3                  |         32.255 |             10.335 |     68.0% |
| 5                  |         33.400 |             14.630 |     56.2% |
| 9                  |         33.170 |             21.830 |     34.2% |

Allocation changed from 16 bytes and one allocation per head to zero. This is a
kernel measurement, not a complete gossip throughput claim. Governor and unrelated
host load were not controlled; CPU performance counters were unavailable.

The first candidate automatically detected byte readers inside `ReadHead`. It
was rejected because the fallback path regressed by about 9-12% on most widths.

Build the fixed benchmark binary with `go test -c ./internal/cbor`, then alternate
these commands with no other builds or tests running:

```sh
GOMAXPROCS=1 taskset -c 2 ./cbor.test -test.run='^$' -test.bench='^BenchmarkReadHead$/^bytes.Reader$' -test.benchmem -test.benchtime=150ms
GOMAXPROCS=1 taskset -c 2 ./cbor.test -test.run='^$' -test.bench='^BenchmarkReadByteHead$' -test.benchmem -test.benchtime=150ms
```

Use an allowed CPU on your own host. Keep a buffered reader alive across session
boundaries so read-ahead bytes remain available.

## Reconciliation queues

Every session previously allocated all 34 queue buffers at their maximum
negotiated width. The retained implementation grows wider queues with actual
occupancy and keeps native channels for one-slot windows. Queue capacities,
memory pricing, wire bytes, and stream counts remain the same.

The same host and affinity settings apply. Fixed binaries used identical
fixtures: 100 or 10,000 initial messages, 1 or 100 sends or redactions, 32-byte
or 1-KiB payloads, and zero or default 512-MiB budgets. Four alternating samples
per variant screened the change. Four reversed-order samples independently
confirmed the 32-byte cases; four alternating samples guarded the 1-KiB cases.

At the default budget, elapsed time improved 9-65% for 32-byte payloads and
6-67% for 1-KiB payloads. Allocation fell by about 2.7 MB per paired exchange.
Selected confirmation medians for 32-byte payloads:

| Initial messages | New sends | Eager us/op | Retained us/op | Eager B/op | Retained B/op |
| ---------------: | --------: | ----------: | -------------: | ---------: | ------------: |
|              100 |         1 |       422.0 |          171.1 |  2,812,889 |       116,515 |
|           10,000 |         1 |     1,385.4 |          488.9 |  2,971,689 |       275,798 |
|           10,000 |       100 |     8,416.2 |        7,622.0 |  5,229,004 |     2,555,137 |

At the minimum budget, timing ranged from 3.6% slower to 2.8% faster across
the tested cases. The first candidate used lazy rings even for one-slot
windows; it added 34 allocations and about 4 KB there. Keeping native channels
at that floor removed that allocation penalty. These results support the
default-budget allocation improvement; they do not establish a universal
throughput gain.

Build with `go test -c ./internal/reconcile`. The `BenchmarkRun` hierarchy
records initial set size, divergence, operation, payload size, and budget.
Run fixed binaries with `GOMAXPROCS=1 taskset -c 2`, `-test.benchmem`, and
`-test.benchtime=75ms`. Compare the retained source against the eager queues
before the lazy-storage change; keep all benchmark fixtures identical. Raw local samples,
source overlays, environment records, and binary hashes are retained under
`.agi.local/perf/`.

## Routed connection reuse

Completed data streams now reuse TCP connections after the remote READY
handshake. The pool retains at most 17 connections in each direction. `Open`
dials a fresh connection when none is ready. Explicit completion and joined
cancellation callbacks prevent an old stream from closing a later generation.

Fixed loopback binaries used the same `BenchmarkRoutedSessions` fixture, CPU,
and runtime settings. Each iteration sends 1 or 17 streams, then exchanges a
control acknowledgment. Four alternating samples screened the first candidate;
four reversed samples confirmed it. Independent review then found a
cancellation callback race. After that repair, four further alternating samples
compared the final candidate directly with the original implementation:

| Streams | Bytes per stream | Fresh us/op | Pooled us/op | Fresh B/op | Pooled B/op |
| ------: | ---------------: | ----------: | -----------: | ---------: | ----------: |
|       1 |              128 |      99.462 |       35.571 |      2,560 |       1,501 |
|       1 |            4,096 |     100.627 |       35.382 |      2,560 |       1,500 |
|      17 |              128 |   1,444.674 |      447.285 |     43,460 |      26,872 |
|      17 |            4,096 |   1,445.679 |      469.676 |     43,463 |      26,894 |

The final candidate reduces elapsed time by 64-69% and allocated bytes by
38-41% in these workloads. This measures repeated transport streams over
loopback, not complete gossip over a wide-area network. Disable reuse with
`routed.Config.DisablePooling` for diagnosis; that path retains the cancellation
repair and is not allocation-identical to the old implementation.

Race tests cover delayed half completion, canceled reuse, withheld READY,
shutdown, and repeated public bootstrap/gossip. The real Rust verifier proves
READY reuse and data-bearing epoch wrap in both provider directions. Build
with `go test -c ./link/routed`; run `BenchmarkRoutedSessions` with the settings
above and `-test.benchtime=150ms`.

## Clock growth and fill

A CPU and allocation profile of a fragmented identity attributed 54.5% of
sampled CPU time and 69.2% of allocated objects to growth-route discovery.
Packed route indices replace one heap object per visited identity branch.
Unchanged fill subtrees also retain their pointers, which removes redundant
leaf construction and equality walks.

The first arena candidate improved fragmented clocks but slowed seed ticks by
12.2%; it was rejected. Moving arena storage into a separate partial-identity
helper restored the seed path. A separate trial then added fill sharing. The
three variants used identical fixtures and four balanced screen samples each.
The final candidate was compared directly with the original implementation in
eight further samples per variant, one second per case, with the same pinned
CPU and quiet-host rules above.

| Tick topology          | Original ns/op | Retained ns/op | Original B/op | Retained B/op | Original allocs/op | Retained allocs/op |
| ---------------------- | -------------: | -------------: | ------------: | ------------: | -----------------: | -----------------: |
| Seed                   |         30.270 |         28.065 |            48 |            48 |                  1 |                  1 |
| Unary depth 1          |        146.400 |         75.795 |           208 |            96 |                  5 |                  2 |
| Unary depth 8          |        757.350 |        334.650 |           992 |           432 |                 26 |                  9 |
| Unary depth 64         |        6,931.5 |        3,249.5 |         7,264 |         3,120 |                194 |                 65 |
| Unary depth 256        |         28,787 |         14,310 |        28,768 |        15,408 |                770 |                259 |
| Fragmented 16 regions  |          1,287 |         389.15 |         1,312 |           288 |                 48 |                  6 |
| Fragmented 128 regions |         10,069 |          2,429 |         9,456 |         3,504 |                367 |                 11 |

Seed ticks improve by 7.3%, unary cases by 48-56%, and fragmented cases by
70-76%. At 10,000 resident messages, tree insertion and one-insert joins improve
by 0.5% and 0.3%. The one-redaction join guard slows by 1.17% (`p = .021`),
below the predeclared 3% guard limit. Its allocation counts remain unchanged.
These are kernel results, not a complete-session speedup claim.

Correctness gates include the pinned Rust clock replay, wide counts, deep
decoded atoms, tree/race tests, and an independent two-coordinate fill/grow
oracle. Build `go test -c ./before` and `go test -c ./internal/tree`; compare
`BenchmarkTickSeed`, `BenchmarkTickTopology`, `BenchmarkInsert`, and the
`BenchmarkJoin/one-insert` and `one-redaction` guards. Source archives, binary
hashes, raw trials, and benchstat output live under `.agi.local/perf/clocks/`.

## Byte-credit comparison windows

Comparison queues now charge their allocated slot capacity and detached child
fans against one seventeenth of `SyncMemoryBudget`. Immutable opening nodes,
clocks, and payloads remain shared. A depth's count ceiling also reflects the
number of possible occupied prefixes. An empty queue always admits one item for
progress. Growth and shrink use geometric steps to avoid repeated full-ring
copies. This replaces conservative full-fan pricing and the fixed 4,096 ceiling.

The calibrated test transport models shared directional bandwidth, absolute
packet delivery times, independent stream credit, and joined delivery workers.
These are simulated network results. Fixed matching binaries used the same
100,000 shared messages, 10,000 new messages per side, and 1-KiB payloads. Each
iteration includes negotiation, reconciliation, confirmed completion, and link
shutdown; fixture construction and the independent Join oracle are outside
timing. CPU affinity and runtime settings match the experiments above.

One screen sample and two reversed-order confirmation samples per variant gave
these medians. Both confirmation pairs reproduced the primary network gains:

| Link and budget        | Original s/op | Retained s/op | Time change | Allocation change |
| ---------------------- | ------------: | ------------: | ----------: | ----------------: |
| 100 ms, 1 Gbps, 16 MiB |        28.688 |         2.048 |     -92.86% |            +2.68% |
| 1 ms, 100 Gbps, 1 MiB  |         8.029 |         1.711 |     -78.70% |            +0.15% |
| CPU, zero budget       |         1.681 |         1.686 |      +0.29% |             0.00% |
| CPU, default 512 MiB   |         1.708 |         1.725 |      +1.03% |            +4.18% |

Every main sample sends exactly 23,768,676 data bytes. The 16-MiB scope-count
ceiling changes from 29 to 12,336; the 1-MiB ceiling changes from 1 to 771.
`WindowGranted` reports that ceiling, not occupancy; byte credit can stall sooner.
The zero-budget path retains native one-slot channels and identical allocation.
Default CPU allocation increases by 14.9 MB/op as more work overlaps.

Seven additional screen guards cover default-budget WAN/LAN, small floor
fixtures, sparse differences, disjoint sets, redactions, and 16-KiB payloads.
All preserve wire bytes and showed flat or improved latency in one sample each;
these are guards, not confirmed speedup claims. Redactions have the largest
allocation increase: 9.37%, or 16.4 MB/op. Transport peaks also rise with overlap:
the disjoint case uses 9.0 MB versus 1.36 MB; the bulk case uses 13.7 MB versus
11.1 MB. Transport storage is a separate term.

`SyncMemoryBudget` targets comparison metadata, not total resident memory.
Nonempty queues charge retained slot high-water plus live fan capacity. An empty
queue has a one-item progress exception. During replacement, old and new arrays
can coexist; reachable queue storage has a conservative two-share plus one-item
envelope. Allocator rounding, GC headroom, shared opening trees, complete-reply
actions, encoded runs, result custody, observers, and transport are separate.

An artificial default-budget saturation test at 100,000 messages holds about
220.8 MB of queue heap, including 125.2 MB of slots and 84.0 MB of fans; drained
queues retain 125.3 MB of charged high-water slots. The old count limit held
about 2.29 MB in that diagnostic. The new window deliberately uses more of the
configured budget. This test fills queues to their individual barriers; it does
not predict ordinary session occupancy.

A separate full-session diagnostic forces GC during sampling, which changes
scheduling. It uses 32-byte payloads on a 2-ms/1-Gbps model, unlike the timing
fixtures above. At 110,000 opening messages and a 16-MiB budget it observes 17.35 MB
incremental live heap, including 11.39 MB of final result custody. The sampled
excess above both endpoint baselines is 5.96 MB; after result release the delta
is about 4.7 KB. These are sampled lower bounds on transients, not natural peak
measurements or a hard memory guarantee. Enable the opt-in memory tests
with `HEARSAY_WINDOW_MEMORY=1`.

The first credit candidate repeatedly trimmed growth to one affordable extra
slot. Its profile attributed 819 MB to resizing and total allocation reached
1.21-1.26 GB/op. A half-share heuristic did not repair it. Both trials were
rejected. Final geometric resizing passed an independent 307,200-operation
FIFO/credit/reference oracle, repeated races, and real Rust sessions.

Reproduce with `go test -c .` and `BenchmarkSessionNetwork` using fixed binaries
and the settings above. Compare only the engine/queue variants with identical
fixtures. The regular matrix uses smaller long-RTT floor fixtures to stay within
its session deadline. Raw samples, binary hashes, source overlays, memory logs,
and the full eleven-case comparison remain in `.agi.local/perf/`, with the
analysis in `WINDOW_RESULTS.md`.
