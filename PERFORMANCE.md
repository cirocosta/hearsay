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
before `23d7a95`; keep all benchmark fixtures identical. Raw local samples,
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
