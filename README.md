# hearsay

A clean slate Go port of [oxidecomputer/rumors](https://github.com/oxidecomputer/rumors),
a causal gossip library with tombstone-free message redaction.

The implementation and its compatibility gates target upstream revision
`bf2ba264e2360eab1ba5f06148ea3ef3050825c8`.

The target design uses interval tree clocks, immutable snapshots of a compressed
Merkle radix trie, and bounded reconciliation over independent data streams.
Performance changes require reproducible benchmarks and correctness checks.

Requires Go 1.27.1 or later.

The current implementation includes interval tree clocks, a persistent Merkle
trie, atomic local batches and snapshots, resumable message observers, independent
memory and routed socket streams, bounded V2 reconciliation, bootstrap and
retirement, continuous gossip, active session deadlines, wire observers, and
durable bookmark recovery. See
[PORTING.md](PORTING.md) for the complete acceptance gates.

Run `make check` for tests, race checks, vet, and lint. Lint and formatting require
`golangci-lint` v2. Run `make bench` for the benchmark suite. Performance comparisons
must run without concurrent builds or tests; pin both variants to the same CPU.
Run `make interop` for the explicit Rust-Go verifier. It needs Rust and initial
network access; ordinary Go tests do not. See [interop/README.md](interop/README.md)
for the configuration matrix and [PERFORMANCE.md](PERFORMANCE.md) for measured
gains and tradeoffs.

The regular network benchmark matrix keeps each session within a one-minute
deadline. It tests the one-slot floor on a 1,000-message set with 100 new
messages per side over a 100 ms link. The 100,000-message case retains floor
coverage on CPU and 1 ms links. Large sets at the floor over 20-100 ms links
are stress workloads and are excluded from the regular matrix.

For a local replica:

```go
peer, err := hearsay.Seed(hearsay.BytesCodec())
if err != nil {
    return err
}
replica, err := peer.IntoRumors()
if err != nil {
    return err
}
defer replica.Close()

version, err := replica.Send([]byte("the meeting is at noon"))
if err != nil {
    return err
}
snapshot := replica.Snapshot()
if err := replica.Redact(version); err != nil {
    return err
}
// The retained snapshot still contains the message.
_ = snapshot
```

`Clone` creates another independently closable writable handle. Ordinary Go
copies share a lifetime token. Recovering a `Peer` requires releasing the other
handles. Snapshots and observers do not retain identity ownership.

Create another member with `Bootstrap(codec).Join(ctx, managedLink)` while an
existing member runs `GossipOnce` on the other link half. Independently seeded
peers belong to different networks. See the executable
[bootstrap and redaction example](example_test.go).

Custom codecs must provide `Encode` and `DecodeBounded`. Equality defaults to
`reflect.DeepEqual`; provide `Equal` when the payload needs different semantics.
`DecodeBounded` must enforce the payload type's decoding recursion budget;
generic CBOR nesting depth does not match Rust's typed decoding rules. The
built-in `BytesCodec` and `JSONCodec` use CBOR byte-string envelopes.
