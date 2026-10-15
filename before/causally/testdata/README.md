# Causal query oracle

`query_trace.rs` evaluates the upstream Rust query implementation at
oxidecomputer/rumors commit `bf2ba264e2360eab1ba5f06148ea3ef3050825c8`.
Run it in a Cargo package that depends on the upstream `before` crate.

The generator enumerates nine two-coordinate versions (each count is 0, 1, or 2),
every ordered span between them, and fourteen query forms for each bound pair.
It emits concrete memberships followed by exact span coverage as bytes.
The 51,030-byte output has SHA-256:

```
ad85933cff1c00fd20794c376e35ec15479456f7a45b289ee9176e56582d5554
```

`TestQueryCoverageAgainstExhaustiveLattice` generates the same byte stream in Go
and checks this digest, in addition to checking exhaustive membership directly.
