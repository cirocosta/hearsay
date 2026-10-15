# Clock trace oracle

`clock_trace.txt` records 300 deterministic operations from the original Rust
`before` crate at oxidecomputer/rumors commit
`bf2ba264e2360eab1ba5f06148ea3ef3050825c8`.

Each line gives an operation, participant indices, and the exact resulting
canonical clock bytes in hexadecimal. `fork` appends a participant. `join`
removes the last participant and transfers its identity and history to the given
participant. `absorb` merges history from the second participant into the first.

`clock_trace.rs` is the generator. Run it in a Cargo package that depends on the
upstream `before` crate and save standard output as `clock_trace.txt`.
`TestUpstreamClockTrace` replays every operation in Go and compares the canonical
clock bytes after every step.
