# Batch history oracle

`batch_semantics.txt` records exact canonical frontier bytes, live counts, and
sorted live version bytes from the original Rust `rumors` crate at revision
`bf2ba264e2360eab1ba5f06148ea3ef3050825c8`. A dash denotes an empty version set.
`batch_semantics.rs` is the generator. Its payload type differs from Go's byte
codec; payloads do not enter version addresses or causal history.

Run the generator in a Cargo package with `rumors` and `before` path dependencies
pointing to that pinned checkout and its `crates/before` directory. Use Rust
1.97.1, matching the upstream toolchain. Save stdout as `batch_semantics.txt`.
The Go replay is `TestBatchUpstreamPerKeyHistoryFixtures`.

The cases distinguish per-key batch semantics from sequential independent
commits. Repeated redactions of a key that existed before the batch retain that
key's final action timestamp. A key that starts and ends absent contributes no
history. Cancelled actions still consume ordinal ticks when later effective
actions remain in the same batch.
