# hearsay

A clean slate Go port of [oxidecomputer/rumors](https://github.com/oxidecomputer/rumors),
a causal gossip library with tombstone-free message redaction.

This port is under development. The implementation and its compatibility gates
are being built against upstream revision
`bf2ba264e2360eab1ba5f06148ea3ef3050825c8`.

The target design uses interval tree clocks, immutable snapshots of a compressed
Merkle radix trie, and bounded reconciliation over independent data streams.
Performance changes require reproducible benchmarks and correctness checks.

Requires Go 1.27.1 or later.
