# Hearsay

Hearsay is a Go library for replicating messages between peers through causal
gossip. Peers exchange differences and converge after reconnecting. Redaction
removes messages without retaining a tombstone for each deleted message.

## Features

- Causal versions distinguish concurrent messages from observed deletions.
- Immutable snapshots support lookup, iteration, and causal queries.
- Atomic sends, redactions, and batches publish consistent changes.
- Resumable observers deliver messages in unordered or causal order.
- Bounded streaming reconciliation works over memory links or routed TCP.
- Bootstrap, retirement, and durable bookmarks manage peer identity ownership.
- Continuous gossip, active-session deadlines, and wire observers support services.

## Quick start

Requires Go 1.27.1 or later. Import `github.com/cirocosta/hearsay`.

```go
peer, err := hearsay.Seed(hearsay.BytesCodec())
if err != nil {
    return err
}
replica, err := peer.IntoReplica()
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
fmt.Println(snapshot.Len(), replica.Snapshot().Len()) // 1 0
```

`Peer[T]` owns an identity exclusively. `IntoReplica` consumes it and returns a
shared writable `Replica[T]`. Use `Clone` for independently closable handles;
ordinary Go copies share a lifetime token. `TryIntoPeer` or `IntoPeer` recovers
exclusive ownership after the other handles are released.

To add a member, call `Bootstrap(codec).Join` while an existing member runs
`GossipOnce` on the other end of a managed link. Independently seeded peers
belong to different networks. The executable [example](example_test.go) shows
bootstrap, message exchange, and redaction.

`BytesCodec` and `JSONCodec` use CBOR byte-string envelopes. Custom codecs must
provide `Encode` and `DecodeBounded`, including typed recursion enforcement.
Equality defaults to `reflect.DeepEqual`; override `Equal` when needed.

`SynchronizationSettings.SyncMemoryBudget` targets pending comparison metadata.
It permits a one-item progress floor and does not cap total process memory.
Payloads, snapshots, replies, and transport buffers have separate ownership.

## Development

```sh
make check    # tests, race detector, vet, and golangci-lint v2
make bench    # local and network-model benchmarks
```

See [the shared Kanban worker example](examples/kanban/README.md) for a replicated
member list and autonomous demo jobs over sparse gossip connections.

Hearsay is inspired by Oxide's [rumors](https://github.com/oxidecomputer/rumors).
