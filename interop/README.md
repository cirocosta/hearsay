# Rust-Go interoperability verifier

Run the explicit verifier from the repository root:

```sh
./interop/run.sh
```

Requirements: Git, Go matching `go.mod`, Rustup/Cargo with Rust 1.97.1, and
network access for the initial clone and Cargo
dependencies. Ordinary `go test ./...` does not invoke Cargo or network access.

The script creates `.agi.local/repositories/rumors` only when absent. It pins
upstream commit `bf2ba264e2360eab1ba5f06148ea3ef3050825c8`. An existing clone must
already be clean and at that commit; the script exits without changing it if
its commit or tracked contents differ. The Rust CLI uses the real upstream
`rumors` crate through path dependencies. Its lock file preserves dependency
versions from the upstream lock, including `ciborium` 0.2.2. The Rust toolchain
file pins upstream's compiler version 1.97.1.

Build artifacts stay under `.agi.local/interop-target`. The runner exchanges
V2 sessions over loopback TCP using the public routed adapters on both sides.
The Rust payload type is `bytes::Bytes`, whose serde CBOR byte-string encoding
matches Go `BytesCodec`.

## Scenarios

Both languages act as bootstrap provider, and both initiate routed links.
Each direction verifies:

- Bootstrap from a provider with existing data.
- Forty independent concurrent sends per side.
- One 1-MiB payload per side, exceeding the target frame size.
- Concurrent redactions of messages originating on the opposite side.
- Transitive deletion through a Rust-Go-Rust relay.
- 270 consecutive sessions on one link, with sparse sends and fresh data at
  epochs 254, 255, 0, 1, and 2. The verifier asserts outgoing data streams at
  wrapped epochs 0, 1, and 2 and fewer fresh Go TCP dials than outgoing streams,
  proving READY-confirmed data connection reuse against Rust.
- Mutual retirement decline and continued link use.
- Retirement, survivor send, and bootstrap again over the same link.

The runner compares canonical version bytes, payload bytes, and causal
frontiers after each phase. Every Rust command has a 20-second observation
deadline; every Go scenario has a 90-second context. These limits detect stalls;
the verifier does not measure performance. TCP windows are not restricted to
one byte; the repository's transport conformance tests cover bounded streams.

## Configuration matrix

Environment variables apply to `./interop/run.sh`:

| Variable | Effect |
| --- | --- |
| `HEARSAY_INTEROP_MINIMAL=1` | Go memory budget 1, target message size 0 |
| `RUST_INTEROP_MINIMAL=1` | Rust memory budget 1, target message size 0 |
| `HEARSAY_INTEROP_EMPTY=1` | Start with an empty bootstrap provider |
| `HEARSAY_INTEROP_RACE=1` | Run the Go participant under the race detector |
| `HEARSAY_INTEROP_PROVIDER=go` or `rust` | Run one provider direction |
| `HEARSAY_INTEROP_DEBUG=1` | Write a goroutine dump after five seconds |
| `HEARSAY_INTEROP_STACK_FILE=/path` | Override the dump path |

A complete matrix uses default settings, each minimum-budget direction,
both endpoints at minimum budget, and empty providers. For example:

```sh
HEARSAY_INTEROP_MINIMAL=1 ./interop/run.sh
RUST_INTEROP_MINIMAL=1 ./interop/run.sh
HEARSAY_INTEROP_MINIMAL=1 RUST_INTEROP_MINIMAL=1 ./interop/run.sh
HEARSAY_INTEROP_EMPTY=1 ./interop/run.sh
HEARSAY_INTEROP_MINIMAL=1 RUST_INTEROP_MINIMAL=1 HEARSAY_INTEROP_RACE=1 ./interop/run.sh
```

The debug dump defaults to `.agi.local/interop-artifacts/go-stack.txt`.
Raw logs are caller-owned output; redirect them into `.agi.local/` to retain
local evidence.

## Rust CLI

The CLI prints one JSON object per stdout line. Its startup object contains
its TCP address. It accepts these operations on stdin:

- `accept` and `connect` establish a retained routed link. `connect` requires
  an `address` string such as `127.0.0.1:12345`.
- `bootstrap`, `gossip`, and `retire` run one session on that link.
- `send` accepts a `payload` string containing hex bytes.
- `redact` accepts a `version` string containing canonical Version bytes in hex.
- `list` returns version/payload records and the causal frontier.

For example, `{"op":"send","payload":"616263"}` sends `abc`.
Passing the executable argument `seed` starts a new universe. Without it,
the process must bootstrap before sending or listing data.
