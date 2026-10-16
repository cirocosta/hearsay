# Shared Kanban workers

Each process is an identical worker with a local HTTP API. Submit an issue to
any worker. Workers gossip the same board, claim a `todo` card, move it to
`in_progress` with their name, and finally publish `done` with a result.

The built-in job waits two seconds and computes SHA-256 of
`title + "\n" + body`. It demonstrates the workflow without a model, credentials,
or a coding-agent harness. There is no master worker. The initial seed only
creates the Hearsay network; any surviving worker can serve future joiners.

The same Hearsay message set carries four application payload kinds:
`member`, `todo`, `in_progress`, and `done`. Member heartbeats and card updates
share the same sparse gossip connections.

## Run six workers

From the repository root, build the example. The commands below use `curl` and
`jq` to inspect JSON.

```sh
go build -o /tmp/hearsay-kanban ./examples/kanban
mkdir -p /tmp/hearsay-kanban-demo

/tmp/hearsay-kanban -name worker-0 \
  -listen 127.0.0.1:9000 -http 127.0.0.1:8000 \
  > /tmp/hearsay-kanban-demo/0.log 2>&1 &
echo $! > /tmp/hearsay-kanban-demo/0.pid

for i in 1 2 3 4 5; do
  /tmp/hearsay-kanban -name "worker-$i" \
    -listen "127.0.0.1:$((9000+i))" \
    -http "127.0.0.1:$((8000+i))" \
    -join 127.0.0.1:9000 \
    > "/tmp/hearsay-kanban-demo/$i.log" 2>&1 &
  echo $! > "/tmp/hearsay-kanban-demo/$i.pid"
done
```

Each process prints its HTTP address and a membership snapshot when its member
statuses or direct links change. Check a worker's whole view separately from
its direct neighbors:

```sh
curl -fsS http://127.0.0.1:8000/members | jq .
curl -fsS http://127.0.0.1:8005/members | jq .
```

After synchronization, both views include all six addresses. Each worker targets
three distinct neighbors, counting incoming and outgoing connections together.
It retains healthy links rather than dialing every discovered address. The
soft target permits one extra retained connection so a full worker can admit a
newcomer. More links can exist briefly during concurrent admission; after a
successful exchange, the oldest other link is closed above that limit.

By default HTTP binds a random loopback port and prints it. Set `-http` explicitly
as above for repeatable commands. Set `-http ''` to disable the HTTP listener.
Use numeric, dialable IP addresses for `-listen` and `-join`; different machines
must advertise addresses that the other workers can reach.

## Submit and inspect work

The curl client does not join the gossip network. It submits to one worker's
HTTP API:

```sh
curl -fsS http://127.0.0.1:8003/issues \
  -H 'Content-Type: application/json' \
  -d '{"title":"Check the example","body":"Run the demo workload."}' | jq .

curl -fsS http://127.0.0.1:8000/board | jq .
curl -fsS http://127.0.0.1:8005/board | jq .
```

Repeat the board requests to see the card progress. Its ID stays the same across
columns. Every worker has the same API and eventually the same board while the
network is connected. `in_progress` and `done` cards include an assignee;
`done` also includes the checksum result. Use `-job-duration 10s` when starting
workers to make the intermediate state easier to inspect.

| Request | Response |
| --- | --- |
| `POST /issues` with `{"title":"...","body":"..."}` | `201` and the new card |
| `GET /board` | Arrays named `todo`, `in_progress`, and `done` |
| `GET /members` | This worker, its direct neighbors, and its local membership view |

Claims are eventually replicated, not exclusive locks. Two workers can claim
and execute the same card during a race or partition. A deterministic,
causal-compatible version order selects one visible update. Execution checks
that the observed claim is still current before publishing completion. The
checksum job is safe to repeat; this example does not provide exactly-once
execution for external side effects.

## Join, leave, fail, and recover

Join a seventh worker through just worker 5:

```sh
/tmp/hearsay-kanban -name worker-6 \
  -listen 127.0.0.1:9006 -http 127.0.0.1:8006 \
  -join 127.0.0.1:9005 \
  > /tmp/hearsay-kanban-demo/6.log 2>&1 &
echo $! > /tmp/hearsay-kanban-demo/6.pid

curl -fsS http://127.0.0.1:8000/members | jq .
```

After synchronization, worker 0 learns worker 6 without requiring a direct link.
Terminate worker 6 gracefully:

```sh
kill -TERM "$(cat /tmp/hearsay-kanban-demo/6.pid)"
```

It stops accepting work, publishes `left`, and allows up to five seconds for
synchronization before closing. Its final log reports successful synchronization
with current direct neighbors; it does not claim delivery to the entire group.
Surviving workers retain the departed row with status `left`.

To simulate an unresponsive member and temporary connection loss on macOS or
Linux, pause worker 5. Its OS sockets can initially remain open, but it stops
servicing gossip; active exchanges time out and peers reconnect:

```sh
kill -STOP "$(cat /tmp/hearsay-kanban-demo/5.pid)"
```

On other workers, worker 5 becomes `suspected` after five seconds without a newer
heartbeat, then `unreachable` after fifteen seconds. Resume it:

```sh
kill -CONT "$(cat /tmp/hearsay-kanban-demo/5.pid)"
```

New heartbeats restore `active`. This pause demonstrates an unresponsive process;
the tests also isolate two live groups by blocking TCP dials and closing their
cross-group routed links, then verify stable recovery after the block is lifted.

For an unexpected termination and restart at the same address:

```sh
kill -KILL "$(cat /tmp/hearsay-kanban-demo/4.pid)"
# inspect /members on another worker until worker 4 is unreachable
/tmp/hearsay-kanban -name worker-4 \
  -listen 127.0.0.1:9004 -http 127.0.0.1:8004 \
  -join 127.0.0.1:9001 \
  > /tmp/hearsay-kanban-demo/4.log 2>&1 &
echo $! > /tmp/hearsay-kanban-demo/4.pid
```

The restart gets a fresh incarnation ID. Its current row replaces the old
incarnation at that address. If bootstrap knowledge was stale, the live writer
advances its logical generation when it learns a dominating prior incarnation.
One process must own each advertised address.

An in-progress card whose assignee has left, become unreachable, or been replaced
by a later incarnation becomes eligible for retry. Failure detection is a local
observation, so partitioned workers can retry work concurrently.

Stop the remaining demo processes when finished:

```sh
for i in 0 1 2 3 4 5; do
  kill -TERM "$(cat "/tmp/hearsay-kanban-demo/$i.pid")" 2>/dev/null || true
done
```

## Replication and failure detection

Hearsay supplies causal reconciliation and continuous pairwise gossip over the
routed TCP transport. Joining copies the same network identity from an existing
peer. Independently seeded networks cannot merge. An application-owned,
length-prefixed admission frame selects a retained connection or a temporary
probe before the Hearsay protocol begins.

This example supplies addresses, neighbor discovery, connection retries,
heartbeats, local failure detection, card projection, and worker scheduling.
Each member publishes an increasing counter every second. Only a newer counter
or incarnation refreshes local freshness; replaying an old heartbeat does not.
Suspicion remains local rather than becoming a writer that can erase someone
else's heartbeat.

Retries use a jittered backoff between one and ten seconds. Even at the neighbor
target, workers occasionally probe known non-neighbors, including unreachable
members. Recovering a stale member retains that connection so a repaired bridge
continues to carry heartbeats. Healthy non-neighbor probes synchronize once and
close. This is a small discovery policy, not a graph-connectivity guarantee.

During a partition each component has its own current view. A timeout cannot
prove a process is dead. Views converge after a reachable connection is restored;
healing requires at least one known reachable address across the components.
The program does not discover unknown networks or provide consensus.

Heartbeat publications replace observed records for the same address in one
batch. Card transitions replace observed records for the same stable card ID.
Concurrent unseen updates survive until a later transition observes them.
Departed and crashed addresses retain a last record for diagnosis and recovery;
there is no infinite heartbeat log, but distinct historical addresses and cards
remain in memory. All state is in memory: stopping every worker loses the board.
The loopback demo has no authentication or persistence layer.

## Verify

```sh
go test ./examples/kanban
go test -race ./examples/kanban
go vet ./examples/kanban
golangci-lint run ./examples/kanban/...
```

The tests exercise real loopback routed TCP, multi-hop joins and departures,
crash/restart, stale-bootstrap incarnation repair, replay suppression, timeout
transitions, sparse admission, simultaneous dial deduplication, partition
recovery while already at the neighbor target, and the shared board/HTTP flow.
