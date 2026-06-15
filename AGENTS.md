# Agent Notes

Notes for AI agents working on this codebase.

## Project overview

Redix is a low-level Redis/Valkey driver for Elixir. It supports single-node connections,
Sentinel-based failover, PubSub, and Redis Cluster. The philosophy is minimal and
composable: build on `Redix` connections (one per node), use ETS for fast lookups,
mirror the `Redix` API in higher-level modules.

## Codebase conventions

- **gen_statem over GenServer** for stateful processes. `Redix.Connection`,
  `Redix.PubSub.Connection`, and `Redix.Cluster.Manager` all use `:gen_statem`.
  Prefer `:state_functions` callback mode when there are meaningful states.
- **NimbleOptions** for option validation. See `Redix.StartOptions` and the
  `@start_link_opts_schema` in `Redix.Cluster`.
- **Telemetry only, no Logger** in library code. All observability goes through
  `:telemetry.execute/3`. The `Redix.Telemetry` module documents events and provides
  a default handler that logs them. When adding new functionality, add telemetry events
  rather than Logger calls.
- **Typespecs** on all public functions.
- **`@moduledoc false`** on internal modules (Manager, Hash, CommandParser, etc.).
- **`!` variants** for all public command functions (raises instead of returning error tuples).
- Tests use `start_supervised!` and unique names to avoid collisions in async tests.
  Cluster integration tests use `@moduletag :cluster` and skip when the Docker cluster
  isn't available.

## Redis Cluster implementation

### Architecture

```
Redix.Cluster (Supervisor, public API)
├── Registry (unique keys, node_id -> Redix pid)
├── DynamicSupervisor (supervises Redix connections)
├── Task.Supervisor (parallel pipeline execution)
└── Redix.Cluster.Manager (gen_statem: topology + connection lifecycle)
    ├── ETS: slot_table (slot 0..16383 -> {primary_id, [replica_id]})
    └── ETS: command_cache (command name -> key spec, for commands outside the table)
```

All resource names are derived deterministically from the cluster name:
`:my_cluster` -> `:"my_cluster_slots"`, `:"my_cluster_command_cache"`,
`:"my_cluster_registry"`, `:"my_cluster_manager"`, `:"my_cluster_pool"`,
`:"my_cluster_task_supervisor"`.
This eliminates the need for `persistent_term` or any external lookup.

### Key modules

- **`Redix.Cluster`** (`lib/redix/cluster.ex`) — Public API + Supervisor. `command/3`,
  `pipeline/3`, `transaction_pipeline/3`. Handles MOVED/ASK redirections, transparent
  pipeline splitting across nodes. The `:name` option is required.

- **`Redix.Cluster.Manager`** (`lib/redix/cluster/manager.ex`) — gen_statem with three
  states: `:disconnected` (no topology yet, async connect), `:ready`, and
  `:cooling_down`. Manages topology via `CLUSTER SLOTS`, starts
  Redix connections registered in the Registry via
  `{:via, Registry, {registry, node_id, role}}` where `role` is `:primary` or
  `:replica` (the Registry *value*). Uses named timeout `{:timeout, :periodic_refresh}`
  for periodic refresh and `:state_timeout` for the 1-second cooldown after reactive
  refreshes.

- **`Redix.Cluster.Hash`** (`lib/redix/cluster/hash.ex`) — CRC16-XMODEM with
  compile-time lookup table, hash tag extraction, `hash_slot/1`.

- **`Redix.Cluster.CommandParser`** (`lib/redix/cluster/command_parser.ex`) — Static
  lookup table (~180 commands) mapping command names to first key position. Handles
  EVAL/EVALSHA numkeys parsing, XREAD/XREADGROUP STREAMS keyword scanning, and the
  position-2 commands OBJECT/BITOP. Returns `{:ok, key}`, `:no_key`, or `:unknown`
  (the last resolved at runtime by `Redix.Cluster` via COMMAND INFO/GETKEYS).

### Design decisions

- **Registry for connections, ETS for slots.** The slot table is pure data (16384 entries
  mapping to `{primary_id, [replica_id]}`) — ETS is the right tool. The connection map
  (node_id -> pid) is process registration — Registry handles auto-cleanup on process
  death and supports `:via` tuples for transparent naming. The Registry *value* records
  the node's `role` so keyless commands route to primaries (`get_random_connection/1`)
  and replica lookups stay separate from primary lookups.

- **Replica reads are opt-in** (`read_from_replicas: true`). When off (default), only
  primaries are connected and the slot table stores `[]` for replicas — behavior is
  identical to primary-only. When on, the Manager also connects/supervises/monitors one
  connection per replica `host:port`, passing `readonly: true` so the connector issues
  `READONLY` after *every* (re)connect (it lives in `Redix.Connector.auth_and_select`
  alongside AUTH/SELECT, so reconnects redo it automatically). Per-call routing is the
  `:route` option on `command/3`/`pipeline/3` (`:primary` | `:replica` | `:prefer_replica`),
  resolved in `Redix.Cluster.resolve_connection/4`. `transaction_pipeline/3` only allows
  `:primary` (MULTI/EXEC must run on the primary). A write mistakenly routed to a replica
  comes back as `MOVED` and is followed to the primary by the existing redirect machinery.

- **Node roles are reconciled on every topology refresh.** `ensure_connections/2`
  compares each live connection's registered role (the Registry *value*) against the
  role the latest `CLUSTER SLOTS` assigns it, and terminates+restarts on a mismatch
  (`restart_connection/6`). After a failover a demoted primary's connection never
  issued `READONLY` (so `route: :replica` reads would bounce back `MOVED` forever) and
  a promoted replica still carried `readonly: true` and was excluded from keyless-command
  routing — the stale role survives `DynamicSupervisor`/`handle_down` restarts (the child
  spec re-uses the original `{:via, Registry, {registry, node_id, role}}` tuple), so only
  this reconciliation fixes it (issue #318). The restart demonitors before
  `terminate_child` so the deliberate DOWN doesn't resurrect the old role via
  `handle_down/2`.

- **One Redix connection per node** (multiplexing model, like ioredis/Lettuce). Redix
  already pipelines internally. Users who need more throughput start multiple named
  `Redix.Cluster` instances.

- **gen_statem with state_functions** for the Manager. The cooldown after a reactive
  topology refresh is modeled as a state (`:cooling_down`) with a `:state_timeout`,
  not a boolean flag. Periodic refresh uses a named timeout `{:timeout, :periodic_refresh}`
  that restarts itself. The periodic refresh is `:postpone`d during cooldown.

- **Async connect by default, `sync_connect: true` to opt out** (issue #323), matching
  single-node Redix's lazy connect — a `Redix.Cluster` in a supervision tree doesn't
  crash-loop the app when Redis comes up after it. By default `Manager.init/1` enters
  `:disconnected` and triggers the first topology fetch via an internal `:next_event`;
  failures retry on a `:state_timeout` with the same exponential backoff as
  `Redix.Connection` (`:backoff_initial`/`:backoff_max` conn opts, exponent 1.5, reset
  on success). Commands check a `{:discovery_attempted, true}` marker in the slot
  table (one ETS lookup; set when the *initial* fetch attempt completes — on
  success after the table and Registry are populated, on failure right away in the
  `:disconnected` state); while it's absent they block on
  `Manager.await_topology_discovery/2` — a call whose only job is to queue behind
  the in-flight fetch event, mirroring how `Redix.Connection` postpones pipelines
  while connecting. So commands only ever await that one initial attempt: if it
  succeeded their lookups find the topology, and once it failed they fail fast with
  `%Redix.ConnectionError{reason: :closed}` (empty slot table, no registered
  connections) without contacting the Manager again — no blocking callers on every
  backoff retry when the cluster couldn't be reached on the first try. Reactive
  refresh casts are dropped while `:disconnected` (the retry timer is in charge), and
  `connect_to_node` calls are still served. With `sync_connect: true`, `init/1` fetches
  synchronously and returns `{:stop, reason}` on failure so `start_link/1` fails fast.
  Node connections always use `sync_connect: false` regardless: the cluster-level
  option only covers topology discovery. Relatedly, `:nodes` rejects `[]` at validation
  time (`__parse_nodes__`) instead of failing later with `:no_reachable_node`.

- **Transient socket for topology fetches.** `try_fetch_slots/4` connects with
  `Redix.Connector.connect/2` against a raw socket, runs `CLUSTER SLOTS` via
  `Redix.Connector.sync_command/4`, and closes the socket in an `after` block. There's
  no `Redix.start_link` and no linked process, so an unreachable host can't take down
  the Manager — failures just fall through to the next node.

- **Transparent pipeline splitting.** Pipelines spanning multiple nodes are grouped by
  target node, executed in parallel via `Task.Supervisor`, and results reassembled in
  original order. Single-node pipelines skip the task overhead.

- **Server-assisted routing for commands outside the static table.** Commands not in
  CommandParser's static table return `:unknown` (not `:no_key`).
  `Redix.Cluster.resolve_unknown_slots/3` resolves them by asking the server and **caching
  the answer** in a per-cluster ETS table (`:"#{cluster}_command_cache"`, owned by the
  Manager). A command's key specification (first-key position, or "movable") is stable, so
  it's learned once via `COMMAND INFO` (`keyspec_from_info/1` reads `first_key` + the
  `movablekeys` flag) and cached per command name; later calls resolve locally with no
  round-trip. Commands the server reports as having *movable* keys (e.g. `MIGRATE`,
  `BLMPOP`) can't be pinned to a fixed position and fall back to a per-call `COMMAND
  GETKEYS`, but are still cached as `:movable` so `COMMAND INFO` isn't re-issued. Anything
  that can't be resolved (unknown to the server, no node reachable) becomes `:no_slot` —
  random-node routing, the prior behavior. This runs in both `command/3`/`pipeline/3` and
  `transaction_pipeline/3`, so a transaction of such commands still computes its single
  target slot. The static table itself was expanded (bit commands, blocking `B*` pops,
  hash-field TTL, `XSETID`, `BITOP`) so the fallback only fires for the long tail.

- **`:name` is required.** All internal resource names derive from it. No `persistent_term`
  or PID-based lookups needed. Callers always use the atom name.

- **Null hosts in `CLUSTER SLOTS` resolve to the answering node.** Redis 7+ with
  `cluster-preferred-endpoint-type unknown-endpoint` (managed/NAT'd deployments) returns
  a null host meaning "use the address you connected to". `normalize_slots/2` in the
  Manager substitutes the host that answered the topology query right after the fetch
  (issue #328), so `update_slot_map/2` and `nodes_to_connect/2` never see nil/empty
  hosts. Tested via the fake-RESP-node pattern in `slot_map_refresh_test.exs`.

- **The slot table is rewritten, not just upserted, on each refresh.**
  `Redix.Cluster.Manager.update_slot_map/2` overwrites every *covered* slot in place
  (so a reshard/reassignment is seamless and a concurrent lookup never sees a covered slot
  vanish) and then deletes any slot the new `CLUSTER SLOTS` response no longer covers. In a
  fully-covered cluster nothing is ever deleted; the deletion only matters for a partially
  covered cluster (mid-setup, or one that lost coverage), where a stale `{slot, primary,
  replicas}` mapping would otherwise route to a node that no longer owns the slot (issue
  #314). Lookups for a genuinely unassigned slot return `:error`, which is correct.

- **MOVED and ASK redirects connect on demand.** When a redirect points at a node not
  yet in the Registry, `handle_moved_redirect/6` and `handle_ask_redirect/6` fall back to
  `Manager.connect_to_node/3` — a `:gen_statem.call` served in both `:ready` and
  `:cooling_down` that starts+monitors a connection to the redirect target and returns its
  pid. For `MOVED` the typical case is mid-resharding (the topology refresh cast is async
  and the Manager may be `:cooling_down`); for `ASK` it's a slot migrating to a
  *brand-new* node, which serves zero slots and so never appears in `CLUSTER SLOTS` —
  without the fallback every ASK-redirected command would fail until the first migration
  completed (issue #319). Redirects are authoritative, so we trust the address rather
  than returning a fake "unreachable" error. The next `ensure_connections` adopts the
  connection (if `CLUSTER SLOTS` lists it) or terminates it (if not), so a bogus address
  can't leak connections. For `MOVED`, the async refresh still fires to update the slot
  table for future routing. The call takes a **finite timeout** — the redirected
  command's `:timeout` (`Redix.Cluster.connect_timeout/1`), not `:infinity`: the Manager
  fetches topology *serially* and each unreachable node costs up to the connection
  `:timeout`, so an `:infinity` call would block every on-demand connect for the whole
  refresh against a partially-down cluster. On timeout the caught `:exit` degrades to the
  normal "unreachable" error path (issue #327).

- **Redirect chains are followed, not just single hops.** `MOVED` and `ASK` both
  flow through `follow_redirections/5`, the single place the `@max_redirections`
  budget is decremented, so a chain like `ASK -> ASK` or `ASK -> MOVED` is followed
  to completion (bounded) instead of being handed back verbatim. `ASK` is per-command
  and per-request: `handle_ask_redirect/6` issues each command on its own behind an
  `ASKING` prefix via `execute_asking_command/5`, then re-feeds the result through
  `follow_redirections/5` so a further hop re-issues `ASKING` at the new target.

### Telemetry events

All under `[:redix, :cluster, ...]`:

| Event | Emitted from | Key metadata |
|---|---|---|
| `:topology_change` | Manager, on successful refresh | `cluster`, `nodes` |
| `:failed_topology_refresh` | Manager, when no node reachable | `cluster`, `reason` |
| `:node_connection_failed` | Manager, when a node conn fails | `cluster`, `address`, `reason` |
| `:redirection` | Cluster, on MOVED/ASK | `cluster`, `type`, `slot`, `target_address` |

### Docker cluster setup

`test/docker/cluster/` — 9 Redis nodes (ports 7000-7008), 3 masters + 6 replicas
(`--cluster-replicas 2`, so every slot has *two* replicas — needed to exercise
replica read load-spreading). The master/replica assignment is NOT deterministic
(Redis decides during `--cluster create`). Tests must handle READONLY errors when
flushing replicas.

### Running tests

```sh
docker compose up -d          # start all services including cluster
mix test                      # all tests
mix test test/redix/cluster/  # unit tests (hash, command parser)
mix test test/redix/cluster_test.exs  # integration tests (needs Docker cluster)
```

### Overriding host ports

If something on the host is already bound to one of the default ports (most
commonly `6379`), the host-side mapping can be overridden via env vars. Defaults
match the original `docker-compose.yml`, so leaving them unset changes nothing.

| Service                              | Env var                         | Default |
|--------------------------------------|---------------------------------|---------|
| `base`                               | `REDIX_BASE_PORT`               | `6379`  |
| `pubsub`                             | `REDIX_PUBSUB_PORT`             | `6380`  |
| `base_with_auth`                     | `REDIX_AUTH_PORT`               | `16379` |
| `base_with_acl`                      | `REDIX_ACL_PORT`                | `6385`  |
| `base_with_stunnel`                  | `REDIX_STUNNEL_PORT`            | `6384`  |
| `base_with_disallowed_client_command`| `REDIX_DISALLOWED_CLIENT_PORT`  | `6386`  |

The same env vars are read by `Redix.TestPorts` (`test/support/test_ports.ex`),
which the test suite uses for every connection — so `mix test` honors the
override automatically:

```sh
REDIX_BASE_PORT=6479 docker compose up -d base
REDIX_BASE_PORT=6479 mix test
```

The `sentinel`, `sentinel_with_auth`, and `cluster` services are **not**
parameterized. Their ports (`6381`, `6382`, `6383`, `26379-26383`, `7000-7008`)
are baked into the container topology — sentinel returns `localhost:6381` to
clients, and cluster MOVED redirections point at `127.0.0.1:7000..7008`.
Remapping their host-side ports would break client redirection, so those
services need their default host ports free.

### Known limitations / future work

- Replica reads use a random reachable replica; no read-load balancing strategy,
  staleness tolerance, or zone/locality awareness yet.
- No cluster PubSub (different semantics — messages broadcast to all nodes)
- No `noreply_*` functions in cluster mode
