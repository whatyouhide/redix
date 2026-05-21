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
    └── ETS: slot_table (slot 0..16383 -> node_id string)
```

All resource names are derived deterministically from the cluster name:
`:my_cluster` -> `:"my_cluster_slots"`, `:"my_cluster_registry"`,
`:"my_cluster_manager"`, `:"my_cluster_pool"`, `:"my_cluster_task_supervisor"`.
This eliminates the need for `persistent_term` or any external lookup.

### Key modules

- **`Redix.Cluster`** (`lib/redix/cluster.ex`) — Public API + Supervisor. `command/3`,
  `pipeline/3`, `transaction_pipeline/3`. Handles MOVED/ASK redirections, transparent
  pipeline splitting across nodes. The `:name` option is required.

- **`Redix.Cluster.Manager`** (`lib/redix/cluster/manager.ex`) — gen_statem with two
  states: `:ready` and `:cooling_down`. Manages topology via `CLUSTER SLOTS`, starts
  Redix connections registered in the Registry via `{:via, Registry, {registry, node_id}}`.
  Uses named timeout `{:timeout, :periodic_refresh}` for periodic refresh and
  `:state_timeout` for the 1-second cooldown after reactive refreshes.

- **`Redix.Cluster.Hash`** (`lib/redix/cluster/hash.ex`) — CRC16-XMODEM with
  compile-time lookup table, hash tag extraction, `hash_slot/1`.

- **`Redix.Cluster.CommandParser`** (`lib/redix/cluster/command_parser.ex`) — Static
  lookup table (~150 commands) mapping command names to first key position. Handles
  EVAL/EVALSHA numkeys parsing, XREAD/XREADGROUP STREAMS keyword scanning.
  Returns `{:ok, key}`, `:no_key`, or `:unknown`.

### Design decisions

- **Registry for connections, ETS for slots.** The slot table is pure data (16384 entries
  mapping to ~3 node_id strings) — ETS is the right tool. The connection map (node_id ->
  pid) is process registration — Registry handles auto-cleanup on process death and
  supports `:via` tuples for transparent naming.

- **One Redix connection per node** (multiplexing model, like ioredis/Lettuce). Redix
  already pipelines internally. Users who need more throughput start multiple named
  `Redix.Cluster` instances.

- **gen_statem with state_functions** for the Manager. The cooldown after a reactive
  topology refresh is modeled as a state (`:cooling_down`) with a `:state_timeout`,
  not a boolean flag. Periodic refresh uses a named timeout `{:timeout, :periodic_refresh}`
  that restarts itself. The periodic refresh is `:postpone`d during cooldown.

- **trap_exit in try_fetch_slots.** `Redix.start_link(sync_connect: true)` to an
  unreachable host sends an EXIT that would kill the Manager via the link. The Manager
  temporarily traps exits around this call and flushes them afterward. This was caught
  by a telemetry test for `failed_topology_refresh`.

- **Transparent pipeline splitting.** Pipelines spanning multiple nodes are grouped by
  target node, executed in parallel via `Task.Supervisor`, and results reassembled in
  original order. Single-node pipelines skip the task overhead.

- **`:name` is required.** All internal resource names derive from it. No `persistent_term`
  or PID-based lookups needed. Callers always use the atom name.

### Telemetry events

All under `[:redix, :cluster, ...]`:

| Event | Emitted from | Key metadata |
|---|---|---|
| `:topology_change` | Manager, on successful refresh | `cluster`, `nodes` |
| `:failed_topology_refresh` | Manager, when no node reachable | `cluster`, `reason` |
| `:node_connection_failed` | Manager, when a node conn fails | `cluster`, `address`, `reason` |
| `:redirection` | Cluster, on MOVED/ASK | `cluster`, `type`, `slot`, `target_address` |

### Docker cluster setup

`test/docker/cluster/` — 6 Redis nodes (ports 7000-7005), 3 masters + 3 replicas.
The master/replica assignment is NOT deterministic (Redis decides during
`--cluster create`). Tests must handle READONLY errors when flushing replicas.

### Running tests

```sh
docker compose up -d          # start all services including cluster
mix test                      # all tests
mix test test/redix/cluster/  # unit tests (hash, command parser)
mix test test/redix/cluster_test.exs  # integration tests (needs Docker cluster)
```

### Known limitations / future work

- Primary-only routing (no READONLY replica reads)
- No cluster PubSub (different semantics — messages broadcast to all nodes)
- No `noreply_*` functions in cluster mode
- `COMMAND GETKEYS` fallback for unknown commands not yet implemented
  (returns `:unknown` from CommandParser, treated as keyless)
