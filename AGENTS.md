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

- **Node connections are `restart: :temporary`, and a *semantic* setup failure parks the
  node for the topology refresh rather than restarting it.** `start_connection/7`
  overrides the `{Redix, opts}` child spec to `restart: :temporary` so the pool
  `DynamicSupervisor` never restarts a node connection itself — the Manager owns the
  lifecycle (it monitors every connection and reacts to `DOWN`, with the
  `:already_started` adoption path). A node that fails connection *setup* with a
  *semantic* error (a `%Redix.Error{}`: `NOAUTH`/`WRONGPASS` from per-node password/ACL
  drift, or `READONLY` on a node with cluster support disabled) stops its
  `Redix.Connection` by design (`connection.ex`'s `disconnect(_, %Redix.Error{}) ->
  {:stop, error}`). As a `:permanent` pool child that crash-looped with no backoff, blew
  past the `DynamicSupervisor`'s default intensity (3/5s) in milliseconds, and — via the
  top-level `:one_for_all` — took the whole cluster tree (and often the host app) down
  within a second (issue #334). Now the crash is absorbed (temporary child) and handled
  in `handle_down/3`, which **branches on the DOWN reason**: a `%Redix.Error{}` is *not*
  restarted (retrying on a timer can't fix a misconfiguration — only an operator can, on
  no bounded schedule), so the node is left to the periodic topology refresh, which
  `ensure_connections/2` uses to re-attempt every needed-but-unconnected node each
  interval. That re-evaluates the node on the correct trigger (a fresh `CLUSTER SLOTS`):
  it reconnects within one refresh once fixed, or is torn down for good if it has left
  the cluster. A `node_connection_failed` telemetry event makes a parked node observable.
  Any *other* DOWN reason (a process crash or deliberate kill) *is* restarted inline for
  fast recovery — and can't degenerate into a loop, because a *transient network* failure
  never reaches `handle_down/3` at all (`Redix.Connection` handles it internally, moving
  to its own `:disconnected` state and reconnecting, rather than stopping). This is why
  no per-node backoff machinery is needed: exponential backoff's only edge over the
  refresh — a fast *first* retry — is worthless for a semantic error (the retry is
  guaranteed to fail again), and its steady state would just match the refresh interval
  anyway.

- **One Redix connection per node** (multiplexing model, like ioredis/Lettuce). Redix
  already pipelines internally. Users who need more throughput start multiple named
  `Redix.Cluster` instances.

- **Only TLS is lifted from a seed URI; credentials/database raise.** `__parse_node__`
  returns a full set of connection options per seed (`Redix.URI.to_start_options/1` for
  a URI, the validated keyword list otherwise), not just `{host, port}`. In `start_link/1`
  the `{host, port}` of each becomes `seed_nodes` (only host/port are used to reach a
  seed); `merge_seed_node_opts!/2` then handles the rest via `merge_seed_opt!/2`, run
  *before* `StartOptions.sanitize` so an explicit `ssl:` stays distinguishable from the
  default. TLS is genuinely cluster-wide (nodes speak TLS to each other or not at all),
  so a `rediss://` seed is lifted into `conn_opts` as `ssl: true` (a `rediss://` seed
  plus explicit `ssl: false` raises). Credentials and a non-default database *can't* be
  lifted — every node is authenticated with the shared `conn_opts`, and seeds may
  legitimately differ — so userinfo in a seed URI raises (pass `:username`/`:password`
  as options) and a non-zero database raises (cluster only supports db 0; db 0 is a
  no-op). This surfaces the dropped-info footgun loudly instead of silently (issue #322).
  A plain `redis://`/`valkey://` seed carries no `:ssl` key (default scheme, not an
  assertion of non-TLS), so `redis://` + `ssl: true` is fine.

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

- **Transient socket for topology fetches.** `try_fetch_slots/5` connects with
  `Redix.Connector.connect/2` against a raw socket, runs `CLUSTER SLOTS` via
  `Redix.Connector.sync_command/4`, and closes the socket in an `after` block. There's
  no `Redix.start_link` and no linked process, so an unreachable host can't take down
  the Manager — failures just fall through to the next node.

- **Per-node failure reasons are collected, not discarded, on a failed topology
  fetch.** `fetch_cluster_slots/3` threads a `node_errors` accumulator (`{host, port,
  reason}` triples, in the order tried) through every rejection branch in
  `try_fetch_slots/5` — connect failure, `AUTH`/`ACL` failure surfaced as
  `Redix.Connector.connect/2`'s `{:stop, reason}`, a `CLUSTER SLOTS` error reply, and
  an invalid/malformed reply (tagged `:invalid_cluster_slots_reply`) — and returns
  `{:error, {:no_reachable_node, node_errors}}` once every node is exhausted. Before
  this, every failure mode collapsed to the bare atom `:no_reachable_node`, so a wrong
  password (`NOAUTH`/`WRONGPASS`) was indistinguishable from a network partition in
  both the `sync_connect: true` start error and the `:failed_topology_refresh`
  telemetry event (issue #339).

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

- **`CLUSTER SLOTS` replies are validated and normalized in a single pass before anything
  downstream trusts their shape.** `validate_and_normalize_slots/2` in the Manager checks
  every range is `[start, end, primary | replicas]` with integer, in-range, ordered slot
  bounds and at least one well-formed node entry (`[host, port | _]`, host binary-or-nil,
  port a valid port number), and substitutes a null/empty host with the host that
  answered the topology query along the way — Redis 7+ with
  `cluster-preferred-endpoint-type unknown-endpoint` (managed/NAT'd deployments) sends a
  null host meaning "use the address you connected to" (issue #328). `try_fetch_slots/4`
  calls it right after the raw reply comes back, so `update_slot_map/2` and
  `nodes_to_connect/2` only ever see well-formed, host-normalized ranges. Without the
  validation half, a range like `[start, end]` with no node array, or non-integer slot
  bounds, hit hard matches/arithmetic in those two functions and crashed the Manager — a
  `:one_for_all` restart of the whole cluster, and a crash loop if the server kept
  answering that way (issue #344, same escalation as #334). An invalid reply is treated
  exactly like an unreachable node: `fetch_cluster_slots/2` falls through to the next
  seed, and the refresh fails via the ordinary `:no_reachable_node` path if none answer
  validly. Mirrors how redirect payloads are parsed defensively
  (`parse_redirection_target/2`, issue #325). Tested via the fake-RESP-node pattern in
  `fake_node_test.exs`.

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

- **Node identity is the resolved IP, not the literal `"host:port"` string.**
  `Redix.Cluster.Manager.canonical_node_id/2` resolves `host` (via
  `:inet.parse_address/1`, falling back to `:inet.getaddr/2` for a hostname) and uses
  the IP in the Registry key / slot-table entry; the literal host is kept separately
  in `data.node_addresses` (`node_id => host`, populated in
  `start_and_monitor_connection/5`) so reconnects (`handle_down/2`) and refresh
  seeding (`get_known_nodes/1`) still dial the *original* address — resolving to an
  IP for identity but dialing the literal host means this can't break TLS hostname
  verification. Without this, a MOVED/ASK redirect announcing a node under a
  different address form than `CLUSTER SLOTS` does for that same node (IP vs
  hostname, typical of `cluster-preferred-endpoint-type` in managed/NAT'd
  deployments) would never match the Registry key `CLUSTER SLOTS` already
  registered that node under: `connect_for_redirect`'s fast-path lookup always
  missed, so every redirect to such a node dialed a *second*, short-lived
  connection under the redirect's own form — which the next topology refresh tore
  down as "unneeded" (computed from `CLUSTER SLOTS`'s form), only for the next
  redirect to repeat the cycle (issue #340). Canonicalizing makes both forms
  resolve to the same key, so the fast path finds the existing connection directly
  and no extra dial ever happens. Resolution only runs where an address enters
  persistent state (topology processing, the on-demand connect fallback) or on the
  `get_connection_by_node/2` fast path itself — not behind a cache, so a
  hostname-vs-IP mismatch costs a DNS lookup (typically OS-cached) on each such
  redirect rather than a full connection churn cycle; a literal IP address costs
  nothing (`:inet.parse_address/1` is pure). Resolution failure falls back to the
  literal host, same as before this fix (not worse, just not deduplicated).

- **Redirect chains are followed, not just single hops.** `MOVED` and `ASK` both
  flow through `follow_redirections/5`, the single place the `@max_redirections`
  budget is decremented, so a chain like `ASK -> ASK` or `ASK -> MOVED` is followed
  to completion (bounded) instead of being handed back verbatim. `ASK` is per-command
  and per-request: `handle_ask_redirect/6` issues each command on its own behind an
  `ASKING` prefix via `execute_asking_command/5`, then re-feeds the result through
  `follow_redirections/5` so a further hop re-issues `ASKING` at the new target.

- **Running out of redirect budget only fails the commands still in flight.**
  When `follow_redirections/5`'s `remaining` budget hits 0, only the commands still
  awaiting a hop are turned into `%Redix.ConnectionError{reason: :too_many_redirections}`
  values at their original indices; commands in the same group that already
  finished (no redirect needed, or resolved on an earlier hop) keep their real
  result instead of the whole group being discarded (issue #341). This relies on
  the recursive MOVED/ASK handlers (`handle_moved_redirect/6`,
  `handle_ask_redirect/6`, `execute_asking_command/5`) always returning a list of
  `{idx, result}` tuples rather than a bare `{:error, reason}` — a bare error would
  match the `{:error, _}` check one level up in `follow_redirections/5` and discard
  that level's `final_results` too. `command/3` checks the sole result for
  `%Redix.ConnectionError{}` in addition to `%Redix.Error{}`, since a single-command
  call can now surface this as a value rather than a top-level `{:error, _}`.

- **A whole call shares one deadline across its redirect chain, not a fresh
  `:timeout` per hop.** Each MOVED/ASK hop re-issued `Redix.pipeline/3` with the
  caller's *full* `:timeout`, and on-demand connects were unbounded, so with the
  5-hop budget a command called with `timeout: 5_000` could legally run for ~30s.
  Only the multi-node path was bounded overall (by `Task.yield_many/2`); the common
  single-group `pipeline/3`/`command/3` case and `transaction_pipeline/3` were not
  (issue #337). `execute_pipeline/3` and `transaction_pipeline/3` now capture an
  absolute-monotonic deadline once (`put_deadline/1`, derived from the effective
  `:timeout`, `:infinity` stays `:infinity`) into `opts` under a private
  `:__deadline__` key, *after* `await_topology_discovery` so discovery-wait keeps its
  own budget. Every network hop then reads the *remaining* time off it:
  `pipeline_catching_exit/3` strips the key (`Redix.pipeline/3` rejects unknown
  options), and passes the remaining ms as `:timeout` — or short-circuits with
  `%Redix.ConnectionError{reason: :timeout}` when the deadline already elapsed on an
  earlier hop; `connect_timeout/1` (used by the on-demand `connect_to_node`) does the
  same. This bounds the single-group and transaction paths to ~one `:timeout` total.
  The multi-node path's inner tasks now self-terminate at the deadline too, so
  `redirect_aware_timeout/1`'s `(@max_redirections + 1) × :timeout` `Task.yield_many/2`
  value is now purely a backstop that fires strictly *after* the task's own deadline
  (kept generous so it never preempts a group about to return); in practice the
  deadline returns first. Complements the per-node fetch bounds of issues #327/#335.

- **A table-missing race during a cluster tree restart degrades to a connection
  error (or a cache miss) instead of raising in the caller.** The Manager owns the
  slot and command-cache ETS tables, and `Redix.Cluster`'s top-level supervisor uses
  `:one_for_all`, so a Manager crash briefly tears down Registry and both tables
  before restarting them. A lookup landing in that window used to raise
  `ArgumentError` straight out of the underlying `:ets`/`Registry` call — `:ets`
  itself says "table identifier does not refer to an existing ETS table"; `Registry`
  catches that internally and re-raises as "unknown registry: ..." — instead of
  just not finding an answer, like every other "cluster unreachable" case (issue
  #338). `Manager.guard_missing_table/2` wraps a single lookup and its `default`
  (`fun.() rescue e in ArgumentError -> if missing_table_error?(e), do: default,
  else: reraise e, __STACKTRACE__`), so only that specific message shape degrades —
  any other `ArgumentError` (a genuine bug in the wrapped lookup) still propagates.
  It's applied at the narrow leaf call sites, each defaulting to what an ordinary
  "not found" already looks like there: `Manager.get_connection/3`,
  `get_replica_connection/3`, `get_connection_by_node/2`, and `get_random_connection/1`
  default to `:error` (their existing "no connection" contract, which
  `Redix.Cluster`'s callers already turn into a `%Redix.ConnectionError{}`);
  `Redix.Cluster`'s own `await_topology_discovery/3` defaults its `:ets.member/2`
  check to `false` (falls through to awaiting the Manager, same as "discovery not
  attempted yet"); `Redix.Cluster.KeyResolver`'s `cached_keyspec/2` and
  `cache_missing_keyspecs/3` default to `:no_key`/a no-op (same as an ordinary cache
  miss). No `try/rescue` lives in `Redix.Cluster`'s public entry points themselves —
  every path degrades through the leaf functions' existing return contracts, so a
  multi-node pipeline racing this window still gets the documented **partial**
  per-command error values instead of one blanket top-level error. Reproduced
  deterministically in tests by deleting the wired slot table (and, for the Registry
  case, killing the wired Registry process) rather than racing a real Manager crash.

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
- **Topology refresh is synchronous on the `Manager` process.** `do_refresh_topology/2`
  runs inside the gen_statem callback, so while a refresh is in flight the Manager
  serves no other events. Steady-state commands don't touch the Manager, so this is
  invisible there; it only delays the on-demand `connect_to_node` path (MOVED/ASK to a
  not-yet-known node) and DOWN-triggered restarts during a slow refresh against a
  partially-down cluster, where each unreachable node costs up to the connection
  `:timeout`. Two things bound the damage: `connect_to_node` uses a finite call timeout
  and fails fast (issue #327), and a *steady-state* refresh now bounds its whole sweep to
  a single `:timeout` budget — `do_refresh_topology/2` takes a `deadline` and
  `fetch_cluster_slots/3` stops probing once `past_deadline?/1` trips before the next
  node, so N black-holed nodes can no longer cost N × `:timeout` and block the Manager
  for tens of seconds (issue #335). Callers pass the deadline: reactive/periodic
  refreshes in `:ready` pass `fetch_deadline/1` (finite, `:infinity` `:timeout` opts
  out); the *initial* discovery fetch (sync `init/1` and the `:disconnected` retry)
  passes `:infinity` so startup reliably reaches a reachable seed even past an earlier
  black hole, and because nothing else needs the Manager yet. A skipped node is retried
  on the next refresh, and the first node in a sweep is always tried (the deadline is in
  the future at the start), so a refresh always attempts at least one node. The remaining,
  larger step — moving `fetch_cluster_slots/3` (the network-bound part) into a Task and
  feeding the result back as an event so the Manager never blocks at all — stays deferred
  because it reworks the `await_topology_discovery/2` handshake, which relies on the
  initial fetch being synchronous within the callback.
