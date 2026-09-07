# Agent Notes

Low-level Redis/Valkey driver for Elixir. It supports single-node connections, sentinel, pub/sub, and Redis Cluster. Keep higher-level APIs small and consistent with `Redix`.

## Code Conventions

- Use `:gen_statem` for stateful processes. Prefer `:state_functions` when states have distinct behavior.
- Validate options with the NimbleOptions library; see `Redix.StartOptions`.
- Use `:telemetry.execute/3` instead of logging. Document events in `Redix.Telemetry`. Add typespecs to public functions and `!` variants to public command functions.
- Set `@moduledoc false` on internal modules.
- Do not change `CHANGELOG.md` or commit unless the user asks. Leave edits unstaged.

## Cluster

`Redix.Cluster` is the public API and a `:one_for_all` supervisor. It owns a Registry, a DynamicSupervisor for node connections, a `Task.Supervisor` for parallel pipelines, and `Redix.Cluster.Manager` for managing connections and slots (in ETS).

- Require an atom `:name`; derive all resource names from it. Do not add `persistent_term` or PID-based resource lookups.
- `lib/redix/cluster.ex` handles the public API, routing, and `MOVED`/`ASK` redirects. `lib/redix/cluster/manager.ex` handles topology and node connection lifetimes. `Hash` computes CRC16 slots and hash tags; `CommandParser` finds command keys; `KeyResolver` handles server-assisted key lookup.

## Tests

Use `start_supervised!` and unique names in async tests. Tag cluster integration
tests with `@moduletag :cluster`. Use the fake RESP node tests for controlled protocol failures.

```sh
docker compose up -d
mix test
mix test test/redix/cluster/
mix test test/redix/cluster_test.exs
```

The Docker cluster in `test/docker/cluster/` has nine nodes on ports 7000-7008: three primaries and six replicas. Redis chooses roles during setup. Tests must handle READONLY errors when flushing replicas.

For host port conflicts, set the same environment variable for Docker and tests:

```sh
REDIX_BASE_PORT=6479 docker compose up -d base
REDIX_BASE_PORT=6479 mix test
```

See `docker-compose.yml` and `test/support/test_ports.ex` for defaults and other overrides.

Sentinel and cluster ports must stay fixed: 6381-6383, 26379-26383, and 7000-7008. Their topology replies contain these addresses, so host port remapping breaks client redirects.
