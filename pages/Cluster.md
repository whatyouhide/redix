# Redis Cluster

Redix supports [Redis Cluster](https://redis.io/technology/redis-enterprise-cluster-architecture/) through the `Redix.Cluster` module.

## Overview

Redis Cluster distributes data across multiple Redis nodes [using **16384 hash slots**](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#key-distribution-model). Each primary (master) node is responsible for a *subset* of these slots. `Redix.Cluster` takes care of these things transparently:

  * Routing commands to the correct node based on key hash slots.
  * Handling `MOVED` and `ASK` redirections during resharding.
  * Maintaining a topology map of the cluster.
  * Splitting pipelines across nodes and reassembling results.

## Getting Started

The API for `Redix.Cluster` will feel familiar to folks used to `Redix`.

```elixir
# Start a cluster connection with a name and one or more seed nodes.
{:ok, _pid} = Redix.Cluster.start_link(
  name: :my_cluster,
  nodes: ["redis://localhost:7000", "redis://localhost:7001"]
)

# Issue commands across the cluster.
Redix.Cluster.command(:my_cluster, ["SET", "mykey", "myvalue"])
#=> {:ok, "OK"}
Redix.Cluster.command(:my_cluster, ["GET", "mykey"])
#=> {:ok, "myvalue"}
```

## Startup Behavior

Like single-node `Redix` connections, starting a cluster does **not** require any node to be reachable. By default, `Redix.Cluster.start_link/1` returns right away and discovers the cluster topology in the background, retrying with exponential backoff (controlled by the `:backoff_initial` and `:backoff_max` options) until a seed node answers. This means a `Redix.Cluster` in your supervision tree won't crash-loop your application at boot if Redis comes up *after* your app.

Commands issued while the *initial* discovery attempt is in flight **wait for it to complete** (up to their `:timeout`), just like a single `Redix` connection postpones commands while it's connecting — so starting a cluster and issuing commands right away works without retries. Once that first attempt fails (no seed node is reachable), commands stop waiting and return `{:error, %Redix.ConnectionError{reason: :closed}}` until a node becomes reachable.

If you prefer to block until the topology has been discovered—and fail fast if no seed node is reachable—pass `sync_connect: true`:

```elixir
Redix.Cluster.start_link(
  name: :my_cluster,
  nodes: ["redis://localhost:7000"],
  sync_connect: true
)
```

## Pipelines

Pipelines that span multiple hash slots are transparently split across nodes, executed in parallel, and reassembled in the original order:

```elixir
Redix.Cluster.pipeline(cluster, [
  ["SET", "key1", "a"], # Maybe executes on node 1
  ["SET", "key2", "b"], # Maybe executes on node 2
  ["GET", "key1"],      # Same, node 1
  ["GET", "key2"]       # Same, node 2
])
#=> {:ok, ["OK", "OK", "a", "b"]}
```

## Transactions

`MULTI`/`EXEC` transactions require all keys to be in the **same hash slot**. Use [hash tags](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags) to ensure this:

```elixir
# These keys all hash to the same slot because only the "{user:1}" part of the key is hashed:
Redix.Cluster.transaction_pipeline(cluster, [
  ["SET", "{user:1}.name", "Alice"],
  ["SET", "{user:1}.email", "alice@example.com"]
])
#=> {:ok, ["OK", "OK"]}
```

If commands span multiple slots, a `CROSSSLOT` error is returned.

## Redirections

Redis Cluster handles slot migrations transparently:

  * `MOVED`: the slot has permanently moved to another node. Redix updates its topology map and retries the command on the new node.
  * `ASK`: the slot is being migrated. Redix sends `ASKING` followed by the command to the target node without updating the topology map.

Up to 5 redirections are followed before returning an error.

## Limitations

  * **Database `0` only**: Redis Cluster does not support the `SELECT` command. Passing a non-zero `:database` option raises an error.
  * **Primary-only routing**: all commands are routed to primary nodes. Replica reads (`READONLY`) are not yet supported.
  * **No Pub/Sub**: Redis Cluster Pub/Sub has different semantics (messages broadcast to all nodes). Use `Redix.PubSub` with a direct connection instead.
  * **No `noreply_*` functions**: fire-and-forget commands are not supported in cluster mode.
