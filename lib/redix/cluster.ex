defmodule Redix.Cluster do
  @moduledoc """
  Main API for using [Redis Cluster](https://redis.io/docs/management/scaling/).

  For a higher-level guide, see the [Cluster guide](cluster.html).

  The design of this module follows the same low-level philosophy as the rest of Redix:
  it builds on existing `Redix` connections (one per cluster node), uses ETS for fast slot
  lookups, and mostly mirrors the `Redix` API.

  ## Usage

  You can start connections to a Redis Cluster similarly to how you'd start a single
  `Redix` connection, but specifying a *list of seed nodes* and a *name*:

      {:ok, cluster} = Redix.Cluster.start_link(
        name: :my_cluster,
        nodes: ["redis://localhost:7000"]
      )

      Redix.Cluster.command(:my_cluster, ["SET", "mykey", "myvalue"])
      #=> {:ok, "OK"}
      Redix.Cluster.command(:my_cluster, ["GET", "mykey"])
      #=> {:ok, "myvalue"}

  Like single-node `Redix` connections, the cluster starts even if no seed node is
  currently reachable: the topology is discovered in the background, retrying with
  exponential backoff. Commands issued while the *initial* discovery attempt is in
  flight wait for it to complete (just like a single Redix connection postpones
  commands while it's connecting), so the common "start the cluster, issue a
  command right away" pattern works without retries. If that first attempt fails,
  commands return `{:error, %Redix.ConnectionError{reason: :closed}}` until a seed
  node becomes reachable. Pass `sync_connect: true` to `start_link/1` to block
  until the topology has been discovered instead.

  ### Pipelines

  Pipelines that span multiple hash slots are transparently split across nodes,
  executed in parallel, and reassembled in the original order:

      Redix.Cluster.pipeline(:my_cluster, [
        ["SET", "key1", "a"],
        ["SET", "key2", "b"],
        ["GET", "key1"],
        ["GET", "key2"]
      ])
      #=> {:ok, ["OK", "OK", "a", "b"]}

  ### Transactions

  `MULTI`/`EXEC` transactions require all keys to be in the same hash slot.
  Use [hash tags](https://redis.io/docs/reference/cluster-spec/#hash-tags) to
  ensure this:

      Redix.Cluster.transaction_pipeline(:my_cluster, [
        ["SET", "{user:1}.name", "Alice"],
        ["SET", "{user:1}.email", "alice@example.com"]
      ])

  ### Reading from replicas

  By default all commands are routed to primaries. To allow reads from replicas,
  start the cluster with `read_from_replicas: true` (which opens and supervises a
  connection to each replica, issuing `READONLY` on it) and pass a `:route` option
  **per call**:

      Redix.Cluster.start_link(
        name: :my_cluster,
        nodes: ["redis://localhost:7000"],
        read_from_replicas: true
      )

      # Read from a replica for the key's slot, failing if none is reachable.
      Redix.Cluster.command(:my_cluster, ["GET", "mykey"], route: :replica)

      # Prefer a replica but fall back to the primary if none is reachable.
      Redix.Cluster.command(:my_cluster, ["GET", "mykey"], route: :prefer_replica)

  See `command/3` for the full list of `:route` values.

  ### Limitations

    * Only database `0` is supported (Redis Cluster does not support `SELECT`).
    * Pub/Sub is not supported through the cluster interface (*yet*).
    * The `:noreply_*` functions are not supported in cluster mode.

  ## Telemetry

  `Redix.Cluster` emits cluster-specific Telemetry events for topology changes
  and redirections. See `Redix.Telemetry` for details.
  """
  @moduledoc since: "1.6.0"

  alias Redix.Cluster.{CommandParser, Hash, Manager}

  @typedoc """
  A node endpoint.

  It can be either a Redis URI string (such as `"redis://localhost:7000"`)
  or a keyword list with `:host` and `:port` keys
  (such as `[host: "localhost", port: 7000]`).

  Only the host and port of a seed are used to discover the cluster, with one
  exception: the `rediss://` scheme enables TLS for *every* node connection.
  Credentials (`user:pass@`) and a non-zero database in a seed URI are not
  supported and raise. See `start_link/1`.
  """
  @type endpoint() :: String.t() | [{:host, String.t()} | {:port, :inet.port_number()}]

  @max_redirections 5

  @default_timeout 5_000

  # Upper bound on the per-cluster command cache (issue #329). Real Redis exposes a
  # few hundred commands, so genuine traffic never approaches this; the cap only
  # stops the cache from growing without bound on a pathological stream of distinct
  # unknown command names. Past the cap, uncached commands keep resolving per-call
  # (to a random node), exactly as they did before any caching existed.
  @command_cache_max_size 2048

  @valid_routes [:primary, :replica, :prefer_replica]

  node_as_keyword_opts_schema = [
    host: [type: :string, default: "localhost"],
    port: [type: {:in, 0..65_535}, default: 6379]
  ]

  @node_as_keyword_opts_schema NimbleOptions.new!(node_as_keyword_opts_schema)

  start_link_opts_schema = [
    name: [
      type: :atom,
      required: true,
      doc: """
      An atom to register the cluster process under.
      All internal resources (ETS tables, Registry, connection supervisor) are named
      deterministically based on this name. You use this name to issue commands:

          Redix.Cluster.start_link(name: :my_cluster, nodes: [...])
          Redix.Cluster.command(:my_cluster, ["GET", "key"])
      """
    ],
    nodes: [
      type: {:custom, __MODULE__, :__parse_nodes__, []},
      type_doc: "non-empty list of `t:endpoint/0`",
      required: true,
      doc: """
      A non-empty list of seed nodes to connect to. Only **one reachable node** is needed:
      the full cluster topology is discovered automatically via `CLUSTER SLOTS`.
      """
    ],
    sync_connect: [
      type: :boolean,
      default: false,
      doc: """
      Whether to discover the initial cluster topology *before* or *after*
      `start_link/1` returns. When `false` (the default), `start_link/1` returns right
      away and the topology is fetched in the background, retrying with exponential
      backoff (driven by the `:backoff_initial` and `:backoff_max` options) until a
      seed node answers. Commands issued while the *initial* discovery attempt is in
      flight wait for it to complete (up to their `:timeout`); once that attempt
      fails, they return `{:error, %Redix.ConnectionError{reason: :closed}}` until a
      seed node becomes reachable. When `true`, `start_link/1` blocks until the topology has been
      fetched, and returns an error if no seed node is reachable. This mirrors the
      `:sync_connect` option of `Redix.start_link/1`.
      """
    ],
    topology_refresh_interval: [
      type: :timeout,
      default: 30_000,
      doc: "How often (in milliseconds) to refresh the cluster topology."
    ],
    read_from_replicas: [
      type: :boolean,
      default: false,
      doc: """
      if `true`, the cluster also opens (and supervises) a connection to every replica
      node discovered via `CLUSTER SLOTS`, issuing `READONLY` on each so it can serve
      reads. This is required to use `route: :replica` or `route: :prefer_replica` (see
      `command/3`). Defaults to `false`, in which case only primaries are connected.
      """
    ]
  ]

  @start_link_opts_schema NimbleOptions.new!(start_link_opts_schema)
  @start_link_opts_keys Keyword.keys(start_link_opts_schema)

  @doc """
  Starts a connection to a Redis Cluster.

  ## Options

  These are cluster-specific options:

  #{NimbleOptions.docs(@start_link_opts_schema)}

  All other standard `Redix` connection options (`:password`, `:ssl`, `:socket_opts`,
  `:timeout`, and so on) are passed through to *each underlying node connection*. The
  exceptions are `:sentinel` and `:exit_on_disconnection`, which are not supported in
  cluster mode (the cluster supervises node connections and handles disconnections
  itself).

  ## TLS and credentials in seed URIs

  The full topology is discovered from a seed node, and every node connection
  (the seeds and the nodes discovered from them) is made with a single shared
  configuration. Only the host and port of a seed are used to reach it, so the
  other parts of a seed URI are handled specially rather than silently dropped:

    * The `rediss://` scheme enables TLS for *every* node connection (equivalent
      to `ssl: true`), since TLS in a cluster is all-or-nothing. Combining a
      `rediss://` seed with an explicit `ssl: false` raises an error,
      so a `rediss://` seed is never silently downgraded to plain TCP.

    * Credentials in a seed URI (`redis://user:pass@host`) raise an error: every
      node is authenticated with the shared configuration, so pass `:username`
      and `:password` as connection options instead.

    * A non-zero database in a seed URI raises an error, as a cluster
      only supports database `0`.

  ## Examples

      Redix.Cluster.start_link(
        name: :my_cluster,
        nodes: ["redis://localhost:7000", "redis://localhost:7001"]
      )

      Redix.Cluster.start_link(
        name: :my_cluster,
        nodes: [[host: "redis1.example.com", port: 6379]],
        password: "secret"
      )

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    {cluster_opts, conn_opts} = Keyword.split(opts, @start_link_opts_keys)
    cluster_opts = NimbleOptions.validate!(cluster_opts, @start_link_opts_schema)

    name = Keyword.fetch!(cluster_opts, :name)
    parsed_nodes = Keyword.fetch!(cluster_opts, :nodes)
    refresh_interval = Keyword.fetch!(cluster_opts, :topology_refresh_interval)
    read_from_replicas = Keyword.fetch!(cluster_opts, :read_from_replicas)
    sync_connect = Keyword.fetch!(cluster_opts, :sync_connect)

    if Keyword.has_key?(conn_opts, :sentinel) do
      raise ArgumentError, "Sentinel connections are not supported in cluster mode"
    end

    if Keyword.has_key?(conn_opts, :exit_on_disconnection) do
      raise ArgumentError, """
      the :exit_on_disconnection option is not supported in cluster mode: node \
      connections are supervised by the cluster, which handles disconnections and \
      topology changes itself\
      """
    end

    # Each parsed seed is a full set of connection options. :host and :port identify
    # the individual seed; everything else (the rediss:// scheme, userinfo, database)
    # is cluster-wide config, since every node connection shares one configuration.
    # Fold those into conn_opts so a `rediss://` seed isn't silently downgraded to
    # plain TCP and URI credentials aren't dropped (issue #322).
    seed_nodes =
      Enum.map(parsed_nodes, fn opts ->
        {Keyword.get(opts, :host, "localhost"), Keyword.get(opts, :port, 6379)}
      end)

    conn_opts = merge_seed_node_opts!(conn_opts, parsed_nodes)

    conn_opts = Redix.StartOptions.sanitize(:redix, conn_opts)

    # The :database option type allows both the integer and the string form, so
    # accept "0" as well as 0 (both mean database 0, which is the only one a cluster
    # supports) rather than rejecting the string form with a confusing error.
    case Keyword.fetch(conn_opts, :database) do
      {:ok, db} when db in [0, "0", nil] ->
        :ok

      {:ok, db} ->
        raise ArgumentError, "Redis Cluster only supports database 0, got: #{inspect(db)}"

      :error ->
        :ok
    end

    children = [
      {Registry, keys: :unique, name: registry_name(name)},
      {DynamicSupervisor, name: pool_name(name), strategy: :one_for_one},
      {Task.Supervisor, name: task_supervisor_name(name)},
      {Manager,
       name: manager_name(name),
       cluster_name: name,
       seed_nodes: seed_nodes,
       pool_supervisor: pool_name(name),
       conn_opts: conn_opts,
       refresh_interval: refresh_interval,
       read_from_replicas: read_from_replicas,
       sync_connect: sync_connect,
       table_name: slot_table_name(name),
       command_cache_table: command_cache_name(name),
       registry: registry_name(name)}
    ]

    Supervisor.start_link(children, name: name, strategy: :one_for_all)
  end

  @doc """
  Returns a child spec for use in supervision trees.

  ## Examples

      children = [
        {Redix.Cluster, name: :my_cluster, nodes: ["redis://localhost:7000"]}
      ]

  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    %{
      id: Keyword.fetch!(opts, :name),
      type: :supervisor,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  Stops the cluster and all its connections.

  ## Examples

      Redix.Cluster.stop(:my_cluster)

  """
  @spec stop(atom(), timeout()) :: :ok
  def stop(cluster, timeout \\ 60_000)
      when is_atom(cluster) and ((is_integer(timeout) and timeout >= 0) or timeout == :infinity) do
    Supervisor.stop(cluster, :normal, timeout)
  end

  @doc """
  Issues a command on the Redis Cluster.

  The command is routed to the correct node based on the key's hash slot.
  If the command has no key (`PING`), it is sent to a random node.

  Returns `{:ok, response}` or `{:error, reason}`.

  ## Options

    * `:timeout` - request timeout in milliseconds. Defaults to `5_000`.

    * `:route` - where to send the command. One of:

      * `:primary` (default) - always route to the slot's primary.
      * `:replica` - route to a replica for the slot, failing with a connection
        error if none is reachable.
      * `:prefer_replica` - route to a replica if one is reachable, otherwise fall
        back to the primary.

      `:replica` and `:prefer_replica` require the cluster to have been started with
      `read_from_replicas: true`. Use them only for reads: a write routed to a replica
      is transparently redirected (via `MOVED`) to the primary.

  ## Examples

      Redix.Cluster.command(:my_cluster, ["SET", "mykey", "foo"])
      #=> {:ok, "OK"}

      Redix.Cluster.command(:my_cluster, ["GET", "mykey"])
      #=> {:ok, "foo"}

      Redix.Cluster.command(:my_cluster, ["GET", "mykey"], route: :prefer_replica)
      #=> {:ok, "foo"}

  """
  @spec command(atom(), Redix.command(), keyword()) ::
          {:ok, Redix.Protocol.redis_value()}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def command(cluster, command, opts \\ []) when is_atom(cluster) do
    with {:ok, [result]} <- execute_pipeline(cluster, [command], opts) do
      case result do
        %Redix.Error{} = error -> {:error, error}
        other -> {:ok, other}
      end
    end
  end

  @doc """
  Same as `command/3` but raises on errors.
  """
  @spec command!(atom(), Redix.command(), keyword()) :: Redix.Protocol.redis_value()
  def command!(cluster, command, opts \\ []) do
    case command(cluster, command, opts) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  @doc """
  Issues a pipeline of commands on the Redis Cluster.

  Commands are grouped by target node based on key hash slots, sent *in parallel*
  to the respective nodes, and results are reassembled in the original order.

  Returns `{:ok, results}` or `{:error, reason}`.

  If `commands` is an empty list (`[]`) then an `ArgumentError` exception is
  raised right away, matching `Redix.pipeline/3`.

  ## Partial failures across nodes

  When a pipeline spans multiple nodes and only *some* of those nodes fail (for example, it times out or its task crashes), the call still returns `{:ok, results}`.
  The positions of the commands routed to a failed node are filled with the relevant
  error *value* (a `%Redix.ConnectionError{}`, or a `%Redix.Error{}` for a Redis-level
  error) at their original indices, while the results from nodes that succeeded stay
  visible. This extends Redix's "errors are values" philosophy to the cross-node connection-error case, so a failure
  affecting one node no longer discards the work that committed on the others.

  This is a deliberate extension of `Redix.pipeline/3`, which fails the whole call
  with `{:error, reason}` on a connection error. There is no partial-failure analog
  for a single node, so a pipeline that targets a single node (all keys in one slot
  group) still returns `{:error, reason}` on a connection error, matching
  `Redix.pipeline/3`.

  ## Options

    * `:timeout` - request timeout in milliseconds. Defaults to `5_000`.

    * `:route` - where to send the pipeline. See `command/3` for the accepted
      values. The option applies to the whole pipeline: every command uses the
      same routing choice.

  ## Examples

      Redix.Cluster.pipeline(:my_cluster, [["SET", "a", "1"], ["SET", "b", "2"]])
      #=> {:ok, ["OK", "OK"]}

  """
  @spec pipeline(atom(), [Redix.command()], keyword()) ::
          {:ok, [Redix.Protocol.redis_value()]}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def pipeline(cluster, commands, opts \\ []) when is_atom(cluster) and is_list(commands) do
    execute_pipeline(cluster, commands, opts)
  end

  @doc """
  Same as `pipeline/3` but raises on errors.
  """
  @spec pipeline!(atom(), [Redix.command()], keyword()) :: [Redix.Protocol.redis_value()]
  def pipeline!(cluster, commands, opts \\ []) do
    case pipeline(cluster, commands, opts) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  @doc """
  Executes a `MULTI`/`EXEC` transaction on the Redis Cluster.

  All commands must target the same hash slot (use hash tags to ensure this).
  Returns `{:error, %Redix.Error{message: "CROSSSLOT" <> _}}` if commands
  span multiple slots. At least one command must contain a key to route the
  transaction on; a pipeline of only keyless commands (such as `PING`) returns
  an error since there's no slot to send it to.

  Transactions always run on the slot's primary; passing `route:` anything other
  than `:primary` raises an `ArgumentError`.

  Like `command/3` and `pipeline/3`, a "transaction" follows `MOVED`/`ASK`
  redirections: since all commands target one slot, a redirect means the whole
  transaction belongs on another node, so the entire `MULTI`/`EXEC` is re-run
  there (bounded by the internal redirection limit). A `MOVED` also triggers a
  reactive topology refresh, so a transaction-only workload self-heals after a
  failover instead of failing until the next periodic refresh.

  ## Options

    * `:timeout` - request timeout in milliseconds. Defaults to `5_000`.

  ## Examples

      Redix.Cluster.transaction_pipeline(:my_cluster, [
        ["SET", "{user:1}.name", "Alice"],
        ["SET", "{user:1}.email", "alice@example.com"]
      ])
      #=> {:ok, ["OK", "OK"]}

  """
  @spec transaction_pipeline(atom(), [Redix.command()], keyword()) ::
          {:ok, [Redix.Protocol.redis_value()]}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def transaction_pipeline(cluster, [_ | _] = commands, opts \\ []) when is_atom(cluster) do
    case validate_route!(opts) do
      :primary ->
        :ok

      other ->
        raise ArgumentError,
              "transaction_pipeline/3 only supports route: :primary, got: #{inspect(other)} " <>
                "(MULTI/EXEC must run on the slot's primary)"
    end

    opts = Keyword.delete(opts, :route)
    slot_table = slot_table_name(cluster)
    registry = registry_name(cluster)

    await_topology_discovery(cluster, slot_table, opts)

    # All commands in a transaction must target the same slot.
    indexed_commands =
      commands
      |> Enum.with_index()
      |> Enum.map(fn {cmd, idx} -> {idx, cmd, command_slot(cmd)} end)

    # A command outside the static table comes back as :unknown and is resolved
    # against the server. If that resolution can't reach any node it degrades to
    # :no_slot—indistinguishable here from a genuinely keyless command—so
    # remember whether any command *needed* the server before resolving.
    had_unknown? = Enum.any?(indexed_commands, fn {_idx, _cmd, slot} -> slot == :unknown end)

    slots =
      indexed_commands
      |> resolve_unknown_slots(registry, command_cache_name(cluster))
      |> Enum.map(fn {_idx, _cmd, slot} -> slot end)
      |> Enum.reject(&(&1 == :no_slot))
      |> Enum.uniq()

    case slots do
      [slot] ->
        case Manager.get_connection(slot_table, registry, slot) do
          {:ok, conn} ->
            execute_transaction(
              cluster,
              conn,
              commands,
              opts,
              @max_redirections,
              _asking? = false
            )

          :error ->
            {:error, %Redix.ConnectionError{reason: :closed}}
        end

      [] ->
        empty_transaction_slots_error(had_unknown?, registry)

      _multiple ->
        {:error, %Redix.Error{message: "CROSSSLOT Keys in request don't hash to the same slot"}}
    end
  end

  # No command in the transaction yielded a slot. If a command needed server-side
  # resolution (an :unknown command) and no node is currently reachable, the keys
  # we *do* have just couldn't be resolved — surface that as a connection error
  # rather than the misleading "requires a key" message (A4). A genuinely keyless
  # transaction (or one resolved against a reachable cluster) keeps that message.
  defp empty_transaction_slots_error(had_unknown?, registry) do
    if had_unknown? and Manager.get_random_connection(registry) == :error do
      {:error, %Redix.ConnectionError{reason: :closed}}
    else
      {:error,
       %Redix.Error{
         message: "ERR transaction_pipeline requires at least one command with a key to route on"
       }}
    end
  end

  @doc """
  Same as `transaction_pipeline/3` but raises on errors.
  """
  @spec transaction_pipeline!(atom(), [Redix.command()], keyword()) :: [
          Redix.Protocol.redis_value()
        ]
  def transaction_pipeline!(cluster, commands, opts \\ []) do
    case transaction_pipeline(cluster, commands, opts) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  ## Transaction implementation

  # Runs a MULTI/EXEC transaction on `conn`, following a MOVED/ASK redirection to
  # its target. A cluster transaction is pinned to a single slot, so a redirect on
  # any queued command means the *whole* transaction belongs on one other node —
  # we re-run the entire MULTI/EXEC there, bounded by the same @max_redirections
  # budget command/3 and pipeline/3 use. This is what lets a transaction-only
  # workload self-heal after a failover instead of failing until the next periodic
  # topology refresh (issue #321). We issue the pipeline directly rather than via
  # Redix.transaction_pipeline/3 because that only inspects the EXEC reply, while a
  # redirect surfaces in the (otherwise hidden) queueing replies.
  defp execute_transaction(cluster, conn, commands, opts, remaining, asking?) do
    # ASKING must precede MULTI: Redis preserves the ASKING flag for the whole
    # transaction (it only clears it outside a MULTI), so one ASKING covers every
    # queued command.
    prefix = if asking?, do: [["ASKING"], ["MULTI"]], else: [["MULTI"]]

    case pipeline_catching_exit(conn, prefix ++ commands ++ [["EXEC"]], opts) do
      {:ok, responses} ->
        # Replies line up as [prefix..., queueing replies..., EXEC reply]. A
        # wrong-slot command is rejected with MOVED/ASK at *queue* time (which
        # aborts EXEC with EXECABORT), so the redirection lives among the queueing
        # replies, not in the EXEC reply.
        queue_responses = responses |> Enum.drop(length(prefix)) |> Enum.drop(-1)

        case Enum.find_value(queue_responses, &parse_redirection/1) do
          {type, slot, host, port} ->
            follow_transaction_redirect(
              cluster,
              type,
              slot,
              host,
              port,
              commands,
              opts,
              remaining
            )

          nil ->
            case List.last(responses) do
              %Redix.Error{} = error -> {:error, error}
              other -> {:ok, other}
            end
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp follow_transaction_redirect(_cluster, _type, _slot, _host, _port, _commands, _opts, 0) do
    {:error, %Redix.ConnectionError{reason: :too_many_redirections}}
  end

  defp follow_transaction_redirect(cluster, type, slot, host, port, commands, opts, remaining) do
    # MOVED means the slot's ownership changed (typically a failover); trigger the
    # same reactive refresh command/3 does so future routing is corrected. ASK is a
    # transient per-request migration hint and signals no topology change.
    if type == :moved, do: Manager.refresh_topology(manager_name(cluster))

    :telemetry.execute([:redix, :cluster, :redirection], %{}, %{
      cluster: cluster,
      type: type,
      slot: slot,
      target_address: "#{host}:#{port}"
    })

    # The target may not be in the Registry yet (mid-resharding for MOVED, a
    # brand-new node serving zero slots for ASK), so connect on demand — the
    # redirect address is authoritative. Mirrors handle_moved_redirect/6.
    conn =
      case Manager.get_connection_by_node(registry_name(cluster), {host, port}) do
        {:ok, conn} ->
          {:ok, conn}

        :error ->
          Manager.connect_to_node(manager_name(cluster), {host, port}, connect_timeout(opts))
      end

    case conn do
      {:ok, conn} ->
        execute_transaction(cluster, conn, commands, opts, remaining - 1, type == :ask)

      {:error, _reason} ->
        {:error, %Redix.ConnectionError{reason: :closed}}
    end
  end

  ## Pipeline implementation

  defp execute_pipeline(_cluster, [] = _commands, _opts) do
    raise ArgumentError, "no commands passed to the pipeline"
  end

  defp execute_pipeline(cluster, commands, opts) do
    route = validate_route!(opts)
    opts = Keyword.delete(opts, :route)

    slot_table = slot_table_name(cluster)
    registry = registry_name(cluster)

    await_topology_discovery(cluster, slot_table, opts)

    # Group commands by target node
    indexed_commands =
      commands
      |> Enum.with_index()
      |> Enum.map(fn {cmd, idx} ->
        slot = command_slot(cmd)
        {idx, cmd, slot}
      end)
      |> resolve_unknown_slots(registry, command_cache_name(cluster))

    # Group by slot -> node. Returns a list of {conn, [{idx, cmd}]} tuples.
    groups = group_by_node(slot_table, registry, indexed_commands, route)

    # Execute each group on its target node
    results = execute_groups(cluster, groups, opts)

    case results do
      {:error, _} = error ->
        error

      results when is_list(results) ->
        # Reassemble results in original order
        sorted =
          results
          |> List.flatten()
          |> Enum.sort_by(fn {idx, _} -> idx end)
          |> Enum.map(fn {_, result} -> result end)

        {:ok, sorted}
    end
  end

  defp group_by_node(slot_table, registry, indexed_commands, route) do
    # Resolve a single connection per distinct slot and reuse it for every command
    # on that slot, so same-slot commands land in one group (and thus one pipeline).
    # This matters for replica routes: resolve_connection/4 picks a random replica
    # per call, so without "memoizing", two reads on the same slot could resolve to
    # different replica pids and be split across parallel tasks (issue #315).
    resolved_by_slot =
      indexed_commands
      |> Enum.map(fn {_idx, _cmd, slot} -> slot end)
      |> Enum.uniq()
      |> Map.new(fn
        :no_slot -> {:no_slot, :random}
        slot -> {slot, resolve_connection(slot_table, registry, slot, route)}
      end)

    indexed_commands
    |> Enum.group_by(fn {_idx, _cmd, slot} -> Map.fetch!(resolved_by_slot, slot) end)
    |> Enum.map(fn {node_key, commands} ->
      # When resolution fails, carry the appropriate connection error in the conn slot
      # (rather than nil) so execute_groups/3 can surface it for both single- and
      # multi-group pipelines without re-deriving the reason.
      conn =
        case node_key do
          {:ok, pid} ->
            pid

          :random ->
            case Manager.get_random_connection(registry) do
              {:ok, pid} -> pid
              :error -> %Redix.ConnectionError{reason: :closed}
            end

          :error ->
            no_connection_error(route)
        end

      cmds = Enum.map(commands, fn {idx, cmd, _slot} -> {idx, cmd} end)
      {conn, cmds}
    end)
  end

  # The error to surface when no connection can be resolved for a slot. A `:replica`
  # route that resolves to nothing means no replica was reachable — most often because
  # the cluster wasn't started with `read_from_replicas: true` — so a descriptive reason
  # saves debugging time over a bare `:closed`. Any other route resolving to nothing is a
  # genuinely closed/absent connection.
  defp no_connection_error(:replica), do: %Redix.ConnectionError{reason: :no_replica_connection}
  defp no_connection_error(_route), do: %Redix.ConnectionError{reason: :closed}

  # No connection could be resolved for this group (for example `route: :replica`
  # when no replica is reachable). group_by_node/4 put the right `%Redix.ConnectionError{}`
  # in the conn slot; surface it rather than crashing on `Redix.pipeline(error, ...)`.
  defp execute_groups(_cluster, [{%Redix.ConnectionError{} = error, _cmds}], _opts) do
    {:error, error}
  end

  # If there's only a single group to execute, we don't need parallel tasks.
  defp execute_groups(cluster, [{conn, cmds}], opts) when is_pid(conn) do
    execute_and_handle_redirections(cluster, conn, cmds, opts, @max_redirections)
  end

  defp execute_groups(cluster, groups, opts) do
    task_supervisor = task_supervisor_name(cluster)

    groups_with_tasks =
      Enum.map(groups, fn {conn, cmds} ->
        task =
          case conn do
            %Redix.ConnectionError{} = error ->
              Task.completed({:error, error})

            conn when is_pid(conn) ->
              # async_nolink, *not* async: a crashing task must not take the caller down
              # with it via a link (issue #317). With async_nolink, Task.yield_many/2
              # reports the crash as {:exit, reason}, which __collect_group_results__
              # turns into a per-command error value instead of letting the link kill us
              # before yield_many even returns.
              Task.Supervisor.async_nolink(task_supervisor, fn ->
                execute_and_handle_redirections(cluster, conn, cmds, opts, @max_redirections)
              end)
          end

        {cmds, task}
      end)

    {group_cmds, tasks} = Enum.unzip(groups_with_tasks)

    tasks
    |> Task.yield_many(
      timeout: redirect_aware_timeout(Keyword.get(opts, :timeout, @default_timeout)),
      on_timeout: :kill_task
    )
    |> Enum.zip(group_cmds)
    |> __collect_group_results__()
  end

  # Collapses the `Task.yield_many/2` output into a list of per-group result lists
  # (issue #316). A failing group does *not* discard the whole pipeline: its commands'
  # positions are filled with the relevant error *value* (a `%Redix.ConnectionError{}`
  # or `%Redix.Error{}`) at their original indices. Successful groups'
  # results stay visible. Each entry pairs a `Task.yield_many/2` result with the group's
  # `[{idx, cmd}]` list (so failures can be mapped back to the right indices) and is one
  # of the shapes `yield_many` reports per task:
  #
  #   * `{task, {:ok, result}}` — the group finished; `result` is its `[{idx, value}]`
  #     list or an `{:error, _}` tuple returned by `execute_and_handle_redirections/5`.
  #   * `{task, {:exit, reason}}` — the group's task crashed. The raw exit reason
  #     (often `{exception, stacktrace}`) is *not* propagated as the `%Redix.ConnectionError{}`
  #     reason: that field is typed `atom() | {:wrong_role, binary()}`, so a crashed
  #     group surfaces as `:disconnected` (the request was in flight when it died).
  #   * `{task, nil}` — the group timed out and was killed (`on_timeout: :kill_task`).
  #
  # Public (with a name-mangled name) only so it can be unit-tested without a live
  # cluster; see issues #304 and #316.
  @doc false
  def __collect_group_results__(results_with_cmds) do
    Enum.map(results_with_cmds, fn {entry, cmds} ->
      case entry do
        {_task, {:ok, {:error, error}}} ->
          fill_group_with_error(cmds, error)

        {_task, {:ok, results}} ->
          results

        {_task, {:exit, _reason}} ->
          fill_group_with_error(cmds, %Redix.ConnectionError{reason: :disconnected})

        {_task, nil} ->
          fill_group_with_error(cmds, %Redix.ConnectionError{reason: :timeout})
      end
    end)
  end

  # Turns a failed group into `[{idx, error}]`, so the error surfaces as a per-command
  # value at each of the group's original indices instead of failing the whole pipeline.
  defp fill_group_with_error(cmds, error) do
    Enum.map(cmds, fn {idx, _cmd} -> {idx, error} end)
  end

  # A group's task may issue up to `1 + @max_redirections` sequential pipelines (the
  # initial request plus a MOVED/ASK chain), each bounded by the per-call `:timeout`.
  # `Task.yield_many/2` is only a backstop against a wedged task, so it must allow the
  # whole chain its per-hop timeout: budgeting just `:timeout` would kill a redirected
  # group even though every hop stayed within it, while the single-node path (which
  # never goes through yield_many) lets the same chain run to completion (A3). A wedged
  # task can't actually reach this bound — each inner pipeline times out at `:timeout`
  # and returns an error value — so this only matters for genuine redirect work.
  defp redirect_aware_timeout(:infinity) do
    :infinity
  end

  defp redirect_aware_timeout(timeout) when is_integer(timeout) do
    timeout * (@max_redirections + 1)
  end

  # Redix.pipeline/3 *exits* the caller with {:redix_exited_during_call, reason} when the
  # connection pid is already dead (lib/redix/connection.ex). For single-node Redix the
  # user owns the pid, so an exit is acceptable; in cluster mode the pid is internal and
  # its death is a routine topology event — `ensure_connections` terminates the connection
  # of every node that leaves the cluster. A command that races a scale-in/reshard (the
  # Registry lookup returns a pid, the Manager terminates it, then we issue the pipeline)
  # would otherwise exit the *calling* process instead of returning an error value. Catch
  # that exit and turn it into a connection-error value, matching how every other
  # unreachable-node case surfaces here (issue #317).
  defp pipeline_catching_exit(conn, commands, opts) do
    Redix.pipeline(conn, commands, opts)
  catch
    :exit, {:redix_exited_during_call, _reason} ->
      {:error, %Redix.ConnectionError{reason: :closed}}
  end

  # Same exit guard as pipeline_catching_exit/3, for the single-command COMMAND INFO
  # issued during unknown-slot resolution (A1).
  defp command_catching_exit(conn, command) do
    Redix.command(conn, command)
  catch
    :exit, {:redix_exited_during_call, _reason} ->
      {:error, %Redix.ConnectionError{reason: :closed}}
  end

  defp execute_and_handle_redirections(cluster, conn, cmds, opts, remaining) do
    commands = Enum.map(cmds, fn {_idx, cmd} -> cmd end)

    case pipeline_catching_exit(conn, commands, opts) do
      {:ok, results} ->
        indexed_results = Enum.zip(cmds, results) |> Enum.map(fn {{idx, _}, r} -> {idx, r} end)
        follow_redirections(cluster, cmds, indexed_results, opts, remaining)

      {:error, _reason} = error ->
        error
    end
  end

  # Inspects each result for a MOVED/ASK redirection and, for any found, re-issues
  # the affected commands against their redirect targets, following further hops
  # recursively. This is the single place the redirection budget (`remaining`) is
  # decremented, so both MOVED and ASK chains share the same bound. Returns a list
  # of `{idx, result}` tuples, or `{:error, reason}` if a hop fails.
  defp follow_redirections(cluster, cmds, indexed_results, opts, remaining) do
    {redirect_cmds, final_results} =
      Enum.reduce(indexed_results, {%{}, []}, fn {idx, result}, {redirects, finals} ->
        case parse_redirection(result) do
          {type, slot, host, port} when type in [:moved, :ask] ->
            if type == :moved, do: Manager.refresh_topology(manager_name(cluster))

            :telemetry.execute([:redix, :cluster, :redirection], %{}, %{
              cluster: cluster,
              type: type,
              slot: slot,
              target_address: "#{host}:#{port}"
            })

            redirect_key = {type, host, port}
            cmd = Enum.find(cmds, fn {i, _} -> i == idx end)

            redirects = Map.update(redirects, redirect_key, [cmd], &[cmd | &1])
            {redirects, finals}

          nil ->
            {redirects, [{idx, result} | finals]}
        end
      end)

    cond do
      map_size(redirect_cmds) == 0 ->
        final_results

      remaining <= 0 ->
        {:error, %Redix.ConnectionError{reason: :too_many_redirections}}

      true ->
        redirect_results =
          Enum.flat_map(redirect_cmds, fn {redirect_key, redirect_cmds_list} ->
            redirect_cmds_list = Enum.reverse(redirect_cmds_list)

            case redirect_key do
              {:moved, host, port} ->
                handle_moved_redirect(
                  cluster,
                  host,
                  port,
                  redirect_cmds_list,
                  opts,
                  remaining - 1
                )

              {:ask, host, port} ->
                handle_ask_redirect(cluster, host, port, redirect_cmds_list, opts, remaining - 1)
            end
          end)

        case Enum.find(redirect_results, &match?({:error, _}, &1)) do
          nil -> final_results ++ redirect_results
          {:error, _} = error -> error
        end
    end
  end

  defp handle_moved_redirect(cluster, host, port, cmds, opts, remaining) do
    registry = registry_name(cluster)

    # Fast path: the target node is already known. If it isn't (for example a
    # node added mid-resharding that the topology refresh hasn't caught up to
    # yet), connect on demand via the Manager — MOVED is authoritative, so we
    # trust the address rather than surfacing a fake "unreachable" error.
    conn =
      case Manager.get_connection_by_node(registry, {host, port}) do
        {:ok, conn} ->
          {:ok, conn}

        :error ->
          Manager.connect_to_node(manager_name(cluster), {host, port}, connect_timeout(opts))
      end

    case conn do
      {:ok, conn} ->
        case execute_and_handle_redirections(cluster, conn, cmds, opts, remaining) do
          {:error, _} = error -> [error]
          results -> results
        end

      {:error, _reason} ->
        # The redirect target is unreachable. Surface an honest connection error
        # rather than a fabricated %Redix.Error{} that masquerades as a server reply
        # (and starts with "MOVED ", which redirect-parsing user code could misread).
        Enum.map(cmds, fn {idx, _cmd} ->
          {idx, %Redix.ConnectionError{reason: :closed}}
        end)
    end
  end

  defp handle_ask_redirect(cluster, host, port, cmds, opts, remaining) do
    registry = registry_name(cluster)

    # The classic ASK scenario is a slot migrating to a brand-new node: a node
    # serving zero slots doesn't appear in CLUSTER SLOTS, so it has no Registry
    # entry and no topology refresh will ever connect it. Like MOVED, fall back
    # to connecting on demand — ASK targets are cluster members, and the next
    # `ensure_connections` adopts or terminates the connection (issue #319).
    conn =
      case Manager.get_connection_by_node(registry, {host, port}) do
        {:ok, conn} ->
          {:ok, conn}

        :error ->
          Manager.connect_to_node(manager_name(cluster), {host, port}, connect_timeout(opts))
      end

    case conn do
      {:ok, conn} ->
        # ASK is a per-command, per-request redirection: each command needs its
        # own ASKING prefix, and the target may redirect us again (ASK -> ASK or
        # ASK -> MOVED). Issue each command on its own so we can follow any
        # further hop through the shared redirection machinery.
        Enum.map(cmds, fn indexed_cmd ->
          execute_asking_command(cluster, conn, indexed_cmd, opts, remaining)
        end)

      {:error, _reason} ->
        # See handle_moved_redirect/6: an honest connection error rather than a
        # fabricated %Redix.Error{} that starts with "ASK ".
        Enum.map(cmds, fn {idx, _cmd} ->
          {idx, %Redix.ConnectionError{reason: :closed}}
        end)
    end
  end

  # Issues a single command behind an ASKING prefix at the redirect target, then
  # follows any further redirection the target returns. ASKING only needs to ride
  # the first request to a given node, so subsequent hops re-issue it themselves.
  defp execute_asking_command(cluster, conn, {idx, cmd}, opts, remaining) do
    case pipeline_catching_exit(conn, [["ASKING"], cmd], opts) do
      {:ok, [_asking_ok, result]} ->
        case follow_redirections(cluster, [{idx, cmd}], [{idx, result}], opts, remaining) do
          {:error, _} = error -> error
          [single_result] -> single_result
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_redirection(%Redix.Error{message: "MOVED " <> rest}) do
    parse_redirection_target(:moved, rest)
  end

  defp parse_redirection(%Redix.Error{message: "ASK " <> rest}) do
    parse_redirection_target(:ask, rest)
  end

  defp parse_redirection(_), do: nil

  # The slot and address are server-controlled, so parse them defensively: a
  # malformed redirect is handed back to the caller as a plain Redis error value
  # instead of crashing the calling process (issue #325).
  defp parse_redirection_target(type, rest) do
    with [slot_str, address] <- String.split(rest, " "),
         {slot, ""} when slot in 0..16383 <- Integer.parse(slot_str),
         {:ok, host, port} <- Manager.split_host_port(address) do
      {type, slot, host, port}
    else
      _other -> nil
    end
  end

  ## Helpers

  # Mirrors single-node Redix's behavior for commands issued right after an async
  # start: Redix.Connection *postpones* pipelines while its first connection attempt
  # is in flight, so they wait for it instead of failing. Here, until the *initial*
  # topology fetch attempt completes (no :discovery_attempted marker in the slot
  # table yet), we block on the Manager — whose call queue makes us wait out exactly
  # the attempt in flight — and then fall through to normal resolution. The marker is
  # set when that first attempt completes, *success or failure*: if it failed,
  # resolution finds nothing and this and all later commands fail fast with :closed
  # (if the cluster couldn't be reached on the first try it might be a while before
  # it can, so blocking callers for every backoff retry wouldn't help). Once the
  # marker is set this costs one ETS lookup and the Manager is never contacted again.
  defp await_topology_discovery(cluster, slot_table, opts) do
    unless :ets.member(slot_table, :discovery_attempted) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      Manager.await_topology_discovery(manager_name(cluster), timeout)
    end

    :ok
  end

  defp connect_timeout(opts) do
    Keyword.get(opts, :timeout, @default_timeout)
  end

  # Resolves the connection for a slot according to the routing choice. Returns
  # `{:ok, pid}` or `:error` (the same shape `Manager.get_connection/3` returns),
  # so the grouping/execution path handles a missing connection uniformly.
  defp resolve_connection(slot_table, registry, slot, :primary) do
    Manager.get_connection(slot_table, registry, slot)
  end

  defp resolve_connection(slot_table, registry, slot, :replica) do
    Manager.get_replica_connection(slot_table, registry, slot)
  end

  defp resolve_connection(slot_table, registry, slot, :prefer_replica) do
    case Manager.get_replica_connection(slot_table, registry, slot) do
      {:ok, _pid} = ok -> ok
      :error -> Manager.get_connection(slot_table, registry, slot)
    end
  end

  defp validate_route!(opts) do
    case Keyword.get(opts, :route, :primary) do
      route when route in @valid_routes ->
        route

      other ->
        raise ArgumentError,
              "invalid :route option: #{inspect(other)}, expected one of #{inspect(@valid_routes)}"
    end
  end

  defp command_slot(command) do
    case CommandParser.key_from_command(command) do
      {:ok, key} -> Hash.hash_slot(key)
      :no_key -> :no_slot
      :unknown -> :unknown
    end
  end

  # Commands outside CommandParser's static table come back as :unknown. Rather than
  # blindly routing them to a random node, we ask the server how it routes them and
  # cache the answer. Each :unknown entry is replaced with an integer slot (when a key
  # is found) or :no_slot (no key, or the lookup itself failed — a genuinely unknown
  # command, or no reachable node). On :no_slot the command keeps the prior behavior of
  # being sent to a random node.
  defp resolve_unknown_slots(indexed_commands, registry, command_cache) do
    unknowns = for {idx, cmd, :unknown} <- indexed_commands, do: {idx, cmd}

    if unknowns == [] do
      indexed_commands
    else
      slots_by_index = resolve_unknowns(unknowns, registry, command_cache)

      Enum.map(indexed_commands, fn
        {idx, cmd, :unknown} -> {idx, cmd, Map.fetch!(slots_by_index, idx)}
        {_idx, _cmd, _slot} = resolved -> resolved
      end)
    end
  end

  # Resolves a list of `{idx, command}` unknowns to a `%{idx => slot | :no_slot}` map.
  #
  # The key specification of a command (first-key position, or "movable" keys) is stable,
  # so we learn it once via COMMAND INFO and cache it per command name. Subsequent calls
  # to the same command resolve locally with no network round-trip. Commands Redis reports
  # as having *movable* keys (e.g. MIGRATE, BLMPOP) can't be pinned to a fixed position, so
  # those fall back to a per-call COMMAND GETKEYS — but they're rare and still cached as
  # "movable" so we don't re-issue COMMAND INFO for them.
  defp resolve_unknowns(unknowns, registry, command_cache) do
    case Manager.get_random_connection(registry) do
      {:ok, conn} ->
        cache_missing_keyspecs(conn, command_cache, unknowns)
        slots_from_keyspecs(conn, command_cache, unknowns)

      :error ->
        Map.new(unknowns, fn {idx, _cmd} -> {idx, :no_slot} end)
    end
  end

  # Fetches and caches the key specification for any command name not already cached.
  defp cache_missing_keyspecs(conn, command_cache, unknowns) do
    missing =
      unknowns
      |> Enum.map(fn {_idx, cmd} -> command_name(cmd) end)
      |> Enum.uniq()
      |> Enum.reject(&(:ets.lookup(command_cache, &1) != []))

    if missing != [] and :ets.info(command_cache, :size) < @command_cache_max_size do
      # command_catching_exit, not Redix.command: a randomly-chosen `conn` that died
      # mid-flight would otherwise exit the *caller* (A1). The caught exit yields an
      # `{:error, _}` tuple, which falls through to the `_other` clause (nothing cached,
      # commands resolve to :no_slot for this call, COMMAND INFO retried next time).
      case command_catching_exit(conn, ["COMMAND", "INFO" | missing]) do
        {:ok, infos} when length(infos) == length(missing) ->
          missing
          |> Enum.zip(infos)
          |> Enum.each(fn {name, info} ->
            :ets.insert(command_cache, {name, keyspec_from_info(info)})
          end)

        # A malformed *whole* reply (wrong length) or a connection error isn't cached:
        # the commands resolve to :no_slot for this call and we retry COMMAND INFO next
        # time. Note this is only about the whole reply — a well-formed reply whose entry
        # for a given command is `nil` (a command the server doesn't know) *is* cached as
        # :no_key by keyspec_from_info/1.
        _other ->
          :ok
      end
    end
  end

  # COMMAND INFO reply per command: [name, arity, flags, first_key, last_key, step | _].
  # We route on the first key, so we only need first_key (1-based, command name at 0) and
  # whether the keys are movable.
  defp keyspec_from_info([_name, _arity, flags, first_key, _last_key, _step | _]) do
    cond do
      "movablekeys" in flags -> :movable
      first_key >= 1 -> {:first_key, first_key}
      true -> :no_key
    end
  end

  # A command the server doesn't know comes back as `nil` (and any other unparseable
  # entry lands here too). We cache it as :no_key deliberately: the answer is stable, so
  # re-issuing COMMAND INFO for it on every call would be wasted work. This per-entry
  # caching is distinct from the malformed/short *whole* reply that cache_missing_keyspecs/3
  # leaves uncached.
  defp keyspec_from_info(_other), do: :no_key

  defp slots_from_keyspecs(conn, command_cache, unknowns) do
    # Static-position and keyless commands resolve locally; movable ones need a per-call
    # COMMAND GETKEYS, which we batch into a single pipeline.
    {movable, local} =
      Enum.split_with(unknowns, fn {_idx, cmd} ->
        cached_keyspec(command_cache, cmd) == :movable
      end)

    local_slots =
      Map.new(local, fn {idx, cmd} ->
        {idx, slot_from_keyspec(cached_keyspec(command_cache, cmd), cmd)}
      end)

    Map.merge(local_slots, getkeys_slots(conn, movable))
  end

  defp cached_keyspec(command_cache, cmd) do
    case :ets.lookup(command_cache, command_name(cmd)) do
      [{_name, keyspec}] -> keyspec
      [] -> :no_key
    end
  end

  defp slot_from_keyspec({:first_key, position}, cmd) do
    # first_key counts the command name as position 0, so it indexes straight into cmd.
    case Enum.at(cmd, position) do
      nil -> :no_slot
      key -> Hash.hash_slot(to_string(key))
    end
  end

  defp slot_from_keyspec(_no_key_or_unknown, _cmd), do: :no_slot

  # Per-call COMMAND GETKEYS for movable-key commands, batched into one pipeline.
  defp getkeys_slots(_conn, []), do: %{}

  defp getkeys_slots(conn, movable) do
    getkeys = Enum.map(movable, fn {_idx, cmd} -> ["COMMAND", "GETKEYS" | cmd] end)

    # pipeline_catching_exit, not Redix.pipeline: `conn` is a randomly-chosen node
    # that may already be dying (topology churn), and an uncaught exit here would
    # crash the *caller* instead of degrading to :no_slot (A1). The caught exit
    # surfaces as `{:error, _}`, which the clause below maps to :no_slot.
    case pipeline_catching_exit(conn, getkeys, []) do
      {:ok, results} ->
        movable
        |> Enum.zip(results)
        |> Map.new(fn {{idx, _cmd}, result} -> {idx, slot_from_getkeys(result)} end)

      {:error, _reason} ->
        Map.new(movable, fn {idx, _cmd} -> {idx, :no_slot} end)
    end
  end

  # COMMAND GETKEYS returns the list of keys for a command. We route on the first one. A
  # command with no keys comes back as a Redix.Error, which falls through to :no_slot.
  defp slot_from_getkeys([key | _]) when is_binary(key), do: Hash.hash_slot(key)
  defp slot_from_getkeys(_other), do: :no_slot

  defp command_name([name | _]), do: name |> to_string() |> String.upcase()
  defp command_name([]), do: ""

  # Deterministic resource names derived from the cluster name.
  defp slot_table_name(cluster), do: :"#{cluster}_slots"
  defp command_cache_name(cluster), do: :"#{cluster}_command_cache"
  defp registry_name(cluster), do: :"#{cluster}_registry"
  defp manager_name(cluster), do: :"#{cluster}_manager"
  defp pool_name(cluster), do: :"#{cluster}_pool"
  defp task_supervisor_name(cluster), do: :"#{cluster}_task_supervisor"

  ## NimbleOptions custom validators

  @doc false
  def __parse_nodes__([_ | _] = nodes) do
    Enum.map(nodes, fn node ->
      case __parse_node__(node) do
        {:ok, parsed} -> parsed
        {:error, reason} -> throw({:error, reason})
      end
    end)
  catch
    :throw, {:error, reason} ->
      {:error, "invalid node in :nodes list: " <> reason}
  else
    parsed_nodes -> {:ok, parsed_nodes}
  end

  def __parse_nodes__(other) do
    {:error, "expected a non-empty list of nodes, got: #{inspect(other)}"}
  end

  # Each parsed seed is a full set of connection options, but the cluster derives
  # `seed_nodes` from only their host/port: every node (the seeds and the nodes
  # discovered from them) is reached with the *shared* conn_opts. So most options a seed
  # URI can carry can't be honored per-seed and we handle them here rather than silently
  # dropping them (issue #322):
  #
  #   * TLS is genuinely cluster-wide, so a `rediss://` seed is lifted into conn_opts.
  #   * Credentials can't be (the shared conn_opts authenticates every node, and seeds
  #     may legitimately differ), so userinfo in a seed URI raises — pass
  #     :username/:password as options instead.
  #   * A cluster only supports database 0, so a non-zero database raises (0 is a no-op).
  defp merge_seed_node_opts!(conn_opts, parsed_nodes) do
    parsed_nodes
    |> Enum.flat_map(&Keyword.drop(&1, [:host, :port]))
    |> Enum.reduce(conn_opts, &merge_seed_opt!/2)
  end

  defp merge_seed_opt!({:ssl, true}, conn_opts) do
    case Keyword.fetch(conn_opts, :ssl) do
      {:ok, false} ->
        raise ArgumentError,
              "a rediss:// seed node enables TLS, but ssl: false was also passed; remove one"

      _ssl_true_or_absent ->
        Keyword.put(conn_opts, :ssl, true)
    end
  end

  defp merge_seed_opt!({:database, 0}, conn_opts) do
    conn_opts
  end

  defp merge_seed_opt!({:database, database}, _conn_opts) do
    raise ArgumentError, "Redis Cluster only supports database 0, got: #{inspect(database)}"
  end

  defp merge_seed_opt!({credential, _value}, _conn_opts)
       when credential in [:username, :password] do
    raise ArgumentError, """
    a seed node in :nodes carries credentials in its URI (#{credential}), which can't \
    be honored in cluster mode: every node is authenticated with the shared \
    configuration, so pass :username and :password as connection options instead\
    """
  end

  @doc false
  def __parse_node__(node)

  def __parse_node__(uri) when is_binary(uri) do
    {:ok, Redix.URI.to_start_options(uri)}
  end

  def __parse_node__(opts) when is_list(opts) do
    case NimbleOptions.validate(opts, @node_as_keyword_opts_schema) do
      {:ok, opts} -> {:ok, opts}
      {:error, reason} -> {:error, Exception.message(reason)}
    end
  end

  def __parse_node__(other) do
    {:error, "expected a Redis URI or a :host/:port keyword list, got: #{inspect(other)}"}
  end
end
