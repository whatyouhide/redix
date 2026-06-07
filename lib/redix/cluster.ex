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
  """
  @type endpoint() :: String.t() | [{:host, String.t()} | {:port, :inet.port_number()}]

  @max_redirections 5

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
      type: {:list, {:custom, __MODULE__, :__parse_node__, []}},
      type_doc: "list of `t:endpoint/0`",
      required: true,
      doc: """
      A list of seed nodes to connect to. Only **one reachable node** is needed:
      the full cluster topology is discovered automatically via `CLUSTER SLOTS`.
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
  `:timeout`, and so on) are passed through to *each underlying node connection*.

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
    seed_nodes = Keyword.fetch!(cluster_opts, :nodes)
    refresh_interval = Keyword.fetch!(cluster_opts, :topology_refresh_interval)
    read_from_replicas = Keyword.fetch!(cluster_opts, :read_from_replicas)

    if Keyword.has_key?(conn_opts, :sentinel) do
      raise ArgumentError, "Sentinel connections are not supported in cluster mode"
    end

    conn_opts = Redix.StartOptions.sanitize(:redix, conn_opts)

    case Keyword.fetch(conn_opts, :database) do
      {:ok, db} when db in [0, nil] ->
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
       table_name: slot_table_name(name),
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

    # All commands in a transaction must target the same slot
    slots =
      commands
      |> Enum.map(&command_slot/1)
      |> Enum.reject(&(&1 == :no_slot))
      |> Enum.uniq()

    case slots do
      [slot] ->
        case Manager.get_connection(slot_table, registry, slot) do
          {:ok, conn} ->
            Redix.transaction_pipeline(conn, commands, opts)

          :error ->
            {:error, %Redix.ConnectionError{reason: :closed}}
        end

      [] ->
        {:error,
         %Redix.Error{
           message:
             "ERR transaction_pipeline requires at least one command with a key to route on"
         }}

      _multiple ->
        {:error, %Redix.Error{message: "CROSSSLOT Keys in request don't hash to the same slot"}}
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

  ## Pipeline implementation

  defp execute_pipeline(cluster, commands, opts) do
    route = validate_route!(opts)
    opts = Keyword.delete(opts, :route)

    slot_table = slot_table_name(cluster)
    registry = registry_name(cluster)

    # Group commands by target node
    indexed_commands =
      commands
      |> Enum.with_index()
      |> Enum.map(fn {cmd, idx} ->
        slot = command_slot(cmd)
        {idx, cmd, slot}
      end)

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
    indexed_commands
    |> Enum.group_by(fn
      {_idx, _cmd, :no_slot} -> :random
      {_idx, _cmd, slot} -> resolve_connection(slot_table, registry, slot, route)
    end)
    |> Enum.map(fn {node_key, commands} ->
      conn =
        case node_key do
          {:ok, pid} ->
            pid

          :random ->
            case Manager.get_random_connection(registry) do
              {:ok, pid} -> pid
              :error -> nil
            end

          :error ->
            nil
        end

      cmds = Enum.map(commands, fn {idx, cmd, _slot} -> {idx, cmd} end)
      {conn, cmds}
    end)
  end

  # No connection could be resolved for this group (for example `route: :replica`
  # when no replica is reachable). Surface a connection error rather than crashing
  # on `Redix.pipeline(nil, ...)`.
  defp execute_groups(_cluster, [{nil, _cmds}], _opts) do
    {:error, %Redix.ConnectionError{reason: :closed}}
  end

  # If there's only a single group to execute, we don't need parallel tasks.
  defp execute_groups(cluster, [{conn, cmds}], opts) do
    execute_and_handle_redirections(cluster, conn, cmds, opts, @max_redirections)
  end

  defp execute_groups(cluster, groups, opts) do
    task_supervisor = task_supervisor_name(cluster)

    tasks =
      Enum.map(groups, fn {conn, cmds} ->
        if conn == nil do
          Task.completed({:error, %Redix.ConnectionError{reason: :closed}})
        else
          Task.Supervisor.async(task_supervisor, fn ->
            execute_and_handle_redirections(cluster, conn, cmds, opts, @max_redirections)
          end)
        end
      end)

    tasks_with_results =
      Task.yield_many(tasks, timeout: Keyword.get(opts, :timeout, 5_000), on_timeout: :kill_task)

    first_error =
      Enum.find(tasks_with_results, fn
        {_task, {:error, _} = res} -> res
        {_task, {:exit, reason}} -> {:error, reason}
        {_task, _other} -> nil
      end)

    first_error || Enum.map(tasks_with_results, fn {_task, {:ok, res}} -> res end)
  end

  defp execute_and_handle_redirections(cluster, conn, cmds, opts, remaining) do
    commands = Enum.map(cmds, fn {_idx, cmd} -> cmd end)

    case Redix.pipeline(conn, commands, opts) do
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
        {:ok, conn} -> {:ok, conn}
        :error -> Manager.connect_to_node(manager_name(cluster), {host, port})
      end

    case conn do
      {:ok, conn} ->
        case execute_and_handle_redirections(cluster, conn, cmds, opts, remaining) do
          {:error, _} = error -> [error]
          results -> results
        end

      {:error, _reason} ->
        Enum.map(cmds, fn {idx, _cmd} ->
          {idx, %Redix.Error{message: "MOVED to unreachable node #{host}:#{port}"}}
        end)
    end
  end

  defp handle_ask_redirect(cluster, host, port, cmds, opts, remaining) do
    case Manager.get_connection_by_node(registry_name(cluster), {host, port}) do
      {:ok, conn} ->
        # ASK is a per-command, per-request redirection: each command needs its
        # own ASKING prefix, and the target may redirect us again (ASK -> ASK or
        # ASK -> MOVED). Issue each command on its own so we can follow any
        # further hop through the shared redirection machinery.
        Enum.map(cmds, fn indexed_cmd ->
          execute_asking_command(cluster, conn, indexed_cmd, opts, remaining)
        end)

      :error ->
        Enum.map(cmds, fn {idx, _cmd} ->
          {idx, %Redix.Error{message: "ASK to unreachable node #{host}:#{port}"}}
        end)
    end
  end

  # Issues a single command behind an ASKING prefix at the redirect target, then
  # follows any further redirection the target returns. ASKING only needs to ride
  # the first request to a given node, so subsequent hops re-issue it themselves.
  defp execute_asking_command(cluster, conn, {idx, cmd}, opts, remaining) do
    case Redix.pipeline(conn, [["ASKING"], cmd], opts) do
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
    case String.split(rest, " ") do
      [slot_str, address] ->
        {slot, ""} = Integer.parse(slot_str)
        {host, port} = Manager.split_host_port(address)
        {:moved, slot, host, port}

      _ ->
        nil
    end
  end

  defp parse_redirection(%Redix.Error{message: "ASK " <> rest}) do
    case String.split(rest, " ") do
      [slot_str, address] ->
        {slot, ""} = Integer.parse(slot_str)
        {host, port} = Manager.split_host_port(address)
        {:ask, slot, host, port}

      _ ->
        nil
    end
  end

  defp parse_redirection(_), do: nil

  ## Helpers

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
      :unknown -> :no_slot
    end
  end

  # Deterministic resource names derived from the cluster name.
  defp slot_table_name(cluster), do: :"#{cluster}_slots"
  defp registry_name(cluster), do: :"#{cluster}_registry"
  defp manager_name(cluster), do: :"#{cluster}_manager"
  defp pool_name(cluster), do: :"#{cluster}_pool"
  defp task_supervisor_name(cluster), do: :"#{cluster}_task_supervisor"

  ## NimbleOptions custom validators

  @doc false
  def __parse_node__(node)

  def __parse_node__(uri) when is_binary(uri) do
    parsed = Redix.URI.to_start_options(uri)
    {:ok, {Keyword.get(parsed, :host, "localhost"), Keyword.get(parsed, :port, 6379)}}
  end

  def __parse_node__(opts) when is_list(opts) do
    case NimbleOptions.validate(opts, @node_as_keyword_opts_schema) do
      {:ok, opts} -> {:ok, {Keyword.get(opts, :host), Keyword.get(opts, :port)}}
      {:error, reason} -> {:error, Exception.message(reason)}
    end
  end

  def __parse_node__(other) do
    {:error, "expected a Redis URI or a :host/:port keyword list, got: #{inspect(other)}"}
  end
end
