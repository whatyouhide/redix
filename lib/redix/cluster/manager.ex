defmodule Redix.Cluster.Manager do
  @moduledoc false

  @behaviour :gen_statem

  @refresh_cooldown 1_000

  defstruct [
    :cluster_name,
    :slot_table,
    :registry,
    :pool_supervisor,
    :conn_opts,
    :seed_nodes,
    :refresh_interval,
    read_from_replicas: false,
    # Maps a monitor ref to `{node_id, role}` so a crashed connection is
    # restarted with the same role (and therefore the same READONLY behavior).
    monitors: %{}
  ]

  ## Public API

  @doc """
  Starts the cluster manager.
  """
  @spec start_link(keyword()) :: :gen_statem.start_ret()
  def start_link(opts) when is_list(opts) do
    {name, init_opts} = Keyword.pop(opts, :name)

    if name do
      :gen_statem.start_link({:local, name}, __MODULE__, init_opts, [])
    else
      :gen_statem.start_link(__MODULE__, init_opts, [])
    end
  end

  @doc """
  Child spec for this module.
  """
  @spec child_spec(keyword()) :: map()
  def child_spec(opts) when is_list(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc """
  Looks up the **primary** Redix connection PID for a given hash slot.

  Reads the slot table from ETS and then looks up the connection in the Registry.
  """
  @spec get_connection(atom(), atom(), non_neg_integer()) :: {:ok, pid()} | :error
  def get_connection(slot_table, registry, slot) when is_integer(slot) do
    case :ets.lookup(slot_table, slot) do
      [{^slot, primary_id, _replica_ids}] -> lookup_connection(registry, primary_id)
      [] -> :error
    end
  end

  @doc """
  Looks up a **replica** Redix connection PID for a given hash slot.

  Picks a reachable replica at random. Returns `:error` if the slot is unknown or
  has no reachable replica connection (for example when `:read_from_replicas` is
  disabled, in which case no replicas are tracked). Callers that want to fall back
  to the primary should do so explicitly (see `Redix.Cluster`'s `:prefer_replica`).
  """
  @spec get_replica_connection(atom(), atom(), non_neg_integer()) :: {:ok, pid()} | :error
  def get_replica_connection(slot_table, registry, slot) when is_integer(slot) do
    case :ets.lookup(slot_table, slot) do
      [{^slot, _primary_id, replica_ids}] when replica_ids != [] ->
        replica_ids
        |> Enum.shuffle()
        |> Enum.find_value(:error, fn replica_id ->
          case lookup_connection(registry, replica_id) do
            {:ok, pid} -> {:ok, pid}
            :error -> false
          end
        end)

      _other ->
        :error
    end
  end

  @doc """
  Looks up the Redix connection PID for a given `{host, port}`.

  Used for `MOVED`/`ASK` redirection to a specific node.
  """
  @spec get_connection_by_node(atom(), {String.t(), non_neg_integer()}) :: {:ok, pid()} | :error
  def get_connection_by_node(registry, {host, port}) do
    lookup_connection(registry, "#{host}:#{port}")
  end

  @doc """
  Returns any available **primary** connection PID from the cluster.

  Used for keyless commands like `PING`, `INFO`, and so on. Primaries are preferred
  so that keyless *write* commands (such as `FLUSHALL`) don't land on a read-only
  replica; if no primary is registered yet, it falls back to any connection.
  """
  @spec get_random_connection(atom()) :: {:ok, pid()} | :error
  def get_random_connection(registry) do
    case Registry.select(registry, [{{:_, :"$1", :primary}, [], [:"$1"]}]) do
      [_ | _] = pids ->
        {:ok, Enum.random(pids)}

      [] ->
        case Registry.select(registry, [{{:_, :"$1", :_}, [], [:"$1"]}]) do
          [] -> :error
          pids -> {:ok, Enum.random(pids)}
        end
    end
  end

  @doc """
  Triggers an asynchronous topology refresh. Rate-limited to at most once per second.
  """
  @spec refresh_topology(:gen_statem.server_ref()) :: :ok
  def refresh_topology(manager) do
    :gen_statem.cast(manager, :refresh_topology)
  end

  @doc """
  Ensures a connection to `{host, port}` exists, starting one **on demand**
  if needed.

  Used when a `MOVED` redirect points at a node we haven't discovered yet (for
  example mid-resharding). `MOVED` is authoritative, so we trust the address,
  connect, register, and monitor it just like a node found via `CLUSTER SLOTS`.
  The next topology refresh "adopts" the connection (if the node shows up in
  `CLUSTER SLOTS`) or terminates it (if it doesn't), so a bogus address can't
  leak connections.
  """
  @spec connect_to_node(:gen_statem.server_ref(), {String.t(), :inet.port_number()}) ::
          {:ok, pid()} | {:error, term()}
  def connect_to_node(manager, {host, port}) do
    # This runs in the command hot path, so a Manager that's briefly busy (say,
    # mid-refresh against slow nodes) must not crash the caller: degrade to an
    # error tuple, which the MOVED handler turns into a normal Redix error.
    :gen_statem.call(manager, {:connect_to_node, host, port})
  catch
    :exit, reason -> {:error, reason}
  end

  ## gen_statem callbacks

  @impl true
  def callback_mode, do: :state_functions

  @impl true
  def init(opts) do
    cluster_name = Keyword.fetch!(opts, :cluster_name)
    seed_nodes = Keyword.fetch!(opts, :seed_nodes)
    pool_supervisor = Keyword.fetch!(opts, :pool_supervisor)
    conn_opts = Keyword.fetch!(opts, :conn_opts)
    refresh_interval = Keyword.fetch!(opts, :refresh_interval)
    table_name = Keyword.fetch!(opts, :table_name)
    registry = Keyword.fetch!(opts, :registry)
    read_from_replicas = Keyword.fetch!(opts, :read_from_replicas)

    slot_table = :ets.new(table_name, [:named_table, :public, :set, {:read_concurrency, true}])

    data = %__MODULE__{
      cluster_name: cluster_name,
      slot_table: slot_table,
      registry: registry,
      pool_supervisor: pool_supervisor,
      conn_opts: conn_opts,
      seed_nodes: seed_nodes,
      refresh_interval: refresh_interval,
      read_from_replicas: read_from_replicas
    }

    case do_refresh_topology(data) do
      {:ok, data} ->
        {:ok, :ready, data, [periodic_refresh_action(refresh_interval)]}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  ## State: :ready — reactive refreshes are accepted.

  def ready(:cast, :refresh_topology, data) do
    data =
      case do_refresh_topology(data) do
        {:ok, data} -> data
        {:error, _reason} -> data
      end

    # Transition to :cooling_down with a state timeout that will bring us back.
    {:next_state, :cooling_down, data, [{:state_timeout, @refresh_cooldown, :cooldown_expired}]}
  end

  def ready({:timeout, :periodic_refresh}, :refresh, data) do
    data =
      case do_refresh_topology(data) do
        {:ok, data} -> data
        {:error, _reason} -> data
      end

    {:keep_state, data, [periodic_refresh_action(data.refresh_interval)]}
  end

  def ready(:info, {:DOWN, ref, :process, _pid, _reason}, data) do
    {:keep_state, handle_down(data, ref)}
  end

  def ready({:call, from}, {:connect_to_node, host, port}, data) do
    handle_connect_to_node(from, host, port, data)
  end

  ## State: :cooling_down — reactive refreshes are silently dropped.

  def cooling_down(:state_timeout, :cooldown_expired, data) do
    {:next_state, :ready, data}
  end

  # Drop, we just refreshed.
  def cooling_down(:cast, :refresh_topology, _data) do
    :keep_state_and_data
  end

  def cooling_down({:timeout, :periodic_refresh}, :refresh, _data) do
    {:keep_state_and_data, :postpone}
  end

  def cooling_down(:info, {:DOWN, ref, :process, _pid, _reason}, data) do
    {:keep_state, handle_down(data, ref)}
  end

  # On-demand connects are not refreshes, so they're served even during cooldown.
  def cooling_down({:call, from}, {:connect_to_node, host, port}, data) do
    handle_connect_to_node(from, host, port, data)
  end

  def cooling_down(:info, _msg, _data), do: :keep_state_and_data

  ## Private helpers

  defp periodic_refresh_action(interval) do
    {{:timeout, :periodic_refresh}, interval, :refresh}
  end

  defp handle_connect_to_node(from, host, port, data) do
    node_id = "#{host}:#{port}"

    case lookup_connection(data.registry, node_id) do
      {:ok, pid} ->
        {:keep_state_and_data, [{:reply, from, {:ok, pid}}]}

      :error ->
        # MOVED is authoritative and always points at the slot's primary, so an
        # on-demand connection is registered as a primary.
        {result, data} = start_and_monitor_connection(data, node_id, host, port, :primary)
        {:keep_state, data, [{:reply, from, result}]}
    end
  end

  defp handle_down(data, ref) do
    {node_info, monitors} = Map.pop(data.monitors, ref)
    data = %{data | monitors: monitors}

    case node_info do
      {node_id, role} ->
        {host, port} = split_host_port(node_id)
        {_result, data} = start_and_monitor_connection(data, node_id, host, port, role)
        data

      nil ->
        data
    end
  end

  defp lookup_connection(registry, node_id) do
    case Registry.lookup(registry, node_id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end

  defp do_refresh_topology(data) do
    all_nodes = get_known_nodes(data) ++ data.seed_nodes

    case fetch_cluster_slots(all_nodes, data.conn_opts) do
      {:ok, slots_data} ->
        update_slot_map(data, slots_data)
        data = ensure_connections(data, slots_data)

        node_addresses =
          slots_data
          |> Enum.map(fn [_start, _end, [host, port | _] | _] -> "#{host}:#{port}" end)
          |> Enum.uniq()

        :telemetry.execute([:redix, :cluster, :topology_change], %{}, %{
          cluster: data.cluster_name,
          nodes: node_addresses
        })

        {:ok, data}

      {:error, reason} ->
        :telemetry.execute([:redix, :cluster, :failed_topology_refresh], %{}, %{
          cluster: data.cluster_name,
          reason: reason
        })

        {:error, reason}
    end
  end

  # TODO: should we use the slot_table to get the known nodes?
  defp get_known_nodes(data) do
    data.registry
    |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}])
    |> Enum.map(&split_host_port/1)
  end

  # Splits a "host:port" address on its *last* colon. IPv6 hosts contain colons of
  # their own (e.g. "::1:7000"), so splitting on every colon and matching
  # [host, port] breaks on them (see issue #306). This is the single place that
  # turns the internal "host:port" node-id / redirection address format back into
  # {host, port}; Redix.Cluster.parse_redirection/1 reuses it.
  @spec split_host_port(String.t()) :: {String.t(), :inet.port_number()}
  def split_host_port(address) do
    [host, port_str] = :string.split(address, ":", :trailing)
    {host, String.to_integer(port_str)}
  end

  defp fetch_cluster_slots(_all_nodes = [], _conn_opts) do
    {:error, :no_reachable_node}
  end

  defp fetch_cluster_slots([{host, port} | rest], conn_opts) do
    try_fetch_slots(host, port, conn_opts, rest)
  end

  defp try_fetch_slots(host, port, conn_opts, rest) do
    transport = if(conn_opts[:ssl], do: :ssl, else: :gen_tcp)

    opts =
      conn_opts
      |> Keyword.delete(:name)
      |> Keyword.merge(host: to_charlist(host), port: port)

    timeout = opts[:timeout]

    case Redix.Connector.connect(opts, _unused_conn_pid = self()) do
      {:ok, socket, _address} ->
        try do
          case Redix.Connector.sync_command(transport, socket, ["CLUSTER", "SLOTS"], timeout) do
            {:ok, slots} -> {:ok, slots}
            {:error, _} -> fetch_cluster_slots(rest, conn_opts)
          end
        after
          transport.close(socket)
        end

      {:error, _} ->
        fetch_cluster_slots(rest, conn_opts)

      {:stop, _} ->
        fetch_cluster_slots(rest, conn_opts)
    end
  end

  defp update_slot_map(data, slots_data) do
    for slot_range <- slots_data do
      [start_slot, end_slot, [host, port | _] | replica_entries] = slot_range
      primary_id = "#{host}:#{port}"

      replica_ids =
        if data.read_from_replicas do
          for [r_host, r_port | _] <- replica_entries, do: "#{r_host}:#{r_port}"
        else
          []
        end

      for slot <- start_slot..end_slot do
        :ets.insert(data.slot_table, {slot, primary_id, replica_ids})
      end
    end
  end

  defp ensure_connections(data, slots_data) do
    needed_nodes = nodes_to_connect(data, slots_data)

    data =
      Enum.reduce(needed_nodes, data, fn {node_id, host, port, role}, acc ->
        case Registry.lookup(acc.registry, node_id) do
          [{pid, _}] when is_pid(pid) ->
            if Process.alive?(pid) do
              acc
            else
              {_result, acc} = start_and_monitor_connection(acc, node_id, host, port, role)
              acc
            end

          [] ->
            {_result, acc} = start_and_monitor_connection(acc, node_id, host, port, role)
            acc
        end
      end)

    needed_ids = MapSet.new(needed_nodes, fn {node_id, _, _, _} -> node_id end)

    registered_nodes =
      Registry.select(data.registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    for {node_id, pid} <- registered_nodes, not MapSet.member?(needed_ids, node_id) do
      if Process.alive?(pid) do
        DynamicSupervisor.terminate_child(data.pool_supervisor, pid)
      end
    end

    data
  end

  # Builds the list of `{node_id, host, port, role}` tuples the cluster should be
  # connected to. Primaries always; replicas only when `:read_from_replicas` is on.
  # Primaries are listed first so `uniq_by` keeps the primary role if a node ever
  # appears in both lists.
  defp nodes_to_connect(data, slots_data) do
    primaries =
      Enum.map(slots_data, fn [_start, _end, [host, port | _] | _replicas] ->
        {"#{host}:#{port}", host, port, :primary}
      end)

    replicas =
      if data.read_from_replicas do
        for [_start, _end, _primary | replica_entries] <- slots_data,
            [host, port | _] <- replica_entries do
          {"#{host}:#{port}", host, port, :replica}
        end
      else
        []
      end

    Enum.uniq_by(primaries ++ replicas, fn {node_id, _, _, _} -> node_id end)
  end

  # Returns `{result, data}` where `result` is `{:ok, pid}` or `{:error, reason}`.
  # Callers that only care about the updated data can discard the result.
  defp start_and_monitor_connection(data, node_id, host, port, role) do
    case start_connection(
           data.pool_supervisor,
           data.registry,
           node_id,
           host,
           port,
           data.conn_opts,
           role
         ) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        {{:ok, pid}, %{data | monitors: Map.put(data.monitors, ref, {node_id, role})}}

      # A concurrent monitor restart (or the connection's own retry) may have
      # already registered this node. Treat it as success.
      {:error, {:already_started, pid}} ->
        {{:ok, pid}, data}

      {:error, reason} ->
        :telemetry.execute([:redix, :cluster, :node_connection_failed], %{}, %{
          cluster: data.cluster_name,
          address: node_id,
          reason: reason
        })

        {{:error, reason}, data}
    end
  end

  defp start_connection(pool_supervisor, registry, node_id, host, port, conn_opts, role) do
    opts =
      conn_opts
      |> Keyword.delete(:name)
      |> Keyword.merge(
        host: host,
        port: port,
        sync_connect: false,
        # The Registry value records the node's role so keyless commands can be
        # routed to primaries (see `get_random_connection/1`).
        name: {:via, Registry, {registry, node_id, role}}
      )
      |> maybe_put_readonly(role)

    DynamicSupervisor.start_child(pool_supervisor, {Redix, opts})
  end

  # Replica connections issue READONLY after connecting so they serve reads.
  defp maybe_put_readonly(opts, :replica), do: Keyword.put(opts, :readonly, true)
  defp maybe_put_readonly(opts, :primary), do: opts
end
