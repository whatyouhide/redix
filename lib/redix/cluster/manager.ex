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
  Looks up the Redix connection PID for a given hash slot.

  Reads the slot table from ETS and then looks up the connection in the Registry.
  """
  @spec get_connection(atom(), atom(), non_neg_integer()) :: {:ok, pid()} | :error
  def get_connection(slot_table, registry, slot) when is_integer(slot) do
    case :ets.lookup(slot_table, slot) do
      [{^slot, node_id}] -> lookup_connection(registry, node_id)
      [] -> :error
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
  Returns any available connection PID from the cluster.

  Used for keyless commands like `PING`, `INFO`, and so on.
  """
  @spec get_random_connection(atom()) :: {:ok, pid()} | :error
  def get_random_connection(registry) do
    case Registry.select(registry, [{{:_, :"$1", :_}, [], [:"$1"]}]) do
      [] -> :error
      pids -> {:ok, Enum.random(pids)}
    end
  end

  @doc """
  Triggers an asynchronous topology refresh. Rate-limited to at most once per second.
  """
  @spec refresh_topology(:gen_statem.server_ref()) :: :ok
  def refresh_topology(manager) do
    :gen_statem.cast(manager, :refresh_topology)
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

    slot_table = :ets.new(table_name, [:named_table, :public, :set, {:read_concurrency, true}])

    data = %__MODULE__{
      cluster_name: cluster_name,
      slot_table: slot_table,
      registry: registry,
      pool_supervisor: pool_supervisor,
      conn_opts: conn_opts,
      seed_nodes: seed_nodes,
      refresh_interval: refresh_interval
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

  def cooling_down(:info, _msg, _data), do: :keep_state_and_data

  ## Private helpers

  defp periodic_refresh_action(interval) do
    {{:timeout, :periodic_refresh}, interval, :refresh}
  end

  defp handle_down(data, ref) do
    {node_id, monitors} = Map.pop(data.monitors, ref)
    data = %{data | monitors: monitors}

    if node_id do
      [host, port_str] = String.split(node_id, ":")
      port = String.to_integer(port_str)
      start_and_monitor_connection(data, node_id, host, port)
    else
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
    |> Enum.map(fn node_id ->
      [host, port_str] = String.split(node_id, ":")
      {host, String.to_integer(port_str)}
    end)
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
      [start_slot, end_slot, [host, port | _] | _replicas] = slot_range
      node_id = "#{host}:#{port}"

      for slot <- start_slot..end_slot do
        :ets.insert(data.slot_table, {slot, node_id})
      end
    end
  end

  defp ensure_connections(data, slots_data) do
    needed_nodes =
      slots_data
      |> Enum.map(fn [_start, _end, [host, port | _] | _] -> {"#{host}:#{port}", host, port} end)
      |> Enum.uniq_by(fn {node_id, _, _} -> node_id end)

    data =
      Enum.reduce(needed_nodes, data, fn {node_id, host, port}, acc ->
        case Registry.lookup(acc.registry, node_id) do
          [{pid, _}] when is_pid(pid) ->
            if Process.alive?(pid),
              do: acc,
              else: start_and_monitor_connection(acc, node_id, host, port)

          [] ->
            start_and_monitor_connection(acc, node_id, host, port)
        end
      end)

    needed_ids = MapSet.new(needed_nodes, fn {node_id, _, _} -> node_id end)

    registered_nodes =
      Registry.select(data.registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    for {node_id, pid} <- registered_nodes, not MapSet.member?(needed_ids, node_id) do
      if Process.alive?(pid) do
        DynamicSupervisor.terminate_child(data.pool_supervisor, pid)
      end
    end

    data
  end

  defp start_and_monitor_connection(data, node_id, host, port) do
    case start_connection(
           data.pool_supervisor,
           data.registry,
           node_id,
           host,
           port,
           data.conn_opts
         ) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        %{data | monitors: Map.put(data.monitors, ref, node_id)}

      {:error, reason} ->
        :telemetry.execute([:redix, :cluster, :node_connection_failed], %{}, %{
          cluster: data.cluster_name,
          address: node_id,
          reason: reason
        })

        data
    end
  end

  defp start_connection(pool_supervisor, registry, node_id, host, port, conn_opts) do
    opts =
      conn_opts
      |> Keyword.delete(:name)
      |> Keyword.merge(
        host: host,
        port: port,
        sync_connect: false,
        name: {:via, Registry, {registry, node_id}}
      )

    DynamicSupervisor.start_child(pool_supervisor, {Redix, opts})
  end
end
