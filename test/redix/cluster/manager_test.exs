defmodule Redix.Cluster.ManagerTest do
  use ExUnit.Case

  @moduletag :cluster

  @nodes ["redis://localhost:7000", "redis://localhost:7001", "redis://localhost:7002"]

  setup_all do
    case :gen_tcp.connect(~c"localhost", 7000, []) do
      {:ok, socket} -> :gen_tcp.close(socket)
      {:error, _reason} -> flunk("Redis Cluster not available on localhost:7000")
    end

    :ok
  end

  setup do
    cluster_name = :"mgr_test_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Redix.Cluster, nodes: @nodes, name: cluster_name, primary_pool_size: 3, sync_connect: true}
    )

    registry = :"#{cluster_name}_registry"

    wait_until_passes(2_000, fn ->
      connected_members =
        Registry.select(registry, [{{{:_, :_}, :_, {:primary, :connected}}, [], [true]}])

      assert length(connected_members) >= 9
    end)

    %{
      cluster: cluster_name,
      manager: :"#{cluster_name}_manager",
      registry: registry
    }
  end

  describe "state transitions" do
    test "cooling_down drops a second reactive refresh", %{cluster: cluster, manager: manager} do
      :telemetry_test.attach_event_handlers(self(), [
        [:redix, :cluster, :topology_change]
      ])

      # Two rapid refreshes — second should be dropped during cooldown.
      Redix.Cluster.Manager.refresh_topology(manager)
      Redix.Cluster.Manager.refresh_topology(manager)

      # Should receive exactly one topology_change for this cluster.
      assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^cluster}}
      refute_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^cluster}}, 500
    end

    test "periodic refresh fires on schedule", %{cluster: _cluster} do
      short_name = :"periodic_#{System.unique_integer([:positive])}"

      :telemetry_test.attach_event_handlers(self(), [
        [:redix, :cluster, :topology_change]
      ])

      start_supervised!(
        {Redix.Cluster,
         nodes: @nodes, name: short_name, topology_refresh_interval: 500, sync_connect: true},
        id: :short_refresh
      )

      # One fires on init, then at least one more from periodic refresh.
      assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^short_name}},
                     2_000

      assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^short_name}},
                     2_000
    end

    test "periodic refresh is postponed during cooldown, not dropped", %{
      cluster: _cluster,
      manager: _manager
    } do
      short_name = :"postpone_#{System.unique_integer([:positive])}"

      :telemetry_test.attach_event_handlers(self(), [
        [:redix, :cluster, :topology_change]
      ])

      start_supervised!(
        {Redix.Cluster,
         nodes: @nodes, name: short_name, topology_refresh_interval: 500, sync_connect: true},
        id: :postpone_refresh
      )

      # Consume the init topology_change.
      assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^short_name}},
                     2_000

      # Trigger reactive refresh to enter cooling_down (1s cooldown).
      Redix.Cluster.Manager.refresh_topology(:"#{short_name}_manager")

      assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^short_name}},
                     2_000

      # The periodic refresh should still fire after cooldown expires,
      # not be dropped. Wait for it.
      assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^short_name}},
                     3_000
    end

    test "a stray :info message in :ready does not crash the Manager (#326)", %{
      manager: manager
    } do
      {:ready, _data} = :sys.get_state(manager)

      pid = Process.whereis(manager)
      ref = Process.monitor(pid)

      send(pid, :some_stray_message)

      # The Manager must absorb it and stay :ready, not crash the whole tree.
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200
      assert {:ready, _data} = :sys.get_state(manager)
    end
  end

  describe "connection lifecycle" do
    test "pool members are started for all primary nodes", %{registry: registry} do
      registered =
        Registry.select(
          registry,
          [
            {{{:"$1", :"$2"}, :"$3", {:"$4", :_}}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}
          ]
        )

      # A 3-primary cluster should have at least 3 pools of 3 connections.
      assert length(registered) >= 9

      for {node_id, index, pid, role} <- registered do
        assert is_binary(node_id)
        assert String.contains?(node_id, ":")
        assert index in 0..2
        assert Process.alive?(pid)
        assert role == :primary
      end

      assert Enum.all?(Enum.group_by(registered, &elem(&1, 0)), fn {_node_id, members} ->
               Enum.sort(Enum.map(members, &elem(&1, 1))) == [0, 1, 2]
             end)
    end

    test "dead pool member is restarted with the same index and a new PID", %{
      registry: registry
    } do
      [{{node_id, index}, pid} | _] =
        Registry.select(registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      sibling_pids =
        registry
        |> Registry.select([{{{node_id, :_}, :"$1", :_}, [], [:"$1"]}])
        |> MapSet.new()
        |> MapSet.delete(pid)

      Process.exit(pid, :kill)

      wait_until_passes(2_000, fn ->
        assert [{new_pid, _}] = Registry.lookup(registry, {node_id, index})
        assert new_pid != pid
        assert Process.alive?(new_pid)

        current_siblings =
          registry
          |> Registry.select([{{{node_id, :_}, :"$1", :_}, [], [:"$1"]}])
          |> MapSet.new()
          |> MapSet.delete(new_pid)

        assert current_siblings == sibling_pids
      end)
    end

    test "lookup spreads across callers and keeps each caller sticky through redirects", %{
      registry: registry,
      manager: manager
    } do
      [node_id | _] =
        Registry.select(registry, [{{{:"$1", :_}, :_, :_}, [], [:"$1"]}])

      {:ok, host, port} = Redix.Cluster.Manager.split_host_port(node_id)
      address = {host, port}

      same_caller_pids =
        for _ <- 1..10 do
          {:ok, pid} = Redix.Cluster.Manager.get_connection_by_node(registry, address, self())
          pid
        end

      assert same_caller_pids |> Enum.uniq() |> length() == 1

      caller_pids =
        1..24
        |> Task.async_stream(
          fn _ -> Redix.Cluster.Manager.get_connection_by_node(registry, address, self()) end,
          ordered: false
        )
        |> Enum.map(fn {:ok, {:ok, pid}} -> pid end)

      assert caller_pids |> Enum.uniq() |> length() > 1

      original_caller = self()
      [expected_pid] = Enum.uniq(same_caller_pids)

      redirected_pids =
        1..12
        |> Task.async_stream(
          fn _ ->
            Redix.Cluster.Manager.get_connection_by_node(registry, address, original_caller)
          end,
          ordered: false
        )
        |> Enum.map(fn {:ok, {:ok, pid}} -> pid end)

      assert Enum.uniq(redirected_pids) == [expected_pid]

      manager_result =
        Task.async(fn ->
          Redix.Cluster.Manager.connect_to_node(manager, address, 5_000, original_caller)
        end)
        |> Task.await()

      assert manager_result == {:ok, expected_pid}
    end

    test "lookup uses a sibling while its sticky member is down", %{
      registry: registry,
      manager: manager
    } do
      [node_id | _] =
        Registry.select(registry, [{{{:"$1", :_}, :_, :_}, [], [:"$1"]}])

      index = :erlang.phash2(self(), 3)
      [{pid, _role}] = Registry.lookup(registry, {node_id, index})
      {:ok, host, port} = Redix.Cluster.Manager.split_host_port(node_id)

      parent = self()
      telemetry_ref = make_ref()
      handler_id = "#{inspect(manager)}_restarted"

      :telemetry.attach(
        handler_id,
        [:redix, :cluster, :node_connection_restarted],
        fn _event, _measurements, meta, _config -> send(parent, {telemetry_ref, meta}) end,
        :no_config
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      :sys.suspend(manager)

      try do
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000

        wait_until_passes(1_000, fn ->
          assert Registry.lookup(registry, {node_id, index}) == []
        end)

        assert {:ok, sibling_pid} =
                 Redix.Cluster.Manager.get_connection_by_node(registry, {host, port}, self())

        assert sibling_pid != pid
        assert Process.alive?(sibling_pid)
      after
        :sys.resume(manager)
      end

      wait_until_passes(2_000, fn ->
        assert [{new_pid, _role}] = Registry.lookup(registry, {node_id, index})
        assert new_pid != pid
      end)

      assert_receive {^telemetry_ref, %{address: ^node_id, role: :primary, reason: :killed}}
    end
  end

  describe "connected pool member routing" do
    test "commands skip a disconnected sticky member for many callers" do
      %{cluster: cluster, registry: registry, slot_table: slot_table} =
        start_routing_cluster(backoff_initial: 5_000, backoff_max: 5_000)

      key = "connected-pool-member"
      node_id = primary_node_for_key(slot_table, key)
      disconnected_index = 0

      assert Redix.Cluster.command(cluster, ["SET", key, "value"]) == {:ok, "OK"}
      disconnected_pid = force_disconnect(registry, node_id, disconnected_index)

      results =
        for _ <- 1..20 do
          call_from_pool_index(disconnected_index, 3, fn ->
            Redix.Cluster.command(cluster, ["GET", key])
          end)
        end

      assert results |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == 20
      assert Enum.all?(results, fn {_caller, result} -> result == {:ok, "value"} end)

      assert [{^disconnected_pid, {:primary, :disconnected}}] =
               Registry.lookup(registry, {node_id, disconnected_index})
    end

    test "commands return closed when all members of a node are disconnected" do
      %{cluster: cluster, registry: registry, slot_table: slot_table} =
        start_routing_cluster(backoff_initial: 5_000, backoff_max: 5_000)

      key = "disconnected-pool"
      node_id = primary_node_for_key(slot_table, key)

      assert Redix.Cluster.command(cluster, ["SET", key, "value"]) == {:ok, "OK"}

      for index <- 0..2 do
        force_disconnect(registry, node_id, index)
      end

      assert Redix.Cluster.command(cluster, ["GET", key]) ==
               {:error, %Redix.ConnectionError{reason: :closed}}
    end

    test "a reconnected member becomes eligible for its sticky callers again" do
      %{cluster: cluster, registry: registry, slot_table: slot_table} =
        start_routing_cluster(backoff_initial: 500, backoff_max: 500)

      key = "reconnected-pool-member"
      node_id = primary_node_for_key(slot_table, key)
      reconnected_index = 0

      assert Redix.Cluster.command(cluster, ["SET", key, "value"]) == {:ok, "OK"}
      reconnected_pid = force_disconnect(registry, node_id, reconnected_index)

      wait_until_passes(2_000, fn ->
        assert [{^reconnected_pid, {:primary, :connected}}] =
                 Registry.lookup(registry, {node_id, reconnected_index})
      end)

      test_pid = self()
      event_ref = make_ref()
      handler_id = "reconnected_member_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:redix, :pipeline, :start],
        fn _event, _measurements, metadata, _config ->
          if metadata.extra_metadata[:cluster] == cluster do
            send(test_pid, {event_ref, metadata.connection})
          end
        end,
        :no_config
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      {_caller, result} =
        call_from_pool_index(reconnected_index, 3, fn ->
          Redix.Cluster.command(cluster, ["GET", key])
        end)

      assert result == {:ok, "value"}
      assert_receive {^event_ref, ^reconnected_pid}, 1_000
    end

    test "keyless commands skip disconnected primary members" do
      %{cluster: cluster, registry: registry} =
        start_routing_cluster(backoff_initial: 5_000, backoff_max: 5_000)

      primary_members =
        Registry.select(registry, [
          {{{:"$1", :"$2"}, :"$3", {:primary, :connected}}, [], [{{:"$1", :"$2", :"$3"}}]}
        ])

      random_seed = :rand.export_seed()
      {node_id, index, expected_old_pid} = Enum.random(primary_members)
      :rand.seed(random_seed)

      force_disconnect(registry, node_id, index)

      assert Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"}

      assert [{^expected_old_pid, {:primary, :disconnected}}] =
               Registry.lookup(registry, {node_id, index})
    end
  end

  describe "monitor cleanup" do
    test "old monitor ref is removed when connection dies", %{
      cluster: _cluster,
      registry: registry,
      manager: manager
    } do
      {_state, data_before} = :sys.get_state(manager)
      monitors_before = data_before.monitors
      assert map_size(monitors_before) >= 9

      # Pick a pool member that we know is in the monitors map.
      {old_ref, {node_id, index, _role}} = Enum.at(monitors_before, 0)
      [{pid, _}] = Registry.lookup(registry, {node_id, index})

      Process.exit(pid, :kill)

      # Wait for the DOWN handler to process and the old ref to be cleaned up.
      wait_until_passes(2_000, fn ->
        {_state, data_after} = :sys.get_state(manager)
        # The old monitor ref must no longer be in the monitors map.
        refute Map.has_key?(data_after.monitors, old_ref)
      end)
    end

    test "connection is replaced in registry after kill", %{
      cluster: _cluster,
      registry: registry,
      manager: manager
    } do
      {_state, data_before} = :sys.get_state(manager)
      monitors_before = data_before.monitors

      # Pick a monitored pool member and kill its connection.
      {_old_ref, {node_id, index, _role}} = Enum.at(monitors_before, 0)
      [{pid, _}] = Registry.lookup(registry, {node_id, index})

      Process.exit(pid, :kill)

      # Wait for the new connection to appear in the registry. The DOWN-driven
      # restart backs off (issue #334), so the replacement isn't instantaneous —
      # `assert` (not a bare match) so `wait_until_passes` retries until it lands.
      wait_until_passes(2_000, fn ->
        assert [{new_pid, _}] = Registry.lookup(registry, {node_id, index})
        assert new_pid != pid
        assert Process.alive?(new_pid)
      end)
    end
  end

  describe "node removal" do
    test "a removed node is terminated and not resurrected on refresh (#305)", %{
      registry: registry,
      manager: manager
    } do
      # Simulate a node that the Manager tracks (connected + monitored) but that is
      # not part of `CLUSTER SLOTS` — e.g. a node that just left the cluster.
      # `connect_to_node/3` registers and monitors it exactly like a real node, and
      # `sync_connect: false` means the connection starts even if 7099 is refused.
      fake_node = {"127.0.0.1", 7099}
      fake_id = "127.0.0.1:7099"

      {:ok, fake_pid} = Redix.Cluster.Manager.connect_to_node(manager, fake_node, 5_000)
      ref = Process.monitor(fake_pid)

      assert 3 ==
               registry
               |> Registry.select([{{{fake_id, :_}, :_, :_}, [], [true]}])
               |> length()

      # The Manager should be monitoring it.
      {_state, data} = :sys.get_state(manager)
      assert Enum.count(data.monitors, fn {_ref, {id, _index, _role}} -> id == fake_id end) == 3

      :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])

      # A refresh: the fake node is absent from `CLUSTER SLOTS`, so
      # `ensure_connections/2` must terminate it — and must NOT bring it back.
      Redix.Cluster.Manager.refresh_topology(manager)

      # The fake connection is terminated...
      assert_receive {:DOWN, ^ref, :process, ^fake_pid, _reason}, 2_000

      # ...and the refresh finishes.
      assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, _meta}, 2_000

      # With the bug, the deliberate `terminate_child` DOWN lands in `handle_down/2`
      # and resurrects the node. Give the Manager time to process that DOWN, then
      # assert it stayed gone — both from the registry and the monitors map.
      Process.sleep(200)
      assert Registry.select(registry, [{{{fake_id, :_}, :_, :_}, [], [true]}]) == []

      {_state, data} = :sys.get_state(manager)
      refute Enum.any?(data.monitors, fn {_ref, {id, _index, _role}} -> id == fake_id end)
    end
  end

  defp start_routing_cluster(conn_opts) do
    cluster = :"pool_routing_#{System.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [nodes: @nodes, name: cluster, primary_pool_size: 3, sync_connect: true],
        conn_opts
      )

    start_supervised!({Redix.Cluster, opts}, id: cluster)

    registry = :"#{cluster}_registry"

    wait_until_passes(2_000, fn ->
      connected_members =
        Registry.select(registry, [{{{:_, :_}, :_, {:primary, :connected}}, [], [true]}])

      assert length(connected_members) == 9
    end)

    %{
      cluster: cluster,
      registry: registry,
      slot_table: :"#{cluster}_slots"
    }
  end

  defp primary_node_for_key(slot_table, key) do
    slot = Redix.Cluster.Hash.hash_slot(key)
    [{^slot, node_id, _replica_ids}] = :ets.lookup(slot_table, slot)
    node_id
  end

  defp force_disconnect(registry, node_id, index) do
    [{pid, {role, :connected}}] = Registry.lookup(registry, {node_id, index})
    {:connected, data} = :sys.get_state(pid)
    send(data.socket_owner, {:force_disconnect, pid, :closed})

    wait_until_passes(1_000, fn ->
      assert [{^pid, {^role, :disconnected}}] = Registry.lookup(registry, {node_id, index})
    end)

    pid
  end

  defp call_from_pool_index(index, pool_size, fun) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        send(parent, {ref, self(), :erlang.phash2(self(), pool_size)})

        receive do
          {:run, ^ref} -> send(parent, {ref, :result, fun.()})
          {:stop, ^ref} -> :ok
        end
      end)

    receive do
      {^ref, ^pid, ^index} ->
        send(pid, {:run, ref})

        receive do
          {^ref, :result, result} -> {pid, result}
        end

      {^ref, ^pid, _other_index} ->
        send(pid, {:stop, ref})
        call_from_pool_index(index, pool_size, fun)
    end
  end

  defp wait_until_passes(timeout, fun) when timeout <= 0, do: fun.()

  defp wait_until_passes(timeout, fun) do
    fun.()
  rescue
    ExUnit.AssertionError ->
      Process.sleep(10)
      wait_until_passes(timeout - 10, fun)
  end
end
