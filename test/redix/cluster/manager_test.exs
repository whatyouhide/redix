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

defmodule Redix.Cluster.ManagerTopologyTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.{FakeNode, Manager}

  @moduletag :cluster

  test "DNS rotation keeps every pool member and starts no new pool connections" do
    addresses = start_supervised!({Agent, fn -> [{127, 0, 0, 1}, {127, 0, 0, 2}] end})
    test = self()

    inet_lookup_fun = fn ~c"localhost", family ->
      send(test, {:lookup, family})

      case family do
        :inet -> {:ok, Agent.get(addresses, & &1)}
        :inet6 -> {:error, :nxdomain}
      end
    end

    node = FakeNode.reserve()

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] ->
        FakeNode.cluster_slots([
          {0, 8191, {"localhost", node.port}},
          {8192, 16_383, {"localhost", node.port}}
        ])

      _command ->
        "+OK\r\n"
    end)

    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])

    %{cluster: cluster, manager: manager, registry: registry, slots: slots} =
      start_manager(node, inet_lookup_fun)

    assert_receive {[:redix, :cluster, :topology_change], _, _,
                    %{cluster: ^cluster, changed: true}}

    assert_one_lookup_per_family()
    before_members = members(registry)
    assert length(before_members) == 5

    assert Enum.all?(before_members, fn {_key, pid, _value} ->
             Redix.command(pid, ["PING"]) == {:ok, "OK"}
           end)

    baseline = FakeNode.connections_accepted(node)

    Agent.update(addresses, &Enum.reverse/1)
    refresh(manager, cluster, false)
    assert_one_lookup_per_family()

    assert members(registry) == before_members
    node_id = "localhost:#{node.port}"
    assert [{0, ^node_id, []}] = :ets.lookup(slots, 0)
    assert [{16_383, ^node_id, []}] = :ets.lookup(slots, 16_383)
    # Each refresh opens one short connection to fetch CLUSTER SLOTS.
    assert FakeNode.connections_accepted(node) == baseline + 1
  end

  test "successful DNS changes replace aliases and failed lookups keep the last addresses" do
    addresses = start_supervised!({Agent, fn -> {:ok, [{127, 0, 0, 1}, {127, 0, 0, 2}]} end})

    inet_lookup_fun = fn
      ~c"localhost", :inet -> Agent.get(addresses, & &1)
      ~c"localhost", :inet6 -> {:error, :nxdomain}
    end

    node = FakeNode.reserve()

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, {"localhost", node.port}}])
      _command -> "+OK\r\n"
    end)

    %{cluster: cluster, manager: manager, registry: registry} =
      start_manager(node, inet_lookup_fun)

    before_members = members(registry)
    assert {:ok, pid} = Manager.get_connection_by_node(registry, {"127.0.0.2", node.port}, self())
    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])

    Agent.update(addresses, fn _ -> {:ok, [{127, 0, 0, 3}]} end)
    refresh(manager, cluster, false)

    assert members(registry) == before_members
    assert Manager.get_connection_by_node(registry, {"127.0.0.2", node.port}, self()) == :error

    assert Manager.get_connection_by_node(registry, {"127.0.0.3", node.port}, self()) ==
             {:ok, pid}

    Agent.update(addresses, fn _ -> {:error, :nxdomain} end)
    refresh(manager, cluster, false)

    assert Manager.connect_to_node(manager, {"127.0.0.3", node.port}, 1_000) == {:ok, pid}
    assert members(registry) == before_members

    assert Manager.get_connection_by_node(registry, {"127.0.0.3", node.port}, self()) ==
             {:ok, pid}

    assert Manager.get_connection_by_node(registry, {"localhost", node.port}, self()) ==
             {:ok, pid}

    Agent.update(addresses, fn _ -> {:ok, []} end)
    refresh(manager, cluster, false)
    assert Manager.get_connection_by_node(registry, {"127.0.0.3", node.port}, self()) == :error
    assert members(registry) == before_members
  end

  for redirect <- ["MOVED", "ASK"] do
    @tag redirect: redirect
    test "#{redirect} to any known IP reuses the hostname pool after address mapping", %{
      redirect: redirect
    } do
      node = FakeNode.reserve()
      test = self()
      ipv6 = {0, 0, 0, 0, 0, 0, 0, 1}

      inet_lookup_fun = fn ~c"localhost", family ->
        send(test, {:lookup, family})

        case family do
          :inet -> {:ok, [{127, 0, 0, 1}, {127, 0, 0, 2}]}
          :inet6 -> {:ok, [ipv6]}
        end
      end

      FakeNode.serve(node, fn
        ["CLUSTER", "SLOTS"] ->
          FakeNode.cluster_slots([{0, 16_383, {"announced.invalid", 7000}}])

        ["GET", key] ->
          if Process.get({:redirected, key}) do
            "+VALUE\r\n"
          else
            Process.put({:redirected, key}, true)
            "-#{redirect} #{Redix.Cluster.Hash.hash_slot(key)} #{key}:7000\r\n"
          end

        _command ->
          "+OK\r\n"
      end)

      mapper = fn
        "announced.invalid", 7000 -> {"localhost", node.port}
        host, 7000 -> {host, node.port}
      end

      %{cluster: cluster, manager: manager, registry: registry, slots: slots} =
        start_manager(node, inet_lookup_fun, address_mapper: mapper)

      assert_one_lookup_per_family()
      before_members = members(registry)
      baseline = FakeNode.connections_accepted(node)

      for host <- ["localhost", "127.0.0.1", "127.0.0.2", "::1", "0:0:0:0:0:0:0:1"] do
        assert {:ok, pid} = Manager.get_connection_by_node(registry, {host, node.port}, self())
        assert Enum.any?(before_members, fn {_key, member, _value} -> member == pid end)
        assert Manager.connect_to_node(manager, {host, node.port}, 1_000) == {:ok, pid}
      end

      assert {:ok, _pid} = Manager.get_connection(slots, registry, 0, 5)

      refute_receive {:lookup, _family}
      assert FakeNode.connections_accepted(node) == baseline

      :telemetry_test.attach_event_handlers(self(), [
        [:redix, :cluster, :topology_change],
        [:redix, :connection]
      ])

      for host <- ["127.0.0.1", "127.0.0.2", "::1"] do
        assert Redix.Cluster.command(cluster, ["GET", host]) == {:ok, "VALUE"}
      end

      if redirect == "MOVED" do
        assert_receive {[:redix, :cluster, :topology_change], _, _,
                        %{cluster: ^cluster, changed: false}}

        assert_one_lookup_per_family()
        assert FakeNode.connections_accepted(node) == baseline + 1
      else
        refute_receive {:lookup, _family}
        assert FakeNode.connections_accepted(node) == baseline
      end

      assert members(registry) == before_members
      refute_receive {[:redix, :connection], _, _, %{cluster: ^cluster, reconnection: false}}
    end
  end

  test "node removal clears aliases retained after a failed DNS lookup" do
    node = FakeNode.reserve()
    state = start_supervised!({Agent, fn -> %{failed?: false, removed?: false} end})

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] ->
        if Agent.get(state, & &1.removed?) do
          FakeNode.cluster_slots([])
        else
          FakeNode.cluster_slots([{0, 16_383, {"localhost", node.port}}])
        end

      _command ->
        "+OK\r\n"
    end)

    inet_lookup_fun = fn
      _host, :inet ->
        if Agent.get(state, & &1.failed?), do: {:error, :timeout}, else: {:ok, [{127, 0, 0, 1}]}

      _host, :inet6 ->
        {:error, :nxdomain}
    end

    %{cluster: cluster, manager: manager, registry: registry} =
      start_manager(node, inet_lookup_fun)

    assert {:ok, _pid} =
             Manager.get_connection_by_node(registry, {"127.0.0.1", node.port}, self())

    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])
    Agent.update(state, &%{&1 | failed?: true})
    refresh(manager, cluster, false)

    # A lookup failure must not retain a node that left the topology.
    Agent.update(state, &%{&1 | removed?: true})
    refresh(manager, cluster, true)
    FakeNode.wait_until(fn -> members(registry) == [] end)
    assert Manager.get_connection_by_node(registry, {"127.0.0.1", node.port}, self()) == :error
  end

  for timeout <- [500, :infinity] do
    @tag capture_log: true
    test "a failed DNS task keeps the Manager and pool with timeout #{inspect(timeout)}" do
      node = FakeNode.reserve()
      failed = start_supervised!({Agent, fn -> false end})

      FakeNode.serve(node, fn
        ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, {"localhost", node.port}}])
        _command -> "+OK\r\n"
      end)

      inet_lookup_fun = fn
        _host, :inet ->
          if Agent.get(failed, & &1),
            do: raise("DNS lookup failed"),
            else: {:ok, [{127, 0, 0, 1}]}

        _host, :inet6 ->
          {:error, :nxdomain}
      end

      %{cluster: cluster, manager: manager, registry: registry} =
        start_manager(node, inet_lookup_fun,
          conn_opts: Redix.StartOptions.sanitize(:redix, timeout: unquote(timeout))
        )

      manager_pid = Process.whereis(manager)
      before_members = members(registry)
      assert length(before_members) == 5
      :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])

      Agent.update(failed, fn _ -> true end)
      refresh(manager, cluster, false)
      assert Process.whereis(manager) == manager_pid
      assert {:ok, _pid} = Manager.connect_to_node(manager, {"127.0.0.1", node.port}, 1_000)
      assert members(registry) == before_members
    end
  end

  test "one slow DNS family does not prevent another host from updating its addresses" do
    first = FakeNode.reserve()
    second = FakeNode.reserve()
    hosts = %{"localhost" => first.port, "LOCALHOST" => second.port}
    delay = start_supervised!({Agent, fn -> false end})
    test = self()

    for node <- [first, second] do
      FakeNode.serve(node, fn
        ["CLUSTER", "SLOTS"] ->
          FakeNode.cluster_slots([
            {0, 8191, {"localhost", first.port}},
            {8192, 16_383, {"LOCALHOST", second.port}}
          ])

        _command ->
          "+OK\r\n"
      end)
    end

    inet_lookup_fun = fn
      _host, :inet ->
        ip = if Agent.get(delay, & &1), do: {127, 0, 0, 2}, else: {127, 0, 0, 1}
        {:ok, [ip]}

      host, :inet6 ->
        if Agent.get(delay, & &1) do
          send(test, {:dns_blocked, to_string(host)})
          Process.sleep(1_000)
        end

        {:error, :nxdomain}
    end

    %{cluster: cluster, manager: manager, registry: registry} =
      start_manager(first, inet_lookup_fun,
        conn_opts: Redix.StartOptions.sanitize(:redix, timeout: 300)
      )

    before_members = members(registry)
    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])
    Agent.update(delay, fn _ -> true end)
    Manager.refresh_topology(manager)
    assert_receive {:dns_blocked, blocked_host}
    assert_receive {[:redix, :cluster, :topology_change], _, _, %{cluster: ^cluster}}
    [{_other_host, other_port}] = Enum.reject(hosts, fn {host, _port} -> host == blocked_host end)

    assert {:ok, _pid} =
             Manager.get_connection_by_node(registry, {"127.0.0.2", other_port}, self())

    assert members(registry) == before_members
  end

  test "a redirect cannot assign a shared IP to one hostname pool" do
    node = FakeNode.reserve()
    failed = start_supervised!({Agent, fn -> false end})

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] ->
        FakeNode.cluster_slots([
          {0, 8191, {"localhost", node.port}},
          {8192, 16_383, {"LOCALHOST", node.port}}
        ])

      _command ->
        "+OK\r\n"
    end)

    inet_lookup_fun = fn
      ~c"LOCALHOST", :inet ->
        if Agent.get(failed, & &1), do: {:error, :timeout}, else: {:ok, [{127, 0, 0, 1}]}

      _host, :inet ->
        {:ok, [{127, 0, 0, 1}]}

      _host, :inet6 ->
        {:error, :nxdomain}
    end

    %{cluster: cluster, manager: manager, registry: registry} =
      start_manager(node, inet_lookup_fun)

    shared_ip = {"127.0.0.1", node.port}
    before_members = members(registry)
    assert Manager.get_connection_by_node(registry, shared_ip, self()) == :error
    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])
    Agent.update(failed, fn _ -> true end)
    refresh(manager, cluster, false)
    assert Manager.get_connection_by_node(registry, shared_ip, self()) == :error

    assert {:ok, redirected_pid} =
             Manager.connect_to_node(manager, {"localhost.", node.port}, 1_000)

    assert Manager.get_connection_by_node(registry, {"localhost.", node.port}, self()) ==
             {:ok, redirected_pid}

    assert Manager.get_connection_by_node(registry, shared_ip, self()) == :error

    refresh(manager, cluster, false)
    refute Process.alive?(redirected_pid)
    # Registry can still list dead PIDs until it processes their exit messages.
    FakeNode.wait_until(fn -> members(registry) == before_members end)
  end

  test "a slow DNS family cannot block the Manager past the refresh budget" do
    node = FakeNode.reserve()
    delay = start_supervised!({Agent, fn -> false end})
    test = self()

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, {"localhost", node.port}}])
      _command -> "+OK\r\n"
    end)

    inet_lookup_fun = fn
      _host, :inet ->
        {:ok, [{127, 0, 0, 1}]}

      _host, :inet6 ->
        if Agent.get(delay, & &1) do
          send(test, {:dns_blocked, self()})

          receive do
            :release_dns -> :ok
          after
            1_000 -> :ok
          end
        end

        {:ok, [{0, 0, 0, 0, 0, 0, 0, 1}]}
    end

    %{cluster: cluster, manager: manager, registry: registry} =
      start_manager(node, inet_lookup_fun,
        conn_opts: Redix.StartOptions.sanitize(:redix, timeout: 300)
      )

    before_members = members(registry)
    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])
    Agent.update(delay, fn _ -> true end)
    Manager.refresh_topology(manager)
    assert_receive {:dns_blocked, worker}
    ref = Process.monitor(worker)
    assert {:ok, _pid} = Manager.connect_to_node(manager, {"localhost", node.port}, 600)
    assert_receive {:DOWN, ^ref, :process, ^worker, reason}
    # The worker can stop before the test gets time to monitor it.
    assert reason in [:killed, :noproc]

    assert_receive {[:redix, :cluster, :topology_change], _, _,
                    %{cluster: ^cluster, changed: false}}

    assert members(registry) == before_members

    assert {:ok, _pid} =
             Manager.get_connection_by_node(registry, {"127.0.0.1", node.port}, self())

    assert {:ok, _pid} = Manager.get_connection_by_node(registry, {"::1", node.port}, self())
  end

  test "equivalent literal IPv6 addresses keep node IDs and do not count as topology changes" do
    node = FakeNode.reserve(inet6: true)
    host = start_supervised!({Agent, fn -> "::1" end})

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] ->
        FakeNode.cluster_slots([{0, 16_383, {Agent.get(host, & &1), node.port}}])

      _command ->
        "+OK\r\n"
    end)

    inet_lookup_fun = fn _host, _family -> flunk("literal IPs must not use DNS") end

    %{cluster: cluster, manager: manager, registry: registry} =
      start_manager(node, inet_lookup_fun,
        conn_opts: Redix.StartOptions.sanitize(:redix, socket_opts: [:inet6])
      )

    before_members = members(registry)
    assert length(node_members(before_members, node.id)) == 5
    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])
    Agent.update(host, fn _ -> "0:0:0:0:0:0:0:1" end)
    refresh(manager, cluster, false)
    assert members(registry) == before_members
  end

  test "pool recovery keeps the dial hostname after an IP redirect" do
    node = FakeNode.reserve()

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, {"localhost", node.port}}])
      _command -> "+OK\r\n"
    end)

    inet_lookup_fun = fn
      _host, :inet -> {:ok, [{127, 0, 0, 1}]}
      _host, :inet6 -> {:error, :nxdomain}
    end

    %{manager: manager, registry: registry} = start_manager(node, inet_lookup_fun)
    assert {:ok, pid} = Manager.connect_to_node(manager, {"127.0.0.1", node.port}, 1_000)
    {key, ^pid, _value} = Enum.find(members(registry), &(elem(&1, 1) == pid))
    Process.exit(pid, :kill)

    FakeNode.wait_until(fn ->
      case Registry.lookup(registry, key) do
        [{new_pid, {:primary, :connected}}] when new_pid != pid -> true
        _other -> false
      end
    end)

    [{new_pid, _value}] = Registry.lookup(registry, key)
    {:connected, data} = :sys.get_state(new_pid)
    assert to_string(data.opts[:host]) == "localhost"
    assert Redix.command(new_pid, ["PING"]) == {:ok, "OK"}
  end

  test "the first successful empty topology counts as a change after failed discovery" do
    node = FakeNode.reserve()
    reply = start_supervised!({Agent, fn -> "-ERR unavailable\r\n" end})

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] -> Agent.get(reply, & &1)
      _command -> "+OK\r\n"
    end)

    inet_lookup_fun = fn _host, _family -> flunk("an empty topology needs no DNS") end

    :telemetry_test.attach_event_handlers(self(), [
      [:redix, :cluster, :failed_topology_refresh],
      [:redix, :cluster, :topology_change]
    ])

    %{cluster: cluster, manager: manager} =
      start_manager(node, inet_lookup_fun,
        sync_connect: false,
        conn_opts: Redix.StartOptions.sanitize(:redix, backoff_initial: 100)
      )

    assert_receive {[:redix, :cluster, :failed_topology_refresh], _, _, %{cluster: ^cluster}}
    Agent.update(reply, fn _ -> FakeNode.cluster_slots([]) end)

    assert_receive {[:redix, :cluster, :topology_change], _, %{node_count: 0},
                    %{cluster: ^cluster, changed: true}}

    refresh(manager, cluster, false)
  end

  test "telemetry detects slot moves, role changes, and node removal" do
    first = FakeNode.reserve()
    second = FakeNode.reserve()
    first_address = {"localhost", first.port}
    second_address = {"localhost", second.port}

    initial = [
      {0, 8191, first_address, [second_address]},
      {8192, 16_383, second_address, [first_address]}
    ]

    topology = start_supervised!({Agent, fn -> initial end})

    handler = fn
      ["CLUSTER", "SLOTS"] ->
        FakeNode.cluster_slots(Agent.get(topology, & &1))

      _command ->
        "+OK\r\n"
    end

    FakeNode.serve(first, handler)
    FakeNode.serve(second, handler)
    inet_lookup_fun = fn _host, _family -> {:ok, [{127, 0, 0, 1}]} end

    :telemetry_test.attach_event_handlers(self(), [[:redix, :cluster, :topology_change]])

    %{cluster: cluster, manager: manager, registry: registry, slots: slots} =
      start_manager(first, inet_lookup_fun, read_from_replicas: true)

    assert_receive {[:redix, :cluster, :topology_change], _, _,
                    %{cluster: ^cluster, changed: true}}

    original_members = members(registry)

    # Reordering and splitting ranges does not change the slot map.
    Agent.update(topology, fn _ ->
      [
        {8192, 16_383, second_address, [first_address]},
        {0, 4000, first_address, [second_address]},
        {4001, 8191, first_address, [second_address]}
      ]
    end)

    refresh(manager, cluster, false)
    assert members(registry) == original_members

    Agent.update(topology, fn _ ->
      [
        {0, 8192, first_address, [second_address]},
        {8193, 16_383, second_address, [first_address]}
      ]
    end)

    refresh(manager, cluster, true)
    assert members(registry) == original_members
    first_id = "localhost:#{first.port}"
    second_id = "localhost:#{second.port}"
    assert [{8192, ^first_id, [^second_id]}] = :ets.lookup(slots, 8192)

    # The second node becomes a replica.
    Agent.update(topology, fn _ -> [{0, 16_383, first_address, [second_address]}] end)
    refresh(manager, cluster, true)

    # The second node becomes a primary again.
    Agent.update(topology, fn _ -> initial end)
    refresh(manager, cluster, true)

    # Remove the second node and leave the last slot unassigned.
    Agent.update(topology, fn _ -> [{0, 16_382, first_address}] end)
    refresh(manager, cluster, true)
    assert :ets.lookup(slots, 16_383) == []
    assert Manager.get_connection_by_node(registry, {"127.0.0.1", second.port}, self()) == :error
    assert {:ok, aliases} = Registry.meta(registry, :node_aliases)
    refute Map.has_key?(aliases, second_id)
  end

  defp node_members(members, id) do
    Enum.filter(members, fn {{node_id, _index}, _pid, _value} -> node_id == id end)
  end

  defp assert_one_lookup_per_family do
    assert_receive {:lookup, :inet}
    assert_receive {:lookup, :inet6}
    refute_receive {:lookup, _family}, 0
  end

  defp refresh(manager, cluster, changed?) do
    FakeNode.wait_until(fn -> elem(:sys.get_state(manager), 0) == :ready end)
    Manager.refresh_topology(manager)

    assert_receive {[:redix, :cluster, :topology_change], _, _,
                    %{cluster: ^cluster, changed: ^changed?}}
  end

  defp start_manager(node, inet_lookup_fun, opts \\ []) do
    cluster = :"dns_manager_#{System.unique_integer([:positive])}"
    registry = :"#{cluster}_registry"
    pool = :"#{cluster}_pool"
    manager = :"#{cluster}_manager"

    start_supervised!({Registry, keys: :unique, name: registry})
    start_supervised!({DynamicSupervisor, name: pool, strategy: :one_for_one})
    start_supervised!({Task.Supervisor, name: :"#{cluster}_task_supervisor"})

    start_supervised!(
      {Manager,
       Keyword.merge(
         [
           name: manager,
           cluster_name: cluster,
           seed_nodes: [{node.host, node.port}],
           pool_supervisor: pool,
           registry: registry,
           table_name: :"#{cluster}_slots",
           command_cache_table: :"#{cluster}_command_cache",
           conn_opts: Redix.StartOptions.sanitize(:redix, []),
           refresh_interval: 30_000,
           primary_pool_size: 5,
           replica_pool_size: 2,
           read_from_replicas: false,
           sync_connect: true,
           inet_lookup_fun: inet_lookup_fun
         ],
         opts
       )}
    )

    FakeNode.wait_until(fn ->
      Enum.all?(members(registry), fn {_key, _pid, {_role, state}} -> state == :connected end)
    end)

    %{cluster: cluster, manager: manager, registry: registry, slots: :"#{cluster}_slots"}
  end

  defp members(registry) do
    registry
    |> Registry.select([
      {{{:"$1", :"$2"}, :"$3", :"$4"}, [], [{{{{:"$1", :"$2"}}, :"$3", :"$4"}}]}
    ])
    |> Enum.sort()
  end
end
