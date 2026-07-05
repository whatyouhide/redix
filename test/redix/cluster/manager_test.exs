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

    start_supervised!({Redix.Cluster, nodes: @nodes, name: cluster_name, sync_connect: true})

    %{
      cluster: cluster_name,
      manager: :"#{cluster_name}_manager",
      registry: :"#{cluster_name}_registry"
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
    test "connections are started for all master nodes", %{registry: registry} do
      registered =
        Registry.select(registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      # A 3-master cluster should have at least 3 connections.
      assert length(registered) >= 3

      for {node_id, pid} <- registered do
        assert is_binary(node_id)
        assert String.contains?(node_id, ":")
        assert Process.alive?(pid)
      end
    end

    test "dead connection is restarted with a new PID", %{registry: registry} do
      [{node_id, pid} | _] =
        Registry.select(registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

      Process.exit(pid, :kill)

      wait_until_passes(2_000, fn ->
        assert [{new_pid, _}] = Registry.lookup(registry, node_id)
        assert new_pid != pid
        assert Process.alive?(new_pid)
      end)
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
      assert map_size(monitors_before) >= 3

      # Pick a node that we know is in the monitors map.
      {old_ref, {node_id, _role}} = Enum.at(monitors_before, 0)
      [{pid, _}] = Registry.lookup(registry, node_id)

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

      # Pick a monitored node and kill its connection.
      {_old_ref, {node_id, _role}} = Enum.at(monitors_before, 0)
      [{pid, _}] = Registry.lookup(registry, node_id)

      Process.exit(pid, :kill)

      # Wait for the new connection to appear in the registry. The DOWN-driven
      # restart backs off (issue #334), so the replacement isn't instantaneous —
      # `assert` (not a bare match) so `wait_until_passes` retries until it lands.
      wait_until_passes(2_000, fn ->
        assert [{new_pid, _}] = Registry.lookup(registry, node_id)
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
      assert [{^fake_pid, _}] = Registry.lookup(registry, fake_id)

      # The Manager should be monitoring it.
      {_state, data} = :sys.get_state(manager)
      assert Enum.any?(data.monitors, fn {_ref, {id, _role}} -> id == fake_id end)

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
      assert Registry.lookup(registry, fake_id) == []

      {_state, data} = :sys.get_state(manager)
      refute Enum.any?(data.monitors, fn {_ref, {id, _role}} -> id == fake_id end)
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
