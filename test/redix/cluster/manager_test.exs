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

    start_supervised!({Redix.Cluster, nodes: @nodes, name: cluster_name})

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
        {Redix.Cluster, nodes: @nodes, name: short_name, topology_refresh_interval: 500},
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
        {Redix.Cluster, nodes: @nodes, name: short_name, topology_refresh_interval: 500},
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

      # Wait for the new connection to appear in the registry.
      wait_until_passes(2_000, fn ->
        [{new_pid, _}] = Registry.lookup(registry, node_id)
        assert new_pid != pid
        assert Process.alive?(new_pid)
      end)
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
