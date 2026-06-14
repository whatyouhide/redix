defmodule Redix.Cluster.RoleReconciliationTest do
  use ExUnit.Case, async: true

  import Redix.Cluster.FakeNode, only: [wait_until: 1]

  alias Redix.Cluster.FakeNode

  # Black-box test for node-role reconciliation after a failover (issue #318).
  # `CLUSTER SLOTS` can swap a node from primary to replica (and vice versa) when
  # Redis fails over, but a healthy Docker cluster won't do that on demand. We drive
  # a real Redix.Cluster (with replica reads on) against two scriptable fake RESP
  # nodes (Redix.Cluster.FakeNode), swap the topology, and observe that the demoted
  # node's connection is restarted as a replica — new pid, `:replica` Registry
  # value, and a fresh `READONLY`.

  test "restarts a connection whose role changed after a failover" do
    cluster = :"role_#{System.unique_integer([:positive])}"

    node_a = FakeNode.reserve()
    node_b = FakeNode.reserve()

    # Each node counts the READONLY commands it receives (one per replica
    # (re)connect), so the test can observe READONLY being (re)issued.
    {:ok, a_readonly} = Agent.start_link(fn -> 0 end)
    {:ok, b_readonly} = Agent.start_link(fn -> 0 end)

    # Initial topology: A is primary, B is its replica.
    {:ok, topology} =
      Agent.start_link(fn -> FakeNode.cluster_slots([{0, 16_383, node_a, [node_b]}]) end)

    FakeNode.serve(node_a, topology_handler(topology, a_readonly))
    FakeNode.serve(node_b, topology_handler(topology, b_readonly))

    start_supervised!(
      {Redix.Cluster,
       name: cluster, nodes: ["redis://#{node_a}"], read_from_replicas: true, sync_connect: true}
    )

    registry = :"#{cluster}_registry"
    a_id = node_a.id
    b_id = node_b.id

    # Node connections are async (sync_connect: false regardless of the cluster
    # option), so wait for both to register and for the replica to issue READONLY.
    wait_until(fn ->
      role(registry, a_id) == :primary and role(registry, b_id) == :replica and
        Agent.get(b_readonly, & &1) >= 1
    end)

    [{a_pid, _}] = Registry.lookup(registry, a_id)
    # A was the primary, so it never issued READONLY.
    assert Agent.get(a_readonly, & &1) == 0

    # Failover: B is promoted to primary and A demoted to its replica.
    Agent.update(topology, fn _ -> FakeNode.cluster_slots([{0, 16_383, node_b, [node_a]}]) end)
    Redix.Cluster.Manager.refresh_topology(:"#{cluster}_manager")

    # A's connection must be torn down and restarted as a replica: new pid, a
    # `:replica` Registry value, and a fresh READONLY. With the bug the stale
    # `:primary` connection is kept, so the role never flips and READONLY is never
    # sent — leaving `route: :replica` reads to bounce back MOVED forever.
    wait_until(fn ->
      match?([{new_pid, :replica}] when new_pid != a_pid, Registry.lookup(registry, a_id)) and
        Agent.get(a_readonly, & &1) >= 1
    end)

    # And B is now registered as a primary.
    assert role(registry, b_id) == :primary
  end

  # Builds a handler that answers CLUSTER SLOTS from the swappable `topology`
  # Agent and bumps `readonly` on every READONLY (one per replica (re)connect).
  defp topology_handler(topology, readonly) do
    fn
      ["CLUSTER", "SLOTS"] ->
        Agent.get(topology, & &1)

      ["READONLY"] ->
        Agent.update(readonly, &(&1 + 1))
        "+OK\r\n"

      ["PING"] ->
        "+PONG\r\n"

      _other ->
        "+OK\r\n"
    end
  end

  defp role(registry, node_id) do
    case Registry.lookup(registry, node_id) do
      [{_pid, role}] -> role
      [] -> nil
    end
  end
end
