defmodule Redix.Cluster.SlotMapRefreshTest do
  use ExUnit.Case, async: true

  import Redix.Cluster.FakeNode, only: [wait_until: 1]

  alias Redix.Cluster.FakeNode

  # Black-box tests for CLUSTER SLOTS handling that can't be reproduced against a
  # healthy Docker cluster (issues #314 and #328). We drive a real Redix.Cluster
  # against a scriptable fake RESP node (Redix.Cluster.FakeNode) that answers
  # CLUSTER SLOTS, then observe the cluster's routing state (the slot table) after
  # a refresh. No Docker cluster needed.

  test "drops slot-table entries for slots that become unassigned on refresh" do
    cluster = :"slotmap_#{System.unique_integer([:positive])}"

    node = FakeNode.reserve()

    # Holds the CLUSTER SLOTS reply the fake node serves, swapped between refreshes.
    {:ok, topology} =
      Agent.start_link(fn -> FakeNode.cluster_slots([{0, 16_383, node}]) end)

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] -> Agent.get(topology, & &1)
      _other -> "+OK\r\n"
    end)

    start_supervised!(
      {Redix.Cluster, name: cluster, nodes: ["redis://#{node}"], sync_connect: true}
    )

    slot_table = :"#{cluster}_slots"
    node_id = node.id

    # With sync_connect: true the init refresh is synchronous, so full coverage is
    # already in the table.
    assert :ets.lookup(slot_table, 0) == [{0, node_id, []}]
    assert :ets.lookup(slot_table, 9_000) == [{9_000, node_id, []}]
    assert :ets.lookup(slot_table, 16_383) == [{16_383, node_id, []}]

    # The node now reports a smaller range: 8192..16383 become unassigned.
    Agent.update(topology, fn _ -> FakeNode.cluster_slots([{0, 8_191, node}]) end)
    Redix.Cluster.Manager.refresh_topology(:"#{cluster}_manager")

    # The reactive refresh is async and stale slots are deleted one at a time,
    # so wait for both probed slots to be gone (waiting on just one races with
    # the deletion loop). With the bug, slot 9000 keeps pointing at a node that
    # no longer owns it.
    wait_until(fn ->
      :ets.lookup(slot_table, 9_000) == [] and :ets.lookup(slot_table, 16_383) == []
    end)

    # Slots still covered are untouched.
    assert :ets.lookup(slot_table, 0) == [{0, node_id, []}]
    assert :ets.lookup(slot_table, 8_191) == [{8_191, node_id, []}]
  end

  # A healthy cluster always covers all 16384 slots, so the "slot becomes
  # unassigned" case above can only be reproduced with a controllable node.
  # Likewise, a null host requires "cluster-preferred-endpoint-type
  # unknown-endpoint" (Redis 7+, typical of managed/NAT'd deployments), which the
  # Docker cluster doesn't use.
  test "substitutes the answering node's address for null hosts in CLUSTER SLOTS" do
    cluster = :"nullhost_#{System.unique_integer([:positive])}"

    node = FakeNode.reserve()

    FakeNode.serve(node, fn
      # A null host means "use the address you connected to" (issue #328). With
      # the bug, this produced the node id ":#{port}" and a connection attempt to
      # host "".
      ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, {nil, node.port}}])
      ["PING"] -> "+PONG\r\n"
      _other -> "+OK\r\n"
    end)

    start_supervised!(
      {Redix.Cluster, name: cluster, nodes: ["redis://#{node}"], sync_connect: true}
    )

    # The slot table records the address the topology query was answered from.
    assert :ets.lookup(:"#{cluster}_slots", 0) == [{0, node.id, []}]
    assert :ets.lookup(:"#{cluster}_slots", 16_383) == [{16_383, node.id, []}]

    # And commands route to a working connection registered under that address.
    assert Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"}
  end
end
