defmodule Redix.Cluster.AsyncConnectTest do
  use ExUnit.Case, async: true

  import Redix.Cluster.FakeNode, only: [wait_until: 1]

  alias Redix.Cluster.FakeNode

  # Tests for the cluster's startup behavior (issue #323): by default the initial
  # topology fetch happens *after* start_link/1 returns and is retried with backoff
  # until a seed node answers, mirroring single-node Redix's lazy connect. With
  # sync_connect: true the fetch is synchronous and start_link/1 fails fast. We
  # drive a real Redix.Cluster against a scriptable fake RESP node
  # (Redix.Cluster.FakeNode) so we can control exactly when the "cluster" becomes
  # reachable, toggling it via FakeNode.set_status/2.

  test "async connect (the default): starts up even when no node is reachable, " <>
         "then discovers the topology in the background" do
    cluster = :"async_#{System.unique_integer([:positive])}"

    # The fake node starts "down": it accepts connections but closes them right
    # away, so the topology fetch fails at the socket level.
    node = FakeNode.reserve(status: :down)

    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, node}])
      ["PING"] -> "+PONG\r\n"
      _other -> "+OK\r\n"
    end)

    :telemetry_test.attach_event_handlers(self(), [
      [:redix, :cluster, :failed_topology_refresh],
      [:redix, :cluster, :topology_change]
    ])

    # start_link returns right away even though the node is unreachable. A short
    # backoff keeps the test fast.
    start_supervised!(
      {Redix.Cluster, name: cluster, nodes: ["redis://#{node}"], backoff_initial: 50}
    )

    # The node is down, so the initial discovery attempt fails: commands await at
    # most that first attempt and then fail fast with a connection error — both
    # keyed commands (empty slot table) and keyless ones (no connections).
    assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
             {:error, %Redix.ConnectionError{reason: :closed}}

    assert Redix.Cluster.command(cluster, ["PING"]) ==
             {:error, %Redix.ConnectionError{reason: :closed}}

    # The failed first attempt sets the marker, so commands no longer await the
    # Manager (they'd otherwise block for in-flight backoff retries too).
    assert :ets.member(:"#{cluster}_slots", :discovery_attempted)

    # The Manager keeps retrying in the background.
    assert_receive {[:redix, :cluster, :failed_topology_refresh], _ref, %{},
                    %{cluster: ^cluster}},
                   1_000

    # The node comes up: the next retry discovers the topology and the cluster
    # becomes usable with no intervention.
    FakeNode.set_status(node, :up)

    assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^cluster}},
                   2_000

    wait_until(fn -> Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"} end)

    assert :ets.lookup(:"#{cluster}_slots", 0) == [{0, node.id, []}]
  end

  test "commands issued while the initial topology fetch is in flight wait for it " <>
         "instead of failing" do
    cluster = :"await_#{System.unique_integer([:positive])}"

    node = FakeNode.reserve()

    # The node answers CLUSTER SLOTS slowly, so the first fetch is reliably still
    # in flight when the command below is issued. Each connection is served in its
    # own process, so the sleep doesn't block the node connection's PING.
    FakeNode.serve(node, fn
      ["CLUSTER", "SLOTS"] ->
        Process.sleep(300)
        FakeNode.cluster_slots([{0, 16_383, node}])

      ["PING"] ->
        "+PONG\r\n"

      _other ->
        "+OK\r\n"
    end)

    start_supervised!({Redix.Cluster, name: cluster, nodes: ["redis://#{node}"]})

    # Mirrors single-node Redix: the command awaits the in-flight fetch (and then
    # the node connection, which postpones commands while connecting) instead of
    # failing right away with :closed.
    assert Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"}
    assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "OK"}
  end

  @tag :capture_log
  test "sync_connect: true makes start_link/1 fail fast when no node is reachable" do
    Process.flag(:trap_exit, true)

    # Bind to an ephemeral port and close it again: connecting to it is then
    # refused immediately.
    {:ok, listen} = :gen_tcp.listen(0, [:binary, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    :ok = :gen_tcp.close(listen)

    cluster = :"sync_#{System.unique_integer([:positive])}"

    assert {:error, {:shutdown, {:failed_to_start_child, Redix.Cluster.Manager, reason}}} =
             Redix.Cluster.start_link(
               name: cluster,
               nodes: ["redis://127.0.0.1:#{port}"],
               sync_connect: true
             )

    assert reason == :no_reachable_node
  end

  test "nodes: [] is rejected at validation time" do
    assert_raise NimbleOptions.ValidationError, ~r/expected a non-empty list of nodes/, fn ->
      Redix.Cluster.start_link(name: :rejected_cluster, nodes: [])
    end
  end
end
