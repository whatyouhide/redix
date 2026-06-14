defmodule Redix.Cluster.FakeNodeTest do
  use ExUnit.Case, async: true

  import Redix.Cluster.FakeNode, only: [wait_until: 1]

  alias Redix.Cluster.FakeNode
  alias Redix.Cluster.Hash

  # Black-box tests for Redix.Cluster scenarios a healthy Docker cluster can't
  # reproduce on demand — mid-failover role swaps, partial slot coverage, null
  # hosts, ASK to a brand-new node, malformed/hostile redirects, connections dying
  # mid-command. We drive a real-ish cluster against scriptable fake RESP servers
  # (Redix.Cluster.FakeNode) instead of Docker, so these run `async: true` and
  # without the `:cluster` tag — the fast, always-on counterpart to cluster_test.exs.
  #
  # Two setup styles live here:
  #
  #   * Routing tests (`wire_cluster`) hand-wire the registry + slot table and
  #     pre-route slots, then exercise command routing — MOVED/ASK redirection
  #     (issues #295, #306, #319, #321, #325) and a node connection dying
  #     mid-command (#317).
  #
  #   * Topology tests boot a real Redix.Cluster per test and observe its lifecycle
  #     — slot-map refresh, role reconciliation, and startup/discovery (issues #314,
  #     #318, #323, #328).

  describe "MOVED/ASK redirection" do
    setup :wire_cluster

    test "follows an ASK -> ASK chain to the final node", %{cluster: cluster} do
      slot = Hash.hash_slot("x")

      # Terminal node: accepts ASKING, returns the value.
      node_c =
        FakeNode.start_connected(cluster, fn
          ["ASKING"] -> "+OK\r\n"
          ["GET", _] -> "$3\r\nbar\r\n"
        end)

      # Middle node: ASKs onward to C.
      node_b =
        FakeNode.start_connected(cluster, fn
          ["ASKING"] -> "+OK\r\n"
          ["GET", _] -> "-ASK #{slot} #{node_c}\r\n"
        end)

      # Slot owner: the initial command lands here (plainly) and ASKs to B.
      node_a =
        FakeNode.start_connected(cluster, fn ["GET", _] -> "-ASK #{slot} #{node_b}\r\n" end)

      route_slot(cluster, slot, node_a)

      assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "bar"}
    end

    test "follows an ASK -> MOVED redirection", %{cluster: cluster} do
      slot = Hash.hash_slot("x")

      # MOVED target: gets a plain command (no ASKING) and serves the value.
      node_c = FakeNode.start_connected(cluster, fn ["GET", _] -> "$5\r\nhello\r\n" end)

      node_b =
        FakeNode.start_connected(cluster, fn
          ["ASKING"] -> "+OK\r\n"
          ["GET", _] -> "-MOVED #{slot} #{node_c}\r\n"
        end)

      node_a =
        FakeNode.start_connected(cluster, fn ["GET", _] -> "-ASK #{slot} #{node_b}\r\n" end)

      route_slot(cluster, slot, node_a)

      assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "hello"}
    end

    test "bounds an endless ASK redirection loop", %{cluster: cluster} do
      slot = Hash.hash_slot("x")

      # Two nodes that bounce ASK back and forth forever. Both must handle ASKING
      # since each becomes the other's ASK target after the first hop. Reserve both
      # so each handler can reference the other's id.
      node_a = FakeNode.reserve() |> FakeNode.connect(cluster)
      node_b = FakeNode.reserve() |> FakeNode.connect(cluster)

      FakeNode.serve(node_a, fn
        ["ASKING"] -> "+OK\r\n"
        ["GET", _] -> "-ASK #{slot} #{node_b}\r\n"
      end)

      FakeNode.serve(node_b, fn
        ["ASKING"] -> "+OK\r\n"
        ["GET", _] -> "-ASK #{slot} #{node_a}\r\n"
      end)

      route_slot(cluster, slot, node_a)

      assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
               {:error, %Redix.ConnectionError{reason: :too_many_redirections}}
    end

    # Reproduces issue #306: the cluster code splits a "host:port" node id on every
    # colon and matches `[host, port_str]`, which blows up on IPv6 addresses. Redis
    # emits MOVED/ASK targets unbracketed (e.g. "MOVED 866 ::1:7000"), so following
    # such a redirect raises a MatchError in Redix.Cluster.parse_redirection/1 and
    # crashes the caller. Here the slot owner redirects to a real IPv6 node, which
    # the redirection machinery should follow to completion instead of crashing.
    @tag :ipv6
    test "follows a MOVED redirect to an IPv6 node", %{cluster: cluster} do
      slot = Hash.hash_slot("x")

      node_c =
        FakeNode.start_connected(cluster, fn ["GET", _] -> "$5\r\nhello\r\n" end, inet6: true)

      node_a =
        FakeNode.start_connected(cluster, fn ["GET", _] -> "-MOVED #{slot} #{node_c}\r\n" end)

      route_slot(cluster, slot, node_a)

      assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "hello"}
    end

    # Reproduces issue #319: the classic ASK scenario is a slot migrating to a
    # brand-new node. A node serving zero slots doesn't appear in CLUSTER SLOTS at
    # all, so the cluster has no connection to it and no topology refresh will
    # create one — the redirection code must connect on demand (as it already does
    # for MOVED) instead of failing every ASK-redirected command until the first
    # migration completes. On-demand connects go through the Manager, so unlike
    # the tests above this one boots a real Redix.Cluster against a fake node
    # that claims all slots (ignoring the hand-wired resources from `wire_cluster`).
    # The fake nodes here aren't pre-connected — the cluster discovers/connects them.
    test "follows an ASK to a brand-new node that isn't in the topology yet" do
      cluster = :"ask_new_node_#{System.unique_integer([:positive])}"
      slot = Hash.hash_slot("x")

      # The brand-new node receiving the migrating slot.
      node_new =
        FakeNode.start(fn
          ["ASKING"] -> "+OK\r\n"
          ["GET", _] -> "$3\r\nbar\r\n"
        end)

      # The slot owner: answers the topology fetch claiming every slot, then ASKs
      # the command onward to the new node. Reserve first so the CLUSTER SLOTS reply
      # can name its own id.
      node_owner = FakeNode.reserve()

      FakeNode.serve(node_owner, fn
        ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, node_owner}])
        ["GET", _] -> "-ASK #{slot} #{node_new}\r\n"
        _other -> "+OK\r\n"
      end)

      start_supervised!(
        {Redix.Cluster, name: cluster, nodes: ["redis://#{node_owner}"], sync_connect: true}
      )

      assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "bar"}
    end

    # Reproduces issue #325: redirect messages are server-controlled input, so a
    # malformed MOVED/ASK from a buggy or hostile server must come back to the
    # caller as a plain Redis error instead of crashing the calling process with
    # a parse error.
    test "returns malformed redirects to the caller as plain errors", %{cluster: cluster} do
      slot = Hash.hash_slot("x")

      malformed_redirects = [
        "MOVED garbage",
        "MOVED 12x 127.0.0.1:7000",
        "MOVED 99999 127.0.0.1:7000",
        "MOVED #{slot} 127.0.0.1",
        "MOVED #{slot} :7000",
        "MOVED #{slot} 127.0.0.1:no_port",
        "ASK garbage",
        "ASK #{slot} 127.0.0.1:no_port"
      ]

      for message <- malformed_redirects do
        node = FakeNode.start_connected(cluster, fn ["GET", _] -> "-#{message}\r\n" end)
        route_slot(cluster, slot, node)

        assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
                 {:error, %Redix.Error{message: message}}
      end
    end

    # Reproduces issue #321: a transaction queued on a stale primary (e.g. right
    # after a failover) is rejected with MOVED at queue time, aborting EXEC with
    # EXECABORT. Since a cluster transaction targets a single slot, the whole
    # MULTI/EXEC is re-run at the redirect target.
    test "follows a MOVED redirect for a transaction by re-running it at the target", %{
      cluster: cluster
    } do
      slot = Hash.hash_slot("x")

      node_b =
        FakeNode.start_connected(cluster, fn
          ["MULTI"] -> "+OK\r\n"
          ["SET", _, _] -> "+QUEUED\r\n"
          ["EXEC"] -> "*1\r\n+OK\r\n"
        end)

      node_a =
        FakeNode.start_connected(cluster, fn
          ["MULTI"] -> "+OK\r\n"
          ["SET", _, _] -> "-MOVED #{slot} #{node_b}\r\n"
          ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
        end)

      route_slot(cluster, slot, node_a)

      assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) == {:ok, ["OK"]}
    end

    test "follows an ASK redirect for a transaction with an ASKING-prefixed re-run", %{
      cluster: cluster
    } do
      slot = Hash.hash_slot("x")

      # The importing node accepts a single ASKING that flags the whole MULTI.
      node_b =
        FakeNode.start_connected(cluster, fn
          ["ASKING"] -> "+OK\r\n"
          ["MULTI"] -> "+OK\r\n"
          ["SET", _, _] -> "+QUEUED\r\n"
          ["EXEC"] -> "*1\r\n+OK\r\n"
        end)

      node_a =
        FakeNode.start_connected(cluster, fn
          ["MULTI"] -> "+OK\r\n"
          ["SET", _, _] -> "-ASK #{slot} #{node_b}\r\n"
          ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
        end)

      route_slot(cluster, slot, node_a)

      assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) == {:ok, ["OK"]}
    end

    test "bounds an endless MOVED redirection loop for a transaction", %{cluster: cluster} do
      slot = Hash.hash_slot("x")

      node_a = FakeNode.reserve() |> FakeNode.connect(cluster)
      node_b = FakeNode.reserve() |> FakeNode.connect(cluster)

      FakeNode.serve(node_a, fn
        ["MULTI"] -> "+OK\r\n"
        ["SET", _, _] -> "-MOVED #{slot} #{node_b}\r\n"
        ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
      end)

      FakeNode.serve(node_b, fn
        ["MULTI"] -> "+OK\r\n"
        ["SET", _, _] -> "-MOVED #{slot} #{node_a}\r\n"
        ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
      end)

      route_slot(cluster, slot, node_a)

      assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) ==
               {:error, %Redix.ConnectionError{reason: :too_many_redirections}}
    end
  end

  describe "dead node connections" do
    setup :wire_cluster

    # Regression tests for issue #317: a node connection that dies while a command is
    # being routed to it must surface a connection-error *value*, not exit the calling
    # process. In cluster mode the connection pid is internal and its death is routine
    # (`ensure_connections` terminates the connection of every node that leaves the
    # cluster), so a command racing a scale-in/reshard would otherwise crash the caller
    # via Redix.pipeline/3's `{:redix_exited_during_call, _}` exit.
    #
    # We reproduce the race deterministically with a stand-in "connection" process that
    # registers itself under a node id (so the routing code resolves it like any live
    # connection) and then exits the moment a pipeline is cast to it — exactly the
    # window the issue describes (lookup returns a live pid, it dies, the pipeline runs
    # against it).

    test "a single-node command whose connection dies returns a connection error, not a caller exit",
         %{cluster: cluster} do
      node_id = "127.0.0.1:65000"
      dying_connection(cluster, node_id)
      route_slot(cluster, Hash.hash_slot("x"), node_id)

      # Before the fix this exited the caller with {:redix_exited_during_call, :shutdown};
      # the test process reaching the assertion at all proves it survived.
      assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
               {:error, %Redix.ConnectionError{reason: :closed}}
    end

    test "a dying node in a multi-node pipeline fills its slots with an error and keeps the " <>
           "healthy node's results",
         %{cluster: cluster} do
      # One healthy node serving its slot...
      good = FakeNode.start_connected(cluster, fn ["GET", _] -> "$2\r\nhi\r\n" end)
      good_slot = Hash.hash_slot("good")
      route_slot(cluster, good_slot, good)

      # ...and one node whose connection dies the moment we pipeline to it.
      bad_id = "127.0.0.1:65001"
      dying_connection(cluster, bad_id)
      bad_slot = Hash.hash_slot("bad")
      route_slot(cluster, bad_slot, bad_id)

      # Sanity: the keys land in different slots, so the pipeline really splits across
      # two nodes and runs each group as a Task (exercising the async_nolink path).
      assert good_slot != bad_slot

      # The dead node's command is filled with a connection-error value at its original
      # index; the healthy node's result stays visible. The crashing group's Task no
      # longer takes the caller down with it.
      assert Redix.Cluster.pipeline(cluster, [["GET", "good"], ["GET", "bad"]]) ==
               {:ok, ["hi", %Redix.ConnectionError{reason: :closed}]}
    end

    test "a transaction whose connection dies returns a connection error, not a caller exit", %{
      cluster: cluster
    } do
      node_id = "127.0.0.1:65002"
      dying_connection(cluster, node_id)
      route_slot(cluster, Hash.hash_slot("x"), node_id)

      assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) ==
               {:error, %Redix.ConnectionError{reason: :closed}}
    end
  end

  describe "slot map refresh" do
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

  describe "role reconciliation" do
    # Black-box test for node-role reconciliation after a failover (issue #318).
    # `CLUSTER SLOTS` can swap a node from primary to replica (and vice versa) when
    # Redis fails over, but a healthy Docker cluster won't do that on demand. We drive
    # a real Redix.Cluster (with replica reads on) against two scriptable fake RESP
    # nodes, swap the topology, and observe that the demoted node's connection is
    # restarted as a replica — new pid, `:replica` Registry value, and a fresh
    # `READONLY`.
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
  end

  describe "async connect" do
    # Tests for the cluster's startup behavior (issue #323): by default the initial
    # topology fetch happens *after* start_link/1 returns and is retried with backoff
    # until a seed node answers, mirroring single-node Redix's lazy connect. With
    # sync_connect: true the fetch is synchronous and start_link/1 fails fast. We
    # control exactly when the "cluster" becomes reachable via FakeNode.set_status/2.
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

  ## Helpers

  # Hand-wires a cluster's internal resources — registry, task supervisor, and slot
  # table — without a Manager, so routing tests can place slots directly. The marker
  # mimics a cluster whose initial topology fetch completed, so commands don't try to
  # await the (nonexistent) Manager. Everything dies with the test process.
  defp wire_cluster(_context) do
    cluster = :"routing_#{System.unique_integer([:positive])}"

    start_supervised!({Registry, keys: :unique, name: :"#{cluster}_registry"})
    start_supervised!({Task.Supervisor, name: :"#{cluster}_task_supervisor"})

    :ets.new(:"#{cluster}_slots", [:named_table, :public, :set])
    :ets.insert(:"#{cluster}_slots", {:discovery_attempted, true})

    %{cluster: cluster}
  end

  defp route_slot(cluster, slot, node) do
    :ets.insert(:"#{cluster}_slots", {slot, "#{node}", _replicas = []})
  end

  # Spawns a process that registers itself under `node_id` in the cluster registry
  # (so the routing code resolves it as the slot's connection) and then exits as soon
  # as it receives the pipeline cast — standing in for a real Redix connection pid
  # terminated by the Manager between resolution and use.
  defp dying_connection(cluster, node_id) do
    test = self()

    pid =
      spawn(fn ->
        {:ok, _} = Registry.register(:"#{cluster}_registry", node_id, _value = :primary)
        send(test, {:registered, self()})

        receive do
          _cast -> exit(:shutdown)
        end
      end)

    assert_receive {:registered, ^pid}
    pid
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
