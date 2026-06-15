defmodule Redix.ClusterTest do
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
    cluster_name = :"cluster_#{System.unique_integer([:positive])}"

    start_supervised!({Redix.Cluster, nodes: @nodes, name: cluster_name, sync_connect: true})

    # Flush test keys on each node (skip replicas which return READONLY)
    for port <- 7000..7008 do
      flusher_id = :"flusher_#{cluster_name}_#{port}"

      conn =
        start_supervised!({Redix, host: "localhost", port: port, sync_connect: true},
          id: flusher_id
        )

      case Redix.command(conn, ["FLUSHALL"]) do
        {:ok, _} -> :ok
        {:error, %Redix.Error{message: "READONLY" <> _}} -> :ok
      end

      stop_supervised(flusher_id)
    end

    %{cluster: cluster_name}
  end

  describe "command/3" do
    test "SET and GET", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["SET", "mykey", "myvalue"]) == {:ok, "OK"}
      assert Redix.Cluster.command(cluster, ["GET", "mykey"]) == {:ok, "myvalue"}
    end

    test "routes to correct nodes based on key slot", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["SET", "foo", "1"]) == {:ok, "OK"}
      assert Redix.Cluster.command(cluster, ["SET", "bar", "2"]) == {:ok, "OK"}
      assert Redix.Cluster.command(cluster, ["SET", "hello", "3"]) == {:ok, "OK"}

      assert Redix.Cluster.command(cluster, ["GET", "foo"]) == {:ok, "1"}
      assert Redix.Cluster.command(cluster, ["GET", "bar"]) == {:ok, "2"}
      assert Redix.Cluster.command(cluster, ["GET", "hello"]) == {:ok, "3"}
    end

    test "INCR/DECR", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["SET", "counter", "10"]) == {:ok, "OK"}
      assert Redix.Cluster.command(cluster, ["INCR", "counter"]) == {:ok, 11}
      assert Redix.Cluster.command(cluster, ["DECR", "counter"]) == {:ok, 10}
    end

    test "hash commands", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["HSET", "myhash", "field1", "value1"]) == {:ok, 1}
      assert Redix.Cluster.command(cluster, ["HGET", "myhash", "field1"]) == {:ok, "value1"}
      assert Redix.Cluster.command(cluster, ["HGETALL", "myhash"]) == {:ok, ["field1", "value1"]}
    end

    test "list commands", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["RPUSH", "mylist", "a", "b", "c"]) == {:ok, 3}

      assert Redix.Cluster.command(cluster, ["LRANGE", "mylist", "0", "-1"]) ==
               {:ok, ["a", "b", "c"]}

      assert Redix.Cluster.command(cluster, ["LLEN", "mylist"]) == {:ok, 3}
    end

    test "set commands", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["SADD", "myset", "a", "b", "c"]) == {:ok, 3}
      assert Redix.Cluster.command(cluster, ["SCARD", "myset"]) == {:ok, 3}
      assert Redix.Cluster.command(cluster, ["SISMEMBER", "myset", "a"]) == {:ok, 1}
    end

    test "sorted set commands", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["ZADD", "myzset", "1", "a", "2", "b"]) == {:ok, 2}
      assert Redix.Cluster.command(cluster, ["ZSCORE", "myzset", "a"]) == {:ok, "1"}
      assert Redix.Cluster.command(cluster, ["ZRANGE", "myzset", "0", "-1"]) == {:ok, ["a", "b"]}
    end

    test "returns Redis errors", %{cluster: cluster} do
      Redix.Cluster.command!(cluster, ["SET", "strkey", "notanumber"])

      assert {:error, %Redix.Error{message: "ERR" <> _}} =
               Redix.Cluster.command(cluster, ["INCR", "strkey"])
    end

    test "keyless commands (PING)", %{cluster: cluster} do
      assert Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"}
    end

    test "commands unknown to the parser are routed to a random node", %{cluster: cluster} do
      # The command parser returns :unknown for this command, so it has no slot
      # and is routed to a random node, where Redis rejects it.
      assert {:error, %Redix.Error{message: "ERR" <> _}} =
               Redix.Cluster.command(cluster, ["SOMEFUTURECOMMAND", "arg"])
    end
  end

  describe "command!/3" do
    test "returns result directly on success", %{cluster: cluster} do
      assert Redix.Cluster.command!(cluster, ["SET", "k", "v"]) == "OK"
      assert Redix.Cluster.command!(cluster, ["GET", "k"]) == "v"
    end

    test "raises on error", %{cluster: cluster} do
      Redix.Cluster.command!(cluster, ["SET", "k", "notnum"])

      assert_raise Redix.Error, fn ->
        Redix.Cluster.command!(cluster, ["INCR", "k"])
      end
    end
  end

  describe "pipeline/3" do
    test "single-slot pipeline", %{cluster: cluster} do
      commands = [
        ["SET", "{tag}.k1", "v1"],
        ["SET", "{tag}.k2", "v2"],
        ["GET", "{tag}.k1"],
        ["GET", "{tag}.k2"]
      ]

      assert {:ok, ["OK", "OK", "v1", "v2"]} = Redix.Cluster.pipeline(cluster, commands)
    end

    test "multi-slot pipeline (transparent splitting)", %{cluster: cluster} do
      commands = [
        ["SET", "foo", "1"],
        ["SET", "bar", "2"],
        ["SET", "hello", "3"],
        ["GET", "foo"],
        ["GET", "bar"],
        ["GET", "hello"]
      ]

      assert {:ok, ["OK", "OK", "OK", "1", "2", "3"]} =
               Redix.Cluster.pipeline(cluster, commands)
    end

    test "results are in original order", %{cluster: cluster} do
      Redix.Cluster.command!(cluster, ["SET", "foo", "a"])
      Redix.Cluster.command!(cluster, ["SET", "bar", "b"])
      Redix.Cluster.command!(cluster, ["SET", "hello", "c"])

      commands = [["GET", "foo"], ["GET", "bar"], ["GET", "hello"]]

      assert {:ok, ["a", "b", "c"]} = Redix.Cluster.pipeline(cluster, commands)
    end

    test "raises on an empty command list, like Redix.pipeline/3 (issue #313)",
         %{cluster: cluster} do
      assert_raise ArgumentError, "no commands passed to the pipeline", fn ->
        Redix.Cluster.pipeline(cluster, [])
      end
    end

    # Issue #316: a partial failure (one node unresponsive) must not discard the
    # results of the nodes that succeeded. The failed node's positions are filled
    # with an error value at their original indices.
    test "partial node failure fills only the failed node's positions, keeps the rest",
         %{cluster: cluster} do
      slot_table = :"#{cluster}_slots"
      registry = :"#{cluster}_registry"

      # Enough distinct keys to be confident they span at least two nodes.
      keys = for i <- 1..16, do: "k#{i}"
      for k <- keys, do: Redix.Cluster.command!(cluster, ["SET", k, "v-#{k}"])

      conn_for = fn key ->
        slot = Redix.Cluster.Hash.hash_slot(key)
        {:ok, conn} = Redix.Cluster.Manager.get_connection(slot_table, registry, slot)
        conn
      end

      key_conns = Map.new(keys, fn k -> {k, conn_for.(k)} end)

      assert key_conns |> Map.values() |> Enum.uniq() |> length() >= 2,
             "test needs keys spanning at least two nodes"

      # Knock out one node by suspending its connection so its group times out while
      # the other nodes still respond.
      victim = key_conns |> Map.values() |> Enum.min()
      victim_keys = for {k, c} <- key_conns, c == victim, into: MapSet.new(), do: k

      commands = for k <- keys, do: ["GET", k]

      :sys.suspend(victim)

      try do
        assert {:ok, results} = Redix.Cluster.pipeline(cluster, commands, timeout: 200)

        for {k, result} <- Enum.zip(keys, results) do
          if MapSet.member?(victim_keys, k) do
            assert %Redix.ConnectionError{reason: :timeout} = result
          else
            assert result == "v-#{k}"
          end
        end
      after
        :sys.resume(victim)
      end
    end
  end

  describe "pipeline!/3" do
    test "returns results directly", %{cluster: cluster} do
      assert ["OK"] = Redix.Cluster.pipeline!(cluster, [["SET", "k", "v"]])
    end

    test "raises on an empty command list (issue #313)", %{cluster: cluster} do
      assert_raise ArgumentError, "no commands passed to the pipeline", fn ->
        Redix.Cluster.pipeline!(cluster, [])
      end
    end
  end

  describe "transaction_pipeline/3" do
    test "succeeds when all keys in same slot", %{cluster: cluster} do
      commands = [
        ["SET", "{user:1}.name", "Alice"],
        ["SET", "{user:1}.email", "alice@example.com"],
        ["GET", "{user:1}.name"]
      ]

      assert {:ok, ["OK", "OK", "Alice"]} =
               Redix.Cluster.transaction_pipeline(cluster, commands)
    end

    test "fails with CROSSSLOT when keys span multiple slots", %{cluster: cluster} do
      commands = [
        ["SET", "key_in_slot_a", "v1"],
        ["SET", "key_in_slot_b", "v2"]
      ]

      assert {:error, %Redix.Error{message: "CROSSSLOT" <> _}} =
               Redix.Cluster.transaction_pipeline(cluster, commands)
    end

    test "fails with a descriptive error when no command has a key", %{cluster: cluster} do
      commands = [["PING"], ["PING"]]

      assert {:error, %Redix.Error{message: message}} =
               Redix.Cluster.transaction_pipeline(cluster, commands)

      assert message =~ "requires at least one command with a key"
      refute message =~ "CROSSSLOT"
    end
  end

  describe "transaction_pipeline!/3" do
    test "returns results directly on success", %{cluster: cluster} do
      commands = [["SET", "{t}.a", "1"], ["SET", "{t}.b", "2"]]
      assert ["OK", "OK"] = Redix.Cluster.transaction_pipeline!(cluster, commands)
    end

    test "raises on CROSSSLOT error", %{cluster: cluster} do
      assert_raise Redix.Error, ~r/CROSSSLOT/, fn ->
        Redix.Cluster.transaction_pipeline!(cluster, [
          ["SET", "different_slot_a", "v1"],
          ["SET", "different_slot_b", "v2"]
        ])
      end
    end
  end

  # Commands outside CommandParser's static table (here BLMPOP, a movable-key command)
  # are routed by asking the server via COMMAND INFO / COMMAND GETKEYS, and the key
  # specification is cached per command name.
  describe "routing commands outside the static table (issue #307)" do
    test "command/3 routes an unknown command to the key's node", %{cluster: cluster} do
      assert Redix.Cluster.command!(cluster, ["RPUSH", "mylist", "a", "b", "c"]) == 3

      # BLMPOP (unknown, movable keys) must hit the same node as the list.
      assert Redix.Cluster.command(cluster, ["BLMPOP", "0.1", "1", "mylist", "LEFT"]) ==
               {:ok, ["mylist", ["a"]]}
    end

    test "pipeline/3 splits unknown commands across slots", %{cluster: cluster} do
      Redix.Cluster.command!(cluster, ["RPUSH", "foo", "1"])
      Redix.Cluster.command!(cluster, ["RPUSH", "bar", "2"])

      commands = [
        ["BLMPOP", "0.1", "1", "foo", "LEFT"],
        ["BLMPOP", "0.1", "1", "bar", "LEFT"]
      ]

      assert {:ok, [["foo", ["1"]], ["bar", ["2"]]]} = Redix.Cluster.pipeline(cluster, commands)
    end

    test "pipeline/3 mixes known and unknown commands", %{cluster: cluster} do
      commands = [
        ["RPUSH", "{tag}.k", "v"],
        ["BLMPOP", "0.1", "1", "{tag}.k", "LEFT"],
        ["EXISTS", "{tag}.k"]
      ]

      assert {:ok, [1, ["{tag}.k", ["v"]], 0]} = Redix.Cluster.pipeline(cluster, commands)
    end

    test "transaction_pipeline/3 of only unknown commands resolves the slot", %{cluster: cluster} do
      # Without resolving the key, these would all be :no_slot and the transaction would
      # fail with "requires at least one command with a key".
      commands = [
        ["BLMPOP", "0.01", "1", "{bt}.a", "LEFT"],
        ["BLMPOP", "0.01", "1", "{bt}.b", "LEFT"]
      ]

      assert {:ok, [nil, nil]} = Redix.Cluster.transaction_pipeline(cluster, commands)
    end

    test "transaction_pipeline/3 of unknown commands across slots fails with CROSSSLOT",
         %{cluster: cluster} do
      commands = [
        ["BLMPOP", "0.01", "1", "key_in_slot_a", "LEFT"],
        ["BLMPOP", "0.01", "1", "key_in_slot_b", "LEFT"]
      ]

      assert {:error, %Redix.Error{message: "CROSSSLOT" <> _}} =
               Redix.Cluster.transaction_pipeline(cluster, commands)
    end

    test "the command's key specification is cached after the first call", %{cluster: cluster} do
      cache = :"#{cluster}_command_cache"

      # Movable-key command: cached as :movable (still needs COMMAND GETKEYS per call).
      Redix.Cluster.command!(cluster, ["RPUSH", "cachelist", "v"])
      Redix.Cluster.command(cluster, ["BLMPOP", "0.1", "1", "cachelist", "LEFT"])
      assert :ets.lookup(cache, "BLMPOP") == [{"BLMPOP", :movable}]

      # Static first-key-position command: cached as a position and resolved locally
      # (no further network round-trip). PFDEBUG's first key is at position 2.
      Redix.Cluster.command(cluster, ["PFDEBUG", "GETREG", "somekey"])
      assert :ets.lookup(cache, "PFDEBUG") == [{"PFDEBUG", {:first_key, 2}}]
    end

    test "a genuinely unknown command falls back to random routing", %{cluster: cluster} do
      # COMMAND INFO returns nil for an unrecognized command, so we fall back to :no_slot
      # (random node) and surface Redis's own error rather than crashing.
      assert {:error, %Redix.Error{message: message}} =
               Redix.Cluster.command(cluster, ["NOTACOMMAND", "arg"])

      assert message =~ "unknown command"
    end
  end

  describe "hash tags" do
    test "keys with same hash tag go to same slot", %{cluster: cluster} do
      Redix.Cluster.command!(cluster, ["SET", "{user:1000}.name", "Alice"])
      Redix.Cluster.command!(cluster, ["SET", "{user:1000}.email", "alice@example.com"])
      Redix.Cluster.command!(cluster, ["SET", "{user:1000}.age", "30"])

      assert Redix.Cluster.command!(cluster, ["GET", "{user:1000}.name"]) == "Alice"

      assert Redix.Cluster.command!(cluster, ["GET", "{user:1000}.email"]) ==
               "alice@example.com"

      assert Redix.Cluster.command!(cluster, ["GET", "{user:1000}.age"]) == "30"
    end
  end

  describe "connection via different seed nodes" do
    test "can connect using a single seed node" do
      name = :"single_seed_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Redix.Cluster, nodes: ["redis://localhost:7001"], name: name, sync_connect: true},
        id: :single_seed
      )

      assert Redix.Cluster.command!(name, ["PING"]) == "PONG"
    end

    test "can connect using keyword list nodes" do
      name = :"kw_seed_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Redix.Cluster, nodes: [[host: "localhost", port: 7002]], name: name, sync_connect: true},
        id: :kw_seed
      )

      assert Redix.Cluster.command!(name, ["PING"]) == "PONG"
    end
  end

  describe "stop/2" do
    test "stops the cluster cleanly" do
      name = :"stop_test_#{System.unique_integer([:positive])}"
      {:ok, pid} = Redix.Cluster.start_link(nodes: @nodes, name: name, sync_connect: true)

      assert Redix.Cluster.command!(name, ["PING"]) == "PONG"
      assert :ok = Redix.Cluster.stop(name)
      refute Process.alive?(pid)
    end
  end

  describe "error handling" do
    test "name is required" do
      assert_raise NimbleOptions.ValidationError, ~r/required :name option not found/, fn ->
        Redix.Cluster.start_link(nodes: @nodes)
      end
    end

    test "database option must be 0 or nil" do
      assert_raise ArgumentError, ~r/database 0/, fn ->
        Redix.Cluster.start_link(name: :db_test, nodes: @nodes, database: 1)
      end
    end

    test "database 0 is allowed" do
      assert {:ok, _pid} =
               Redix.Cluster.start_link(name: :db_zero_test, nodes: @nodes, database: 0)

      Redix.Cluster.stop(:db_zero_test)
    end

    test "sentinel connections are not supported in cluster mode" do
      assert_raise ArgumentError, ~r/Sentinel connections are not supported/, fn ->
        Redix.Cluster.start_link(
          name: :sentinel_cluster_test,
          nodes: @nodes,
          sentinel: [sentinels: ["redis://localhost:9999"], group: "main"]
        )
      end
    end

    test ":exit_on_disconnection is not supported in cluster mode" do
      assert_raise ArgumentError, ~r/:exit_on_disconnection option is not supported/, fn ->
        Redix.Cluster.start_link(
          name: :exit_on_disconnection_cluster_test,
          nodes: @nodes,
          exit_on_disconnection: true
        )
      end
    end
  end

  describe "__parse_node__/1" do
    test "parses a URI string" do
      assert Redix.Cluster.__parse_node__("redis://example.com:7000") ==
               {:ok, {"example.com", 7000}}
    end

    test "parses a host/port keyword list" do
      assert Redix.Cluster.__parse_node__(host: "example.com", port: 7000) ==
               {:ok, {"example.com", 7000}}
    end

    test "returns an error for an invalid keyword list" do
      assert {:error, message} = Redix.Cluster.__parse_node__(port: "not a port")
      assert is_binary(message)
    end

    test "returns an error for an unexpected value" do
      assert {:error, message} = Redix.Cluster.__parse_node__(123)
      assert message =~ "expected a Redis URI or a :host/:port keyword list"
    end
  end

  # Regression tests for issue #304 (a multi-node pipeline where one node doesn't
  # respond within the timeout used to crash with a FunctionClauseError) and issue #316
  # (a single failing group used to discard the results of the groups that succeeded).
  # These exercise the collection logic directly, feeding it the exact shapes
  # `Task.yield_many/2` produces, zipped with each group's `[{idx, cmd}]` list (as
  # `execute_groups/3` does).
  describe "__collect_group_results__/1 (issues #304 and #316)" do
    test "fills a timed-out group's positions with a timeout error, keeping successes" do
      tasks = [
        Task.async(fn -> [{0, "OK"}] end),
        Task.async(fn -> Process.sleep(:infinity) end)
      ]

      results = Task.yield_many(tasks, timeout: 100, on_timeout: :kill_task)
      group_cmds = [[{0, ["GET", "a"]}], [{1, ["GET", "b"]}]]

      assert Redix.Cluster.__collect_group_results__(Enum.zip(results, group_cmds)) ==
               [[{0, "OK"}], [{1, %Redix.ConnectionError{reason: :timeout}}]]
    end

    test "returns the list of per-node result lists when every node responds" do
      tasks = [
        Task.async(fn -> [{0, "a"}] end),
        Task.async(fn -> [{1, "b"}] end)
      ]

      results = Task.yield_many(tasks, timeout: 1000, on_timeout: :kill_task)
      group_cmds = [[{0, ["GET", "a"]}], [{1, ["GET", "b"]}]]

      assert Redix.Cluster.__collect_group_results__(Enum.zip(results, group_cmds)) ==
               [[{0, "a"}], [{1, "b"}]]
    end

    test "fills a failing group's positions with the inline error value, keeping successes" do
      error = %Redix.ConnectionError{reason: :closed}

      tasks = [
        Task.async(fn -> [{0, "a"}] end),
        Task.async(fn -> {:error, error} end)
      ]

      results = Task.yield_many(tasks, timeout: 1000, on_timeout: :kill_task)
      group_cmds = [[{0, ["GET", "a"]}], [{1, ["GET", "b"]}, {2, ["GET", "c"]}]]

      assert Redix.Cluster.__collect_group_results__(Enum.zip(results, group_cmds)) ==
               [[{0, "a"}], [{1, error}, {2, error}]]
    end

    test "maps a crashed task's exit reason to a connection error value at its indices" do
      results = [
        {%Task{ref: make_ref(), owner: self(), pid: self(), mfa: {:erlang, :apply, 2}},
         {:ok, [{0, "a"}]}},
        {%Task{ref: make_ref(), owner: self(), pid: self(), mfa: {:erlang, :apply, 2}},
         {:exit, :boom}}
      ]

      group_cmds = [[{0, ["GET", "a"]}], [{1, ["GET", "b"]}]]

      assert Redix.Cluster.__collect_group_results__(Enum.zip(results, group_cmds)) ==
               [[{0, "a"}], [{1, %Redix.ConnectionError{reason: :boom}}]]
    end
  end

  describe "MOVED to a node not yet in the registry (issue #293)" do
    test "Manager.connect_to_node connects to a reachable target on demand", %{cluster: cluster} do
      registry = :"#{cluster}_registry"
      manager = :"#{cluster}_manager"

      registered_ports = registered_ports(registry)

      # Replicas are reachable Redis nodes that aren't registered as primaries,
      # so they stand in for a node a MOVED redirect points at before the
      # topology refresh has discovered it.
      unknown_port = Enum.find(7000..7008, &(&1 not in registered_ports))
      assert unknown_port, "expected at least one unregistered (replica) cluster port"

      address = {"127.0.0.1", unknown_port}

      # Precondition: the node is not known yet.
      assert Redix.Cluster.Manager.get_connection_by_node(registry, address) == :error

      # On-demand connect succeeds and registers the node.
      assert {:ok, pid} = Redix.Cluster.Manager.connect_to_node(manager, address, 5_000)
      assert is_pid(pid)
      assert Redix.Cluster.Manager.get_connection_by_node(registry, address) == {:ok, pid}

      # The connection is real and usable.
      assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}

      # Idempotent: a second call returns the same connection.
      assert Redix.Cluster.Manager.connect_to_node(manager, address, 5_000) == {:ok, pid}
    end

    test "Manager.connect_to_node returns the existing pid for a known node", %{cluster: cluster} do
      registry = :"#{cluster}_registry"
      manager = :"#{cluster}_manager"

      [node_id | _] = Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
      [host, port_str] = String.split(node_id, ":")
      address = {host, String.to_integer(port_str)}

      assert {:ok, pid} = Redix.Cluster.Manager.get_connection_by_node(registry, address)
      assert Redix.Cluster.Manager.connect_to_node(manager, address, 5_000) == {:ok, pid}
    end

    test "Manager.connect_to_node returns an error for an unreachable target", %{cluster: cluster} do
      manager = :"#{cluster}_manager"

      # sync_connect is false, so the Redix process still starts; PING is what
      # surfaces the connection failure.
      assert {:ok, pid} =
               Redix.Cluster.Manager.connect_to_node(manager, {"127.0.0.1", 9999}, 5_000)

      assert {:error, %Redix.ConnectionError{}} = Redix.command(pid, ["PING"])
    end

    test "Manager.connect_to_node degrades to an error when the manager is busy (issue #327)" do
      # A manager stuck in a slow serial topology refresh would otherwise block the
      # on-demand connect (and thus the MOVED/ASK redirect) for the whole refresh.
      # A busy manager is stood in for by a process that never replies to the call;
      # the finite timeout must fire and the caught exit must degrade to an error.
      busy_manager = spawn(fn -> Process.sleep(:infinity) end)

      assert {:error, _reason} =
               Redix.Cluster.Manager.connect_to_node(busy_manager, {"127.0.0.1", 7000}, 50)
    end
  end

  describe "node connection lifecycle" do
    test "the manager re-establishes a node connection when it dies", %{cluster: cluster} do
      registry = :"#{cluster}_registry"

      [node_id | _] = Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])

      {:ok, pid} =
        Redix.Cluster.Manager.get_connection_by_node(registry, node_id_address(node_id))

      monitor = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, 1000

      # The manager (and supervisor) bring the node back, registered under the same node id.
      assert wait_until(fn ->
               match?(
                 {:ok, new_pid} when new_pid != pid,
                 Redix.Cluster.Manager.get_connection_by_node(registry, node_id_address(node_id))
               )
             end)

      # And the cluster is usable again.
      assert Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"}
    end
  end

  describe ":route option (no replicas connected)" do
    test "route: :replica returns a connection error when no replica is connected", %{
      cluster: cluster
    } do
      Redix.Cluster.command!(cluster, ["SET", "noreplica", "v"])

      assert {:error, %Redix.ConnectionError{}} =
               Redix.Cluster.command(cluster, ["GET", "noreplica"], route: :replica)
    end

    test "route: :prefer_replica falls back to the primary when no replica is connected", %{
      cluster: cluster
    } do
      Redix.Cluster.command!(cluster, ["SET", "fallback", "v"])

      assert Redix.Cluster.command(cluster, ["GET", "fallback"], route: :prefer_replica) ==
               {:ok, "v"}
    end

    test "an invalid :route raises ArgumentError", %{cluster: cluster} do
      assert_raise ArgumentError, ~r/invalid :route option/, fn ->
        Redix.Cluster.command(cluster, ["GET", "k"], route: :bogus)
      end
    end

    test "transaction_pipeline rejects non-primary routes", %{cluster: cluster} do
      assert_raise ArgumentError, ~r/only supports route: :primary/, fn ->
        Redix.Cluster.transaction_pipeline(cluster, [["SET", "{t}.a", "1"]], route: :replica)
      end
    end
  end

  describe "reading from replicas (read_from_replicas: true)" do
    setup do
      name = :"replica_cluster_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Redix.Cluster, nodes: @nodes, name: name, read_from_replicas: true, sync_connect: true},
        id: name
      )

      %{replica_cluster: name}
    end

    test "connects to replicas and registers them with the :replica role", %{
      replica_cluster: cluster
    } do
      registry = :"#{cluster}_registry"

      # Replica connections are established asynchronously after the cluster starts.
      assert wait_until(fn -> replica_pids(registry) != [] end)
      assert Enum.all?(replica_pids(registry), &Process.alive?/1)
    end

    test "route: :replica reads a value written to the primary", %{replica_cluster: cluster} do
      assert Redix.Cluster.command!(cluster, ["SET", "replica_key", "v1"]) == "OK"

      # Allow for replica connection setup and replication lag.
      assert wait_until(fn ->
               Redix.Cluster.command(cluster, ["GET", "replica_key"], route: :replica) ==
                 {:ok, "v1"}
             end)
    end

    test "route: :prefer_replica reads a value", %{replica_cluster: cluster} do
      assert Redix.Cluster.command!(cluster, ["SET", "prefer_key", "v2"]) == "OK"

      assert wait_until(fn ->
               Redix.Cluster.command(cluster, ["GET", "prefer_key"], route: :prefer_replica) ==
                 {:ok, "v2"}
             end)
    end

    test "route: :replica applies to a whole single-slot pipeline", %{replica_cluster: cluster} do
      Redix.Cluster.command!(cluster, ["SET", "{r}.a", "1"])
      Redix.Cluster.command!(cluster, ["SET", "{r}.b", "2"])

      assert wait_until(fn ->
               Redix.Cluster.pipeline(cluster, [["GET", "{r}.a"], ["GET", "{r}.b"]],
                 route: :replica
               ) == {:ok, ["1", "2"]}
             end)
    end

    test "route: :replica batches same-slot reads onto a single replica (issue #315)", %{
      replica_cluster: cluster
    } do
      # `CLUSTER MYID`, run inside a keyed EVAL, routes by the key's hash slot and
      # returns the id of whichever node actually served it. So a pipeline of
      # same-slot reads reveals, black-box, how many distinct replicas served it:
      # the fix groups them onto one connection (one pipeline), so every id is the
      # same. Without it, get_replica_connection/3 picks a random replica per
      # command and the reads scatter across the slot's two replicas.
      whoami = ["EVAL", "return redis.call('CLUSTER','MYID')", "1", "{315}"]

      # Wait for the replica connections to come up (they're established async).
      assert wait_until(fn ->
               match?({:ok, _}, Redix.Cluster.command(cluster, whoami, route: :replica))
             end)

      # The split is random, so repeat: a missing fix scatters reads almost every
      # run (P(20 reads pick the same replica) = (1/2)^19).
      per_call_ids =
        for _ <- 1..30 do
          {:ok, results} =
            Redix.Cluster.pipeline(cluster, List.duplicate(whoami, 20), route: :replica)

          Enum.uniq(results)
        end

      # Each individual pipeline was served by exactly one replica.
      assert Enum.all?(per_call_ids, &match?([_one], &1))

      # Sanity check on the topology itself: across calls the per-pipeline random
      # pick exercised *both* of the slot's replicas. If only one replica were
      # reachable this test couldn't tell the fix from its absence, so assert we
      # really saw two distinct replicas.
      replica_ids = per_call_ids |> List.flatten() |> Enum.uniq()
      assert length(replica_ids) == 2

      # And the reads went to replicas, never the slot's primary.
      assert {:ok, primary_id} = Redix.Cluster.command(cluster, whoami, route: :primary)
      refute primary_id in replica_ids
    end

    test "topology_change includes replica addresses", %{replica_cluster: cluster} do
      {test_name, _arity} = __ENV__.function
      parent = self()
      ref = make_ref()

      handler = fn _event, _measurements, meta, _config ->
        if meta.cluster == cluster, do: send(parent, {ref, meta})
      end

      :telemetry.attach("#{test_name}", [:redix, :cluster, :topology_change], handler, :no_config)

      Redix.Cluster.Manager.refresh_topology(:"#{cluster}_manager")

      assert_receive {^ref, meta}, 5_000

      # The Docker cluster has 3 primaries + 6 replicas, so with replica reads
      # enabled every node (not just the primaries) shows up in the event.
      assert length(meta.nodes) == 9
      assert Enum.all?(meta.nodes, &String.contains?(&1, ":"))

      :telemetry.detach("#{test_name}")
    end
  end

  describe "telemetry" do
    test "topology_change is emitted on startup", %{cluster: cluster} do
      {test_name, _arity} = __ENV__.function
      parent = self()
      ref = make_ref()

      handler = fn _event, _measurements, meta, _config ->
        if meta.cluster == cluster, do: send(parent, {ref, meta})
      end

      :telemetry.attach(
        "#{test_name}",
        [:redix, :cluster, :topology_change],
        handler,
        :no_config
      )

      # Trigger a topology refresh
      Redix.Cluster.Manager.refresh_topology(:"#{cluster}_manager")

      assert_receive {^ref, meta}, 5_000
      assert meta.cluster == cluster
      assert is_list(meta.nodes)
      assert length(meta.nodes) > 0
      assert Enum.all?(meta.nodes, &String.contains?(&1, ":"))

      :telemetry.detach("#{test_name}")
    end

    test "failed_topology_refresh is emitted when no nodes are reachable" do
      {test_name, _arity} = __ENV__.function
      parent = self()
      ref = make_ref()

      handler = fn _event, _measurements, meta, _config ->
        send(parent, {ref, meta})
      end

      :telemetry.attach(
        "#{test_name}",
        [:redix, :cluster, :failed_topology_refresh],
        handler,
        :no_config
      )

      name = :"failed_topo_#{System.unique_integer([:positive])}"

      # With sync_connect: true the Manager fails its init when no node is reachable;
      # the Supervisor.start_link propagates the EXIT from that failure.
      Process.flag(:trap_exit, true)

      result =
        Redix.Cluster.start_link(
          name: name,
          nodes: ["redis://localhost:9999"],
          sync_connect: true
        )

      assert {:error, _} = result

      assert_receive {^ref, meta}, 5_000
      assert meta.cluster == name
      assert meta.reason == :no_reachable_node

      :telemetry.detach("#{test_name}")
    end

    @tag :capture_log
    test "default handler logs cluster events" do
      import ExUnit.CaptureLog

      Redix.Telemetry.attach_default_handler()

      name = :"log_test_#{System.unique_integer([:positive])}"

      Process.flag(:trap_exit, true)

      log =
        capture_log(fn ->
          Redix.Cluster.start_link(name: name, nodes: ["redis://localhost:9999"])
          Process.sleep(100)
        end)

      assert log =~ ~r/Cluster.*failed to refresh topology/
    end
  end

  defp node_id_address(node_id) do
    [host, port_str] = String.split(node_id, ":")
    {host, String.to_integer(port_str)}
  end

  defp registered_ports(registry) do
    registry
    |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}])
    |> Enum.map(fn node_id ->
      node_id |> String.split(":") |> List.last() |> String.to_integer()
    end)
    |> MapSet.new()
  end

  defp replica_pids(registry) do
    Registry.select(registry, [{{:_, :"$1", :replica}, [], [:"$1"]}])
  end

  defp wait_until(fun, attempts \\ 50) do
    cond do
      fun.() -> true
      attempts <= 0 -> false
      true -> Process.sleep(50) && wait_until(fun, attempts - 1)
    end
  end
end
