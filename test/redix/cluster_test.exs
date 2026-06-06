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

    start_supervised!({Redix.Cluster, nodes: @nodes, name: cluster_name})

    # Flush test keys on each node (skip replicas which return READONLY)
    for port <- 7000..7005 do
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
  end

  describe "pipeline!/3" do
    test "returns results directly", %{cluster: cluster} do
      assert ["OK"] = Redix.Cluster.pipeline!(cluster, [["SET", "k", "v"]])
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
        {Redix.Cluster, nodes: ["redis://localhost:7001"], name: name},
        id: :single_seed
      )

      assert Redix.Cluster.command!(name, ["PING"]) == "PONG"
    end

    test "can connect using keyword list nodes" do
      name = :"kw_seed_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Redix.Cluster, nodes: [[host: "localhost", port: 7002]], name: name},
        id: :kw_seed
      )

      assert Redix.Cluster.command!(name, ["PING"]) == "PONG"
    end
  end

  describe "stop/2" do
    test "stops the cluster cleanly" do
      name = :"stop_test_#{System.unique_integer([:positive])}"
      {:ok, pid} = Redix.Cluster.start_link(nodes: @nodes, name: name)

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

  describe "MOVED to a node not yet in the registry (issue #293)" do
    test "Manager.connect_to_node connects to a reachable target on demand", %{cluster: cluster} do
      registry = :"#{cluster}_registry"
      manager = :"#{cluster}_manager"

      registered_ports = registered_ports(registry)

      # Replicas are reachable Redis nodes that aren't registered as primaries,
      # so they stand in for a node a MOVED redirect points at before the
      # topology refresh has discovered it.
      unknown_port = Enum.find(7000..7005, &(&1 not in registered_ports))
      assert unknown_port, "expected at least one unregistered (replica) cluster port"

      address = {"127.0.0.1", unknown_port}

      # Precondition: the node is not known yet.
      assert Redix.Cluster.Manager.get_connection_by_node(registry, address) == :error

      # On-demand connect succeeds and registers the node.
      assert {:ok, pid} = Redix.Cluster.Manager.connect_to_node(manager, address)
      assert is_pid(pid)
      assert Redix.Cluster.Manager.get_connection_by_node(registry, address) == {:ok, pid}

      # The connection is real and usable.
      assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}

      # Idempotent: a second call returns the same connection.
      assert Redix.Cluster.Manager.connect_to_node(manager, address) == {:ok, pid}
    end

    test "Manager.connect_to_node returns the existing pid for a known node", %{cluster: cluster} do
      registry = :"#{cluster}_registry"
      manager = :"#{cluster}_manager"

      [node_id | _] = Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
      [host, port_str] = String.split(node_id, ":")
      address = {host, String.to_integer(port_str)}

      assert {:ok, pid} = Redix.Cluster.Manager.get_connection_by_node(registry, address)
      assert Redix.Cluster.Manager.connect_to_node(manager, address) == {:ok, pid}
    end

    test "Manager.connect_to_node returns an error for an unreachable target", %{cluster: cluster} do
      manager = :"#{cluster}_manager"

      # sync_connect is false, so the Redix process still starts; PING is what
      # surfaces the connection failure.
      assert {:ok, pid} = Redix.Cluster.Manager.connect_to_node(manager, {"127.0.0.1", 9999})
      assert {:error, %Redix.ConnectionError{}} = Redix.command(pid, ["PING"])
    end
  end

  describe "node connection lifecycle" do
    test "the manager re-establishes a node connection when it dies", %{cluster: cluster} do
      registry = :"#{cluster}_registry"

      [node_id | _] = Registry.select(registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
      {:ok, pid} = Redix.Cluster.Manager.get_connection_by_node(registry, node_id_address(node_id))

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
        {Redix.Cluster, nodes: @nodes, name: name, read_from_replicas: true},
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

      # The Supervisor.start_link propagates the EXIT from the Manager's init failure.
      Process.flag(:trap_exit, true)
      result = Redix.Cluster.start_link(name: name, nodes: ["redis://localhost:9999"])
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
