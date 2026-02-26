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
    cluster_name = :"cluster_#{:erlang.unique_integer([:positive])}"

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
      name = :"single_seed_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {Redix.Cluster, nodes: ["redis://localhost:7001"], name: name},
        id: :single_seed
      )

      assert Redix.Cluster.command!(name, ["PING"]) == "PONG"
    end

    test "can connect using keyword list nodes" do
      name = :"kw_seed_#{:erlang.unique_integer([:positive])}"

      start_supervised!(
        {Redix.Cluster, nodes: [[host: "localhost", port: 7002]], name: name},
        id: :kw_seed
      )

      assert Redix.Cluster.command!(name, ["PING"]) == "PONG"
    end
  end

  describe "stop/2" do
    test "stops the cluster cleanly" do
      name = :"stop_test_#{:erlang.unique_integer([:positive])}"
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
  end
end
