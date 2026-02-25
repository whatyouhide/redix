defmodule Redix.Cluster.CommandParserTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.CommandParser, as: Command

  describe "key_from_command/1" do
    test "basic key-value commands" do
      assert Command.key_from_command(["GET", "mykey"]) == {:ok, "mykey"}
      assert Command.key_from_command(["SET", "mykey", "value"]) == {:ok, "mykey"}
      assert Command.key_from_command(["DEL", "mykey"]) == {:ok, "mykey"}
      assert Command.key_from_command(["INCR", "counter"]) == {:ok, "counter"}
      assert Command.key_from_command(["APPEND", "mykey", "hello"]) == {:ok, "mykey"}
    end

    test "case insensitive command names" do
      assert Command.key_from_command(["get", "mykey"]) == {:ok, "mykey"}
      assert Command.key_from_command(["Set", "mykey", "val"]) == {:ok, "mykey"}
    end

    test "hash commands" do
      assert Command.key_from_command(["HGET", "myhash", "field"]) == {:ok, "myhash"}
      assert Command.key_from_command(["HSET", "myhash", "f", "v"]) == {:ok, "myhash"}
      assert Command.key_from_command(["HMGET", "myhash", "f1", "f2"]) == {:ok, "myhash"}
      assert Command.key_from_command(["HDEL", "myhash", "f"]) == {:ok, "myhash"}
    end

    test "list commands" do
      assert Command.key_from_command(["LPUSH", "mylist", "v"]) == {:ok, "mylist"}
      assert Command.key_from_command(["RPOP", "mylist"]) == {:ok, "mylist"}
      assert Command.key_from_command(["LRANGE", "mylist", "0", "-1"]) == {:ok, "mylist"}
    end

    test "set commands" do
      assert Command.key_from_command(["SADD", "myset", "m"]) == {:ok, "myset"}
      assert Command.key_from_command(["SCARD", "myset"]) == {:ok, "myset"}
      assert Command.key_from_command(["SMEMBERS", "myset"]) == {:ok, "myset"}
    end

    test "sorted set commands" do
      assert Command.key_from_command(["ZADD", "myzset", "1", "m"]) == {:ok, "myzset"}
      assert Command.key_from_command(["ZSCORE", "myzset", "m"]) == {:ok, "myzset"}
      assert Command.key_from_command(["ZRANGE", "myzset", "0", "-1"]) == {:ok, "myzset"}
    end

    test "stream commands" do
      assert Command.key_from_command(["XADD", "mystream", "*", "f", "v"]) == {:ok, "mystream"}
      assert Command.key_from_command(["XLEN", "mystream"]) == {:ok, "mystream"}
      assert Command.key_from_command(["XRANGE", "mystream", "-", "+"]) == {:ok, "mystream"}
    end

    test "multi-key commands return first key" do
      assert Command.key_from_command(["MGET", "k1", "k2", "k3"]) == {:ok, "k1"}
      assert Command.key_from_command(["MSET", "k1", "v1", "k2", "v2"]) == {:ok, "k1"}
      assert Command.key_from_command(["DEL", "k1", "k2"]) == {:ok, "k1"}
    end

    test "EVAL/EVALSHA with keys" do
      assert Command.key_from_command(["EVAL", "return 1", "2", "k1", "k2", "arg"]) == {:ok, "k1"}

      assert Command.key_from_command(["EVALSHA", "abc123", "1", "mykey"]) == {:ok, "mykey"}
    end

    test "EVAL/EVALSHA with zero keys" do
      assert Command.key_from_command(["EVAL", "return 1", "0"]) == :no_key
      assert Command.key_from_command(["EVALSHA", "abc123", "0"]) == :no_key
    end

    test "XREAD extracts first stream key" do
      assert Command.key_from_command(["XREAD", "COUNT", "10", "STREAMS", "s1", "s2", "0", "0"]) ==
               {:ok, "s1"}

      assert Command.key_from_command(["XREAD", "STREAMS", "mystream", "0"]) ==
               {:ok, "mystream"}
    end

    test "XREADGROUP extracts first stream key" do
      cmd = [
        "XREADGROUP",
        "GROUP",
        "mygroup",
        "consumer1",
        "COUNT",
        "10",
        "STREAMS",
        "s1",
        ">"
      ]

      assert Command.key_from_command(cmd) == {:ok, "s1"}
    end

    test "keyless commands" do
      assert Command.key_from_command(["PING"]) == :no_key
      assert Command.key_from_command(["INFO"]) == :no_key
      assert Command.key_from_command(["DBSIZE"]) == :no_key
      assert Command.key_from_command(["CLUSTER", "INFO"]) == :no_key
      assert Command.key_from_command(["TIME"]) == :no_key
      assert Command.key_from_command(["MULTI"]) == :no_key
      assert Command.key_from_command(["EXEC"]) == :no_key
      assert Command.key_from_command(["FLUSHALL"]) == :no_key
      assert Command.key_from_command(["CONFIG", "GET", "maxmemory"]) == :no_key
      assert Command.key_from_command(["SELECT", "0"]) == :no_key
      assert Command.key_from_command(["AUTH", "password"]) == :no_key
    end

    test "empty command" do
      assert Command.key_from_command([]) == :no_key
    end

    test "unknown commands return :unknown" do
      assert Command.key_from_command(["SOMEFUTURECOMMAND", "arg"]) == :unknown
    end

    test "String.Chars arguments are converted" do
      assert Command.key_from_command(["GET", :mykey]) == {:ok, "mykey"}
      assert Command.key_from_command(["SET", :mykey, 42]) == {:ok, "mykey"}
    end

    test "expiry-related commands" do
      assert Command.key_from_command(["EXPIRE", "mykey", "100"]) == {:ok, "mykey"}
      assert Command.key_from_command(["TTL", "mykey"]) == {:ok, "mykey"}
      assert Command.key_from_command(["PERSIST", "mykey"]) == {:ok, "mykey"}
      assert Command.key_from_command(["PEXPIRE", "mykey", "100"]) == {:ok, "mykey"}
    end

    test "HyperLogLog commands" do
      assert Command.key_from_command(["PFADD", "hll", "a", "b"]) == {:ok, "hll"}
      assert Command.key_from_command(["PFCOUNT", "hll"]) == {:ok, "hll"}
    end

    test "geo commands" do
      assert Command.key_from_command(["GEOADD", "geo", "1", "2", "m"]) == {:ok, "geo"}
      assert Command.key_from_command(["GEOPOS", "geo", "m"]) == {:ok, "geo"}
    end
  end
end
