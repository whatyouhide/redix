defmodule Redix.Cluster.CommandParserTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

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

  describe "key_from_command/1 properties" do
    @key_pos_1_commands ~w(
      GET SET DEL INCR APPEND HGET HSET HMGET HDEL LPUSH RPOP LRANGE
      SADD SCARD SMEMBERS ZADD ZSCORE ZRANGE XADD XLEN XRANGE
      MGET MSET EXPIRE TTL PERSIST PFADD PFCOUNT GEOADD GEOPOS
      DUMP EXISTS GETDEL TYPE UNLINK STRLEN SRANDMEMBER RPUSH LLEN
    )

    @keyless_commands ~w(
      PING INFO DBSIZE TIME MULTI EXEC FLUSHALL SELECT AUTH
      QUIT MONITOR BGSAVE DISCARD READONLY READWRITE RESET
    )

    property "key-at-position-1 commands are case-insensitive" do
      check all command_name <- member_of(@key_pos_1_commands),
                key <- string(:printable, min_length: 1, max_length: 50),
                casing <- member_of([:up, :down, :mixed]) do
        cmd =
          case casing do
            :up -> String.upcase(command_name)
            :down -> String.downcase(command_name)
            :mixed -> mix_case(command_name)
          end

        assert {:ok, ^key} = Command.key_from_command([cmd, key, "extra"])
      end
    end

    property "keyless commands always return :no_key regardless of arguments" do
      check all command_name <- member_of(@keyless_commands),
                args <- list_of(string(:printable, min_length: 1, max_length: 20), max_length: 5) do
        assert :no_key = Command.key_from_command([command_name | args])
      end
    end

    property "EVAL with numkeys=0 always returns :no_key" do
      check all script <- string(:printable, min_length: 1, max_length: 50),
                extra <-
                  list_of(string(:printable, min_length: 1, max_length: 10), max_length: 3),
                command_name <- member_of(["EVAL", "EVALSHA"]) do
        assert :no_key = Command.key_from_command([command_name, script, "0" | extra])
      end
    end

    property "EVAL with numkeys>=1 returns the first key" do
      check all script <- string(:printable, min_length: 1, max_length: 50),
                numkeys <- integer(1..5),
                keys <-
                  list_of(string(:alphanumeric, min_length: 1, max_length: 20), length: numkeys),
                args <- list_of(string(:printable, min_length: 1, max_length: 10), max_length: 3),
                command_name <- member_of(["EVAL", "EVALSHA"]) do
        first_key = hd(keys)

        assert {:ok, ^first_key} =
                 Command.key_from_command(
                   [command_name, script, to_string(numkeys)] ++ keys ++ args
                 )
      end
    end

    property "XREAD always extracts the first stream key after STREAMS keyword" do
      check all prefix_opts <- xread_prefix_gen(),
                stream_key <- string(:alphanumeric, min_length: 1, max_length: 20),
                extra_streams <-
                  list_of(string(:alphanumeric, min_length: 1, max_length: 20), max_length: 3) do
        stream_keys = [stream_key | extra_streams]
        ids = Enum.map(stream_keys, fn _ -> "0" end)

        assert {:ok, ^stream_key} =
                 Command.key_from_command(
                   ["XREAD" | prefix_opts] ++ ["STREAMS"] ++ stream_keys ++ ids
                 )
      end
    end

    property "return value is always {:ok, binary}, :no_key, or :unknown" do
      check all command <-
                  list_of(string(:printable, min_length: 1, max_length: 30),
                    min_length: 0,
                    max_length: 8
                  ) do
        result = Command.key_from_command(command)

        assert match?({:ok, key} when is_binary(key), result) or result == :no_key or
                 result == :unknown
      end
    end
  end

  defp mix_case(string) do
    string
    |> String.graphemes()
    |> Enum.with_index()
    |> Enum.map(fn {char, i} ->
      if rem(i, 2) == 0, do: String.downcase(char), else: String.upcase(char)
    end)
    |> Enum.join()
  end

  defp xread_prefix_gen do
    gen all opts <- list_of(member_of([["COUNT", "10"], ["BLOCK", "0"]]), max_length: 2) do
      List.flatten(opts)
    end
  end
end
