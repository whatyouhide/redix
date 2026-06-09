defmodule Redix.Cluster.CommandParserTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Redix.Cluster.CommandParser

  describe "key_from_command/1" do
    test "basic key-value commands" do
      assert key_from_command(["GET", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["SET", "mykey", "value"]) == {:ok, "mykey"}
      assert key_from_command(["DEL", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["INCR", "counter"]) == {:ok, "counter"}
      assert key_from_command(["APPEND", "mykey", "hello"]) == {:ok, "mykey"}
    end

    test "case insensitive command names" do
      assert key_from_command(["get", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["Set", "mykey", "val"]) == {:ok, "mykey"}
    end

    test "hash commands" do
      assert key_from_command(["HGET", "myhash", "field"]) == {:ok, "myhash"}
      assert key_from_command(["HSET", "myhash", "f", "v"]) == {:ok, "myhash"}
      assert key_from_command(["HMGET", "myhash", "f1", "f2"]) == {:ok, "myhash"}
      assert key_from_command(["HDEL", "myhash", "f"]) == {:ok, "myhash"}
    end

    test "list commands" do
      assert key_from_command(["LPUSH", "mylist", "v"]) == {:ok, "mylist"}
      assert key_from_command(["RPOP", "mylist"]) == {:ok, "mylist"}
      assert key_from_command(["LRANGE", "mylist", "0", "-1"]) == {:ok, "mylist"}
    end

    test "set commands" do
      assert key_from_command(["SADD", "myset", "m"]) == {:ok, "myset"}
      assert key_from_command(["SCARD", "myset"]) == {:ok, "myset"}
      assert key_from_command(["SMEMBERS", "myset"]) == {:ok, "myset"}
    end

    test "sorted set commands" do
      assert key_from_command(["ZADD", "myzset", "1", "m"]) == {:ok, "myzset"}
      assert key_from_command(["ZSCORE", "myzset", "m"]) == {:ok, "myzset"}
      assert key_from_command(["ZRANGE", "myzset", "0", "-1"]) == {:ok, "myzset"}
    end

    test "stream commands" do
      assert key_from_command(["XADD", "mystream", "*", "f", "v"]) == {:ok, "mystream"}
      assert key_from_command(["XLEN", "mystream"]) == {:ok, "mystream"}
      assert key_from_command(["XRANGE", "mystream", "-", "+"]) == {:ok, "mystream"}
    end

    test "multi-key commands return first key" do
      assert key_from_command(["MGET", "k1", "k2", "k3"]) == {:ok, "k1"}
      assert key_from_command(["MSET", "k1", "v1", "k2", "v2"]) == {:ok, "k1"}
      assert key_from_command(["DEL", "k1", "k2"]) == {:ok, "k1"}
    end

    test "EVAL/EVALSHA with keys" do
      assert key_from_command(["EVAL", "return 1", "2", "k1", "k2", "arg"]) == {:ok, "k1"}

      assert key_from_command(["EVALSHA", "abc123", "1", "mykey"]) == {:ok, "mykey"}
    end

    test "EVAL/EVALSHA with zero keys" do
      assert key_from_command(["EVAL", "return 1", "0"]) == :no_key
      assert key_from_command(["EVALSHA", "abc123", "0"]) == :no_key
    end

    test "XREAD extracts first stream key" do
      assert key_from_command(["XREAD", "COUNT", "10", "STREAMS", "s1", "s2", "0", "0"]) ==
               {:ok, "s1"}

      assert key_from_command(["XREAD", "STREAMS", "mystream", "0"]) ==
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

      assert key_from_command(cmd) == {:ok, "s1"}
    end

    test "keyless commands" do
      assert key_from_command(["PING"]) == :no_key
      assert key_from_command(["INFO"]) == :no_key
      assert key_from_command(["DBSIZE"]) == :no_key
      assert key_from_command(["CLUSTER", "INFO"]) == :no_key
      assert key_from_command(["TIME"]) == :no_key
      assert key_from_command(["MULTI"]) == :no_key
      assert key_from_command(["EXEC"]) == :no_key
      assert key_from_command(["FLUSHALL"]) == :no_key
      assert key_from_command(["CONFIG", "GET", "maxmemory"]) == :no_key
      assert key_from_command(["SELECT", "0"]) == :no_key
      assert key_from_command(["AUTH", "password"]) == :no_key
    end

    test "empty command" do
      assert key_from_command([]) == :no_key
    end

    test "bit commands" do
      assert key_from_command(["SETBIT", "mykey", "7", "1"]) == {:ok, "mykey"}
      assert key_from_command(["GETBIT", "mykey", "7"]) == {:ok, "mykey"}
    end

    test "BITOP extracts the destination key (position 2)" do
      assert key_from_command(["BITOP", "AND", "dest", "src1", "src2"]) == {:ok, "dest"}
      assert key_from_command(["BITOP", "NOT", "dest", "src"]) == {:ok, "dest"}
    end

    test "BITOP without a destination key returns :no_key" do
      assert key_from_command(["BITOP", "AND"]) == :no_key
      assert key_from_command(["BITOP"]) == :no_key
    end

    test "blocking list/zset commands" do
      assert key_from_command(["BLPOP", "mylist", "0"]) == {:ok, "mylist"}
      assert key_from_command(["BRPOP", "mylist", "0"]) == {:ok, "mylist"}
      assert key_from_command(["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"]) == {:ok, "src"}
      assert key_from_command(["BRPOPLPUSH", "src", "dst", "0"]) == {:ok, "src"}
      assert key_from_command(["BZPOPMAX", "myzset", "0"]) == {:ok, "myzset"}
      assert key_from_command(["BZPOPMIN", "myzset", "0"]) == {:ok, "myzset"}
    end

    test "hash field expiration commands" do
      assert key_from_command(["HEXPIRE", "h", "100", "FIELDS", "1", "f"]) == {:ok, "h"}
      assert key_from_command(["HTTL", "h", "FIELDS", "1", "f"]) == {:ok, "h"}
      assert key_from_command(["HPERSIST", "h", "FIELDS", "1", "f"]) == {:ok, "h"}
      assert key_from_command(["HPEXPIRETIME", "h", "FIELDS", "1", "f"]) == {:ok, "h"}
    end

    test "XSETID" do
      assert key_from_command(["XSETID", "mystream", "1-1"]) == {:ok, "mystream"}
    end

    test "unknown commands return :unknown" do
      assert key_from_command(["SOMEFUTURECOMMAND", "arg"]) == :unknown
      # Movable-key commands are not in the static table (their first-key position can
      # vary), so the cluster resolves them via COMMAND GETKEYS at runtime.
      assert key_from_command(["BLMPOP", "0", "1", "mylist", "LEFT"]) == :unknown
      assert key_from_command(["MIGRATE", "host", "6379", "key", "0", "1000"]) == :unknown
    end

    test "String.Chars arguments are converted" do
      assert key_from_command(["GET", :mykey]) == {:ok, "mykey"}
      assert key_from_command(["SET", :mykey, 42]) == {:ok, "mykey"}
    end

    test "expiry-related commands" do
      assert key_from_command(["EXPIRE", "mykey", "100"]) == {:ok, "mykey"}
      assert key_from_command(["TTL", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["PERSIST", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["PEXPIRE", "mykey", "100"]) == {:ok, "mykey"}
    end

    test "HyperLogLog commands" do
      assert key_from_command(["PFADD", "hll", "a", "b"]) == {:ok, "hll"}
      assert key_from_command(["PFCOUNT", "hll"]) == {:ok, "hll"}
    end

    test "geo commands" do
      assert key_from_command(["GEOADD", "geo", "1", "2", "m"]) == {:ok, "geo"}
      assert key_from_command(["GEOPOS", "geo", "m"]) == {:ok, "geo"}
    end

    test "OBJECT command extracts the key after the subcommand" do
      assert key_from_command(["OBJECT", "ENCODING", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["OBJECT", "REFCOUNT", "mykey"]) == {:ok, "mykey"}
    end

    test "OBJECT command without a key returns :no_key" do
      assert key_from_command(["OBJECT", "HELP"]) == :no_key
      assert key_from_command(["OBJECT"]) == :no_key
    end

    test "numkeys-first commands extract the key at position 2" do
      assert key_from_command(["LMPOP", "2", "k1", "k2", "LEFT"]) == {:ok, "k1"}
      assert key_from_command(["ZMPOP", "2", "k1", "k2", "MIN"]) == {:ok, "k1"}
      assert key_from_command(["SINTERCARD", "2", "k1", "k2"]) == {:ok, "k1"}
      assert key_from_command(["ZINTERCARD", "2", "k1", "k2"]) == {:ok, "k1"}
      assert key_from_command(["ZDIFF", "2", "k1", "k2"]) == {:ok, "k1"}
      assert key_from_command(["ZINTER", "2", "k1", "k2"]) == {:ok, "k1"}
      assert key_from_command(["ZUNION", "2", "k1", "k2"]) == {:ok, "k1"}
    end

    test "numkeys-first commands are case-insensitive" do
      assert key_from_command(["lmpop", "1", "k1", "LEFT"]) == {:ok, "k1"}
      assert key_from_command(["ZuNiOn", "1", "k1"]) == {:ok, "k1"}
    end

    test "numkeys-first commands without a key return :no_key" do
      assert key_from_command(["LMPOP", "0"]) == :no_key
      assert key_from_command(["ZMPOP"]) == :no_key
      assert key_from_command(["SINTERCARD"]) == :no_key
    end

    test "STORE variants keep their destination key at position 1" do
      assert key_from_command(["ZUNIONSTORE", "dest", "2", "k1", "k2"]) == {:ok, "dest"}
      assert key_from_command(["ZINTERSTORE", "dest", "2", "k1", "k2"]) == {:ok, "dest"}
      assert key_from_command(["ZDIFFSTORE", "dest", "2", "k1", "k2"]) == {:ok, "dest"}
    end

    test "blocking numkeys-first variants fall through to :unknown" do
      assert key_from_command(["BLMPOP", "0", "1", "mylist", "LEFT"]) == :unknown
      assert key_from_command(["BZMPOP", "0", "1", "myzset", "MIN"]) == :unknown
    end

    test "XINFO extracts the key after the subcommand" do
      assert key_from_command(["XINFO", "STREAM", "mystream"]) == {:ok, "mystream"}
      assert key_from_command(["XINFO", "GROUPS", "mystream"]) == {:ok, "mystream"}
      assert key_from_command(["XINFO", "CONSUMERS", "mystream", "mygroup"]) == {:ok, "mystream"}
    end

    test "XINFO HELP returns :no_key" do
      assert key_from_command(["XINFO", "HELP"]) == :no_key
      assert key_from_command(["XINFO"]) == :no_key
    end

    test "XGROUP extracts the key after the subcommand" do
      assert key_from_command(["XGROUP", "CREATE", "mystream", "mygroup", "$"]) ==
               {:ok, "mystream"}

      assert key_from_command(["XGROUP", "DESTROY", "mystream", "mygroup"]) == {:ok, "mystream"}

      assert key_from_command(["XGROUP", "CREATECONSUMER", "mystream", "mygroup", "c"]) ==
               {:ok, "mystream"}
    end

    test "XGROUP HELP returns :no_key" do
      assert key_from_command(["XGROUP", "HELP"]) == :no_key
      assert key_from_command(["XGROUP"]) == :no_key
    end

    test "MEMORY USAGE extracts the key at position 2" do
      assert key_from_command(["MEMORY", "USAGE", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["MEMORY", "USAGE", "mykey", "SAMPLES", "5"]) == {:ok, "mykey"}
      assert key_from_command(["memory", "usage", "mykey"]) == {:ok, "mykey"}
    end

    test "other MEMORY subcommands are keyless" do
      assert key_from_command(["MEMORY", "DOCTOR"]) == :no_key
      assert key_from_command(["MEMORY", "STATS"]) == :no_key
      assert key_from_command(["MEMORY", "MALLOC-STATS"]) == :no_key
      assert key_from_command(["MEMORY", "PURGE"]) == :no_key
      assert key_from_command(["MEMORY", "HELP"]) == :no_key
      assert key_from_command(["MEMORY"]) == :no_key
    end

    test "DEBUG OBJECT extracts the key at position 2" do
      assert key_from_command(["DEBUG", "OBJECT", "mykey"]) == {:ok, "mykey"}
      assert key_from_command(["debug", "object", "mykey"]) == {:ok, "mykey"}
    end

    test "other DEBUG subcommands are keyless" do
      assert key_from_command(["DEBUG", "SLEEP", "0"]) == :no_key
      assert key_from_command(["DEBUG", "JMAP"]) == :no_key
      assert key_from_command(["DEBUG", "SET-ACTIVE-EXPIRE", "0"]) == :no_key
      assert key_from_command(["DEBUG"]) == :no_key
    end

    test "key-at-position-1 command with no arguments returns :no_key" do
      assert key_from_command(["GET"]) == :no_key
      assert key_from_command(["DEL"]) == :no_key
    end

    test "EVAL with non-zero numkeys but no keys returns :no_key" do
      assert key_from_command(["EVAL", "return 1", "2"]) == :no_key
    end

    test "EVAL with unparseable numkeys returns :no_key" do
      assert key_from_command(["EVAL", "return 1", "notanumber", "k1"]) == :no_key
    end

    test "EVAL with too few arguments returns :no_key" do
      assert key_from_command(["EVAL"]) == :no_key
      assert key_from_command(["EVAL", "return 1"]) == :no_key
    end

    test "XREAD without a STREAMS keyword returns :no_key" do
      assert key_from_command(["XREAD", "COUNT", "10"]) == :no_key
      assert key_from_command(["XREAD"]) == :no_key
    end

    test "XREAD with STREAMS but no keys returns :no_key" do
      assert key_from_command(["XREAD", "STREAMS"]) == :no_key
    end

    test "XREADGROUP without a STREAMS keyword returns :no_key" do
      assert key_from_command(["XREADGROUP", "GROUP", "g", "c", "COUNT", "10"]) == :no_key
    end

    test "XREADGROUP with too few arguments returns :no_key" do
      assert key_from_command(["XREADGROUP", "GROUP"]) == :no_key
      assert key_from_command(["XREADGROUP"]) == :no_key
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

        assert {:ok, ^key} = key_from_command([cmd, key, "extra"])
      end
    end

    property "keyless commands always return :no_key regardless of arguments" do
      check all command_name <- member_of(@keyless_commands),
                args <- list_of(string(:printable, min_length: 1, max_length: 20), max_length: 5) do
        assert :no_key = key_from_command([command_name | args])
      end
    end

    property "EVAL with numkeys=0 always returns :no_key" do
      check all script <- string(:printable, min_length: 1, max_length: 50),
                extra <-
                  list_of(string(:printable, min_length: 1, max_length: 10), max_length: 3),
                command_name <- member_of(["EVAL", "EVALSHA"]) do
        assert :no_key = key_from_command([command_name, script, "0" | extra])
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
                 key_from_command([command_name, script, to_string(numkeys)] ++ keys ++ args)
      end
    end

    @numkeys_pos_2_commands ~w(
      LMPOP ZMPOP SINTERCARD ZINTERCARD ZDIFF ZINTER ZUNION
    )

    property "numkeys-first commands extract the key at position 2 (never the numkeys count)" do
      check all command_name <- member_of(@numkeys_pos_2_commands),
                numkeys <- integer(1..5),
                keys <-
                  list_of(string(:alphanumeric, min_length: 1, max_length: 20), length: numkeys),
                casing <- member_of([:up, :down, :mixed]) do
        cmd =
          case casing do
            :up -> String.upcase(command_name)
            :down -> String.downcase(command_name)
            :mixed -> mix_case(command_name)
          end

        first_key = hd(keys)

        assert {:ok, ^first_key} =
                 key_from_command([cmd, to_string(numkeys)] ++ keys)
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
                 key_from_command(["XREAD" | prefix_opts] ++ ["STREAMS"] ++ stream_keys ++ ids)
      end
    end

    property "return value is always {:ok, binary}, :no_key, or :unknown" do
      check all command <-
                  list_of(string(:printable, min_length: 1, max_length: 30),
                    min_length: 0,
                    max_length: 8
                  ) do
        result = key_from_command(command)

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
