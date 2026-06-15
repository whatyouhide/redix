defmodule Redix.Cluster.CommandParser do
  @moduledoc false

  # Static table mapping Redis command names to the (1-based) position of the first key argument.
  # This covers all common Redis commands.
  # For commands not in this table, `Redix.Cluster` falls back to COMMAND GETKEYS / COMMAND INFO.

  # Commands with first key at position 1 (the most common case).
  @key_pos_1 MapSet.new(~w(
    APPEND BITCOUNT BITFIELD BITFIELD_RO BITPOS COPY DECR DECRBY DEL DUMP
    EXISTS EXPIRE EXPIREAT EXPIRETIME
    GEOADD GEODIST GEOHASH GEOPOS GEORADIUS GEORADIUSBYMEMBER GEORADIUSBYMEMBER_RO
    GEORADIUS_RO GEOSEARCH GEOSEARCHSTORE GET GETBIT GETDEL GETEX GETRANGE GETSET
    HDEL HEXISTS HEXPIRE HEXPIREAT HEXPIRETIME HGET HGETALL HINCRBY HINCRBYFLOAT
    HKEYS HLEN HMGET HMSET HPERSIST HPEXPIRE HPEXPIREAT HPEXPIRETIME HPTTL HTTL
    HRANDFIELD HSCAN HSET HSETNX HSTRLEN HVALS
    INCR INCRBY INCRBYFLOAT
    BLMOVE BLPOP BRPOP BRPOPLPUSH
    LINDEX LINSERT LLEN LMOVE LPOP LPOS LPUSH LPUSHX LRANGE LREM LSET LTRIM
    MGET MSET MSETNX MOVE
    PERSIST PEXPIRE PEXPIREAT PEXPIRETIME PSETEX PTTL PUBLISH
    RPOP RPOPLPUSH RPUSH RPUSHX
    SADD SCARD SDIFF SDIFFSTORE SET SETBIT SETEX SETNX SETRANGE SINTER
    SINTERSTORE SISMEMBER SMEMBERS SMISMEMBER SMOVE SORT SORT_RO SPOP SRANDMEMBER
    SREM SSCAN STRLEN SUBSTR SUNION SUNIONSTORE
    TOUCH TTL TYPE UNLINK
    WATCH
    XACK XADD XAUTOCLAIM XCLAIM XDEL XLEN XPENDING XRANGE
    XREVRANGE XSETID XTRIM
    BZPOPMAX BZPOPMIN
    ZADD ZCARD ZCOUNT ZDIFFSTORE ZINCRBY ZINTERSTORE
    ZLEXCOUNT ZMSCORE ZPOPMAX ZPOPMIN ZRANDMEMBER ZRANGE ZRANGEBYLEX
    ZRANGEBYSCORE ZRANGESTORE ZRANK ZREM ZREMRANGEBYLEX ZREMRANGEBYSCORE
    ZREMRANGEBYRANK ZREVRANGE ZREVRANGEBYLEX ZREVRANGEBYSCORE ZREVRANK ZSCAN
    ZSCORE ZUNIONSTORE
    PFADD PFCOUNT PFMERGE
    RENAME RENAMENX
    RESTORE
    LCS
  ))

  # Commands whose first key is at position 2 because position 1 is a `numkeys` count.
  # Syntax: `CMD numkeys key [key ...] [...]`. The blocking variants (BLMPOP/BZMPOP) and
  # the STORE variants (ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE) are deliberately NOT here:
  # the former have a movable key (resolved via COMMAND GETKEYS), the latter have their
  # destination key at position 1.
  @numkeys_pos_2 MapSet.new(~w(
    LMPOP ZMPOP SINTERCARD ZINTERCARD ZDIFF ZINTER ZUNION
  ))

  # Commands that take no keys (keyless commands).
  @keyless MapSet.new(~w(
    AUTH BGSAVE BGREWRITEAOF CLIENT CLUSTER COMMAND CONFIG
    DBSIZE DISCARD ECHO EXEC FLUSHALL FLUSHDB
    INFO LASTSAVE LATENCY MONITOR MULTI
    PING PSUBSCRIBE PUNSUBSCRIBE QUIT RANDOMKEY READONLY READWRITE
    REPLICAOF RESET ROLE SAVE SCAN SELECT SHUTDOWN SLAVEOF
    SLOWLOG SUBSCRIBE SWAPDB TIME UNSUBSCRIBE UNWATCH WAIT WAITAOF
  ))

  @doc """
  Extracts the first key from a Redis command.

  Returns `{:ok, key}` if a key is found, `:no_key` if the command takes no keys,
  or `:unknown` if the command is not in the static table and a fallback is needed.
  """
  @spec key_from_command(Redix.command()) :: {:ok, String.t()} | :no_key | :unknown
  def key_from_command(command)

  def key_from_command([]) do
    :no_key
  end

  # Only the command name (always) and the specific argument we route on are converted
  # to a string. Mapping `to_string/1` over the *whole* command would walk and reallocate
  # every argument — wasteful on the hot path for many-argument commands (a large MSET,
  # MGET, or DEL), which is exactly the common @key_pos_1 case.
  def key_from_command([name | args]) do
    upcased = name |> to_string() |> String.upcase()

    cond do
      upcased in @keyless ->
        :no_key

      upcased in ["EVAL", "EVALSHA", "EVALSHA_RO", "EVAL_RO", "FCALL", "FCALL_RO"] ->
        key_from_eval(args)

      upcased == "XREAD" ->
        key_from_xread(args)

      upcased == "XREADGROUP" ->
        key_from_xreadgroup(args)

      # OBJECT <subcommand> key, XINFO <subcommand> key, XGROUP <subcommand> key — the
      # key sits at position 2, after a subcommand. Keyless subcommands (OBJECT HELP,
      # XINFO HELP, XGROUP HELP) have no following argument, so the two-element pattern
      # naturally falls through to :no_key.
      upcased in ["OBJECT", "XINFO", "XGROUP"] ->
        case args do
          [_subcommand, key | _] -> {:ok, to_string(key)}
          _ -> :no_key
        end

      # MEMORY USAGE key — the only MEMORY subcommand that takes a key. Everything else
      # (DOCTOR, STATS, MALLOC-STATS, PURGE, HELP) is keyless.
      upcased == "MEMORY" ->
        case args do
          [subcommand, key | _] ->
            if String.upcase(to_string(subcommand)) == "USAGE",
              do: {:ok, to_string(key)},
              else: :no_key

          _ ->
            :no_key
        end

      # DEBUG OBJECT key — the only DEBUG subcommand that takes a key. Everything else
      # (SLEEP, JMAP, SET-ACTIVE-EXPIRE, ...) is keyless.
      upcased == "DEBUG" ->
        case args do
          [subcommand, key | _] ->
            if String.upcase(to_string(subcommand)) == "OBJECT",
              do: {:ok, to_string(key)},
              else: :no_key

          _ ->
            :no_key
        end

      # BITOP operation destkey srckey [srckey ...] — the first key is the destination,
      # at position 2 (right after the operation).
      upcased == "BITOP" ->
        case args do
          [_operation, destkey | _] -> {:ok, to_string(destkey)}
          _ -> :no_key
        end

      # numkeys-first commands: CMD numkeys key [key ...]. First key is at position 2.
      upcased in @numkeys_pos_2 ->
        case args do
          [_numkeys, key | _] -> {:ok, to_string(key)}
          _ -> :no_key
        end

      upcased in @key_pos_1 ->
        case args do
          [key | _] -> {:ok, to_string(key)}
          [] -> :no_key
        end

      true ->
        :unknown
    end
  end

  # EVAL/EVALSHA: EVAL script numkeys key [key ...] arg [arg ...]
  defp key_from_eval([_script, numkeys | rest]) do
    case Integer.parse(to_string(numkeys)) do
      {0, ""} -> :no_key
      {_n, ""} when rest != [] -> {:ok, to_string(hd(rest))}
      _other -> :no_key
    end
  end

  defp key_from_eval(_), do: :no_key

  # XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
  defp key_from_xread(args) do
    case find_streams_keyword(args) do
      nil -> :no_key
      rest -> extract_first_stream_key(rest)
    end
  end

  # XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [...] id [...]
  defp key_from_xreadgroup([_group_kw, _group, _consumer | rest]) do
    case find_streams_keyword(rest) do
      nil -> :no_key
      keys_and_ids -> extract_first_stream_key(keys_and_ids)
    end
  end

  defp key_from_xreadgroup(_), do: :no_key

  defp find_streams_keyword([]), do: nil

  defp find_streams_keyword([arg | rest]) do
    if String.upcase(to_string(arg)) == "STREAMS" do
      rest
    else
      find_streams_keyword(rest)
    end
  end

  defp extract_first_stream_key([key | _]), do: {:ok, to_string(key)}
  defp extract_first_stream_key([]), do: :no_key
end
