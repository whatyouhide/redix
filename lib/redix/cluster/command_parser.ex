defmodule Redix.Cluster.CommandParser do
  @moduledoc false

  # Static table mapping Redis command names to the (1-based) position of the first key argument.
  # This covers all common Redis commands.
  # For commands not in this table, the caller can use COMMAND GETKEYS as a fallback.

  # Commands with first key at position 1 (the most common case).
  @key_pos_1 MapSet.new(~w(
    APPEND BITCOUNT BITFIELD BITFIELD_RO BITPOS COPY DECR DECRBY DEL DUMP
    EXISTS EXPIRE EXPIREAT EXPIRETIME
    GEOADD GEODIST GEOHASH GEOPOS GEORADIUS GEORADIUSBYMEMBER GEORADIUSBYMEMBER_RO
    GEORADIUS_RO GEOSEARCH GEOSEARCHSTORE GET GETDEL GETEX GETRANGE GETSET
    HDEL HEXISTS HGET HGETALL HINCRBY HINCRBYFLOAT HKEYS HLEN HMGET HMSET
    HRANDFIELD HSCAN HSET HSETNX HSTRLEN HVALS
    INCR INCRBY INCRBYFLOAT
    LINDEX LINSERT LLEN LMOVE LMPOP LPOP LPOS LPUSH LPUSHX LRANGE LREM LSET LTRIM
    MGET MSET MSETNX MOVE
    PERSIST PEXPIRE PEXPIREAT PEXPIRETIME PSETEX PTTL PUBLISH
    RPOP RPOPLPUSH RPUSH RPUSHX
    SADD SCARD SDIFF SDIFFSTORE SET SETEX SETNX SETRANGE SINTER SINTERCARD
    SINTERSTORE SISMEMBER SMEMBERS SMISMEMBER SMOVE SORT SORT_RO SPOP SRANDMEMBER
    SREM SSCAN STRLEN SUBSTR SUNION SUNIONSTORE
    TOUCH TTL TYPE UNLINK
    WATCH
    XACK XADD XAUTOCLAIM XCLAIM XDEL XGROUP XINFO XLEN XPENDING XRANGE
    XREVRANGE XTRIM
    ZADD ZCARD ZCOUNT ZDIFF ZDIFFSTORE ZINCRBY ZINTER ZINTERCARD ZINTERSTORE
    ZLEXCOUNT ZMPOP ZMSCORE ZPOPMAX ZPOPMIN ZRANDMEMBER ZRANGE ZRANGEBYLEX
    ZRANGEBYSCORE ZRANGESTORE ZRANK ZREM ZREMRANGEBYLEX ZREMRANGEBYSCORE
    ZREMRANGEBYRANK ZREVRANGE ZREVRANGEBYLEX ZREVRANGEBYSCORE ZREVRANK ZSCAN
    ZSCORE ZUNION ZUNIONSTORE
    PFADD PFCOUNT PFMERGE
    RENAME RENAMENX
    RESTORE
    LCS
  ))

  # Commands that take no keys (keyless commands).
  @keyless MapSet.new(~w(
    AUTH BGSAVE BGREWRITEAOF CLIENT CLUSTER COMMAND CONFIG
    DBSIZE DEBUG DISCARD ECHO EXEC FLUSHALL FLUSHDB
    INFO LASTSAVE LATENCY MEMORY MONITOR MULTI
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

  def key_from_command(command) when is_list(command) do
    [name | args] = Enum.map(command, &to_string/1)
    upcased = String.upcase(name)

    cond do
      upcased in @keyless ->
        :no_key

      upcased in ["EVAL", "EVALSHA", "EVALSHA_RO", "EVAL_RO", "FCALL", "FCALL_RO"] ->
        key_from_eval(args)

      upcased == "XREAD" ->
        key_from_xread(args)

      upcased == "XREADGROUP" ->
        key_from_xreadgroup(args)

      upcased == "OBJECT" ->
        case args do
          [_subcommand, key | _] -> {:ok, key}
          _ -> :no_key
        end

      upcased in @key_pos_1 ->
        case args do
          [key | _] -> {:ok, key}
          [] -> :no_key
        end

      true ->
        :unknown
    end
  end

  # EVAL/EVALSHA: EVAL script numkeys key [key ...] arg [arg ...]
  defp key_from_eval([_script, numkeys_str | rest]) do
    case Integer.parse(numkeys_str) do
      {0, ""} -> :no_key
      {_n, ""} when rest != [] -> {:ok, hd(rest)}
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
    if String.upcase(arg) == "STREAMS" do
      rest
    else
      find_streams_keyword(rest)
    end
  end

  defp extract_first_stream_key([key | _]) when is_binary(key), do: {:ok, key}
  defp extract_first_stream_key(_), do: :no_key
end
