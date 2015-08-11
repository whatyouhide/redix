defmodule Recs do
  @type command :: [binary]

  @default_opts [
    host: "localhost",
    port: 6379,
    socket_opts: [],
  ]

  @redis_opts ~w(host port password database)a

  @spec start_link(Keyword.t | binary) :: GenServer.on_start
  def start_link(uri_or_opts \\ [])

  def start_link(uri) when is_binary(uri) do
    uri |> Recs.URI.opts_from_uri |> start_link
  end

  def start_link(opts) do
    {_redis_opts, connection_opts} = Keyword.split(opts, @redis_opts)
    opts = merge_with_default_opts(opts)
    Connection.start_link(Recs.Connection, opts, connection_opts)
  end

  @spec command(pid, command) :: Recs.Protocol.redis_value
  def command(conn, args) do
    Connection.call(conn, {:command, args})
  end

  @spec pipeline(pid, [command]) :: [Recs.Protocol.redis_value]
  def pipeline(conn, commands) do
    Connection.call(conn, {:pipeline, commands})
  end

  defp merge_with_default_opts(opts) do
    Keyword.merge @default_opts, opts, fn
      _k, default_val, nil ->
        default_val
      _k, _default_val, val ->
        val
    end
  end
end
