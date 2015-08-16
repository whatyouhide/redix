defmodule Redix do
  @moduledoc """
  This module provides the main API to interface with Redis.

  ## Overview

  `start_link/1` and `start_link/2` start a process that connects to Redis. Each
  Elixir process started with these functions maps to a client connection to the
  specified Redis server.

  The architecture is very simple: when you issue commands to Redis (via
  `command/3` or `pipeline/3`), the Redix process sends the command to Redis right
  away and is immediately able to send new commands. When a response arrives
  from Redis, only then the Redix process replies to the caller with the
  response. This pattern avoids blocking the Redix process for each request (until
  a response arrives), increasing the performance of this driver.

  ## Reconnections

  Redix tries to be as resilient as possible: it tries to recover automatically
  from most network errors. For example, if the connection to Redis drops, then
  the Redix process will try to periodically reconnect at a given interval. This
  interval can be specified with the `:backoff` option passed to `start_link/1`
  or `start_link/2` (by default it's `2000` milliseconds).

  These reconnections attempts only happen when the connection to Redis has been
  established at least once before. If a connection error happens when
  connecting to Redis for the first time, the Redix process will just crash with
  the proper error (see `start_link/1`, `start_link/2`).

  All this behaviour is implemented using the
  [connection](https://github.com/fishcakez/connection) library (a dependency of
  Redix).
  """

  @type command :: [binary]

  @default_opts [
    host: 'localhost',
    port: 6379,
    socket_opts: [],
    backoff: 2000,
  ]

  @redis_opts ~w(host port password database)a

  @default_timeout 5000

  @doc """
  Starts a connection to Redis.

  This function returns `{:ok, pid}` if the connection is successful. The actual
  TCP connection to the Redis server happens asynchronously: when `start_link/1`
  is called, a pid is returned right away and the connection process starts. All
  the calls to Redis that happen during the connection have to wait for the
  connection to be established before being issued.

  This function accepts a single argument: `uri_or_opts`. As the name suggests,
  this argument can either be a Redis URI or a list of options.

  ## URI

  In case `uri_or_opts` is a Redis URI, it must be in the form:

      redis://[:password@]host[:port][/db]

  Here are some examples of valid URIs:

      redis://localhost
      redis://:secret@localhost:6397
      redis://example.com:6380/1

  Usernames before the password are ignored, so the these two URIs are
  equivalent:

      redis://:secret@localhost
      redis://myuser:secret@localhost

  The only mandatory thing when using is the host. All other elements (password,
  port, database) are optional and their default value can be found in the
  "Options" section below.

  ## Options

  The following options can be used to specify the connection parameters
  (instead of a URI as described above):

    * `:host` - (string) the host where the Redis server is running. Defaults to
      `"localhost"`.
    * `:port` - (integer) the port on which the Redis server is
      running. Defaults to `6379`.
    * `:password` - (string) the password used to connect to Redis. Defaults to
      `nil`, meaning no password is used. When this option is provided, all Redix
      does is issue an `AUTH` command to Redis in order to authenticate.
    * `:database` - (integer or string) the database to connect to. Defaults to
      `nil`, meaning don't connect to any database (Redis connects to database
      `0` by default). When this option is provided, all Redix does is issue a
      `SELECT` command to Redis in order to select the given database.

  The following options can be specified in the list of options or as a second
  argument when using a URI (using `start_link/2` instead of `start_link/1`):

    * `:socket_opts` - (list of options) this option specifies a list of options
      that are passed to `:gen_tcp.connect/4` when connecting to the Redis
      server. Some socket options (like `:active` or `:binary`) will be
      overridden by Redix so that it functions properly.
    * `:backoff` - the time (in milliseconds) to wait before trying to reconnect
      when a network error occurs.

  In addition to these options, all options accepted by `GenServer.start_link/3`
  are forwarded to it. For example, a Redix connection can be registered with a
  name:

      Redix.start_link(name: :redix)
      Process.whereis(:redix)
      #=> #PID<...>

  ## Examples

      iex> Redix.start_link
      {:ok, #PID<...>}

      iex> Redix.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Redix.start_link(database: 3, name: :redix_3)
      {:ok, #PID<...>}

  """
  @spec start_link(Keyword.t | binary) :: GenServer.on_start
  def start_link(uri_or_opts \\ [])

  def start_link(uri) when is_binary(uri) do
    uri |> Redix.URI.opts_from_uri |> start_link
  end

  def start_link(opts) do
    {_redis_opts, connection_opts} = Keyword.split(opts, @redis_opts)
    opts = merge_with_default_opts(opts)
    Connection.start_link(Redix.Connection, opts, connection_opts)
  end

  @doc """
  Works like `start_link/1` but accepts a Redis URI *and* a list of options.

  The options you can pass are the ones not about the connection (e.g.,
  `:socket_opts` or `:name`). Read the docs for `start_link/1` for more
  information.

  ## Examples

      iex> Redix.start_link("redis://foo.com/2", name: :redix_foo)
      {:ok, #PID<...>}

  """
  @spec start_link(binary, Keyword.t) :: GenServer.on_start
  def start_link(uri, opts) when is_binary(uri) and is_list(opts) do
    uri |> Redix.URI.opts_from_uri |> Keyword.merge(opts) |> start_link
  end

  @doc """
  Closes the connection to the Redis server.

  This function is asynchronous: it returns `:ok` as soon as it's called and
  performs the closing of the connection after that.

  ## Examples

      iex> Redix.stop(conn)
      :ok

  """
  @spec stop(GenServer.server) :: :ok
  def stop(conn) do
    Connection.cast(conn, :stop)
  end

  @doc """
  Issues a command on the Redis server.

  This function sends `command` to the Redis server and returns the response
  returned by Redis. `pid` must be the pid of a Redix connection. `command` must
  be a list of strings making up the Redis command and its arguments.

  The return value is `{:ok, response}` if the request is successful and the
  response is not a Redis error. `{:error, reason}` is returned in case there's
  an error in sending the response or in case the response is a Redis error. In
  the latter case, `reason` will be the error returned by Redis.

  ## Examples

      iex> Redix.command(conn, ["SET", "mykey", "foo"])
      {:ok, "OK"}
      iex> Redix.command(conn, ["GET", "mykey"])
      {:ok, "foo"}

      iex> Redix.command(conn, ["INCR", "mykey"])
      {:error, "ERR value is not an integer or out of range"}

  If Redis goes down (before a reconnection happens):

      iex> Redix.command(conn, ["GET", "mykey"])
      {:error, :closed}

  """
  @spec command(GenServer.server, command, Keyword.t) ::
    {:ok, Redix.Protocol.redis_value} |
    {:error, atom | Redix.Error.t}
  def command(conn, args, opts \\ []) do
    Connection.call(conn, {:command, args}, opts[:timeout] || @default_timeout)
  end

  @doc """
  Issues a pipeline of commands on the Redis server.

  `commands` must be a list of commands, where each command is a list of strings
  making up the command and its arguments. The commands will be sent as a single
  "block" to Redis, and a list of ordered responses (one for each command) will
  be returned.

  The return value is `{:ok, response}` if the request is successful and the response has

  ## Examples

      iex> Redix.command(conn, [~w(INCR mykey), ~w(INCR mykey), ~w(DECR mykey)])
      {:ok, [1, 2, 1]}

  """
  @spec pipeline(GenServer.server, [command], Keyword.t) ::
    {:ok, [Redix.Protocol.redis_value]} |
    {:error, atom}
  def pipeline(conn, commands, opts \\ []) do
    Connection.call(conn, {:pipeline, commands}, opts[:timeout] || @default_timeout)
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
