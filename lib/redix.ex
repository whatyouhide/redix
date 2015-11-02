defmodule Redix do
  @moduledoc """
  This module provides the main API to interface with Redis.

  ## Overview

  `start_link/2` starts a process that connects to Redis. Each Elixir process
  started with this function maps to a client TCP connection to the specified
  Redis server.

  The architecture is very simple: when you issue commands to Redis (via
  `command/3` or `pipeline/3`), the Redix process sends the command to Redis right
  away and is immediately able to send new commands. When a response arrives
  from Redis, only then the Redix process replies to the caller with the
  response. This pattern avoids blocking the Redix process for each request (until
  a response arrives), increasing the performance of this driver.

  This pattern is different when using PubSub since no commands can be sent to
  Redis once PubSub is active. Have a look at the "PubSub" section below to know
  more about PubSub support in Redix.

  ## Reconnections

  Redix tries to be as resilient as possible: it tries to recover automatically
  from most network errors.

  If there's a network error sending data to Redis or if the connection to Redis
  drops, this happens:

    * a reconnection attempt is made right away.
    * if this attempt fails, reconnections are attempted at a given "backoff"
      interval. The duration of this interval can be specified with the
      `:backoff` option passed to `start_link/2`. The default is `2000`
      milliseconds.

  This behaviour can be tweaked with the `:max_reconnection_attempts` option,
  which controls the max number of reconnection attempts that will be made. For
  example, to never attempt reconnecting this option can be set to `0`. By
  default it's `nil`, meaning there's no limit on the number of reconnection
  attempts that will be made.

  These reconnections attempts only happen when the connection to Redis has been
  established at least once before. If a connection error happens when
  connecting to Redis for the first time, the Redix process will just crash with
  the proper error (see `start_link/2`).

  All this behaviour is implemented using the
  [connection](https://github.com/fishcakez/connection) library (a dependency of
  Redix).

  ## PubSub

  Redix provides an interface for the [Redis PubSub
  functionality](http://redis.io/topics/pubsub). You can read more about it in
  the documentation for the `Redix.PubSub` module.

  """

  @type command :: [binary]

  @redis_defaults [
    host: 'localhost',
    port: 6379,
  ]

  @connection_defaults [
    socket_opts: [],
    backoff: 2000,
  ]

  @default_timeout 5000

  @doc """
  Starts a connection to Redis.

  This function returns `{:ok, pid}` if the connection is successful. The actual
  TCP connection to the Redis server happens asynchronously: when `start_link/2`
  is called, a pid is returned right away and the connection process starts. All
  the calls to `Redix` that happen during the connection have to wait for the
  connection to be established before being issued (and sent to the Redis
  server).

  This function accepts two arguments: the options to connect to the Redis
  server (like host, port, and so on) and the options to manage the connection
  and the resiliency. The Redis options can be specified as a keyword list or as
  a URI.

  ## Redis options

  ### URI

  In case `uri_or_redis_opts` is a Redis URI, it must be in the form:

      redis://[:password@]host[:port][/db]

  Here are some examples of valid URIs:

      redis://localhost
      redis://:secret@localhost:6397
      redis://example.com:6380/1

  Usernames before the password are ignored, so the these two URIs are
  equivalent:

      redis://:secret@localhost
      redis://myuser:secret@localhost

  The only mandatory thing when using URIs is the host. All other elements
  (password, port, database) are optional and their default value can be found
  in the "Options" section below.

  ### Options

  The following options can be used to specify the parameters used to connect to
  Redis (instead of a URI as described above):

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

  ## Connection options

  `connection_opts` is a list of options used to manage the connection. These
  are the Redix-specific options that can be used:

    * `:socket_opts` - (list of options) this option specifies a list of options
      that are passed to `:gen_tcp.connect/4` when connecting to the Redis
      server. Some socket options (like `:active` or `:binary`) will be
      overridden by Redix so that it functions properly. Defaults to `[]`.
    * `:backoff` - (integer) the time (in milliseconds) to wait before trying to
      reconnect when a network error occurs. Defaults to `2000`.
    * `:max_reconnection_attempts` - (integer or `nil`) the maximum number of
      reconnection attempts that the Redix process is allowed to make. When the
      Redix process "consumes" all the reconnection attempts allowed to it, it
      will exit with the original error's reason. If the value is `nil`, there's
      no limit to the reconnection attempts that can be made. Defaults to `nil`.

  In addition to these options, all options accepted by
  `Connection.start_link/3` (and thus `GenServer.start_link/3`) are forwarded to
  it. For example, a Redix connection can be registered with a name:

      Redix.start_link([], name: :redix)
      Process.whereis(:redix)
      #=> #PID<...>

  ## Examples

      iex> Redix.start_link
      {:ok, #PID<...>}

      iex> Redix.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Redix.start_link([database: 3], [name: :redix_3])
      {:ok, #PID<...>}

  """
  @spec start_link(binary | Keyword.t, Keyword.t) :: GenServer.on_start
  def start_link(uri_or_redis_opts \\ [], connection_opts \\ [])

  def start_link(uri, connection_opts) when is_binary(uri) and is_list(connection_opts) do
    uri |> Redix.URI.opts_from_uri |> start_link(connection_opts)
  end

  def start_link(redis_opts, connection_opts) when is_list(redis_opts) and is_list(connection_opts) do
    {other_opts, connection_opts} =
      Keyword.split(connection_opts, [:socket_opts, :backoff, :max_reconnection_attempts])

    redis_opts = Keyword.merge(@redis_defaults, redis_opts)
    other_opts = Keyword.merge(@connection_defaults, other_opts)
    redix_opts = Keyword.merge(redis_opts, other_opts)

    Connection.start_link(Redix.Connection, redix_opts, connection_opts)
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

  If the given command (`args`) is an empty command (`[]`), `{:error,
  :empty_command}` will be returned.

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
    case pipeline(conn, [args], opts) do
      {:ok, [%Redix.Error{} = error]} ->
        {:error, error}
      {:ok, [resp]} ->
        {:ok, resp}
      o ->
        o
    end
  end

  @doc """
  Issues a command on the Redis server, raising if there's an error.

  This function works exactly like `command/3` but:

    * if the command is successful, then the result is returned not wrapped in a
      `{:ok, result}` tuple.
    * if there's a Redis error, a `Redix.Error` error is raised (with the
      original message).
    * if there's a network error (e.g., `{:error, :closed}`) a `Redix.Network`
      error is raised.

  This function accepts the same options as `command/3`.

  ## Examples

      iex> Redix.command!(conn, ["SET", "mykey", "foo"])
      "OK"

      iex> Redix.command!(conn, ["INCR", "mykey"])
      ** (Redix.Error) ERR value is not an integer or out of range

  If Redis goes down (before a reconnection happens):

      iex> Redix.command!(conn, ["GET", "mykey"])
      ** (Redix.ConnectionError) :closed

  """
  @spec command!(GenServer.server, command, Keyword.t) :: Redix.Protocol.redis_value
  def command!(conn, args, opts \\ []) do
    case command(conn, args, opts) do
      {:ok, resp} ->
        resp
      {:error, %Redix.Error{} = error} ->
        raise error
      {:error, error} ->
        raise Redix.ConnectionError, error
    end
  end

  @doc """
  Issues a pipeline of commands on the Redis server.

  `commands` must be a list of commands, where each command is a list of strings
  making up the command and its arguments. The commands will be sent as a single
  "block" to Redis, and a list of ordered responses (one for each command) will
  be returned.

  The return value is `{:ok, results}` if the request is successful, `{:error,
  reason}` otherwise.

  Note that `{:ok, results}` is returned even if `results` contains one or more
  Redis errors (`Redix.Error` structs). This is done to avoid having to walk the
  list of results (a `O(n)` operation) to look for errors, leaving the
  responsibility to the user. That said, errors other than Redis errors (like
  network errors) always cause the return value to be `{:error, reason}`.

  If `commands` is an empty list (`[]`), then a `Redix.Error` will be raised
  right away. If any of the commands in `commands` is an empty command (`[]`),
  `{:error, :empty_command}` will be returned (which mirrors the behaviour of
  `command/3` in case of empty commands).

  ## Examples

      iex> Redix.pipeline(conn, [~w(INCR mykey), ~w(INCR mykey), ~w(DECR mykey)])
      {:ok, [1, 2, 1]}

      iex> Redix.pipeline(conn, [~w(SET k foo), ~w(INCR k), ~(GET k)])
      {:ok, ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]}

  If Redis goes down (before a reconnection happens):

      iex> Redix.pipeline(conn, [~w(SET mykey foo), ~w(GET mykey)])
      {:error, :closed}

  """
  @spec pipeline(GenServer.server, [command], Keyword.t) ::
    {:ok, [Redix.Protocol.redis_value]} |
    {:error, atom}
  def pipeline(conn, commands, opts \\ [])

  def pipeline(_conn, [], _opts) do
    raise(Redix.Error, "no commands passed to the pipeline")
  end

  def pipeline(conn, commands, opts) do
    if Enum.any?(commands, &(&1 == [])) do
      {:error, :empty_command}
    else
      Connection.call(conn, {:commands, commands}, opts[:timeout] || @default_timeout)
    end
  end

  @doc """
  Issues a pipeline of commands to the Redis server, raising if there's an error.

  This function works similarly to `pipeline/3`, except:

    * if there are no errors in issuing the commands (even if there are one or
      more Redis errors in the results), the results are returned directly (not
      wrapped in a `{:ok, results}` tuple).
    * if there's a connection error then a `Redix.ConnectionError` exception is raised.

  For more information on why nothing is raised if there are one or more Redis
  errors (`Redix.Error` structs) in the list of results, look at the
  documentation for `pipeline/3`.

  This function accepts the same options as `pipeline/3`.

  ## Examples

      iex> Redix.pipeline!(conn, [~w(INCR mykey), ~w(INCR mykey), ~w(DECR mykey)])
      [1, 2, 1]

      iex> Redix.pipeline!(conn, [~w(SET k foo), ~w(INCR k), ~(GET k)])
      ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]

  If Redis goes down (before a reconnection happens):

      iex> Redix.pipeline!(conn, [~w(SET mykey foo), ~w(GET mykey)])
      ** (Redix.ConnectionError) :closed

  """
  @spec pipeline!(GenServer.server, [command], Keyword.t) :: [Redix.Protocol.redis_value]
  def pipeline!(conn, commands,  opts \\ []) do
    case pipeline(conn, commands, opts) do
      {:ok, resp} ->
        resp
      {:error, error} ->
        raise Redix.ConnectionError, error
    end
  end
end
