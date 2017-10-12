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

  ## Reconnections

  Redix tries to be as resilient as possible: it tries to recover automatically
  from most network errors.

  If there's a network error when sending data to Redis or if the connection to Redis
  drops, Redix tries to reconnect. The first reconnection attempt will happen
  after a fixed time interval; if this attempt fails, reconnections are
  attempted until successful, and the time interval between reconnections is
  increased exponentially. Some aspects of this behaviour can be configured; see
  `start_link/2` and the "Reconnections" page in the docs for more information.

  All this behaviour is implemented using the
  [connection](https://github.com/fishcakez/connection) library (a dependency of
  Redix).
  """

  # This module is only a "wrapper" module that exposes the public API alongside
  # documentation for it. The real work is done in Redix.Connection and every
  # function in this module goes through Redix.Connection.pipeline/3 one way or
  # another.

  @type command :: [binary]

  @default_timeout 5000

  @doc false
  def child_spec(args)

  def child_spec([]) do
    child_spec([[], []])
  end

  def child_spec([uri_or_redis_opts]) do
    child_spec([uri_or_redis_opts, []])
  end

  def child_spec([uri_or_redis_opts, connection_opts] = args)
      when (is_binary(uri_or_redis_opts) or is_list(uri_or_redis_opts)) and
             is_list(connection_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args},
      type: :worker
    }
  end

  @doc """
  Starts a connection to Redis.

  This function returns `{:ok, pid}` if the Redix process is started
  successfully.

  The actual TCP connection to the Redis server may happen either synchronously,
  before `start_link/2` returns, or asynchronously: this behaviour is decided by
  the `:sync_connect` option (see below).

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
    * `:sync_connect` - (boolean) decides whether Redix should initiate the TCP
      connection to the Redis server *before* or *after* returning from
      `start_link/2`. This option also changes some reconnection semantics; read
      the "Reconnections" page in the docs.
    * `:backoff_initial` - (integer) the initial backoff time (in milliseconds),
      which is the time that will be waited by the Redix process before
      attempting to reconnect to Redis after a disconnection or failed first
      connection. See the "Reconnections" page in the docs for more information.
    * `:backoff_max` - (integer) the maximum length (in milliseconds) of the
      time interval used between reconnection attempts. See the "Reconnections"
      page in the docs for more information.
    * `:exit_on_disconnection` - (boolean) if `true`, the Redix server will exit
      if it fails to connect or disconnects from Redis. Note that setting this
      option to `true` means that the `:backoff_initial` and `:backoff_max` options
      will be ignored. Defaults to `false`.
    * `:log` - (keyword list) a keyword list of `{action, level}` where `level` is
      the log level to use to log `action`. The possible actions and their default
      values are:
        * `:disconnection` (defaults to `:error`) - logged when the connection to
          Redis is lost
        * `:failed_connection` (defaults to `:error`) - logged when Redix can't
          establish a connection to Redis
        * `:reconnection` (defaults to `:info`) - logged when Redix manages to
          reconnect to Redis after the connection was lost

  In addition to these options, all options accepted by
  `Connection.start_link/3` (and thus `GenServer.start_link/3`) are forwarded to
  it. For example, a Redix connection can be registered with a name by using the
  `:name` option:

      Redix.start_link([], name: :redix)
      Process.whereis(:redix)
      #=> #PID<...>

  ## Examples

      iex> Redix.start_link()
      {:ok, #PID<...>}

      iex> Redix.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Redix.start_link([database: 3], [name: :redix_3])
      {:ok, #PID<...>}

  """
  @spec start_link(binary | Keyword.t(), Keyword.t()) :: GenServer.on_start()
  def start_link(uri_or_redis_opts \\ [], connection_opts \\ [])

  def start_link(uri, other_opts) when is_binary(uri) and is_list(other_opts) do
    uri |> Redix.URI.opts_from_uri() |> start_link(other_opts)
  end

  def start_link(redis_opts, other_opts) do
    Redix.Connection.start_link(redis_opts, other_opts)
  end

  @doc """
  Closes the connection to the Redis server.

  This function is synchronous and blocks until the given Redix connection frees
  all its resources and disconnects from the Redis server. `timeout` can be
  passed to limit the amout of time allowed for the connection to exit; if it
  doesn't exit in the given interval, this call exits.

  ## Examples

      iex> Redix.stop(conn)
      :ok

  """
  @spec stop(GenServer.server(), timeout) :: :ok
  def stop(conn, timeout \\ :infinity) do
    Redix.Connection.stop(conn, timeout)
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

  If `commands` is an empty list (`[]`) or any of the commands in `commands` is
  an empty command (`[]`) then an `ArgumentError` exception is raised right
  away.

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`. If the Redis server
      doesn't reply within this timeout, `{:error,
      %Redix.ConnectionError{reason: :timeout}}` is returned.

  ## Examples

      iex> Redix.pipeline(conn, [["INCR", "mykey"], ["INCR", "mykey"], ["DECR", "mykey"]])
      {:ok, [1, 2, 1]}

      iex> Redix.pipeline(conn, [["SET", "k", "foo"], ["INCR", "k"], ["GET", "k"]])
      {:ok, ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]}

  If Redis goes down (before a reconnection happens):

      iex> {:error, error} = Redix.pipeline(conn, [["SET", "mykey", "foo"], ["GET", "mykey"]])
      iex> error.reason
      :closed

  """
  @spec pipeline(GenServer.server(), [command], Keyword.t()) ::
          {:ok, [Redix.Protocol.redis_value()]} | {:error, atom}
  def pipeline(conn, commands, opts \\ []) do
    assert_valid_pipeline_commands(commands)
    Redix.Connection.pipeline(conn, commands, opts[:timeout] || @default_timeout)
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

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`. If the Redis server
      doesn't reply within this timeout, `{:error,
      %Redix.ConnectionError{reason: :timeout}}` is returned.

  ## Examples

      iex> Redix.pipeline!(conn, [["INCR", "mykey"], ["INCR", "mykey"], ["DECR", "mykey"]])
      [1, 2, 1]

      iex> Redix.pipeline!(conn, [["SET", "k", "foo"], ["INCR", "k"], ["GET", "k"]])
      ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]

  If Redis goes down (before a reconnection happens):

      iex> Redix.pipeline!(conn, [["SET", "mykey", "foo"], ["GET", "mykey"]])
      ** (Redix.ConnectionError) :closed

  """
  @spec pipeline!(GenServer.server(), [command], Keyword.t()) ::
          [Redix.Protocol.redis_value()] | no_return
  def pipeline!(conn, commands, opts \\ []) do
    case pipeline(conn, commands, opts) do
      {:ok, resp} ->
        resp

      {:error, error} ->
        raise error
    end
  end

  @doc """
  Issues a command on the Redis server.

  This function sends `command` to the Redis server and returns the response
  returned by Redis. `pid` must be the pid of a Redix connection. `command` must
  be a list of strings making up the Redis command and its arguments.

  The return value is `{:ok, response}` if the request is successful and the
  response is not a Redis error. `{:error, reason}` is returned in case there's
  an error in the request (such as losing the connection to Redis in between the
  request). If Redis returns an error (such as a type error), a `Redix.Error`
  exception is raised; the reason for this is that these errors are semantic
  errors that most of the times won't go away by themselves over time and users
  of Redix should be notified of them as soon as possible. Connection errors,
  instead, are often temporary errors that will go away when the connection is
  back.

  If the given command is an empty command (`[]`), an `ArgumentError`
  exception is raised.

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`. If the Redis server
      doesn't reply within this timeout, `{:error,
      %Redix.ConnectionError{reason: :timeout}}` is returned.

  ## Examples

      iex> Redix.command(conn, ["SET", "mykey", "foo"])
      {:ok, "OK"}
      iex> Redix.command(conn, ["GET", "mykey"])
      {:ok, "foo"}

      iex> Redix.command(conn, ["INCR", "mykey"])
      {:error, "ERR value is not an integer or out of range"}

  If Redis goes down (before a reconnection happens):

      iex> {:error, error} = Redix.command(conn, ["GET", "mykey"])
      iex> error.reason
      :closed

  """
  @spec command(GenServer.server(), command, Keyword.t()) ::
          {:ok, Redix.Protocol.redis_value()} | {:error, atom | Redix.Error.t()}
  def command(conn, command, opts \\ []) do
    case pipeline(conn, [command], opts) do
      {:ok, [%Redix.Error{} = error]} ->
        raise error

      {:ok, [resp]} ->
        {:ok, resp}

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Issues a command on the Redis server, raising if there's an error.

  This function works exactly like `command/3` but:

    * if the command is successful, then the result is returned not wrapped in a
      `{:ok, result}` tuple.
    * if there's a Redis error, a `Redix.Error` error is raised (with the
      original message).
    * if there's a connection error, a `Redix.ConnectionError`
      error is raised.

  This function accepts the same options as `command/3`.

  ## Options

    * `:timeout` - (integer or `:infinity`) request timeout (in
      milliseconds). Defaults to `#{@default_timeout}`. If the Redis server
      doesn't reply within this timeout, `{:error,
      %Redix.ConnectionError{reason: :timeout}}` is returned.

  ## Examples

      iex> Redix.command!(conn, ["SET", "mykey", "foo"])
      "OK"

      iex> Redix.command!(conn, ["INCR", "mykey"])
      ** (Redix.Error) ERR value is not an integer or out of range

  If Redis goes down (before a reconnection happens):

      iex> Redix.command!(conn, ["GET", "mykey"])
      ** (Redix.ConnectionError) :closed

  """
  @spec command!(GenServer.server(), command, Keyword.t()) ::
          Redix.Protocol.redis_value() | no_return
  def command!(conn, command, opts \\ []) do
    case command(conn, command, opts) do
      {:ok, resp} ->
        resp

      {:error, error} ->
        raise error
    end
  end

  defp assert_valid_pipeline_commands([] = _commands) do
    raise ArgumentError, "no commands passed to the pipeline"
  end

  defp assert_valid_pipeline_commands(commands) when is_list(commands) do
    Enum.each(commands, fn
      [] ->
        raise ArgumentError, "got an empty command ([]), which is not a valid Redis command"

      [first | _] = command when first in ~w(SUBSCRIBE PSUBSCRIBE UNSUBSCRIBE PUNSUBSCRIBE) ->
        raise ArgumentError,
              "Redix doesn't support Pub/Sub commands; use redix_pubsub " <>
                "(https://github.com/whatyouhide/redix_pubsub) for Pub/Sub " <>
                "functionality support. Offending command: #{inspect(command)}"

      command when is_list(command) ->
        :ok

      other ->
        raise ArgumentError,
              "expected a list of binaries as each Redis command, got: #{inspect(other)}"
    end)
  end

  defp assert_valid_pipeline_commands(other) do
    raise ArgumentError, "expected a list of Redis commands, got: #{inspect(other)}"
  end
end
