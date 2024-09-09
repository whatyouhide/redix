defmodule Redix do
  @moduledoc """
  This module provides the main API to interface with [Redis](http://redis.io) and
  [Valkey](https://valkey.io/).

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

  ## Sentinel

  Redix supports [Redis Sentinel](https://redis.io/topics/sentinel) by passing a `:sentinel`
  option to `start_link/1` (or `start_link/2`) instead of `:host` and `:port`. In `:sentinel`,
  you'll specify a list of sentinel nodes to try when connecting and the name of a primary group
  (see `start_link/1` for more detailed information on these options). When connecting, Redix will
  attempt to connect to each of the specified sentinels in the given order. When it manages to
  connect to a sentinel, it will ask that sentinel for the address of the primary for the given
  primary group. Then, it will connect to that primary and ask it for confirmation that it is
  indeed a primary. If anything in this process doesn't go right, the next sentinel in the list
  will be tried.

  All of this happens in case of disconnections as well. If there's a disconnection, the whole
  process of asking sentinels for a primary is executed again.

  You should only care about Redis Sentinel when starting a `Redix` connection: once started,
  using the connection will be exactly the same as the non-sentinel scenario.

  ## Transactions or pipelining?

  Pipelining and transactions have things in common but they're fundamentally different.
  With a pipeline, you're sending all commands in the pipeline *at once* on the connection
  to Redis. This means Redis receives all commands at once, but the Redis server is not
  guaranteed to process all those commands at once.

  On the other hand, a `MULTI`/`EXEC` transaction guarantees that when `EXEC` is called
  all the queued commands in the transaction are executed atomically. However, you don't
  need to send all the commands in the transaction at once. If you want to combine
  pipelining with `MULTI`/`EXEC` transactions, use `transaction_pipeline/3`.

  ## Skipping replies

  Redis provides commands to control whether you want replies to your commands or not.
  These commands are `CLIENT REPLY ON`, `CLIENT REPLY SKIP`, and `CLIENT REPLY OFF`.
  When you use `CLIENT REPLY SKIP`, only the command that follows will not get a reply.
  When you use `CLIENT REPLY OFF`, all the commands that follow will not get replies until
  `CLIENT REPLY ON` is issued. Redix does not support these commands directly because they
  would change the whole state of the connection. To skip replies, use `noreply_pipeline/3`
  or `noreply_command/3`.

  Skipping replies is useful to improve performance when you want to issue many commands
  but are not interested in the responses to those commands.

  > ### Blocked `CLIENT` commands {: .warning}
  >
  > Some servers may block `CLIENT` commands. For example, Google Memorystorage is [does
  > this](https://cloud.google.com/memorystore/docs/redis/product-constraints).
  > If this is the case, the `noreply_*` functions mentioned above won't work.

  ## SSL

  Redix supports SSL by passing `ssl: true` in `start_link/1`. You can use the `:socket_opts`
  option to pass options that will be used by the SSL socket, like certificates.

  If the [CAStore](https://hex.pm/packages/castore) dependency is available, Redix will pick
  up its CA certificate store file automatically. You can select a different CA certificate
  store by passing in the `:cacertfile` or `:cacerts` socket options. If the server uses a
  self-signed certificate, such as for testing purposes, disable certificate verification by
  passing `verify: :verify_none` in the socket options.

  Some Redis servers, notably Amazon ElastiCache, use wildcard certificates that require
  additional socket options for successful verification (requires OTP 21.0 or later):

      Redix.start_link(
        host: "example.com", port: 9999, ssl: true,
        socket_opts: [
          customize_hostname_check: [
            match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
          ]
        ]
      )

  ## Telemetry

  Redix uses Telemetry for instrumentation and logging. See `Redix.Telemetry`.
  """

  # This module is only a "wrapper" module that exposes the public API alongside
  # documentation for it. The real work is done in Redix.Connection and every
  # function in this module goes through Redix.Connection.pipeline/3 one way or
  # another.

  @typedoc """
  A command, which is a list of things that can be converted to strings.

  For example, this is a valid command:

      ["INCR", :my_key, 1]

  We recommend using strings directly to avoid needless conversions.
  """
  @type command() :: [String.Chars.t()]

  @typedoc """
  The reference to a Redix connection.
  """
  @type connection() :: GenServer.server()

  @typedoc """
  Passwords that can be passed to the `:password` option (see `start_link/1`).
  """
  @typedoc since: "1.3.0"
  @type password() :: String.t() | {module(), function_name :: atom(), arguments :: [term()]}

  @typedoc """
  The possible role of a Redis sentinel (see `start_link/1`).
  """
  @typedoc since: "1.3.0"
  @type sentinel_role() :: :primary | :replica

  pipeline_opts_schema = [
    timeout: [
      type: :timeout,
      default: 5_000,
      doc: """
      request timeout (in milliseconds). If the Redis server doesn't reply within this timeout,
      `{:error, %Redix.ConnectionError{reason: :timeout}}` is returned.
      """
    ],
    telemetry_metadata: [
      type: {:map, :any, :any},
      default: %{},
      doc: """
      extra metadata to add to the `[:redix, :pipeline, *]` Telemetry events.
      These end up in the `:extra_metadata` metadata key of these events. See `Redix.Telemetry`.
      """
    ]
  ]

  @pipeline_opts_schema NimbleOptions.new!(pipeline_opts_schema)

  @doc """
  Starts a connection to Redis.

  This function returns `{:ok, pid}` if the Redix process is started
  successfully.

      {:ok, pid} = Redix.start_link()

  The actual TCP connection to the Redis server may happen either synchronously,
  before `start_link/2` returns, or asynchronously. This behaviour is decided by
  the `:sync_connect` option (see below).

  This function accepts one argument which can either be an string representing
  a URI or a keyword list of options.

  ## Using in supervision trees

  Redix supports child specs, so you can use it as part of a supervision tree:

      children = [
        {Redix, host: "redix.myapp.com", name: :redix}
      ]

  See `child_spec/1` for more information.

  ## Using a Redis URI

  In case `uri_or_options` is a Redis URI, it must be in the form:

      redis://[username:password@]host[:port][/db]

  Here are some examples of valid URIs:

    * `redis://localhost`
    * `redis://:secret@localhost:6397`
    * `redis://username:secret@localhost:6397`
    * `redis://example.com:6380/1`
    * `rediss://example.com:6380/1` (for SSL connections)
    * `valkey://example.com:6380/1` (for [Valkey](https://valkey.io/) connections)

  The only mandatory thing when using URIs is the host. All other elements are optional
  and their default value can be found in the "Options" section below.

  In earlier versions of Redix, the username in the URI was ignored. Redis 6 introduced [ACL
  support](https://redis.io/topics/acl). Now, Redix supports usernames as well.

  > #### Valkey {: .info}
  >
  > The `valkey://` schema is supported since Redix v1.5.0.

  ## Options

  The following options can be used to specify the connection:

  #{Redix.StartOptions.options_docs(:redix)}

  ## Examples

      iex> Redix.start_link()
      {:ok, #PID<...>}

      iex> Redix.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Redix.start_link(database: 3, name: :redix_3)
      {:ok, #PID<...>}

  """
  @spec start_link(binary() | keyword()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(uri_or_options \\ [])

  def start_link(uri) when is_binary(uri), do: start_link(uri, [])
  def start_link(opts) when is_list(opts), do: Redix.Connection.start_link(opts)

  @doc """
  Starts a connection to Redis.

  This is the same as `start_link/1`, but the URI and the options get merged. `other_opts` have
  precedence over the things specified in `uri`. Take this code:

      Redix.start_link("redis://localhost:6379", port: 6380)

  In this example, port `6380` will be used.
  """
  @spec start_link(binary(), keyword()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(uri, other_options)

  def start_link(uri, other_options) when is_binary(uri) and is_list(other_options) do
    opts = Redix.URI.to_start_options(uri)
    start_link(Keyword.merge(opts, other_options))
  end

  @doc """
  Returns a child spec to use Redix in supervision trees.

  To use Redix with the default options (same as calling `Redix.start_link()`):

      children = [
        Redix,
        # ...
      ]

  You can pass options:

      children = [
        {Redix, host: "redix.example.com", name: :redix},
        # ...
      ]

  You can also pass a URI:

      children = [
        {Redix, "redis://redix.example.com:6380"}
      ]

  If you want to pass both a URI and options, you can do it by passing a tuple with the URI as the
  first element and the list of options (make sure it has brackets around if using literals) as
  the second element:

      children = [
        {Redix, {"redis://redix.example.com", [name: :redix]}}
      ]

  """
  @spec child_spec(uri | keyword() | {uri, keyword()}) :: Supervisor.child_spec()
        when uri: binary()
  def child_spec(uri_or_options)

  def child_spec({uri, opts}) when is_binary(uri) and is_list(opts) do
    child_spec_with_args([uri, opts])
  end

  def child_spec(uri_or_opts) when is_binary(uri_or_opts) or is_list(uri_or_opts) do
    child_spec_with_args([uri_or_opts])
  end

  defp child_spec_with_args(args) do
    %{
      id: __MODULE__,
      type: :worker,
      start: {__MODULE__, :start_link, args}
    }
  end

  @doc """
  Closes the connection to the Redis server.

  This function is synchronous and blocks until the given Redix connection frees
  all its resources and disconnects from the Redis server. `timeout` can be
  passed to limit the amount of time allowed for the connection to exit; if it
  doesn't exit in the given interval, this call exits.

  ## Examples

      iex> Redix.stop(conn)
      :ok

  """
  @spec stop(connection(), timeout()) :: :ok
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

  Pipelining is not the same as a transaction. For more information, see the
  module documentation.

  ## Options

  #{NimbleOptions.docs(@pipeline_opts_schema)}

  ## Examples

      iex> Redix.pipeline(conn, [["INCR", "mykey"], ["INCR", "mykey"], ["DECR", "mykey"]])
      {:ok, [1, 2, 1]}

      iex> Redix.pipeline(conn, [["SET", "k", "foo"], ["INCR", "k"], ["GET", "k"]])
      {:ok, ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]}

  If Redis goes down (before a reconnection happens):

      iex> {:error, error} = Redix.pipeline(conn, [["SET", "mykey", "foo"], ["GET", "mykey"]])
      iex> error.reason
      :closed

  Extra Telemetry metadata:

      iex> Redix.pipeline(conn, [["PING"]], telemetry_metadata: %{connection: "My conn"})

  """
  @spec pipeline(connection(), [command()], keyword()) ::
          {:ok, [Redix.Protocol.redis_value()]}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def pipeline(conn, commands, options \\ []) when is_list(options) do
    assert_valid_pipeline_commands(commands)
    pipeline_without_checks(conn, commands, options)
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

      iex> Redix.pipeline!(conn, [["INCR", "mykey"], ["INCR", "mykey"], ["DECR", "mykey"]])
      [1, 2, 1]

      iex> Redix.pipeline!(conn, [["SET", "k", "foo"], ["INCR", "k"], ["GET", "k"]])
      ["OK", %Redix.Error{message: "ERR value is not an integer or out of range"}, "foo"]

  If Redis goes down (before a reconnection happens):

      iex> Redix.pipeline!(conn, [["SET", "mykey", "foo"], ["GET", "mykey"]])
      ** (Redix.ConnectionError) :closed

  """
  @spec pipeline!(connection(), [command()], keyword()) :: [Redix.Protocol.redis_value()]
  def pipeline!(conn, commands, options \\ []) do
    case pipeline(conn, commands, options) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  @doc """
  Issues a pipeline of commands to the Redis server, asking the server to not send responses.

  This function is useful when you want to issue commands to the Redis server but you don't
  care about the responses. For example, you might want to set a bunch of keys but you don't
  care for a confirmation that they were set. In these cases, you can save bandwidth by asking
  Redis to not send replies to your commands.

  Since no replies are sent back, this function returns `:ok` in case there are no network
  errors, or `{:error, reason}` otherwise.any()

  This function accepts the same options as `pipeline/3`.

  ## Examples

      iex> commands = [["INCR", "mykey"], ["INCR", "meykey"]]
      iex> Redix.noreply_pipeline(conn, commands)
      :ok
      iex> Redix.command(conn, ["GET", "mykey"])
      {:ok, "2"}

  """
  @doc since: "0.8.0"
  @spec noreply_pipeline(connection(), [command()], keyword()) ::
          :ok | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def noreply_pipeline(conn, commands, options \\ []) when is_list(options) do
    assert_valid_pipeline_commands(commands)
    commands = [["CLIENT", "REPLY", "OFF"]] ++ commands ++ [["CLIENT", "REPLY", "ON"]]

    case pipeline_without_checks(conn, commands, options) do
      # The "OK" response comes from the last "CLIENT REPLY ON".
      {:ok, ["OK"]} -> :ok
      # This can happen if there's an error with the first "CLIENT REPLY OFF" command, like
      # when the Redis server disabled CLIENT commands.
      {:ok, [%Redix.Error{} = error]} -> {:error, error}
      # Connection errors and such are bubbled up.
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Same as `noreply_pipeline/3` but raises in case of errors.
  """
  @doc since: "0.8.0"
  @spec noreply_pipeline!(connection(), [command()], keyword()) :: :ok
  def noreply_pipeline!(conn, commands, options \\ []) do
    case noreply_pipeline(conn, commands, options) do
      :ok -> :ok
      {:error, error} -> raise error
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
  request). `reason` can also be a `Redix.Error` exception in case Redis is
  reachable but returns an error (such as a type error).

  If the given command is an empty command (`[]`), an `ArgumentError`
  exception is raised.

  This function accepts the same options as `pipeline/3`.

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
  @spec command(connection(), command(), keyword()) ::
          {:ok, Redix.Protocol.redis_value()}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def command(conn, command, options \\ []) when is_list(options) do
    case pipeline(conn, [command], options) do
      {:ok, [%Redix.Error{} = error]} -> {:error, error}
      {:ok, [response]} -> {:ok, response}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Issues a command on the Redis server, raising if there's an error.

  This function works exactly like `command/3` but:

    * if the command is successful, then the result is returned directly (not wrapped in a
      `{:ok, result}` tuple).
    * if there's a Redis error or a connection error, a `Redix.Error` or `Redix.ConnectionError`
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
  @spec command!(connection(), command(), keyword()) :: Redix.Protocol.redis_value()
  def command!(conn, command, options \\ []) do
    case command(conn, command, options) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  @doc """
  Same as `command/3` but tells the Redis server to not return a response.

  This function is useful when you want to send a command but you don't care about the response.
  Since the response is not returned, the return value of this function in case the command
  is successfully sent to Redis is `:ok`.

  Not receiving a response means saving traffic on the network and memory allocation for the
  response. See also `noreply_pipeline/3`.

  This function accepts the same options as `pipeline/3`.

  ## Examples

      iex> Redix.noreply_command(conn, ["INCR", "mykey"])
      :ok
      iex> Redix.command(conn, ["GET", "mykey"])
      {:ok, "1"}

  """
  @doc since: "0.8.0"
  @spec noreply_command(connection(), command(), keyword()) ::
          :ok | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def noreply_command(conn, command, options \\ []) when is_list(options) do
    noreply_pipeline(conn, [command], options)
  end

  @doc """
  Same as `noreply_command/3` but raises in case of errors.
  """
  @doc since: "0.8.0"
  @spec noreply_command!(connection(), command(), keyword()) :: :ok
  def noreply_command!(conn, command, options \\ []) do
    case noreply_command(conn, command, options) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Executes a `MULTI`/`EXEC` transaction.

  Redis supports something akin to transactions. It works by sending a `MULTI` command,
  then some commands, and then an `EXEC` command. All the commands after `MULTI` are
  queued until `EXEC` is issued. When `EXEC` is issued, all the responses to the queued
  commands are returned in a list.

  This function accepts the same options as `pipeline/3`.

  ## Examples

  To run a `MULTI`/`EXEC` transaction in one go, use this function and pass a list of
  commands to use in the transaction:

      iex> Redix.transaction_pipeline(conn, [["SET", "mykey", "foo"], ["GET", "mykey"]])
      {:ok, ["OK", "foo"]}

  ## Problems with transactions

  There's an inherent problem with Redix's architecture and `MULTI`/`EXEC` transaction.
  A Redix process is a single connection to Redis that can be used by many clients. If
  a client A sends `MULTI` and client B sends a command before client A sends `EXEC`,
  client B's command will be part of the transaction. This is intended behaviour, but
  it might not be what you expect. This is why `transaction_pipeline/3` exists: this function
  wraps `commands` in `MULTI`/`EXEC` but *sends all in a pipeline*. Since everything
  is sent in the pipeline, it's sent at once on the connection and no commands can
  end up in the middle of the transaction.

  ## Running `MULTI`/`EXEC` transactions manually

  There are still some cases where you might want to start a transaction with `MULTI`,
  then send commands from different processes that you actively want to be in the
  transaction, and then send an `EXEC` to run the transaction. It's still fine to do
  this with `command/3` or `pipeline/3`, but remember what explained in the section
  above. If you do this, do it in an isolated connection (open a new one if necessary)
  to avoid mixing things up.
  """
  @doc since: "0.8.0"
  @spec transaction_pipeline(connection(), [command()], keyword()) ::
          {:ok, [Redix.Protocol.redis_value()]}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def transaction_pipeline(conn, [_ | _] = commands, options \\ []) when is_list(options) do
    with {:ok, responses} <- Redix.pipeline(conn, [["MULTI"]] ++ commands ++ [["EXEC"]], options) do
      case List.last(responses) do
        %Redix.Error{} = error -> {:error, error}
        other -> {:ok, other}
      end
    end
  end

  @doc """
  Executes a `MULTI`/`EXEC` transaction.

  Same as `transaction_pipeline/3`, but returns the result directly instead of wrapping it
  in an `{:ok, result}` tuple or raises if there's an error.

  This function accepts the same options as `pipeline/3`.

  ## Examples

      iex> Redix.transaction_pipeline!(conn, [["SET", "mykey", "foo"], ["GET", "mykey"]])
      ["OK", "foo"]

  """
  @doc since: "0.8.0"
  @spec transaction_pipeline!(connection(), [command()], keyword()) :: [
          Redix.Protocol.redis_value()
        ]
  def transaction_pipeline!(conn, commands, options \\ []) do
    case transaction_pipeline(conn, commands, options) do
      {:ok, response} -> response
      {:error, error} -> raise(error)
    end
  end

  defp pipeline_without_checks(conn, commands, opts) do
    opts = NimbleOptions.validate!(opts, @pipeline_opts_schema)
    timeout = Keyword.fetch!(opts, :timeout)
    telemetry_metadata = Keyword.fetch!(opts, :telemetry_metadata)
    Redix.Connection.pipeline(conn, commands, timeout, telemetry_metadata)
  end

  defp assert_valid_pipeline_commands([] = _commands) do
    raise ArgumentError, "no commands passed to the pipeline"
  end

  defp assert_valid_pipeline_commands(commands) when is_list(commands) do
    Enum.each(commands, &assert_valid_command/1)
  end

  defp assert_valid_pipeline_commands(other) do
    raise ArgumentError, "expected a list of Redis commands, got: #{inspect(other)}"
  end

  defp assert_valid_command([]) do
    raise ArgumentError, "got an empty command ([]), which is not a valid Redis command"
  end

  defp assert_valid_command([first, second | _] = command) do
    case String.upcase(first) do
      first when first in ["SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE"] ->
        raise ArgumentError,
              "Redix doesn't support Pub/Sub commands; use redix_pubsub " <>
                "(https://github.com/whatyouhide/redix_pubsub) for Pub/Sub " <>
                "functionality support. Offending command: #{inspect(command)}"

      "CLIENT" ->
        if String.upcase(second) == "REPLY" do
          raise ArgumentError,
                "CLIENT REPLY commands are forbidden because of how Redix works internally. " <>
                  "If you want to issue commands without getting a reply, use noreply_pipeline/2 or noreply_command/2"
        end

      _other ->
        :ok
    end
  end

  defp assert_valid_command(other) when not is_list(other) do
    raise ArgumentError,
          "expected a list of binaries as each Redis command, got: #{inspect(other)}"
  end

  defp assert_valid_command(_command) do
    :ok
  end
end
