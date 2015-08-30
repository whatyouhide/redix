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
      `:backoff` option passed to `start_link/1` or `start_link/2`. The default
      is `2000` milliseconds.

  This behaviour can be tweaked with the `:max_reconnection_attempts` option,
  which controls the max number of reconnection attempts that will be made. For
  example, to never attempt reconnecting this option can be set to `0`. By
  default it's `nil`, meaning there's no limit on the number of reconnection
  attempts that will be made.

  These reconnections attempts only happen when the connection to Redis has been
  established at least once before. If a connection error happens when
  connecting to Redis for the first time, the Redix process will just crash with
  the proper error (see `start_link/1`, `start_link/2`).

  All this behaviour is implemented using the
  [connection](https://github.com/fishcakez/connection) library (a dependency of
  Redix).

  ## PubSub

  Redix provides an interface for the [Redis PubSub
  functionality](http://redis.io/topics/pubsub).

  Every Redix connection has two modes of operation: "commands" mode and PubSub
  mode. "Commands" mode is the one every Redix connection starts with and the
  one described in the sections above: clients call functions in the `Redix`
  module, Redix sends the commands to Redis and then sends the responses back to
  the clients. PubSub mode works differently: clients can only
  subscribe/unsubscribe from channels, but all communication from Redis to the
  clients happens via (Elixir) messages.

  The rest of this section will assume the reader knows how PubSub works in
  Redis and knows the meaning of the Redis commands `SUBSCRIBE`, `PSUBSCRIBE`,
  `UNSUBSCRIBE`, and `PUNSUBSCRIBE`.

  #### Starting PubSub mode

  We already mentioned that every Redix connection starts in "commands" mode. To
  move to PubSub mode, we can just call one of `subscribe/4` or `psubscribe/4`.
  Once in PubSub mode, `command/3` and `pipeline/3` will return `{:error,
  :pubsub_mode}` as we can't send messages to Redis anymore.

  As a side note, `unsubscribe/4` or `punsubscribe/4` return `{:error,
  :not_pubsub_mode}` when the Redix connection is in "commands" mode. In both
  cases, you can use `pubsub?/2` to check if a Redix connection is in PubSub
  mode or not.

  Once a Redix connection goes into PubSub mode, all communication with the
  clients is done through messages. The recipients of these messages will be the
  processes specified at subscription time. The format of *all* PubSub messages
  delivered by Redix is this:

      {:redix_pubsub, message_type, message_subject, other_data}

  Given this format, it's easy to match on all Redix PubSub messages by just
  matching on `{:redix_pubsub, _, _, _}`.

  The message subject and the additional data strictly depend of the message type.

  #### List of possible messages

  The following is a list of all possible PubSub messages that Redix sends:

    * `{:redix_pubsub, :subscribe, channel, client_count}` - sent when a client
      successfully subscribes to a `channel` using `subscribe/4`. `client_count` is the number of
      clients subscribed to `channel`. Note that when `subscribe/4` is called
      with more than one channel, a message like this one will be sent for each
      of the channels in the list.
    * `{:redix_pubsub, :psubscribe, pattern, client_count}` - exactly like the
      previous message, except it's sent after calls to `psubscribe/4`.
    * `{:redix_pubsub, :unsubscribe, channel, client_count}` - sent when a
      client successfully unsubscribes from a `channel` using `unsubscribe/4`.
      `client_count` is the number of clients subscribed to `channel`. Note that
      when `subscribe/4` is called with more than one channel, a message like
      this one will be sent for each of the channels in the list.
    * `{:redix_pubsub, :punsubscribes, pattern, client_count}` - exactly like
      the previous message, except it's sent after calls to `punsubscribe/4`.
    * `{:redix_pubsub, :message, content, channel}` - sent when a message is
      published on `channel`. `content` is the content of the message.
    * `{:redix_pubsub, :pmessage, content, {original_pattern, channel}}` - sent
      when a message is published on `channel`. `original_pattern` is the
      pattern that caused this message to be delivered.

  """

  @type command :: [binary]
  @type pubsub_recipient :: pid | port | atom | {atom, node}

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
    * `:backoff` - (integer) the time (in milliseconds) to wait before trying to
      reconnect when a network error occurs.
    * `:max_reconnection_attempts` - (integer) the maximum number of
      reconnection attempts that the Redix process is allowed to make. When the
      Redix process "consumes" all the reconnection attempts allowed to it, it
      will exit with the original error's reason.

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
    {pubsub?, opts} = Keyword.pop(opts, :pubsub, false)
    Connection.start_link(Redix.Connection, %{opts: opts, pubsub: pubsub?}, connection_opts)
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
  def pipeline(conn, commands, opts \\ []) do
    Connection.call(conn, {:commands, commands}, opts[:timeout] || @default_timeout)
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

  @doc """
  Subscribes `recipient` to the given channel or list of channels.

  Subscribes `recipient` (which can be anything that can be passed to `send/2`)
  to `channels`, which can be a single channel as well as a list of channels.

  See the "PubSub" section in the docs for the `Redix` module for more info on
  PubSub.

  ## Examples

      iex> Redix.subscribe(conn, ["foo", "bar", "baz"], self())
      :ok

  """
  @spec subscribe(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) ::
    :ok | {:error, term}
  def subscribe(conn, channels, recipient, opts \\ []) do
    timeout = opts[:timeout] || @default_timeout
    Connection.call(conn, {:subscribe, List.wrap(channels), recipient}, timeout)
  end

  @doc """
  Subscribes `recipient` to the given channel or list of channels, raising if
  there's an error.

  Works exactly like `subscribe/4` but raises if there's an error.
  """
  @spec subscribe!(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) :: :ok
  def subscribe!(conn, channels, recipient, opts \\ []) do
    conn
    |> subscribe(channels, recipient, opts)
    |> raise_for_pubsub_errors
  end

  @doc """
  Subscribes `recipient` to the given pattern or list of patterns.

  Works like `subscribe/3` but subscribing `recipient` to a pattern (or list of
  patterns) instead of regular channels.

  See the "PubSub" section in the docs for the `Redix` module for more info on
  PubSub.

  ## Examples

      iex> Redix.psubscribe(conn, "ba*", self())
      :ok

  """
  @spec psubscribe(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) ::
    :ok | {:error, term}
  def psubscribe(conn, patterns, recipient, opts \\ []) do
    timeout = opts[:timeout] || @default_timeout
    Connection.call(conn, {:psubscribe, List.wrap(patterns), recipient}, timeout)
  end

  @doc """
  Subscribes `recipient` to the given pattern or list of patterns, raising if
  there's an error.

  Works exactly like `psubscribe/4` but raises if there's an error.
  """
  @spec psubscribe!(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) :: :ok
  def psubscribe!(conn, patterns, recipient, opts \\ []) do
    conn
    |> psubscribe(patterns, recipient, opts)
    |> raise_for_pubsub_errors
  end

  @doc """
  Unsubscribes `recipient` from the given channel or list of channels.

  This function basically "undoes" what `subscribe/3` does: it unsubscribes
  `recipient` from the given channel or list of channels.

  See the "PubSub" section in the docs for the `Redix` module for more info on
  PubSub.

  ## Examples

      iex> Redix.unsubscribe(conn, ["foo", "bar", "baz"], self())
      :ok

  """
  @spec unsubscribe(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) ::
    :ok | {:error, term}
  def unsubscribe(conn, channels, recipient, opts \\ []) do
    timeout = opts[:timeout] || @default_timeout
    Connection.call(conn, {:unsubscribe, List.wrap(channels), recipient}, timeout)
  end

  @doc """
  Subscribes `recipient` to the given channel or list of channels, raising if
  there's an error.

  Works exactly like `unsubscribe/4` but raises if there's an error.
  """
  @spec unsubscribe!(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) :: :ok
  def unsubscribe!(conn, channels, recipient, opts \\ []) do
    conn
    |> unsubscribe(channels, recipient, opts)
    |> raise_for_pubsub_errors
  end

  @doc """
  Unsubscribes `recipient` from the given pattern or list of patterns.

  This function basically "undoes" what `psubscribe/3` does: it unsubscribes
  `recipient` from the given channel or list of channels.

  See the "PubSub" section in the docs for the `Redix` module for more info on
  PubSub.

  ## Examples

      iex> Redix.punsubscribe(conn, ["foo", "bar", "baz"], self())
      :ok

  """
  @spec punsubscribe(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) ::
    :ok | {:error, term}
  def punsubscribe(conn, patterns, recipient, opts \\ []) do
    timeout = opts[:timeout] || @default_timeout
    Connection.call(conn, {:punsubscribe, List.wrap(patterns), recipient}, timeout)
  end

  @doc """
  Subscribes `recipient` to the given pattern or list of patterns, raising if
  there's an error.

  Works exactly like `punsubscribe/4` but raises if there's an error.
  """
  @spec punsubscribe!(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) :: :ok
  def punsubscribe!(conn, patterns, recipient, opts \\ []) do
    conn
    |> punsubscribe(patterns, recipient, opts)
    |> raise_for_pubsub_errors
  end

  @doc """
  Tells whether `conn` is in PubSub mode.

  Returns `true` if `conn` is in PubSub mode (look at the documentation for the
  Redix module) to know more about PubSub), `false` otherwise.

  ## Examples

      iex> Redix.pubsub?(conn)
      false
      iex> Redix.subscribe(conn, "foo", self())
      :ok
      iex> Redix.pubsub?(conn)
      true

  """
  @spec pubsub?(GenServer.server, Keyword.t) :: boolean
  def pubsub?(conn, opts \\ []) do
    Connection.call(conn, :pubsub?, opts[:timeout] || @default_timeout)
  end

  defp merge_with_default_opts(opts) do
    Keyword.merge @default_opts, opts, fn
      _k, default_val, nil ->
        default_val
      _k, _default_val, val ->
        val
    end
  end

  defp raise_for_pubsub_errors(:ok),
    do: :ok
  defp raise_for_pubsub_errors({:error, %Redix.Error{} = err}),
    do: raise(err)
  defp raise_for_pubsub_errors({:error, err}),
    do: raise(Redix.ConnectionError, err)
end
