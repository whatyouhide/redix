defmodule Redix.PubSub do
  @moduledoc """
  Interface for the Redis pub/sub functionality.

  The rest of this documentation will assume the reader knows how pub/sub works
  in Redis and knows the meaning of the following Redis commands:

    * `SUBSCRIBE` and `UNSUBSCRIBE`
    * `PSUBSCRIBE` and `PUNSUBSCRIBE`
    * `PUBLISH`

  ## Usage

  Each `Redix.PubSub` process is able to subcribe to/unsubscribe from multiple
  Redis channels/patterns, and is able to handle multiple Elixir processes subscribing
  each to different channels/patterns.

  A `Redix.PubSub` process can be started via `Redix.PubSub.start_link/2`; such
  a process holds a single TCP connection to the Redis server.

  `Redix.PubSub` has a message-oriented API. Subscribe operations are synchronous and return
  a reference that can then be used to match on all messages sent by the `Redix.PubSub` process.

  When `Redix.PubSub` registers a subscriptions, the subscriber process will receive a
  confirmation message:

      {:ok, pubsub} = Redix.PubSub.start_link()
      {:ok, ref} = Redix.PubSub.subscribe(pubsub, "my_channel", self())

      receive do message -> message end
      #=> {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "my_channel"}}

  After a subscription, messages published to a channel are delivered to all
  Elixir processes subscribed to that channel via `Redix.PubSub`:

      # Someone publishes "hello" on "my_channel"
      receive do message -> message end
      #=> {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "my_channel", payload: "hello"}}

  Messages are also delivered as a confirmation of an unsubscription as well as when the
  `Redix.PubSub` connection goes down. See the "Messages" section below.

  ## Messages

  Most of the communication with a PubSub connection is done via (Elixir) messages: the
  subscribers of these messages will be the processes specified at subscription time (in
  `subscribe/3` or `psubscribe/3`). All `Redix.PubSub` messages have the same form: they're a
  five-element tuple that looks like this:

      {:redix_pubsub, pubsub_pid, subscription_ref, message_type, message_properties}

  where:

    * `pubsub_pid` is the pid of the `Redix.PubSub` process that sent this message.

    * `subscription_ref` is the reference returned by `subscribe/3` or `psubscribe/3`.

    * `message_type` is the type of this message, such as `:subscribed` for subscription
      confirmations, `:message` for pub/sub messages, and so on.

    * `message_properties` is a map of data related to that that varies based on `message_type`.

  Given this format, it's easy to match on all Redix pub/sub messages for a subscription
  as `{:redix_pubsub, _, ^subscription_ref, _, _}`.

  ### List of possible message types and properties

  The following is a comprehensive list of possible message types alongside the properties
  that each can have.

    * `:subscribe` - sent as confirmation of subscription to a channel (via `subscribe/3` or
      after a disconnection and reconnection). One `:subscribe` message is received for every
      channel a process subscribed to. `:subscribe` messages have the following properties:

        * `:channel` - the channel the process has been subscribed to.

    * `:psubscribe` - sent as confirmation of subscription to a pattern (via `psubscribe/3` or
      after a disconnection and reconnection). One `:psubscribe` message is received for every
      pattern a process subscribed to. `:psubscribe` messages have the following properties:

        * `:pattern` - the pattern the process has been subscribed to.

    * `:unsubscribe` - sent as confirmation of unsubscription from a channel (via
      `unsubscribe/3`). `:unsubscribe` messages are received for every channel a
      process unsubscribes from. `:unsubscribe` messages havethe following properties:

        * `:channel` - the channel the process has unsubscribed from.

    * `:punsubscribe` - sent as confirmation of unsubscription from a pattern (via
      `unsubscribe/3`). `:unsubscribe` messages are received for every pattern a
      process unsubscribes from. `:unsubscribe` messages havethe following properties:

        * `:pattern` - the pattern the process has unsubscribed from.

    * `:message` - sent to subscribers to a given channel when a message is published on
      that channel. `:message` messages have the following properties:

        * `:channel` - the channel the message was published on
        * `:payload` - the contents of the message

    * `:pmessage` - sent to subscribers to a given pattern when a message is published on
      a channel that matches that pattern. `:pmessage` messages have the following properties:

        * `:channel` - the channel the message was published on
        * `:pattern` - the original pattern that matched the channel
        * `:payload` - the contents of the message

    * `:disconnected` messages - sent to all subscribers to all channels/patterns when the
      connection to Redis is interrupted. `:disconnected` messages have the following properties:

        * `:error` - the reason for the disconnection, a `Redix.ConnectionError`
          exception struct (that can be raised or turned into a message through
          `Exception.message/1`).

  ## Reconnections

  `Redix.PubSub` tries to be resilient to failures: when the connection with
  Redis is interrupted (for whatever reason), it will try to reconnect to the
  Redis server. When a disconnection happens, `Redix.PubSub` will notify all
  clients subscribed to all channels with a `{:redix_pubsub, pid, subscription_ref, :disconnected,
  _}` message (more on the format of messages above). When the connection goes
  back up, `Redix.PubSub` takes care of actually re-subscribing to the
  appropriate channels on the Redis server and subscribers are notified with a
  `{:redix_pubsub, pid, subscription_ref, :subscribed | :psubscribed, _}` message, the same as
  when a client subscribes to a channel/pattern.

  Note that if `exit_on_disconnection: true` is passed to
  `Redix.PubSub.start_link/2`, the `Redix.PubSub` process will exit and not send
  any `:disconnected` messages to subscribed clients.

  ## Examples

  This is an example of a workflow using the PubSub functionality; it uses
  [Redix](https://github.com/whatyouhide/redix) as a Redis client for publishing
  messages.

      {:ok, pubsub} = Redix.PubSub.start_link()
      {:ok, client} = Redix.start_link()

      Redix.PubSub.subscribe(pubsub, "my_channel", self())
      #=> {:ok, ref}

      # We wait for the subscription confirmation
      receive do
        {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "my_channel"}} -> :ok
      end

      Redix.command!(client, ~w(PUBLISH my_channel hello)

      receive do
        {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "my_channel"} = properties} ->
          properties.payload
      end
      #=> "hello"

      Redix.PubSub.unsubscribe(pubsub, "foo", self())
      #=> :ok

      # We wait for the unsubscription confirmation
      receive do
        {:redix_pubsub, ^pubsub, ^ref, :unsubscribed, _} -> :ok
      end

  """

  @type subscriber() :: pid() | port() | atom() | {atom(), node()}
  @type connection() :: :gen_statem.server_ref()

  alias Redix.Utils

  @doc """
  Starts a pub/sub connection to Redis.

  This function returns `{:ok, pid}` if the PubSub process is started successfully.

  The actual TCP/SSL connection to the Redis server may happen either synchronously,
  before `start_link/2` returns, or asynchronously: this behaviour is decided by
  the `:sync_connect` option (see below).

  This function accepts one argument, either a Redis URI as a string or a list of options.

  ## Redis URI

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

  The only mandatory thing when using URIs is the host. All other elements
  (password, port, database) are optional and their default value can be found
  in the "Options" section below.

  ## Options

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

    * `:socket_opts` - (list of options) this option specifies a list of options
      that are passed to `:gen_tcp.connect/4` when connecting to the Redis
      server. Some socket options (like `:active` or `:binary`) will be
      overridden by `Redix.PubSub` so that it functions properly. Defaults to
      `[]`.

    * `:sync_connect` - (boolean) decides whether Redix should initiate the TCP
      connection to the Redis server *before* or *after* returning from
      `start_link/2`. This option also changes some reconnection semantics; read
      the ["Reconnections" page](http://hexdocs.pm/redix/reconnections.html) in
      the docs for `Redix` for more information.

    * `:backoff_initial` - (integer) the initial backoff time (in milliseconds),
      which is the time that will be waited by the `Redix.PubSub` process before
      attempting to reconnect to Redis after a disconnection or failed first
      connection. See the ["Reconnections"
      page](http://hexdocs.pm/redix/reconnections.html) in the docs for `Redix`
      for more information.

    * `:backoff_max` - (integer) the maximum length (in milliseconds) of the
      time interval used between reconnection attempts. See the ["Reconnections"
      page](http://hexdocs.pm/redix/reconnections.html) in the docs for `Redix`
      for more information.

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

    * `:name` - Redix is bound to the same registration rules as a `GenServer`. See the
      `GenServer` documentation for more information.

    * `:ssl` - (boolean) if `true`, connect through SSL, otherwise through TCP. The
      `:socket_opts` option applies to both SSL and TCP, so it can be used for things
      like certificates. See `:ssl.connect/4`. Defaults to `false`.

  ## Examples

      iex> Redix.PubSub.start_link()
      {:ok, #PID<...>}

      iex> Redix.PubSub.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Redix.PubSub.start_link([database: 3], [name: :redix_3])
      {:ok, #PID<...>}

  """
  @spec start_link(String.t() | keyword()) :: :gen_statem.start_ret()
  def start_link(uri_or_opts \\ [])

  def start_link(uri) when is_binary(uri) do
    uri |> Redix.URI.opts_from_uri() |> start_link()
  end

  def start_link(opts) when is_list(opts) do
    opts = Utils.sanitize_starting_opts(opts)

    case Keyword.pop(opts, :name) do
      {nil, opts} ->
        :gen_statem.start_link(Redix.PubSub.Connection, opts, [])

      {atom, opts} when is_atom(atom) ->
        :gen_statem.start_link({:local, atom}, Redix.PubSub.Connection, opts, [])

      {{:global, _term} = tuple, opts} ->
        :gen_statem.start_link(tuple, Redix.PubSub.Connection, opts, [])

      {{:via, via_module, _term} = tuple, opts} when is_atom(via_module) ->
        :gen_statem.start_link(tuple, Redix.PubSub.Connection, opts, [])

      {other, _opts} ->
        raise ArgumentError, """
        expected :name option to be one of the following:

          * nil
          * atom
          * {:global, term}
          * {:via, module, term}

        Got: #{inspect(other)}
        """
    end
  end

  @doc """
  Same as `start_link/1` but using both a Redis URI and a list of options.

  In this case, options specified in `opts` have precedence over values specified by `uri`.
  For example, if `uri` is `redix://example1.com` but `opts` is `[host: "example2.com"]`, then
  `example2.com` will be used as the host when connecting.
  """
  @spec start_link(String.t(), keyword()) :: :gen_statem.start_ret()
  def start_link(uri, opts) when is_binary(uri) and is_list(opts) do
    uri |> Redix.URI.opts_from_uri() |> Keyword.merge(opts) |> start_link()
  end

  # TODO: Remove on redix_pubsub 0.6.
  def start_link(redis_opts, connection_opts)
      when is_list(redis_opts) and is_list(connection_opts) do
    IO.warn("""
    start_link/2 with two lists of options as arguments is deprecated. Options can now be
    passed as a single list, but you can still pass a Redis URI and a list of options.
    These are all valid:

        start_link()
        start_link(host: "redis.example.com", name: :redix)
        start_link("redis://redis.example.com", name: :redix)

    start_link/2 with two lists of options is going to be removed in the next Redix version.
    """)

    start_link(Keyword.merge(redis_opts, connection_opts))
  end

  @doc """
  Stops the given pub/sub process.

  This function is synchronous and blocks until the given pub/sub connection
  frees all its resources and disconnects from the Redis server. `timeout` can
  be passed to limit the amount of time allowed for the connection to exit; if
  it doesn't exit in the given interval, this call exits.

  ## Examples

      iex> Redix.PubSub.stop(conn)
      :ok

  """
  @spec stop(connection()) :: :ok
  def stop(conn, timeout \\ :infinity) do
    :gen_statem.stop(conn, :normal, timeout)
  end

  @doc """
  Subscribes `subscriber` to the given channel or list of channels.

  Subscribes `subscriber` (which can be anything that can be passed to `send/2`)
  to `channels`, which can be a single channel or a list of channels.

  For each of the channels in `channels` which `subscriber` successfully
  subscribes to, a message will be sent to `subscriber` with this form:

      {:redix_pubsub, pid, subscription_ref, :subscribed, %{channel: channel}}

  See the documentation for `Redix.PubSub` for more information about the format
  of messages.

  ## Examples

      iex> Redix.subscribe(conn, ["foo", "bar"], self())
      {:ok, subscription_ref}
      iex> flush()
      {:redix_pubsub, ^conn, ^subscription_ref, :subscribed, %{channel: "foo"}}
      {:redix_pubsub, ^conn, ^subscription_ref, :subscribed, %{channel: "bar"}}
      :ok

  """
  @spec subscribe(connection(), String.t() | [String.t()], subscriber) :: {:ok, reference()}
  def subscribe(conn, channels, subscriber \\ self()) do
    :gen_statem.call(conn, {:subscribe, List.wrap(channels), subscriber})
  end

  @doc """
  Subscribes `subscriber` to the given pattern or list of patterns.

  Works like `subscribe/3` but subscribing `subscriber` to a pattern (or list of
  patterns) instead of regular channels.

  Upon successful subscription to each of the `patterns`, a message will be sent
  to `subscriber` with the following form:

      {:redix_pubsub, pid, ^subscription_ref, :psubscribed, %{pattern: pattern}}

  See the documentation for `Redix.PubSub` for more information about the format
  of messages.

  ## Examples

      iex> Redix.psubscribe(conn, "ba*", self())
      :ok
      iex> flush()
      {:redix_pubsub, ^conn, ^subscription_ref, :psubscribe, %{pattern: "ba*"}}
      :ok

  """
  @spec psubscribe(connection(), String.t() | [String.t()], subscriber) :: {:ok, reference}
  def psubscribe(conn, patterns, subscriber \\ self()) do
    :gen_statem.call(conn, {:psubscribe, List.wrap(patterns), subscriber})
  end

  @doc """
  Unsubscribes `subscriber` from the given channel or list of channels.

  This function basically "undoes" what `subscribe/3` does: it unsubscribes
  `subscriber` from the given channel or list of channels.

  Upon successful unsubscription from each of the `channels`, a message will be
  sent to `subscriber` with the following form:

      {:redix_pubsub, pid, ^subscription_ref, :unsubscribed, %{channel: channel}}

  See the documentation for `Redix.PubSub` for more information about the format
  of messages.

  ## Examples

      iex> Redix.unsubscribe(conn, ["foo", "bar"], self())
      :ok
      iex> flush()
      {:redix_pubsub, ^conn, ^subscription_ref, :unsubscribed, %{channel: "foo"}}
      {:redix_pubsub, ^conn, ^subscription_ref, :unsubscribed, %{channel: "bar"}}
      :ok

  """
  @spec unsubscribe(connection(), String.t() | [String.t()], subscriber) :: :ok
  def unsubscribe(conn, channels, subscriber \\ self()) do
    :gen_statem.call(conn, {:unsubscribe, List.wrap(channels), subscriber})
  end

  @doc """
  Unsubscribes `subscriber` from the given pattern or list of patterns.

  This function basically "undoes" what `psubscribe/3` does: it unsubscribes
  `subscriber` from the given pattern or list of patterns.

  Upon successful unsubscription from each of the `patterns`, a message will be
  sent to `subscriber` with the following form:

      {:redix_pubsub, pid, ^subscription_ref, :punsubscribed, %{pattern: pattern}}

  See the documentation for `Redix.PubSub` for more information about the format
  of messages.

  ## Examples

      iex> Redix.punsubscribe(conn, "foo_*", self())
      :ok
      iex> flush()
      {:redix_pubsub, ^conn, ^subscription_ref, :punsubscribed, %{pattern: "foo_*"}}
      :ok

  """
  @spec punsubscribe(connection(), String.t() | [String.t()], subscriber) :: :ok
  def punsubscribe(conn, patterns, subscriber \\ self()) do
    :gen_statem.call(conn, {:punsubscribe, List.wrap(patterns), subscriber})
  end
end
