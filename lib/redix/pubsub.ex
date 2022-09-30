defmodule Redix.PubSub do
  @moduledoc """
  Interface for the Redis pub/sub functionality.

  The rest of this documentation will assume the reader knows how pub/sub works
  in Redis and knows the meaning of the following Redis commands:

    * `SUBSCRIBE` and `UNSUBSCRIBE`
    * `PSUBSCRIBE` and `PUNSUBSCRIBE`
    * `PUBLISH`

  ## Usage

  Each `Redix.PubSub` process is able to subscribe to/unsubscribe from multiple
  Redis channels/patterns, and is able to handle multiple Elixir processes subscribing
  each to different channels/patterns.

  A `Redix.PubSub` process can be started via `Redix.PubSub.start_link/2`; such
  a process holds a single TCP (or SSL) connection to the Redis server.

  `Redix.PubSub` has a message-oriented API. Subscribe operations are synchronous and return
  a reference that can then be used to match on all messages sent by the `Redix.PubSub` process.

  When `Redix.PubSub` registers a subscriptions, the subscriber process will receive a
  confirmation message:

      {:ok, pubsub} = Redix.PubSub.start_link()
      {:ok, ref} = Redix.PubSub.subscribe(pubsub, "my_channel", self())

      receive do message -> message end
      #=> {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "my_channel"}}

  When the `:subscribed` message is received, it's guaranteed that the `Redix.PubSub` process has
  subscribed to the given channel. This means that after a subscription, messages published to
  a channel are delivered to all Elixir processes subscribed to that channel via `Redix.PubSub`:

      # Someone publishes "hello" on "my_channel"
      receive do message -> message end
      #=> {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "my_channel", payload: "hello"}}

  It's advised to wait for the subscription confirmation for a channel before doing any
  other operation involving that channel.

  Note that unsubscription confirmations are delivered right away even if the `Redix.PubSub`
  process is still subscribed to the given channel: this is by design, as once a process
  is unsubscribed from a channel it won't receive messages anyways, even if the `Redix.PubSub`
  process still receives them.

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

  ## Sentinel support

  Works exactly the same as for normal `Redix` connections. See the documentation for `Redix`
  for more information.

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
  @type connection() :: GenServer.server()

  alias Redix.StartOptions

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
      redis://username:secret@localhost:6397
      redis://example.com:6380/1

  The only mandatory thing when using URIs is the host. All other elements are optional
  and their default value can be found in the "Options" section below.

  In earlier versions of Redix, the username in the URI was ignored. Redis 6 introduced [ACL
  support](https://redis.io/topics/acl). Now, Redix supports usernames as well.

  ## Options

  The following options can be used to specify the parameters used to connect to
  Redis (instead of a URI as described above):

    * `:host` - (string) the host where the Redis server is running. Defaults to
      `"localhost"`.

    * `:port` - (integer) the port on which the Redis server is
      running. Defaults to `6379`.

    * `:username` - (string) the username to connect to Redis. Defaults to `nil`, meaning no
      username is used. Redis supports usernames only since Redis 6 (see the [ACL
      documentation](https://redis.io/topics/acl)). If a username is provided (either via
      options or via URIs) and the Redis version used doesn't support ACL, then Redix falls
      back to using just the password and emits a warning. In future Redix versions, Redix
      will raise if a username is passed and the Redis version used doesn't support ACL.

    * `:password` - (string) the password used to connect to Redis. Defaults to
      `nil`, meaning no password is used. When this option is provided, all Redix
      does is issue an `AUTH` command to Redis in order to authenticate. This can be used
      to fetch the password dynamically on every reconnection but most importantly to
      hide the password from crash reports in case the Redix connection crashes for
      any reason. For example, you can use `password: {System, :fetch_env!, ["REDIX_PASSWORD"]}`.

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

    * `:name` - Redix is bound to the same registration rules as a `GenServer`. See the
      `GenServer` documentation for more information.

    * `:ssl` - (boolean) if `true`, connect through SSL, otherwise through TCP. The
      `:socket_opts` option applies to both SSL and TCP, so it can be used for things
      like certificates. See `:ssl.connect/4`. Defaults to `false`.

    * `:sentinel` - (list of options) exactly the same as the `:sentinel` options in
      `Redix.start_link/1`.

    * `:hibernate_after` - (integer) if present, the Redix connection process awaits any
      message for the given number of milliseconds and if no message is received, the process
      goes into hibernation automatically (by calling `:proc_lib.hibernate/3`). See
      `t::gen_statem.start_opt/0`. Not present by default.

    * `:spawn_opt` - (options) if present, its value is passed as options to the
      Redix connection process as in `Process.spawn/4`. See `t::gen_statem.start_opt/0`.
      Not present by default.

    * `:debug` - (options) if present, the corresponding function in the
      [`:sys` module](http://www.erlang.org/doc/man/sys.html) is invoked.
      Not present by default.

  ## Examples

      iex> Redix.PubSub.start_link()
      {:ok, #PID<...>}

      iex> Redix.PubSub.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Redix.PubSub.start_link([database: 3], [name: :redix_3])
      {:ok, #PID<...>}

  """
  @spec start_link(String.t() | keyword()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(uri_or_opts \\ [])

  def start_link(uri) when is_binary(uri) do
    uri |> Redix.URI.to_start_options() |> start_link()
  end

  def start_link(opts) when is_list(opts) do
    opts = StartOptions.sanitize(opts)
    {gen_statem_opts, opts} = Keyword.split(opts, [:hibernate_after, :debug, :spawn_opt])

    case Keyword.fetch(opts, :name) do
      :error ->
        :gen_statem.start_link(Redix.PubSub.Connection, opts, gen_statem_opts)

      {:ok, atom} when is_atom(atom) ->
        :gen_statem.start_link({:local, atom}, Redix.PubSub.Connection, opts, gen_statem_opts)

      {:ok, {:global, _term} = tuple} ->
        :gen_statem.start_link(tuple, Redix.PubSub.Connection, opts, gen_statem_opts)

      {:ok, {:via, via_module, _term} = tuple} when is_atom(via_module) ->
        :gen_statem.start_link(tuple, Redix.PubSub.Connection, opts, gen_statem_opts)

      {:ok, other} ->
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
  @spec start_link(String.t(), keyword()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(uri, opts) when is_binary(uri) and is_list(opts) do
    uri |> Redix.URI.to_start_options() |> Keyword.merge(opts) |> start_link()
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

      iex> Redix.PubSub.subscribe(conn, ["foo", "bar"], self())
      {:ok, subscription_ref}
      iex> flush()
      {:redix_pubsub, ^conn, ^subscription_ref, :subscribed, %{channel: "foo"}}
      {:redix_pubsub, ^conn, ^subscription_ref, :subscribed, %{channel: "bar"}}
      :ok

  """
  @spec subscribe(connection(), String.t() | [String.t()], subscriber) :: {:ok, reference()}
  def subscribe(conn, channels, subscriber \\ self())
      when is_binary(channels) or is_list(channels) do
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
  def psubscribe(conn, patterns, subscriber \\ self())
      when is_binary(patterns) or is_list(patterns) do
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
  def unsubscribe(conn, channels, subscriber \\ self())
      when is_binary(channels) or is_list(channels) do
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
  def punsubscribe(conn, patterns, subscriber \\ self())
      when is_binary(patterns) or is_list(patterns) do
    :gen_statem.call(conn, {:punsubscribe, List.wrap(patterns), subscriber})
  end
end
