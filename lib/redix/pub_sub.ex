defmodule Redix.PubSub do
  @moduledoc """
  Interface for the Redis PubSub functionality.

  The rest of this documentation will assume the reader knows how PubSub works
  in Redis and knows the meaning of the following Redis commands:

    * `SUBSCRIBE` and `UNSUBSCRIBE`
    * `PSUBSCRIBE` and `PUNSUBSCRIBE`
    * `PUBLISH`

  ## PubSub connections

  Redix requires users to start a separate connection for using the PubSub
  functionality: connections started via `Redix.start_link/2` cannot be used for
  PubSub, only for sending regular commands and pipelines of commands. In the
  same fashion, "PubSub connections" (connections started via
  `Redix.PubSub.start_link/2`) cannot be used to send commands, only to
  subscribe/unsubscribe to channels.

  ## Reconnections

  PubSub connections are subject to the same reconnection behaviour described in
  the "reconnections" section of the documentation for the `Redix` module. The
  only difference is that a PubSub connection notifies subscribed clients when
  it disconnects and reconnects. The exact form of the messages is described in
  the "Messages" section.

  ## Messages

  All communication with a PubSub connection is done via (Elixir) messages: the
  recipients of these messages will be the processes specified at subscription
  time.

  Every PubSub message sent by Redix has the same form, which is this:

      {:redix_pubsub, message_type, message_subject, other_data}


  Given this format, it's easy to match on all Redix PubSub messages by just
  matching on `{:redix_pubsub, _, _, _}`.

  The message subject and the additional data strictly depend of the message type.

  #### List of possible messages

  The following is a list of all possible PubSub messages that Redix sends:

    * `{:redix_pubsub, :subscribe, channel, nil}` - sent when a client
      successfully subscribes to a `channel` using `subscribe/4`. `client_count`
      is the number of clients subscribed to `channel`. Note that when
      `subscribe/4` is called with more than one channel, a message like this
      one will be sent for each of the channels in the list.
    * `{:redix_pubsub, :psubscribe, pattern, nil}` - exactly like the
      previous message, except it's sent after calls to `psubscribe/4`.
    * `{:redix_pubsub, :unsubscribe, channel, nil}` - sent when a
      client successfully unsubscribes from a `channel` using `unsubscribe/4`.
      `client_count` is the number of clients subscribed to `channel`. Note that
      when `subscribe/4` is called with more than one channel, a message like
      this one will be sent for each of the channels in the list.
    * `{:redix_pubsub, :punsubscribes, pattern, nil}` - exactly like
      the previous message, except it's sent after calls to `punsubscribe/4`.
    * `{:redix_pubsub, :message, content, channel}` - sent when a message is
      published on `channel`. `content` is the content of the message.
    * `{:redix_pubsub, :pmessage, content, {pattern, originating_channel}}` -
      sent when a message is published on `originating_channel` and delivered to
      the recipient because that channels matches `pattern.
    * `{:redix_pubsub, :disconnected, subscribed_channels, nil}` - sent when a
      Redix PubSub connection disconnects from Redis. `subscribed_channels` is
      the list of channels and patterns to which the recipient process was
      subscribed before the disconnection (elements of this list have the form
      `{:channel, channel}` or `{:pattern, pattern}`).
    * `{:redix_pubsub, :reconnected, nil, nil}` - sent when a Redix PubSub
      connection reconnects after a disconnection.

  ## Example

  This is an example of a workflow using the PubSub functionality.

      {:ok, pubsub} = Redix.PubSub.start_link
      {:ok, client} = Redix.start_link

      :ok = Redix.PubSub.subscribe(pubsub, "foo", self())
      # We wait for the subscription confirmation
      {:redix_pubsub, :subscribe, "foo", _} = receive do m -> m end

      Redix.command!(client, ~w(PUBLISH foo hello_foo)

      {:redix_pubsub, :message, msg, channel} = receive do m -> m end
      message #=> "hello_foo"
      channel #=> "foo"

      Redix.PubSub.unsubscribe(pubsub, "foo", self())
      {:redix_pubsub, :unsubscribe, "foo", _} = receive do m -> m end

  """

  @type pubsub_recipient :: pid | port | atom | {atom, node}

  alias Redix.ConnectionUtils

  @default_timeout 5_000

  @doc """
  Starts a PubSub connection to Redis.

  The behaviour of this function and the arguments it takes are exactly the same
  as in `Redix.start_link/2`; look at its documentation for more information.
  """
  @spec start_link(binary | Keyword.t, Keyword.t) :: GenServer.on_start
  def start_link(uri_or_redis_opts \\ [], connection_opts \\ [])

  def start_link(uri, other_opts) when is_binary(uri) and is_list(other_opts) do
    uri |> Redix.URI.opts_from_uri() |> start_link(other_opts)
  end

  def start_link(redis_opts, other_opts) do
    ConnectionUtils.start_link(Redix.PubSub.Connection, redis_opts, other_opts)
  end

  @doc """
  Closes the connection to Redis `conn`.

  This function is asynchronous (*fire and forget*): it returns `:ok` as soon as
  it's called and performs the closing of the connection after that.

  ## Examples

      iex> Redix.PubSub.stop(conn)
      :ok

  """
  @spec stop(GenServer.server) :: :ok
  def stop(conn) do
    Connection.cast(conn, :stop)
  end

  @doc """
  Subscribes `recipient` to the given channel or list of channels.

  Subscribes `recipient` (which can be anything that can be passed to `send/2`)
  to `channels`, which can be a single channel as well as a list of channels.

  For each of the channels in `channels` which `recipient` successfully
  subscribes to, a message will be sent to `recipient` with this form:

      {:redix_pubsub, :subscribe, channel, nil}

  ## Examples

      iex> Redix.subscribe(conn, ["foo", "bar", "baz"], self())
      :ok
      iex> flush()
      {:redix_pubsub, :subscribe, "foo", nil}
      {:redix_pubsub, :subscribe, "bar", nil}
      {:redix_pubsub, :subscribe, "baz", nil}
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
    |> maybe_raise
  end

  @doc """
  Subscribes `recipient` to the given pattern or list of patterns.

  Works like `subscribe/3` but subscribing `recipient` to a pattern (or list of
  patterns) instead of regular channels.

  Upon successful subscription to each of the `patterns`, a message will be sent
  to `recipient` with the following form:

     {:redix_pubsub, :psubscribe, pattern, nil}

  ## Examples

      iex> Redix.psubscribe(conn, "ba*", self())
      :ok
      iex> flush()
      {:redix_pubsub, :psubscribe, "ba*", nil}
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
    |> maybe_raise
  end

  @doc """
  Unsubscribes `recipient` from the given channel or list of channels.

  This function basically "undoes" what `subscribe/3` does: it unsubscribes
  `recipient` from the given channel or list of channels.

  Upon successful unsubscription from each of the `channels`, a message will be
  sent to `recipient` with the following form:

      {:redix_pubsub, :unsubscribe, channel, nil}

  ## Examples

      iex> Redix.unsubscribe(conn, ["foo", "bar", "baz"], self())
      :ok
      iex> flush()
      {:redix_pubsub, :unsubscribe, "foo", nil}
      {:redix_pubsub, :unsubscribe, "bar", nil}
      {:redix_pubsub, :unsubscribe, "baz", nil}
      :ok

  """
  @spec unsubscribe(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) ::
    :ok | {:error, term}
  def unsubscribe(conn, channels, recipient, opts \\ []) do
    timeout = opts[:timeout] || @default_timeout
    Connection.call(conn, {:unsubscribe, List.wrap(channels), recipient}, timeout)
  end

  @doc """
  Unsubscribes `recipient` from the given channel or list of channels, raising
  if there's an error.

  Works exactly like `unsubscribe/4` but raises if there's an error.
  """
  @spec unsubscribe!(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) :: :ok
  def unsubscribe!(conn, channels, recipient, opts \\ []) do
    conn
    |> unsubscribe(channels, recipient, opts)
    |> maybe_raise
  end

  @doc """
  Unsubscribes `recipient` from the given pattern or list of patterns.

  This function basically "undoes" what `psubscribe/3` does: it unsubscribes
  `recipient` from the given channel or list of channels.

  Upon successful unsubscription from each of the `patterns`, a message will be
  sent to `recipient` with the following form:

      {:redix_pubsub, :punsubscribe, pattern, nil}

  ## Examples

      iex> Redix.punsubscribe(conn, "foo_*", self())
      :ok
      iex> flush()
      {:redix_pubsub, :punsubscribe, "foo_*", nil}
      :ok

  """
  @spec punsubscribe(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) ::
    :ok | {:error, term}
  def punsubscribe(conn, patterns, recipient, opts \\ []) do
    timeout = opts[:timeout] || @default_timeout
    Connection.call(conn, {:punsubscribe, List.wrap(patterns), recipient}, timeout)
  end

  @doc """
  Unsubscribes `recipient` from the given pattern or list of patterns, raising
  if there's an error.

  Works exactly like `punsubscribe/4` but raises if there's an error.
  """
  @spec punsubscribe!(GenServer.server, String.t | [String.t], pubsub_recipient, Keyword.t) :: :ok
  def punsubscribe!(conn, patterns, recipient, opts \\ []) do
    conn
    |> punsubscribe(patterns, recipient, opts)
    |> maybe_raise
  end

  # Raises the appropriate error based on the result of a
  # subscription/unsubscription.
  defp maybe_raise(:ok),
    do: :ok
  defp maybe_raise({:error, %Redix.Error{} = err}),
    do: raise(err)
  defp maybe_raise({:error, err}),
    do: raise(Redix.ConnectionError, err)
end
