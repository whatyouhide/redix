defmodule Redix.PubSub do
  @type pubsub_recipient :: pid | port | atom | {atom, node}

  @default_timeout 5_000

  @default_opts [
    host: 'localhost',
    port: 6379,
    socket_opts: [],
    backoff: 2000,
  ]

  def start_link(opts_or_uri \\ [])

  def start_link(uri) when is_binary(uri) do
    uri |> Redix.URI.opts_from_uri |> start_link
  end

  def start_link(opts) when is_list(opts) do
    opts = Keyword.merge(@default_opts, opts)
    Connection.start_link(Redix.PubSub.Connection, opts, opts)
  end

  def start_link(uri, opts) when is_binary(uri) and is_list(opts) do
    uri |> Redix.URI.opts_from_uri |> Keyword.merge(opts) |> start_link
  end

  def stop(conn) do
    Connection.cast(conn, :stop)
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

  defp raise_for_pubsub_errors(:ok),
    do: :ok
  defp raise_for_pubsub_errors({:error, %Redix.Error{} = err}),
    do: raise(err)
  defp raise_for_pubsub_errors({:error, err}),
    do: raise(Redix.ConnectionError, err)
end
