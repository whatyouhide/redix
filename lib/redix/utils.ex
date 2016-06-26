defmodule Redix.Utils do
  @moduledoc false

  require Logger

  alias Redix.Auth

  # We use exit_on_close: false so that we can consistently close the socket
  # (with :gen_tcp.close/1) in the disconnect/2 callback. If we left the default
  # value of exit_on_close: true and still called :gen_tcp.close/1 in
  # disconnect/2, then we would sometimes close an already closed socket, which
  # is harmless but inconsistent. Credit for this strategy goes to James Fish.
  @socket_opts [:binary, active: false, exit_on_close: false]

  @redis_opts [:host, :port, :password, :database]
  @redis_default_opts [
    host: 'localhost',
    port: 6379,
  ]

  @redix_behaviour_opts [:socket_opts, :sync_connect, :backoff_initial, :backoff_max, :log, :exit_on_disconnection]
  @redix_default_behaviour_opts [
    socket_opts: [],
    sync_connect: false,
    backoff_initial: 500,
    backoff_max: 30_000,
    log: [],
    exit_on_disconnection: false,
  ]

  @log_default_opts [
    disconnection: :error,
    failed_connection: :error,
    reconnection: :info,
  ]

  @default_timeout 5000

  @spec sanitize_starting_opts(Keyword.t, Keyword.t) :: {Keyword.t, Keyword.t}
  def sanitize_starting_opts(redis_opts, other_opts)
      when is_list(redis_opts) and is_list(other_opts) do
    check_redis_opts(redis_opts)

    # `connection_opts` are the opts to be passed to `Connection.start_link/3`.
    # `redix_behaviour_opts` are the other options to tweak the behaviour of
    # Redix (e.g., the backoff time).
    {redix_behaviour_opts, connection_opts} = Keyword.split(other_opts, @redix_behaviour_opts)

    redis_opts = Keyword.merge(@redis_default_opts, redis_opts)
    redix_behaviour_opts = Keyword.merge(@redix_default_behaviour_opts, redix_behaviour_opts)

    redix_behaviour_opts = Keyword.update!(redix_behaviour_opts, :log, fn(log_opts) ->
      unless Keyword.keyword?(log_opts) do
        raise ArgumentError,
          "the :log option must be a keyword list of {action, level}, got: #{inspect log_opts}"
      end

      Keyword.merge(@log_default_opts, log_opts)
    end)

    redix_opts = Keyword.merge(redix_behaviour_opts, redis_opts)

    {redix_opts, connection_opts}
  end

  @spec connect(Keyword.t) :: {:ok, :gen_tcp.socket} | {:error, term} | {:stop, term, %{}}
  def connect(opts) do
    {host, port, socket_opts, timeout} = tcp_connection_opts(opts)

    # TODO: let's replace with `with` when we depend on ~> 1.2.
    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, socket} ->
        setup_socket_buffers(socket)
        case Auth.auth_and_select_db(socket, opts) do
          {:ok, ""} ->
            :inet.setopts(socket, active: :once)
            {:ok, socket}
          {:ok, tail} when byte_size(tail) > 0 ->
            {:stop, :unexpected_tail_after_auth}
          {:error, reason} ->
            {:stop, reason}
        end
      {:error, _reason} = error ->
        error
    end
  end

  @spec format_host(Redix.Connection.state) :: String.t
  def format_host(%{opts: opts} = _state) do
    "#{opts[:host]}:#{opts[:port]}"
  end

  @spec reply_to_client({pid, reference}, reference, term) :: :ok
  def reply_to_client(from, request_id, reply) do
    Connection.reply(from, {request_id, reply})
  end

  # Extracts the TCP connection options (host, port and socket opts) from the
  # given `opts`.
  defp tcp_connection_opts(opts) do
    host = to_char_list(Keyword.fetch!(opts, :host))
    port = Keyword.fetch!(opts, :port)
    socket_opts = @socket_opts ++ Keyword.fetch!(opts, :socket_opts)
    timeout = opts[:timeout] || @default_timeout

    {host, port, socket_opts, timeout}
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(socket) do
    {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
      :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])

    buffer = buffer |> max(sndbuf) |> max(recbuf)
    :ok = :inet.setopts(socket, [buffer: buffer])
  end

  defp check_redis_opts(opts) when is_list(opts) do
    Enum.each opts, fn {opt, _value} ->
      unless opt in @redis_opts do
        raise ArgumentError,
          "unknown Redis connection option: #{inspect opt}." <>
          " The first argument to start_link/1 should only" <>
          " contain Redis-specific options (host, port," <>
          " password, database)"
      end
    end
  end
end
