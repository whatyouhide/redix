defmodule Redix.ConnectionUtils do
  @moduledoc false

  require Logger
  alias Redix.Connection.Auth

  # We use exit_on_close: false so that we can consistently close the socket
  # (with :gen_tcp.close/1) in the disconnect/2 callback. If we left the default
  # value of exit_on_close: true and still called :gen_tcp.close/1 in
  # disconnect/2, then we would sometimes close an already closed socket, which
  # is harmless but inconsistent. Credit for this strategy goes to James Fish.
  @socket_opts [:binary, active: false, exit_on_close: false]

  @default_timeout 5000

  @spec connect(term, Redix.Connection.state) :: term
  def connect(info, %{opts: opts} = s) do
    {host, port, socket_opts, timeout} = tcp_connection_opts(opts)

    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, socket} ->
        setup_socket_buffers(socket)
        Auth.auth_and_select_db(%{s | socket: socket, reconnection_attempts: 0})
      {:error, reason} ->
        Logger.error "Error connecting to Redis (#{host_for_logging(s)}): #{:inet.format_error(reason)}"
        handle_connection_error(s, info, reason)
    end
  end

  @spec host_for_logging(Redix.Connection.state) :: String.t
  def host_for_logging(%{opts: opts} = _s) do
    "#{opts[:host]}:#{opts[:port]}"
  end

  @spec send_reply(Redix.Connection.state, iodata, term) ::
    {:reply, term, Redix.Connection.state} |
    {:disconnect, term, Redix.Connection.state}
  def send_reply(%{socket: socket} = s, data, reply) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:reply, reply, s}
      {:error, _reason} = err ->
        {:disconnect, err, s}
    end
  end

  @spec send_noreply(Redix.Connection.state, iodata) ::
    {:noreply, Redix.Connection.state} |
    {:disconnect, term, Redix.Connection.state}
  def send_noreply(%{socket: socket} = s, data) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:noreply, s}
      {:error, _reason} = err ->
        {:disconnect, err, s}
    end
  end

  # This function is called every time we want to try and reconnect. It returns
  # {:backoff, ...} if we're below the max number of allowed reconnection
  # attempts (or if there's no such limit), {:stop, ...} otherwise.
  @spec backoff_or_stop(Redix.Connection.state, non_neg_integer, term) ::
    {:backoff, non_neg_integer, Redix.Connection.state} |
    {:stop, term, Redix.Connection.state}
  def backoff_or_stop(s, backoff, stop_reason) do
    s = update_in(s.reconnection_attempts, &(&1 + 1))

    if attempt_to_reconnect?(s) do
      {:backoff, backoff, s}
    else
      {:stop, stop_reason, s}
    end
  end

  defp attempt_to_reconnect?(%{opts: opts, reconnection_attempts: attempts}) do
    max_attempts = opts[:max_reconnection_attempts]
    is_nil(max_attempts) or (max_attempts > 0 and attempts <= max_attempts)
  end

  # If `info` is :backoff then this is a *reconnection* attempt, so if there's
  # an error let's try to just reconnect after a backoff time (if we're under
  # the max number of retries). If `info` is :init, then this is the first
  # connection attempt so if it fails let's just die.
  defp handle_connection_error(s, :init, reason),
    do: {:stop, reason, s}
  defp handle_connection_error(s, :backoff, reason),
    do: backoff_or_stop(s, s.opts[:backoff], reason)

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
end
