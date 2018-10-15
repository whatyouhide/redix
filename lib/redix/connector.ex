defmodule Redix.Connector do
  @moduledoc false

  @socket_opts [:binary, active: false]
  @default_timeout 5000

  require Logger

  @spec connect(keyword()) :: {:ok, socket} | {:error, term} | {:stop, term}
        when socket: :gen_tcp.socket() | :ssl.sslsocket()
  def connect(opts) do
    case Keyword.pop(opts, :sentinel) do
      {nil, opts} ->
        host = Keyword.fetch!(opts, :host)
        port = Keyword.fetch!(opts, :port)
        connect_directly(host, port, opts)

      {sentinel_opts, opts} when is_list(sentinel_opts) ->
        connect_through_sentinel(opts, sentinel_opts)
    end
  end

  defp connect_directly(host, port, opts) do
    transport = if opts[:ssl], do: :ssl, else: :gen_tcp
    socket_opts = @socket_opts ++ Keyword.fetch!(opts, :socket_opts)
    timeout = opts[:timeout] || @default_timeout

    with {:ok, socket} <- transport.connect(host, port, socket_opts, timeout),
         :ok <- setup_socket_buffers(transport, socket) do
      case auth_and_select(transport, socket, opts, timeout) do
        :ok -> {:ok, socket}
        {:error, reason} -> {:stop, reason}
      end
    end
  end

  defp auth_and_select(transport, socket, opts, timeout) do
    with :ok <- maybe_auth(transport, socket, opts, timeout),
         :ok <- maybe_select(transport, socket, opts, timeout),
         do: :ok
  end

  defp maybe_auth(transport, socket, opts, timeout) do
    if password = opts[:password] do
      with {:ok, "OK"} <- sync_command(transport, socket, ["AUTH", password], timeout), do: :ok
    else
      :ok
    end
  end

  defp maybe_select(transport, socket, opts, timeout) do
    if database = opts[:database] do
      with {:ok, "OK"} <- sync_command(transport, socket, ["SELECT", database], timeout), do: :ok
    else
      :ok
    end
  end

  defp connect_through_sentinel(opts, sentinel_opts) do
    sentinels = Keyword.fetch!(sentinel_opts, :sentinels)
    transport = if sentinel_opts[:ssl], do: :ssl, else: :gen_tcp

    connect_through_sentinel(sentinels, sentinel_opts, opts, transport)
  end

  defp connect_through_sentinel([], _sentinel_opts, _opts, _transport) do
    {:error, :no_sentinel_replied}
  end

  defp connect_through_sentinel([sentinel | rest], sentinel_opts, opts, transport) do
    Logger.debug(fn -> "Attempting to connect through sentinel: #{inspect(sentinel)}" end)

    with {:ok, sent_socket} <- connect_to_sentinel(sentinel, sentinel_opts, transport),
         _ = Logger.debug(fn -> "Connected to sentinel #{inspect(sentinel)}" end),
         {:ok, primary_address} <-
           ask_sentinel_for_primary(transport, sent_socket, sentinel_opts),
         _ = Logger.debug(fn -> "Sentinel reported primary: #{inspect(primary_address)}" end),
         {:ok, primary_socket} <- connect_directly_to_primary(opts, primary_address),
         :ok <- verify_primary_role(primary_socket, opts) do
      :ok = transport.close(sent_socket)
      {:ok, primary_socket}
    else
      {:error, reason} ->
        log(
          opts,
          :failed_connection,
          "Couldn't connect to a primary through #{inspect(sentinel)}: " <> inspect(reason)
        )

        connect_through_sentinel(rest, sentinel_opts, opts, transport)
    end
  end

  defp connect_to_sentinel(_sentinel = {host, port}, sentinel_opts, transport) do
    socket_opts = @socket_opts ++ Keyword.fetch!(sentinel_opts, :socket_opts)
    transport.connect(host, port, socket_opts, sentinel_opts[:timeout])
  end

  defp ask_sentinel_for_primary(transport, sent_socket, sentinel_opts) do
    group = Keyword.fetch!(sentinel_opts, :group)
    command = ["SENTINEL", "get-master-addr-by-name", group]

    case sync_command(transport, sent_socket, command, sentinel_opts[:timeout]) do
      {:ok, [primary_host, primary_port]} -> {:ok, {primary_host, primary_port}}
      {:ok, nil} -> {:error, :sentinel_doesnt_know_primary}
      {:error, reason} -> {:error, reason}
    end
  end

  defp connect_directly_to_primary(opts, {primary_host, primary_port}) do
    connect_directly(to_charlist(primary_host), String.to_integer(primary_port), opts)
  end

  defp verify_primary_role(primary_socket, opts) do
    transport = if opts[:ssl], do: :ssl, else: :gen_tcp
    timeout = opts[:timeout] || @default_timeout

    case sync_command(transport, primary_socket, ["ROLE"], timeout) do
      {:ok, ["master" | _]} -> :ok
      {:ok, [role | _]} -> {:error, {:wrong_role, role}}
      {:error, %Redix.Error{}} = error -> error
      {:error, reason} -> {:error, reason}
    end
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(transport, socket) do
    inet_mod = if transport == :ssl, do: :ssl, else: :inet

    with {:ok, opts} <- inet_mod.getopts(socket, [:sndbuf, :recbuf, :buffer]) do
      sndbuf = Keyword.fetch!(opts, :sndbuf)
      recbuf = Keyword.fetch!(opts, :recbuf)
      buffer = Keyword.fetch!(opts, :buffer)
      inet_mod.setopts(socket, buffer: buffer |> max(sndbuf) |> max(recbuf))
    end
  end

  defp sync_command(transport, socket, command, timeout) do
    with :ok <- transport.send(socket, Redix.Protocol.pack(command)),
         do: recv_response(transport, socket, &Redix.Protocol.parse/1, timeout)
  end

  defp recv_response(transport, socket, continuation, timeout) do
    with {:ok, data} <- transport.recv(socket, 0, timeout) do
      case continuation.(data) do
        {:ok, %Redix.Error{} = error, ""} -> {:error, error}
        {:ok, response, ""} -> {:ok, response}
        {:ok, _response, rest} when byte_size(rest) > 0 -> {:error, :extra_bytes_after_reply}
        {:continuation, continuation} -> recv_response(transport, socket, continuation, timeout)
      end
    end
  end

  defp log(opts, kind, message) do
    level =
      opts
      |> Keyword.fetch!(:log)
      |> Keyword.fetch!(kind)

    Logger.log(level, message)
  end
end
