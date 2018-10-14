defmodule Redix.Utils do
  @moduledoc false

  @socket_opts [:binary, active: false]
  @default_timeout 5000

  @spec connect(Keyword.t()) :: {:ok, :gen_tcp.socket()} | {:error, term} | {:stop, term, %{}}
  def connect(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    transport = if opts[:ssl], do: :ssl, else: :gen_tcp
    socket_opts = @socket_opts ++ Keyword.fetch!(opts, :socket_opts)
    timeout = opts[:timeout] || @default_timeout

    with {:ok, socket} <- transport.connect(host, port, socket_opts, timeout),
         :ok <- setup_socket_buffers(socket, transport) do
      result =
        with :ok <- maybe_auth(socket, transport, opts, timeout),
             :ok <- maybe_select(socket, transport, opts, timeout),
             do: :ok

      case result do
        :ok -> {:ok, socket}
        {:error, reason} -> {:stop, reason}
      end
    end
  end

  defp maybe_auth(socket, transport, opts, timeout) do
    if password = opts[:password] do
      auth(socket, transport, password, timeout)
    else
      :ok
    end
  end

  defp maybe_select(socket, transport, opts, timeout) do
    if database = opts[:database] do
      select(socket, transport, database, timeout)
    else
      :ok
    end
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(socket, transport) do
    inet_mod = if transport == :ssl, do: :ssl, else: :inet

    with {:ok, opts} <- inet_mod.getopts(socket, [:sndbuf, :recbuf, :buffer]) do
      sndbuf = Keyword.fetch!(opts, :sndbuf)
      recbuf = Keyword.fetch!(opts, :recbuf)
      buffer = Keyword.fetch!(opts, :buffer)
      inet_mod.setopts(socket, buffer: buffer |> max(sndbuf) |> max(recbuf))
    end
  end

  defp auth(socket, transport, password, timeout) do
    with :ok <- transport.send(socket, Redix.Protocol.pack(["AUTH", password])),
         do: recv_ok_response(socket, transport, timeout)
  end

  defp select(socket, transport, database, timeout) do
    with :ok <- transport.send(socket, Redix.Protocol.pack(["SELECT", database])),
         do: recv_ok_response(socket, transport, timeout)
  end

  defp recv_ok_response(socket, transport, timeout) do
    recv_ok_response(socket, transport, _continuation = nil, timeout)
  end

  defp recv_ok_response(socket, transport, continuation, timeout) do
    with {:ok, data} <- transport.recv(socket, 0, timeout) do
      case (continuation || (&Redix.Protocol.parse/1)).(data) do
        {:ok, "OK", ""} ->
          :ok

        {:ok, %Redix.Error{} = error, ""} ->
          {:error, error}

        {:ok, _response, tail} when byte_size(tail) > 0 ->
          {:error, :extra_bytes_after_reply}

        {:continuation, continuation} ->
          recv_ok_response(socket, transport, continuation, timeout)
      end
    end
  end
end
