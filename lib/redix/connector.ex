defmodule Redix.Connector do
  @moduledoc false

  @socket_opts [:binary, active: false]
  @default_timeout 5000

  @spec connect(keyword()) :: {:ok, socket} | {:error, term} | {:stop, term}
        when socket: :gen_tcp.socket() | :ssl.sslsocket()
  def connect(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
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
end
