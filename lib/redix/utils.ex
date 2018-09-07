defmodule Redix.Utils do
  @moduledoc false

  @socket_opts [:binary, active: false]

  @log_default_opts [
    disconnection: :error,
    failed_connection: :error,
    reconnection: :info
  ]

  @default_opts [
    socket_opts: [],
    sync_connect: false,
    backoff_initial: 500,
    backoff_max: 30000,
    log: @log_default_opts,
    exit_on_disconnection: false
  ]

  @allowed_opts [:host, :port, :database, :password, :name] ++ Keyword.keys(@default_opts)

  @default_timeout 5000

  @spec sanitize_starting_opts(keyword()) :: keyword()
  def sanitize_starting_opts(opts) when is_list(opts) do
    opts =
      Enum.map(opts, fn
        {:log, log_opts} ->
          unless Keyword.keyword?(log_opts) do
            raise ArgumentError,
                  "the :log option must be a keyword list of {action, level}, " <>
                    "got: #{inspect(log_opts)}"
          end

          Keyword.merge(@log_default_opts, log_opts)

        {opt, _value} when not (opt in @allowed_opts) ->
          raise ArgumentError, "unknown option: #{inspect(opt)}"

        other ->
          other
      end)

    opts = sanitize_host_and_port(opts)

    Keyword.merge(@default_opts, opts)
  end

  defp sanitize_host_and_port(opts) do
    {host, port} =
      case {Keyword.get(opts, :host, "localhost"), Keyword.fetch(opts, :port)} do
        {{:local, _unix_socket_path}, {:ok, port}} when port != 0 ->
          raise ArgumentError,
                "when using Unix domain sockets, the port must be 0, got: #{inspect(port)}"

        {{:local, _unix_socket_path} = host, :error} ->
          {host, 0}

        {_host, {:ok, port}} when not is_integer(port) ->
          raise ArgumentError,
                "expected an integer as the value of the :port option, got: #{inspect(port)}"

        {host, {:ok, port}} when is_binary(host) ->
          {String.to_charlist(host), port}

        {host, :error} when is_binary(host) ->
          {String.to_charlist(host), 6379}
      end

    Keyword.merge(opts, host: host, port: port)
  end

  @spec connect(Keyword.t()) :: {:ok, :gen_tcp.socket()} | {:error, term} | {:stop, term, %{}}
  def connect(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    socket_opts = @socket_opts ++ Keyword.fetch!(opts, :socket_opts)
    timeout = opts[:timeout] || @default_timeout

    with {:ok, socket} <- :gen_tcp.connect(host, port, socket_opts, timeout),
         :ok <- setup_socket_buffers(socket) do
      result =
        with :ok <- if(opts[:password], do: auth(socket, opts[:password]), else: :ok),
             :ok <- if(opts[:database], do: select(socket, opts[:database]), else: :ok),
             do: :ok

      case result do
        :ok -> {:ok, socket}
        {:error, reason} -> {:stop, reason}
      end
    end
  end

  @spec format_host(Redix.Connection.state()) :: String.t()
  def format_host(%{opts: opts} = _state) do
    "#{opts[:host]}:#{opts[:port]}"
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(socket) do
    with {:ok, opts} <- :inet.getopts(socket, [:sndbuf, :recbuf, :buffer]) do
      [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer] = opts
      :inet.setopts(socket, buffer: buffer |> max(sndbuf) |> max(recbuf))
    end
  end

  defp auth(socket, password) do
    with :ok <- :gen_tcp.send(socket, Redix.Protocol.pack(["AUTH", password])),
         do: recv_ok_response(socket)
  end

  defp select(socket, database) do
    with :ok <- :gen_tcp.send(socket, Redix.Protocol.pack(["SELECT", database])),
         do: recv_ok_response(socket)
  end

  defp recv_ok_response(socket) do
    recv_ok_response(socket, _continuation = nil)
  end

  defp recv_ok_response(socket, continuation) do
    with {:ok, data} <- :gen_tcp.recv(socket, 0) do
      parser = continuation || (&Redix.Protocol.parse/1)

      case parser.(data) do
        {:ok, "OK", ""} ->
          :ok

        {:ok, %Redix.Error{} = error, ""} ->
          {:error, error}

        {:ok, _response, tail} when byte_size(tail) > 0 ->
          {:error, :extra_bytes_after_reply}

        {:continuation, continuation} ->
          recv_ok_response(socket, continuation)
      end
    end
  end
end
