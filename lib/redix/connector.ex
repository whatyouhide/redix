defmodule Redix.Connector do
  @moduledoc false

  @socket_opts [:binary, active: false]
  @default_timeout 5000
  @default_ssl_opts [verify: :verify_peer, depth: 3]

  alias Redix.{ConnectionError, Format}

  require Logger

  @spec connect(keyword(), pid()) ::
          {:ok, socket, connected_address} | {:error, term} | {:stop, term}
        when socket: :gen_tcp.socket() | :ssl.sslsocket(),
             connected_address: String.t()
  def connect(opts, conn_pid) when is_list(opts) and is_pid(conn_pid) do
    case Keyword.pop(opts, :sentinel) do
      {nil, opts} ->
        host = Keyword.fetch!(opts, :host)
        port = Keyword.fetch!(opts, :port)
        connect_directly(host, port, opts)

      {sentinel_opts, opts} when is_list(sentinel_opts) ->
        connect_through_sentinel(opts, sentinel_opts, conn_pid)
    end
  end

  defp connect_directly(host, port, opts) do
    transport = if opts[:ssl], do: :ssl, else: :gen_tcp
    socket_opts = build_socket_opts(transport, opts[:socket_opts])
    timeout = Keyword.fetch!(opts, :timeout)

    with {:ok, socket} <- transport.connect(host, port, socket_opts, timeout),
         :ok <- setup_socket_buffers(transport, socket) do
      # Here, we should stop if AUTHing or SELECTing a DB fails with a *semantic* error
      # because disconnecting and retrying doesn't make sense, but we should not
      # stop if the issue is at the network layer, because it might happen due to
      # a race condition where the network conn breaks after connecting but before
      # AUTH/SELECT.
      case auth_and_select(transport, socket, opts, timeout) do
        :ok -> {:ok, socket, Format.format_host_and_port(host, port)}
        {:error, %Redix.Error{} = error} -> {:stop, error}
        {:error, :extra_bytes_after_reply} -> {:stop, :extra_bytes_after_reply}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp auth_and_select(transport, socket, opts, timeout) do
    with :ok <- maybe_auth(transport, socket, opts, timeout),
         :ok <- maybe_select(transport, socket, opts, timeout),
         do: :ok
  end

  defp maybe_auth(transport, socket, opts, timeout) do
    username = opts[:username]

    password =
      case opts[:password] do
        {mod, fun, args} -> apply(mod, fun, args)
        password when is_binary(password) -> password
        nil -> nil
      end

    cond do
      username && password ->
        auth_with_username_and_password(transport, socket, username, password, timeout)

      password ->
        auth_with_password(transport, socket, password, timeout)

      true ->
        :ok
    end
  end

  defp auth_with_username_and_password(transport, socket, username, password, timeout) do
    case sync_command(transport, socket, ["AUTH", username, password], timeout) do
      {:ok, "OK"} ->
        :ok

      # An alternative to this hacky code would be to use the INFO command and check the Redis
      # version to see if it's >= 6.0.0 (when ACL was introduced). However, if you're not
      # authenticated, you cannot run INFO (or any other command), so that doesn't work. This
      # solution is a bit fragile since it relies on the exact error message, but that's the best
      # Redis gives use. The only alternative left would be to provide an explicit :use_username
      # option but that feels very orced on the user.
      {:error, %Redix.Error{message: "ERR wrong number of arguments for 'auth' command"}} ->
        Logger.warning("""
        a username was provided to connect to Redis (either via options or via a URI). However, \
        the Redis server version for this connection seems to not support ACLs, which are only \
        supported from Redis version 6.0.0 (https://redis.io/topics/acl). Earlier versions of \
        Redix used to ignore the username if provided, so Redix is now falling back to that \
        behavior. Future Redix versions will raise an error in this particular case, so either \
        remove the username or upgrade Redis to support ACLs.\
        """)

        auth_with_password(transport, socket, password, timeout)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp auth_with_password(transport, socket, password, timeout) do
    with {:ok, "OK"} <- sync_command(transport, socket, ["AUTH", password], timeout), do: :ok
  end

  defp maybe_select(transport, socket, opts, timeout) do
    if database = opts[:database] do
      with {:ok, "OK"} <- sync_command(transport, socket, ["SELECT", database], timeout), do: :ok
    else
      :ok
    end
  end

  defp connect_through_sentinel(opts, sentinel_opts, conn_pid) do
    sentinels = Keyword.fetch!(sentinel_opts, :sentinels)
    transport = if sentinel_opts[:ssl], do: :ssl, else: :gen_tcp

    connect_through_sentinel(sentinels, sentinel_opts, opts, transport, conn_pid)
  end

  defp connect_through_sentinel([], _sentinel_opts, _opts, _transport, _conn_pid) do
    {:error, :no_viable_sentinel_connection}
  end

  defp connect_through_sentinel([sentinel | rest], sentinel_opts, opts, transport, conn_pid) do
    case connect_to_sentinel(sentinel, sentinel_opts, transport) do
      {:ok, sent_socket} ->
        _ = Logger.debug(fn -> "Connected to sentinel #{inspect(sentinel)}" end)

        with :ok <- maybe_auth(transport, sent_socket, sentinel, sentinel_opts[:timeout]),
             {:ok, {server_host, server_port}} <-
               ask_sentinel_for_server(transport, sent_socket, sentinel_opts),
             _ =
               Logger.debug(fn ->
                 "Sentinel reported #{sentinel_opts[:role]}: #{server_host}:#{server_port}"
               end),
             server_host = string_address_to_erlang(server_host),
             {:ok, server_socket, address} <-
               connect_directly(
                 server_host,
                 String.to_integer(server_port),
                 opts
               ),
             :ok <- verify_server_role(server_socket, opts, sentinel_opts) do
          :ok = transport.close(sent_socket)
          {:ok, server_socket, address}
        else
          {cause, reason} when cause in [:error, :stop] ->
            :telemetry.execute([:redix, :failed_connection], %{}, %{
              connection: conn_pid,
              connection_name: opts[:name],
              reason: %ConnectionError{reason: reason},
              sentinel_address: Format.format_host_and_port(sentinel[:host], sentinel[:port])
            })

            :ok = transport.close(sent_socket)
            connect_through_sentinel(rest, sentinel_opts, opts, transport, conn_pid)
        end

      {:error, reason} ->
        :telemetry.execute([:redix, :failed_connection], %{}, %{
          connection: conn_pid,
          connection_name: opts[:name],
          reason: %ConnectionError{reason: reason},
          sentinel_address: Format.format_host_and_port(sentinel[:host], sentinel[:port])
        })

        connect_through_sentinel(rest, sentinel_opts, opts, transport, conn_pid)
    end
  end

  defp string_address_to_erlang(address) when is_binary(address) do
    address = String.to_charlist(address)

    case :inet.parse_address(address) do
      {:ok, ip} -> ip
      {:error, :einval} -> address
    end
  end

  defp string_address_to_erlang(address) do
    address
  end

  defp connect_to_sentinel(sentinel, sentinel_opts, transport) do
    host = Keyword.fetch!(sentinel, :host)
    port = Keyword.fetch!(sentinel, :port)
    socket_opts = build_socket_opts(transport, sentinel_opts[:socket_opts])
    transport.connect(host, port, socket_opts, sentinel_opts[:timeout])
  end

  defp ask_sentinel_for_server(transport, sent_socket, sentinel_opts) do
    group = Keyword.fetch!(sentinel_opts, :group)

    case sentinel_opts[:role] do
      :primary ->
        command = ["SENTINEL", "get-master-addr-by-name", group]

        case sync_command(transport, sent_socket, command, sentinel_opts[:timeout]) do
          {:ok, [primary_host, primary_port]} -> {:ok, {primary_host, primary_port}}
          {:ok, nil} -> {:error, :sentinel_no_primary_found}
          {:error, reason} -> {:error, reason}
        end

      :replica ->
        command = ["SENTINEL", "slaves", group]

        case sync_command(transport, sent_socket, command, sentinel_opts[:timeout]) do
          {:ok, replicas} when replicas != [] ->
            _ = Logger.debug(fn -> "Available replicas: #{inspect(replicas)}" end)
            ["name", _, "ip", host, "port", port | _] = Enum.random(replicas)
            {:ok, {host, port}}

          {:ok, []} ->
            {:error, :sentinel_no_replicas_found_for_given_primary}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp verify_server_role(server_socket, opts, sentinel_opts) do
    transport = if opts[:ssl], do: :ssl, else: :gen_tcp
    timeout = opts[:timeout] || @default_timeout

    expected_role =
      case sentinel_opts[:role] do
        :primary -> "master"
        :replica -> "slave"
      end

    case sync_command(transport, server_socket, ["ROLE"], timeout) do
      {:ok, [^expected_role | _]} -> :ok
      {:ok, [role | _]} -> {:error, {:wrong_role, role}}
      {:error, _reason_or_redis_error} = error -> error
    end
  end

  defp build_socket_opts(:gen_tcp, user_socket_opts) do
    @socket_opts ++ user_socket_opts
  end

  defp build_socket_opts(:ssl, user_socket_opts) do
    # Needs to be dynamic to avoid compile-time warnings.
    ca_store_mod = CAStore

    default_opts =
      if Code.ensure_loaded?(ca_store_mod) do
        [{:cacertfile, ca_store_mod.file_path()} | @default_ssl_opts]
      else
        @default_ssl_opts
      end
      |> Keyword.drop(Keyword.keys(user_socket_opts))

    @socket_opts ++ user_socket_opts ++ default_opts
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

  @spec sync_command(
          :ssl | :gen_tcp,
          :gen_tcp.socket() | :ssl.sslsocket(),
          [String.t()],
          integer()
        ) ::
          {:ok, any}
          | {:error, :extra_bytes_after_reply}
          | {:error, Redix.Error.t()}
          | {:error, :inet.posix()}
  def sync_command(transport, socket, command, timeout) do
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
