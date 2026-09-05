defmodule Redix.Connector do
  @moduledoc false

  @socket_opts [:binary, active: false]
  @default_timeout 5000

  alias Redix.{ConnectionError, Format}

  require Logger

  @spec peer_address(:gen_tcp | :ssl, :gen_tcp.socket() | :ssl.sslsocket()) :: String.t() | nil
  def peer_address(transport, socket) do
    inet_mod = if transport == :ssl, do: :ssl, else: :inet

    case inet_mod.peername(socket) do
      {:ok, {ip, port}} when is_tuple(ip) and tuple_size(ip) in [4, 8] ->
        Format.format_host_and_port(ip, port)

      {:ok, {:local, path}} when path != <<>> ->
        IO.chardata_to_string(path)

      _other ->
        nil
    end
  end

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
    socket_opts = build_socket_opts(transport, opts[:socket_opts], host)
    timeout = Keyword.fetch!(opts, :timeout)

    with {:ok, socket} <-
           connect_socket(transport, host, port, socket_opts, timeout, opts[:address_selection]),
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

  # Public for testing DNS and connection failures without replacing OTP modules.
  @doc false
  @spec connect_socket(
          module(),
          :inet.socket_address() | charlist(),
          :inet.port_number(),
          list(),
          timeout(),
          :first | :random | nil,
          (charlist(), :inet.address_family() -> {:ok, [:inet.ip_address()]} | {:error, term()})
        ) ::
          {:ok, :gen_tcp.socket() | :ssl.sslsocket()} | {:error, term()}
  def connect_socket(
        transport,
        host,
        port,
        socket_opts,
        timeout,
        selection,
        lookup \\ &:inet.getaddrs/2
      ) do
    if selection == :random and hostname?(host) do
      deadline =
        if timeout == :infinity,
          do: :infinity,
          else: System.monotonic_time(:millisecond) + timeout

      with {:ok, addresses} <-
             lookup_addresses(host, address_family(socket_opts), timeout, lookup) do
        address_count = length(addresses)

        addresses
        |> Enum.shuffle()
        |> Enum.with_index()
        |> Enum.reduce_while({:error, :nxdomain}, fn {address, index}, _last_error ->
          timeout =
            if deadline == :infinity,
              do: :infinity,
              else: deadline - System.monotonic_time(:millisecond)

          if timeout == :infinity or timeout > 0 do
            # Divide the time left among the addresses still to try.
            attempt_timeout =
              if timeout == :infinity,
                do: :infinity,
                else: max(div(timeout, address_count - index), 1)

            case transport.connect(address, port, socket_opts, attempt_timeout) do
              {:ok, socket} -> {:halt, {:ok, socket}}
              {:error, _reason} = error -> {:cont, error}
            end
          else
            {:halt, {:error, :timeout}}
          end
        end)
      end
    else
      transport.connect(host, port, socket_opts, timeout)
    end
  end

  # Mirrors how gen_tcp picks inet_tcp or inet6_tcp on OTP 24 to 28: the first of
  # :inet, :inet6, or tcp_module: wins, then the last bind address, then the inet_db
  # default. OTP 29 lets the last tcp_module: override an earlier family atom.
  defp address_family(socket_opts) do
    tcp_module_overrides? = String.to_integer(System.otp_release()) >= 29

    family =
      Enum.reduce(socket_opts, nil, fn
        :inet, family ->
          family || :inet

        :inet6, family ->
          family || :inet6

        {:tcp_module, :inet_tcp}, family ->
          if tcp_module_overrides?, do: :inet, else: family || :inet

        {:tcp_module, :inet6_tcp}, family ->
          if tcp_module_overrides?, do: :inet6, else: family || :inet6

        _other, family ->
          family
      end)

    bind_address =
      List.last(for {key, address} <- socket_opts, key in [:ip, :ifaddr], do: address)

    cond do
      family -> family
      is_tuple(bind_address) and tuple_size(bind_address) == 8 -> :inet6
      match?(%{family: :inet6}, bind_address) -> :inet6
      List.keyfind(:inet.get_rc(), :tcp, 0) == {:tcp, :inet6_tcp} -> :inet6
      true -> :inet
    end
  end

  defp lookup_addresses(_host, _family, 0, _lookup), do: {:error, :timeout}
  defp lookup_addresses(host, family, :infinity, lookup), do: lookup.(host, family)

  defp lookup_addresses(host, family, timeout, lookup) do
    # getaddrs/2 follows the configured resolver but has no timeout argument.
    # Task.shutdown/2 stops a late lookup and removes its reply and monitor.
    task = Task.async(fn -> lookup.(host, family) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :timeout}
    end
  end

  defp auth_and_select(transport, socket, opts, timeout) do
    with :ok <- maybe_auth(transport, socket, opts, timeout),
         :ok <- maybe_select(transport, socket, opts, timeout),
         :ok <- maybe_readonly(transport, socket, opts, timeout),
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

  # Used for connections to Redis Cluster replicas: READONLY makes the replica
  # serve read-only commands instead of redirecting them to the primary. It must
  # be re-issued on every (re)connection, which is why it lives here alongside
  # AUTH/SELECT rather than being sent once after start_link.
  defp maybe_readonly(transport, socket, opts, timeout) do
    if opts[:readonly] do
      with {:ok, "OK"} <- sync_command(transport, socket, ["READONLY"], timeout), do: :ok
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
    case connect_to_sentinel(sentinel, sentinel_opts, transport, opts[:address_selection]) do
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

  defp connect_to_sentinel(sentinel, sentinel_opts, transport, selection) do
    host = Keyword.fetch!(sentinel, :host)
    port = Keyword.fetch!(sentinel, :port)
    socket_opts = build_socket_opts(transport, sentinel_opts[:socket_opts], host)
    connect_socket(transport, host, port, socket_opts, sentinel_opts[:timeout], selection)
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

  # Public for testing: building the final socket options (and especially merging
  # the SSL defaults with user-provided options) is security-sensitive enough to
  # warrant direct unit tests.
  @doc false
  @spec build_socket_opts(:gen_tcp | :ssl, list(), :inet.socket_address() | charlist()) :: list()
  def build_socket_opts(:gen_tcp, user_socket_opts, _host) do
    @socket_opts ++ user_socket_opts
  end

  def build_socket_opts(:ssl, user_socket_opts, host) do
    # Needs to be dynamic to avoid compile-time warnings.
    ca_store_mod = CAStore

    ca_opts =
      if Keyword.has_key?(user_socket_opts, :cacertfile) or
           Keyword.has_key?(user_socket_opts, :cacerts) do
        []
      else
        try do
          [cacerts: :public_key.cacerts_get()]
        rescue
          _ ->
            if Code.ensure_loaded?(ca_store_mod) do
              [cacertfile: ca_store_mod.file_path()]
            else
              []
            end
        end
      end

    default_opts =
      (ca_opts ++ default_ssl_opts(host))
      |> Keyword.drop(Keyword.keys(user_socket_opts))

    @socket_opts ++ user_socket_opts ++ default_opts
  end

  # The defaults applied to every SSL connection. They're filled in only for keys
  # the user didn't pass in :socket_opts (see the Keyword.drop/2 above), so any of
  # them can be overridden per-connection.
  #
  # :customize_hostname_check makes the client validate hostnames the same way
  # browsers do (RFC 6125), which crucially accepts the wildcard certificates used
  # by servers like Amazon ElastiCache. Without it, the stricter default match
  # function in :ssl rejects them. Requires OTP 21.0+, always satisfied since we
  # require Elixir 1.15+ (OTP 24+).
  defp default_ssl_opts(host) do
    opts = [
      verify: :verify_peer,
      depth: 3,
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]

    # :ssl only defaults SNI (and the hostname check) to the host when it connects
    # with a host name. Random address selection connects with an IP address, so
    # the host name has to be passed explicitly.
    if hostname?(host), do: [{:server_name_indication, host} | opts], else: opts
  end

  defp hostname?(host) when is_list(host), do: match?({:error, _}, :inet.parse_address(host))
  defp hostname?(_host), do: false

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
