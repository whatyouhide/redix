defmodule Redix.ConnectorTest do
  use ExUnit.Case, async: true

  alias Redix.Connector

  describe "build_socket_opts/3 with :ssl" do
    test "enables peer verification and browser-style hostname checking by default" do
      opts = Connector.build_socket_opts(:ssl, [], ~c"redis.example.com")

      assert opts[:verify] == :verify_peer
      assert opts[:depth] == 3
      assert opts[:server_name_indication] == ~c"redis.example.com"

      assert [match_fun: match_fun] = opts[:customize_hostname_check]
      assert is_function(match_fun, 2)
      assert match_fun == :public_key.pkix_verify_hostname_match_fun(:https)
    end

    test "lets the user override individual defaults without dropping the others" do
      opts = Connector.build_socket_opts(:ssl, [verify: :verify_none], ~c"redis.example.com")

      # Overridden by the user.
      assert opts[:verify] == :verify_none

      # Still filled in by the defaults.
      assert opts[:depth] == 3
      assert [match_fun: _] = opts[:customize_hostname_check]
    end

    test "lets the user override customize_hostname_check itself" do
      custom = [match_fun: fn _ref_id, _present_id -> true end]

      opts =
        Connector.build_socket_opts(
          :ssl,
          [customize_hostname_check: custom],
          ~c"redis.example.com"
        )

      assert opts[:customize_hostname_check] == custom
      # The user value is not duplicated by the default.
      assert keyword_get_values(opts, :customize_hostname_check) == [custom]
    end

    test "user options take precedence and are not duplicated by defaults" do
      opts =
        Connector.build_socket_opts(
          :ssl,
          [verify: :verify_none, depth: 1, server_name_indication: :disable],
          ~c"redis.example.com"
        )

      assert keyword_get_values(opts, :verify) == [:verify_none]
      assert keyword_get_values(opts, :depth) == [1]
      assert keyword_get_values(opts, :server_name_indication) == [:disable]
    end

    test "does not add server name indication for an IP address" do
      opts = Connector.build_socket_opts(:ssl, [], {127, 0, 0, 1})

      refute Keyword.has_key?(opts, :server_name_indication)
    end
  end

  # The returned options list starts with the bare atom `:binary`, so it isn't a
  # proper keyword list and Keyword.* functions would raise on it. Filter to pairs.
  defp keyword_get_values(opts, key) do
    opts
    |> Enum.filter(&match?({_, _}, &1))
    |> Keyword.get_values(key)
  end

  describe "build_socket_opts/3 with :gen_tcp" do
    test "does not inject SSL defaults" do
      opts = Connector.build_socket_opts(:gen_tcp, [], ~c"redis.example.com")

      refute Keyword.has_key?(opts, :verify)
      refute Keyword.has_key?(opts, :customize_hostname_check)
      refute Keyword.has_key?(opts, :server_name_indication)
    end
  end

  describe "connect/2" do
    test "reads the peer address of a TLS socket" do
      port = Redix.TestPorts.port(:stunnel)

      opts =
        Redix.StartOptions.sanitize(:redix,
          host: "localhost",
          port: port,
          ssl: true,
          socket_opts: [verify: :verify_none],
          address_selection: :random
        )

      assert {:ok, socket, _address} = Connector.connect(opts, self())
      on_exit(fn -> :ssl.close(socket) end)
      assert Connector.peer_address(:ssl, socket) == "127.0.0.1:#{port}"
    end

    test "keeps the host name with address_selection: :random" do
      assert_connected_address(address_selection: :random)
    end

    test "keeps the host name with the default address selection" do
      assert_connected_address([])
    end

    test "supports an infinite timeout with random address selection" do
      assert_connected_address(address_selection: :random, timeout: :infinity)
    end

    test "supports Unix sockets with random address selection" do
      path = "/tmp/redix-connector-#{System.unique_integer([:positive])}.sock"
      on_exit(fn -> File.rm(path) end)
      {listener, _path} = listen(ifaddr: {:local, path})
      opts = Redix.StartOptions.sanitize(:redix, host: {:local, path}, address_selection: :random)

      assert {:ok, socket, ^path} = Connector.connect(opts, self())
      assert Connector.peer_address(:gen_tcp, socket) == path
      assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
      :gen_tcp.close(socket)
      :gen_tcp.close(server_socket)
    end

    test "preserves IPv6 selection from socket options and Sentinel addresses" do
      ip = {0, 0, 0, 0, 0, 0, 0, 1}
      {listener, port} = listen([:inet6, ip: ip])

      for {host, socket_opts} <- [
            {~c"::1", [:inet6]},
            {~c"::1", [ip: ip]},
            {~c"::1", [ifaddr: ip]},
            {~c"::1", [tcp_module: :inet6_tcp]},
            {ip, []}
          ] do
        opts =
          Redix.StartOptions.sanitize(:redix,
            host: "::1",
            port: port,
            socket_opts: socket_opts,
            address_selection: :random
          )
          |> Keyword.put(:host, host)

        expected_address = "::1:#{port}"
        assert {:ok, socket, ^expected_address} = Connector.connect(opts, self())
        assert Connector.peer_address(:gen_tcp, socket) == expected_address
        assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
        :gen_tcp.close(socket)
        :gen_tcp.close(server_socket)
      end
    end

    test "matches OTP address-family selection when TCP options conflict" do
      tcp_module_overrides? = String.to_integer(System.otp_release()) >= 29
      family = if tcp_module_overrides?, do: :inet6, else: :inet
      other_family = if tcp_module_overrides?, do: :inet, else: :inet6

      for {socket_opts, family} <- [
            {[:inet, tcp_module: :inet6_tcp], family},
            {[:inet6, tcp_module: :inet_tcp], other_family},
            {[tcp_module: :inet_tcp, tcp_module: :inet6_tcp], family}
          ] do
        ip = if family == :inet6, do: {0, 0, 0, 0, 0, 0, 0, 1}, else: {127, 0, 0, 1}
        {listener, port} = listen([family, ip: ip])

        for selection <- [:first, :random] do
          opts =
            Redix.StartOptions.sanitize(:redix,
              host: "localhost",
              port: port,
              socket_opts: socket_opts,
              address_selection: selection
            )

          assert {:ok, socket, _address} = Connector.connect(opts, self())
          assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
          :gen_tcp.close(socket)
          :gen_tcp.close(server_socket)
        end
      end
    end
  end

  defp assert_connected_address(extra_opts) do
    {listener, port} = listen(ip: {127, 0, 0, 1})

    opts = Redix.StartOptions.sanitize(:redix, [host: "localhost", port: port] ++ extra_opts)
    expected_address = "localhost:#{port}"

    assert {:ok, socket, ^expected_address} = Connector.connect(opts, self())
    assert Connector.peer_address(:gen_tcp, socket) == "127.0.0.1:#{port}"
    assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)

    :ok = :gen_tcp.close(socket)
    assert Connector.peer_address(:gen_tcp, socket) == nil
    :ok = :gen_tcp.close(server_socket)
  end

  defp listen(opts) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true] ++ opts)
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    {listener, port}
  end
end
