defmodule Redix.ConnectorSocketOptionsTest do
  # These tests change the VM's socket backend and default TCP module.
  use ExUnit.Case, async: false

  alias Redix.Connector

  defmodule CustomTCP do
    def getaddrs(~c"redis.internal.invalid", _timer), do: {:ok, [{127, 0, 0, 1}]}
    def getserv(port), do: :inet_tcp.getserv(port)
    def connect(ip, port, opts, timeout), do: :inet_tcp.connect(ip, port, opts, timeout)
  end

  setup do
    backend_key = {:kernel, :inet_backend}
    old_backend = :persistent_term.get(backend_key, :unset)
    old_tcp_module = :inet_db.tcp_module()

    on_exit(fn ->
      if old_backend == :unset,
        do: :persistent_term.erase(backend_key),
        else: :persistent_term.put(backend_key, old_backend)

      :inet_db.set_tcp_module(old_tcp_module)
    end)

    :persistent_term.put(backend_key, :inet)
    :inet_db.set_tcp_module(:inet_tcp)
    :ok
  end

  test "keeps a custom TCP module's hostname lookup in both modes" do
    {listener, port} = listen(:inet)

    for selection <- [:system, :random], allocation <- [:remaining, :split] do
      opts =
        Redix.StartOptions.sanitize(:redix,
          host: "redis.internal.invalid",
          port: port,
          address_selection: selection,
          connect_timeout_allocation: allocation,
          socket_opts: [tcp_module: CustomTCP]
        )

      assert_connected(listener, port, opts)
    end
  end

  test "keeps a custom TCP module set through the runtime defaults" do
    {listener, port} = listen(:inet)
    :inet_db.set_tcp_module(CustomTCP)

    for selection <- [:system, :random], allocation <- [:remaining, :split] do
      opts =
        Redix.StartOptions.sanitize(:redix,
          host: "redis.internal.invalid",
          port: port,
          address_selection: selection,
          connect_timeout_allocation: allocation
        )

      assert_connected(listener, port, opts)
    end
  end

  test "matches family option order with either runtime socket backend" do
    for backend <- [:inet, :socket],
        family_opts <- [[:inet, :inet6], [:inet6, :inet]] do
      :persistent_term.put({:kernel, :inet_backend}, backend)
      family = expected_family(backend, family_opts)
      {listener, port} = listen(family)

      # Check the expected family through OTP before checking the connector.
      assert {:ok, socket} = :gen_tcp.connect(~c"localhost", port, family_opts, 1000)
      :gen_tcp.close(socket)
      assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
      :gen_tcp.close(server_socket)

      for selection <- [:system, :random], allocation <- [:remaining, :split] do
        opts =
          Redix.StartOptions.sanitize(:redix,
            host: "localhost",
            port: port,
            address_selection: selection,
            connect_timeout_allocation: allocation,
            socket_opts: family_opts
          )

        assert_connected(listener, port, opts)
      end
    end
  end

  test "honors an explicit socket backend during address selection" do
    family_opts = [:inet, :inet6]
    {listener, port} = listen(expected_family(:socket, family_opts))
    socket_opts = [{:inet_backend, :socket} | family_opts]

    for selection <- [:system, :random], allocation <- [:remaining, :split] do
      assert {:ok, socket} =
               Connector.connect_socket(
                 :gen_tcp,
                 ~c"localhost",
                 port,
                 socket_opts,
                 1000,
                 selection,
                 allocation
               )

      :gen_tcp.close(socket)
      assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
      :gen_tcp.close(server_socket)
    end
  end

  defp expected_family(backend, family_opts) do
    if backend == :socket and String.to_integer(System.otp_release()) >= 29,
      do: List.last(family_opts),
      else: hd(family_opts)
  end

  defp listen(family) do
    ip = if family == :inet6, do: {0, 0, 0, 0, 0, 0, 0, 1}, else: {127, 0, 0, 1}
    {:ok, listener} = :gen_tcp.listen(0, [family, {:ip, ip}, {:active, false}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    {listener, port}
  end

  defp assert_connected(listener, port, opts) do
    address = "#{opts[:host]}:#{port}"
    assert {:ok, socket, ^address} = Connector.connect(opts, self())
    :gen_tcp.close(socket)
    assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
    :gen_tcp.close(server_socket)
  end
end
