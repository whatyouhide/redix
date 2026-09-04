defmodule Redix.ConnectorAddressSelectionTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.FakeNode
  alias Redix.Connector

  @host ~c"redis.test"
  @loopback {127, 0, 0, 1}
  @unreachable {192, 0, 2, 1}

  # Each test controls DNS results and connection failures through its own Agent.
  # Successful connections use real loopback sockets.
  defmodule Transport do
    def connect(address, port, opts, timeout) do
      {state, opts} = Keyword.pop(opts, :test_state)

      {result, delay} =
        Agent.get_and_update(state, fn state ->
          {Map.get(state.results, address, {:connect, 0}),
           %{state | attempts: state.attempts ++ [{address, timeout}]}}
        end)

      if timeout != :infinity and delay >= timeout do
        Process.sleep(timeout)
        {:error, :timeout}
      else
        Process.sleep(delay)

        case result do
          :connect -> :gen_tcp.connect(address, port, opts, timeout)
          {:error, _reason} = error -> error
        end
      end
    end
  end

  setup do
    state =
      start_supervised!(
        {Agent,
         fn ->
           %{
             addresses: {:ok, [@loopback, @unreachable]},
             resolve_delay: 0,
             results: %{@unreachable => {{:error, :econnrefused}, 0}},
             lookups: [],
             attempts: []
           }
         end}
      )

    lookup = fn host, family ->
      {result, delay} =
        Agent.get_and_update(state, fn state ->
          {{state.addresses, state.resolve_delay},
           %{state | lookups: state.lookups ++ [{host, family}]}}
        end)

      Process.sleep(delay)
      result
    end

    %{state: state, lookup: lookup}
  end

  test "tries the other addresses after a refused connection", context do
    {listener, port} = listen()
    seed_for_fallback()

    assert {:ok, socket} = connect(context, port)
    assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
    :gen_tcp.close(socket)
    :gen_tcp.close(server_socket)

    assert [{@unreachable, _}, {@loopback, _}] = Agent.get(context.state, & &1.attempts)
  end

  test "shuffles all addresses and returns the last error", %{state: state} = context do
    addresses = [@loopback, @unreachable, {192, 0, 2, 2}]

    errors = %{
      @loopback => {{:error, :econnrefused}, 0},
      @unreachable => {{:error, :enetunreach}, 0},
      {192, 0, 2, 2} => {{:error, :ehostunreach}, 0}
    }

    Agent.update(state, &%{&1 | addresses: {:ok, addresses}, results: errors})

    first_addresses =
      for seed <- 1..12 do
        :rand.seed(:exsss, {seed, seed, seed})
        Agent.update(state, &%{&1 | attempts: []})
        assert {:error, _reason} = result = connect(context, 0)
        attempted = Agent.get(state, &Enum.map(&1.attempts, fn {address, _} -> address end))
        assert Enum.sort(attempted) == Enum.sort(addresses)
        {last_error, _delay} = Map.fetch!(errors, List.last(attempted))
        assert result == last_error
        hd(attempted)
      end

    assert MapSet.size(MapSet.new(first_addresses)) > 1
    assert length(Agent.get(state, & &1.lookups)) == 12
  end

  test "first mode leaves hostname resolution to the transport", context do
    Agent.update(context.state, &%{&1 | results: %{@host => {{:error, :econnrefused}, 0}}})

    assert {:error, :econnrefused} =
             Connector.connect_socket(
               Transport,
               @host,
               0,
               [test_state: context.state],
               5000,
               :first,
               context.lookup
             )

    assert [{@host, 5000}] = Agent.get(context.state, & &1.attempts)
    assert Agent.get(context.state, & &1.lookups) == []
  end

  test "shares the timeout across lookup and connection attempts", %{state: state} = context do
    addresses = [@loopback, @unreachable, {192, 0, 2, 2}]

    Agent.update(
      state,
      &%{
        &1
        | addresses: {:ok, addresses},
          resolve_delay: 60,
          results: Map.new(addresses, fn address -> {address, {{:error, :econnrefused}, 100}} end)
      }
    )

    # The elapsed difference works even when the monotonic timestamps are negative.
    assert {:error, :timeout} = connect(context, 0, 250)
    attempts = Agent.get(state, & &1.attempts)
    assert length(attempts) in 1..2
    [{_, first_timeout} | _] = attempts
    assert first_timeout <= 190

    for [{_, earlier}, {_, later}] <- Enum.chunk_every(attempts, 2, 1, :discard) do
      assert later < earlier
    end
  end

  test "stops a late DNS lookup and removes its reply and monitor", context do
    parent = self()

    lookup = fn _host, _family ->
      send(parent, {:lookup_worker, self()})
      Process.sleep(5000)
      {:ok, [@loopback]}
    end

    started = System.monotonic_time(:millisecond)
    assert {:error, :timeout} = connect(%{context | lookup: lookup}, 0, 50)
    assert System.monotonic_time(:millisecond) - started < 500
    assert_receive {:lookup_worker, worker}
    refute Process.alive?(worker)
    assert Agent.get(context.state, & &1.attempts) == []
    refute_receive {:DOWN, _, :process, ^worker, _}, 0
    refute_receive {_ref, {:ok, _addresses}}, 0
  end

  test "zero timeout skips DNS and connection attempts", context do
    assert {:error, :timeout} = connect(context, 0, 0)
    assert Agent.get(context.state, & &1.lookups) == []
    assert Agent.get(context.state, & &1.attempts) == []
  end

  test "supports an infinite timeout", %{state: state} = context do
    Agent.update(state, &%{&1 | addresses: {:ok, [@unreachable]}})
    assert {:error, :econnrefused} = connect(context, 0, :infinity)
    assert [{@unreachable, :infinity}] = Agent.get(state, & &1.attempts)
  end

  test "resolves again on each attempt, including after DNS errors", %{state: state} = context do
    for result <- [{:error, :nxdomain}, {:ok, []}] do
      Agent.update(state, &%{&1 | addresses: result})
      assert {:error, :nxdomain} = connect(context, 0)
      assert Agent.get(state, & &1.attempts) == []
    end

    Agent.update(state, &%{&1 | addresses: {:ok, [@unreachable]}})
    assert {:error, :econnrefused} = connect(context, 0)
    assert length(Agent.get(state, & &1.lookups)) == 3
  end

  test "selects the DNS family from socket options", %{state: state, lookup: lookup} do
    ipv6 = {0, 0, 0, 0, 0, 0, 0, 1}
    tcp_module_overrides? = String.to_integer(System.otp_release()) >= 29
    Agent.update(state, &%{&1 | addresses: {:error, :nxdomain}})

    for {opts, family} <- [
          {[:inet6], :inet6},
          {[ip: ipv6], :inet6},
          {[ifaddr: ipv6], :inet6},
          {[ifaddr: %{family: :inet6, addr: ipv6}], :inet6},
          {[tcp_module: :inet6_tcp], :inet6},
          {[:inet, ip: ipv6], :inet},
          {[:inet6, tcp_module: :inet_tcp], if(tcp_module_overrides?, do: :inet, else: :inet6)},
          {[{:tcp_module, :inet_tcp}, :inet6], :inet},
          {[:inet, :inet6], :inet},
          {[:inet6, tcp_module: CustomTCP], :inet6}
        ] do
      assert {:error, :nxdomain} =
               Connector.connect_socket(:gen_tcp, @host, 0, opts, 5000, :random, lookup)

      assert List.last(Agent.get(state, & &1.lookups)) == {@host, family}
    end
  end

  test "keeps telemetry addresses on connection, disconnection, and reconnection", %{test: test} do
    {listener, port} = listen()
    parent = self()
    events = [[:redix, :connection], [:redix, :disconnection]]

    :ok =
      :telemetry.attach_many(
        test,
        events,
        fn event, _, meta, _ ->
          if meta.connection_name == test, do: send(parent, {event, meta})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(test) end)

    start_supervised!(
      {Redix,
       name: test,
       host: "localhost",
       port: port,
       sync_connect: true,
       backoff_initial: 20,
       address_selection: :random}
    )

    address = "localhost:#{port}"
    assert_receive {[:redix, :connection], %{address: ^address, reconnection: false}}
    assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
    :gen_tcp.close(server_socket)
    assert_receive {[:redix, :disconnection], %{address: ^address}}, 1000
    assert_receive {[:redix, :connection], %{address: ^address, reconnection: true}}, 1000
    assert {:ok, server_socket} = :gen_tcp.accept(listener, 1000)
    stop_supervised(Redix)
    :gen_tcp.close(server_socket)
  end

  test "applies random selection to Sentinel discovery" do
    primary = FakeNode.start(fn ["ROLE"] -> "*1\r\n$6\r\nmaster\r\n" end)
    primary_port = Integer.to_string(primary.port)

    sentinel =
      FakeNode.start(fn ["SENTINEL", "get-master-addr-by-name", "main"] ->
        "*2\r\n$9\r\nlocalhost\r\n$#{byte_size(primary_port)}\r\n#{primary_port}\r\n"
      end)

    opts =
      Redix.StartOptions.sanitize(:redix,
        address_selection: :random,
        sentinel: [sentinels: [[host: "localhost", port: sentinel.port]], group: "main"]
      )

    address = "localhost:#{primary.port}"
    assert {:ok, socket, ^address} = Connector.connect(opts, self())
    :gen_tcp.close(socket)
  end

  defp connect(context, port, timeout \\ 5000) do
    Connector.connect_socket(
      Transport,
      @host,
      port,
      [test_state: context.state, active: false],
      timeout,
      :random,
      context.lookup
    )
  end

  defp seed_for_fallback do
    # Shuffle results can differ between Elixir versions. Choose a seed that
    # puts the failed address first, then restore it before the connection.
    seed =
      Enum.find(1..100, fn seed ->
        :rand.seed(:exsss, {seed, seed, seed})
        hd(Enum.shuffle([@loopback, @unreachable])) == @unreachable
      end)

    :rand.seed(:exsss, {seed, seed, seed})
  end

  defp listen do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: @loopback])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    {listener, port}
  end
end
