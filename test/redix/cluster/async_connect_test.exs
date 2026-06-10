defmodule Redix.Cluster.AsyncConnectTest do
  use ExUnit.Case, async: true

  # Tests for the cluster's startup behavior (issue #323): by default the initial
  # topology fetch happens *after* start_link/1 returns and is retried with backoff
  # until a seed node answers, mirroring single-node Redix's lazy connect. With
  # sync_connect: true the fetch is synchronous and start_link/1 fails fast. We
  # drive a real Redix.Cluster against a scriptable fake RESP node so we can
  # control exactly when the "cluster" becomes reachable — same pattern as
  # slot_map_refresh_test.exs.

  test "async connect (the default): starts up even when no node is reachable, " <>
         "then discovers the topology in the background" do
    cluster = :"async_#{System.unique_integer([:positive])}"

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)
    node_id = "127.0.0.1:#{port}"

    # The fake node starts "down": it accepts connections but closes them right
    # away, so the topology fetch fails at the socket level.
    {:ok, state} = Agent.start_link(fn -> :down end)

    serve(listen, state, fn
      ["CLUSTER", "SLOTS"] -> cluster_slots([{0, 16_383, node_id}])
      ["PING"] -> "+PONG\r\n"
      _other -> "+OK\r\n"
    end)

    :telemetry_test.attach_event_handlers(self(), [
      [:redix, :cluster, :failed_topology_refresh],
      [:redix, :cluster, :topology_change]
    ])

    # start_link returns right away even though the node is unreachable. A short
    # backoff keeps the test fast.
    start_supervised!(
      {Redix.Cluster, name: cluster, nodes: ["redis://#{node_id}"], backoff_initial: 50}
    )

    # The node is down, so the initial discovery attempt fails: commands await at
    # most that first attempt and then fail fast with a connection error — both
    # keyed commands (empty slot table) and keyless ones (no connections).
    assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
             {:error, %Redix.ConnectionError{reason: :closed}}

    assert Redix.Cluster.command(cluster, ["PING"]) ==
             {:error, %Redix.ConnectionError{reason: :closed}}

    # The failed first attempt sets the marker, so commands no longer await the
    # Manager (they'd otherwise block for in-flight backoff retries too).
    assert :ets.member(:"#{cluster}_slots", :discovery_attempted)

    # The Manager keeps retrying in the background.
    assert_receive {[:redix, :cluster, :failed_topology_refresh], _ref, %{},
                    %{cluster: ^cluster}},
                   1_000

    # The node comes up: the next retry discovers the topology and the cluster
    # becomes usable with no intervention.
    Agent.update(state, fn _ -> :up end)

    assert_receive {[:redix, :cluster, :topology_change], _ref, %{}, %{cluster: ^cluster}},
                   2_000

    wait_until(fn -> Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"} end)

    assert :ets.lookup(:"#{cluster}_slots", 0) == [{0, node_id, []}]
  end

  test "commands issued while the initial topology fetch is in flight wait for it " <>
         "instead of failing" do
    cluster = :"await_#{System.unique_integer([:positive])}"

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)
    node_id = "127.0.0.1:#{port}"

    {:ok, state} = Agent.start_link(fn -> :up end)

    # The node answers CLUSTER SLOTS slowly, so the first fetch is reliably still
    # in flight when the command below is issued. Each connection is served in its
    # own process, so the sleep doesn't block the node connection's PING.
    serve(listen, state, fn
      ["CLUSTER", "SLOTS"] ->
        Process.sleep(300)
        cluster_slots([{0, 16_383, node_id}])

      ["PING"] ->
        "+PONG\r\n"

      _other ->
        "+OK\r\n"
    end)

    start_supervised!({Redix.Cluster, name: cluster, nodes: ["redis://#{node_id}"]})

    # Mirrors single-node Redix: the command awaits the in-flight fetch (and then
    # the node connection, which postpones commands while connecting) instead of
    # failing right away with :closed.
    assert Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"}
    assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "OK"}
  end

  @tag :capture_log
  test "sync_connect: true makes start_link/1 fail fast when no node is reachable" do
    Process.flag(:trap_exit, true)

    # Bind to an ephemeral port and close it again: connecting to it is then
    # refused immediately.
    {:ok, listen} = :gen_tcp.listen(0, [:binary, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    :ok = :gen_tcp.close(listen)

    cluster = :"sync_#{System.unique_integer([:positive])}"

    assert {:error, {:shutdown, {:failed_to_start_child, Redix.Cluster.Manager, reason}}} =
             Redix.Cluster.start_link(
               name: cluster,
               nodes: ["redis://127.0.0.1:#{port}"],
               sync_connect: true
             )

    assert reason == :no_reachable_node
  end

  test "nodes: [] is rejected at validation time" do
    assert_raise NimbleOptions.ValidationError, ~r/expected a non-empty list of nodes/, fn ->
      Redix.Cluster.start_link(name: :rejected_cluster, nodes: [])
    end
  end

  ## Fake RESP node (shared shape with slot_map_refresh_test.exs). When the state
  ## Agent holds :down, accepted connections are closed immediately to simulate an
  ## unreachable node; when :up, commands are served through `handler`.

  defp serve(listen, state, handler) do
    spawn_link(fn -> accept_loop(listen, state, handler) end)
  end

  defp accept_loop(listen, state, handler) do
    case :gen_tcp.accept(listen, 10_000) do
      {:ok, socket} ->
        case Agent.get(state, & &1) do
          :down -> :gen_tcp.close(socket)
          :up -> spawn_link(fn -> loop(socket, handler, "") end)
        end

        accept_loop(listen, state, handler)

      {:error, _reason} ->
        :ok
    end
  end

  defp loop(socket, handler, buffer) do
    case :gen_tcp.recv(socket, 0, 10_000) do
      {:ok, data} ->
        {commands, rest} = parse_commands(buffer <> data, [])
        Enum.each(commands, &:gen_tcp.send(socket, handler.(&1)))
        loop(socket, handler, rest)

      {:error, _reason} ->
        :ok
    end
  end

  # Encodes a CLUSTER SLOTS reply from `{start, stop, "host:port"}` ranges, each
  # owned by a single primary (no replicas).
  defp cluster_slots(ranges) do
    body =
      Enum.map(ranges, fn {start, stop, node_id} ->
        [host, port] = String.split(node_id, ":")

        [
          "*3\r\n",
          ":#{start}\r\n",
          ":#{stop}\r\n",
          "*3\r\n",
          "$#{byte_size(host)}\r\n",
          host,
          "\r\n",
          ":#{port}\r\n",
          "$40\r\n",
          String.duplicate("a", 40),
          "\r\n"
        ]
      end)

    IO.iodata_to_binary(["*#{length(ranges)}\r\n", body])
  end

  ## Minimal RESP request parser (shared shape with slot_map_refresh_test.exs)

  defp parse_commands(buffer, acc) do
    case parse_command(buffer) do
      {:ok, command, rest} -> parse_commands(rest, [command | acc])
      :incomplete -> {Enum.reverse(acc), buffer}
    end
  end

  defp parse_command("*" <> rest) do
    with {:ok, count, rest} <- parse_int_line(rest) do
      parse_bulk_strings(count, rest, [])
    end
  end

  defp parse_command(_other), do: :incomplete

  defp parse_bulk_strings(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp parse_bulk_strings(count, "$" <> rest, acc) do
    with {:ok, length, rest} <- parse_int_line(rest) do
      case rest do
        <<value::binary-size(^length), "\r\n", rest::binary>> ->
          parse_bulk_strings(count - 1, rest, [value | acc])

        _incomplete ->
          :incomplete
      end
    end
  end

  defp parse_bulk_strings(_count, _rest, _acc), do: :incomplete

  defp parse_int_line(binary) do
    case :binary.split(binary, "\r\n") do
      [int_string, rest] -> {:ok, String.to_integer(int_string), rest}
      [_no_crlf_yet] -> :incomplete
    end
  end

  defp wait_until(fun, timeout \\ 2_000)
  defp wait_until(_fun, timeout) when timeout <= 0, do: flunk("condition not met in time")

  defp wait_until(fun, timeout) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, timeout - 20)
    end
  end
end
