defmodule Redix.Cluster.SlotMapRefreshTest do
  use ExUnit.Case, async: true

  # Black-box tests for CLUSTER SLOTS handling that can't be reproduced against a
  # healthy Docker cluster (issues #314 and #328). We drive a real Redix.Cluster
  # against a scriptable fake RESP node that answers CLUSTER SLOTS, then observe
  # the cluster's routing state (the slot table) after a refresh. No Docker
  # cluster needed — this follows the fake-node pattern from redirection_test.exs.

  test "drops slot-table entries for slots that become unassigned on refresh" do
    cluster = :"slotmap_#{System.unique_integer([:positive])}"

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)
    node_id = "127.0.0.1:#{port}"

    # Holds the CLUSTER SLOTS reply the fake node serves, swapped between refreshes.
    {:ok, topology} = Agent.start_link(fn -> cluster_slots([{0, 16_383, node_id}]) end)

    serve(listen, fn
      ["CLUSTER", "SLOTS"] -> Agent.get(topology, & &1)
      _other -> "+OK\r\n"
    end)

    start_supervised!({Redix.Cluster, name: cluster, nodes: ["redis://#{node_id}"]})

    slot_table = :"#{cluster}_slots"

    # The init refresh is synchronous, so full coverage is already in the table.
    assert :ets.lookup(slot_table, 0) == [{0, node_id, []}]
    assert :ets.lookup(slot_table, 9_000) == [{9_000, node_id, []}]
    assert :ets.lookup(slot_table, 16_383) == [{16_383, node_id, []}]

    # The node now reports a smaller range: 8192..16383 become unassigned.
    Agent.update(topology, fn _ -> cluster_slots([{0, 8_191, node_id}]) end)
    Redix.Cluster.Manager.refresh_topology(:"#{cluster}_manager")

    # The reactive refresh is async; wait for the stale entry to be deleted. With
    # the bug, slot 9000 keeps pointing at a node that no longer owns it.
    wait_until(fn -> :ets.lookup(slot_table, 9_000) == [] end)

    assert :ets.lookup(slot_table, 16_383) == []
    # Slots still covered are untouched.
    assert :ets.lookup(slot_table, 0) == [{0, node_id, []}]
    assert :ets.lookup(slot_table, 8_191) == [{8_191, node_id, []}]
  end

  # A healthy cluster always covers all 16384 slots, so the "slot becomes
  # unassigned" case above can only be reproduced with a controllable node.
  # Likewise, a null host requires "cluster-preferred-endpoint-type
  # unknown-endpoint" (Redis 7+, typical of managed/NAT'd deployments), which the
  # Docker cluster doesn't use.
  test "substitutes the answering node's address for null hosts in CLUSTER SLOTS" do
    cluster = :"nullhost_#{System.unique_integer([:positive])}"

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)
    node_id = "127.0.0.1:#{port}"

    serve(listen, fn
      # A null host means "use the address you connected to" (issue #328). With
      # the bug, this produced the node id ":#{port}" and a connection attempt to
      # host "".
      ["CLUSTER", "SLOTS"] -> cluster_slots([{0, 16_383, {nil, port}}])
      ["PING"] -> "+PONG\r\n"
      _other -> "+OK\r\n"
    end)

    start_supervised!({Redix.Cluster, name: cluster, nodes: ["redis://#{node_id}"]})

    # The slot table records the address the topology query was answered from.
    assert :ets.lookup(:"#{cluster}_slots", 0) == [{0, node_id, []}]
    assert :ets.lookup(:"#{cluster}_slots", 16_383) == [{16_383, node_id, []}]

    # And commands route to a working connection registered under that address.
    assert Redix.Cluster.command(cluster, ["PING"]) == {:ok, "PONG"}
  end

  ## Fake RESP node

  defp serve(listen, handler) do
    spawn_link(fn -> accept_loop(listen, handler) end)
  end

  defp accept_loop(listen, handler) do
    case :gen_tcp.accept(listen, 5_000) do
      {:ok, socket} ->
        spawn_link(fn -> loop(socket, handler, "") end)
        accept_loop(listen, handler)

      {:error, _reason} ->
        :ok
    end
  end

  defp loop(socket, handler, buffer) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} ->
        {commands, rest} = parse_commands(buffer <> data, [])
        Enum.each(commands, &:gen_tcp.send(socket, handler.(&1)))
        loop(socket, handler, rest)

      {:error, _reason} ->
        :ok
    end
  end

  # Encodes a CLUSTER SLOTS reply from `{start, stop, "host:port"}` (or
  # `{start, stop, {host_or_nil, port}}`) ranges, each owned by a single primary
  # (no replicas). Mirrors the real RESP shape: an array of ranges, each
  # `[start, stop, [host, port, node_id]]`. A `nil` host is encoded as a RESP
  # null bulk string, like Redis 7+ does with
  # "cluster-preferred-endpoint-type unknown-endpoint".
  defp cluster_slots(ranges) do
    body =
      Enum.map(ranges, fn {start, stop, address} ->
        {host, port} =
          case address do
            {_host_or_nil, _port} = host_port -> host_port
            node_id when is_binary(node_id) -> split_host_port(node_id)
          end

        [
          "*3\r\n",
          ":#{start}\r\n",
          ":#{stop}\r\n",
          "*3\r\n",
          encode_host(host),
          ":#{port}\r\n",
          "$40\r\n",
          String.duplicate("a", 40),
          "\r\n"
        ]
      end)

    IO.iodata_to_binary(["*#{length(ranges)}\r\n", body])
  end

  defp encode_host(nil), do: "$-1\r\n"
  defp encode_host(host), do: ["$#{byte_size(host)}\r\n", host, "\r\n"]

  defp split_host_port(node_id) do
    [host, port] = String.split(node_id, ":")
    {host, String.to_integer(port)}
  end

  ## Minimal RESP request parser (shared shape with redirection_test.exs)

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
