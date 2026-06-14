defmodule Redix.Cluster.RedirectionTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.Hash

  # These tests drive Redix.Cluster's MOVED/ASK redirection handling against
  # scriptable fake RESP servers, so they don't need the Docker cluster. We wire
  # up the cluster's internal resources (registry, slot table) by hand and route
  # the redirect targets at fake nodes we fully control — letting us reproduce
  # multi-hop chains (ASK -> ASK, ASK -> MOVED) that a healthy cluster won't emit
  # on demand. See issue #295.

  setup do
    cluster = :"redir_#{System.unique_integer([:positive])}"

    start_supervised!({Registry, keys: :unique, name: :"#{cluster}_registry"})
    start_supervised!({Task.Supervisor, name: :"#{cluster}_task_supervisor"})

    # The Manager normally owns this table; here we create it directly and route
    # slots ourselves. It dies with the test process. The marker mimics a cluster
    # whose initial topology fetch completed, so commands don't try to await the
    # (nonexistent) Manager.
    :ets.new(:"#{cluster}_slots", [:named_table, :public, :set])
    :ets.insert(:"#{cluster}_slots", {:discovery_attempted, true})

    %{cluster: cluster}
  end

  test "follows an ASK -> ASK chain to the final node", %{cluster: cluster} do
    slot = Hash.hash_slot("x")

    # Terminal node: accepts ASKING, returns the value.
    node_c =
      start_node(cluster, fn
        ["ASKING"] -> "+OK\r\n"
        ["GET", _] -> "$3\r\nbar\r\n"
      end)

    # Middle node: ASKs onward to C.
    node_b =
      start_node(cluster, fn
        ["ASKING"] -> "+OK\r\n"
        ["GET", _] -> "-ASK #{slot} #{node_c}\r\n"
      end)

    # Slot owner: the initial command lands here (plainly) and ASKs to B.
    node_a = start_node(cluster, fn ["GET", _] -> "-ASK #{slot} #{node_b}\r\n" end)

    route_slot(cluster, slot, node_a)

    assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "bar"}
  end

  test "follows an ASK -> MOVED redirection", %{cluster: cluster} do
    slot = Hash.hash_slot("x")

    # MOVED target: gets a plain command (no ASKING) and serves the value.
    node_c = start_node(cluster, fn ["GET", _] -> "$5\r\nhello\r\n" end)

    node_b =
      start_node(cluster, fn
        ["ASKING"] -> "+OK\r\n"
        ["GET", _] -> "-MOVED #{slot} #{node_c}\r\n"
      end)

    node_a = start_node(cluster, fn ["GET", _] -> "-ASK #{slot} #{node_b}\r\n" end)

    route_slot(cluster, slot, node_a)

    assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "hello"}
  end

  test "bounds an endless ASK redirection loop", %{cluster: cluster} do
    slot = Hash.hash_slot("x")

    # Two nodes that bounce ASK back and forth forever. Both must handle ASKING
    # since each becomes the other's ASK target after the first hop.
    {listen_a, node_a} = listen(cluster)
    {listen_b, node_b} = listen(cluster)

    serve(listen_a, fn
      ["ASKING"] -> "+OK\r\n"
      ["GET", _] -> "-ASK #{slot} #{node_b}\r\n"
    end)

    serve(listen_b, fn
      ["ASKING"] -> "+OK\r\n"
      ["GET", _] -> "-ASK #{slot} #{node_a}\r\n"
    end)

    route_slot(cluster, slot, node_a)

    assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
             {:error, %Redix.ConnectionError{reason: :too_many_redirections}}
  end

  # Reproduces issue #306: the cluster code splits a "host:port" node id on every
  # colon and matches `[host, port_str]`, which blows up on IPv6 addresses. Redis
  # emits MOVED/ASK targets unbracketed (e.g. "MOVED 866 ::1:7000"), so following
  # such a redirect raises a MatchError in Redix.Cluster.parse_redirection/1 and
  # crashes the caller. Here the slot owner redirects to a real IPv6 node, which
  # the redirection machinery should follow to completion instead of crashing.
  @tag :ipv6
  test "follows a MOVED redirect to an IPv6 node", %{cluster: cluster} do
    slot = Hash.hash_slot("x")

    {_listen_c, node_c} = listen_ipv6(cluster, fn ["GET", _] -> "$5\r\nhello\r\n" end)
    node_a = start_node(cluster, fn ["GET", _] -> "-MOVED #{slot} #{node_c}\r\n" end)

    route_slot(cluster, slot, node_a)

    assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "hello"}
  end

  # Reproduces issue #319: the classic ASK scenario is a slot migrating to a
  # brand-new node. A node serving zero slots doesn't appear in CLUSTER SLOTS at
  # all, so the cluster has no connection to it and no topology refresh will
  # create one — the redirection code must connect on demand (as it already does
  # for MOVED) instead of failing every ASK-redirected command until the first
  # migration completes. On-demand connects go through the Manager, so unlike
  # the tests above this one boots a real Redix.Cluster against a fake node
  # that claims all slots.
  test "follows an ASK to a brand-new node that isn't in the topology yet" do
    cluster = :"ask_new_node_#{System.unique_integer([:positive])}"
    slot = Hash.hash_slot("x")

    # The brand-new node receiving the migrating slot.
    {listen_new, node_new} = raw_listen()

    serve(listen_new, fn
      ["ASKING"] -> "+OK\r\n"
      ["GET", _] -> "$3\r\nbar\r\n"
    end)

    # The slot owner: answers the topology fetch claiming every slot, then ASKs
    # the command onward to the new node.
    {listen_owner, node_owner} = raw_listen()

    serve(listen_owner, fn
      ["CLUSTER", "SLOTS"] -> cluster_slots_reply(node_owner)
      ["GET", _] -> "-ASK #{slot} #{node_new}\r\n"
      _other -> "+OK\r\n"
    end)

    start_supervised!(
      {Redix.Cluster, name: cluster, nodes: ["redis://#{node_owner}"], sync_connect: true}
    )

    assert Redix.Cluster.command(cluster, ["GET", "x"]) == {:ok, "bar"}
  end

  # Reproduces issue #325: redirect messages are server-controlled input, so a
  # malformed MOVED/ASK from a buggy or hostile server must come back to the
  # caller as a plain Redis error instead of crashing the calling process with
  # a parse error.
  test "returns malformed redirects to the caller as plain errors", %{cluster: cluster} do
    slot = Hash.hash_slot("x")

    malformed_redirects = [
      "MOVED garbage",
      "MOVED 12x 127.0.0.1:7000",
      "MOVED 99999 127.0.0.1:7000",
      "MOVED #{slot} 127.0.0.1",
      "MOVED #{slot} :7000",
      "MOVED #{slot} 127.0.0.1:no_port",
      "ASK garbage",
      "ASK #{slot} 127.0.0.1:no_port"
    ]

    for message <- malformed_redirects do
      node = start_node(cluster, fn ["GET", _] -> "-#{message}\r\n" end)
      route_slot(cluster, slot, node)

      assert Redix.Cluster.command(cluster, ["GET", "x"]) ==
               {:error, %Redix.Error{message: message}}
    end
  end

  # Reproduces issue #321: a transaction queued on a stale primary (e.g. right
  # after a failover) is rejected with MOVED at queue time, aborting EXEC with
  # EXECABORT. Since a cluster transaction targets a single slot, the whole
  # MULTI/EXEC is re-run at the redirect target.
  test "follows a MOVED redirect for a transaction by re-running it at the target", %{
    cluster: cluster
  } do
    slot = Hash.hash_slot("x")

    node_b =
      start_node(cluster, fn
        ["MULTI"] -> "+OK\r\n"
        ["SET", _, _] -> "+QUEUED\r\n"
        ["EXEC"] -> "*1\r\n+OK\r\n"
      end)

    node_a =
      start_node(cluster, fn
        ["MULTI"] -> "+OK\r\n"
        ["SET", _, _] -> "-MOVED #{slot} #{node_b}\r\n"
        ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
      end)

    route_slot(cluster, slot, node_a)

    assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) == {:ok, ["OK"]}
  end

  test "follows an ASK redirect for a transaction with an ASKING-prefixed re-run", %{
    cluster: cluster
  } do
    slot = Hash.hash_slot("x")

    # The importing node accepts a single ASKING that flags the whole MULTI.
    node_b =
      start_node(cluster, fn
        ["ASKING"] -> "+OK\r\n"
        ["MULTI"] -> "+OK\r\n"
        ["SET", _, _] -> "+QUEUED\r\n"
        ["EXEC"] -> "*1\r\n+OK\r\n"
      end)

    node_a =
      start_node(cluster, fn
        ["MULTI"] -> "+OK\r\n"
        ["SET", _, _] -> "-ASK #{slot} #{node_b}\r\n"
        ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
      end)

    route_slot(cluster, slot, node_a)

    assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) == {:ok, ["OK"]}
  end

  test "bounds an endless MOVED redirection loop for a transaction", %{cluster: cluster} do
    slot = Hash.hash_slot("x")

    {listen_a, node_a} = listen(cluster)
    {listen_b, node_b} = listen(cluster)

    serve(listen_a, fn
      ["MULTI"] -> "+OK\r\n"
      ["SET", _, _] -> "-MOVED #{slot} #{node_b}\r\n"
      ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
    end)

    serve(listen_b, fn
      ["MULTI"] -> "+OK\r\n"
      ["SET", _, _] -> "-MOVED #{slot} #{node_a}\r\n"
      ["EXEC"] -> "-EXECABORT Transaction discarded because of previous errors.\r\n"
    end)

    route_slot(cluster, slot, node_a)

    assert Redix.Cluster.transaction_pipeline(cluster, [["SET", "x", "1"]]) ==
             {:error, %Redix.ConnectionError{reason: :too_many_redirections}}
  end

  ## Helpers

  defp route_slot(cluster, slot, node_id) do
    :ets.insert(:"#{cluster}_slots", {slot, node_id, _replicas = []})
  end

  # Starts a fake node and a Redix connection to it, registered under its node_id
  # so the redirection code can resolve it. Returns the "host:port" node_id.
  defp start_node(cluster, handler) do
    {listen, node_id} = listen(cluster)
    serve(listen, handler)
    node_id
  end

  # Like listen/1, but without starting (or registering) a connection to the
  # node — for fake nodes the cluster must discover or connect to on its own.
  defp raw_listen do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)
    {listen, "127.0.0.1:#{port}"}
  end

  # Encodes a CLUSTER SLOTS reply with all 16384 slots owned by a single
  # primary and no replicas: `[[0, 16383, [host, port, node_id]]]`.
  defp cluster_slots_reply(node_id) do
    [host, port] = String.split(node_id, ":")

    "*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n" <>
      "$#{byte_size(host)}\r\n#{host}\r\n:#{port}\r\n$40\r\n#{String.duplicate("a", 40)}\r\n"
  end

  defp listen(cluster) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)
    node_id = "127.0.0.1:#{port}"

    start_supervised!(
      {Redix,
       host: "127.0.0.1",
       port: port,
       sync_connect: true,
       name: {:via, Registry, {:"#{cluster}_registry", node_id}}},
      id: {:conn, node_id}
    )

    {listen, node_id}
  end

  # Like listen/1 + serve/2, but binds the fake node on the IPv6 loopback. The
  # node id is the unbracketed "::1:port" form that Redis uses in MOVED/ASK
  # replies, which is exactly what trips up the colon-splitting parser (#306).
  defp listen_ipv6(cluster, handler) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, :inet6, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)
    node_id = "::1:#{port}"

    start_supervised!(
      {Redix,
       host: "::1",
       port: port,
       socket_opts: [:inet6],
       sync_connect: true,
       name: {:via, Registry, {:"#{cluster}_registry", node_id}}},
      id: {:conn, node_id}
    )

    serve(listen, handler)
    {listen, node_id}
  end

  defp serve(listen, handler) do
    spawn_link(fn -> accept_loop(listen, handler) end)
  end

  # Accepts connections until the listen socket closes (when the owning test
  # exits) or no client shows up for 5s. Some fake nodes get more than one
  # connection — e.g. the Manager's transient topology-fetch socket plus the
  # managed Redix connection.
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

  # Minimal RESP request parser: pulls as many complete `*N\r\n$len\r\n...`
  # commands as the buffer holds, returning the parsed commands and any leftover.
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
end
