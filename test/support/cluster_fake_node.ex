defmodule Redix.Cluster.FakeNode do
  @moduledoc false

  # A scriptable fake RESP server for Redix Cluster tests.
  #
  # It lets the cluster test suite exercise topology and redirection edge cases
  # that a healthy Docker cluster won't emit on demand (issues #295, #306, #314,
  # #318, #319, #323, #328) without needing the Docker cluster at all. A node
  # listens on an ephemeral loopback port and replies to each parsed RESP command
  # with whatever its `handler` returns — a raw RESP binary. The listener and
  # every accepted connection are `spawn_link`ed to the calling test, so they die
  # with it.
  #
  # ## Single-shot node
  #
  #     node = FakeNode.start(fn ["GET", _] -> "$3\r\nbar\r\n" end)
  #     "#{node}"   # => "127.0.0.1:53123" — the node id (FakeNode implements String.Chars)
  #     node.id     # => same
  #
  # ## Nodes that reference each other (e.g. a redirect chain)
  #
  # Reserve the ports first so each node id is known before any handler is set,
  # then attach handlers:
  #
  #     a = FakeNode.reserve()
  #     b = FakeNode.reserve()
  #     FakeNode.serve(a, fn ["GET", _] -> "-ASK 0 #{b}\r\n" end)
  #     FakeNode.serve(b, fn ["GET", _] -> "$3\r\nbar\r\n" end)
  #
  # ## Wiring a real connection (redirection tests)
  #
  # `connect/2` registers a supervised `Redix` connection under the node id in a
  # cluster's registry, so redirection code that looks the node up by id resolves
  # it. `start_connected/3` does reserve + connect + serve in one step.
  #
  # ## Simulating an unreachable node
  #
  # A node starts `:up`. `set_status(node, :down)` makes it close accepted
  # connections immediately (so a topology fetch fails at the socket level);
  # flipping back to `:up` resumes serving. Pass `status: :down` to `reserve/1`
  # to start down.

  alias Redix.Cluster.FakeNode

  @accept_timeout 10_000
  @recv_timeout 10_000

  @enforce_keys [:id, :host, :port, :listen, :inet6, :status]
  defstruct [:id, :host, :port, :listen, :inet6, :status]

  @type t :: %__MODULE__{
          id: String.t(),
          host: String.t(),
          port: :inet.port_number(),
          listen: :gen_tcp.socket(),
          inet6: boolean(),
          status: pid()
        }

  @type handler :: ([binary()] -> iodata())

  ## Lifecycle

  # Reserve an ephemeral loopback port and node id without serving yet. Pass
  # `inet6: true` to bind the IPv6 loopback — the node id is then the unbracketed
  # "::1:port" form Redis uses in MOVED/ASK replies (issue #306). Pass
  # `status: :down` to start unreachable (see `set_status/2`).
  @spec reserve(keyword()) :: t()
  def reserve(opts \\ []) do
    inet6? = Keyword.get(opts, :inet6, false)
    {host, family_opts} = if inet6?, do: {"::1", [:inet6]}, else: {"127.0.0.1", []}

    {:ok, listen} =
      :gen_tcp.listen(
        0,
        [:binary, active: false, reuseaddr: true, packet: :raw] ++ family_opts
      )

    {:ok, port} = :inet.port(listen)
    {:ok, status} = Agent.start_link(fn -> Keyword.get(opts, :status, :up) end)

    %FakeNode{
      id: "#{host}:#{port}",
      host: host,
      port: port,
      listen: listen,
      inet6: inet6?,
      status: status
    }
  end

  # Attach a handler to a reserved node and start accepting connections. The
  # handler maps a parsed command (a list of binaries) to a raw RESP reply.
  # Returns the node so it can be threaded through a pipe.
  @spec serve(t(), handler()) :: t()
  def serve(%FakeNode{} = node, handler) when is_function(handler, 1) do
    spawn_link(fn -> accept_loop(node, handler) end)
    node
  end

  # Reserve + serve in one step, for a node whose id no one needs in advance.
  @spec start(handler(), keyword()) :: t()
  def start(handler, opts \\ []) when is_function(handler, 1) do
    opts |> reserve() |> serve(handler)
  end

  # Register a supervised `Redix` connection to the node under its id in
  # `cluster`'s registry, so redirection code can resolve it by id. Must be
  # called from the test process (uses `start_supervised!`). Returns the node.
  @spec connect(t(), atom()) :: t()
  def connect(%FakeNode{} = node, cluster) do
    socket_opts = if node.inet6, do: [socket_opts: [:inet6]], else: []

    ExUnit.Callbacks.start_supervised!(
      {Redix,
       [
         host: node.host,
         port: node.port,
         sync_connect: true,
         name: {:via, Registry, {:"#{cluster}_registry", node.id}}
       ] ++ socket_opts},
      id: {:conn, node.id}
    )

    node
  end

  # reserve + connect + serve, for a fully wired node (redirection tests).
  @spec start_connected(atom(), handler(), keyword()) :: t()
  def start_connected(cluster, handler, opts \\ []) when is_function(handler, 1) do
    opts |> reserve() |> connect(cluster) |> serve(handler)
  end

  # Flip a node between serving (`:up`) and closing connections immediately
  # (`:down`), to simulate a node going down/up under a running test.
  @spec set_status(t(), :up | :down) :: :ok
  def set_status(%FakeNode{status: status}, value) when value in [:up, :down] do
    Agent.update(status, fn _ -> value end)
  end

  ## CLUSTER SLOTS encoding

  # Encode a `CLUSTER SLOTS` reply. Each range is `{start, stop, primary}` or
  # `{start, stop, primary, replicas}`, where a node is a `FakeNode`, a
  # "host:port" string, or a `{host_or_nil, port}` tuple. A `nil` host is encoded
  # as a RESP null bulk string, like Redis 7+ does with
  # "cluster-preferred-endpoint-type unknown-endpoint" (issue #328). Mirrors the
  # real RESP shape: an array of ranges, each `[start, stop, [host, port, id],
  # [replica_host, replica_port, id]...]`.
  @spec cluster_slots([tuple()]) :: binary()
  def cluster_slots(ranges) do
    body =
      Enum.map(ranges, fn range ->
        {start, stop, primary, replicas} =
          case range do
            {start, stop, primary} -> {start, stop, primary, []}
            {start, stop, primary, replicas} -> {start, stop, primary, replicas}
          end

        nodes = [primary | replicas]

        [
          "*#{2 + length(nodes)}\r\n",
          ":#{start}\r\n",
          ":#{stop}\r\n",
          Enum.map(nodes, &encode_node/1)
        ]
      end)

    IO.iodata_to_binary(["*#{length(ranges)}\r\n", body])
  end

  defp encode_node(node) do
    {host, port} = host_port(node)

    [
      "*3\r\n",
      encode_host(host),
      ":#{port}\r\n",
      "$40\r\n",
      String.duplicate("a", 40),
      "\r\n"
    ]
  end

  defp host_port(%FakeNode{host: host, port: port}), do: {host, port}
  defp host_port({_host_or_nil, _port} = host_port), do: host_port

  defp host_port(node_id) when is_binary(node_id) do
    [host, port] = String.split(node_id, ":")
    {host, String.to_integer(port)}
  end

  defp encode_host(nil), do: "$-1\r\n"
  defp encode_host(host), do: ["$#{byte_size(host)}\r\n", host, "\r\n"]

  ## Polling helper

  # Poll `fun` until it returns truthy or the timeout elapses (then flunk). Used
  # by the fake-node tests to wait for the cluster to converge after a topology
  # change, since reactive refreshes are async.
  @spec wait_until((-> as_boolean(term())), timeout()) :: :ok
  def wait_until(fun, timeout \\ 2_000)

  def wait_until(_fun, timeout) when timeout <= 0 do
    ExUnit.Assertions.flunk("condition not met in time")
  end

  def wait_until(fun, timeout) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, timeout - 20)
    end
  end

  ## Accept / serve loop

  # Accepts connections until the listen socket closes (when the owning test
  # exits) or no client shows up before the timeout. Some fake nodes get more
  # than one connection — e.g. the Manager's transient topology-fetch socket plus
  # the managed Redix connection. While the node is `:down`, accepted connections
  # are closed immediately to simulate an unreachable node.
  defp accept_loop(node, handler) do
    case :gen_tcp.accept(node.listen, @accept_timeout) do
      {:ok, socket} ->
        case Agent.get(node.status, & &1) do
          :up -> spawn_link(fn -> loop(socket, handler, "") end)
          :down -> :gen_tcp.close(socket)
        end

        accept_loop(node, handler)

      {:error, _reason} ->
        :ok
    end
  end

  defp loop(socket, handler, buffer) do
    case :gen_tcp.recv(socket, 0, @recv_timeout) do
      {:ok, data} ->
        {commands, rest} = parse_commands(buffer <> data, [])
        Enum.each(commands, &:gen_tcp.send(socket, handler.(&1)))
        loop(socket, handler, rest)

      {:error, _reason} ->
        :ok
    end
  end

  ## Minimal RESP request parser

  # Pulls as many complete `*N\r\n$len\r\n...` commands as the buffer holds,
  # returning the parsed commands and any leftover bytes.
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

defimpl String.Chars, for: Redix.Cluster.FakeNode do
  def to_string(%Redix.Cluster.FakeNode{id: id}), do: id
end
