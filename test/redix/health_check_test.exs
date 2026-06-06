defmodule Redix.HealthCheckTest do
  use ExUnit.Case, async: true

  alias Redix.ConnectionError

  # These tests use a fully controllable fake TCP server (no Docker needed) to
  # reproduce a *half-open* connection: the server accepts the socket and stays
  # connected, but never replies to commands. This is what happens during a
  # Sentinel failover when the old primary is paused (e.g. `CLIENT PAUSE`) — the
  # socket stays open, commands time out, and without a health check Redix stays
  # wedged on the dead primary instead of reconnecting and re-resolving it.
  # See https://github.com/whatyouhide/redix/issues/287.

  setup do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    on_exit(fn -> :gen_tcp.close(listen) end)
    %{listen: listen, port: port}
  end

  test "reconnects when an in-flight command goes unanswered for the interval", %{
    listen: listen,
    port: port
  } do
    # A server that accepts connections but never replies to any command.
    accept_loop(listen)

    {test_name, _arity} = __ENV__.function
    parent = self()

    handler = fn event, _measurements, meta, _config ->
      if meta.connection_name == test_name, do: send(parent, {event, meta})
    end

    :ok =
      :telemetry.attach_many(
        to_string(test_name),
        [[:redix, :disconnection], [:redix, :connection]],
        handler,
        :no_config
      )

    on_exit(fn -> :telemetry.detach(to_string(test_name)) end)

    conn =
      start_supervised!(
        {Redix,
         host: "127.0.0.1",
         port: port,
         name: test_name,
         sync_connect: true,
         health_check_interval: 100}
      )

    # The command never gets a reply; the caller times out...
    assert {:error, %ConnectionError{reason: :timeout}} =
             Redix.command(conn, ["GET", "foo"], timeout: 50)

    # ...but unlike before, the *connection* now notices the stalled command and
    # tears itself down with a :health_check_timeout reason, then reconnects.
    assert_receive {[:redix, :disconnection], meta}, 1000
    assert %ConnectionError{reason: :health_check_timeout} = meta.reason

    assert_receive {[:redix, :connection], %{reconnection: true}}, 1000
  end

  test "without a health check, a half-open connection stays wedged (the bug)", %{
    listen: listen,
    port: port
  } do
    accept_loop(listen)

    {test_name, _arity} = __ENV__.function
    parent = self()

    handler = fn _event, _measurements, meta, _config ->
      if meta.connection_name == test_name, do: send(parent, :disconnected)
    end

    :ok = :telemetry.attach(to_string(test_name), [:redix, :disconnection], handler, :no_config)
    on_exit(fn -> :telemetry.detach(to_string(test_name)) end)

    conn =
      start_supervised!(
        {Redix, host: "127.0.0.1", port: port, name: test_name, sync_connect: true}
      )

    assert {:error, %ConnectionError{reason: :timeout}} =
             Redix.command(conn, ["GET", "foo"], timeout: 50)

    # The command timed out, but the connection itself never notices it's wedged:
    # no disconnection, no reconnection. It's pinned to the dead server.
    refute_receive :disconnected, 300
  end

  test "survives a normal disconnection with a health check armed", %{listen: listen, port: port} do
    # The server answers one PING, then closes the socket. With a health-check interval far
    # shorter than the reconnect backoff, the (generic) health-check timer is still armed when
    # we drop into the disconnected/connecting states and will fire there. The connection must
    # ignore it rather than crash. Regression guard for the leftover-timer case.
    pinged = self()

    accept_loop(listen, fn ["PING"] ->
      send(pinged, :got_ping)
      :close
    end)

    conn =
      start_supervised!(
        {Redix,
         host: "127.0.0.1",
         port: port,
         sync_connect: true,
         health_check_interval: 30,
         backoff_initial: 500,
         backoff_max: 500}
      )

    # Trigger the close; the caller sees the disconnection.
    Redix.command(conn, ["PING"], timeout: 200)
    assert_receive :got_ping, 500

    # Let the leftover health-check timer fire during the backoff window, then confirm the
    # connection process is still alive (it didn't crash on the stray timeout).
    Process.sleep(200)
    assert Process.alive?(GenServer.whereis(conn))
  end

  test "does not tear down a healthy idle connection", %{listen: listen, port: port} do
    # A server that answers PING with PONG and otherwise stays connected.
    accept_loop(listen, fn
      ["PING"] -> "+PONG\r\n"
      _ -> "+OK\r\n"
    end)

    {test_name, _arity} = __ENV__.function
    parent = self()

    handler = fn _event, _measurements, meta, _config ->
      if meta.connection_name == test_name, do: send(parent, :disconnected)
    end

    :ok = :telemetry.attach(to_string(test_name), [:redix, :disconnection], handler, :no_config)
    on_exit(fn -> :telemetry.detach(to_string(test_name)) end)

    conn =
      start_supervised!(
        {Redix,
         host: "127.0.0.1",
         port: port,
         name: test_name,
         sync_connect: true,
         health_check_interval: 50}
      )

    assert Redix.command!(conn, ["PING"]) == "PONG"

    # The connection sits idle across several health-check intervals and must not
    # be torn down — the check only fires on *unanswered in-flight* commands.
    refute_receive :disconnected, 300
    assert Redix.command!(conn, ["PING"]) == "PONG"
  end

  # Accepts one connection and serves it. With no handler, never replies (the
  # half-open case). With a handler, replies to each parsed command.
  defp accept_loop(listen, handler \\ nil) do
    test_pid = self()

    spawn_link(fn ->
      case :gen_tcp.accept(listen, 5_000) do
        {:ok, socket} -> serve(socket, handler, "")
        {:error, _reason} -> Process.unlink(test_pid)
      end
    end)

    :ok
  end

  defp serve(socket, handler, buffer) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} ->
        buffer = buffer <> data

        if handler do
          {commands, rest} = parse_commands(buffer, [])

          if Enum.any?(commands, &(handler.(&1) == :close)) do
            # A handler can ask us to drop the connection after replying to the
            # commands before the one that returned :close.
            Enum.each(commands, fn command ->
              case handler.(command) do
                :close -> :gen_tcp.close(socket)
                reply -> :gen_tcp.send(socket, reply)
              end
            end)
          else
            Enum.each(commands, &:gen_tcp.send(socket, handler.(&1)))
            serve(socket, handler, rest)
          end
        else
          # Half-open: read and discard, never reply.
          serve(socket, handler, "")
        end

      {:error, _reason} ->
        :ok
    end
  end

  defp parse_commands(buffer, acc) do
    case parse_command(buffer) do
      {:ok, command, rest} -> parse_commands(rest, [command | acc])
      :incomplete -> {Enum.reverse(acc), buffer}
    end
  end

  defp parse_command("*" <> rest) do
    case Integer.parse(rest) do
      {n, "\r\n" <> rest} -> parse_args(rest, n, [])
      _ -> :incomplete
    end
  end

  defp parse_command(_other), do: :incomplete

  defp parse_args(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp parse_args("$" <> rest, n, acc) do
    case Integer.parse(rest) do
      {len, "\r\n" <> rest} when byte_size(rest) >= len + 2 ->
        <<arg::binary-size(^len), "\r\n", rest::binary>> = rest
        parse_args(rest, n - 1, [arg | acc])

      _ ->
        :incomplete
    end
  end

  defp parse_args(_rest, _n, _acc), do: :incomplete
end
