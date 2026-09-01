defmodule Redix.ConnectionAvailabilityTest do
  use ExUnit.Case, async: true

  import Redix.Cluster.FakeNode, only: [wait_until: 1]

  alias Redix.Cluster.FakeNode

  test "standalone connections with an atom name and no name reconnect normally" do
    node =
      FakeNode.start(fn
        ["PING"] -> "+PONG\r\n"
        _other -> "+OK\r\n"
      end)

    name = :"standalone_reconnect_#{System.unique_integer([:positive])}"

    for {id, name_opts} <- [{:unnamed, []}, {:named, [name: name]}] do
      opts =
        [
          host: node.host,
          port: node.port,
          sync_connect: true,
          backoff_initial: 200
        ] ++ name_opts

      conn = start_supervised!(Supervisor.child_spec({Redix, opts}, id: {id, name}))
      assert Redix.command(conn, ["PING"]) == {:ok, "PONG"}

      {:connected, data} = :sys.get_state(conn)
      send(data.socket_owner, {:force_disconnect, conn, :test_socket_drop})

      wait_until(fn -> match?({:disconnected, _data}, :sys.get_state(conn)) end)
      assert Process.alive?(conn)

      wait_until(fn -> match?({:connected, _data}, :sys.get_state(conn)) end)
      assert Redix.command(conn, ["PING"]) == {:ok, "PONG"}
    end
  end
end
