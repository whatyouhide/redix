defmodule Redix.Cluster.ConnectionTelemetryTest do
  use ExUnit.Case, async: true

  alias Redix.Cluster.FakeNode

  setup do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:redix, :connection],
        [:redix, :disconnection],
        [:redix, :failed_connection]
      ])

    on_exit(fn -> :telemetry.detach(ref) end)

    :ok
  end

  @tag :capture_log
  test "primary and replica events keep the cluster name across reconnects and restarts" do
    cluster = :"connection_telemetry_#{System.unique_integer([:positive])}"
    primary = FakeNode.reserve()
    replica = FakeNode.start(fn _command -> "+OK\r\n" end)

    FakeNode.serve(primary, fn
      ["CLUSTER", "SLOTS"] -> FakeNode.cluster_slots([{0, 16_383, primary, [replica]}])
      _command -> "+OK\r\n"
    end)

    start_supervised!(
      {Redix.Cluster,
       name: cluster,
       nodes: ["redis://#{primary}"],
       password: "secret",
       read_from_replicas: true,
       primary_pool_size: 1,
       replica_pool_size: 1,
       backoff_initial: 10,
       backoff_max: 10,
       sync_connect: true}
    )

    registry = :"#{cluster}_registry"

    for {node, role} <- [{primary, :primary}, {replica, :replica}] do
      address = node.id

      assert_receive {[:redix, :connection], _ref, %{},
                      %{
                        cluster: ^cluster,
                        address: ^address,
                        connection: conn,
                        reconnection: false
                      }}

      assert Redix.command(conn, ["PING"]) == {:ok, "OK"}
      assert [{^conn, {^role, :connected}}] = Registry.lookup(registry, {node.id, 0})

      FakeNode.set_status(node, :down)
      {:connected, data} = :sys.get_state(conn)
      send(data.socket_owner, {:force_disconnect, conn, :test_socket_drop})

      assert_receive {[:redix, :disconnection], _ref, %{},
                      %{cluster: ^cluster, connection: ^conn, address: ^address}}

      assert_receive {[:redix, :failed_connection], _ref, %{},
                      %{cluster: ^cluster, connection: ^conn, address: ^address}}

      FakeNode.set_status(node, :up)

      assert_receive {[:redix, :connection], _ref, %{},
                      %{cluster: ^cluster, connection: ^conn, reconnection: true}}

      Process.exit(conn, :kill)

      assert_receive {[:redix, :connection], _ref, %{},
                      %{
                        cluster: ^cluster,
                        address: ^address,
                        connection: replacement,
                        reconnection: false
                      }}

      assert replacement != conn
      assert Redix.command(replacement, ["PING"]) == {:ok, "OK"}
      assert [{^replacement, {^role, :connected}}] = Registry.lookup(registry, {node.id, 0})
    end
  end

  for sync_connect <- [true, false] do
    test "standalone events omit the cluster name with sync_connect: #{sync_connect}" do
      node = FakeNode.start(fn _command -> "+OK\r\n" end)

      conn =
        start_supervised!(
          {Redix,
           host: node.host,
           port: node.port,
           password: "secret",
           backoff_initial: 10,
           sync_connect: unquote(sync_connect)}
        )

      assert_receive {[:redix, :connection], _ref, %{}, %{connection: ^conn} = metadata}
      refute Map.has_key?(metadata, :cluster)

      FakeNode.set_status(node, :down)
      {:connected, data} = :sys.get_state(conn)
      send(data.socket_owner, {:force_disconnect, conn, :test_socket_drop})

      assert_receive {[:redix, :disconnection], _ref, %{}, %{connection: ^conn} = metadata}
      refute Map.has_key?(metadata, :cluster)

      assert_receive {[:redix, :failed_connection], _ref, %{}, %{connection: ^conn} = metadata}
      refute Map.has_key?(metadata, :cluster)
    end
  end
end
