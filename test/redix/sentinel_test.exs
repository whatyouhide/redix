defmodule Redix.SentinelTest do
  use ExUnit.Case, async: true

  @sentinels [
    "redis://localhost:26379",
    "redis://localhost:26380",
    [host: "localhost", port: 26381]
  ]

  setup do
    sentinel_config = [
      sentinels: Enum.shuffle(@sentinels),
      group: "main",
      timeout: 500
    ]

    %{sentinel_config: sentinel_config}
  end

  test "connection can select primary", %{sentinel_config: sentinel_config} do
    primary = start_supervised!({Redix, sentinel: sentinel_config, sync_connect: true})

    assert Redix.command!(primary, ["PING"]) == "PONG"
    assert Redix.command!(primary, ["CONFIG", "GET", "port"]) == ["port", "6381"]
    assert ["master", _, _] = Redix.command!(primary, ["ROLE"])
  end

  test "connection can select replica", %{sentinel_config: sentinel_config} do
    sentinel_config = Keyword.put(sentinel_config, :role, :replica)

    replica =
      start_supervised!({Redix, sentinel: sentinel_config, sync_connect: true, timeout: 500})

    assert Redix.command!(replica, ["PING"]) == "PONG"
    assert Redix.command!(replica, ["CONFIG", "GET", "port"]) == ["port", "6382"]
    assert ["slave" | _] = Redix.command!(replica, ["ROLE"])
  end

  test "Redix.PubSub supports sentinel as well", %{sentinel_config: sentinel_config} do
    primary = start_supervised!({Redix, sentinel: sentinel_config, sync_connect: true})
    {:ok, pubsub} = Redix.PubSub.start_link(sentinel: sentinel_config, sync_connect: true)

    {:ok, ref} = Redix.PubSub.subscribe(pubsub, "foo", self())

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    Redix.command!(primary, ["PUBLISH", "foo", "hello"])

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}}
  end

  test "when no sentinels are reachable" do
    Process.flag(:trap_exit, true)

    {:ok, conn} =
      Redix.start_link(
        sentinel: [sentinels: ["redis://nonexistent:9999"], group: "main"],
        exit_on_disconnection: true
      )

    assert_receive {:EXIT, ^conn, error}, 10000
    assert %Redix.ConnectionError{reason: :no_viable_sentinel_connection} = error
  end

  test "sentinel supports password", %{sentinel_config: sentinel_config} do
    sentinel_config =
      Keyword.merge(sentinel_config,
        password: "sentinel-password",
        sentinels: ["redis://localhost:26383"]
      )

    pid =
      start_supervised!(
        {Redix, sentinel: sentinel_config, password: "main-password", sync_connect: true}
      )

    assert Redix.command!(pid, ["PING"]) == "PONG"
  end

  test "sentinel supports password mfa in sentinels list", %{sentinel_config: sentinel_config} do
    System.put_env("REDIX_SENTINEL_MFA_PASSWORD", "sentinel-password")

    password_mfa = {System, :get_env, ["REDIX_SENTINEL_MFA_PASSWORD"]}

    sentinel_config =
      Keyword.merge(sentinel_config,
        sentinels: [[host: "localhost", port: 26383, password: password_mfa]]
      )

    pid =
      start_supervised!(
        {Redix, sentinel: sentinel_config, password: "main-password", sync_connect: true}
      )

    assert Redix.command!(pid, ["PING"]) == "PONG"
  after
    System.delete_env("REDIX_SENTINEL_MFA_PASSWORD")
  end

  test "sentinel supports global password mfa", %{sentinel_config: sentinel_config} do
    System.put_env("REDIX_SENTINEL_MFA_PASSWORD", "sentinel-password")

    sentinel_config =
      Keyword.merge(sentinel_config,
        password: {System, :get_env, ["REDIX_SENTINEL_MFA_PASSWORD"]},
        sentinels: ["redis://localhost:26383"]
      )

    pid =
      start_supervised!(
        {Redix, sentinel: sentinel_config, password: "main-password", sync_connect: true}
      )

    assert Redix.command!(pid, ["PING"]) == "PONG"
  after
    System.delete_env("REDIX_SENTINEL_MFA_PASSWORD")
  end

  test "failed sentinel connection" do
    {test_name, _arity} = __ENV__.function

    parent = self()
    ref = make_ref()

    handler = fn event, measurements, meta, _config ->
      if meta.connection_name == :failed_sentinel_telemetry_test do
        send(parent, {ref, event, measurements, meta})
      end
    end

    :ok =
      :telemetry.attach(to_string(test_name), [:redix, :failed_connection], handler, :no_config)

    conn =
      start_supervised!(
        {Redix,
         name: :failed_sentinel_telemetry_test,
         sentinel: [group: "main", sentinels: ["redis://localhost:9999"]]}
      )

    assert_receive {^ref, [:redix, :failed_connection], measurements, meta}
    assert measurements == %{}

    assert meta == %{
             connection: conn,
             connection_name: :failed_sentinel_telemetry_test,
             reason: %Redix.ConnectionError{reason: :econnrefused},
             sentinel_address: "localhost:9999"
           }

    :telemetry.detach(to_string(test_name))
  end
end
