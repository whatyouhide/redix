defmodule Redix.SentinelTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

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
    {:ok, primary} = Redix.start_link(sentinel: sentinel_config, sync_connect: true)

    assert Redix.command!(primary, ["PING"]) == "PONG"
    assert Redix.command!(primary, ["CONFIG", "GET", "port"]) == ["port", "6381"]
    assert ["master", _, _] = Redix.command!(primary, ["ROLE"])
  end

  test "connection can select replica", %{sentinel_config: sentinel_config} do
    sentinel_config = Keyword.put(sentinel_config, :role, :replica)
    {:ok, replica} = Redix.start_link(sentinel: sentinel_config, sync_connect: true, timeout: 500)

    assert Redix.command!(replica, ["PING"]) == "PONG"
    assert Redix.command!(replica, ["CONFIG", "GET", "port"]) == ["port", "6382"]
    assert ["slave" | _] = Redix.command!(replica, ["ROLE"])
  end

  test "Redix.PubSub supports sentinel as well", %{sentinel_config: sentinel_config} do
    {:ok, primary} = Redix.start_link(sentinel: sentinel_config, sync_connect: true)
    {:ok, pubsub} = Redix.PubSub.start_link(sentinel: sentinel_config, sync_connect: true)

    {:ok, ref} = Redix.PubSub.subscribe(pubsub, "foo", self())

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    Redix.command!(primary, ["PUBLISH", "foo", "hello"])

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}}
  end

  test "when no sentinels are reachable" do
    Process.flag(:trap_exit, true)

    log =
      capture_log(fn ->
        {:ok, conn} =
          Redix.start_link(
            sentinel: [sentinels: ["redis://nonexistent:9999"], group: "main"],
            exit_on_disconnection: true
          )

        assert_receive {:EXIT, ^conn,
                        %Redix.ConnectionError{reason: :no_viable_sentinel_connection}}
      end)

    assert log =~ "Couldn't connect to primary through nonexistent:9999: :nxdomain"
  end
end
