defmodule Redix.SentinelTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  @sentinel_config [
    sentinels: [{"localhost", 26379}, {"localhost", 26380}, {"localhost", 26381}],
    group: "main"
  ]

  test "connection can select primary" do
    {:ok, primary} = Redix.start_link(sentinel: @sentinel_config, sync_connect: true)

    assert Redix.command!(primary, ["PING"]) == "PONG"
    assert Redix.command!(primary, ["CONFIG", "GET", "port"]) == ["port", "6381"]
    assert ["master", _, _] = Redix.command!(primary, ["ROLE"])
  end

  test "Redix.PubSub supports sentinel as well" do
    {:ok, primary} = Redix.start_link(sentinel: @sentinel_config, sync_connect: true)
    {:ok, pubsub} = Redix.PubSub.start_link(sentinel: @sentinel_config, sync_connect: true)

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
            sentinel: [sentinels: [{"nonexistent", 9999}], group: "main"],
            exit_on_disconnection: true
          )

        assert_receive {:EXIT, ^conn, %Redix.ConnectionError{reason: :no_sentinel_replied}}
      end)

    assert log =~ "Couldn't connect to a primary through {'nonexistent', 9999}: :nxdomain"
  end
end
