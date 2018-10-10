defmodule Redix.PubSubTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Redix.{ConnectionError, PubSub}

  setup do
    {:ok, pubsub} = PubSub.start_link()
    {:ok, conn} = Redix.start_link()
    {:ok, %{pubsub: pubsub, conn: conn}}
  end

  test "subscribe/unsubscribe flow", %{pubsub: pubsub, conn: conn} do
    # First, we subscribe.
    assert {:ok, ref} = PubSub.subscribe(pubsub, ["foo", "bar"], self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "bar"}}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["foo", "bar"])
    end)

    # Then, we test messages are routed correctly.
    Redix.command!(conn, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}}
    Redix.command!(conn, ~w(PUBLISH bar world))
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "bar", payload: "world"}}

    # Then, we unsubscribe.
    assert PubSub.unsubscribe(pubsub, ["foo"], self()) == :ok
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :unsubscribed, %{channel: "foo"}}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["bar"])
    end)

    # And finally, we test that we don't receive messages anymore for
    # unsubscribed channels, but we do for subscribed channels.
    Redix.command!(conn, ~w(PUBLISH foo hello))
    refute_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}}
    Redix.command!(conn, ~w(PUBLISH bar world))
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "bar", payload: "world"}}

    # We check we didn't leak messages.
    refute_receive _any
  end

  test "psubscribe/punsubscribe flow", %{pubsub: pubsub, conn: conn} do
    assert {:ok, ref} = PubSub.psubscribe(pubsub, ["foo*", "ba?"], self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :psubscribed, %{pattern: "foo*"}}
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :psubscribed, %{pattern: "ba?"}}

    Redix.pipeline!(conn, [
      ~w(PUBLISH foo_1 foo_1),
      ~w(PUBLISH foo_2 foo_2),
      ~w(PUBLISH bar bar),
      ~w(PUBLISH barfoo barfoo)
    ])

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :pmessage,
                    %{payload: "foo_1", channel: "foo_1", pattern: "foo*"}}

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :pmessage,
                    %{payload: "foo_2", channel: "foo_2", pattern: "foo*"}}

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :pmessage,
                    %{payload: "bar", channel: "bar", pattern: "ba?"}}

    refute_receive {:redix_pubsub, ^pubsub, ^ref, :pmessage, %{payload: "barfoo"}}

    PubSub.punsubscribe(pubsub, "foo*", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :punsubscribed, %{pattern: "foo*"}}

    Redix.pipeline!(conn, [~w(PUBLISH foo_x foo_x), ~w(PUBLISH baz baz)])

    refute_receive {:redix_pubsub, ^pubsub, ^ref, :pmessage, %{payload: "foo_x"}}

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :pmessage,
                    %{payload: "baz", channel: "baz", pattern: "ba?"}}
  end

  test "subscribing the same pid to the same channel more than once has no effect",
       %{pubsub: pubsub, conn: conn} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert {:ok, ^ref} = PubSub.subscribe(pubsub, "foo", self())

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["foo"])
    end)

    Redix.command!(conn, ~w(PUBLISH foo hello))

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}}
    refute_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}}
  end

  test "pubsub: unsubscribing a recipient doesn't affect other recipients",
       %{pubsub: pubsub, conn: conn} do
    channel = "foo"
    parent = self()
    mirror = spawn_link(fn -> message_mirror(parent) end)

    # Let's subscribe two different pids to the same channel.
    assert {:ok, ref} = PubSub.subscribe(pubsub, channel, self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, _properties}
    assert {:ok, mirror_ref} = PubSub.subscribe(pubsub, channel, mirror)
    assert_receive {^mirror, {:redix_pubsub, ^pubsub, ^mirror_ref, :subscribed, _properties}}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["foo"])
    end)

    # Let's ensure both those pids receive messages published on that channel.
    Redix.command!(conn, ["PUBLISH", channel, "hello"])
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{payload: "hello"}}
    assert_receive {^mirror, {:redix_pubsub, ^pubsub, ^mirror_ref, :message, %{payload: "hello"}}}

    # Now let's unsubscribe just one pid from that channel.
    PubSub.unsubscribe(pubsub, channel, self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :unsubscribed, %{channel: ^channel}}

    refute_receive {^mirror,
                    {:redix_pubsub, ^pubsub, ^mirror_ref, :unsubscribed, %{channel: ^channel}}}

    # The connection is still connected to the channel.
    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["foo"])
    end)

    # Publishing now should send a message to the non-unsubscribed pid.
    Redix.command!(conn, ["PUBLISH", channel, "hello"])
    refute_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{payload: "hello"}}
    assert_receive {^mirror, {:redix_pubsub, ^pubsub, ^mirror_ref, :message, %{payload: "hello"}}}
  end

  test "after unsubscribing from a channel, resubscribing one recipient resubscribes correctly",
       %{pubsub: pubsub, conn: conn} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, _properties}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["foo"])
    end)

    Redix.command!(conn, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{payload: "hello"}}

    assert :ok = PubSub.unsubscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :unsubscribed, _properties}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new()
    end)

    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, _properties}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["foo"])
    end)

    Redix.command!(conn, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{payload: "hello"}}
  end

  test "recipients are monitored and the connection unsubcribes when they go down",
       %{pubsub: pubsub, conn: conn} do
    parent = self()
    mirror = spawn(fn -> message_mirror(parent) end)

    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", mirror)
    assert_receive {^mirror, {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}}

    # Before the mirror process goes down, we're subscribed to the channel.
    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new(["foo"])
    end)

    # Let's just ensure no errors happen when we kill the recipient.
    Process.exit(mirror, :kill)

    # Since the only subscribed went down, we unsubscribe from the channel.
    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new()
    end)

    Process.sleep(100)
  end

  test "disconnections/reconnections", %{pubsub: pubsub, conn: conn} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    capture_log(fn ->
      Redix.command!(conn, ~w(CLIENT KILL TYPE pubsub))

      assert_receive {:redix_pubsub, ^pubsub, ^ref, :disconnected,
                      %{error: %Redix.ConnectionError{}}}

      assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}, 1000
    end)

    Redix.command!(conn, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}}
  end

  test ":exit_on_disconnection option", %{conn: conn} do
    {:ok, pubsub} = PubSub.start_link(exit_on_disconnection: true)

    # We need to subscribe to something so that this client becomes a PubSub
    # client and we can kill it with "CLIENT KILL TYPE pubsub".
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    Process.flag(:trap_exit, true)

    capture_log(fn ->
      Redix.command!(conn, ~w(CLIENT KILL TYPE pubsub))
      assert_receive {:EXIT, ^pubsub, %ConnectionError{reason: :tcp_closed}}
    end)
  end

  defp wait_until_passes(timeout, fun) when timeout <= 0 do
    fun.()
  end

  defp wait_until_passes(timeout, fun) do
    try do
      fun.()
    rescue
      ExUnit.AssertionError ->
        Process.sleep(10)
        wait_until_passes(timeout - 10, fun)
    end
  end

  defp subscribed_channels(conn) do
    conn
    |> Redix.command!(~w(PUBSUB CHANNELS))
    |> MapSet.new()
  end

  # This function just sends back to this process every message it receives.
  defp message_mirror(parent) do
    receive do
      msg ->
        send(parent, {self(), msg})
        message_mirror(parent)
    end
  end
end
