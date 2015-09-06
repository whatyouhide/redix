defmodule Redix.PubSubTest do
  use ExUnit.Case, async: true

  alias Redix.PubSub

  setup do
    {:ok, ps} = PubSub.start_link
    {:ok, %{conn: ps}}
  end

  test "subscribe/3: one channel", %{conn: ps} do
    assert :ok = PubSub.subscribe(ps, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
  end

  test "subscribe/3: multiple channels", %{conn: ps} do
    assert :ok = PubSub.subscribe(ps, ["foo", "bar"], self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}
  end

  test "psubscribe/3: one pattern", %{conn: ps} do
    assert :ok = PubSub.psubscribe(ps, "foo*", self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
  end

  test "psubscribe/3: multiple patterns", %{conn: ps} do
    assert :ok = PubSub.psubscribe(ps, ["foo*", "bar*"], self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :psubscribe, "bar*", _}
  end

  test "unsubscribe/3: single channel", %{conn: ps} do
    assert :ok = PubSub.subscribe(ps, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert :ok = PubSub.unsubscribe(ps, "foo", self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}
  end

  test "unsubscribe/3: multiple channels", %{conn: ps} do
    assert :ok = PubSub.subscribe(ps, ~w(foo bar), self())
    assert_receive {:redix_pubsub, :subscribe, _, _}
    assert_receive {:redix_pubsub, :subscribe, _, _}
    assert :ok = PubSub.unsubscribe(ps, ~w(foo bar), self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}
    assert_receive {:redix_pubsub, :unsubscribe, "bar", _}
  end

  test "punsubscribe/3: single channel", %{conn: ps} do
    assert :ok = PubSub.psubscribe(ps, "foo*", self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
    assert :ok = PubSub.punsubscribe(ps, "foo*", self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}
  end

  test "punsubscribe/3: multiple channels", %{conn: ps} do
    assert :ok = PubSub.psubscribe(ps, ~w(foo* bar?), self())
    assert_receive {:redix_pubsub, :psubscribe, _, _}
    assert_receive {:redix_pubsub, :psubscribe, _, _}
    assert :ok = PubSub.punsubscribe(ps, ~w(foo* bar?), self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :punsubscribe, "bar?", _}
  end

  test "subscribing the same pid to the same channel more than once has no effect", %{conn: ps} do
    assert :ok = PubSub.subscribe(ps, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert :ok = PubSub.subscribe(ps, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}

    {:ok, c} = Redix.start_link

    Redix.command!(c, ~w(PUBLISH foo hello))

    assert_receive {:redix_pubsub, :message, "hello", "foo"}
    refute_receive {:redix_pubsub, :message, "hello", "foo"}
  end

  test "pubsub: subscribing to channels and receiving messages", %{conn: ps} do
    {:ok, c} = Redix.start_link

    PubSub.subscribe(ps, ~w(foo bar), self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}

    Redix.pipeline!(c, [~w(PUBLISH foo foo), ~w(PUBLISH bar bar), ~w(PUBLISH baz baz)])
    assert_receive {:redix_pubsub, :message, "foo", "foo"}
    assert_receive {:redix_pubsub, :message, "bar", "bar"}
    refute_receive {:redix_pubsub, :message, "baz", "baz"}

    PubSub.unsubscribe(ps, "foo", self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}

    Redix.pipeline!(c, [~w(PUBLISH foo foo), ~w(PUBLISH bar bar)])
    refute_receive {:redix_pubsub, :message, "foo", "foo"}
    assert_receive {:redix_pubsub, :message, "bar", "bar"}
  end

  test "pubsub: subscribing to patterns and receiving messages", %{conn: ps} do
    {:ok, c} = Redix.start_link

    PubSub.psubscribe(ps, ~w(foo* ba?), self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :psubscribe, "ba?", _}

    Redix.pipeline!(c, [~w(PUBLISH foo_1 foo_1),
                        ~w(PUBLISH foo_2 foo_2),
                        ~w(PUBLISH bar bar),
                        ~w(PUBLISH barfoo barfoo)])

    assert_receive {:redix_pubsub, :pmessage, "foo_1", {"foo*", "foo_1"}}
    assert_receive {:redix_pubsub, :pmessage, "foo_2", {"foo*", "foo_2"}}
    assert_receive {:redix_pubsub, :pmessage, "bar", {"ba?", "bar"}}
    refute_receive {:redix_pubsub, :pmessage, "barfoo", {_, "barfoo"}}

    PubSub.punsubscribe(ps, "foo*", self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}

    Redix.pipeline!(c, [~w(PUBLISH foo_x foo_x), ~w(PUBLISH baz baz)])

    refute_receive {:redix_pubsub, :pmessage, "foo_x", {"foo*", "foo_x"}}
    assert_receive {:redix_pubsub, :pmessage, "baz", {"ba?", "baz"}}
  end

  test "pubsub: subscribing multiple times to the same channel has no effects", %{conn: ps} do
    {:ok, c} = Redix.start_link

    PubSub.subscribe(ps, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}

    PubSub.subscribe(ps, ["foo", "bar"], self())
    # We still receive two messages so that we don't have to keep track of
    # already subscribed channels.
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}

    # Let's be sure we only receive *one* message on the "foo" channel.
    Redix.command!(c, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, :message, "hello", _}
    refute_receive {:redix_pubsub, :message, "hello", _}
  end

  test "pubsub: unsubscribing a recipient doesn't affect other recipients", %{conn: ps} do
    {:ok, c} = Redix.start_link

    parent = self
    mirror = spawn_link(fn -> message_mirror(parent) end)

    # Let's subscribe two different pids to the same channel.
    PubSub.subscribe(ps, "foo", self)
    assert_receive {:redix_pubsub, :subscribe, _, _}
    PubSub.subscribe(ps, "foo", mirror)
    assert_receive {^mirror, {:redix_pubsub, :subscribe, _, _}}

    # Let's ensure both those pids receive messages published on that channel.
    Redix.command!(c, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, :message, _, _}
    assert_receive {^mirror, {:redix_pubsub, :message, _, _}}

    # Now let's unsubscribe just one pid from that channel.
    PubSub.unsubscribe(ps, "foo", self)
    assert_receive {:redix_pubsub, :unsubscribe, _, _}
    refute_receive {^mirror, {:redix_pubsub, :unsubscribe, _, _}}

    # Publishing now should send a message to the non-unsubscribed pid.
    Redix.command!(c, ~w(PUBLISH foo hello))
    refute_receive {:redix_pubsub, :message, _, _}
    assert_receive {^mirror, _}
  end

  test "recipients are monitored and the connection unsubcribes when they go down", %{conn: ps} do
    parent = self
    pid = spawn(fn -> message_mirror(parent) end)

    assert :ok = PubSub.subscribe(ps, "foo", pid)
    assert_receive {^pid, {:redix_pubsub, :subscribe, "foo", _}}

    # Let's just ensure no errors happen when we kill the recipient.
    Process.exit(pid, :kill)

    :timer.sleep(100)
  end

  # This function just sends back to this process every message it receives.
  defp message_mirror(parent) do
    receive do
      msg -> send(parent, {self(), msg})
    end
    message_mirror(parent)
  end
end
