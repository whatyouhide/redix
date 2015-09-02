defmodule Redix.PubSubTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, c} = Redix.start_link
    {:ok, %{conn: c}}
  end

  test "subscribe/3: one channel", %{conn: c} do
    assert :ok = Redix.subscribe(c, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
  end

  test "subscribe/3: multiple channels", %{conn: c} do
    assert :ok = Redix.subscribe(c, ["foo", "bar"], self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}
  end

  test "psubscribe/3: one pattern", %{conn: c} do
    assert :ok = Redix.psubscribe(c, "foo*", self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
  end

  test "psubscribe/3: multiple patterns", %{conn: c} do
    assert :ok = Redix.psubscribe(c, ["foo*", "bar*"], self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :psubscribe, "bar*", _}
  end

  test "subscribe/3: sending to a pid", %{conn: c} do
    task = Task.async fn ->
      assert_receive {:redix_pubsub, :subscribe, "foo", _}
    end

    assert :ok = Redix.subscribe(c, "foo", task.pid)
    Task.await(task)
  end

  test "unsubscribe/3: single channel", %{conn: c} do
    assert :ok = Redix.subscribe(c, "foo", self())
    assert :ok = Redix.unsubscribe(c, "foo", self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}
  end

  test "unsubscribe/3: multiple channels", %{conn: c} do
    assert :ok = Redix.subscribe(c, ~w(foo bar), self())
    # assert_receive {:redix_pubsub, :subscribe, _, _}
    # assert_receive {:redix_pubsub, :subscribe, _, _}
    assert :ok = Redix.unsubscribe(c, ~w(foo bar), self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}
    assert_receive {:redix_pubsub, :unsubscribe, "bar", _}
  end

  test "punsubscribe/3: single channel", %{conn: c} do
    assert :ok = Redix.psubscribe(c, "foo*", self())
    assert :ok = Redix.punsubscribe(c, "foo*", self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}
  end

  test "punsubscribe/3: multiple channels", %{conn: c} do
    assert :ok = Redix.psubscribe(c, ~w(foo* bar?), self())
    assert :ok = Redix.punsubscribe(c, ~w(foo* bar?), self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :punsubscribe, "bar?", _}
  end

  test "pubsub: subscribing to channels and receiving messages", %{conn: c} do
    {:ok, pubsub} = Redix.start_link

    Redix.subscribe(pubsub, ~w(foo bar), self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}

    Redix.pipeline!(c, [~w(PUBLISH foo foo), ~w(PUBLISH bar bar), ~w(PUBLISH baz baz)])
    assert_receive {:redix_pubsub, :message, "foo", "foo"}
    assert_receive {:redix_pubsub, :message, "bar", "bar"}
    refute_receive {:redix_pubsub, :message, "baz", "baz"}

    Redix.unsubscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}

    Redix.pipeline!(c, [~w(PUBLISH foo foo), ~w(PUBLISH bar bar)])
    refute_receive {:redix_pubsub, :message, "foo", "foo"}
    assert_receive {:redix_pubsub, :message, "bar", "bar"}
  end

  test "pubsub: subscribing to patterns and receiving messages", %{conn: c} do
    {:ok, pubsub} = Redix.start_link

    Redix.psubscribe(pubsub, ~w(foo* ba?), self())
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

    Redix.punsubscribe(pubsub, "foo*", self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}

    Redix.pipeline!(c, [~w(PUBLISH foo_x foo_x), ~w(PUBLISH baz baz)])

    refute_receive {:redix_pubsub, :pmessage, "foo_x", {"foo*", "foo_x"}}
    assert_receive {:redix_pubsub, :pmessage, "baz", {"ba?", "baz"}}
  end

  test "pubsub: subscribing multiple times to the same channel has no effects", %{conn: c} do
    {:ok, pubsub} = Redix.start_link

    Redix.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}

    Redix.subscribe(pubsub, ["foo", "bar"], self())
    # We still receive two messages so that we don't have to keep track of
    # already subscribed channels.
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}

    # Let's be sure we only receive *one* message on the "foo" channel.
    Redix.command!(c, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, :message, "hello", _}
    refute_receive {:redix_pubsub, :message, "hello", _}
  end

  test "pubsub: once you (p)subscribe, you go in pubsub mode (with subscribe/3)", %{conn: c} do
    assert :ok = Redix.subscribe(c, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert Redix.command(c, ["PING"]) == {:error, :pubsub_mode}
  end

  test "pubsub: once you (p)subscribe, you go in pubsub mode (with psubscribe/3)", %{conn: c} do
    assert :ok = Redix.psubscribe(c, "fo*", self())
    assert_receive {:redix_pubsub, :psubscribe, "fo*", _}
    assert Redix.command(c, ["PING"]) == {:error, :pubsub_mode}
  end

  test "pubsub: you can't unsubscribe if you're not in pubsub mode", %{conn: c} do
    assert Redix.unsubscribe(c, "foo", self()) == {:error, :not_pubsub_mode}
    assert Redix.punsubscribe(c, "fo*", self()) == {:error, :not_pubsub_mode}
  end

  test "pubsub?/1: returns true if the conn is in pubsub mode, false otherwise", %{conn: c} do
    refute Redix.pubsub?(c)
    Redix.subscribe(c, "foo", self())
    assert Redix.pubsub?(c)
  end

  test "pubsub: unsubscribing a recipient doesn't affect other recipients", %{conn: c} do
    {:ok, pubsub} = Redix.start_link

    parent = self
    mirror = spawn_link(fn -> message_mirror(parent) end)

    # Let's subscribe two different pids to the same channel.
    Redix.subscribe(pubsub, "foo", self)
    assert_receive {:redix_pubsub, :subscribe, _, _}
    Redix.subscribe(pubsub, "foo", mirror)
    assert_receive {^mirror, {:redix_pubsub, :subscribe, _, _}}

    # Let's ensure both those pids receive messages published on that channel.
    Redix.command!(c, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, :message, _, _}
    assert_receive {^mirror, {:redix_pubsub, :message, _, _}}

    # Now let's unsubscribe just one pid from that channel.
    Redix.unsubscribe(pubsub, "foo", self)
    assert_receive {:redix_pubsub, :unsubscribe, _, _}
    refute_receive {^mirror, {:redix_pubsub, :unsubscribe, _, _}}

    # Publishing now should send a message to the non-unsubscribed pid.
    Redix.command!(c, ~w(PUBLISH foo hello))
    refute_receive {:redix_pubsub, :message, _, _}
    assert_receive {^mirror, _}
  end

  # This function just sends back to this process every message it receives.
  defp message_mirror(parent) do
    receive do
      msg ->
        send(parent, {self(), msg})
    end
    message_mirror(parent)
  end
end
