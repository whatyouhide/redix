defmodule Redix.PubSubTest do
  use ExUnit.Case, async: true

  test "pubsub without patterns" do
    {:ok, c} = Redix.start_link
    {:ok, other} = Redix.start_link

    Redix.subscribe(c, "foo", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo hey_foo))
    Redix.command!(other, ~w(PUBLISH bar hey_bar))

    assert_receive {:redix_pubsub, :message, "foo", "hey_foo"}
    refute_receive {:redix_pubsub, :message, "bar", "hey_bar"}

    Redix.unsubscribe(c, "foo", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo hey_foo))

    refute_receive {:redix_pubsub, :message, "foo", "hey_foo"}
  end

  test "pubsub with patterns" do
    {:ok, c} = Redix.start_link
    {:ok, other} = Redix.start_link

    Redix.psubscribe(c, "foo*", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo_1 hey_foo_1))
    Redix.command!(other, ~w(PUBLISH foo_2 hey_foo_2))
    Redix.command!(other, ~w(PUBLISH bar hey_bar))

    assert_receive {:redix_pubsub, :pmessage, {"foo*", "foo_1"}, "hey_foo_1"}
    assert_receive {:redix_pubsub, :pmessage, {"foo*", "foo_2"}, "hey_foo_2"}
    refute_receive {:redix_pubsub, :pmessage, {"foo*", "bar"}, "hey_bar"}

    Redix.punsubscribe(c, "foo*", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo_x hey_foo))

    refute_receive {:redix_pubsub, :pmessage, {"foo*", "foo_x"}, "hey_foo"}
  end
end
