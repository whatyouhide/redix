defmodule Redix.PubSubTest do
  use ExUnit.Case, async: true

  alias Redix.PubSub

  test "pubsub without patterns" do
    {:ok, c} = PubSub.start_link
    {:ok, other} = Redix.start_link

    PubSub.subscribe(c, "foo", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo hey_foo))
    Redix.command!(other, ~w(PUBLISH bar hey_bar))

    assert_receive {:redix_pubsub, "hey_foo"}
    refute_receive {:redix_pubsub, "hey_bar"}

    PubSub.unsubscribe(c, "foo", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo hey_foo))

    refute_receive {:redix_pubsub, "hey_foo"}
  end

  test "pubsub with patterns" do
    {:ok, c} = PubSub.start_link
    {:ok, other} = Redix.start_link

    PubSub.psubscribe(c, "foo*", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo_1 hey_foo_1))
    Redix.command!(other, ~w(PUBLISH foo_2 hey_foo_2))
    Redix.command!(other, ~w(PUBLISH bar hey_bar))

    assert_receive {:redix_pubsub, "hey_foo_1"}
    assert_receive {:redix_pubsub, "hey_foo_2"}
    refute_receive {:redix_pubsub, "hey_bar"}

    PubSub.punsubscribe(c, "foo*", self())
    :timer.sleep 100

    Redix.command!(other, ~w(PUBLISH foo_x hey_foo))

    refute_receive {:redix_pubsub, "hey_foo"}
  end
end
