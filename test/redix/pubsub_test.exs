defmodule Redix.PubSubTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Redix.{ConnectionError, PubSub}

  @moduletag :pubsub

  # See docker-compose.yml.
  @port 6380

  setup do
    {:ok, pubsub} = PubSub.start_link(port: @port)
    {:ok, conn} = Redix.start_link(port: @port)
    {:ok, %{pubsub: pubsub, conn: conn}}
  end

  test "using gen_statem options in start_link/2" do
    fullsweep_after = Enum.random(0..50000)
    {:ok, pid} = PubSub.start_link(port: @port, spawn_opt: [fullsweep_after: fullsweep_after])
    {:garbage_collection, info} = Process.info(pid, :garbage_collection)
    assert info[:fullsweep_after] == fullsweep_after
  end

  test "client_id/1 should be available after start_link/2" do
    {:ok, pid} = PubSub.start_link(port: @port, fetch_client_id_on_connect: true)
    assert {:ok, client_id} = PubSub.get_client_id(pid)
    assert is_integer(client_id)
  end

  test "client_id/1 returns an error if connection fails" do
    {:ok, pid} = PubSub.start_link(port: 9999, name: :redix_pubsub_telemetry_failed_conn_test)
    assert {:error, %ConnectionError{reason: :closed}} = PubSub.get_client_id(pid)
  end

  test "client_id/1 returns an error if :fetch_client_id_on_connect is not true" do
    {:ok, pid} = PubSub.start_link(port: @port, fetch_client_id_on_connect: false)
    assert {:error, %ConnectionError{reason: :client_id_not_stored}} = PubSub.get_client_id(pid)
  end

  test "subscribe/unsubscribe flow", %{pubsub: pubsub, conn: conn} do
    # First, we subscribe.
    assert {:ok, ref} = PubSub.subscribe(pubsub, ["foo", "bar"], self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "bar"}}

    assert subscribed_channels(conn) == MapSet.new(["foo", "bar"])

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
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    assert {:ok, ^ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    assert subscribed_channels(conn) == MapSet.new(["foo"])

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

    assert subscribed_channels(conn) == MapSet.new(["foo"])

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

  test "if a pid crashes and then resubscribes right away it is resubscribed correctly",
       %{pubsub: pubsub} do
    parent = self()
    {pid, monitor_ref} = spawn_monitor(fn -> message_mirror(parent) end)

    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", pid)
    assert_receive {^pid, {:redix_pubsub, ^pubsub, ^ref, :subscribed, _properties}}

    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor_ref, _, _, _}

    pid = spawn(fn -> message_mirror(parent) end)
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", pid)
    assert_receive {^pid, {:redix_pubsub, ^pubsub, ^ref, :subscribed, _properties}}
  end

  test "after unsubscribing from a channel, resubscribing one recipient resubscribes correctly",
       %{pubsub: pubsub, conn: conn} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, _properties}

    assert subscribed_channels(conn) == MapSet.new(["foo"])

    Redix.command!(conn, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{payload: "hello"}}

    assert :ok = PubSub.unsubscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :unsubscribed, _properties}

    wait_until_passes(200, fn ->
      assert subscribed_channels(conn) == MapSet.new()
    end)

    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, _properties}

    assert subscribed_channels(conn) == MapSet.new(["foo"])

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
  end

  test "disconnections/reconnections", %{pubsub: pubsub, conn: conn} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}

    capture_log(fn ->
      Redix.command!(conn, ~w(CLIENT KILL TYPE pubsub))

      assert_receive {:redix_pubsub, ^pubsub, ^ref, :disconnected, properties}
      assert %{error: %Redix.ConnectionError{}} = properties

      assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}, 1000
    end)

    Redix.command!(conn, ~w(PUBLISH foo hello))

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "foo", payload: "hello"}},
                   1000
  end

  test "emits connection-related events on disconnections and reconnections", %{conn: conn} do
    {test_name, _arity} = __ENV__.function

    parent = self()
    ref = make_ref()

    handler = fn event, measurements, meta, _config ->
      # We need to run this test only if was called for this Redix connection so that
      # we can run in parallel with the other tests.
      if meta.connection_name == :redix_pubsub_telemetry_test do
        assert measurements == %{}

        case event do
          [:redix, :connection] -> send(parent, {ref, :connected, meta})
          [:redix, :disconnection] -> send(parent, {ref, :disconnected, meta})
        end
      end
    end

    events = [[:redix, :connection], [:redix, :disconnection]]
    :ok = :telemetry.attach_many(to_string(test_name), events, handler, :no_config)

    {:ok, pubsub} = PubSub.start_link(port: @port, name: :redix_pubsub_telemetry_test)
    # Make sure to call subscribe/3 so that Redis considers this a PubSub connection.
    {:ok, pubsub_ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^pubsub_ref, :subscribed, %{channel: "foo"}}

    assert_receive {^ref, :connected, meta}, 1000
    assert %{address: "localhost" <> _port, reconnection: false, connection: ^pubsub} = meta

    capture_log(fn ->
      # Assert that we effectively kill one client.
      assert Redix.command!(conn, ~w(CLIENT KILL TYPE pubsub)) == 1

      assert_receive {^ref, :disconnected, meta}, 1000
      assert %{address: "localhost" <> _port, connection: ^pubsub} = meta

      assert_receive {^ref, :connected, meta}, 1000
      assert %{address: "localhost" <> _port, reconnection: true, connection: ^pubsub} = meta
    end)
  end

  test "emits connection-related events on failed connections" do
    {test_name, _arity} = __ENV__.function

    parent = self()
    ref = make_ref()

    handler = fn event, measurements, meta, _config ->
      # We need to run this test only if was called for this Redix connection so that
      # we can run in parallel with the other tests.
      if meta.connection_name == :redix_pubsub_telemetry_failed_conn_test do
        assert event == [:redix, :failed_connection]
        assert measurements == %{}
        send(parent, {ref, :failed_connection, meta})
      end
    end

    :telemetry.attach(to_string(test_name), [:redix, :failed_connection], handler, :no_config)

    {:ok, pubsub} = PubSub.start_link(port: 9999, name: :redix_pubsub_telemetry_failed_conn_test)
    # Make sure to call subscribe/3 so that Redis considers this a PubSub connection.
    {:ok, _pubsub_ref} = PubSub.subscribe(pubsub, "foo", self())

    assert_receive {^ref, :failed_connection, meta}, 1000

    assert %{
             address: "localhost:9999",
             reason: %ConnectionError{reason: :econnrefused},
             connection: ^pubsub
           } = meta
  end

  @tag :capture_log
  test "subscribing while the connection is down", %{pubsub: pubsub, conn: conn} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}, 1000

    Redix.command!(conn, ~w(CLIENT KILL TYPE pubsub))

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :disconnected, _properties}

    assert {:ok, ^ref} = PubSub.subscribe(pubsub, "bar", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "bar"}}, 1000
    Redix.command!(conn, ~w(PUBLISH bar hello))

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "bar", payload: "hello"}},
                   1000
  end

  test ":exit_on_disconnection option", %{conn: conn} do
    {:ok, pubsub} = PubSub.start_link(port: @port, exit_on_disconnection: true)

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

  test "continuation gets cleared on reconnection", %{pubsub: pubsub} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "my_channel", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "my_channel"}}

    # This exposes internals but I couldn't think of a better way to simulate this situation
    {:connected, state} = :sys.get_state(pubsub)
    socket = state.socket

    send(pubsub, {:tcp, socket, "*3\r\n$7\r\nmessage\r\n$10\r\nmy_channel\r\n$10\r\nhello"})
    send(pubsub, {:tcp_closed, socket})

    assert_receive {:redix_pubsub, ^pubsub, ^ref, :disconnected, %{error: _error}}

    assert {:disconnected, new_state} = :sys.get_state(pubsub)
    refute new_state.continuation
  end

  test "pubsub: multiple pings succeeds", %{pubsub: pubsub} do
    assert :ok = PubSub.ping(pubsub)
    assert :ok = PubSub.ping(pubsub)
  end

  test "pubsub: ping gives error when disconnected", %{pubsub: pubsub} do
    {:connected, state} = :sys.get_state(pubsub)
    socket = state.socket

    send(pubsub, {:tcp_closed, socket})
    assert {:disconnected, _new_state} = :sys.get_state(pubsub)

    assert :error = PubSub.ping(pubsub)
  end

  test "pubsub: multiple pings succeeds when subscribed to channel", %{pubsub: pubsub} do
    assert {:ok, ref} = PubSub.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "foo"}}, 1000

    assert :ok = PubSub.ping(pubsub)
    assert :ok = PubSub.ping(pubsub)
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
