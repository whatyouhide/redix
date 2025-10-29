defmodule Redix.PubSubPropertiesTest do
  use ExUnit.Case

  use PropCheck.StateM
  use PropCheck

  @moduletag :capture_log
  @moduletag :propcheck

  defstruct [:channels, :ref]

  defmodule PubSub do
    def start_link do
      Redix.PubSub.start_link(
        name: __MODULE__,
        backoff_initial: 0,
        sync_connect: true
      )
    end

    def subscribe(channel) do
      {:ok, ref} = Redix.PubSub.subscribe(__MODULE__, channel, self())
      ref
    end

    def unsubscribe(channel), do: Redix.PubSub.unsubscribe(__MODULE__, channel, self())

    def stop, do: Redix.PubSub.stop(__MODULE__)
  end

  defmodule ControlConn do
    def start_link, do: Redix.start_link(name: __MODULE__, sync_connect: true)

    def subscribed_channels,
      do: MapSet.new(Redix.command!(__MODULE__, ["PUBSUB", "CHANNELS"]))

    def publish(channel, message), do: Redix.command!(__MODULE__, ["PUBLISH", channel, message])

    def disconnect_pubsub,
      do: Redix.command!(__MODULE__, ["CLIENT", "KILL", "TYPE", "pubsub"])

    def stop, do: Redix.stop(__MODULE__)
  end

  property "subscribing and unsubscribing from channels", [:verbose] do
    numtests(
      50,
      forall cmds <- commands(__MODULE__) do
        trap_exit do
          {:ok, _} = PubSub.start_link()
          {:ok, _} = ControlConn.start_link()

          {history, state, result} = run_commands(__MODULE__, cmds)

          :ok = PubSub.stop()
          :ok = ControlConn.stop()

          fail_report = """
          History: #{inspect(history, pretty: true)}

          State: #{inspect(state, pretty: true)}

          Result: #{inspect(result, pretty: true)}
          """

          (result == :ok)
          |> when_fail(IO.puts(fail_report))
          |> aggregate(command_names(cmds))
        end
      end
    )
  end

  def initial_state do
    %__MODULE__{channels: MapSet.new()}
  end

  def command(_state) do
    frequency([
      {3, {:call, PubSub, :subscribe, [channel()]}},
      {2, {:call, PubSub, :unsubscribe, [channel()]}},
      {4, {:call, ControlConn, :publish, [channel(), "hello"]}},
      {1, {:call, ControlConn, :disconnect_pubsub, []}}
    ])
  end

  ## Preconditions

  # Only unsubcribe from channels we're subscribed to.
  def precondition(state, {:call, PubSub, :unsubscribe, [channel]}) do
    channel in state.channels
  end

  # Only disconnect if we're subscribed to something.
  def precondition(state, {:call, ControlConn, :disconnect_pubsub, []}) do
    MapSet.size(state.channels) > 0
  end

  def precondition(_state, _call) do
    true
  end

  ## Postconditions

  def postcondition(state, {:call, PubSub, :subscribe, [channel]}, ref = _result) do
    assert_receive {:redix_pubsub, _pid, ^ref, :subscribed, %{channel: ^channel}}
    MapSet.put(state.channels, channel) == ControlConn.subscribed_channels()
  end

  def postcondition(state, {:call, PubSub, :unsubscribe, [channel]}, result) do
    ref = state.ref

    assert_receive {:redix_pubsub, _pid, ^ref, :unsubscribed, %{channel: ^channel}}
    assert result == :ok

    # Redix.PubSub sends the confirmation message to the caller process *before* it sends
    # the UNSUBSCRIBE command to Redis, because it will eventually unsubscribe anyways.
    # If we call ControlConn.subscribed_channels() (which calls to Redis) right away,
    # the channel will still be in there. That's why we wait for a bit until this passes.
    wait_for_true(_timeout = 200, fn ->
      MapSet.delete(state.channels, channel) == ControlConn.subscribed_channels()
    end)
  end

  def postcondition(state, {:call, ControlConn, :publish, [channel, message]}, _result) do
    ref = state.ref

    if channel in state.channels do
      assert_receive {:redix_pubsub, _pid, ^ref, :message, %{channel: ^channel, payload: payload}}

      payload == message
    else
      true
    end
  end

  def postcondition(state, {:call, ControlConn, :disconnect_pubsub, []}, _result) do
    ref = state.ref

    assert_receive {:redix_pubsub, _pid, ^ref, :disconnected, %{error: _error}}, 500

    for channel <- state.channels do
      assert_receive {:redix_pubsub, _pid, ^ref, :subscribed, %{channel: ^channel}}, 1000
    end

    true
  end

  ## Next state

  def next_state(%__MODULE__{} = state, result, {:call, PubSub, :subscribe, [channel]}) do
    %{state | channels: MapSet.put(state.channels, channel), ref: result}
  end

  def next_state(state, _result, {:call, PubSub, :unsubscribe, [channel]}) do
    update_in(state.channels, &MapSet.delete(&1, channel))
  end

  def next_state(state, _result, _call) do
    state
  end

  ## Helpers

  defp channel do
    oneof(["foo", "bar", "baz"])
  end

  defp wait_for_true(timeout, fun) do
    cond do
      timeout < 0 ->
        fun.()

      fun.() ->
        true

      true ->
        Process.sleep(10)
        wait_for_true(timeout - 10, fun)
    end
  end
end
