defmodule Redix.PropertiesTest do
  use ExUnit.Case

  use PropCheck.StateM
  use PropCheck

  @moduletag :capture_log
  @moduletag :propcheck

  defstruct [:map, :state]

  setup_all do
    Application.ensure_all_started(:crypto)
    :ok
  end

  defmodule Conn do
    def start_link(), do: Redix.start_link(name: __MODULE__, backoff_initial: 10)

    def set(key, value), do: Redix.command!(__MODULE__, ["SET", key, value])

    def get_existing(key), do: get(key)

    def get_non_existing(key), do: get(key)

    def stop(), do: Redix.stop(__MODULE__)

    defp get(key), do: Redix.command!(__MODULE__, ["GET", key])
  end

  property "setting and getting keys", [:verbose] do
    forall cmds <- commands(__MODULE__) do
      trap_exit do
        {:ok, _} = Conn.start_link()

        {history, state, result} = run_commands(__MODULE__, cmds)

        :ok = Conn.stop()

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
  end

  def initial_state do
    %{}
  end

  def command(state) do
    commands = [
      {:call, Conn, :set, [key(), value()]},
      {:call, Conn, :get_non_existing, [non_existing_key()]}
    ]

    commands =
      if map_size(state) > 0 do
        existing_keys = Map.keys(state)
        [{:call, Conn, :get_existing, [oneof(existing_keys)]}] ++ commands
      else
        commands
      end

    oneof(commands)
  end

  ## Preconditions

  def precondition(_state, _call) do
    true
  end

  ## Postconditions

  def postcondition(_state, {:call, Conn, :set, [_key, _value]}, result) do
    result == "OK"
  end

  def postcondition(state, {:call, Conn, :get_existing, [key]}, result) do
    Map.fetch!(state, key) == result
  end

  def postcondition(_state, {:call, Conn, :get_non_existing, [_key]}, result) do
    result == nil
  end

  ## Next state

  def next_state(state, _result, {:call, Conn, :set, [key, value]}) do
    Map.put(state, key, value)
  end

  def next_state(state, _result, _call) do
    state
  end

  ## Helpers

  defp key do
    utf8(20)
  end

  defp value do
    utf8(20)
  end

  defp non_existing_key do
    :crypto.strong_rand_bytes(40)
    |> Base.encode64()
  end
end
