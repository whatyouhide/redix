defmodule Redix.PubSub.Connection.Monitor do
  use GenServer

  @empty Map.new()
  @absorb_timeout 1_000

  defstruct [
    :absorb_timeout,
    :connection,
    monitored: Map.new(),
    disconnected: Map.new()
  ]

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def init([conn, nil]) do
    {:ok, %__MODULE__{connection: conn}}
  end
  def init([conn: conn, absorb_timeout: absorb_timeout]) do
    {:ok, %__MODULE__{connection: conn, absorb_timeout: absorb_timeout}}
  end

  def add(pid, monitored_pid), do: GenServer.call(pid, {:add, monitored_pid})

  def remove(pid, monitored_pid), do: GenServer.call(pid, {:remove, monitored_pid})

  def flush_monitors(pid), do: flush_monitors(pid, @absorb_timeout)

  def flush_monitors(pid, nil), do: flush_monitors(pid, @absorb_timeout)

  def flush_monitors(pid, absorb_timeout), do: Process.send_after(pid, {:flush}, absorb_timeout)

  ## CALLBACKS ##

  def handle_call({:add,  monitored_pid}, _from, state) do
    {state, ref} = case state.monitored do
      %{^monitored_pid => ref} -> {state, ref}

      _ ->
        ref = Process.monitor(monitored_pid)
        {put_in(state.monitored[monitored_pid], ref), ref}
    end
    {:reply, {:ok, ref}, state}
  end

  def handle_call({:remove,  monitored_pid}, _from, state) do
    Process.demonitor(state.monitored[monitored_pid], [:flush])
    state = update_in(state.monitored, &Map.delete(&1, monitored_pid))

    {:reply, :ok, state}
  end

  def handle_info({:flush}, %__MODULE__{disconnected: disconnected_map} = state)
      when disconnected_map == @empty do
    {:noreply, state}
  end

  def handle_info({:flush}, %__MODULE__{connection: conn, disconnected: disconnected_map} = state) do
    :gen_statem.cast(conn, {:monitors_disconnected, disconnected_map})
    state = %{state | disconnected: Map.new()}

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, monitored_pid, _reason}, state) do
    state = update_in(state.monitored, &Map.delete(&1, monitored_pid))
    state = put_in(state.disconnected[monitored_pid], ref)

    flush_monitors(self())
    {:noreply, state}
  end

end