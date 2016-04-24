defmodule Redix.Connection.TimeoutStore do
  @moduledoc false

  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, nil)
  end

  def stop(pid) do
    GenServer.call(pid, :stop)
  end

  def add(pid, request_id) do
    GenServer.cast(pid, {:add, request_id})
  end

  def remove(pid, request_id) do
    GenServer.cast(pid, {:remove, request_id})
  end

  def timed_out?(pid, request_id) do
    GenServer.call(pid, {:timed_out?, request_id})
  end

  ## Callbacks

  def init(_) do
    {:ok, HashSet.new}
  end

  def handle_call(:stop, from, state) do
    GenServer.reply(from, :ok)
    {:stop, :normal, state}
  end

  def handle_call({:timed_out?, request_id}, _from, state) do
    if HashSet.member?(state, request_id) do
      {:reply, true, HashSet.delete(state, request_id)}
    else
      {:reply, false, state}
    end
  end

  def handle_cast({:add, request_id}, state) do
    {:noreply, HashSet.put(state, request_id)}
  end

  def handle_cast({:remove, request_id}, state) do
    {:noreply, HashSet.delete(state, request_id)}
  end
end
