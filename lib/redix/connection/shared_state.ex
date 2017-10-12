defmodule Redix.Connection.SharedState do
  @moduledoc false

  use GenServer

  @type client :: {:commands, reference, GenServer.from(), pos_integer}

  ## GenServer state

  defstruct clients_queue: :queue.new(),
            timed_out_requests: MapSet.new()

  ## Public API

  @spec start_link() :: GenServer.on_start()
  def start_link() do
    GenServer.start_link(__MODULE__, nil)
  end

  @spec enqueue(GenServer.server(), client) :: :ok
  def enqueue(pid, client) do
    GenServer.cast(pid, {:enqueue, client})
  end

  @spec dequeue(GenServer.server()) :: {boolean, client}
  def dequeue(pid) do
    GenServer.call(pid, :dequeue)
  end

  @spec add_timed_out_request(GenServer.server(), reference) :: :ok
  def add_timed_out_request(pid, request_id) do
    GenServer.call(pid, {:add_timed_out_request, request_id})
  end

  @spec cancel_timed_out_request(GenServer.server(), reference) :: :ok
  def cancel_timed_out_request(pid, request_id) do
    GenServer.call(pid, {:cancel_timed_out_request, request_id})
  end

  @spec disconnect_clients_and_stop(GenServer.server()) :: :ok
  def disconnect_clients_and_stop(pid) do
    GenServer.call(pid, :disconnect_clients_and_stop)
  end

  ## GenServer callbacks

  def init(_) do
    {:ok, %__MODULE__{}}
  end

  def handle_call({:add_timed_out_request, request_id}, _from, state) do
    state = %{state | timed_out_requests: MapSet.put(state.timed_out_requests, request_id)}
    {:reply, :ok, state}
  end

  def handle_call({:cancel_timed_out_request, request_id}, _from, state) do
    state = %{state | timed_out_requests: MapSet.delete(state.timed_out_requests, request_id)}
    {:reply, :ok, state}
  end

  # Returns {timed_out_request?, client}.
  def handle_call(:dequeue, _from, state) do
    {{:value, {:commands, request_id, _from, _ncommands} = client}, new_queue} =
      :queue.out(state.clients_queue)

    {timed_out_request?, new_timed_out_requests} =
      pop_from_set(state.timed_out_requests, request_id)

    state = %{state | clients_queue: new_queue, timed_out_requests: new_timed_out_requests}
    {:reply, {timed_out_request?, client}, state}
  end

  def handle_call(:disconnect_clients_and_stop, _from, state) do
    # First, we notify all the clients.
    Enum.each(:queue.to_list(state.clients_queue), fn {:commands, request_id, from, _ncommands} ->
      # We don't care about "popping" the element out of the MapSet (returning
      # the new set) because this process is going to die at the end of this
      # function anyways.
      unless MapSet.member?(state.timed_out_requests, request_id) do
        reply = {request_id, {:error, %Redix.ConnectionError{reason: :disconnected}}}
        Connection.reply(from, reply)
      end
    end)

    {:stop, :normal, _reply = :ok, state}
  end

  def handle_cast({:enqueue, client}, %{clients_queue: queue} = state) do
    state = %{state | clients_queue: :queue.in(client, queue)}
    {:noreply, state}
  end

  ## Helpers

  defp pop_from_set(set, elem) do
    if MapSet.member?(set, elem) do
      {true, MapSet.delete(set, elem)}
    else
      {false, set}
    end
  end
end
