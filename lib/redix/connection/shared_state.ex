defmodule Redix.Connection.SharedState do
  @moduledoc false

  use GenServer

  alias Redix.Utils

  ## GenServer state

  defstruct [
    clients_queue: :queue.new,
    timed_out_requests: HashSet.new,
  ]

  ## Public API

  def start_link() do
    GenServer.start_link(__MODULE__, nil)
  end

  def enqueue(pid, client) do
    GenServer.cast(pid, {:enqueue, client})
  end

  def dequeue(pid) do
    GenServer.call(pid, :dequeue)
  end

  def add_timed_out_request(pid, request_id) do
    GenServer.call(pid, {:add_timed_out_request, request_id})
  end

  def cancel_timed_out_request(pid, request_id) do
    GenServer.call(pid, {:cancel_timed_out_request, request_id})
  end

  def disconnect_clients_and_stop(pid) do
    GenServer.call(pid, :disconnect_clients_and_stop)
  end

  ## GenServer callbacks

  def init(_) do
    {:ok, %__MODULE__{}}
  end

  def handle_call({:add_timed_out_request, request_id}, _from, state) do
    state = %{state | timed_out_requests: HashSet.put(state.timed_out_requests, request_id)}
    {:reply, :ok, state}
  end

  def handle_call({:cancel_timed_out_request, request_id}, _from, state) do
    state = %{state | timed_out_requests: HashSet.delete(state.timed_out_requests, request_id)}
    {:reply, :ok, state}
  end

  # Returns {timed_out_request?, client}.
  def handle_call(:dequeue, _from, state) do
    {{:value, {:commands, request_id, _from, _ncommands} = client}, new_queue} = :queue.out(state.clients_queue)
    result = {HashSet.member?(state.timed_out_requests, request_id), client}
    state = %{state | clients_queue: new_queue}
    {:reply, result, state}
  end

  def handle_call(:disconnect_clients_and_stop, from, state) do
    # First, we notify all the clients.
    Enum.each(:queue.to_list(state.clients_queue), fn({:commands, request_id, from, _ncommands}) ->
      Utils.reply_to_client(from, request_id, {:error, :disconnected})
    end)

    GenServer.reply(from, :ok)
    {:stop, :normal, state}
  end

  def handle_cast({:enqueue, client}, %{clients_queue: queue} = state) do
    state = %{state | clients_queue: :queue.in(client, queue)}
    {:noreply, state}
  end
end
