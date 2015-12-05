defmodule Redix.Connection.Receiver do
  use GenServer

  alias Redix.Protocol

  @initial_state %{
    # The process that sends stuff to the socket and that spawns this process
    sender: nil,
    # The queue of commands issued to Redis
    queue: :queue.new,
    # The TCP socket, which should be passive when given to this process
    socket: nil,
    # The tail of unparsed data
    tail: "",
  }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def enqueue(pid, what) do
    GenServer.cast(pid, {:enqueue, what})
  end

  ## Callbacks

  def init(opts) do
    state = Dict.merge(@initial_state, opts)
    :inet.setopts(state.socket, active: :once)
    {:ok, state}
  end

  def handle_cast({:enqueue, what}, state) do
    state = update_in(state.queue, &:queue.in(what, &1))
    {:noreply, state}
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)
    state = new_data(state, state.tail <> data)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket} = msg, %{socket: socket} = state) do
    disconnect(msg, {:error, :disconnected}, state)
  end

  def handle_info({:tcp_error, socket, reason} = msg, %{socket: socket} = state) do
    disconnect(msg, {:error, reason}, state)
  end

  ## Helpers

  defp new_data(state, <<>>) do
    %{state | tail: <<>>}
  end

  defp new_data(state, data) do
    {{:value, {:commands, from, ncommands}}, new_queue} = :queue.out(state.queue)

    case Protocol.parse_multi(data, ncommands) do
      {:ok, resp, rest} ->
        Connection.reply(from, format_resp(resp))
        state = %{state | queue: new_queue}
        new_data(state, rest)
      {:error, :incomplete} ->
        %{state | tail: data}
    end
  end

  defp disconnect(msg, error, state) do
    state = reply_to_queue(error, state)
    send state.sender, {:receiver, self(), msg}
    {:stop, :normal, state}
  end

  defp reply_to_queue(error, state) do
    for {:commands, from, _} <- :queue.to_list(state.queue) do
      Connection.reply(from, error)
    end

    %{state | queue: :queue.new}
  end

  defp format_resp(%Redix.Error{} = err), do: {:error, err}
  defp format_resp(resp), do: {:ok, resp}
end
