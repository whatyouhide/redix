defmodule Redix.Connection.Receiver do
  @moduledoc false

  use GenServer

  alias Redix.Protocol

  @initial_state %{
    # The process that sends stuff to the socket and that spawns this process
    sender: nil,
    # The queue of commands issued to Redis
    queue: :queue.new,
    # The TCP socket, which should be passive when given to this process
    socket: nil,

    initial_data: "",

    continuation: nil,

    timed_out_requests: HashSet.new,
  }

  @doc """
  Starts this genserver.

  Options in `opts` are injected directly in the state of this genserver.
  """
  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Puts `what` in the internal queue of genserver `pid` asynchronously (cast).
  """
  @spec enqueue(pid, term) :: :ok
  def enqueue(pid, what) do
    GenServer.cast(pid, {:enqueue, what})
  end

  @doc """
  Notifies the receiver that `request_id` timed out.
  """
  @spec timed_out(pid, reference) :: :ok
  def timed_out(pid, request_id) do
    GenServer.call(pid, {:timed_out, request_id})
  end

  @doc """
  Cancels the "timed out" notification for `request_id` stored via
  `timed_out/2`.
  """
  @spec cancel_timed_out(pid, reference) :: :ok
  def cancel_timed_out(pid, request_id) do
    GenServer.call(pid, {:cancel_timed_out, request_id})
  end

  ## Callbacks

  @doc false
  def init(opts) do
    state = Enum.into(opts, @initial_state)
    :inet.setopts(state.socket, active: :once)
    {:ok, state}
  end

  @doc false
  def handle_call(operation, from, state)

  def handle_call({:timed_out, request_id}, _from, state) do
    state = %{state | timed_out_requests: HashSet.put(state.timed_out_requests, request_id)}
    {:reply, :ok, state}
  end

  def handle_call({:cancel_timed_out, request_id}, _from, state) do
    state = %{state | timed_out_requests: HashSet.delete(state.timed_out_requests, request_id)}
    {:reply, :ok, state}
  end

  @doc false
  def handle_cast({:enqueue, what}, state) do
    state = update_in(state.queue, &:queue.in(what, &1))
    {:noreply, state}
  end

  @doc false
  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)

    state =
      if initial = state.initial_data do
        new_data(%{state | initial_data: nil}, initial <> data)
      else
        new_data(state, data)
      end

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
    state
  end

  defp new_data(state, data) do
    {{:value, {:commands, from, ncommands, request_id}}, new_queue} = :queue.out(state.queue)
    parser = state.continuation || &Protocol.parse_multi(&1, ncommands)

    case parser.(data) do
      {:ok, resp, rest} ->
        state =
          if HashSet.member?(state.timed_out_requests, request_id) do
            %{state | timed_out_requests: HashSet.delete(state.timed_out_requests, request_id)}
          else
            Connection.reply(from, format_resp(resp))
            state
          end
        state = %{state | queue: new_queue, continuation: nil}
        new_data(state, rest)
      {:continuation, cont} ->
        %{state | continuation: cont}
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
