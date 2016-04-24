defmodule Redix.Connection.Receiver do
  @moduledoc false

  use GenServer

  alias Redix.Protocol
  alias Redix.Connection.TimeoutStore

  defstruct [
    # The process that sends stuff to the socket and that spawns this process
    sender: nil,
    # The queue of commands issued to Redis
    queue: :queue.new,
    # The TCP socket, which should be passive when given to this process
    socket: nil,
    # TODO: document this
    continuation: nil,
    # TODO: document this
    timeout_store: nil,
  ]

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

  ## Callbacks

  @doc false
  def init(opts) do
    state = struct(__MODULE__, opts)
    :inet.setopts(state.socket, active: :once)
    {:ok, state}
  end

  @doc false
  def handle_cast({:enqueue, what}, state) do
    state = update_in(state.queue, &:queue.in(what, &1))
    {:noreply, state}
  end

  @doc false
  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)

    state = new_data(state, data)

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
        unless TimeoutStore.timed_out?(state.timeout_store, request_id) do
          Connection.reply(from, format_resp(resp))
        end
        state = %{state | queue: new_queue, continuation: nil}
        new_data(state, rest)
      {:continuation, cont} ->
        %{state | continuation: cont}
    end
  end

  defp disconnect(msg, error, state) do
    # We notify all commands in the queue of the disconnection.
    for {:commands, from, _, _} <- :queue.to_list(state.queue) do
      Connection.reply(from, error)
    end

    send state.sender, {:receiver, self(), msg}
    {:stop, :normal, state}
  end

  defp format_resp(%Redix.Error{} = err), do: {:error, err}
  defp format_resp(resp), do: {:ok, resp}
end
