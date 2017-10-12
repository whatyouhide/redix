defmodule Redix.Connection.Receiver do
  @moduledoc false

  use GenServer

  alias Redix.Protocol
  alias Redix.Connection.SharedState

  ## GenServer state

  # sender: The process that sends stuff to the socket and that spawns this process
  # socket: The TCP socket, which should be passive when given to this process
  # continuation: The parsing continuation returned by Redix.Protocol
  # current_client: The client that we'll need to reply to once the current continuation is done
  # shared_state: The shared state process
  defstruct sender: nil,
            socket: nil,
            continuation: nil,
            current_client: nil,
            shared_state: nil

  ## Public API

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec stop(GenServer.server()) :: :ok
  def stop(pid) do
    GenServer.cast(pid, :stop)
  end

  ## GenServer callbacks

  @doc false
  def init(opts) do
    # The socket will be activated once this process is made the controlling
    # process of the socket.
    state = struct(__MODULE__, opts)
    {:ok, state}
  end

  @doc false
  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  @doc false
  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)

    state = new_data(state, data)

    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket} = msg, %{socket: socket} = state) do
    disconnect(msg, state)
  end

  def handle_info({:tcp_error, socket, _reason} = msg, %{socket: socket} = state) do
    disconnect(msg, state)
  end

  ## Helpers

  defp new_data(state, <<>>) do
    %{state | current_client: nil, continuation: nil}
  end

  defp new_data(%{continuation: nil} = state, data) do
    {timed_out_request?, {:commands, request_id, from, ncommands}} =
      client = SharedState.dequeue(state.shared_state)

    case Protocol.parse_multi(data, ncommands) do
      {:ok, resp, rest} ->
        unless timed_out_request? do
          Connection.reply(from, {request_id, format_resp(resp)})
        end

        new_data(%{state | continuation: nil}, rest)

      {:continuation, cont} ->
        %{state | current_client: client, continuation: cont}
    end
  end

  defp new_data(%{current_client: current_client} = state, data) do
    {timed_out_request?, {:commands, request_id, from, _ncommands}} = current_client

    case state.continuation.(data) do
      {:ok, resp, rest} ->
        unless timed_out_request? do
          Connection.reply(from, {request_id, format_resp(resp)})
        end

        new_data(%{state | continuation: nil, current_client: nil}, rest)

      {:continuation, cont} ->
        %{state | continuation: cont}
    end
  end

  # We notify the sender of the error and stop normally.
  defp disconnect(msg, state) do
    send(state.sender, {:receiver, self(), msg})
    {:stop, :normal, state}
  end

  defp format_resp(%Redix.Error{} = error), do: {:error, error}
  defp format_resp(resp), do: {:ok, resp}
end
