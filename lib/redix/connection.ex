defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.ConnectionUtils
  require Logger

  @type state :: %{}

  @initial_state %{
    # The TCP socket that holds the connection to Redis
    socket: nil,
    # The data that couldn't be parsed (yet)
    tail: "",
    # Options passed when the connection is started
    opts: nil,
    # A queue of operations waiting for a response
    queue: :queue.new,
    # The number of times a reconnection has been attempted
    reconnection_attempts: 0,
  }

  ## Callbacks

  @doc false
  def init(opts) do
    {:connect, :init, Dict.merge(@initial_state, opts: opts)}
  end

  @doc false
  def connect(info, state)

  def connect(info, state) do
    ConnectionUtils.connect(info, state)
  end

  @doc false
  def disconnect(reason, state)

  def disconnect(:stop, state) do
    {:stop, :normal, state}
  end

  def disconnect({:error, reason} = _error, %{queue: _queue} = state) do
    Logger.error "Disconnected from Redis (#{ConnectionUtils.format_host(state)}): #{:inet.format_error(reason)}"

    :gen_tcp.close(state.socket)

    for {:commands, from, _} <- :queue.to_list(state.queue) do
      Connection.reply(from, {:error, :disconnected})
    end

    # Backoff with 0 ms as the backoff time to churn through all the commands in
    # the mailbox before reconnecting.
    state
    |> reset_state
    |> ConnectionUtils.backoff_or_stop(0, reason)
  end

  @doc false
  def handle_call(operation, from, state)

  def handle_call(_operation, _from, %{socket: nil} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:commands, commands}, from, state) do
    state
    |> Map.update!(:queue, &:queue.in({:commands, from, length(commands)}, &1))
    |> ConnectionUtils.send_noreply(Enum.map(commands, &Protocol.pack/1))
  end

  @doc false
  def handle_cast(operation, state)

  def handle_cast(:stop, state) do
    {:disconnect, :stop, state}
  end

  @doc false
  def handle_info(msg, state)

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)
    state = new_data(state, state.tail <> data)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :tcp_closed}, state}
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  ## Helper functions

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

  defp format_resp(%Redix.Error{} = err), do: {:error, err}
  defp format_resp(resp), do: {:ok, resp}

  defp reset_state(state) do
    %{state | queue: :queue.new, tail: "", socket: nil}
  end
end
