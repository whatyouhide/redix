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
  def connect(info, s)

  def connect(info, s) do
    ConnectionUtils.connect(info, s)
  end

  @doc false
  def disconnect(reason, s)

  def disconnect(:stop, %{socket: nil} = s) do
    {:stop, :normal, s}
  end

  def disconnect(:stop, %{socket: socket} = s) do
    :gen_tcp.close(socket)
    {:stop, :normal, %{s | socket: nil}}
  end

  def disconnect({:error, reason} = _error, %{queue: _queue} = s) do
    Logger.error "Disconnected from Redis (#{ConnectionUtils.host_for_logging(s)}): #{inspect reason}"

    for {:commands, from, _} <- :queue.to_list(s.queue) do
      Connection.reply(from, {:error, :disconnected})
    end

    # Backoff with 0 ms as the backoff time to churn through all the commands in
    # the mailbox before reconnecting.
    s
    |> reset_state
    |> ConnectionUtils.backoff_or_stop(0, reason)
  end

  @doc false
  def handle_call(operation, from, s)

  def handle_call(_operation, _from, %{socket: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:commands, commands}, from, s) do
    s
    |> Map.update!(:queue, &:queue.in({:commands, from, length(commands)}, &1))
    |> ConnectionUtils.send_noreply(Enum.map(commands, &Protocol.pack/1))
  end

  @doc false
  def handle_cast(operation, s)

  def handle_cast(:stop, s) do
    {:disconnect, :stop, s}
  end

  @doc false
  def handle_info(msg, s)

  def handle_info({:tcp, socket, data}, %{socket: socket} = s) do
    :ok = :inet.setopts(socket, active: :once)
    s = new_data(s, s.tail <> data)
    {:noreply, s}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = s) do
    {:disconnect, {:error, :tcp_closed}, s}
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = s) do
    {:disconnect, {:error, reason}, s}
  end

  ## Helper functions

  defp new_data(s, <<>>) do
    %{s | tail: <<>>}
  end

  defp new_data(s, data) do
    {{:value, {:commands, from, ncommands}}, new_queue} = :queue.out(s.queue)

    case Protocol.parse_multi(data, ncommands) do
      {:ok, resp, rest} ->
        Connection.reply(from, format_resp(resp))
        s = %{s | queue: new_queue}
        new_data(s, rest)
      {:error, :incomplete} ->
        %{s | tail: data}
    end
  end

  defp format_resp(%Redix.Error{} = err), do: {:error, err}
  defp format_resp(resp), do: {:ok, resp}

  defp reset_state(s) do
    %{s | queue: :queue.new, tail: "", socket: nil}
  end
end
