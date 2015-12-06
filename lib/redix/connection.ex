defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.ConnectionUtils
  alias Redix.Connection.Receiver
  require Logger

  @type state :: %{}

  @initial_state %{
    # The TCP socket that holds the connection to Redis
    socket: nil,
    # Options passed when the connection is started
    opts: nil,
    # The number of times a reconnection has been attempted
    reconnection_attempts: 0,
    # The receiver process
    receiver: nil,

    # TODO remove but used by Auth right now
    tail: "",
  }

  ## Callbacks

  @doc false
  def init(opts) do
    {:connect, :init, Dict.merge(@initial_state, opts: opts)}
  end

  @doc false
  def connect(info, state)

  def connect(info, state) do
    case ConnectionUtils.connect(info, state) do
      {:ok, state} ->
        state = start_receiver_and_hand_socket(state)
        {:ok, state}
      other ->
        other
    end
  end

  @doc false
  def disconnect(reason, state)

  def disconnect(:stop, state) do
    {:stop, :normal, state}
  end

  def disconnect({:error, reason} = _error, state) do
    Logger.error ["Disconnected from Redis (#{ConnectionUtils.format_host(state)}): ",
                  :inet.format_error(reason)]

    :gen_tcp.close(state.socket)

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
    :ok = Receiver.enqueue(state.receiver, {:commands, from, length(commands)})
    ConnectionUtils.send_noreply(state, Enum.map(commands, &Protocol.pack/1))
  end

  @doc false
  def handle_cast(operation, state)

  def handle_cast(:stop, state) do
    {:disconnect, :stop, state}
  end

  @doc false
  def handle_info(msg, state)

  def handle_info({:receiver, pid, msg}, %{receiver: pid} = state) do
    handle_msg_from_receiver(msg, state)
  end

  ## Helper functions

  defp reset_state(state) do
    %{state | socket: nil}
  end

  defp start_receiver_and_hand_socket(%{socket: socket, tail: tail, receiver: receiver} = state) do
    if receiver && Process.alive?(receiver) do
      raise "there already is a receiver: #{inspect receiver}"
    end

    {:ok, receiver} = Receiver.start_link(sender: self(), socket: socket, tail: tail)
    :ok = :gen_tcp.controlling_process(socket, receiver)
    %{state | receiver: receiver, tail: ""}
  end

  defp handle_msg_from_receiver({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :tcp_closed}, state}
  end

  defp handle_msg_from_receiver({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end
end
