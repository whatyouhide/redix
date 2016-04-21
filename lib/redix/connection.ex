defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.Utils
  alias Redix.Connection.Receiver

  require Logger

  @type state :: %__MODULE__{}

  defstruct [
    # The TCP socket that holds the connection to Redis
    socket: nil,
    # Options passed when the connection is started
    opts: nil,
    # The receiver process
    receiver: nil,
    # TODO: remove but used by Auth right now
    tail: "",
    # TODO: document this
    continuation: nil,
    # TODO: document this
    current_backoff: nil,
  ]

  @initial_backoff 500

  @backoff_exponent 1.5

  ## Callbacks

  @doc false
  def init(opts) do
    state = %__MODULE__{opts: opts}

    if opts[:sync_connect] do
      sync_connect(state)
    else
      {:connect, :init, state}
    end
  end

  @doc false
  def connect(info, state)

  def connect(info, state) do
    case Utils.connect(state) do
      {:ok, state} ->
        state = start_receiver_and_hand_socket(state)
        {:ok, state}
      {:error, reason} ->
        Logger.error [
          "Failed to connect to Redis (", Utils.format_host(state), "): ",
          Utils.format_error(reason),
        ]

        if info == :init do
          {:backoff, @initial_backoff, %{state | current_backoff: @initial_backoff}}
        else
          max_backoff = state.opts[:max_backoff]
          next_exponential_backoff = round(state.current_backoff * @backoff_exponent)
          next_backoff =
            if max_backoff == :infinity do
              next_exponential_backoff
            else
              min(next_exponential_backoff, max_backoff)
            end
          {:backoff, next_backoff, %{state | current_backoff: next_backoff}}
        end
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
    Logger.error ["Disconnected from Redis (#{Utils.format_host(state)}): ",
                  Utils.format_error(reason)]

    :gen_tcp.close(state.socket)

    {:backoff, @initial_backoff, %{reset_state(state) | current_backoff: @initial_backoff}}
  end

  @doc false
  def handle_call(operation, from, state)

  def handle_call(_operation, _from, %{socket: nil} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:commands, commands, request_id}, from, state) do
    :ok = Receiver.enqueue(state.receiver, {:commands, from, length(commands), request_id})
    Utils.send_noreply(state, Enum.map(commands, &Protocol.pack/1))
  end

  def handle_call({:timed_out, request_id}, _from, state) do
    :ok = Receiver.timed_out(state.receiver, request_id)
    {:reply, :ok, state}
  end

  def handle_call({:cancel_timed_out, request_id}, _from, state) do
    :ok = Receiver.cancel_timed_out(state.receiver, request_id)
    {:reply, :ok, state}
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

  defp sync_connect(state) do
    case Utils.connect(state) do
      {:ok, state} ->
        state = start_receiver_and_hand_socket(state)
        {:ok, state}
      {:error, reason} ->
        {:stop, reason}
      {:stop, reason, _state} ->
        {:stop, reason}
    end
  end

  defp reset_state(state) do
    %{state | socket: nil}
  end

  defp start_receiver_and_hand_socket(%{socket: socket, tail: tail, receiver: receiver} = state) do
    if receiver && Process.alive?(receiver) do
      raise "there already is a receiver: #{inspect receiver}"
    end

    {:ok, receiver} = Receiver.start_link(sender: self(), socket: socket, initial_data: tail)
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
