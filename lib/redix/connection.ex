defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.Utils
  alias Redix.Connection.Receiver
  alias Redix.Connection.TimeoutStore

  require Logger

  @type state :: %__MODULE__{}

  defstruct [
    # The TCP socket that holds the connection to Redis
    socket: nil,
    # Options passed when the connection is started
    opts: nil,
    # The receiver process
    receiver: nil,
    # The timeout store process
    timeout_store: nil,
    # The current backoff (used to compute the next backoff when reconnecting
    # with exponential backoff)
    current_backoff: nil,
  ]

  # TODO: right now, if backoff_max is less then 500ms (=
  # @initial_backoff), we do weird stuff.
  @initial_backoff 500

  @backoff_exponent 1.5

  ## Public API

  def start_link(redis_opts, other_opts) do
    {redix_opts, connection_opts} = Utils.sanitize_starting_opts(redis_opts, other_opts)
    Connection.start_link(__MODULE__, redix_opts, connection_opts)
  end

  def stop(conn) do
    Connection.cast(conn, :stop)
  end

  def pipeline(conn, commands, timeout) do
    request_id = make_ref()

    # All this try-catch dance is required in order to cleanly return {:error,
    # :timeout} on timeouts instead of exiting (which is what `GenServer.call/3`
    # does). The whole process is described in Redix.Connection.TimeoutStore.
    try do
      {^request_id, resp} = Connection.call(conn, {:commands, commands, request_id}, timeout)
      resp
    catch
      :exit, {:timeout, {:gen_server, :call, [^conn | _]}} ->
        Connection.call(conn, {:timed_out, request_id})

        # We try to flush the response because it may have arrived before the
        # connection processed the :timed_out message. In case it arrived, we
        # notify the conncetion that it arrived (canceling the :timed_out
        # message).
        receive do
          {ref, {^request_id, _resp}} when is_reference(ref) ->
            :ok = Connection.call(conn, {:cancel_timed_out, request_id})
        after
          0 -> :ok
        end

        {:error, :timeout}
    end
  end

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
        {:ok, timeout_store} = TimeoutStore.start_link()
        receiver = start_receiver_and_hand_socket(state.socket, timeout_store)
        state = %{state | timeout_store: timeout_store, receiver: receiver}
        {:ok, state}
      {:error, reason} ->
        Logger.error [
          "Failed to connect to Redis (", Utils.format_host(state), "): ",
          Utils.format_error(reason),
        ]

        # If this is the first time we connect, then we just retry after the
        # initial backoff (there's no need to calculate backoff and so on).
        if info == :init do
          {:backoff, @initial_backoff, %{state | current_backoff: @initial_backoff}}
        else
          next_backoff = calc_next_backoff(state.current_backoff, state.opts[:backoff_max])
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
    Logger.error [
      "Disconnected from Redis (", Utils.format_host(state), "): ", Utils.format_error(reason),
    ]

    # When we're here, the receiver already exited with :normal by itself, but
    # we manually have to kill the TimeoutStore.
    :ok = TimeoutStore.stop(state.timeout_store)

    state = %{state |
      socket: nil,
      receiver: nil,
      timeout_store: nil,
      current_backoff: @initial_backoff,
    }
    {:backoff, @initial_backoff, state}
  end

  @doc false
  def handle_call(operation, from, state)

  def handle_call(_operation, _from, %{socket: nil} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:commands, commands, request_id}, from, state) do
    :ok = Receiver.enqueue(state.receiver, {:commands, from, length(commands), request_id})

    # `Utils.send_noreply/2` can wither return `{:noreply, state}` or
    # `{:disconnect, error, state}` where `error` is what is returned by
    # `:gen_tcp.send/2`. What can happen is this: the TCP connection to Redis
    # has problems, and the receiver gets notified about this (:tcp_closed or
    # :tcp_error); the receiver will closed the socket, notify the sender, and
    # die in peace (:normal). However, the sender may try to send messages to
    # the socket before it processes the message from the receiver telling it
    # the socket is closed. In this case, it's fine to still try to send,
    # `:gen_tcp.send/2` will return `{:error, :closed}` and we'll disconnect
    # from here as well. The socket is closed (so no need to do anything to it),
    # the receiver process will not leak (when we spawn a new one when
    # reconnecting) because the old one died right after notifying us. We should
    # be ok.
    Utils.send_noreply(state, Enum.map(commands, &Protocol.pack/1))
  end

  def handle_call({:timed_out, request_id}, _from, state) do
    :ok = TimeoutStore.add(state.timeout_store, request_id)
    {:reply, :ok, state}
  end

  def handle_call({:cancel_timed_out, request_id}, _from, state) do
    :ok = TimeoutStore.remove(state.timeout_store, request_id)
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
        {:ok, timeout_store} = TimeoutStore.start_link()
        receiver = start_receiver_and_hand_socket(state.socket, timeout_store)
        state = %{state | timeout_store: timeout_store, receiver: receiver}
        {:ok, state}
      {:error, reason} ->
        {:stop, reason}
      {:stop, reason, _state} ->
        {:stop, reason}
    end
  end

  defp start_receiver_and_hand_socket(socket, timeout_store) do
    {:ok, receiver} = Receiver.start_link(sender: self(), socket: socket, timeout_store: timeout_store)
    :ok = :gen_tcp.controlling_process(socket, receiver)
    receiver
  end

  defp handle_msg_from_receiver({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :tcp_closed}, state}
  end

  defp handle_msg_from_receiver({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  defp calc_next_backoff(current_backoff, backoff_max) do
    next_exponential_backoff = round(current_backoff * @backoff_exponent)

    if backoff_max == :infinity do
      next_exponential_backoff
    else
      min(next_exponential_backoff, backoff_max)
    end
  end
end
