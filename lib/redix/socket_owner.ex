defmodule Redix.SocketOwner do
  @moduledoc false

  use GenServer

  alias Redix.{Connector, Protocol}

  defstruct [
    :conn,
    :opts,
    :transport,
    :socket,
    :queue_table,
    :continuation
  ]

  def start_link(conn, opts, queue_table) do
    GenServer.start_link(__MODULE__, {conn, opts, queue_table}, [])
  end

  def normal_stop(conn) do
    GenServer.stop(conn, :normal)
  end

  @impl true
  def init({conn, opts, queue_table}) do
    state = %__MODULE__{
      conn: conn,
      opts: opts,
      queue_table: queue_table,
      transport: if(opts[:ssl], do: :ssl, else: :gen_tcp)
    }

    send(self(), :connect)

    {:ok, state}
  end

  @impl true
  def handle_info(msg, state)

  def handle_info(:connect, state) do
    with {:ok, socket, address} <- Connector.connect(state.opts, state.conn),
         :ok <- setopts(state, socket, active: :once) do
      send(state.conn, {:connected, self(), socket, address})
      {:noreply, %{state | socket: socket}}
    else
      {:error, reason} -> stop(reason, state)
      {:stop, reason} -> stop(reason, state)
    end
  end

  # The connection is notifying the socket owner that sending failed. If the socket owner
  # gets this, it can stop normally without waiting for the "closed"/"error" network
  # message from the socket.
  def handle_info({:send_errored, conn}, %__MODULE__{conn: conn} = state) do
    error =
      case state.transport do
        :ssl -> {:ssl_error, :closed}
        :gen_tcp -> {:tcp_error, :closed}
      end

    stop(error, state)
  end

  def handle_info({transport, socket, data}, %__MODULE__{socket: socket} = state)
      when transport in [:tcp, :ssl] do
    :ok = setopts(state, socket, active: :once)
    state = new_data(state, data)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %__MODULE__{socket: socket} = state) do
    stop(:tcp_closed, state)
  end

  def handle_info({:tcp_error, socket, reason}, %__MODULE__{socket: socket} = state) do
    stop({:tcp_error, reason}, state)
  end

  def handle_info({:ssl_closed, socket}, %__MODULE__{socket: socket} = state) do
    stop(:ssl_closed, state)
  end

  def handle_info({:ssl_error, socket, reason}, %__MODULE__{socket: socket} = state) do
    stop({:ssl_error, reason}, state)
  end

  ## Helpers

  defp setopts(%__MODULE__{transport: transport}, socket, opts) do
    case transport do
      :ssl -> :ssl.setopts(socket, opts)
      :gen_tcp -> :inet.setopts(socket, opts)
    end
  end

  defp new_data(state, _data = "") do
    state
  end

  defp new_data(%{continuation: nil} = state, data) do
    ncommands = peek_element_in_queue(state.queue_table, 3)
    continuation = &Protocol.parse_multi(&1, ncommands)
    new_data(%{state | continuation: continuation}, data)
  end

  defp new_data(%{continuation: continuation} = state, data) do
    case continuation.(data) do
      {:ok, resp, rest} ->
        {_counter, {pid, request_id}, _ncommands, timed_out?} =
          take_first_in_queue(state.queue_table)

        if not timed_out? do
          send(pid, {request_id, {:ok, resp}})
        end

        new_data(%{state | continuation: nil}, rest)

      {:continuation, cont} ->
        %{state | continuation: cont}
    end
  end

  defp peek_element_in_queue(queue_table, index) do
    case :ets.first(queue_table) do
      :"$end_of_table" ->
        # We can blow up here because there is nothing we can do.
        # See https://github.com/whatyouhide/redix/issues/192
        raise """
        failed to find an original command in the commands queue. This can happen, for example, \
        when the Redis server you are using does not support CLIENT commands. Redix issues \
        CLIENT commands under the hood when you use noreply_pipeline/3 and other noreply_* \
        functions. If that's not what you are doing, you might have found a bug in Redix: \
        please open an issue! https://github.com/whatyouhide/redix/issues
        """

      first_key ->
        :ets.lookup_element(queue_table, first_key, index)
    end
  end

  defp take_first_in_queue(queue_table) do
    first_key = :ets.first(queue_table)
    [first_client] = :ets.take(queue_table, first_key)
    first_client
  end

  defp stop(reason, %__MODULE__{conn: conn} = state) do
    send(conn, {:stopped, self(), reason})
    {:stop, :normal, state}
  end
end
