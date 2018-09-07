defmodule Redix.Connection do
  @moduledoc false

  alias Redix.{ConnectionError, Protocol, SocketOwner, Utils}

  require Logger

  @behaviour :gen_statem

  defstruct [
    :opts,
    :socket_owner,
    :table,
    :socket,
    :backoff_current,
    counter: 0
  ]

  @backoff_exponent 1.5

  ## Public API

  def start_link(opts) when is_list(opts) do
    opts = Utils.sanitize_starting_opts(opts)

    case Keyword.pop(opts, :name) do
      {nil, opts} ->
        :gen_statem.start_link(__MODULE__, opts, [])

      {atom, opts} when is_atom(atom) ->
        :gen_statem.start_link({:local, atom}, __MODULE__, opts, [])

      {{:global, _term} = tuple, opts} ->
        :gen_statem.start_link(tuple, __MODULE__, opts, [])

      {{:via, via_module, _term} = tuple, opts} when is_atom(via_module) ->
        :gen_statem.start_link(tuple, __MODULE__, opts, [])

      {other, _opts} ->
        raise ArgumentError, """
        expected :name option to be one of the following:

          * nil
          * atom
          * {:global, term}
          * {:via, module, term}

        Got: #{inspect(other)}
        """
    end
  end

  def stop(conn, timeout) do
    :gen_statem.stop(conn, :normal, timeout)
  end

  def pipeline(conn, commands, timeout) do
    request_id = Process.monitor(conn)

    # We cast to the connection process knowing that it will reply at some point,
    # either after roughly timeout or when a response is ready.
    cast = {:pipeline, commands, _from = {self(), request_id}, timeout}
    :ok = :gen_statem.cast(GenServer.whereis(conn), cast)

    receive do
      {^request_id, resp} ->
        _ = Process.demonitor(request_id, [:flush])
        resp

      # TODO: is this right or should we crash?
      {:DOWN, ^request_id, _, _, _} ->
        {:error, %ConnectionError{reason: :disconnected}}
    end
  end

  ## Callbacks

  ## Init callbacks

  @impl true
  def callback_mode(), do: :state_functions

  @impl true
  def init(opts) do
    queue_table = :ets.new(:queue, [:ordered_set, :public])
    {:ok, socket_owner} = SocketOwner.start_link(self(), opts, queue_table)

    data = %__MODULE__{opts: opts, table: queue_table, socket_owner: socket_owner}

    if opts[:sync_connect] do
      receive do
        {:connected, ^socket_owner, socket} ->
          {:ok, :connected, %__MODULE__{data | socket: socket}}

        {:stopped, ^socket_owner, reason} ->
          {:stop, %Redix.ConnectionError{reason: reason}}
      end
    else
      {:ok, :connecting, data}
    end
  end

  ## State functions

  # "Disconnected" state: the connection is down and the socket owner is not alive.

  # We want to connect/reconnect. We start the socket owner process and then go in the :connecting
  # state.
  def disconnected({:timeout, :reconnect}, _timer_info, %__MODULE__{} = data) do
    {:ok, socket_owner} = SocketOwner.start_link(self(), data.opts, data.table)
    new_data = %{data | socket_owner: socket_owner}
    {:next_state, :connecting, new_data}
  end

  def disconnected({:timeout, {:client_timed_out, _counter}}, _from, _data) do
    :keep_state_and_data
  end

  def disconnected(:internal, {:notify_of_disconnection, _reason}, %__MODULE__{table: table}) do
    fun = fn {_counter, from, _ncommands, timed_out?}, _acc ->
      if not timed_out?, do: reply(from, {:error, %ConnectionError{reason: :disconnected}})
    end

    :ets.foldl(fun, nil, table)
    :ets.delete_all_objects(table)

    :keep_state_and_data
  end

  def disconnected(:cast, {:pipeline, _commands, from, _timeout}, _data) do
    reply(from, {:error, %ConnectionError{reason: :closed}})
    :keep_state_and_data
  end

  # This happens when there's a TCP send error. We close the socket right away, but we wait for
  # the socket owner to die so that it can finish processing the data it's processing. When it's
  # dead, we go ahead and notify the remaining clients, setup backoff, and so on.
  def disconnected(:info, {:stopped, owner, reason}, %__MODULE__{socket_owner: owner} = data) do
    log(data, :failed_connection, fn ->
      [
        "Disconnected from Redis (#{Utils.format_host(data)}): ",
        Exception.message(%ConnectionError{reason: reason})
      ]
    end)

    disconnect(data, reason)
  end

  def connecting(:info, {:connected, owner, socket}, %__MODULE__{socket_owner: owner} = data) do
    if data.backoff_current do
      log(data, :reconnection, fn -> "Reconnected to Redis (#{Utils.format_host(data)})" end)
    end

    data = %{data | socket: socket, backoff_current: nil}
    {:next_state, :connected, %{data | socket: socket}}
  end

  def connecting(:cast, {:pipeline, _commands, _from, _timeout}, _data) do
    {:keep_state_and_data, :postpone}
  end

  def connecting(:info, {:stopped, owner, reason}, %__MODULE__{socket_owner: owner} = data) do
    # We log this when the socket owner stopped while connecting.
    log(data, :failed_connection, fn ->
      [
        "Failed to connect to Redis (#{Utils.format_host(data)}): ",
        Exception.message(%ConnectionError{reason: reason})
      ]
    end)

    disconnect(data, reason)
  end

  def connecting({:timeout, {:client_timed_out, _counter}}, _from, _data) do
    :keep_state_and_data
  end

  def connected(:cast, {:pipeline, commands, from, timeout}, data) do
    {counter, data} = get_and_update_in(data.counter, &{&1, &1 + 1})

    row = {counter, from, length(commands), _timed_out? = false}
    :ets.insert(data.table, row)

    case :gen_tcp.send(data.socket, Enum.map(commands, &Protocol.pack/1)) do
      :ok ->
        actions =
          case timeout do
            :infinity -> []
            _other -> [{{:timeout, {:client_timed_out, counter}}, timeout, from}]
          end

        {:keep_state, data, actions}

      {:error, _reason} ->
        # The socket owner will get a TCP closed message at some point, so we just move to the
        # disconnected state.
        :ok = :gen_tcp.close(data.socket)
        {:next_state, :disconnected}
    end
  end

  def connected(:info, {:stopped, owner, reason}, %__MODULE__{socket_owner: owner} = data) do
    log(data, :failed_connection, fn ->
      [
        "Disconnected from Redis (#{Utils.format_host(data)}): ",
        Exception.message(%ConnectionError{reason: reason})
      ]
    end)

    disconnect(data, reason)
  end

  def connected({:timeout, {:client_timed_out, counter}}, from, %__MODULE__{} = data) do
    if _found? = :ets.update_element(data.table, counter, {4, _timed_out? = true}) do
      reply(from, {:error, %ConnectionError{reason: :timeout}})
    end

    :keep_state_and_data
  end

  ## Helpers

  defp reply({pid, request_id} = _from, reply) do
    send(pid, {request_id, reply})
  end

  defp disconnect(_data, %Redix.Error{} = error) do
    {:stop, error}
  end

  defp disconnect(data, reason) do
    if data.opts[:exit_on_disconnection] do
      {:stop, %ConnectionError{reason: reason}}
    else
      {backoff, data} = next_backoff(data)

      actions = [
        {:next_event, :internal, {:notify_of_disconnection, reason}},
        {{:timeout, :reconnect}, backoff, nil}
      ]

      {:next_state, :disconnected, data, actions}
    end
  end

  defp next_backoff(%__MODULE__{backoff_current: nil} = data) do
    backoff_initial = data.opts[:backoff_initial]
    {backoff_initial, %{data | backoff_current: backoff_initial}}
  end

  defp next_backoff(data) do
    next_exponential_backoff = round(data.backoff_current * @backoff_exponent)

    backoff_current =
      if data.opts[:backoff_max] == :infinity do
        next_exponential_backoff
      else
        min(next_exponential_backoff, Keyword.fetch!(data.opts, :backoff_max))
      end

    {backoff_current, %{data | backoff_current: backoff_current}}
  end

  defp log(data, kind, message) do
    level =
      data.opts
      |> Keyword.fetch!(:log)
      |> Keyword.fetch!(kind)

    Logger.log(level, message)
  end
end
