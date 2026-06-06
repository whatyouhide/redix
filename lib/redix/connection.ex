defmodule Redix.Connection do
  @moduledoc false

  alias Redix.{ConnectionError, Format, Protocol, SocketOwner, StartOptions}

  require Logger

  @behaviour :gen_statem

  defstruct [
    :opts,
    :transport,
    :socket_owner,
    :table,
    :socket,
    :backoff_current,
    :connected_address,
    :health_marker,
    counter: 0,
    client_reply: :on
  ]

  @backoff_exponent 1.5

  ## Public API

  def start_link(opts) when is_list(opts) do
    opts = StartOptions.sanitize(:redix, opts)
    {gen_statem_opts, opts} = Keyword.split(opts, [:hibernate_after, :debug, :spawn_opt])

    case Keyword.fetch(opts, :name) do
      :error ->
        :gen_statem.start_link(__MODULE__, opts, gen_statem_opts)

      {:ok, atom} when is_atom(atom) ->
        :gen_statem.start_link({:local, atom}, __MODULE__, opts, gen_statem_opts)

      {:ok, {:global, _term} = tuple} ->
        :gen_statem.start_link(tuple, __MODULE__, opts, gen_statem_opts)

      {:ok, {:via, via_module, _term} = tuple} when is_atom(via_module) ->
        :gen_statem.start_link(tuple, __MODULE__, opts, gen_statem_opts)

      {:ok, other} ->
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

  def pipeline(conn, commands, timeout, telemetry_metadata) do
    conn_pid = GenServer.whereis(conn)

    # The reference doubles as a process alias (OTP 24+): the connection replies to the
    # alias rather than to our pid. If we time out, demonitoring deactivates the alias, so
    # any late reply is silently dropped instead of leaking into our mailbox.
    request_id = Process.monitor(conn_pid, alias: :demonitor)

    telemetry_metadata = telemetry_pipeline_metadata(conn, conn_pid, commands, telemetry_metadata)

    start_time = System.monotonic_time()
    :ok = execute_telemetry_pipeline_start(telemetry_metadata)

    # We cast to the connection process knowing that it will reply at some point (to the
    # alias) when a response is ready. We enforce the timeout on our side.
    :ok = :gen_statem.cast(conn_pid, {:pipeline, commands, _from = request_id})

    receive do
      {^request_id, resp} ->
        _ = Process.demonitor(request_id, [:flush])
        :ok = execute_telemetry_pipeline_stop(telemetry_metadata, start_time, resp)
        resp

      {:DOWN, ^request_id, _, _, reason} ->
        exit({:redix_exited_during_call, reason})
    after
      timeout ->
        _ = Process.demonitor(request_id, [:flush])
        resp = {:error, %ConnectionError{reason: :timeout}}
        :ok = execute_telemetry_pipeline_stop(telemetry_metadata, start_time, resp)
        resp
    end
  end

  defp telemetry_pipeline_metadata(conn, conn_pid, commands, telemetry_metadata) do
    name =
      if is_pid(conn) do
        nil
      else
        conn
      end

    %{
      connection: conn_pid,
      connection_name: name,
      commands: commands,
      extra_metadata: telemetry_metadata
    }
  end

  defp execute_telemetry_pipeline_start(metadata) do
    measurements = %{system_time: System.system_time()}
    :ok = :telemetry.execute([:redix, :pipeline, :start], measurements, metadata)
  end

  defp execute_telemetry_pipeline_stop(metadata, start_time, response) do
    measurements = %{duration: System.monotonic_time() - start_time}

    metadata =
      case response do
        {:ok, _response} -> metadata
        {:error, reason} -> Map.merge(metadata, %{kind: :error, reason: reason})
      end

    :ok = :telemetry.execute([:redix, :pipeline, :stop], measurements, metadata)
  end

  ## Callbacks

  ## Init callbacks

  @impl true
  def callback_mode, do: :state_functions

  @impl true
  def init(opts) do
    transport = if(opts[:ssl], do: :ssl, else: :gen_tcp)
    queue_table = :ets.new(:queue, [:ordered_set, :public])
    {:ok, socket_owner} = SocketOwner.start_link(self(), opts, queue_table)

    data = %__MODULE__{
      opts: opts,
      table: queue_table,
      socket_owner: socket_owner,
      transport: transport
    }

    if opts[:sync_connect] do
      # We don't need to handle a timeout here because we're using a timeout in
      # connect/3 down the pipe.
      receive do
        {:connected, ^socket_owner, socket, address} ->
          :telemetry.execute([:redix, :connection], %{}, %{
            connection: self(),
            connection_name: data.opts[:name],
            address: address,
            reconnection: false
          })

          data = %__MODULE__{data | socket: socket, connected_address: address}
          {:ok, :connected, data, health_check_actions(data)}

        {:stopped, ^socket_owner, reason} ->
          {:stop, %Redix.ConnectionError{reason: reason}}
      end
    else
      {:ok, :connecting, data}
    end
  end

  @impl true
  def terminate(reason, _state, data) do
    if Process.alive?(data.socket_owner) and reason == :normal do
      :ok = SocketOwner.normal_stop(data.socket_owner)
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

  def disconnected(:internal, {:notify_of_disconnection, _reason}, %__MODULE__{table: table}) do
    fun = fn {_counter, from, _ncommands}, _acc ->
      reply(from, {:error, %ConnectionError{reason: :disconnected}})
    end

    :ets.foldl(fun, nil, table)
    :ets.delete_all_objects(table)

    :keep_state_and_data
  end

  def disconnected(:cast, {:pipeline, _commands, from}, _data) do
    reply(from, {:error, %ConnectionError{reason: :closed}})
    :keep_state_and_data
  end

  # A health-check timeout is a *generic* (named) gen_statem timeout, which is not
  # auto-cancelled when we leave the :connected state. If a normal disconnection happens
  # with a health check armed, the leftover timer can fire here: ignore it. The next time
  # we reach :connected we arm a fresh timer (which replaces any leftover by name).
  def disconnected({:timeout, :health_check}, _timer_info, _data) do
    :keep_state_and_data
  end

  # This happens when there's a send error. We close the socket right away, but we wait for
  # the socket owner to die so that it can finish processing the data it's processing. When it's
  # dead, we go ahead and notify the remaining clients, setup backoff, and so on.
  def disconnected(:info, {:stopped, owner, reason}, %__MODULE__{socket_owner: owner} = data) do
    :telemetry.execute([:redix, :disconnection], %{}, %{
      connection: self(),
      connection_name: data.opts[:name],
      address: data.connected_address,
      reason: %ConnectionError{reason: reason}
    })

    data = %{data | connected_address: nil}
    disconnect(data, reason)
  end

  def connecting(
        :info,
        {:connected, owner, socket, address},
        %__MODULE__{socket_owner: owner} = data
      ) do
    :telemetry.execute([:redix, :connection], %{}, %{
      connection: self(),
      connection_name: data.opts[:name],
      address: address,
      reconnection: not is_nil(data.backoff_current)
    })

    data = %{
      data
      | socket: socket,
        backoff_current: nil,
        connected_address: address,
        health_marker: nil
    }

    {:next_state, :connected, %{data | socket: socket}, health_check_actions(data)}
  end

  def connecting(:cast, {:pipeline, _commands, _from}, _data) do
    {:keep_state_and_data, :postpone}
  end

  # See the matching clause in disconnected/3: a leftover health-check timer can fire while
  # we're reconnecting. Ignore it; it'll be re-armed once we're connected again.
  def connecting({:timeout, :health_check}, _timer_info, _data) do
    :keep_state_and_data
  end

  def connecting(:info, {:stopped, owner, reason}, %__MODULE__{socket_owner: owner} = data) do
    # We log this when the socket owner stopped while connecting.
    :telemetry.execute([:redix, :failed_connection], %{}, %{
      connection: self(),
      connection_name: data.opts[:name],
      address: format_address(data),
      reason: %ConnectionError{reason: reason}
    })

    disconnect(data, reason)
  end

  def connected(:cast, {:pipeline, commands, from}, data) do
    {ncommands, data} = get_client_reply(data, commands)

    if ncommands > 0 do
      {counter, data} = get_and_update_in(data.counter, &{&1, &1 + 1})

      row = {counter, from, ncommands}
      :ets.insert(data.table, row)

      case data.transport.send(data.socket, Enum.map(commands, &Protocol.pack/1)) do
        :ok ->
          {:keep_state, data}

        {:error, _reason} ->
          # The socket owner is not guaranteed to get a "closed" message, even if we close the
          # socket here. So, we move to the disconnected state but also notify the owner that
          # sending failed. If the owner already got the "closed" message, it exited so this
          # message goes nowere, otherwise the socket owner will exit and notify the connection.
          # See https://github.com/whatyouhide/redix/issues/265.
          :ok = data.transport.close(data.socket)
          send(data.socket_owner, {:send_errored, self()})
          {:next_state, :disconnected, data}
      end
    else
      reply(from, {:ok, []})
      {:keep_state, data}
    end
  end

  def connected(:info, {:stopped, owner, reason}, %__MODULE__{socket_owner: owner} = data) do
    :telemetry.execute([:redix, :disconnection], %{}, %{
      connection: self(),
      connection_name: data.opts[:name],
      address: data.connected_address,
      reason: %ConnectionError{reason: reason}
    })

    data = %{data | connected_address: nil}
    disconnect(data, reason)
  end

  # Periodic liveness check (only armed when :health_check_interval is set). We use the
  # commands queue table as the progress signal: the socket owner deletes a row as soon as
  # it receives the matching reply, and command counters are monotonic, so the smallest key
  # (`:ets.first/1`) is the oldest in-flight command and can never repeat for a different one.
  def connected({:timeout, :health_check}, _timer_info, %__MODULE__{} = data) do
    case :ets.first(data.table) do
      :"$end_of_table" ->
        # Nothing in flight: the connection is idle, so there's nothing to check.
        {:keep_state, %{data | health_marker: nil}, health_check_actions(data)}

      oldest when oldest == data.health_marker ->
        # The same command has been in flight for a full interval without a reply. The socket
        # is still open but the server isn't making progress (half-open connection, e.g. an
        # old primary paused during a Sentinel failover). Tear it down so we reconnect and,
        # for Sentinel, re-resolve the current primary. We let the socket owner stop (which
        # closes the socket it owns) and drive the disconnection through {:stopped, ...}.
        send(data.socket_owner, {:force_disconnect, self(), :health_check_timeout})
        {:keep_state, %{data | health_marker: nil}}

      oldest ->
        # Either progress was made since the last tick or this is the first time we see this
        # command. Remember it and re-check next interval.
        {:keep_state, %{data | health_marker: oldest}, health_check_actions(data)}
    end
  end

  ## Helpers

  defp health_check_actions(%__MODULE__{opts: opts}) do
    case Keyword.fetch!(opts, :health_check_interval) do
      :infinity -> []
      interval -> [{{:timeout, :health_check}, interval, nil}]
    end
  end

  # `from` is a process alias (see pipeline/4). Sending to a deactivated alias (e.g. after
  # the caller timed out) is silently dropped, so we never leak late replies.
  defp reply(alias_ref, reply) do
    send(alias_ref, {alias_ref, reply})
  end

  defp disconnect(_data, %Redix.Error{} = error) do
    Logger.error("Disconnected from Redis due to error: #{Exception.message(error)}")
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

  defp get_client_reply(data, commands) do
    {ncommands, client_reply} = get_client_reply(commands, _ncommands = 0, data.client_reply)
    {ncommands, put_in(data.client_reply, client_reply)}
  end

  defp get_client_reply([], ncommands, client_reply) do
    {ncommands, client_reply}
  end

  defp get_client_reply([command | rest], ncommands, client_reply) do
    case parse_client_reply(command) do
      :off -> get_client_reply(rest, ncommands, :off)
      :skip when client_reply == :off -> get_client_reply(rest, ncommands, :off)
      :skip -> get_client_reply(rest, ncommands, :skip)
      :on -> get_client_reply(rest, ncommands + 1, :on)
      nil when client_reply == :on -> get_client_reply(rest, ncommands + 1, client_reply)
      nil when client_reply == :off -> get_client_reply(rest, ncommands, client_reply)
      nil when client_reply == :skip -> get_client_reply(rest, ncommands, :on)
    end
  end

  defp parse_client_reply(["CLIENT", "REPLY", "ON"]), do: :on
  defp parse_client_reply(["CLIENT", "REPLY", "OFF"]), do: :off
  defp parse_client_reply(["CLIENT", "REPLY", "SKIP"]), do: :skip
  defp parse_client_reply(["client", "reply", "on"]), do: :on
  defp parse_client_reply(["client", "reply", "off"]), do: :off
  defp parse_client_reply(["client", "reply", "skip"]), do: :skip

  defp parse_client_reply([part1, part2, part3])
       when is_binary(part1) and byte_size(part1) == byte_size("CLIENT") and is_binary(part2) and
              byte_size(part2) == byte_size("REPLY") and
              is_binary(part3) and
              byte_size(part3) in [byte_size("ON"), byte_size("OFF"), byte_size("SKIP")] do
    # We need to do this in a "lazy" way: upcase the first string and check, then the second
    # one, and then the third one. Before, we were upcasing all three parts first and then
    # checking for a CLIENT REPLY * command. That meant that sometimes we would upcase huge
    # but completely unrelated commands causing big memory and CPU spikes. See
    # https://github.com/whatyouhide/redix/issues/177. "if" works here because and/2
    # short-circuits.
    if String.upcase(part1) == "CLIENT" and String.upcase(part2) == "REPLY" do
      case String.upcase(part3) do
        "ON" -> :on
        "OFF" -> :off
        "SKIP" -> :skip
        _other -> nil
      end
    else
      nil
    end
  end

  defp parse_client_reply(_other), do: nil

  defp format_address(%{opts: opts} = _state) do
    if opts[:sentinel] do
      "sentinel"
    else
      Format.format_host_and_port(opts[:host], opts[:port])
    end
  end
end
