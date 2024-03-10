defmodule Redix.PubSub.Connection do
  @moduledoc false

  @behaviour :gen_statem

  alias Redix.{ConnectionError, Connector, Format, Protocol}

  defstruct [
    :opts,
    :transport,
    :socket,
    :continuation,
    :backoff_current,
    :last_disconnect_reason,
    :connected_address,
    :client_id,
    subscriptions: %{},
    monitors: %{}
  ]

  @backoff_exponent 1.5

  @impl true
  def callback_mode, do: :state_functions

  @impl true
  def init(opts) do
    transport = if(opts[:ssl], do: :ssl, else: :gen_tcp)
    data = %__MODULE__{opts: opts, transport: transport}

    if opts[:sync_connect] do
      with {:ok, socket, address, client_id} <- connect(data) do
        data = %__MODULE__{
          data
          | socket: socket,
            last_disconnect_reason: nil,
            backoff_current: nil,
            connected_address: address,
            client_id: client_id
        }

        {:ok, :connected, data}
      else
        {:error, reason} -> {:stop, reason}
        {:stop, reason} -> {:stop, reason}
      end
    else
      send(self(), :handle_possible_erlang_bug)
      {:ok, :state_needed_because_of_possible_erlang_bug, data}
    end
  end

  ## States

  # If I use the action {:next_event, :internal, :connect} when returning
  # {:ok, :disconnected, data} from init/1, then Erlang 20 (not 21) blows up saying:
  # {:bad_return_from_init, {:next_events, :internal, :connect}}. The weird thing is
  # that if I use `{:next_even, :internal, :connect}` it complains but with `:next_even`,
  # but with `:next_event` it seems to add the final "s" (`:next_events`). No idea
  # what's going on and no time to fix it.
  def state_needed_because_of_possible_erlang_bug(:info, :handle_possible_erlang_bug, data) do
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  def state_needed_because_of_possible_erlang_bug(_event, _info, _data) do
    {:keep_state_and_data, :postpone}
  end

  def disconnected(:internal, :handle_disconnection, data) do
    :telemetry.execute([:redix, :disconnection], %{}, %{
      connection: self(),
      connection_name: data.opts[:name],
      address: data.connected_address,
      reason: data.last_disconnect_reason
    })

    if data.opts[:exit_on_disconnection] do
      {:stop, data.last_disconnect_reason}
    else
      :ok =
        Enum.each(data.monitors, fn {pid, ref} ->
          send(pid, ref, :disconnected, %{error: data.last_disconnect_reason})
        end)

      subscriptions =
        Map.new(data.subscriptions, fn
          {target_key, {:subscribed, subscribers}} ->
            {target_key, {:disconnected, subscribers}}

          {target_key, {:subscribing, subscribes, _unsubscribes}} ->
            {target_key, {:disconnected, subscribes}}

          {target_key, {:unsubscribing, resubscribers}} ->
            {target_key, {:disconnected, resubscribers}}
        end)

      data = %{data | subscriptions: subscriptions, connected_address: nil}

      {:keep_state, data}
    end
  end

  def disconnected({:timeout, :reconnect}, nil, _data) do
    {:keep_state_and_data, {:next_event, :internal, :connect}}
  end

  def disconnected(:internal, :connect, data) do
    with {:ok, socket, address, client_id} <- connect(data) do
      :telemetry.execute([:redix, :connection], %{}, %{
        connection: self(),
        connection_name: data.opts[:name],
        address: address,
        reconnection: not is_nil(data.last_disconnect_reason)
      })

      data = %__MODULE__{
        data
        | socket: socket,
          last_disconnect_reason: nil,
          backoff_current: nil,
          connected_address: address,
          client_id: client_id
      }

      {:next_state, :connected, data, {:next_event, :internal, :handle_connection}}
    else
      {:error, reason} ->
        :telemetry.execute([:redix, :failed_connection], %{}, %{
          connection: self(),
          connection_name: data.opts[:name],
          address: format_address(data),
          reason: %ConnectionError{reason: reason}
        })

        disconnect(data, reason, _handle_disconnection? = false)

      {:stop, reason} ->
        {:stop, reason, data}
    end
  end

  def disconnected({:call, from}, {operation, targets, pid}, data)
      when operation in [:subscribe, :psubscribe] do
    {data, ref} = monitor_new(data, pid)
    :ok = :gen_statem.reply(from, {:ok, ref})

    target_type =
      case operation do
        :subscribe -> :channel
        :psubscribe -> :pattern
      end

    data =
      Enum.reduce(targets, data, fn target_name, data_acc ->
        update_in(data_acc.subscriptions[{target_type, target_name}], fn
          {:disconnected, subscribers} -> {:disconnected, MapSet.put(subscribers, pid)}
          nil -> {:disconnected, MapSet.new([pid])}
        end)
      end)

    {:keep_state, data}
  end

  def disconnected({:call, from}, {operation, targets, pid}, data)
      when operation in [:unsubscribe, :punsubscribe] do
    :ok = :gen_statem.reply(from, :ok)

    target_type =
      case operation do
        :unsubscribe -> :channel
        :punsubscribe -> :pattern
      end

    data =
      Enum.reduce(targets, data, fn target_name, data_acc ->
        target_key = {target_type, target_name}

        case data_acc.subscriptions[target_key] do
          nil ->
            data_acc

          {:disconnected, subscribers} ->
            subscribers = MapSet.delete(subscribers, pid)

            if MapSet.size(subscribers) == 0 do
              update_in(data_acc.subscriptions, &Map.delete(&1, target_key))
            else
              put_in(data_acc.subscriptions[target_key], {:disconnected, subscribers})
            end
        end
      end)

    data = demonitor_if_not_subscribed_to_anything(data, pid)
    {:keep_state, data}
  end

  def disconnected({:call, from}, :get_client_id, _data) do
    reply = {:error, %ConnectionError{reason: :closed}}
    {:keep_state_and_data, {:reply, from, reply}}
  end

  def connected(:internal, :handle_connection, data) do
    if map_size(data.subscriptions) > 0 do
      case resubscribe_after_reconnection(data) do
        {:ok, data} -> {:keep_state, data}
        {:error, reason} -> disconnect(data, reason, _handle_disconnection? = true)
      end
    else
      {:keep_state, data}
    end
  end

  def connected({:call, from}, {operation, targets, pid}, data)
      when operation in [:subscribe, :psubscribe] do
    {data, ref} = monitor_new(data, pid)
    :ok = :gen_statem.reply(from, {:ok, ref})

    with {:ok, data} <- subscribe_pid_to_targets(data, operation, targets, pid) do
      {:keep_state, data}
    end
  end

  def connected({:call, from}, {operation, targets, pid}, data)
      when operation in [:unsubscribe, :punsubscribe] do
    :ok = :gen_statem.reply(from, :ok)

    with {:ok, data} <- unsubscribe_pid_from_targets(data, operation, targets, pid) do
      data = demonitor_if_not_subscribed_to_anything(data, pid)
      {:keep_state, data}
    end
  end

  def connected({:call, from}, :get_client_id, data) do
    reply =
      if id = data.client_id do
        {:ok, id}
      else
        {:error, %ConnectionError{reason: :client_id_not_stored}}
      end

    {:keep_state_and_data, {:reply, from, reply}}
  end

  def connected(:info, {transport_closed, socket}, %__MODULE__{socket: socket} = data)
      when transport_closed in [:tcp_closed, :ssl_closed] do
    disconnect(data, transport_closed, _handle_disconnection? = true)
  end

  def connected(:info, {transport_error, socket, reason}, %__MODULE__{socket: socket} = data)
      when transport_error in [:tcp_error, :ssl_error] do
    disconnect(data, reason, _handle_disconnection? = true)
  end

  def connected(:info, {transport, socket, bytes}, %__MODULE__{socket: socket} = data)
      when transport in [:tcp, :ssl] do
    with :ok <- setopts(data, socket, active: :once),
         {:ok, data} <- new_bytes(data, bytes) do
      {:keep_state, data}
    else
      {:error, reason} -> disconnect(data, reason, _handle_disconnection? = true)
    end
  end

  def connected(:info, {:DOWN, _ref, :process, pid, _reason}, data) do
    data = update_in(data.monitors, &Map.delete(&1, pid))

    targets = Map.keys(data.subscriptions)
    channels = for {:channel, channel} <- targets, do: channel
    patterns = for {:pattern, pattern} <- targets, do: pattern

    with {:ok, data} <- unsubscribe_pid_from_targets(data, :unsubscribe, channels, pid),
         {:ok, data} <- unsubscribe_pid_from_targets(data, :punsubscribe, patterns, pid) do
      {:keep_state, data}
    end
  end

  ## Helpers

  defp new_bytes(data, "") do
    {:ok, data}
  end

  defp new_bytes(data, bytes) do
    case (data.continuation || (&Protocol.parse/1)).(bytes) do
      {:ok, resp, rest} ->
        with {:ok, data} <- handle_pubsub_msg(data, resp),
             do: new_bytes(%{data | continuation: nil}, rest)

      {:continuation, continuation} ->
        {:ok, %{data | continuation: continuation}}
    end
  end

  defp handle_pubsub_msg(data, [operation, target, _subscribers_count])
       when operation in ["subscribe", "psubscribe"] do
    target_key =
      case operation do
        "subscribe" -> {:channel, target}
        "psubscribe" -> {:pattern, target}
      end

    {:subscribing, subscribes, _unsubscribes} = data.subscriptions[target_key]

    if MapSet.size(subscribes) == 0 do
      case send_unsubscriptions(data, [target_key]) do
        :ok ->
          data = put_in(data.subscriptions[target_key], {:unsubscribing, MapSet.new()})
          {:ok, data}

        {:error, reason} ->
          {:error, reason}
      end
    else
      Enum.each(subscribes, &send_subscription_confirmation(data, &1, target_key))
      data = put_in(data.subscriptions[target_key], {:subscribed, subscribes})
      {:ok, data}
    end
  end

  defp handle_pubsub_msg(data, [operation, target, _subscribers_count])
       when operation in ["unsubscribe", "punsubscribe"] do
    operation = String.to_existing_atom(operation)
    target_key = key_for_target(operation, target)

    {:unsubscribing, resubscribers} = data.subscriptions[target_key]

    if MapSet.size(resubscribers) == 0 do
      data = update_in(data.subscriptions, &Map.delete(&1, target_key))
      {:ok, data}
    else
      case send_subscriptions(data, [target_key]) do
        :ok ->
          data =
            put_in(data.subscriptions[target_key], {:subscribing, resubscribers, MapSet.new()})

          {:ok, data}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp handle_pubsub_msg(data, ["message", channel, payload]) do
    properties = %{channel: channel, payload: payload}
    handle_pubsub_message_with_payload(data, {:channel, channel}, :message, properties)
  end

  defp handle_pubsub_msg(data, ["pmessage", pattern, channel, payload]) do
    properties = %{channel: channel, pattern: pattern, payload: payload}
    handle_pubsub_message_with_payload(data, {:pattern, pattern}, :pmessage, properties)
  end

  defp handle_pubsub_message_with_payload(data, target_key, kind, properties) do
    case data.subscriptions[target_key] do
      {:subscribed, subscribers} ->
        for pid <- subscribers do
          send(pid, Map.fetch!(data.monitors, pid), kind, properties)
        end

      {:unsubscribing, _to_resubscribe} ->
        :ok
    end

    {:ok, data}
  end

  # Subscribing.

  defp subscribe_pid_to_targets(data, operation, targets, pid) do
    target_type =
      case operation do
        :subscribe -> :channel
        :psubscribe -> :pattern
      end

    {to_subscribe, data} =
      Enum.flat_map_reduce(targets, data, fn target_name, data_acc ->
        target_key = {target_type, target_name}

        {target_state, data_acc} =
          get_and_update_in(data_acc.subscriptions[target_key], &subscribe_pid_to_target(&1, pid))

        case target_state do
          :new ->
            {[target_key], data_acc}

          :already_subscribed ->
            send_subscription_confirmation(data_acc, pid, target_key)
            {[], data_acc}

          :pending ->
            {[], data_acc}
        end
      end)

    case send_subscriptions(data, to_subscribe) do
      :ok -> {:ok, data}
      {:error, reason} -> disconnect(data, reason, _handle_disconnection? = true)
    end
  end

  defp subscribe_pid_to_target(nil, pid) do
    state = {:subscribing, MapSet.new([pid]), MapSet.new()}
    {:new, state}
  end

  defp subscribe_pid_to_target({:subscribed, subscribers}, pid) do
    state = {:subscribed, MapSet.put(subscribers, pid)}
    {:already_subscribed, state}
  end

  defp subscribe_pid_to_target({:subscribing, subscribes, unsubscribes}, pid) do
    state = {:subscribing, MapSet.put(subscribes, pid), MapSet.delete(unsubscribes, pid)}
    {:pending, state}
  end

  defp subscribe_pid_to_target({:unsubscribing, resubscribers}, pid) do
    state = {:unsubscribing, MapSet.put(resubscribers, pid)}
    {:pending, state}
  end

  defp send_subscription_confirmation(data, pid, {:channel, channel}) do
    send(pid, Map.fetch!(data.monitors, pid), :subscribed, %{channel: channel})
  end

  defp send_subscription_confirmation(data, pid, {:pattern, pattern}) do
    send(pid, Map.fetch!(data.monitors, pid), :psubscribed, %{pattern: pattern})
  end

  defp send_subscriptions(_data, []) do
    :ok
  end

  defp send_subscriptions(data, to_subscribe) do
    channels = for {:channel, channel} <- to_subscribe, do: channel
    patterns = for {:pattern, pattern} <- to_subscribe, do: pattern

    pipeline =
      case {channels, patterns} do
        {_, []} -> [["SUBSCRIBE" | channels]]
        {[], _} -> [["PSUBSCRIBE" | patterns]]
        {_, _} -> [["SUBSCRIBE" | channels], ["PSUBSCRIBE" | patterns]]
      end

    data.transport.send(data.socket, Enum.map(pipeline, &Protocol.pack/1))
  end

  # Returns {targets_to_unsubscribe_from, data}.
  defp unsubscribe_pid_from_targets(data, operation, targets, pid) do
    target_type =
      case operation do
        :unsubscribe -> :channel
        :punsubscribe -> :pattern
      end

    {to_unsubscribe, data} =
      Enum.flat_map_reduce(targets, data, fn target_name, data_acc ->
        target_key = {target_type, target_name}

        {target_state, data_acc} =
          get_and_update_in(
            data_acc.subscriptions[target_key],
            &unsubscribe_pid_from_target(&1, pid)
          )

        send_unsubscription_confirmation(data_acc, pid, target_key)

        case target_state do
          :now_empty -> {[target_key], data_acc}
          _other -> {[], data_acc}
        end
      end)

    case send_unsubscriptions(data, to_unsubscribe) do
      :ok -> {:ok, data}
      {:error, reason} -> disconnect(data, reason, _handle_disconnection? = true)
    end
  end

  defp unsubscribe_pid_from_target({:subscribed, subscribers}, pid) do
    if MapSet.size(subscribers) == 1 and MapSet.member?(subscribers, pid) do
      state = {:unsubscribing, _resubscribers = MapSet.new()}
      {:now_empty, state}
    else
      state = {:subscribed, MapSet.delete(subscribers, pid)}
      {:noop, state}
    end
  end

  defp unsubscribe_pid_from_target({:subscribing, subscribes, unsubscribes}, pid) do
    state = {:subscribing, MapSet.delete(subscribes, pid), MapSet.put(unsubscribes, pid)}
    {:noop, state}
  end

  defp unsubscribe_pid_from_target({:unsubscribing, resubscribers}, pid) do
    state = {:unsubscribing, MapSet.delete(resubscribers, pid)}
    {:noop, state}
  end

  defp unsubscribe_pid_from_target(_, _), do: :pop

  defp send_unsubscription_confirmation(data, pid, {:channel, channel}) do
    if ref = data.monitors[pid] do
      send(pid, ref, :unsubscribed, %{channel: channel})
    end
  end

  defp send_unsubscription_confirmation(data, pid, {:pattern, pattern}) do
    if ref = data.monitors[pid] do
      send(pid, ref, :punsubscribed, %{pattern: pattern})
    end
  end

  defp send_unsubscriptions(_data, []) do
    :ok
  end

  defp send_unsubscriptions(data, to_subscribe) do
    channels = for {:channel, channel} <- to_subscribe, do: channel
    patterns = for {:pattern, pattern} <- to_subscribe, do: pattern

    pipeline =
      case {channels, patterns} do
        {_, []} -> [["UNSUBSCRIBE" | channels]]
        {[], _} -> [["PUNSUBSCRIBE" | patterns]]
        {_, _} -> [["UNSUBSCRIBE" | channels], ["PUNSUBSCRIBE" | patterns]]
      end

    data.transport.send(data.socket, Enum.map(pipeline, &Protocol.pack/1))
  end

  defp resubscribe_after_reconnection(data) do
    data =
      update_in(data.subscriptions, fn subscriptions ->
        Map.new(subscriptions, fn {target_key, {:disconnected, subscribers}} ->
          {target_key, {:subscribing, subscribers, MapSet.new()}}
        end)
      end)

    with :ok <- send_subscriptions(data, Map.keys(data.subscriptions)) do
      {:ok, data}
    end
  end

  defp monitor_new(data, pid) do
    case data.monitors do
      %{^pid => ref} ->
        {data, ref}

      _ ->
        ref = Process.monitor(pid)
        data = put_in(data.monitors[pid], ref)
        {data, ref}
    end
  end

  defp demonitor_if_not_subscribed_to_anything(data, pid) do
    still_subscribed_to_something? =
      Enum.any?(data.subscriptions, fn
        {_target, {:subscribing, subscribes, _unsubscribes}} -> pid in subscribes
        {_target, {:subscribed, subscribers}} -> pid in subscribers
        {_target, {:unsubscribing, resubscribers}} -> pid in resubscribers
        {_target, {:disconnected, subscribers}} -> pid in subscribers
      end)

    if still_subscribed_to_something? do
      data
    else
      {monitor_ref, data} = pop_in(data.monitors[pid])
      if monitor_ref, do: Process.demonitor(monitor_ref, [:flush])
      data
    end
  end

  defp key_for_target(:subscribe, channel), do: {:channel, channel}
  defp key_for_target(:unsubscribe, channel), do: {:channel, channel}
  defp key_for_target(:psubscribe, pattern), do: {:pattern, pattern}
  defp key_for_target(:punsubscribe, pattern), do: {:pattern, pattern}

  defp connect(%__MODULE__{opts: opts, transport: transport} = data) do
    timeout = Keyword.fetch!(opts, :timeout)
    fetch_client_id? = Keyword.fetch!(opts, :fetch_client_id_on_connect)

    with {:ok, socket, address} <- Connector.connect(opts, _conn_pid = self()),
         {:ok, client_id} <- maybe_fetch_client_id(fetch_client_id?, transport, socket, timeout),
         :ok <- setopts(data, socket, active: :once) do
      {:ok, socket, address, client_id}
    end
  end

  defp maybe_fetch_client_id(false, _transport, _socket, _timeout),
    do: {:ok, nil}

  defp maybe_fetch_client_id(true, transport, socket, timeout),
    do: Connector.sync_command(transport, socket, ["CLIENT", "ID"], timeout)

  defp setopts(data, socket, opts) do
    inets_mod(data.transport).setopts(socket, opts)
  end

  defp inets_mod(:gen_tcp), do: :inet
  defp inets_mod(:ssl), do: :ssl

  defp next_backoff(data) do
    backoff_current = data.backoff_current || data.opts[:backoff_initial]
    backoff_max = data.opts[:backoff_max]
    next_backoff = round(backoff_current * @backoff_exponent)

    backoff_current =
      if backoff_max == :infinity do
        next_backoff
      else
        min(next_backoff, backoff_max)
      end

    {backoff_current, put_in(data.backoff_current, backoff_current)}
  end

  def disconnect(data, reason, handle_disconnection?) do
    {next_backoff, data} = next_backoff(data)

    if data.socket do
      _ = data.transport.close(data.socket)
    end

    data = put_in(data.last_disconnect_reason, %ConnectionError{reason: reason})
    data = put_in(data.socket, nil)
    data = put_in(data.continuation, nil)

    actions = [{{:timeout, :reconnect}, next_backoff, nil}]

    actions =
      if handle_disconnection? do
        [{:next_event, :internal, :handle_disconnection}] ++ actions
      else
        actions
      end

    {:next_state, :disconnected, data, actions}
  end

  defp send(pid, ref, kind, properties)
       when is_pid(pid) and is_reference(ref) and is_atom(kind) and is_map(properties) do
    send(pid, {:redix_pubsub, self(), ref, kind, properties})
  end

  defp format_address(%{opts: opts} = _state) do
    if opts[:sentinel] do
      "sentinel"
    else
      Format.format_host_and_port(opts[:host], opts[:port])
    end
  end
end
