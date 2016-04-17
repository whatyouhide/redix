defmodule Redix.PubSub.Connection do
  @moduledoc false

  use Connection

  require Logger
  alias Redix.Protocol
  alias Redix.Utils

  @initial_state %{
    tail: "",
    socket: nil,
    recipients: HashDict.new,
    monitors: HashDict.new,
    queue: :queue.new,
    clients_to_notify_of_reconnection: [],
  }

  ## Callbacks

  @doc false
  def init(opts) do
    {:connect, :init, Map.put(@initial_state, :opts, opts)}
  end

  @doc false
  def connect(info, state)

  def connect(info, state) do
    case Utils.connect(info, state) do
      {:ok, state} ->
        state =
          if info == :backoff do
            Enum.each(state.clients_to_notify_of_reconnection, &send(&1, msg(:reconnected, nil)))
            %{state | clients_to_notify_of_reconnection: []}
          else
            state
          end

        {:ok, state}
      o ->
        o
    end
  end

  @doc false
  def disconnect(reason, state)

  def disconnect(:stop, state) do
    {:stop, :normal, state}
  end

  def disconnect({:error, reason}, state) do
    Logger.error ["Disconnected from Redis (#{Utils.format_host(state)}): ",
                  Utils.format_error(reason)]
    :gen_tcp.close(state.socket)
    state = disconnect_and_notify_clients(state, reason)
    {:backoff, 0, %{state | tail: "", socket: nil}}
  end

  @doc false
  def handle_call(operation, from, state)

  def handle_call({op, channels, recipient}, _from, state) when op in [:subscribe, :psubscribe] do
    subscribe(state, op, channels, recipient)
  end

  def handle_call({op, channels, recipient}, _from, state) when op in [:unsubscribe, :punsubscribe] do
    unsubscribe(state, op, channels, recipient)
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

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    recipient_terminated(state, pid)
  end

  ## Helper functions

  defp new_data(state, <<>>) do
    %{state | tail: <<>>}
  end

  defp new_data(state, data) do
    case Protocol.parse(data) do
      {:ok, resp, rest} ->
        state |> handle_message(resp) |> new_data(rest)
      {:error, :incomplete} ->
        %{state | tail: data}
    end
  end

  defp subscribe(state, op, channels, recipient) do
    state = monitor_recipient(state, recipient)

    {channels_to_subscribe_to, recipients} =
      Enum.flat_map_reduce(channels, state.recipients, &subscribe_to_channel(&1, &2, recipient, op))

    state = %{state | recipients: recipients}

    command = op |> Atom.to_string |> String.upcase

    if channels_to_subscribe_to != [] do
      state
      |> enqueue(Enum.map(channels_to_subscribe_to, &{op, &1, recipient}))
      |> Utils.send_reply(Protocol.pack([command|channels_to_subscribe_to]), :ok)
    else
      {:reply, :ok, state}
    end
  end

  defp unsubscribe(state, op, channels, recipient) do
    state = demonitor_recipient(state, recipient)

    {channels_to_unsubscribe_from, recipients} =
      Enum.flat_map_reduce(channels, state.recipients, &unsubscribe_from_channel(&1, &2, recipient, op))

    state = %{state | recipients: recipients}

    command = op |> Atom.to_string |> String.upcase

    if channels_to_unsubscribe_from != [] do
      state
      |> enqueue(Enum.map(channels_to_unsubscribe_from, &{op, &1, recipient}))
      |> Utils.send_reply(Protocol.pack([command|channels_to_unsubscribe_from]), :ok)
    else
      {:reply, :ok, state}
    end
  end

  defp recipient_terminated(state, recipient) do
    {channels_to_unsubscribe_from, recipients} = Enum.flat_map_reduce state.recipients, state.recipients, fn {channel, for_channel}, recipients ->
      if HashSet.member?(for_channel, recipient) do
        for_channel = HashSet.delete(for_channel, recipient)
        if HashSet.size(for_channel) == 0 do
          {[channel], HashDict.delete(recipients, channel)}
        else
          {[], HashDict.put(recipients, channel, for_channel)}
        end
      else
        {[], recipients}
      end
    end

    state = %{state | recipients: recipients}

    {channels, patterns} = Enum.partition(channels_to_unsubscribe_from, &match?({:channel, _}, &1))
    channels = Enum.map(channels, fn({:channel, channel}) -> channel end)
    patterns = Enum.map(patterns, fn({:pattern, pattern}) -> pattern end)

    commands = []

    {commands, state} =
      if channels != [] do
        cmds = [["UNSUBSCRIBE"|channels]|commands]
        s = enqueue(state, Enum.map(channels, &{:unsubscribe, &1, nil}))
        {cmds, s}
      else
        {commands, state}
      end

    {commands, state} =
      if patterns != [] do
        cmds = [["PUNSUBSCRIBE"|patterns]|commands]
        s = enqueue(state, Enum.map(patterns, &{:punsubscribe, &1, nil}))
        {cmds, s}
      else
        {commands, state}
      end

    if commands != [] do
      Utils.send_noreply(state, Enum.map(commands, &Protocol.pack/1))
    else
      {:noreply, state}
    end
  end

  defp subscribe_to_channel(channel, recipients, recipient, op) do
    key = {op_to_type(op), channel}

    if recipients[key] do
      recipients = HashDict.update!(recipients, key, &HashSet.put(&1, recipient))
      send(recipient, msg(op, channel))
      {[], recipients}
    else
      {[channel], recipients}
    end
  end

  defp unsubscribe_from_channel(channel, recipients, recipient, op) do
    key = {op_to_type(op), channel}

    if for_channel = recipients[key] do
      new_for_channel = HashSet.delete(for_channel, recipient)

      if HashSet.size(new_for_channel) == 0 do
        {[channel], recipients}
      else
        send(recipient, msg(op, channel))
        {[], HashDict.put(recipients, key, new_for_channel)}
      end
    else
      {[], recipients}
    end
  end

  defp handle_message(state, [op, channel, _count]) when op in ~w(subscribe psubscribe) do
    op = String.to_atom(op)

    {{:value, {^op, ^channel, recipient}}, new_queue} = :queue.out(state.queue)

    send(recipient, msg(op, channel))

    state = %{state | queue: new_queue}

    update_in state, [:recipients, {op_to_type(op), channel}], fn(set) ->
      (set || HashSet.new) |> HashSet.put(recipient)
    end
  end

  defp handle_message(state, [op, channel, _count]) when op in ~w(unsubscribe punsubscribe) do
    op = String.to_atom(op)

    {{:value, {^op, ^channel, recipient}}, new_queue} = :queue.out(state.queue)

    state = %{state | queue: new_queue}

    # If `recipient` is nil it means that this unsubscription came from a
    # monitored recipient going down: we don't need to send anything to anyone
    # and we don't need to remove it from the state (it had already been
    # removed).
    if is_nil(recipient) do
      state
    else
      send(recipient, msg(op, channel))
      update_in state, [:recipients, {op_to_type(op), channel}], &HashSet.delete(&1, recipient)
    end
  end

  defp handle_message(state, ["message", channel, payload]) do
    recipients = HashDict.fetch!(state.recipients, {:channel, channel})
    Enum.each(recipients, fn(rec) -> send(rec, msg(:message, payload, channel)) end)
    state
  end

  defp handle_message(state, ["pmessage", pattern, channel, payload]) do
    recipients = HashDict.fetch!(state.recipients, {:pattern, pattern})
    Enum.each(recipients, fn(rec) -> send(rec, msg(:pmessage, payload, {pattern, channel})) end)
    state
  end

  defp msg(type, payload, metadata \\ nil) do
    {:redix_pubsub, type, payload, metadata}
  end

  defp enqueue(%{queue: queue} = state, elems) when is_list(elems) do
    %{state | queue: :queue.join(queue, :queue.from_list(elems))}
  end

  defp op_to_type(op) when op in [:subscribe, :unsubscribe], do: :channel
  defp op_to_type(op) when op in [:psubscribe, :punsubscribe], do: :pattern

  defp monitor_recipient(state, recipient) do
    update_in state.monitors[recipient], fn
      nil ->
        Process.monitor(recipient)
      ref when is_reference(ref) ->
        ref
    end
  end

  defp demonitor_recipient(state, recipient) do
    unless Enum.any?(state.recipients, fn({_, recipients}) -> HashSet.member?(recipients, recipient) end) do
      state.monitors |> HashDict.fetch!(recipient) |> Process.demonitor
    end

    state
  end

  defp disconnect_and_notify_clients(state, error_reason) do
    # First, demonitor all the monitored clients and reset the state.
    {clients, state} = get_and_update_in state.monitors, fn(monitors) ->
      monitors |> HashDict.values |> Enum.each(&Process.demonitor/1)
      {HashDict.keys(monitors), HashDict.new}
    end

    # Then, let's send a message to each of those clients.
    for client <- clients do
      subscribed_to = client_subscriptions(state, client)
      send(client, msg(:disconnected, error_reason, subscribed_to))
    end

    %{state | clients_to_notify_of_reconnection: clients}
  end

  defp client_subscriptions(state, client) do
    state.recipients
    |> Enum.filter(fn({_, recipients}) -> HashSet.member?(recipients, client) end)
    |> Enum.map(fn({channel, _recipients}) -> channel end)
  end
end
