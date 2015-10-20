defmodule Redix.PubSub.Connection do
  @moduledoc false

  use Connection

  require Logger
  alias Redix.Protocol
  alias Redix.ConnectionUtils

  @initial_state %{
    tail: "",
    socket: nil,
    recipients: HashDict.new,
    monitors: HashDict.new,
    reconnection_attempts: 0,
    queue: :queue.new,
    clients_to_notify_of_reconnection: [],
  }

  ## Callbacks

  @doc false
  def init(opts) do
    {:connect, :init, Dict.merge(@initial_state, opts: opts)}
  end

  @doc false
  def connect(info, s)

  def connect(info, s) do
    case ConnectionUtils.connect(info, s) do
      {:ok, s} ->
        if info == :backoff do
          Enum.each(s.clients_to_notify_of_reconnection, &send(&1, msg(:reconnected, nil)))
          s = %{s | clients_to_notify_of_reconnection: []}
        end

        {:ok, s}
      o ->
        o
    end
  end

  @doc false
  def disconnect(reason, s)

  def disconnect(:stop, s) do
    {:stop, :normal, s}
  end

  def disconnect({:error, reason}, s) do
    Logger.error "Disconnected from Redis (#{ConnectionUtils.format_host(s)}): #{:inet.format_error(reason)}"
    :gen_tcp.close(s.socket)
    s = disconnect_and_notify_clients(s, reason)
    ConnectionUtils.backoff_or_stop(%{s | tail: "", socket: nil}, 0, reason)
  end

  @doc false
  def handle_call(operation, from, s)

  def handle_call({op, channels, recipient}, _from, s) when op in [:subscribe, :psubscribe] do
    subscribe(s, op, channels, recipient)
  end

  def handle_call({op, channels, recipient}, _from, s) when op in [:unsubscribe, :punsubscribe] do
    unsubscribe(s, op, channels, recipient)
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

  def handle_info({:DOWN, _ref, :process, pid, _reason}, s) do
    recipient_terminated(s, pid)
  end

  ## Helper functions

  defp new_data(s, <<>>) do
    %{s | tail: <<>>}
  end

  defp new_data(s, data) do
    case Protocol.parse(data) do
      {:ok, resp, rest} ->
        s |> handle_message(resp) |> new_data(rest)
      {:error, :incomplete} ->
        %{s | tail: data}
    end
  end

  defp subscribe(s, op, channels, recipient) do
    s = monitor_recipient(s, recipient)

    {channels_to_subscribe_to, recipients} =
      Enum.flat_map_reduce(channels, s.recipients, &subscribe_to_channel(&1, &2, recipient, op))

    s = %{s | recipients: recipients}

    command = op |> Atom.to_string |> String.upcase

    if channels_to_subscribe_to != [] do
      s
      |> enqueue(Enum.map(channels_to_subscribe_to, &{op, &1, recipient}))
      |> ConnectionUtils.send_reply(Protocol.pack([command|channels_to_subscribe_to]), :ok)
    else
      {:reply, :ok, s}
    end
  end

  defp unsubscribe(s, op, channels, recipient) do
    s = demonitor_recipient(s, recipient)

    {channels_to_unsubscribe_from, recipients} =
      Enum.flat_map_reduce(channels, s.recipients, &unsubscribe_from_channel(&1, &2, recipient, op))

    s = %{s | recipients: recipients}

    command = op |> Atom.to_string |> String.upcase

    if channels_to_unsubscribe_from != [] do
      s
      |> enqueue(Enum.map(channels_to_unsubscribe_from, &{op, &1, recipient}))
      |> ConnectionUtils.send_reply(Protocol.pack([command|channels_to_unsubscribe_from]), :ok)
    else
      {:reply, :ok, s}
    end
  end

  defp recipient_terminated(s, recipient) do
    {channels_to_unsubscribe_from, recipients} = Enum.flat_map_reduce s.recipients, s.recipients, fn {channel, for_channel}, recipients ->
      if HashSet.member?(for_channel, recipient) do
        for_channel = HashSet.delete(for_channel, recipient)
        if HashSet.size(for_channel) == 0 do
          {[channel], Dict.delete(recipients, channel)}
        else
          {[], Dict.put(recipients, channel, for_channel)}
        end
      else
        {[], recipients}
      end
    end

    s = %{s | recipients: recipients}

    {channels, patterns} = Enum.partition(channels_to_unsubscribe_from, &match?({:channel, _}, &1))
    channels = Enum.map(channels, fn({:channel, channel}) -> channel end)
    patterns = Enum.map(patterns, fn({:pattern, pattern}) -> pattern end)

    commands = []
    if channels != [] do
      commands = [["UNSUBSCRIBE"|channels]|commands]
      s = enqueue(s, Enum.map(channels, &{:unsubscribe, &1, nil}))
    end
    if patterns != [] do
      commands = [["PUNSUBSCRIBE"|patterns]|commands]
      s = enqueue(s, Enum.map(patterns, &{:punsubscribe, &1, nil}))
    end

    if commands != [] do
      ConnectionUtils.send_noreply(s, Enum.map(commands, &Protocol.pack/1))
    else
      {:noreply, s}
    end
  end

  defp subscribe_to_channel(channel, recipients, recipient, op) do
    key = {op_to_type(op), channel}

    if recipients[key] do
      recipients = Dict.update!(recipients, key, &HashSet.put(&1, recipient))
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
        {[], Dict.put(recipients, key, new_for_channel)}
      end
    else
      {[], recipients}
    end
  end

  defp handle_message(s, [op, channel, _count]) when op in ~w(subscribe psubscribe) do
    op = String.to_atom(op)

    {{:value, {^op, ^channel, recipient}}, new_queue} = :queue.out(s.queue)

    send(recipient, msg(op, channel))

    s = %{s | queue: new_queue}

    update_in s, [:recipients, {op_to_type(op), channel}], fn(set) ->
      (set || HashSet.new) |> HashSet.put(recipient)
    end
  end

  defp handle_message(s, [op, channel, _count]) when op in ~w(unsubscribe punsubscribe) do
    op = String.to_atom(op)

    {{:value, {^op, ^channel, recipient}}, new_queue} = :queue.out(s.queue)

    s = %{s | queue: new_queue}

    # If `recipient` is nil it means that this unsubscription came from a
    # monitored recipient going down: we don't need to send anything to anyone
    # and we don't need to remove it from the state (it had already been
    # removed).
    if is_nil(recipient) do
      s
    else
      send(recipient, msg(op, channel))
      update_in s, [:recipients, {op_to_type(op), channel}], &HashSet.delete(&1, recipient)
    end
  end

  defp handle_message(s, ["message", channel, payload]) do
    recipients = Dict.fetch!(s.recipients, {:channel, channel})
    Enum.each(recipients, fn(rec) -> send(rec, msg(:message, payload, channel)) end)
    s
  end

  defp handle_message(s, ["pmessage", pattern, channel, payload]) do
    recipients = Dict.fetch!(s.recipients, {:pattern, pattern})
    Enum.each(recipients, fn(rec) -> send(rec, msg(:pmessage, payload, {pattern, channel})) end)
    s
  end

  defp msg(type, payload, metadata \\ nil) do
    {:redix_pubsub, type, payload, metadata}
  end

  defp enqueue(%{queue: queue} = s, elems) when is_list(elems) do
    %{s | queue: :queue.join(queue, :queue.from_list(elems))}
  end

  defp op_to_type(op) when op in [:subscribe, :unsubscribe], do: :channel
  defp op_to_type(op) when op in [:psubscribe, :punsubscribe], do: :pattern

  defp monitor_recipient(s, recipient) do
    update_in s.monitors[recipient], fn
      nil ->
        Process.monitor(recipient)
      ref when is_reference(ref) ->
        ref
    end
  end

  defp demonitor_recipient(s, recipient) do
    unless Enum.any?(s.recipients, fn({_, recipients}) -> HashSet.member?(recipients, recipient) end) do
      s.monitors |> Dict.fetch!(recipient) |> Process.demonitor
    end

    s
  end

  defp disconnect_and_notify_clients(s, error_reason) do
    # First, demonitor all the monitored clients and reset the state.
    {clients, s} = get_and_update_in s.monitors, fn(monitors) ->
      monitors |> Dict.values |> Enum.each(&Process.demonitor/1)
      {Dict.keys(monitors), HashDict.new}
    end

    # Then, let's send a message to each of those clients.
    for client <- clients do
      subscribed_to = client_subscriptions(s, client)
      send(client, msg(:disconnected, error_reason, subscribed_to))
    end

    %{s | clients_to_notify_of_reconnection: clients}
  end

  defp client_subscriptions(s, client) do
    s.recipients
    |> Enum.filter(fn({_, recipients}) -> HashSet.member?(recipients, client) end)
    |> Enum.map(fn({channel, _recipients}) -> channel end)
  end
end
