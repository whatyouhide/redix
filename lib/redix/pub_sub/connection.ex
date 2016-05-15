defmodule Redix.PubSub.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.Utils

  require Logger

  defstruct [
    # The options passed when initializing this GenServer
    opts: nil,

    # The TCP socket connected to the Redis server
    socket: nil,

    # The parsing continuation returned by Redix.Protocol if a response is incomplete
    continuation: nil,

    # The current backoff interval
    backoff_current: nil,

    # A dictionary of `channel => recipient_pids` where `channel` is either
    # `{:channel, "foo"}` or `{:pattern, "foo*"}` and `recipient_pids` is an
    # HashSet of pids of recipients for that channel/pattern.
    subscribed_channels: HashDict.new,

    # A dictionary of `recipient_pid => monitor_ref` where `monitor_ref` is the
    # ref of the monitor that this GenServer is keeping on `recipient_pid`.
    monitors: HashDict.new,

    queue: :queue.new,
    clients_to_notify_of_reconnection: [],
  ]

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
        state =
          if info == :backoff do
            Enum.each(state.clients_to_notify_of_reconnection, &send(&1, msg(:reconnected, nil)))
            %{state | clients_to_notify_of_reconnection: []}
          else
            state
          end

        {:ok, state}
      {:error, reason} ->
        Logger.error [
          "Failed to connect to Redis (", Utils.format_host(state), "): ",
          Utils.format_error(reason),
        ]

        next_backoff = calc_next_backoff(state.backoff_current || state.opts[:backoff_initial], state.opts[:backoff_max])
        {:backoff, next_backoff, %{state | backoff_current: next_backoff}}
      other ->
        other
    end
  end

  @doc false
  def disconnect(reason, state)

  def disconnect(:stop, state) do
    # TODO: should we do something here as well?
    {:stop, :normal, state}
  end

  def disconnect({:error, reason}, state) do
    Logger.error [
      "Disconnected from Redis (", Utils.format_host(state), "): ", Utils.format_error(reason),
    ]

    :ok = :gen_tcp.close(state.socket)

    # First, demonitor all the monitored clients and reset the state.
    state.monitors |> HashDict.values |> Enum.each(&Process.demonitor/1)
    clients = HashDict.keys(state.monitors)

    state = %{state | monitors: HashDict.new}

    # Then, let's send a message to each of those clients.
    for client <- clients do
      subscribed_to = client_subscriptions(state, client)
      send(client, msg(:disconnected, reason, subscribed_to))
    end

    %{state | clients_to_notify_of_reconnection: clients}

    state = %{state | socket: nil, continuation: nil, backoff_current: state.opts[:backoff_initial]}
    {:backoff, state.opts[:backoff_initial], state}
  end

  @doc false
  def handle_cast(operation, state)

  def handle_cast({op, channels, recipient}, state)
      when op in [:subscribe, :psubscribe] do
    subscribe(state, op, channels, recipient)
  end

  def handle_cast({op, channels, recipient}, state)
      when op in [:unsubscribe, :punsubscribe] do
    unsubscribe(state, op, channels, recipient)
  end

  def handle_cast(:stop, state) do
    {:disconnect, :stop, state}
  end

  @doc false
  def handle_info(msg, state)

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    :ok = :inet.setopts(socket, active: :once)
    state = new_data(state, data)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :tcp_closed}, state}
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    recipient_terminated(state, pid, ref)
  end

  ## Helper functions

  defp sync_connect(state) do
    case Utils.connect(state) do
      {:ok, _state} = result ->
        result
      {:error, reason} ->
        {:stop, reason}
      {:stop, reason, _state} ->
        {:stop, reason}
    end
  end

  defp new_data(state, <<>>) do
    state
  end

  defp new_data(state, data) do
    case (state.continuation || &Protocol.parse/1).(data) do
      {:ok, resp, rest} ->
        state = handle_pubsub_msg(state, resp)
        new_data(%{state | continuation: nil}, rest)
      {:continuation, continuation} ->
        %{state | continuation: continuation}
    end
  end

  defp subscribe(%{subscribed_channels: subscribed_channels} = state, op, channels, recipient) do
    state = maybe_monitor_recipient(state, recipient)

    # First, we divide `channels` into the list of channels this GenServer is
    # already subscribed to and the ones it isn't.
    {already_subscribed_channels, new_channels} = Enum.partition(channels, fn(channel) ->
      HashDict.has_key?(subscribed_channels, {op_to_type(op), channel})
    end)

    # For channels which this GenServer is already subscribed to, we just notify
    # the recipient that we connected (since we don't have to dependon any
    # network operation) and add that recipient under each channel in the
    # `subscribed_channels` in the state.
    new_subscribed_channels = Enum.reduce(already_subscribed_channels, subscribed_channels, fn(channel, subscribed_channels) ->
      send(recipient, msg(op, channel))
      HashDict.update!(subscribed_channels, {op_to_type(op), channel}, &HashSet.put(&1, recipient))
    end)
    state = %{state | subscribed_channels: new_subscribed_channels}

    # Now, we take care of channels which this recipient is the first to
    # subscribe to (meaning that this GenServer is not subscribed to those). If
    # there are no such channels, we do nothing.
    if new_channels == [] do
      {:noreply, state}
    else
      command = [(op |> Atom.to_string() |> String.upcase()) | new_channels]
      state
      |> enqueue(Enum.map(new_channels, &{op, &1, recipient}))
      |> send_noreply(Protocol.pack(command))
    end
  end

  defp maybe_monitor_recipient(%{monitors: monitors} = state, recipient) do
    if HashDict.has_key?(monitors, recipient) do
      state
    else
      monitor = Process.monitor(recipient)
      %{state | monitors: HashDict.put(monitors, recipient, monitor)}
    end
  end

  defp unsubscribe(%{subscribed_channels: subscribed_channels} = state, op, channels, recipient) do
    # TODO: move this down, after we unsubscribed
    state = maybe_demonitor_recipient(state, recipient)

    {channels_with_no_more_recipients, new_subscribed_channels} =
      Enum.flat_map_reduce(channels, subscribed_channels, fn(channel, subscribed_channels) ->
        key = {op_to_type(op), channel}
        if recipients = subscribed_channels[key] do
          # We remove the recipient and send the unsubscription confirmation no
          # matter what, as we'll remove this recipient from the list of
          # recipients so we're not going to send messages for `channel` to it
          # anymore.
          new_recipients = HashSet.delete(recipients, recipient)
          new_subscribed_channels = HashDict.put(subscribed_channels, key, new_recipients)
          send(recipient, msg(op, channel))

          # If this was the last recipient for `channel`, then let's return
          # `channel` in the list of `channels_with_no_more_recipients`.
          if HashSet.size(new_recipients) == 0 do
            {[channel], new_subscribed_channels}
          else
            {[], new_subscribed_channels}
          end
        else
          # If we didn't have any subscribers to `channel`, than this is a no-op.
          # TODO: but we should have at least `recipient` here, so should we
          # error out if we don't? (that would be the case when a recipient
          # unsubscribes multiple times).
          {[], subscribed_channels}
        end
      end)
    state = %{state | subscribed_channels: new_subscribed_channels}

    if channels_with_no_more_recipients == [] do
      {:noreply, state}
    else
      command = [(op |> Atom.to_string() |> String.upcase()) | channels_with_no_more_recipients]
      state
      |> enqueue(Enum.map(channels_with_no_more_recipients, &{op, &1, recipient}))
      |> send_noreply(Protocol.pack(command))
    end
  end

  defp maybe_demonitor_recipient(%{subscribed_channels: subscribed_channels} = state, recipient) do
    # TODO: we can probably make this much more efficient if we keep something
    # like a counter in `state.monitors` that corresponds to the number of
    # channels each recipient is connected to, and demonitor when it's 0.

    # We only demonitor `recipient` if is not subscribed to any channels.
    subscribed_to_something? = Enum.any?(subscribed_channels, fn({_, recipients}) ->
      HashSet.member?(recipients, recipient)
    end)

    unless subscribed_to_something? do
      state.monitors |> HashDict.fetch!(recipient) |> Process.demonitor()
    end

    state
  end

  defp recipient_terminated(%{subscribed_channels: subscribed_channels} = state, recipient, monitor_ref) do
    {channels_with_no_more_recipients, new_subscribed_channels} =
      Enum.flat_map_reduce(HashDict.keys(subscribed_channels), subscribed_channels, fn({op, channel}, subscribed_channels) ->
        key = {op, channel}
        if recipients = subscribed_channels[key] do
          # We remove the recipient and send the unsubscription confirmation no
          # matter what, as we'll remove this recipient from the list of
          # recipients so we're not going to send messages for `channel` to it
          # anymore.
          new_recipients = HashSet.delete(recipients, recipient)
          new_subscribed_channels = HashDict.put(subscribed_channels, key, new_recipients)
          send(recipient, msg(op, channel))

          # If this was the last recipient for `channel`, then let's return
          # `channel` in the list of `channels_with_no_more_recipients`.
          if HashSet.size(new_recipients) == 0 do
            {[{op, channel}], new_subscribed_channels}
          else
            {[], new_subscribed_channels}
          end
        else
          # If we didn't have any subscribers to `channel`, than this is a no-op.
          # TODO: but we should have at least `recipient` here, so should we
          # error out if we don't? (that would be the case when a recipient
          # unsubscribes multiple times).
          {[], subscribed_channels}
        end
      end)
    state = %{state | subscribed_channels: new_subscribed_channels}

    {channels, patterns} = Enum.partition(channels_with_no_more_recipients, &match?({:channel, _}, &1))
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

    if commands == [] do
      {:noreply, state}
    else
      send_noreply(state, Enum.map(commands, &Protocol.pack/1))
    end
  end

  defp handle_pubsub_msg(state, [op, channel, _count]) when op in ~w(subscribe psubscribe) do
    op = String.to_atom(op)

    {{:value, {^op, ^channel, recipient}}, new_queue} = :queue.out(state.queue)

    send(recipient, msg(op, channel))

    state = %{state | queue: new_queue}

    new_subscribed_channels =
      HashDict.update(state.subscribed_channels, {op_to_type(op), channel}, HashSet.put(HashSet.new, recipient), &HashSet.put(&1, recipient))
    %{state | subscribed_channels: new_subscribed_channels}
  end

  defp handle_pubsub_msg(state, [op, channel, _count]) when op in ~w(unsubscribe punsubscribe) do
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
      new_subscribed_channels = HashDict.update!(state.subscribed_channels, {op_to_type(op), channel}, &HashSet.delete(&1, recipient))
      %{state | subscribed_channels: new_subscribed_channels}
    end
  end

  # A message arrived from a channel.
  defp handle_pubsub_msg(state, ["message", channel, payload]) do
    recipients = HashDict.fetch!(state.subscribed_channels, {:channel, channel})
    Enum.each(recipients, &send(&1, msg(:message, payload, channel)))

    state
  end

  # A message arrived from a pattern.
  defp handle_pubsub_msg(state, ["pmessage", pattern, channel, payload]) do
    recipients = HashDict.fetch!(state.subscribed_channels, {:pattern, pattern})
    Enum.each(recipients, &send(&1, msg(:pmessage, payload, {pattern, channel})))

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

  defp client_subscriptions(state, client) do
    state.subscribed_channels
    |> Enum.filter(fn({_, recipients}) -> HashSet.member?(recipients, client) end)
    |> Enum.map(fn({channel, _recipients}) -> channel end)
  end

  defp send_noreply(%{socket: socket} = state, data) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:noreply, state}
      {:error, _reason} = err ->
        {:disconnect, err, state}
    end
  end

  defp calc_next_backoff(backoff_current, backoff_max) do
    next_exponential_backoff = round(backoff_current * @backoff_exponent)

    if backoff_max == :infinity do
      next_exponential_backoff
    else
      min(next_exponential_backoff, backoff_max)
    end
  end
end
