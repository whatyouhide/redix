defmodule Redix.Connection.PubSub do
  @moduledoc false

  @type subscription_op :: :subscribe | :psubscribe
  @type unsubscription_op :: :unsubscribe | :punsubscribe
  @type channels :: [binary]

  # Subscribes `recipient` to the given list of `channels` (or
  # patterns). Returns a tuple with the state as the first element and the list
  # of channels to actually subscribe to (with (P)SUBSCRIBE) as the second
  # element.
  @spec subscribe(Redix.Connection.state, subscription_op, channels, Redix.pubsub_recipient)
    :: {Redix.Connection.state, channels}
  def subscribe(s, subscription_type, channels, recipient) do
    {s, channels_to_subscribe_to} =
      Enum.reduce(channels, {s, []}, &subscribe_to_channel(&1, &2, subscription_type, recipient))

    s = %{s | pubsub: true}
    {s, Enum.reverse(channels_to_subscribe_to)}
  end

  # Unsubscribes `recipient` from the given list of `channels` (or patterns).
  @spec unsubscribe(Redix.Connection.state, unsubscription_op, channels, Redix.pubsub_recipient)
    :: Redix.Connection.state
  def unsubscribe(s, unsubscription_type, channels, recipient) do
    {s, channels_to_unsubscribe_from} =
      Enum.reduce(channels, {s, []}, &unsubscribe_from_channel(&1, &2, unsubscription_type, recipient))

    channels_to_unsubscribe_from = Enum.reverse(channels_to_unsubscribe_from)

    channels_to_unsubscribe_from =
      channels_to_unsubscribe_from
      |> Enum.into(s.pubsub_waiting_for_subscription_ack)
      |> HashSet.to_list

    to_enqueue =
      channels_to_unsubscribe_from
      |> Enum.map(&{unsubscription_type, &1, recipient})
      |> :queue.from_list

    s = update_in(s.queue, &:queue.join(&1, to_enqueue))

    {s, channels_to_unsubscribe_from}
  end

  # Handles a pubsub message sent by Redis.
  @spec handle_message(Redix.Connection.state, [binary]) :: Redix.Connection.state
  def handle_message(s, message)

  def handle_message(s, ["message", channel, payload]) do
    s.pubsub_clients
    |> Dict.fetch!(channel)
    |> deliver_payload(msg(:message, payload, channel))

    s
  end

  def handle_message(s, ["pmessage", pattern, channel, payload]) do
    s.pubsub_clients
    |> Dict.fetch!(pattern)
    |> deliver_payload(msg(:pmessage, payload, {pattern, channel}))

    s
  end

  def handle_message(s, [op, channel, _count]) when op in ~w(subscribe psubscribe) do
    op = String.to_atom(op)
    {{:value, {^op, ^channel, receiver}}, new_queue} = :queue.out(s.queue)

    # TODO find appropriate metadata to shove in here
    send(receiver, msg(op, channel, nil))

    s
    |> update_in([:pubsub_waiting_for_subscription_ack], &HashSet.delete(&1, channel))
    |> put_in([:pubsub_clients, channel], init_receivers(receiver))
    |> Map.put(:queue, new_queue)
  end

  def handle_message(s, [op, channel, _count]) when op in ~w(unsubscribe punsubscribe) do
    op = String.to_atom(op)
    {{:value, {^op, ^channel, receiver}}, new_queue} = :queue.out(s.queue)

    # TODO find appropriate metadata to shove in here
    send(receiver, msg(op, channel, nil))

    s
    |> update_in([:pubsub_clients], &Dict.delete(&1, channel))
    |> Map.put(:queue, new_queue)
  end

  @spec op_to_command(subscription_op | unsubscription_op) :: binary
  def op_to_command(op) when op in ~w(subscribe psubscribe unsubscribe punsubscribe)a do
    op |> Atom.to_string |> String.upcase
  end

  # Delivers `payload` to the list of `recipients`.
  defp deliver_payload(recipients, payload) do
    Enum.each(recipients, &send(&1, payload))
  end

  defp subscribe_to_channel(channel, {s, channels_to_subscribe_to}, subscription_type, receiver) do
    if receivers = s.pubsub_clients[channel] do
      # TODO find appropriate metadata to shove in here
      send(receiver, msg(subscription_type, channel, nil))
      s = put_in(s, [:pubsub_clients, channel], HashSet.put(receivers, receiver))
      {s, channels_to_subscribe_to}
    else
      # We didn't subscribe to this channel yet: we'll enqueue the subscription
      # and add it to the list of channels to subscribe to.
      s = update_in(s.pubsub_waiting_for_subscription_ack, &HashSet.put(&1, channel))
      s = update_in(s.queue, &:queue.in({subscription_type, channel, receiver}, &1))
      {s, [channel|channels_to_subscribe_to]}
    end
  end

  defp unsubscribe_from_channel(channel, {s, channels_to_unsubscribe_from}, unsubscription_type, receiver) do
    if receivers = s.pubsub_clients[channel] do
      if HashSet.member?(receivers, receiver) do
        # TODO find appropriate metadata to shove in here
        send(receiver, msg(unsubscription_type, channel, nil))
        receivers = HashSet.delete(receivers, receiver)
        s = put_in(s, [:pubsub_clients, channel], receivers)
      end

      if HashSet.size(receivers) == 0 do
        # No receivers remaining, we actually have to unsubscribe.
        {s, [channel|channels_to_unsubscribe_from]}
      else
        {s, channels_to_unsubscribe_from}
      end
    else
      # This channel has no subscribers, we don't need to do anything.
      {s, channels_to_unsubscribe_from}
    end
  end

  # Formats a Redix PubSub message with the given arguments.
  defp msg(type, payload, metadata) do
    {:redix_pubsub, type, payload, metadata}
  end

  defp init_receivers(receiver) do
    HashSet.new |> HashSet.put(receiver)
  end
end
