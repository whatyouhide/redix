defmodule Redix.PubSub.Connection do
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
  }

  ## Callbacks

  @doc false
  def init(opts) do
    {:connect, :init, Dict.merge(@initial_state, opts: opts)}
  end

  @doc false
  def connect(info, s)

  def connect(info, s) do
    ConnectionUtils.connect(info, s)
  end

  @doc false
  def disconnect(reason, s)

  def disconnect(:stop, %{socket: nil} = s) do
    {:stop, :normal, s}
  end

  def disconnect(:stop, %{socket: socket} = s) do
    :gen_tcp.close(socket)
    {:stop, :normal, %{s | socket: nil}}
  end

  def disconnect({:error, reason}, s) do
    Logger.error "Disconnected from Redis (#{ConnectionUtils.host_for_logging(s)}): #{inspect reason}"

    # TODO inform clients of the disconnection.

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

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, _s) do
    # TODO handle recipients going down
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
    # TODO handle monitoring of `recipient`

    {channels_to_subscribe_to, recipients} =
      Enum.flat_map_reduce(channels, s.recipients, &subscribe_to_channel(&1, &2, recipient, op))

    s = %{s | recipients: recipients}

    command = op |> Atom.to_string |> String.upcase

    if channels_to_subscribe_to != [] do
      s
      |> enqueue(Enum.map(channels_to_subscribe_to, &{op, &1, recipient}))
      |> send_reply(Protocol.pack([command|channels_to_subscribe_to]), :ok)
    else
      {:reply, :ok, s}
    end
  end

  defp unsubscribe(s, op, channels, recipient) do
    # TODO handle demonitoring of `recipient`

    {channels_to_unsubscribe_from, recipients} =
      Enum.flat_map_reduce(channels, s.recipients, &unsubscribe_from_channel(&1, &2, recipient, op))

    s = %{s | recipients: recipients}

    command = op |> Atom.to_string |> String.upcase

    if channels_to_unsubscribe_from != [] do
      s
      |> enqueue(Enum.map(channels_to_unsubscribe_from, &{op, &1, recipient}))
      |> send_reply(Protocol.pack([command|channels_to_unsubscribe_from]), :ok)
    else
      {:reply, :ok, s}
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

    send(recipient, msg(op, channel))

    s = %{s | queue: new_queue}

    update_in s, [:recipients, {op_to_type(op), channel}], &HashSet.delete(&1, recipient)
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

  defp send_reply(%{socket: socket} = s, data, reply) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:reply, reply, s}
      {:error, _reason} = err ->
        {:disconnect, err, s}
    end
  end

  defp op_to_type(op) when op in [:subscribe, :unsubscribe], do: :channel
  defp op_to_type(op) when op in [:psubscribe, :punsubscribe], do: :pattern
end
