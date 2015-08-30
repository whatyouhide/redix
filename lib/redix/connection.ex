defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  alias Redix.Connection.Auth
  require Logger

  @type state :: %{}

  @initial_state %{
    socket: nil,
    tail: "",
    opts: nil,
    queue: :queue.new,
    reconnection_attempts: 0,
    pubsub: false,
    pubsub_clients: %{},
  }

  @default_timeout 5000

  @socket_opts [:binary, active: false]

  ## Callbacks

  @doc false
  def init(s) do
    {:connect, :init, Dict.merge(@initial_state, s)}
  end

  @doc false
  def connect(info, s)

  def connect(info, %{opts: opts} = s) do
    {host, port, socket_opts, timeout} = tcp_connection_opts(opts)

    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, socket} ->
        setup_socket_buffers(socket)
        Auth.auth_and_select_db(%{s | socket: socket, reconnection_attempts: 0})
      {:error, reason} ->
        Logger.error "Error connecting to Redis (#{host_for_logging(s)}): #{inspect reason}"
        handle_connection_error(s, info, reason)
    end
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

  def disconnect({:error, reason} = error, %{queue: queue} = s) do
    Logger.error "Disconnected from Redis (#{host_for_logging(s)}): #{inspect reason}"

    queue
    |> :queue.to_list
    |> Stream.filter(&match?({:commands, _, _}, &1))
    |> Stream.map(fn({:commands, from, _}) -> from end)
    |> Enum.map(&Connection.reply(&1, error))

    # Backoff with 0 to churn through all the commands in the mailbox before
    # reconnecting.
    s = %{s | socket: nil, queue: :queue.new, tail: ""}
    backoff_or_stop(s, 0, reason)
  end

  @doc false
  def handle_call(operation, from, s)

  def handle_call(_operation, _from, %{socket: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:commands, _commands}, _from, %{pubsub: true} = s) do
    {:reply, {:error, :pubsub_mode}, s}
  end

  def handle_call({:commands, commands}, from, s) do
    s
    |> enqueue({:commands, from, length(commands)})
    |> send_noreply(Enum.map(commands, &Protocol.pack/1))
  end

  def handle_call({op, _channels, _receiver}, _from, %{pubsub: false} = s)
      when op in [:unsubscribe, :punsubscribe] do
    {:reply, {:error, :not_pubsub_mode}, s}
  end

  def handle_call({:subscribe, channels, receiver}, _from, s) do
    subscriptions = Enum.map(channels, fn(ch) -> {:subscribe, ch, receiver} end)

    s
    |> Map.put(:pubsub, true)
    |> enqueue(subscriptions)
    |> send_reply(Protocol.pack(["SUBSCRIBE"|channels]), :ok)
  end

  def handle_call({:unsubscribe, channels, receiver}, _from, s) do
    unsubscriptions = Enum.map(channels, fn(ch) -> {:unsubscribe, ch, receiver} end)

    s
    |> enqueue(unsubscriptions)
    |> send_reply(Protocol.pack(["UNSUBSCRIBE"|channels]), :ok)
  end

  def handle_call({:psubscribe, channels, receiver}, _from, s) do
    subscriptions = Enum.map(channels, fn(ch) -> {:psubscribe, ch, receiver} end)

    s
    |> Map.put(:pubsub, true)
    |> enqueue(subscriptions)
    |> send_reply(Protocol.pack(["PSUBSCRIBE"|channels]), :ok)
  end

  def handle_call({:punsubscribe, channels, receiver}, _from, s) do
    unsubscriptions = Enum.map(channels, fn(ch) -> {:punsubscribe, ch, receiver} end)

    s
    |> enqueue(unsubscriptions)
    |> send_reply(Protocol.pack(["PUNSUBSCRIBE"|channels]), :ok)
  end

  def handle_call(:pubsub?, _from, s) do
    {:reply, s.pubsub, s}
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

  ## Helper functions

  defp new_data(s, <<>>) do
    %{s | tail: <<>>}
  end

  defp new_data(%{pubsub: false} = s, data) do
    {{:value, {:commands, from, ncommands}}, new_queue} = :queue.out(s.queue)

    case Protocol.parse_multi(data, ncommands) do
      {:ok, resp, rest} ->
        Connection.reply(from, format_resp(resp))
        s = %{s | queue: new_queue}
        new_data(s, rest)
      {:error, :incomplete} ->
        %{s | tail: data}
    end
  end

  defp new_data(%{pubsub: true} = s, data) do
    case Protocol.parse(data) do
      {:ok, resp, rest} ->
        s = new_pubsub_msg(s, resp)
        new_data(s, rest)
      {:error, :incomplete} ->
        %{s | tail: data}
    end
  end

  defp new_pubsub_msg(s, ["message", channel, message]) do
    message = {:redix_pubsub, :message, message, channel}
    deliver_message(s.pubsub_clients[channel], message)
    s
  end

  defp new_pubsub_msg(s, ["pmessage", pattern, channel, message]) do
    message = {:redix_pubsub, :pmessage, message, {pattern, channel}}
    deliver_message(s.pubsub_clients[pattern], message)
    s
  end

  defp new_pubsub_msg(s, ["subscribe", channel, count]) do
    new_pubsub_subscription(s, :subscribe, channel, count)
  end

  defp new_pubsub_msg(s, ["psubscribe", pattern, count]) do
    new_pubsub_subscription(s, :psubscribe, pattern, count)
  end

  defp new_pubsub_msg(s, ["unsubscribe", channel, count]) do
    new_pubsub_unsubscription(s, :unsubscribe, channel, count)
  end

  defp new_pubsub_msg(s, ["punsubscribe", channel, count]) do
    new_pubsub_unsubscription(s, :punsubscribe, channel, count)
  end

  defp new_pubsub_subscription(s, subscription_type, channel, count) do
    {{:value, {^subscription_type, ^channel, receiver}}, new_queue} = :queue.out(s.queue)
    s = update_in s, [:pubsub_clients, channel], fn(set) ->
      set = set || HashSet.new
      unless HashSet.member?(set, receiver) do
        send receiver, {:redix_pubsub, subscription_type, channel, count}
      end

      (set || HashSet.new) |> HashSet.put(receiver)
    end


    %{s | queue: new_queue}
  end

  defp new_pubsub_unsubscription(s, unsubscription_type, channel, count) do
    {{:value, {^unsubscription_type, ^channel, receiver}}, new_queue} = :queue.out(s.queue)
    s = update_in s, [:pubsub_clients, channel], &HashSet.delete(&1, receiver)

    send receiver, {:redix_pubsub, unsubscription_type, channel, count}

    %{s | queue: new_queue}
  end

  defp send_noreply(%{socket: socket} = s, data) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:noreply, s}
      {:error, _reason} = error ->
        {:disconnect, error, s}
    end
  end

  defp send_reply(%{socket: socket} = s, data, reply) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:reply, reply, s}
      {:error, _reason} = error ->
        {:disconnect, error, s}
    end
  end

  # Enqueues `val` in the state.
  defp enqueue(%{queue: queue} = s, vals) when is_list(vals) do
    %{s | queue: :queue.join(queue, :queue.from_list(vals))}
  end

  defp enqueue(%{queue: queue} = s, val) do
    %{s | queue: :queue.in(val, queue)}
  end

  # Extracts the TCP connection options (host, port and socket opts) from the
  # given `opts`.
  defp tcp_connection_opts(opts) do
    host = to_char_list(Keyword.fetch!(opts, :host))
    port = Keyword.fetch!(opts, :port)
    socket_opts = @socket_opts ++ Keyword.fetch!(opts, :socket_opts)
    timeout = opts[:timeout] || @default_timeout

    {host, port, socket_opts, timeout}
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(socket) do
    {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
      :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])

    buffer = buffer |> max(sndbuf) |> max(recbuf)
    :ok = :inet.setopts(socket, [buffer: buffer])
  end

  defp format_resp(%Redix.Error{} = err), do: {:error, err}
  defp format_resp(resp), do: {:ok, resp}

  defp host_for_logging(%{opts: opts} = _s) do
    "#{opts[:host]}:#{opts[:port]}"
  end

  # If `info` is :backoff then this is a *reconnection* attempt, so if there's
  # an error let's try to just reconnect after a backoff time (if we're under
  # the max number of retries). If `info` is :init, then this is the first
  # connection attempt so if it fails let's just die.
  defp handle_connection_error(s, :init, reason),
    do: {:stop, reason, s}
  defp handle_connection_error(s, :backoff, reason),
    do: backoff_or_stop(s, s.opts[:backoff], reason)

  # This function is called every time we want to try and reconnect. It returns
  # {:backoff, ...} if we're below the max number of allowed reconnection
  # attempts (or if there's no such limit), {:stop, ...} otherwise.
  defp backoff_or_stop(s, backoff, stop_reason) do
    s = update_in(s.reconnection_attempts, &(&1 + 1))

    if attempt_to_reconnect?(s) do
      {:backoff, backoff, s}
    else
      {:stop, stop_reason, s}
    end
  end

  defp attempt_to_reconnect?(%{opts: opts, reconnection_attempts: attempts}) do
    max_attempts = opts[:max_reconnection_attempts]
    is_nil(max_attempts) or (max_attempts > 0 and attempts <= max_attempts)
  end

  defp deliver_message(recipients, message) do
    Enum.each recipients, &send(&1, message)
  end
end
