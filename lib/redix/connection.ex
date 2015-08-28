defmodule Redix.Connection do
  @moduledoc false

  use Connection

  alias Redix.Protocol
  require Logger

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
        auth_and_select_db(%{s | socket: socket, reconnection_attempts: 0})
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
    |> Stream.map(&extract_client_from_queued_item/1)
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

  def handle_call({:command, _args}, _from, %{pubsub: true} = s) do
    {:reply, {:error, :pubsub_mode}, s}
  end

  def handle_call({:command, args}, from, s) do
    s
    |> enqueue({:command, from})
    |> send_noreply(Protocol.pack(args))
  end

  def handle_call({:pipeline, _args}, _from, %{pubsub: true} = s) do
    {:reply, {:error, :pubsub_mode}, s}
  end

  def handle_call({:pipeline, commands}, from, s) do
    s
    |> enqueue({:pipeline, from, length(commands)})
    |> send_noreply(Enum.map(commands, &Protocol.pack/1))
  end

  @doc false
  def handle_cast(operation, s)

  def handle_cast(:stop, s) do
    {:disconnect, :stop, s}
  end

  def handle_cast({:pubsub_subscribe, channels, receiver}, s) do
    s
    |> Map.put(:pubsub, true)
    |> enqueue({:subscribe, channels, receiver})
    |> send_noreply(Protocol.pack(["SUBSCRIBE"|channels]))
  end

  def handle_cast({:pubsub_unsubscribe, channels, receiver}, s) do
    s
    |> enqueue({:unsubscribe, channels, receiver})
    |> send_noreply(Protocol.pack(["UNSUBSCRIBE"|channels]))
  end

  def handle_cast({:pubsub_psubscribe, channels, receiver}, %{pubsub: _} = s) do
    s
    |> Map.put(:pubsub, true)
    |> enqueue({:subscribe, channels, receiver})
    |> send_noreply(Protocol.pack(["PSUBSCRIBE"|channels]))
  end

  def handle_cast({:pubsub_punsubscribe, channels, receiver}, s) do
    s
    |> enqueue({:punsubscribe, channels, receiver})
    |> send_noreply(Protocol.pack(["PUNSUBSCRIBE"|channels]))
  end

  @doc false
  def handle_info(msg, s)

  def handle_info({:tcp, socket, data}, %{socket: socket} = s) do
    reactivate_socket(s)
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
    {from, parser, new_queue} = dequeue(s)

    case parser.(data) do
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

  defp dequeue(s) do
    case :queue.out(s.queue) do
      {{:value, {:command, from}}, new_queue} ->
        {from, &Protocol.parse/1, new_queue}
      {{:value, {:pipeline, from, ncommands}}, new_queue} ->
        {from, &Protocol.parse_multi(&1, ncommands), new_queue}
      {:empty, _} ->
        raise "still got data but the queue is empty"
    end
  end

  defp new_pubsub_msg(s, ["message", channel, message]) do
    message = {:redix_pubsub, :message, channel, message}
    deliver_message(s.pubsub_clients[channel], message)
    s
  end

  defp new_pubsub_msg(s, ["pmessage", pattern, channel, message]) do
    message = {:redix_pubsub, :pmessage, {pattern, channel}, message}
    deliver_message(s.pubsub_clients[pattern], message)
    s
  end

  defp new_pubsub_msg(s, ["subscribe", channel, _count]) do
    case :queue.out(s.queue) do
      {{:value, {:subscribe, [^channel|other_channels], receiver}}, new_queue} ->
        s = update_in s.pubsub_clients, fn(clients) ->
          Map.update(clients, channel, [receiver], &[receiver|&1])
        end

        if other_channels != [] do
          new_queue = :queue.in_r({:subscribe, other_channels, receiver}, new_queue)
        end

        message = {:redix_pubsub, :subscribe, channel}
        send(receiver, message)

        %{s | queue: new_queue}
      _ ->
        raise "oops"
    end
  end

  defp new_pubsub_msg(s, ["psubscribe", channel, _count]) do
    case :queue.out(s.queue) do
      {{:value, {:subscribe, [^channel|other_channels], receiver}}, new_queue} ->
        s = update_in s.pubsub_clients, fn(clients) ->
          Map.update(clients, channel, [receiver], &[receiver|&1])
        end

        if other_channels != [] do
          new_queue = :queue.in_r({:subscribe, other_channels, receiver}, new_queue)
        end

        send receiver, {:redix_pubsub, :psubscribe, channel}

        %{s | queue: new_queue}
      _ ->
        raise "oops"
    end
  end

  defp new_pubsub_msg(s, ["unsubscribe", channel, _count]) do
    case :queue.out(s.queue) do
      {{:value, {:unsubscribe, [^channel|other_channels], receiver}}, new_queue} ->
        s = update_in s.pubsub_clients, fn(clients) ->
          Map.update!(clients, channel, &List.delete(&1, receiver))
        end

        if other_channels != [] do
          new_queue = :queue.in_r({:unsubscribe, other_channels, receiver}, new_queue)
        end

        send receiver, {:redix_pubsub, :unsubscribe, channel}

        %{s | queue: new_queue}
      _ ->
        raise "oops"
    end
  end

  defp new_pubsub_msg(s, ["punsubscribe", channel, _count]) do
    case :queue.out(s.queue) do
      {{:value, {:punsubscribe, [^channel|other_channels], receiver}}, new_queue} ->
        s = update_in s.pubsub_clients, fn(clients) ->
          Map.update!(clients, channel, &List.delete(&1, receiver))
        end

        if other_channels != [] do
          new_queue = :queue.in_r({:punsubscribe, other_channels, receiver}, new_queue)
        end

        send receiver, {:redix_pubsub, :punsubscribe, channel}

        %{s | queue: new_queue}
      _ ->
        raise "oops"
    end
  end

  defp send_noreply(%{socket: socket} = s, data) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:noreply, s}
      {:error, _reason} = error ->
        {:disconnect, error, s}
    end
  end

  # Enqueues `val` in the state.
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

  # Reactivates the socket with `active: :once`.
  defp reactivate_socket(%{socket: socket} = _s) do
    :ok = :inet.setopts(socket, active: :once)
  end

  # Setups the `:buffer` option of the given socket.
  defp setup_socket_buffers(socket) do
    {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
      :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])

    buffer = buffer |> max(sndbuf) |> max(recbuf)
    :ok = :inet.setopts(socket, [buffer: buffer])
  end

  defp extract_client_from_queued_item({:command, from}), do: from
  defp extract_client_from_queued_item({:pipeline, from, _}), do: from

  defp format_resp(%Redix.Error{} = err), do: {:error, err}
  defp format_resp(resp), do: {:ok, resp}

  defp auth_and_select_db(s) do
    case auth(s, s.opts[:password]) do
      {:ok, s} ->
        case select_db(s, s.opts[:database]) do
          {:ok, s} ->
            reactivate_socket(s)
            {:ok, s}
          o ->
            o
        end
      o ->
        o
    end
  end

  defp auth(s, nil) do
    {:ok, s}
  end

  defp auth(%{socket: socket} = s, password) when is_binary(password) do
    case :gen_tcp.send(socket, Protocol.pack(["AUTH", password])) do
      :ok ->
        case wait_for_response(s) do
          {:ok, "OK", s} ->
            {:ok, s}
          {:ok, error, s} ->
            {:stop, error, s}
          {:error, reason} ->
            {:stop, reason, s}
        end
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp select_db(s, nil) do
    {:ok, s}
  end

  defp select_db(%{socket: socket} = s, db) do
    case :gen_tcp.send(socket, Protocol.pack(["SELECT", db])) do
      :ok ->
        case wait_for_response(s) do
          {:ok, "OK", s} ->
            {:ok, s}
          {:ok, error, s} ->
            {:stop, error, s}
          {:error, reason} ->
            {:stop, reason, s}
        end
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp wait_for_response(%{socket: socket} = s) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        data = s.tail <> data
        case Protocol.parse(data) do
          {:ok, value, rest} ->
            {:ok, value, %{s | tail: rest}}
          {:error, :incomplete} ->
            wait_for_response(%{s | tail: data})
        end
      {:error, _} = err ->
        err
    end
  end

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
  # {:backoff, ...} if we're under the max number of allowed reconnection
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
