defmodule Recs.Connection do
  @moduledoc false

  use Connection

  alias Recs.Protocol

  @initial_state %{
    socket: nil,
    tail: "",
    opts: nil,
    queue: :queue.new,
  }

  @socket_opts [:binary, active: false]

  ## Callbacks

  @doc false
  def init(opts) do
    {:connect, :init, Dict.merge(@initial_state, opts: opts)}
  end

  @doc false
  def connect(_info, %{opts: opts} = s) do
    case :gen_tcp.connect(to_char_list(opts[:host]), opts[:port], @socket_opts) do
      {:ok, socket} ->
        s = %{s | socket: socket}
        {:ok, s} = case auth(s) do
          {:ok, s} ->
            select_db(s)
        end

        :inet.setopts(socket, active: :once)
        setup_socket_buffers(socket)

        {:ok, s}
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  @doc false
  def handle_call(operation, from, s)

  def handle_call({:command, args}, from, %{queue: queue} = s) do
    s = %{s | queue: :queue.in({:command, from}, queue)}
    send_noreply(s, Protocol.pack(args))
  end

  def handle_call({:pipeline, commands}, from, %{queue: queue} = s) do
    s = %{s | queue: :queue.in({:pipeline, from, length(commands)}, queue)}
    send_noreply(s, Enum.map(commands, &Protocol.pack/1))
  end

  @doc false
  def handle_info(msg, s)

  def handle_info({:tcp, socket, data}, %{socket: socket} = s) do
    :inet.setopts(socket, active: :once)
    s = new_data(s, s.tail <> data)
    {:noreply, s}
  end

  ## Helper functions

  defp new_data(s, <<>>) do
    s
  end

  defp new_data(s, data) do
    {from, parser, new_queue} = dequeue(s)

    case parser.(data) do
      {:ok, resp, rest} ->
        Connection.reply(from, resp)
        s = %{s | queue: new_queue}
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

  defp auth(%{opts: opts, socket: socket} = s) do
    if password = opts[:password] do
      :ok = pack_and_send(s, ["AUTH", password])
      {:ok, data} = :gen_tcp.recv(s, 0)
      {:ok, "OK", rest} = Protocol.parse(data)
    end

    {:ok, s}
  end

  defp select_db(%{opts: opts, socket: socket} = s) do
    if db = opts[:database] do
      :ok = pack_and_send(s, ["SELECT", db])
      {:ok, data} = :gen_tcp.recv(s, 0)
      {:ok, _, rest} = Protocol.parse(data)
    end

    {:ok, s}
  end

  defp send_noreply(%{socket: socket} = s, data) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        {:noreply, s}
      {:error, _reason} = error ->
        {:disconnect, error, s}
    end
  end

  defp pack_and_send(%{socket: socket}, args) do
    :gen_tcp.send(socket, Protocol.pack(args))
  end

  defp setup_socket_buffers(socket) do
    {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
      :inet.getopts(socket, [:sndbuf, :recbuf, :buffer])

    buffer = buffer |> max(sndbuf) |> max(recbuf)
    :ok = :inet.setopts(socket, [buffer: buffer])
  end
end
