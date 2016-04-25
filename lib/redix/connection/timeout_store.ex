defmodule Redix.Connection.TimeoutStore do
  @moduledoc false

  # We want functions like Redix.command/3 to return {:error, :timeout} on
  # timeouts instead of exiting (like GenServer.call/3 does). To do this, we
  # wrap the call in a try/catch block and catch timeout exits. The problem now
  # is that the Redix receiver will still send the reply to the caller once this
  # reply arrives from Redis. To fix this, we changed the architecture a
  # bit. The process flows like this (sender is the Redix.Connection, receiver
  # is the Redix.Connection.Receiver). When the timeout happens, we catch it in
  # the caller; as soon as we catch it, we send a {:timed_out, request_id}
  # message to the sender to notify it that the caller doesn't want the reply
  # anymore (we do it in a GenServer.call, so the caller blocks until the sender
  # returns). The sender (which is not responsible for replying to callers),
  # will store this request_id in the timeout store (this module's GenServer)
  # with a cast; since the caller blocks until the sender returns, we're sure
  # that when the caller finishes then the timed out request id will be stored.
  #
  # Now, everytime a response arrives from Redis to the receiver, the receiver
  # will check if the request_id for the corresponding request in its queue is
  # in the timeout store (with a blocking call obviously, since it needs the
  # result). If it is, the request_id is removed from the timeout store and the
  # receiver will just discard the Redis response and not send it to the
  # caller. If the request_id is not in the timeout store, then the Redis
  # response will be forwarded to the caller.
  #
  # Now, a race condition could happen if the receiver receives the Redis
  # response *before* the timeout store has stored the request_id from the
  # sender cast. For this reason, the caller tries to receive (flush) a Redis
  # response right after sending the {:timed_out, request_id} caller to the
  # sender (with `after 0`). If it finds nothing in it mailbox, it means that no
  # race conditions happened and when the Redis response will arrive it will be
  # discarded by the receier; if it finds something, it discards it. However,
  # now the request_id will be hanging in the timeout store for ever (thus
  # possibly leaking memory): so if the caller finds a response, it will also
  # tell the sender to {:cancel_timed_out, request_id}, and the sender will
  # forward this to the timeout store.

  use GenServer

  def start_link() do
    GenServer.start_link(__MODULE__, nil)
  end

  def stop(pid) do
    GenServer.call(pid, :stop)
  end

  def add(pid, request_id) do
    GenServer.call(pid, {:add, request_id})
  end

  def remove(pid, request_id) do
    GenServer.cast(pid, {:remove, request_id})
  end

  def timed_out?(pid, request_id) do
    GenServer.call(pid, {:timed_out?, request_id})
  end

  ## Callbacks

  def init(_) do
    {:ok, HashSet.new}
  end

  def handle_call(:stop, from, state) do
    GenServer.reply(from, :ok)
    {:stop, :normal, state}
  end

  def handle_call({:timed_out?, request_id}, _from, state) do
    if HashSet.member?(state, request_id) do
      {:reply, true, HashSet.delete(state, request_id)}
    else
      {:reply, false, state}
    end
  end

  def handle_call({:add, request_id}, _from, state) do
    {:reply, :ok, HashSet.put(state, request_id)}
  end

  def handle_cast({:remove, request_id}, state) do
    {:noreply, HashSet.delete(state, request_id)}
  end
end
