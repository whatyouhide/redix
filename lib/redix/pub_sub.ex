defmodule Redix.PubSub do
  def start_link do
    Redix.start_link(pubsub: true)
  end

  def start_link(opts) when is_list(opts) do
    opts |> Keyword.put(:pubsub, true) |> Redix.start_link
  end

  def start_link(uri) when is_binary(uri) do
    Redix.start_link(uri, pubsub: true)
  end

  def start_link(uri, opts) do
    Redix.start_link(uri, Keyword.put(opts, :pubsub, true))
  end

  def subscribe(conn, channels, recipient) do
    Connection.cast(conn, {:pubsub_subscribe, List.wrap(channels), recipient})
  end

  def psubscribe(conn, patterns, recipient) do
    Connection.cast(conn, {:pubsub_psubscribe, List.wrap(patterns), recipient})
  end

  def unsubscribe(conn, channels, recipient) do
    Connection.cast(conn, {:pubsub_unsubscribe, List.wrap(channels), recipient})
  end

  def punsubscribe(conn, patterns, recipient) do
    Connection.cast(conn, {:pubsub_punsubscribe, List.wrap(patterns), recipient})
  end
end
