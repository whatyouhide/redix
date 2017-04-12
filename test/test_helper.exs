ExUnit.start()

host = System.get_env("REDIX_TEST_HOST") || "localhost"
port = String.to_integer(System.get_env("REDIX_TEST_PORT") || "6379")

charlist_host =
  if function_exported?(String, :to_charlist, 1) do
    apply(String, :to_charlist, [host])
  else
    apply(String, :to_char_list, [host])
  end

case :gen_tcp.connect(charlist_host, port, []) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Cannot connect to Redis (http://#{host}:#{port}): #{:inet.format_error(reason)}"
end

defmodule Redix.TestHelpers do
  def test_host(), do: unquote(host)
  def test_port(), do: unquote(port)
end
