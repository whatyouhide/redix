ExUnit.start()

host = System.get_env("REDIX_TEST_HOST") || "localhost"
port = String.to_integer(System.get_env("REDIX_TEST_PORT") || "6379")

case :gen_tcp.connect(String.to_charlist(host), port, []) do
  {:ok, socket} ->
    :gen_tcp.close(socket)

  {:error, reason} ->
    Mix.raise("Cannot connect to Redis (http://#{host}:#{port}): #{:inet.format_error(reason)}")
end
