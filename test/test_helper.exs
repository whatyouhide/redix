ExUnit.start(assert_receive_timeout: 500)

Logger.configure(level: :debug)

# case :gen_tcp.connect('localhost', 6379, []) do
#   {:ok, socket} ->
#     :ok = :gen_tcp.close(socket)

#   {:error, reason} ->
#     Mix.raise("Cannot connect to Redis (http://localhost:6379): #{:inet.format_error(reason)}")
# end
