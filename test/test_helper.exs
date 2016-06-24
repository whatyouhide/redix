ExUnit.start()

host = (System.get_env("HOST") || "localhost") |> String.to_char_list
port = System.get_env("PORT") || 6379

case :gen_tcp.connect(host, port, []) do
  {:ok, socket} ->
    :gen_tcp.close(socket)
  {:error, reason} ->
    Mix.raise "Cannot connect to Redis (http://#{host}:#{port}): #{:inet.format_error(reason)}"
end

defmodule Redix.TestHelpers do
  # TODO: replace with ExUnit.CaptureIO once we can depend on ~> 1.1
  def silence_log(fun) do
    Logger.remove_backend :console
    fun.()
  after
    Logger.add_backend :console, flush: true
  end
end
