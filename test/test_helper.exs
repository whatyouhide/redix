ExUnit.start(assert_receive_timeout: 500)

Logger.configure(level: :info)
:logger.set_application_level(:telemetry, :none)

base_port = Redix.TestPorts.port(:base)

case :gen_tcp.connect(~c"localhost", base_port, []) do
  {:ok, socket} ->
    :ok = :gen_tcp.close(socket)

  {:error, reason} ->
    Mix.raise(
      "Cannot connect to Redis (http://localhost:#{base_port}): #{:inet.format_error(reason)}"
    )
end
