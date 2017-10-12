ExUnit.start()

if Code.ensure_loaded?(ExUnitProperties) do
  Application.ensure_all_started(:stream_data)
end

host = System.get_env("REDIX_TEST_HOST") || "localhost"
port = String.to_integer(System.get_env("REDIX_TEST_PORT") || "6379")

case :gen_tcp.connect(String.to_charlist(host), port, []) do
  {:ok, socket} ->
    :gen_tcp.close(socket)

  {:error, reason} ->
    Mix.raise("Cannot connect to Redis (http://#{host}:#{port}): #{:inet.format_error(reason)}")
end

defmodule Redix.TestHelpers do
  def test_host(), do: unquote(host)
  def test_port(), do: unquote(port)

  def parse_with_continuations(data, parser_fun \\ &Redix.Protocol.parse/1)

  def parse_with_continuations([data], parser_fun) do
    parser_fun.(data)
  end

  def parse_with_continuations([first | rest], parser_fun) do
    import ExUnit.Assertions

    {rest, [last]} = Enum.split(rest, -1)

    assert {:continuation, cont} = parser_fun.(first)

    last_cont =
      Enum.reduce(rest, cont, fn data, cont_acc ->
        assert {:continuation, cont_acc} = cont_acc.(data)
        cont_acc
      end)

    last_cont.(last)
  end
end
