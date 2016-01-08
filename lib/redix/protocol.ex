defmodule Redix.Protocol do
  @moduledoc """
  This module provides functions to work with the [Redis binary
  protocol](http://redis.io/topics/protocol).
  """

  defmodule ParseError do
    @moduledoc """
    Error in parsing data according to the
    [RESP](http://redis.io/topics/protocol) protocol.
    """

    defexception [:message]
  end

  @type redis_value :: binary | integer | nil | Redix.Error.t | [redis_value]

  @crlf "\r\n"

  @doc ~S"""
  Packs a list of Elixir terms to a Redis (RESP) array.

  This function returns an iodata (instead of a binary) because the packed
  result is usually sent to Redis through `:gen_tcp.send/2` or similar. It can
  be converted to a binary with `IO.iodata_to_binary/1`.

  All elements of `elems` are converted to strings with `to_string/1`, hence
  this function supports integers, atoms, string, char lists and whatnot. Since
  `to_string/1` uses the `String.Chars` protocol, running this with consolidated
  protocols makes it quite faster (even if this is probably not the bottleneck
  of your application).

  ## Examples

      iex> iodata = Redix.Protocol.pack ["SET", "mykey", 1]
      iex> IO.iodata_to_binary(iodata)
      "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$1\r\n1\r\n"

  """
  @spec pack([binary]) :: iodata
  def pack(elems) when is_list(elems) do
    packed = for el <- elems, str = to_string(el) do
      [?$, Integer.to_string(byte_size(str)), @crlf, str, @crlf]
    end

    [?*, to_string(length(elems)), @crlf, packed]
  end

  @doc ~S"""
  Parses a RESP-encoded value from the given `data`.

  Returns `{:ok, value, rest}` if a value is parsed successfully, or a
  continuation in the form `{:continuation, fun}` if the data is incomplete.

  ## Examples

      iex> Redix.Protocol.parse "+OK\r\ncruft"
      {:ok, "OK", "cruft"}

      iex> Redix.Protocol.parse "-ERR wrong type\r\n"
      {:ok, %Redix.Error{message: "ERR wrong type"}, ""}

      iex> {:continuation, fun} = Redix.Protocol.parse "+OK"
      iex> fun.("\r\n")
      {:ok, "OK", ""}

  """
  @spec parse(binary) :: {:ok, redis_value, binary} | {:error, term}
  def parse(data)

  def parse("+" <> rest), do: parse_simple_string(rest)
  def parse("-" <> rest), do: parse_error(rest)
  def parse(":" <> rest), do: parse_integer(rest)
  def parse("$" <> rest), do: parse_bulk_string(rest)
  def parse("*" <> rest), do: parse_array(rest)
  def parse(""), do: mkcont(&parse/1)
  def parse(<<b>> <> _), do: raise(ParseError, message: "invalid type specifier (byte #{inspect <<b>>})")

  @doc ~S"""
  Parses `n` RESP-encoded values from the given `data`.

  Each element is parsed as described in `parse/1`. If there's an error in
  parsing any of the elements or there are less than `n` elements, a
  continuation in the form of `{:continuation, fun}` is returned. Otherwise,
  `{:ok, values, rest}` is returned.

  ## Examples

      iex> parse_multi("+OK\r\n+COOL\r\n", 2)
      {:ok, ["OK", "COOL"], ""}

      iex> {:continuation, fun} = parse_multi("+OK\r\n", 2)
      iex> fun.("+OK\r\n")
      {:ok, ["OK", "OK"], ""}

  """
  @spec parse_multi(binary, non_neg_integer) :: {:ok, [redis_value], binary} | {:error, term}
  def parse_multi(data, nelems)

  # We treat the case when we have just one element to parse differently as it's
  # a very common case since single commands are treated as pipelines with just
  # one command in them.
  def parse_multi(data, 1) do
    resolve_cont(parse(data), &{:ok, [&1], &2})
  end

  def parse_multi(data, n) do
    take_elems(data, n, [])
  end

  defp parse_simple_string(data) do
    until_crlf(data)
  end

  defp parse_error(data) do
    data
    |> until_crlf()
    |> resolve_cont(&{:ok, %Redix.Error{message: &1}, &2})
  end

  # We need this clause explicitely only here because parse_integer/1 is the
  # only function that doesn't rely on until_crlf/1 (which returns {:error,
  # :incomplete} for empty binaries.
  defp parse_integer("") do
    mkcont(&parse_integer/1)
  end

  defp parse_integer("-") do
    mkcont(&parse_integer("-" <> &1))
  end

  defp parse_integer(rest) do
    case Integer.parse(rest) do
      {i, @crlf <> rest} ->
        {:ok, i, rest}
      {i, rest} when rest == "" or rest == "\r" ->
        mkcont fn(new_data) ->
          until_crlf(rest <> new_data) |> resolve_cont(fn("", rest) -> {:ok, i, rest} end)
        end
      {_i, _rest} ->
        raise ParseError, message: "not a valid integer: #{inspect rest}"
      :error ->
        raise ParseError, message: "not a valid integer: #{inspect rest}"
    end |> resolve_cont(&{:ok, &1, &2})
  end

  defp parse_bulk_string(rest) do
    resolve_cont parse_integer(rest), fn
      -1, rest  -> {:ok, nil, rest}
      len, rest -> parse_string_of_known_size(rest, len)
    end
  end

  defp parse_string_of_known_size(data, len) do
    case data do
      <<str :: bytes-size(len), @crlf, rest :: binary>> ->
        {:ok, str, rest}
      _ ->
        mkcont fn(new_data) -> parse_string_of_known_size(data <> new_data, len) end
    end
  end

  defp parse_array(rest) do
    resolve_cont parse_integer(rest), fn
      -1, rest     -> {:ok, nil, rest}
      nelems, rest -> take_elems(rest, nelems, [])
    end
  end

  defp until_crlf(data, acc \\ "")

  defp until_crlf(@crlf <> rest, acc) do
    {:ok, acc, rest}
  end

  defp until_crlf(data, acc) when data == "" or data == "\r" do
    mkcont(&until_crlf(data <> &1, acc))
  end

  defp until_crlf(<<h, rest :: binary>>, acc) do
    until_crlf(rest, <<acc :: binary, h>>)
  end

  defp take_elems(data, 0, acc) do
    {:ok, Enum.reverse(acc), data}
  end

  defp take_elems(<<_, _ :: binary>> = data, n, acc) when n > 0 do
    resolve_cont parse(data), fn(elem, rest) ->
      take_elems(rest, n - 1, [elem|acc])
    end
  end

  defp take_elems(<<>>, n, acc) do
    mkcont(&take_elems(&1, n, acc))
  end

  defp resolve_cont({:ok, val, rest}, ok) when is_function(ok, 2),
    do: ok.(val, rest)
  defp resolve_cont({:continuation, cont}, ok),
    do: mkcont(fn(new_data) -> resolve_cont(cont.(new_data), ok) end)

  @compile {:inline, mkcont: 1}
  defp mkcont(fun) do
    {:continuation, fun}
  end
end
