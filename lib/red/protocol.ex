defmodule Red.Protocol do
  defmodule ParseError do
    @moduledoc """
    This error represents an error in parsing data according to the
    [RESP](http://redis.io/topics/protocol) protocol.
    """

    defexception [:message]
  end

  @type redis_value :: binary | integer | nil | [redis_value]

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

      iex> iodata = Red.Protocol.pack ["SET", "mykey", 1]
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
  Parses a RESP-encoded value from the given `binary`.

  Returns `{:ok, value, rest}` if a value is parsed successfully, `{:error,
  :reason}` otherwise.

  ## Examples

      iex> Red.Protocol.parse "+OK\r\ncruft"
      {:ok, "OK", "cruft"}

      iex> Red.Protocol.parse "+OK"
      {:error, :incomplete}

  """
  @spec parse(binary) :: {:ok, redis_value, binary} | {:error, term}
  def parse(data)

  def parse("$-1" <> @crlf <> rest), do: {:ok, nil, rest}
  def parse("*-1" <> @crlf <> rest), do: {:ok, nil, rest}
  def parse("$-1" <> _), do: {:error, :incomplete}
  def parse("*-1" <> _), do: {:error, :incomplete}
  def parse("+" <> rest), do: parse_simple_string(rest)
  def parse("-" <> rest), do: parse_error(rest)
  def parse(":" <> rest), do: parse_integer(rest)
  def parse("$" <> rest), do: parse_bulk_string(rest)
  def parse("*" <> rest), do: parse_array(rest)
  def parse(""), do: {:error, :incomplete}
  def parse(_), do: raise(ParseError, message: "no type specifier")

  @doc ~S"""
  Parses `n` RESP-encoded values from the given `data`.

  Each element is parsed as described in `parse/1`. If there's an error in
  parsing any of the elements or there are less than `n` elements, `{:error,
  reason}` is returned. Otherwise, `{:ok, values, rest}` is returned.

  ## Examples

      iex> parse_multi("+OK\r\n+COOL\r\n", 2)
      {:ok, ["OK", "COOL"], ""}

      iex> parse_multi("+OK\r\n", 2)
      {:error, :incomplete}

  """
  @spec parse_multi(binary, non_neg_integer) :: {:ok, [redis_value], binary} | {:error, term}
  def parse_multi(data, n) do
    parse_multi(data, n, [])
  end

  defp parse_multi(data, 0, acc) do
    {:ok, Enum.reverse(acc), data}
  end

  defp parse_multi(data, n, acc) do
    case parse(data) do
      {:ok, val, rest} ->
        parse_multi(rest, n - 1, [val|acc])
      {:error, _} = error ->
        error
    end
  end

  defp parse_simple_string(data) do
    until_crlf(data)
  end

  defp parse_error(data) do
    until_crlf(data)
  end

  defp parse_integer(rest) do
    case Integer.parse(rest) do
      {i, @crlf <> rest} ->
        {:ok, i, rest}
      {_i, <<>>} ->
        {:error, :incomplete}
      {_, _rest} ->
        raise ParseError, message: "not a valid integer: #{inspect rest}"
      :error ->
        raise ParseError, message: "not a valid integer: #{inspect rest}"
    end
  end

  defp parse_bulk_string(rest) do
    case parse_integer(rest) do
      {:ok, len, rest} ->
        case rest do
          <<str :: bytes-size(len), @crlf, rest :: binary>> ->
            {:ok, str, rest}
          _ ->
            {:error, :incomplete}
        end
      {:error, _} = err ->
        err
    end
  end

  defp parse_array(rest) do
    case parse_integer(rest) do
      {:ok, nelems, rest} ->
        take_n_elems(rest, nelems, [])
      {:error, _} = err ->
        err
    end
  end

  defp until_crlf(data, acc \\ "")

  defp until_crlf(@crlf <> rest, acc) do
    {:ok, acc, rest}
  end

  defp until_crlf(<<h, rest :: binary>>, acc) do
    until_crlf(rest, <<acc :: binary, h>>)
  end

  defp until_crlf(<<>>, _acc) do
    {:error, :incomplete}
  end

  defp take_n_elems(data, 0, acc) do
    {:ok, Enum.reverse(acc), data}
  end

  defp take_n_elems(<<_, _ :: binary>> = data, n, acc) when n > 0 do
    case parse(data) do
      {:ok, val, rest} ->
        take_n_elems(rest, n - 1, [val|acc])
      {:error, _} = err ->
        err
    end
  end

  defp take_n_elems(<<>>, _n, _acc) do
    {:error, :incomplete}
  end
end
