defmodule Recs.Protocol do
  defmodule ParseError do
    defexception [:message]
  end

  @type redis_value :: binary | integer | nil | [redis_value]

  @crlf "\r\n"

  @spec pack([binary]) :: iodata
  def pack(strings) when is_list(strings) do
    packed = for str <- strings, str = to_string(str) do
      [?$, Integer.to_string(byte_size(str)), @crlf, str, @crlf]
    end

    [?*, to_string(length(strings)), @crlf, packed]
  end

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
  def parse(_), do: raise(ParseError, message: "no type specifier")

  @spec parse_multi(binary, non_neg_integer) :: {:ok, [redis_value], binary} | {:error, term}
  def parse_multi(data, n) do
    parse_multi(data, n, [])
  end

  def parse_multi(data, 0, acc) do
    {:ok, Enum.reverse(acc), data}
  end

  def parse_multi(data, n, acc) do
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
