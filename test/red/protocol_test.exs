defmodule Red.ProtocolTest do
  use ExUnit.Case, async: true

  import Red.Protocol
  alias Red.Protocol.ParseError

  doctest Red.Protocol

  test "pack/1: empty array" do
    assert b(pack([])) == "*0\r\n"
  end

  test "pack/1: regular strings" do
    assert b(pack(["foo", "bar"])) == "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    assert b(pack(["with spaces "])) == "*1\r\n$12\r\nwith spaces \r\n"
  end

  test "pack/1: unicode" do
    str = "føø"
    size = byte_size(str)
    assert b(pack([str])) == "*1\r\n$#{size}\r\n#{str}\r\n"
  end

  test "pack/1: raw bytes (non printable)" do
    assert b(pack([<<1, 2>>])) == <<"*1\r\n$2\r\n", 1, 2, "\r\n">>
  end

  defp b(bin) do
    IO.iodata_to_binary(bin)
  end

  test "parse/1: simple strings" do
    assert parse("+OK\r\n") == {:ok, "OK", ""}
    assert parse("+the foo is the bar\r\n") == {:ok, "the foo is the bar", ""}
    assert parse("+\r\n") == {:ok, "", ""}

    # Unicode
    assert parse("+ø\r\n") == {:ok, "ø", ""}
    assert parse("+• º •\r\n") == {:ok, "• º •", ""}

    # Whitespace
    assert parse("+  \r\n") == {:ok, "  ", ""}
    assert parse("+\r\r\n") == {:ok, "\r", ""}
    assert parse("+\n\r\r\n") == {:ok, "\n\r", ""}
    assert parse("+  stripme!  \r\n") == {:ok, "  stripme!  ", ""}

    # Trailing stuff
    assert parse("+foo\r\nbar") == {:ok, "foo", "bar"}
  end

  test "parse/1 with error values that have no ERRORTYPE" do
    assert parse("-Error!\r\n") == {:ok, "Error!", ""}
    assert parse("-\r\n") == {:ok, "", ""}
    assert parse("-Error message\r\n") == {:ok, "Error message", ""}
    assert parse("-üniçø∂e\r\n") == {:ok, "üniçø∂e", ""}
    assert parse("-  whitespace  \r\n") == {:ok, "  whitespace  ", ""}
  end

  test "parse/1 with error values that have an ERRORTYPE" do
    assert parse("-ODDSPACES \r \n\r\n") == {:ok, "ODDSPACES \r \n", ""}
    assert parse("-UNICODE ø\r\n") == {:ok, "UNICODE ø", ""}
    assert parse("-ERR   Message\r\n") == {:ok, "ERR   Message", ""}
    assert parse("-WRONGTYPE foo bar\r\n") == {:ok, "WRONGTYPE foo bar", ""}
  end

  test "parse/1 with integer values" do
    assert parse(":42\r\n") == {:ok, 42, ""}
    assert parse(":1234567890\r\n") == {:ok, 1234567890, ""}
    assert parse(":-100\r\n") == {:ok, -100, ""}
    assert parse(":0\r\n") == {:ok, 0, ""}
  end

  test "parse/1 with bulk string values" do
    assert parse("$0\r\n\r\n") == {:ok, "", ""}
    assert parse("$6\r\nfoobar\r\n") == {:ok, "foobar", ""}

    # Unicode.
    unicode_str = "ªº•¶æ"
    byte_count = byte_size(unicode_str)
    assert parse("$#{byte_count}\r\n#{unicode_str}\r\n") == {:ok, unicode_str, ""}

    # CRLF in the bulk string.
    assert parse("$2\r\n\r\n\r\n") == {:ok, "\r\n", ""}
  end

  test "parse/1 with a null bulk string" do
    assert parse("$-1\r\n") == {:ok, nil, ""}
    assert parse("$-1\r\nrest") == {:ok, nil, "rest"}
    assert parse("$-1") == {:error, :incomplete}
  end

  test "parse/1 with a bulk string that contains non-printable bytes" do
    assert parse("$2\r\n" <> <<1, 2>> <> "\r\n") == {:ok, <<1, 2>>, ""}
  end

  test "parse/1 with a bulk string whose length is malformed" do
    assert parse("$2") == {:error, :incomplete}
  end

  test "parse/1 with an empty array" do
    assert parse("*0\r\n") == {:ok, [], ""}
  end

  test "parse/1 with a null array" do
    assert parse("*-1\r\n") == {:ok, nil, ""}
  end

  test "parse/1 with regular array values" do
    # Regular array.
    assert parse("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == {:ok, ["foo", "bar"], ""}

    # Arrays with mixed types.
    arr = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
    assert parse(arr) == {:ok, [1, 2, 3, 4, "foobar"], ""}

    # says it has only 1 value, has 2
    assert parse("*1\r\n:1\r\n:2\r\n") == {:ok, [1], ":2\r\n"}
  end

  test "parse/1 with nested array values" do
    arr = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-ERR Bar\r\n"
    assert parse(arr) == {:ok, [[1, 2, 3], ["Foo", "ERR Bar"]], ""}
  end

  test "parse/1 with arrays that contain null values" do
    # Null values (of different types) in the array.
    arr = "*4\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n*-1\r\n"
    assert parse(arr) == {:ok, ["foo", nil, "bar", nil], ""}
  end

  test "parse/1 with arrays whose length is incomplete" do
    assert parse("*1") == {:error, :incomplete}
  end

  test "parse/1 with arrays that contain incomplete elements" do
    assert parse("*1\r\n+OK") == {:error, :incomplete}
  end

  test "parse/1 with arrays with not enough elements" do
    assert parse("*2\r\n+OK\r\n") == {:error, :incomplete}
  end

  test "parse/1 when the binary value has no type specifier" do
    msg = "no type specifier"
    assert_raise ParseError, msg, fn -> parse("foo\r\n") end
    assert_raise ParseError, msg, fn -> parse("foo bar baz") end
  end

  test "parse/1 when the binary value is missing the CRLF terminator" do
    assert parse("+OK") == {:error, :incomplete}
    assert parse("+OK\r \n") == {:error, :incomplete}
    assert parse(":3") == {:error, :incomplete}
  end

  test "parse/1 when the binary it's an invalid integer" do
    assert_raise ParseError, ~S(not a valid integer: "\r\n"), fn ->
      parse(":\r\n")
    end

    assert_raise ParseError, ~S(not a valid integer: "43a\r\n"), fn ->
      parse(":43a\r\n")
    end

    assert_raise ParseError, ~S(not a valid integer: "foo\r\n"), fn ->
      parse(":foo\r\n")
    end
  end

  test "parse/1 with malformed arrays" do
    # says it has 4 values, only has 3
    assert parse("*4\r\n:1\r\n:2\r\n:3\r\n") == {:error, :incomplete}
  end

  test "parse/1 with malformed bulk strings" do
    # says it has 4 bytes, only has 3
    assert parse("$4\r\nops\r\n") == {:error, :incomplete}
  end

  test "parse_multi/2 with enough elements" do
    data = "+OK\r\n+OK\r\n+OK\r\n"
    assert parse_multi(data, 3) == {:ok, ~w(OK OK OK), ""}
    assert parse_multi(data, 2) == {:ok, ~w(OK OK), "+OK\r\n"}
  end

  test "parse_multi/2 with not enough data" do
    data = "+OK\r\n+OK\r\n"
    assert parse_multi(data, 3) == {:error, :incomplete}
  end

  test "parse_multi/2 when the data ends abruptedly" do
    data = "+OK\r\n+OK"
    assert parse_multi(data, 2) == {:error, :incomplete}
  end
end
