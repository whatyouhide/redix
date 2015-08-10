defmodule Recs.ParserTest do
  use ExUnit.Case

  import Recs.Parser
  alias Recs.Parser.ParseError

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
end
