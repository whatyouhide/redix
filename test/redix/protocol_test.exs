defmodule Redix.ProtocolTest do
  use ExUnit.Case, async: true

  import Redix.Protocol
  alias Redix.Protocol.ParseError
  alias Redix.Error, as: Err

  doctest Redix.Protocol

  ## Packing

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

  ## Parsing

  test "parse/1: simple strings" do
    assert parse("+OK\r\n") == {:ok, "OK", ""}
    assert parse("+the foo is the bar\r\n") == {:ok, "the foo is the bar", ""}
    assert parse("+\r\n") == {:ok, "", ""}
    assert parse("+• º •\r\n") == {:ok, "• º •", ""}

    # Whitespace
    assert parse("+  \r\n") == {:ok, "  ", ""}
    assert parse("+\r\r\n") == {:ok, "\r", ""}
    assert parse("+\n\r\r\n") == {:ok, "\n\r", ""}
    assert parse("+  stripme!  \r\n") == {:ok, "  stripme!  ", ""}

    # Trailing stuff
    assert parse("+foo\r\nbar") == {:ok, "foo", "bar"}

    # Incomplete
    assert parse_with_continuations(["+", "OK", "\r", "\nrest"]) == {:ok, "OK", "rest"}
    assert parse_with_continuations(["+OK\r", "\nrest"]) == {:ok, "OK", "rest"}
  end

  test "parse/1: errors" do
    assert parse("-ERR Error!\r\n") == {:ok, %Err{message: "ERR Error!"}, ""}
    assert parse("-\r\n") == {:ok, %Err{message: ""}, ""}
    assert parse("- Error message \r\n") == {:ok, %Err{message: " Error message "}, ""}
    assert parse("-üniçø∂e\r\n") == {:ok, %Err{message: "üniçø∂e"}, ""}

    assert parse_with_continuations(["-", "Err", "or", "\r\nrest"]) == {:ok, %Err{message: "Error"}, "rest"}
    assert parse_with_continuations(["-Error\r", "\nrest"]) == {:ok, %Err{message: "Error"}, "rest"}
  end

  test "parse/1: integers" do
    assert parse(":42\r\n") == {:ok, 42, ""}
    assert parse(":1234567890\r\n") == {:ok, 1234567890, ""}
    assert parse(":-100\r\n") == {:ok, -100, ""}
    assert parse(":0\r\nrest") == {:ok, 0, "rest"}

    assert parse_with_continuations([":", "-", "1", "\r\n"]) == {:ok, -1, ""}
    assert parse_with_continuations([":1\r", "\nrest"]) == {:ok, 1, "rest"}
    assert parse_with_continuations([":1", "3", "\r\nrest"]) == {:ok, 13, "rest"}
    assert parse_with_continuations([":-", "1", "3", "\r\nrest"]) == {:ok, -13, "rest"}
  end

  test "parse/1: bulk strings" do
    assert parse("$0\r\n\r\n") == {:ok, "", ""}
    assert parse("$6\r\nfoobar\r\n") == {:ok, "foobar", ""}

    # Unicode.
    unicode_str = "ªº•¶æ"
    byte_count = byte_size(unicode_str)
    assert parse("$#{byte_count}\r\n#{unicode_str}\r\n") == {:ok, unicode_str, ""}

    # Non-printable bytes
    assert parse("$2\r\n" <> <<1, 2>> <> "\r\n") == {:ok, <<1, 2>>, ""}

    # CRLF in the bulk string.
    assert parse("$2\r\n\r\n\r\n") == {:ok, "\r\n", ""}

    # Incomplete
    assert parse_with_continuations(["$0", "\r\n\r", "\nrest"]) == {:ok, "", "rest"}
    assert parse_with_continuations(["$", "3", "\r\n", "foo\r\n"]) == {:ok, "foo", ""}

    # Null
    assert parse("$-1\r\n") == {:ok, nil, ""}
    assert parse("$-1\r\nrest") == {:ok, nil, "rest"}
    assert parse_with_continuations(["$-1", "\r", "\nrest"]) == {:ok, nil, "rest"}
    assert parse_with_continuations(["$", "-1", "\r\nrest"]) == {:ok, nil, "rest"}
    assert parse_with_continuations(["$-", "1", "\r", "\nrest"]) == {:ok, nil, "rest"}
  end

  test "parse/1: arrays" do
    assert parse("*0\r\n") == {:ok, [], ""}
    assert parse("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == {:ok, ["foo", "bar"], ""}

    # Mixed types
    arr = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
    assert parse(arr) == {:ok, [1, 2, 3, 4, "foobar"], ""}

    # Says it has only 1 value, has 2
    assert parse("*1\r\n:1\r\n:2\r\n") == {:ok, [1], ":2\r\n"}

    # Null
    assert parse("*-1\r\n") == {:ok, nil, ""}
    assert parse("*-1\r\nrest") == {:ok, nil, "rest"}
    assert parse_with_continuations(["*-1", "\r", "\nrest"]) == {:ok, nil, "rest"}
    assert parse_with_continuations(["*", "-1", "\r\nrest"]) == {:ok, nil, "rest"}
    assert parse_with_continuations(["*-", "1", "\r", "\nrest"]) == {:ok, nil, "rest"}

    # Null values (of different types) in the array
    arr = "*4\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n*-1\r\n"
    assert parse(arr) == {:ok, ["foo", nil, "bar", nil], ""}

    # Nested
    arr = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-ERR Bar\r\n"
    assert parse(arr) == {:ok, [[1, 2, 3], ["Foo", %Err{message: "ERR Bar"}]], ""}

    assert parse_with_continuations(["*", "1\r", "\n", "+OK", "\r\nrest"]) == {:ok, ["OK"], "rest"}
    assert parse_with_continuations(["*2", "\r\n*1", "\r\n", "+", "OK\r\n", ":1", "\r\n"]) == {:ok, [["OK"], 1], ""}
    assert parse_with_continuations(["*2\r\n+OK\r\n", "+OK\r\nrest"]) == {:ok, ~w(OK OK), "rest"}
  end

  test "parse/1: raising when the binary value has no type specifier" do
    msg = ~s[invalid type specifier ("f")]
    assert_raise ParseError, msg, fn -> parse("foo\r\n") end
    assert_raise ParseError, msg, fn -> parse("foo bar baz") end
  end

  test "parse/1: when the binary it's an invalid integer" do
    assert_raise ParseError, ~S(expected integer, found: "\r"), fn -> parse(":\r\n") end
    assert_raise ParseError, ~S(expected integer, found: "\r"), fn -> parse(":-\r\n") end
    assert_raise ParseError, ~S(expected CRLF, found: "a"), fn -> parse(":43a\r\n") end
    assert_raise ParseError, ~S(expected integer, found: "f"), fn -> parse(":foo\r\n") end
  end

  test "parse_multi/2: enough elements" do
    data = "+OK\r\n+OK\r\n+OK\r\n"
    assert parse_multi(data, 3) == {:ok, ~w(OK OK OK), ""}
    assert parse_multi(data, 2) == {:ok, ~w(OK OK), "+OK\r\n"}
  end

  test "parse_multi/2: not enough data" do
    data = ["+OK\r\n+OK\r\n", "+", "OK", "\r\nrest"]
    assert parse_with_continuations(data, &parse_multi(&1, 3)) == {:ok, ~w(OK OK OK), "rest"}
  end

  defp b(bin) do
    IO.iodata_to_binary(bin)
  end

  defp parse_with_continuations(data, parser_fun \\ &parse/1)

  defp parse_with_continuations([data], parser_fun) do
    parser_fun.(data)
  end

  defp parse_with_continuations([first | rest], parser_fun) do
    {rest, [last]} = Enum.split(rest, -1)

    assert {:continuation, c} = parser_fun.(first)

    last_cont = Enum.reduce rest, c, fn data, acc ->
      assert {:continuation, new_acc} = acc.(data)
      new_acc
    end

    last_cont.(last)
  end

  if Code.ensure_loaded?(PropertyTest) do
    import PropertyTest

    property "simple strings" do
      check all string <- alphanumeric_string(),
                split_command <- random_splits("+#{string}\r\n"),
                split_command_with_rest = append_to_last(split_command, "rest") do
        assert parse_with_continuations(split_command) == {:ok, string, ""}
        assert parse_with_continuations(split_command_with_rest) == {:ok, string, "rest"}
      end
    end

    property "errors" do
      check all error_message <- alphanumeric_string(),
                split_command <- random_splits("-#{error_message}\r\n"),
                split_command_with_rest = append_to_last(split_command, "rest") do
        assert parse_with_continuations(split_command) == {:ok, %Err{message: error_message}, ""}
        assert parse_with_continuations(split_command_with_rest) == {:ok, %Err{message: error_message}, "rest"}
      end
    end

    property "integers" do
      check all int <- int(),
                string_int = Integer.to_string(int),
                split_command <- random_splits(":#{string_int}\r\n"),
                split_command_with_rest = append_to_last(split_command, "rest") do
        assert parse_with_continuations(split_command) == {:ok, int, ""}
        assert parse_with_continuations(split_command_with_rest) == {:ok, int, "rest"}
      end
    end

    property "bulk strings" do
      check all bin <- binary(),
                bin_size = byte_size(bin),
                command = "$#{bin_size}\r\n#{bin}\r\n",
                split_command <- random_splits(command),
                split_command_with_rest = append_to_last(split_command, "rest") do
        assert parse_with_continuations(split_command) == {:ok, bin, ""}
        assert parse_with_continuations(split_command_with_rest) == {:ok, bin, "rest"}
      end

      # nil
      check all split_command <- random_splits("$-1\r\n"),
                split_command_with_rest = append_to_last(split_command, "rest"),
                max_runs: 10 do
        assert parse_with_continuations(split_command) == {:ok, nil, ""}
        assert parse_with_continuations(split_command_with_rest) == {:ok, nil, "rest"}
      end
    end

    defp random_splits(splittable_part) do
      import StreamData
      bytes = for <<byte <- splittable_part>>, do: byte

      bytes
      |> Enum.map(fn _byte -> frequency([{2, false}, {1, true}]) end)
      |> List.replace_at(length(bytes) - 1, constant(false))
      |> fixed_list()
      |> map(fn split_points -> split_command(bytes, split_points, [""]) end)
    end

    defp split_command([byte | bytes], [true | split_points], [current | acc]) do
      split_command(bytes, split_points, ["", <<current::binary, byte>> | acc])
    end

    defp split_command([byte | bytes], [false | split_points], [current | acc]) do
      split_command(bytes, split_points, [<<current::binary, byte>> | acc])
    end

    defp split_command([], [], acc) do
      Enum.reverse(acc)
    end

    defp append_to_last(parts, unsplittable_part) do
      {first_parts, [last_part]} = Enum.split(parts, -1)
      first_parts ++ [last_part <> unsplittable_part]
    end
  end
end
