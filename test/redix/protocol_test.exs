defmodule Redix.ProtocolTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Redix.TestHelpers, only: [parse_with_continuations: 1, parse_with_continuations: 2]

  alias Redix.{Error, Protocol.ParseError}

  doctest Redix.Protocol

  describe "pack/1" do
    import Redix.Protocol, only: [pack: 1]

    test "empty array" do
      assert IO.iodata_to_binary(pack([])) == "*0\r\n"
    end

    test "regular strings" do
      assert IO.iodata_to_binary(pack(["foo", "bar"])) == "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
      assert IO.iodata_to_binary(pack(["with spaces "])) == "*1\r\n$12\r\nwith spaces \r\n"
    end

    test "unicode" do
      str = "føø"
      size = byte_size(str)
      assert IO.iodata_to_binary(pack([str])) == "*1\r\n$#{size}\r\n#{str}\r\n"
    end

    test "raw bytes (non printable)" do
      assert IO.iodata_to_binary(pack([<<1, 2>>])) == <<"*1\r\n$2\r\n", 1, 2, "\r\n">>
    end
  end

  describe "parse/1" do
    import Redix.Protocol, only: [parse: 1]

    property "simple strings" do
      check all string <- string(:alphanumeric),
                split_command <- random_splits("+#{string}\r\n"),
                split_command_with_rest = append_to_last(split_command, "rest") do
        assert parse_with_continuations(split_command) == {:ok, string, ""}
        assert parse_with_continuations(split_command_with_rest) == {:ok, string, "rest"}
      end
    end

    property "errors" do
      check all error_message <- string(:alphanumeric),
                split_command <- random_splits("-#{error_message}\r\n"),
                split_command_with_rest = append_to_last(split_command, "rest") do
        error = %Error{message: error_message}

        assert parse_with_continuations(split_command) == {:ok, error, ""}

        error = %Error{message: error_message}

        assert parse_with_continuations(split_command_with_rest) == {:ok, error, "rest"}
      end
    end

    property "integers" do
      check all int <- integer(),
                split_command <- random_splits(":#{int}\r\n"),
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

    property "arrays" do
      assert parse("*0\r\n") == {:ok, [], ""}
      assert parse("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == {:ok, ["foo", "bar"], ""}

      # Mixed types
      arr = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"
      assert parse(arr) == {:ok, [1, 2, 3, 4, "foobar"], ""}

      # Says it has only 1 value, has 2
      assert parse("*1\r\n:1\r\n:2\r\n") == {:ok, [1], ":2\r\n"}

      command = "*-1\r\n"

      check all split_command <- random_splits(command),
                split_command_with_rest = append_to_last(split_command, "rest") do
        assert parse_with_continuations(split_command) == {:ok, nil, ""}
        assert parse_with_continuations(split_command_with_rest) == {:ok, nil, "rest"}
      end

      # Null values (of different types) in the array
      arr = "*4\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n*-1\r\n"
      assert parse(arr) == {:ok, ["foo", nil, "bar", nil], ""}

      # Nested
      arr = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-ERR Bar\r\n"
      assert parse(arr) == {:ok, [[1, 2, 3], ["Foo", %Error{message: "ERR Bar"}]], ""}

      payload = ["*", "1\r", "\n", "+OK", "\r\nrest"]
      assert parse_with_continuations(payload) == {:ok, ["OK"], "rest"}

      payload = ["*2", "\r\n*1", "\r\n", "+", "OK\r\n", ":1", "\r\n"]
      assert parse_with_continuations(payload) == {:ok, [["OK"], 1], ""}

      payload = ["*2\r\n+OK\r\n", "+OK\r\nrest"]
      assert parse_with_continuations(payload) == {:ok, ~w(OK OK), "rest"}
    end

    test "raising when the binary value has no type specifier" do
      msg = ~s[invalid type specifier ("f")]
      assert_raise ParseError, msg, fn -> parse("foo\r\n") end
      assert_raise ParseError, msg, fn -> parse("foo bar baz") end
    end

    test "when the binary it's an invalid integer" do
      assert_raise ParseError, ~S(expected integer, found: "\r"), fn -> parse(":\r\n") end
      assert_raise ParseError, ~S(expected integer, found: "\r"), fn -> parse(":-\r\n") end
      assert_raise ParseError, ~S(expected CRLF, found: "a"), fn -> parse(":43a\r\n") end
      assert_raise ParseError, ~S(expected integer, found: "f"), fn -> parse(":foo\r\n") end
    end
  end

  describe "parse_multi/2" do
    import Redix.Protocol, only: [parse_multi: 2]

    test "enough elements" do
      data = "+OK\r\n+OK\r\n+OK\r\n"
      assert parse_multi(data, 3) == {:ok, ~w(OK OK OK), ""}
      assert parse_multi(data, 2) == {:ok, ~w(OK OK), "+OK\r\n"}
    end

    test "not enough data" do
      data = ["+OK\r\n+OK\r\n", "+", "OK", "\r\nrest"]
      assert parse_with_continuations(data, &parse_multi(&1, 3)) == {:ok, ~w(OK OK OK), "rest"}
    end
  end

  defp random_splits(splittable_part) do
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
