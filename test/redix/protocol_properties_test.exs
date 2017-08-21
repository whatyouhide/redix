if Code.ensure_compiled?(PropertyTest) do
  defmodule Redix.Protocol.PropertiesTest do
    use ExUnit.Case, async: true

    import PropertyTest
    import Redix.TestHelpers, only: [parse_with_continuations: 1]

    alias Redix.{
      Error,
    }

    describe "parse/1 (with split input)" do
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
          assert parse_with_continuations(split_command) == {:ok, %Error{message: error_message}, ""}
          assert parse_with_continuations(split_command_with_rest) == {:ok, %Error{message: error_message}, "rest"}
        end
      end

      property "integers" do
        check all int <- integer(),
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
