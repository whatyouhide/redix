defmodule Redix.UtilsTest do
  use ExUnit.Case, async: true

  import Redix.Utils

  test "format_error/1 known error" do
    assert format_error(:eaddrinuse) == 'address already in use'
  end

  test "format_error/1 unknown error" do
    assert format_error(:unknown_error) == ":unknown_error"
  end
end
