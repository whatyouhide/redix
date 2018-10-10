defmodule Redix.UtilsTest do
  use ExUnit.Case, async: true

  import Redix.Utils

  test "format_host/1" do
    assert format_host(%{opts: [host: 'example.com', port: 6379]}) == "example.com:6379"
  end
end
