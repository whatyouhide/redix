defmodule Redix.UtilsTest do
  use ExUnit.Case, async: true

  import Redix.Utils

  test "format_host/1" do
    assert format_host(%{opts: [host: 'example.com', port: 6379]}) == "example.com:6379"
  end

  test "sanitize_starting_opts/1" do
    opts = [host: "foo.com", backoff_max: 0, sync_connect: true]
    opts = sanitize_starting_opts(opts)
    assert opts[:host] == 'foo.com'
    assert opts[:backoff_max] == 0
    assert opts[:sync_connect] == true

    assert_raise ArgumentError, "unknown option: :foo", fn ->
      sanitize_starting_opts(foo: 1)
    end

    test "raise ArgumentError on non-keyword list" do
      message = "expected a keyword list as options, got: [\"foo.com\"]"

    assert_raise ArgumentError, message, fn ->
      sanitize_starting_opts(port: %{})
    end
  end
end
