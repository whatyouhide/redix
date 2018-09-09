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
  end
end
