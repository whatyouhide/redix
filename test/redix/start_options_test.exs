defmodule Redix.StartOptionsTest do
  use ExUnit.Case, async: true

  alias Redix.StartOptions

  test "sanitize/1" do
    opts = StartOptions.sanitize(host: "foo.com", backoff_max: 0, sync_connect: true)

    assert opts[:host] == 'foo.com'
    assert opts[:backoff_max] == 0
    assert opts[:sync_connect] == true
  end
end
