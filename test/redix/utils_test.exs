defmodule Redix.UtilsTest do
  use ExUnit.Case, async: true

  import Redix.Utils

  test "format_host/1" do
    assert format_host(%{opts: [host: 'example.com', port: 6379]}) == "example.com:6379"
  end

  test "sanitize_starting_opts/2" do
    redis_opts = [host: "foo.com"]
    other_opts = [backoff_max: 0, sync_connect: true, name: :redix]
    assert {redix_opts, connection_opts} = sanitize_starting_opts(redis_opts, other_opts)
    assert redix_opts[:host] == "foo.com"
    assert redix_opts[:backoff_max] == 0
    assert redix_opts[:sync_connect] == true
    assert connection_opts[:name] == :redix

    assert_raise ArgumentError, ~r/unknown Redis connection option: :foo/, fn ->
      sanitize_starting_opts([foo: 1], [])
    end

    message = "expected an integer as the value of the :port option, got: %{}"

    assert_raise ArgumentError, message, fn ->
      sanitize_starting_opts([port: %{}], [])
    end
  end
end
