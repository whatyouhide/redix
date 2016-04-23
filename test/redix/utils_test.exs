defmodule Redix.UtilsTest do
  use ExUnit.Case, async: true

  import Redix.Utils

  test "format_error/1 known error" do
    assert format_error(:eaddrinuse) == "address already in use"
  end

  test "format_error/1 unknown error" do
    assert format_error(:unknown_error) == ":unknown_error"
  end

  test "format_host/1" do
    assert format_host(%{opts: [host: 'example.com', port: 6379]}) == "example.com:6379"
  end

  test "sanitize_starting_opts/2" do
    redis_opts = [host: "foo.com"]
    other_opts = [max_backoff: 0, sync_connect: true, name: :redix]
    assert {redix_opts, connection_opts} = sanitize_starting_opts(redis_opts, other_opts)
    assert redix_opts[:host] == "foo.com"
    assert redix_opts[:max_backoff] == 0
    assert redix_opts[:sync_connect] == true
    assert connection_opts[:name] == :redix

    assert_raise ArgumentError, ~r/unknown Redis connection option: :foo/, fn ->
      sanitize_starting_opts([foo: 1], [])
    end
  end
end
