defmodule Redix.URITest do
  use ExUnit.Case, async: true

  import Redix.URI

  test "opts_from_uri/1: invalid scheme" do
    message = "expected scheme to be redis:// or rediss://, got: foo://"

    assert_raise ArgumentError, message, fn ->
      opts_from_uri("foo://example.com")
    end
  end

  test "opts_from_uri/1: just the host" do
    opts = opts_from_uri("redis://example.com")
    assert opts[:host] == "example.com"
    assert is_nil(opts[:port])
    assert is_nil(opts[:database])
    assert is_nil(opts[:password])
  end

  test "opts_from_uri/1: host and port" do
    opts = opts_from_uri("redis://localhost:6379")
    assert opts[:host] == "localhost"
    assert opts[:port] == 6379
    assert is_nil(opts[:database])
    assert is_nil(opts[:password])
  end

  test "opts_from_uri/1: password" do
    opts = opts_from_uri("redis://:pass@localhost")
    assert opts[:host] == "localhost"
    assert opts[:password] == "pass"

    # If there's no ":", we error out.
    assert_raise ArgumentError, ~r/expected password/, fn ->
      opts_from_uri("redis://not_a_user_or_password@localhost")
    end
  end

  test "opts_from_uri/1: database" do
    opts = opts_from_uri("redis://localhost/2")
    assert opts[:host] == "localhost"
    assert opts[:database] == 2

    opts = opts_from_uri("redis://localhost/")
    assert opts[:host] == "localhost"
    assert is_nil(opts[:database])

    opts = opts_from_uri("redis://localhost/2/namespace")
    assert opts[:host] == "localhost"
    assert opts[:database] == 2

    message = "expected database to be an integer, got: \"/peanuts\""

    assert_raise ArgumentError, message, fn ->
      opts_from_uri("redis://localhost/peanuts")
    end

    message = "expected database to be an integer, got: \"/0tacos\""

    assert_raise ArgumentError, message, fn ->
      opts_from_uri("redis://localhost/0tacos")
    end
  end

  test "opts_from_uri/1: accepts rediss scheme" do
    opts = opts_from_uri("rediss://example.com")
    assert opts[:host] == "example.com"
    assert opts[:ssl] == true
    assert is_nil(opts[:port])
    assert is_nil(opts[:database])
    assert is_nil(opts[:password])
  end
end
