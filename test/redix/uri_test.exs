defmodule Redix.URITest do
  use ExUnit.Case, async: true

  import Redix.URI
  alias Redix.URI.URIError

  test "opts_from_uri/1: invalid scheme" do
    msg = "scheme is not redis:// but 'foo://'"
    assert_raise URIError, msg, fn ->
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

    # We don't care about the user.
    opts = opts_from_uri("redis://user:pass@localhost")
    assert opts[:host] == "localhost"
    assert opts[:password] == "pass"
  end

  test "opts_from_uri/1: database" do
    opts = opts_from_uri("redis://localhost/2")
    assert opts[:host] == "localhost"
    assert opts[:database] == 2

    opts = opts_from_uri("redis://localhost/")
    assert opts[:host] == "localhost"
    assert is_nil(opts[:database])
  end
end
