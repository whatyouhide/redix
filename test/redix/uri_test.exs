defmodule Redix.URITest do
  use ExUnit.Case, async: true

  doctest Redix.URI

  import Redix.URI

  describe "to_start_options/1" do
    test "invalid scheme" do
      message = "expected scheme to be redis:// or rediss://, got: foo://"

      assert_raise ArgumentError, message, fn ->
        to_start_options("foo://example.com")
      end
    end

    test "just the host" do
      opts = to_start_options("redis://example.com")
      assert opts[:host] == "example.com"
      assert is_nil(opts[:port])
      assert is_nil(opts[:database])
      assert is_nil(opts[:password])
    end

    test "host and port" do
      opts = to_start_options("redis://localhost:6379")
      assert opts[:host] == "localhost"
      assert opts[:port] == 6379
      assert is_nil(opts[:database])
      assert is_nil(opts[:password])
    end

    test "username and password" do
      opts = to_start_options("redis://user:pass@localhost")
      assert opts[:host] == "localhost"
      assert opts[:username] == "user"
      assert opts[:password] == "pass"
    end

    test "password" do
      opts = to_start_options("redis://:pass@localhost")
      assert opts[:host] == "localhost"
      assert opts[:username] == nil
      assert opts[:password] == "pass"

      # If there's no ":", we error out.
      assert_raise ArgumentError, ~r/expected password/, fn ->
        to_start_options("redis://not_a_user_or_password@localhost")
      end
    end

    test "database" do
      opts = to_start_options("redis://localhost/2")
      assert opts[:host] == "localhost"
      assert opts[:database] == 2

      opts = to_start_options("redis://localhost/")
      assert opts[:host] == "localhost"
      assert is_nil(opts[:database])

      # test without a trailing slash
      opts = to_start_options("redis://localhost")
      assert opts[:host] == "localhost"
      assert is_nil(opts[:database])

      opts = to_start_options("redis://localhost/2/namespace")
      assert opts[:host] == "localhost"
      assert opts[:database] == 2

      message = "expected database to be an integer, got: \"/peanuts\""

      assert_raise ArgumentError, message, fn ->
        to_start_options("redis://localhost/peanuts")
      end

      message = "expected database to be an integer, got: \"/0tacos\""

      assert_raise ArgumentError, message, fn ->
        to_start_options("redis://localhost/0tacos")
      end
    end

    test "accepts rediss scheme" do
      opts = to_start_options("rediss://example.com")
      assert opts[:host] == "example.com"
      assert opts[:ssl] == true
      assert is_nil(opts[:port])
      assert is_nil(opts[:database])
      assert is_nil(opts[:password])
    end
  end
end
