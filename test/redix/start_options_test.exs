defmodule Redix.StartOptionsTest do
  use ExUnit.Case, async: true

  alias Redix.StartOptions

  describe "sanitize/1" do
    test "fills in defaults" do
      opts = StartOptions.sanitize(host: "foo.com", backoff_max: 0, sync_connect: true)

      assert opts[:host] == 'foo.com'
      assert opts[:backoff_max] == 0
      assert opts[:sync_connect] == true
    end

    test "raises on unknown options" do
      assert_raise ArgumentError, "unknown option: :foo", fn ->
        StartOptions.sanitize(foo: "bar")
      end
    end

    test "host and port are filled in based on Unix sockets" do
      opts = StartOptions.sanitize([])
      assert opts[:host] == 'localhost'
      assert opts[:port] == 6379

      opts = StartOptions.sanitize(host: {:local, "some_path"})
      assert opts[:port] == 0

      assert_raise ArgumentError, ~r/when using Unix domain sockets, the port must be 0/, fn ->
        StartOptions.sanitize(host: {:local, "some_path"}, port: 1)
      end
    end

    test "sentinel options" do
      opts = StartOptions.sanitize(sentinel: [sentinels: [{"foo", 1}], group: "foo"])
      assert opts[:sentinel][:sentinels] == [{'foo', 1}]
      assert opts[:sentinel][:group] == "foo"
    end

    test "sentinel addresses are validated" do
      assert_raise ArgumentError, ~r/sentinel addresses must be in the form {host, port}/, fn ->
        StartOptions.sanitize(sentinel: [sentinels: [:not_a_sentinel], group: "foo"])
      end
    end
  end
end
