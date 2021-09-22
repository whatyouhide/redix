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

    test "raises if the port is not an integer" do
      message = "expected an integer as the value of the :port option, got: :not_an_integer"

      assert_raise ArgumentError, message, fn ->
        StartOptions.sanitize(port: :not_an_integer)
      end
    end

    test "host and port are filled in based on Unix sockets" do
      opts = StartOptions.sanitize([])
      assert opts[:host] == 'localhost'
      assert opts[:port] == 6379

      opts = StartOptions.sanitize(host: {:local, "some_path"})
      assert opts[:port] == 0

      opts = StartOptions.sanitize(host: {:local, "some_path"}, port: 0)
      assert opts[:port] == 0

      assert_raise ArgumentError, ~r/when using Unix domain sockets, the port must be 0/, fn ->
        StartOptions.sanitize(host: {:local, "some_path"}, port: 1)
      end
    end

    test "sentinel options" do
      opts =
        StartOptions.sanitize(sentinel: [sentinels: ["redis://localhost:26379"], group: "foo"])

      assert [sentinel] = opts[:sentinel][:sentinels]
      assert sentinel[:host] == 'localhost'
      assert sentinel[:port] == 26379

      assert opts[:sentinel][:group] == "foo"
    end

    test "sentinel addresses are validated" do
      assert_raise ArgumentError, ~r/sentinel address should be specified/, fn ->
        StartOptions.sanitize(sentinel: [sentinels: [:not_a_sentinel], group: "foo"])
      end
    end

    test "sentinel options should have a :sentinels option" do
      assert_raise ArgumentError, "the :sentinels option is required inside :sentinel", fn ->
        StartOptions.sanitize(sentinel: [])
      end
    end

    test "sentinel options should have a :group option" do
      assert_raise ArgumentError, "the :group option is required inside :sentinel", fn ->
        StartOptions.sanitize(sentinel: [sentinels: ["redis://localhos:6379"]])
      end
    end

    test "sentinel options should have a non-empty list in :sentinels" do
      message = ~r/the :sentinels option inside :sentinel must be a non-empty list/

      assert_raise ArgumentError, message, fn ->
        StartOptions.sanitize(sentinel: [sentinels: :not_a_list])
      end

      assert_raise ArgumentError, message, fn ->
        StartOptions.sanitize(sentinel: [sentinels: []])
      end
    end

    test "every sentinel address must have a host and a port" do
      assert_raise ArgumentError, "a host should be specified for each sentinel", fn ->
        StartOptions.sanitize(sentinel: [sentinels: ["redis://:6379"]])
      end

      assert_raise ArgumentError, "a port should be specified for each sentinel", fn ->
        StartOptions.sanitize(sentinel: [sentinels: ["redis://localhost"]])
      end
    end

    test "if sentinel options are passed, :host and :port cannot be passed" do
      message = ":host or :port can't be passed as option if :sentinel is used"

      assert_raise ArgumentError, message, fn ->
        StartOptions.sanitize(
          sentinel: [sentinels: ["redis://localhost:6379"], group: "foo"],
          host: "localhost"
        )
      end
    end

    test "gen_statem options are allowed" do
      opts =
        StartOptions.sanitize(hibernate_after: 1000, debug: [], spawn_opt: [fullsweep_after: 0])

      assert opts[:hibernate_after] == 1000
      assert opts[:debug] == []
      assert opts[:spawn_opt] == [fullsweep_after: 0]
    end
  end
end
