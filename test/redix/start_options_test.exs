defmodule Redix.StartOptionsTest do
  use ExUnit.Case, async: true

  alias Redix.StartOptions

  describe "sanitize/1" do
    test "fills in defaults" do
      opts = sanitize(host: "foo.com", backoff_max: 0, sync_connect: true)

      assert opts[:host] == ~c"foo.com"
      assert opts[:backoff_max] == 0
      assert opts[:sync_connect] == true
    end

    test "raises on unknown options" do
      assert_raise NimbleOptions.ValidationError, ~r/unknown options \[:foo\]/, fn ->
        sanitize(foo: "bar")
      end
    end

    test "raises if the port is not an integer" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :port option/, fn ->
        sanitize(port: :not_an_integer)
      end
    end

    test "host and port are filled in based on Unix sockets" do
      opts = sanitize([])
      assert opts[:host] == ~c"localhost"
      assert opts[:port] == 6379

      opts = sanitize(host: {:local, "some_path"})
      assert opts[:port] == 0

      opts = sanitize(host: {:local, "some_path"}, port: 0)
      assert opts[:port] == 0

      assert_raise ArgumentError, ~r/when using Unix domain sockets, the port must be 0/, fn ->
        sanitize(host: {:local, "some_path"}, port: 1)
      end
    end

    test "sentinel options" do
      opts =
        sanitize(sentinel: [sentinels: ["redis://localhost:26379"], group: "foo"])

      assert [sentinel] = opts[:sentinel][:sentinels]
      assert sentinel[:host] == ~c"localhost"
      assert sentinel[:port] == 26379

      assert opts[:sentinel][:group] == "foo"
    end

    test "sentinel addresses are validated" do
      message = ~r/sentinel address should be specified/

      assert_raise ArgumentError, message, fn ->
        sanitize(sentinel: [sentinels: [:not_a_sentinel], group: "foo"])
      end
    end

    test "sentinel options should have a :sentinels option" do
      assert_raise NimbleOptions.ValidationError, ~r/required :sentinels option not found/, fn ->
        sanitize(sentinel: [])
      end
    end

    test "sentinel options should have a :group option" do
      assert_raise NimbleOptions.ValidationError, ~r/required :group option not found/, fn ->
        sanitize(sentinel: [sentinels: ["redis://localhos:6379"]])
      end
    end

    test "sentinel options should have a non-empty list in :sentinels" do
      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :sentinels option/, fn ->
        sanitize(sentinel: [sentinels: :not_a_list])
      end

      assert_raise NimbleOptions.ValidationError, ~r/invalid value for :sentinels option/, fn ->
        sanitize(sentinel: [sentinels: []])
      end
    end

    test "every sentinel address must have a host and a port" do
      assert_raise ArgumentError, "a host should be specified for each sentinel", fn ->
        sanitize(sentinel: [sentinels: ["redis://:6379"]])
      end

      assert_raise ArgumentError, "a port should be specified for each sentinel", fn ->
        sanitize(sentinel: [sentinels: ["redis://localhost"]])
      end
    end

    test "if sentinel options are passed, :host and :port cannot be passed" do
      message = ":host or :port can't be passed as option if :sentinel is used"

      assert_raise ArgumentError, message, fn ->
        sanitize(
          sentinel: [sentinels: ["redis://localhost:6379"], group: "foo"],
          host: "localhost"
        )
      end
    end

    test "sentinel password string" do
      opts =
        sanitize(
          sentinel: [
            sentinels: [
              [host: "host1", port: 26379, password: "secret1"],
              [host: "host2", port: 26379]
            ],
            group: "mygroup",
            password: "secret2"
          ]
        )

      sentinels = opts[:sentinel][:sentinels]

      assert Enum.count(sentinels) == 2
      assert Enum.find(sentinels, &(&1[:host] == ~c"host1"))[:password] == "secret1"
      assert Enum.find(sentinels, &(&1[:host] == ~c"host2"))[:password] == "secret2"
    end

    test "sentinel password mfa" do
      mfa1 = {System, :fetch_env!, ["REDIS_PASS1"]}
      mfa2 = {System, :fetch_env!, ["REDIS_PASS2"]}

      opts =
        sanitize(
          sentinel: [
            sentinels: [
              [host: "host1", port: 26379, password: mfa1],
              [host: "host2", port: 26379]
            ],
            group: "mygroup",
            password: mfa2
          ]
        )

      sentinels = opts[:sentinel][:sentinels]

      assert Enum.count(sentinels) == 2
      assert Enum.find(sentinels, &(&1[:host] == ~c"host1"))[:password] == mfa1
      assert Enum.find(sentinels, &(&1[:host] == ~c"host2"))[:password] == mfa2
    end

    test "gen_statem options are allowed" do
      opts =
        sanitize(hibernate_after: 1000, debug: [], spawn_opt: [fullsweep_after: 0])

      assert opts[:hibernate_after] == 1000
      assert opts[:debug] == []
      assert opts[:spawn_opt] == [fullsweep_after: 0]
    end
  end

  defp sanitize(opts) do
    StartOptions.sanitize(:redix_pubsub, opts)
  end
end
