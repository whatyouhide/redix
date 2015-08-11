defmodule RecsTest do
  use ExUnit.Case, async: true
  import Recs.TestHelpers

  setup_all do
    {:ok, conn} = Recs.start_link
    Recs.command(conn, ["FLUSHDB"])
    {:ok, %{}}
  end

  setup context do
    if context[:no_setup] do
      {:ok, %{}}
    else
      {:ok, conn} = Recs.start_link
      {:ok, %{conn: conn}}
    end
  end

  @tag :no_setup
  test "start_link/1: returns a pid" do
    assert {:ok, pid} = Recs.start_link
    assert is_pid(pid)
  end

  @tag :no_setup
  test "start_link/1: specifying a database" do
    assert {:ok, pid} = Recs.start_link database: 1
    assert Recs.command(pid, ["PING"]) == "PONG"
  end

  @tag :no_setup
  test "start_link/1: specifying a password" do
    capture_log fn ->
      Process.flag :trap_exit, true
      assert {:ok, pid} = Recs.start_link password: "foo"
      assert is_pid(pid)

      assert_receive {:EXIT, ^pid, "ERR Client sent AUTH, but no password is set"}
    end
  end

  @tag :no_setup
  test "start_link/1: when unable to connect to Redis" do
    capture_log fn ->
      Process.flag :trap_exit, true
      assert {:ok, pid} = Recs.start_link host: "nonexistent"
      assert_receive {:EXIT, ^pid, :nxdomain}
    end
  end

  @tag :no_setup
  test "start_link/1: using a redis:// url" do
    assert {:ok, pid} = Recs.start_link "redis://localhost:6379/3"
    assert Recs.command(pid, ["PING"]) == "PONG"
  end

  test "command/2", %{conn: c} do
    assert Recs.command(c, ["PING"]) == "PONG"
  end

  test "pipeline/2", %{conn: c} do
    commands = [
      ["SET", "pipe", "10"],
      ["INCR", "pipe"],
      ["GET", "pipe"],
    ]
    assert Recs.pipeline(c, commands) == ["OK", 11, "11"]
  end

  test "some commands: APPEND", %{conn: c} do
    assert Recs.command(c, ~w(APPEND to_append hello)) == 5
    assert Recs.command(c, ~w(APPEND to_append world)) == 10
  end

  test "some commands: DBSIZE", %{conn: c} do
    i = Recs.command(c, ["DBSIZE"])
    assert is_integer(i)
  end

  test "some commands: INCR and DECR", %{conn: c} do
    assert Recs.command(c, ["INCR", "to_incr"]) == 1
    assert Recs.command(c, ["DECR", "to_incr"]) == 0
  end

  test "some commands: transactions with MULTI/EXEC (executing)", %{conn: c} do
    assert Recs.command(c, ["MULTI"]) == "OK"

    assert Recs.command(c, ["INCR", "multifoo"]) == "QUEUED"
    assert Recs.command(c, ["INCR", "multibar"]) == "QUEUED"
    assert Recs.command(c, ["INCRBY", "multifoo", 4]) == "QUEUED"

    assert Recs.command(c, ["EXEC"]) == [1, 1, 5]
  end

  test "some commands: transactions with MULTI/DISCARD", %{conn: c} do
    "OK" = Recs.command(c, ["SET", "discarding", "foo"])

    assert Recs.command(c, ["MULTI"]) == "OK"
    assert Recs.command(c, ["SET", "discarding", "bar"]) == "QUEUED"

    # Discarding
    assert Recs.command(c, ["DISCARD"]) == "OK"
    assert Recs.command(c, ["GET", "discarding"]) == "foo"
  end

  test "some commands: TYPE", %{conn: c} do
    assert Recs.command(c, ["SET", "string_type", "foo bar"]) == "OK"
    assert Recs.command(c, ["TYPE", "string_type"]) == "string"
  end

  test "some commands: STRLEN", %{conn: c} do
    assert Recs.command(c, ["SET", "string_length", "foo bar"]) == "OK"
    assert Recs.command(c, ["STRLEN", "string_length"]) == 7
  end

  test "some commands: LPUSH, LLEN, LPOP, LINDEX", %{conn: c} do
    assert Recs.command(c, ~w(LPUSH mylist world)) == 1
    assert Recs.command(c, ~w(LPUSH mylist hello)) == 2
    assert Recs.command(c, ~w(LLEN mylist)) == 2
    assert Recs.command(c, ~w(LINDEX mylist 0)) == "hello"
    assert Recs.command(c, ~w(LPOP mylist)) == "hello"
  end

  test "Lua scripting: EVAL", %{conn: c} do
    script = """
    redis.call("SET", "evalling", "yes")
    return {KEYS[1],ARGV[1],ARGV[2]}
    """

    cmds = ["eval", script, "1", "key", "first", "second"]

    assert Recs.command(c, cmds) == ["key", "first", "second"]
    assert Recs.command(c, ["GET", "evalling"]) == "yes"
  end

  test "Lua scripting: SCRIPT LOAD, SCRIPT EXISTS, EVALSHA", %{conn: c} do
    script = """
    return 'hello world'
    """

    sha = Recs.command(c, ["SCRIPT", "LOAD", script])
    assert is_binary(sha)
    assert Recs.command(c, ["SCRIPT", "EXISTS", sha, "foo"]) == [1, 0]

    # Eval'ing the script
    assert Recs.command(c, ["EVALSHA", sha, 0]) == "hello world"
  end
end
