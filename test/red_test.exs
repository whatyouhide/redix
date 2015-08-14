defmodule RedTest do
  use ExUnit.Case, async: true
  import Red.TestHelpers
  alias Red.Error

  setup_all do
    {:ok, conn} = Red.start_link
    Red.command(conn, ["FLUSHDB"])
    {:ok, %{}}
  end

  setup context do
    if context[:no_setup] do
      {:ok, %{}}
    else
      {:ok, conn} = Red.start_link
      {:ok, %{conn: conn}}
    end
  end

  @tag :no_setup
  test "start_link/1: returns a pid" do
    assert {:ok, pid} = Red.start_link
    assert is_pid(pid)
  end

  @tag :no_setup
  test "start_link/1: specifying a database" do
    assert {:ok, pid} = Red.start_link database: 1
    assert Red.command(pid, ["PING"]) == {:ok, "PONG"}
  end

  @tag :no_setup
  test "start_link/1: specifying a password" do
    capture_log fn ->
      Process.flag :trap_exit, true
      assert {:ok, pid} = Red.start_link password: "foo"
      assert is_pid(pid)

      error = %Error{message: "ERR Client sent AUTH, but no password is set"}
      assert_receive {:EXIT, ^pid, ^error}, 500
    end
  end

  @tag :no_setup
  test "start_link/1: when unable to connect to Redis" do
    capture_log fn ->
      Process.flag :trap_exit, true
      assert {:ok, pid} = Red.start_link host: "nonexistent"
      assert_receive {:EXIT, ^pid, :nxdomain}, 500
    end
  end

  @tag :no_setup
  test "start_link/1: using a redis:// url" do
    assert {:ok, pid} = Red.start_link "redis://localhost:6379/3"
    assert Red.command(pid, ["PING"]) == {:ok, "PONG"}
  end

  @tag :no_setup
  test "start_link/2: passing options along with a Redis URI" do
    assert {:ok, _pid} = Red.start_link("redis://localhost:6379", name: :red_uri)
    assert (:red_uri |> Process.whereis |> Process.alive?)
  end

  @tag :no_setup
  test "stop/1" do
    assert {:ok, pid} = Red.start_link "redis://localhost:6379/3"
    assert Red.command(pid, ["PING"]) == {:ok, "PONG"}
    assert Red.stop(pid) == :ok

    Process.flag :trap_exit, true
    assert_receive {:EXIT, ^pid, :normal}, 500
  end

  @tag :no_setup
  test "start_link/1: name registration" do
    assert {:ok, pid} = Red.start_link(name: :red_server)
    assert is_pid(pid)
    assert Process.whereis(:red_server) == pid
  end

  test "command/2", %{conn: c} do
    assert Red.command(c, ["PING"]) == {:ok, "PONG"}
  end

  test "pipeline/2", %{conn: c} do
    commands = [
      ["SET", "pipe", "10"],
      ["INCR", "pipe"],
      ["GET", "pipe"],
    ]
    assert Red.pipeline(c, commands) == {:ok, ["OK", 11, "11"]}
  end

  test "pipeline/2: a lot of commands so that TCP gets stressed", %{conn: c} do
    assert {:ok, "OK"} = Red.command(c, ~w(SET stress_pipeline foo))

    ncommands = 10_000

    # Let's do it twice to be sure the server can handle the data.
    {:ok, results} = Red.pipeline(c, List.duplicate(~w(GET stress_pipeline), ncommands))
    assert length(results) == ncommands
    {:ok, results} = Red.pipeline(c, List.duplicate(~w(GET stress_pipeline), ncommands))
    assert length(results) == ncommands
  end

  test "some commands: APPEND", %{conn: c} do
    assert Red.command(c, ~w(APPEND to_append hello)) == {:ok, 5}
    assert Red.command(c, ~w(APPEND to_append world)) == {:ok, 10}
  end

  test "some commands: DBSIZE", %{conn: c} do
    {:ok, i} = Red.command(c, ["DBSIZE"])
    assert is_integer(i)
  end

  test "some commands: INCR and DECR", %{conn: c} do
    assert Red.command(c, ["INCR", "to_incr"]) == {:ok, 1}
    assert Red.command(c, ["DECR", "to_incr"]) == {:ok, 0}
  end

  test "some commands: transactions with MULTI/EXEC (executing)", %{conn: c} do
    assert Red.command(c, ["MULTI"]) == {:ok, "OK"}

    assert Red.command(c, ["INCR", "multifoo"]) == {:ok, "QUEUED"}
    assert Red.command(c, ["INCR", "multibar"]) == {:ok, "QUEUED"}
    assert Red.command(c, ["INCRBY", "multifoo", 4]) == {:ok, "QUEUED"}

    assert Red.command(c, ["EXEC"]) == {:ok, [1, 1, 5]}
  end

  test "some commands: transactions with MULTI/DISCARD", %{conn: c} do
    {:ok, "OK"} = Red.command(c, ["SET", "discarding", "foo"])

    assert Red.command(c, ["MULTI"]) == {:ok, "OK"}
    assert Red.command(c, ["SET", "discarding", "bar"]) == {:ok, "QUEUED"}

    # Discarding
    assert Red.command(c, ["DISCARD"]) == {:ok, "OK"}
    assert Red.command(c, ["GET", "discarding"]) == {:ok, "foo"}
  end

  test "some commands: TYPE", %{conn: c} do
    assert Red.command(c, ["SET", "string_type", "foo bar"]) == {:ok, "OK"}
    assert Red.command(c, ["TYPE", "string_type"]) == {:ok, "string"}
  end

  test "some commands: STRLEN", %{conn: c} do
    assert Red.command(c, ["SET", "string_length", "foo bar"]) == {:ok, "OK"}
    assert Red.command(c, ["STRLEN", "string_length"]) == {:ok, 7}
  end

  test "some commands: LPUSH, LLEN, LPOP, LINDEX", %{conn: c} do
    assert Red.command(c, ~w(LPUSH mylist world)) == {:ok, 1}
    assert Red.command(c, ~w(LPUSH mylist hello)) == {:ok, 2}
    assert Red.command(c, ~w(LLEN mylist)) == {:ok, 2}
    assert Red.command(c, ~w(LINDEX mylist 0)) == {:ok, "hello"}
    assert Red.command(c, ~w(LPOP mylist)) == {:ok, "hello"}
  end

  test "Lua scripting: EVAL", %{conn: c} do
    script = """
    redis.call("SET", "evalling", "yes")
    return {KEYS[1],ARGV[1],ARGV[2]}
    """

    cmds = ["eval", script, "1", "key", "first", "second"]

    assert Red.command(c, cmds) == {:ok, ["key", "first", "second"]}
    assert Red.command(c, ["GET", "evalling"]) == {:ok, "yes"}
  end

  test "Lua scripting: SCRIPT LOAD, SCRIPT EXISTS, EVALSHA", %{conn: c} do
    script = """
    return 'hello world'
    """

    {:ok, sha} = Red.command(c, ["SCRIPT", "LOAD", script])
    assert is_binary(sha)
    assert Red.command(c, ["SCRIPT", "EXISTS", sha, "foo"]) == {:ok, [1, 0]}

    # Eval'ing the script
    assert Red.command(c, ["EVALSHA", sha, 0]) == {:ok, "hello world"}
  end

  test "command/2: Redis errors", %{conn: c} do
    {:ok, _} = Red.command(c, ~w(SET errs foo))
    msg = "ERR value is not an integer or out of range"
    assert Red.command(c, ~w(INCR errs)) == {:error, %Error{message: msg}}
  end

  test "command/2: timeout", %{conn: c} do
    assert {:timeout, _} = catch_exit(Red.command(c, ~w(PING), timeout: 0))
  end

  test "pipeline/2: timeout", %{conn: c} do
    assert {:timeout, _} = catch_exit(Red.pipeline(c, [["PING"], ["PING"]], timeout: 0))
  end

  @tag :no_setup
  test "client suicide and reconnections" do
    {:ok, c} = Red.start_link(backoff: 80)

    capture_log fn ->
      assert {:ok, _} = Red.command(c, ~w(CLIENT KILL TYPE normal SKIPME no))
      assert {:error, :closed} = Red.command(c, ~w(PING))

      :timer.sleep(100)
      assert {:ok, "PONG"} = Red.command(c, ~w(PING))
    end
  end
end
