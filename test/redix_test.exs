defmodule RedixTest do
  use ExUnit.Case, async: true
  import Redix.TestHelpers
  alias Redix.Error
  alias Redix.ConnectionError

  setup_all do
    {:ok, conn} = Redix.start_link
    Redix.command(conn, ["FLUSHDB"])
    {:ok, %{}}
  end

  setup context do
    if context[:no_setup] do
      {:ok, %{}}
    else
      {:ok, conn} = Redix.start_link
      {:ok, %{conn: conn}}
    end
  end

  @tag :no_setup
  test "start_link/1: returns a pid" do
    assert {:ok, pid} = Redix.start_link
    assert is_pid(pid)
  end

  @tag :no_setup
  test "start_link/1: specifying a database" do
    assert {:ok, pid} = Redix.start_link database: 1
    assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
  end

  @tag :no_setup
  test "start_link/1: specifying a password when no password is set" do
    silence_log fn ->
      Process.flag :trap_exit, true
      assert {:ok, pid} = Redix.start_link password: "foo"
      assert is_pid(pid)

      error = %Error{message: "ERR Client sent AUTH, but no password is set"}
      assert_receive {:EXIT, ^pid, ^error}, 500
    end
  end

  @tag :no_setup
  test "start_link/1: specifying a non existing database" do
    silence_log fn ->
      Process.flag :trap_exit, true
      assert {:ok, pid} = Redix.start_link(database: 1_000)

      error = %Error{message: "ERR invalid DB index"}
      assert_receive {:EXIT, ^pid, ^error}, 500
    end
  end

  @tag :no_setup
  test "start_link/1: when unable to connect to Redis" do
    silence_log fn ->
      Process.flag :trap_exit, true
      assert {:ok, pid} = Redix.start_link host: "nonexistent"
      assert_receive {:EXIT, ^pid, :nxdomain}, 1000
    end
  end

  @tag :no_setup
  test "start_link/1: using a redis:// url" do
    assert {:ok, pid} = Redix.start_link "redis://localhost:6379/3"
    assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
  end

  @tag :no_setup
  test "start_link/2: passing options along with a Redis URI" do
    assert {:ok, _pid} = Redix.start_link("redis://localhost:6379", name: :redix_uri)
    assert (:redix_uri |> Process.whereis |> Process.alive?)
  end

  @tag :no_setup
  test "stop/1" do
    assert {:ok, pid} = Redix.start_link "redis://localhost:6379/3"
    assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
    assert Redix.stop(pid) == :ok

    Process.flag :trap_exit, true
    assert_receive {:EXIT, ^pid, :normal}, 500
  end

  @tag :no_setup
  test "start_link/1: name registration" do
    assert {:ok, pid} = Redix.start_link([], name: :redix_server)
    assert is_pid(pid)
    assert Process.whereis(:redix_server) == pid
    assert Redix.command(:redix_server, ["PING"]) == {:ok, "PONG"}
  end

  test "command/2", %{conn: c} do
    assert Redix.command(c, ["PING"]) == {:ok, "PONG"}
  end

  test "pipeline/2", %{conn: c} do
    commands = [
      ["SET", "pipe", "10"],
      ["INCR", "pipe"],
      ["GET", "pipe"],
    ]
    assert Redix.pipeline(c, commands) == {:ok, ["OK", 11, "11"]}
  end

  test "pipeline/2: a lot of commands so that TCP gets stressed", %{conn: c} do
    assert {:ok, "OK"} = Redix.command(c, ~w(SET stress_pipeline foo))

    ncommands = 10_000

    # Let's do it twice to be sure the server can handle the data.
    {:ok, results} = Redix.pipeline(c, List.duplicate(~w(GET stress_pipeline), ncommands))
    assert length(results) == ncommands
    {:ok, results} = Redix.pipeline(c, List.duplicate(~w(GET stress_pipeline), ncommands))
    assert length(results) == ncommands
  end

  test "pipeline/2: a single command should still return a list of results", %{conn: c} do
    assert Redix.pipeline(c, [["PING"]]) == {:ok, ["PONG"]}
  end

  test "some commands: APPEND", %{conn: c} do
    assert Redix.command(c, ~w(APPEND to_append hello)) == {:ok, 5}
    assert Redix.command(c, ~w(APPEND to_append world)) == {:ok, 10}
  end

  test "some commands: DBSIZE", %{conn: c} do
    {:ok, i} = Redix.command(c, ["DBSIZE"])
    assert is_integer(i)
  end

  test "some commands: INCR and DECR", %{conn: c} do
    assert Redix.command(c, ["INCR", "to_incr"]) == {:ok, 1}
    assert Redix.command(c, ["DECR", "to_incr"]) == {:ok, 0}
  end

  test "some commands: transactions with MULTI/EXEC (executing)", %{conn: c} do
    assert Redix.command(c, ["MULTI"]) == {:ok, "OK"}

    assert Redix.command(c, ["INCR", "multifoo"]) == {:ok, "QUEUED"}
    assert Redix.command(c, ["INCR", "multibar"]) == {:ok, "QUEUED"}
    assert Redix.command(c, ["INCRBY", "multifoo", 4]) == {:ok, "QUEUED"}

    assert Redix.command(c, ["EXEC"]) == {:ok, [1, 1, 5]}
  end

  test "some commands: transactions with MULTI/DISCARD", %{conn: c} do
    {:ok, "OK"} = Redix.command(c, ["SET", "discarding", "foo"])

    assert Redix.command(c, ["MULTI"]) == {:ok, "OK"}
    assert Redix.command(c, ["SET", "discarding", "bar"]) == {:ok, "QUEUED"}

    # Discarding
    assert Redix.command(c, ["DISCARD"]) == {:ok, "OK"}
    assert Redix.command(c, ["GET", "discarding"]) == {:ok, "foo"}
  end

  test "some commands: MULTI/EXEC always returns a list", %{conn: c} do
    assert Redix.command(c, ["MULTI"]) == {:ok, "OK"}
    assert Redix.command(c, ["PING"]) == {:ok, "QUEUED"}
    assert Redix.command(c, ["EXEC"]) == {:ok, ["PONG"]}
  end

  test "some commands: TYPE", %{conn: c} do
    assert Redix.command(c, ["SET", "string_type", "foo bar"]) == {:ok, "OK"}
    assert Redix.command(c, ["TYPE", "string_type"]) == {:ok, "string"}
  end

  test "some commands: STRLEN", %{conn: c} do
    assert Redix.command(c, ["SET", "string_length", "foo bar"]) == {:ok, "OK"}
    assert Redix.command(c, ["STRLEN", "string_length"]) == {:ok, 7}
  end

  test "some commands: LPUSH, LLEN, LPOP, LINDEX", %{conn: c} do
    assert Redix.command(c, ~w(LPUSH mylist world)) == {:ok, 1}
    assert Redix.command(c, ~w(LPUSH mylist hello)) == {:ok, 2}
    assert Redix.command(c, ~w(LLEN mylist)) == {:ok, 2}
    assert Redix.command(c, ~w(LINDEX mylist 0)) == {:ok, "hello"}
    assert Redix.command(c, ~w(LPOP mylist)) == {:ok, "hello"}
  end

  test "Lua scripting: EVAL", %{conn: c} do
    script = """
    redis.call("SET", "evalling", "yes")
    return {KEYS[1],ARGV[1],ARGV[2]}
    """

    cmds = ["eval", script, "1", "key", "first", "second"]

    assert Redix.command(c, cmds) == {:ok, ["key", "first", "second"]}
    assert Redix.command(c, ["GET", "evalling"]) == {:ok, "yes"}
  end

  test "Lua scripting: SCRIPT LOAD, SCRIPT EXISTS, EVALSHA", %{conn: c} do
    script = """
    return 'hello world'
    """

    {:ok, sha} = Redix.command(c, ["SCRIPT", "LOAD", script])
    assert is_binary(sha)
    assert Redix.command(c, ["SCRIPT", "EXISTS", sha, "foo"]) == {:ok, [1, 0]}

    # Eval'ing the script
    assert Redix.command(c, ["EVALSHA", sha, 0]) == {:ok, "hello world"}
  end

  test "command/2: Redis errors", %{conn: c} do
    {:ok, _} = Redix.command(c, ~w(SET errs foo))
    msg = "ERR value is not an integer or out of range"
    assert Redix.command(c, ~w(INCR errs)) == {:error, %Error{message: msg}}
  end

  test "command/2: passing an empty list returns an error", %{conn: c} do
    assert Redix.command(c, []) == {:error, :empty_command}
  end

  test "pipeline/2: Redis errors in the response", %{conn: c} do
    msg = "ERR value is not an integer or out of range"
    assert {:ok, resp} = Redix.pipeline(c, [~w(SET pipeline_errs foo), ~w(INCR pipeline_errs)])
    assert resp == ["OK", %Error{message: msg}]
  end

  test "pipeline/2: passing an empty list of commands raises an error", %{conn: c} do
    msg = "no commands passed to the pipeline"
    assert_raise ConnectionError, msg, fn -> Redix.pipeline(c, []) end
  end

  test "pipeline/2: passing one or more empty commands returns an error", %{conn: c} do
    assert Redix.pipeline(c, [[]]) == {:error, :empty_command}
    assert Redix.pipeline(c, [["PING"], [], ["PING"]]) == {:error, :empty_command}
  end

  test "command!/2: simple commands", %{conn: c} do
    assert Redix.command!(c, ["PING"]) == "PONG"
    assert Redix.command!(c, ["SET", "bang", "foo"]) == "OK"
    assert Redix.command!(c, ["GET", "bang"]) == "foo"
  end

  test "command!/2: Redis errors", %{conn: c} do
    assert_raise Redix.Error, "ERR unknown command 'NONEXISTENT'", fn ->
      Redix.command!(c, ["NONEXISTENT"])
    end

    "OK" = Redix.command!(c, ["SET", "bang_errors", "foo"])
    assert_raise Redix.Error, "ERR value is not an integer or out of range", fn ->
      Redix.command!(c, ["INCR", "bang_errors"])
    end
  end

  test "pipeline!/2: simple commands", %{conn: c} do
    assert Redix.pipeline!(c, [~w(SET ppbang foo), ~w(GET ppbang)]) == ~w(OK foo)
  end

  test "pipeline!/2: Redis errors in the list of results", %{conn: c} do
    commands = [~w(SET ppbang_errors foo), ~w(INCR ppbang_errors)]

    msg = "ERR value is not an integer or out of range"
    assert Redix.pipeline!(c, commands) == ["OK", %Redix.Error{message: msg}]
  end

  test "pipeline!/2: empty commands", %{conn: c} do
    msg = "an empty command ([]) is not a valid Redis command"
    assert_raise ConnectionError, msg, fn ->
      Redix.pipeline!(c, [["PING"], []])
    end
  end

  test "command/2: timeout", %{conn: c} do
    assert {:timeout, _} = catch_exit(Redix.command(c, ~w(PING), timeout: 0))
  end

  test "pipeline/2: timeout", %{conn: c} do
    assert {:timeout, _} = catch_exit(Redix.pipeline(c, [["PING"], ["PING"]], timeout: 0))
  end

  @tag :no_setup
  test "client suicide and reconnections" do
    {:ok, c} = Redix.start_link

    silence_log fn ->
      assert {:ok, _} = Redix.command(c, ~w(CLIENT KILL TYPE normal SKIPME no))
      :timer.sleep(100)
      assert {:ok, "PONG"} = Redix.command(c, ~w(PING))
    end
  end

  @tag :no_setup
  test "exceeding the max number of reconnection attempts" do
    {:ok, c} = Redix.start_link(max_reconnection_attempts: 0)

    silence_log fn ->
      Process.flag :trap_exit, true
      Redix.command(c, ~w(CLIENT KILL TYPE normal SKIPME no))
      assert_receive {:EXIT, ^c, :tcp_closed}
    end
  end
end
