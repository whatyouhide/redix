defmodule RedixTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Redix.Error
  alias Redix.ConnectionError
  alias Redix.TestHelpers

  @host TestHelpers.test_host()
  @port TestHelpers.test_port()

  setup_all do
    {:ok, conn} = Redix.start_link(host: @host, port: @port)
    Redix.command!(conn, ["FLUSHDB"])
    Redix.stop(conn)
    :ok
  end

  setup context do
    if context[:no_setup] do
      {:ok, %{}}
    else
      {:ok, conn} = Redix.start_link(host: @host, port: @port)
      {:ok, %{conn: conn}}
    end
  end

  @tag :no_setup
  test "start_link/2: specifying a database" do
    {:ok, c} = Redix.start_link(host: @host, port: @port, database: 1)
    assert Redix.command(c, ~w(SET my_key my_value)) == {:ok, "OK"}

    # Let's check we didn't write to the default database (which is 0).
    {:ok, c} = Redix.start_link(host: @host, port: @port)
    assert Redix.command(c, ~w(GET my_key)) == {:ok, nil}
  end

  @tag :no_setup
  test "start_link/2: specifying a non existing database" do
    capture_log(fn ->
      Process.flag(:trap_exit, true)
      {:ok, pid} = Redix.start_link(host: @host, port: @port, database: 1000)

      assert_receive {:EXIT, ^pid, %Error{message: message}}, 500
      assert message in ["ERR invalid DB index", "ERR DB index is out of range"]
    end)
  end

  @tag :no_setup
  test "start_link/2: specifying a password when no password is set" do
    capture_log(fn ->
      Process.flag(:trap_exit, true)
      {:ok, pid} = Redix.start_link(host: @host, port: @port, password: "foo")

      error = %Error{message: "ERR Client sent AUTH, but no password is set"}
      assert_receive {:EXIT, ^pid, ^error}, 500
    end)
  end

  @tag :no_setup
  test "start_link/2: when unable to connect to Redis with sync_connect: true" do
    capture_log(fn ->
      Process.flag(:trap_exit, true)
      error = %Redix.ConnectionError{reason: :nxdomain}
      assert Redix.start_link([host: "nonexistent"], sync_connect: true) == {:error, error}
      assert_receive {:EXIT, _pid, ^error}, 1000
    end)
  end

  @tag :no_setup
  test "start_link/2: when unable to connect to Redis with sync_connect: false" do
    capture_log(fn ->
      Process.flag(:trap_exit, true)
      {:ok, pid} = Redix.start_link([host: "nonexistent"], sync_connect: false)
      refute_receive {:EXIT, ^pid, :nxdomain}, 200
    end)
  end

  @tag :no_setup
  test "start_link/2: using a redis:// url" do
    {:ok, pid} = Redix.start_link("redis://#{@host}:#{@port}/3")
    assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
  end

  @tag :no_setup
  test "start_link/2: name registration" do
    {:ok, pid} = Redix.start_link([host: @host, port: @port], name: :redix_server)
    assert Process.whereis(:redix_server) == pid
    assert Redix.command(:redix_server, ["PING"]) == {:ok, "PONG"}
  end

  @tag :no_setup
  test "start_link/2: passing options along with a Redis URI" do
    {:ok, pid} = Redix.start_link("redis://#{@host}:#{@port}", name: :redix_uri)
    assert Process.whereis(:redix_uri) == pid
  end

  @tag :no_setup
  test "stop/1" do
    {:ok, pid} = Redix.start_link("redis://#{@host}:#{@port}/3")
    ref = Process.monitor(pid)
    assert Redix.stop(pid) == :ok

    assert_receive {:DOWN, ^ref, _, _, :normal}, 500
  end

  @tag :no_setup
  test "the :log option given to start_link/2 must be a list" do
    assert_raise ArgumentError, ~r/the :log option must be a keyword list/, fn ->
      Redix.start_link([host: @host, port: @port], log: :not_a_list)
    end
  end

  test "command/2", %{conn: c} do
    assert Redix.command(c, ["PING"]) == {:ok, "PONG"}
  end

  test "command/2: transactions - MULTI/EXEC", %{conn: c} do
    assert Redix.command(c, ["MULTI"]) == {:ok, "OK"}
    assert Redix.command(c, ["INCR", "multifoo"]) == {:ok, "QUEUED"}
    assert Redix.command(c, ["INCR", "multibar"]) == {:ok, "QUEUED"}
    assert Redix.command(c, ["INCRBY", "multifoo", 4]) == {:ok, "QUEUED"}
    assert Redix.command(c, ["EXEC"]) == {:ok, [1, 1, 5]}
  end

  test "command/2: transactions - MULTI/DISCARD", %{conn: c} do
    Redix.command!(c, ["SET", "discarding", "foo"])

    assert Redix.command(c, ["MULTI"]) == {:ok, "OK"}
    assert Redix.command(c, ["SET", "discarding", "bar"]) == {:ok, "QUEUED"}
    # Discarding
    assert Redix.command(c, ["DISCARD"]) == {:ok, "OK"}
    assert Redix.command(c, ["GET", "discarding"]) == {:ok, "foo"}
  end

  test "command/2: Lua scripting - EVAL", %{conn: c} do
    script = """
    redis.call("SET", "evalling", "yes")
    return {KEYS[1],ARGV[1],ARGV[2]}
    """

    cmds = ["eval", script, "1", "key", "first", "second"]

    assert Redix.command(c, cmds) == {:ok, ["key", "first", "second"]}
    assert Redix.command(c, ["GET", "evalling"]) == {:ok, "yes"}
  end

  test "command/2 - Lua scripting: SCRIPT LOAD, SCRIPT EXISTS, EVALSHA", %{conn: c} do
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

    assert_raise Redix.Error, "ERR value is not an integer or out of range", fn ->
      Redix.command(c, ~w(INCR errs))
    end
  end

  test "command/2: passing an empty list returns an error", %{conn: c} do
    message = "got an empty command ([]), which is not a valid Redis command"
    assert_raise ArgumentError, message, fn -> Redix.command(c, []) end
  end

  test "command/2: timeout", %{conn: c} do
    assert {:error, %ConnectionError{reason: :timeout}} = Redix.command(c, ~W(PING), timeout: 0)
  end

  test "command/2: passing a non-list as the command", %{conn: c} do
    message = "expected a list of binaries as each Redis command, got: \"PING\""

    assert_raise ArgumentError, message, fn ->
      Redix.command(c, "PING")
    end
  end

  test "command/2: no_wait_for_reply", %{conn: c} do
    assert Redix.command(c, ~w(SET nowait_test_key foo), no_wait_for_reply: true) == :ok
    assert Redix.command(c, ~w(GET nowait_test_key)) == {:ok, "foo"}
  end

  test "pipeline/2", %{conn: c} do
    commands = [
      ["SET", "pipe", "10"],
      ["INCR", "pipe"],
      ["GET", "pipe"]
    ]

    assert Redix.pipeline(c, commands) == {:ok, ["OK", 11, "11"]}
  end

  test "pipeline/2: a lot of commands so that TCP gets stressed", %{conn: c} do
    assert {:ok, "OK"} = Redix.command(c, ~w(SET stress_pipeline foo))

    ncommands = 10000
    commands = List.duplicate(~w(GET stress_pipeline), ncommands)

    # Let's do it twice to be sure the server can handle the data.
    {:ok, results} = Redix.pipeline(c, commands)
    assert length(results) == ncommands
    {:ok, results} = Redix.pipeline(c, commands)
    assert length(results) == ncommands
  end

  test "pipeline/2: a single command should still return a list of results", %{conn: c} do
    assert Redix.pipeline(c, [["PING"]]) == {:ok, ["PONG"]}
  end

  test "pipeline/2: Redis errors in the response", %{conn: c} do
    msg = "ERR value is not an integer or out of range"
    assert {:ok, resp} = Redix.pipeline(c, [~w(SET pipeline_errs foo), ~w(INCR pipeline_errs)])
    assert resp == ["OK", %Error{message: msg}]
  end

  test "pipeline/2: passing an empty list of commands raises an error", %{conn: c} do
    msg = "no commands passed to the pipeline"
    assert_raise ArgumentError, msg, fn -> Redix.pipeline(c, []) end
  end

  test "pipeline/2: passing one or more empty commands returns an error", %{conn: c} do
    message = "got an empty command ([]), which is not a valid Redis command"

    assert_raise ArgumentError, message, fn ->
      Redix.pipeline(c, [[]])
    end

    assert_raise ArgumentError, message, fn ->
      Redix.pipeline(c, [["PING"], [], ["PING"]])
    end
  end

  test "pipeline/2: passing a PubSub command causes an error", %{conn: c} do
    assert_raise ArgumentError, ~r{Redix doesn't support Pub/Sub}, fn ->
      Redix.pipeline(c, [["PING"], ["SUBSCRIBE", "foo"]])
    end
  end

  test "pipeline/2: timeout", %{conn: c} do
    assert {:error, %ConnectionError{reason: :timeout}} =
             Redix.pipeline(c, [~w(PING), ~w(PING)], timeout: 0)
  end

  test "pipeline/2: commands must be lists of binaries", %{conn: c} do
    message = "expected a list of Redis commands, got: \"PING\""

    assert_raise ArgumentError, message, fn ->
      Redix.pipeline(c, "PING")
    end

    message = "expected a list of binaries as each Redis command, got: \"PING\""

    assert_raise ArgumentError, message, fn ->
      Redix.pipeline(c, ["PING"])
    end
  end

  test "pipeline/2: no_wait_for_reply with multiple subcommands", %{conn: c} do
    assert Redix.pipeline(
             c,
             [~w(PING), ~w(SET nowait_test_key_pipeline foo), ~w(PING)],
             no_wait_for_reply: true
           ) == :ok

    assert Redix.pipeline(c, [~w(GET nowait_test_key_pipeline)]) == {:ok, ["foo"]}
  end

  test "pipeline/2: no_wait_for_reply with redis errors", %{conn: c} do
    # The INCR command will fail; expect it to do so silently and without messing up the queue
    assert Redix.pipeline(
             c,
             [~w(SET pipeline_errs_nowait foo), ~w(INCR pipeline_errs_nowait)],
             no_wait_for_reply: true
           ) == :ok

    assert Redix.pipeline(c, [~w(GET pipeline_errs_nowait)]) == {:ok, ["foo"]}
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

  test "command!/2: connection errors", %{conn: c} do
    assert_raise Redix.ConnectionError, ":timeout", fn ->
      Redix.command!(c, ["PING"], timeout: 0)
    end
  end

  test "command!/2: no_wait_for_reply", %{conn: c} do
    assert Redix.command!(c, ~w(SET nowait_test_key_bang foo), no_wait_for_reply: true) == nil
    assert Redix.command!(c, ~w(GET nowait_test_key_bang)) == "foo"
  end

  test "pipeline!/2: simple commands", %{conn: c} do
    assert Redix.pipeline!(c, [~w(SET ppbang foo), ~w(GET ppbang)]) == ~w(OK foo)
  end

  test "pipeline!/2: Redis errors in the list of results", %{conn: c} do
    commands = [~w(SET ppbang_errors foo), ~w(INCR ppbang_errors)]

    msg = "ERR value is not an integer or out of range"
    assert Redix.pipeline!(c, commands) == ["OK", %Redix.Error{message: msg}]
  end

  test "pipeline!/2: connection errors", %{conn: c} do
    assert_raise Redix.ConnectionError, ":timeout", fn ->
      Redix.pipeline!(c, [["PING"]], timeout: 0)
    end
  end

  test "pipeline!/2: no_wait_for_reply with multiple subcommands", %{conn: c} do
    assert Redix.pipeline!(
             c,
             [~w(PING), ~w(SET nowait_test_key_bang foo), ~w(PING)],
             no_wait_for_reply: true
           ) == nil

    assert Redix.pipeline!(c, [~w(GET nowait_test_key_bang)]) == ["foo"]
  end

  @tag :no_setup
  test "client suicide and reconnections" do
    {:ok, c} = Redix.start_link(host: @host, port: @port)

    capture_log(fn ->
      assert {:ok, _} = Redix.command(c, ~w(QUIT))

      # When the socket is closed, we reply with {:error, closed}. We sleep so
      # we're sure that the socket is closed (and we don't get {:error,
      # disconnected} before the socket closed after we sent the PING command
      # to Redix).
      :timer.sleep(100)
      assert Redix.command(c, ~w(PING)) == {:error, %ConnectionError{reason: :closed}}

      # Redix retries the first reconnection after 500ms, and we waited 100 already.
      :timer.sleep(500)
      assert {:ok, "PONG"} = Redix.command(c, ~w(PING))
    end)
  end

  @tag :no_setup
  test "timeouts" do
    {:ok, c} = Redix.start_link(host: @host, port: @port)

    assert {:error, %ConnectionError{reason: :timeout}} = Redix.command(c, ~w(PING), timeout: 0)

    # Let's check that the Redix connection doesn't reply anyways, even if the
    # timeout happened.
    refute_receive {_ref, _message}
  end

  @tag :no_setup
  test "mid-command disconnections" do
    {:ok, c} = Redix.start_link(host: @host, port: @port)

    capture_log(fn ->
      {_pid, ref} =
        Process.spawn(
          fn ->
            # BLPOP with a timeout of 0 blocks indefinitely
            assert Redix.command(c, ~w(BLPOP mid_command_disconnection 0)) ==
                     {:error, %ConnectionError{reason: :disconnected}}
          end,
          [:monitor, :link]
        )

      Redix.command!(c, ~w(QUIT))
      assert_receive {:DOWN, ^ref, _, _, _}, 200
    end)
  end

  @tag :no_setup
  test "timing out right after the connection drops" do
    {:ok, c} = Redix.start_link(host: @host, port: @port)

    capture_log(fn ->
      Redix.command!(c, ~w(QUIT))
      error = %ConnectionError{reason: :timeout}
      assert Redix.command(c, ~w(PING), timeout: 0) == {:error, error}
      refute_receive {_ref, _message}
    end)
  end

  @tag :no_setup
  test "no leaking messages when timeout happen at the same time as disconnections" do
    {:ok, c} = Redix.start_link(host: @host, port: @port)

    capture_log(fn ->
      {_pid, ref} =
        Process.spawn(
          fn ->
            error = %ConnectionError{reason: :timeout}
            assert Redix.command(c, ~w(BLPOP my_list 0), timeout: 0) == {:error, error}

            # The fact that we timed out should be respected here, even if the
            # connection is killed (no {:error, :disconnected} message should
            # arrive).
            refute_receive {_ref, _message}
          end,
          [:link, :monitor]
        )

      Redix.command!(c, ~w(QUIT))
      assert_receive {:DOWN, ^ref, _, _, _}, 200
    end)
  end

  @tag :no_setup
  test ":exit_on_disconnection option" do
    {:ok, c} = Redix.start_link([host: @host, port: @port], exit_on_disconnection: true)
    Process.flag(:trap_exit, true)

    capture_log(fn ->
      Redix.command!(c, ~w(QUIT))
      assert_receive {:EXIT, ^c, %ConnectionError{reason: :tcp_closed}}
    end)
  end

  @tag :no_setup
  test "child_spec/1" do
    default_spec = %{
      id: Redix,
      start: {Redix, :start_link, [[], []]},
      type: :worker
    }

    assert Redix.child_spec([]) == default_spec
    assert Redix.child_spec([[]]) == default_spec
    assert Redix.child_spec([[], []]) == default_spec

    assert Redix.child_spec(["redis://localhost"]) ==
             Map.put(default_spec, :start, {Redix, :start_link, ["redis://localhost", []]})

    assert Redix.child_spec(["redis://localhost", []]) ==
             Map.put(default_spec, :start, {Redix, :start_link, ["redis://localhost", []]})

    assert Redix.child_spec(["redis://localhost", [name: :redix]]) ==
             Map.put(default_spec, :start, {
               Redix,
               :start_link,
               ["redis://localhost", [name: :redix]]
             })
  end
end
