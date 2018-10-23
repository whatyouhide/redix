defmodule RedixTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Redix.{
    ConnectionError,
    Error
  }

  setup_all do
    {:ok, conn} = Redix.start_link()
    Redix.command!(conn, ["FLUSHALL"])
    Redix.stop(conn)
    :ok
  end

  describe "start_link/2" do
    test "specifying a database" do
      {:ok, c} = Redix.start_link(database: 1)
      assert Redix.command(c, ~w(SET my_key my_value)) == {:ok, "OK"}

      # Let's check we didn't write to the default database (which is 0).
      {:ok, c} = Redix.start_link()
      assert Redix.command(c, ~w(GET my_key)) == {:ok, nil}
    end

    test "specifying a non existing database" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)
        {:ok, pid} = Redix.start_link(database: 1000)

        assert_receive {:EXIT, ^pid, %Error{message: message}}, 500
        assert message in ["ERR invalid DB index", "ERR DB index is out of range"]
      end)
    end

    test "specifying a password when no password is set" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)
        {:ok, pid} = Redix.start_link(password: "foo")

        error = %Error{message: "ERR Client sent AUTH, but no password is set"}
        assert_receive {:EXIT, ^pid, ^error}, 500
      end)
    end

    test "when unable to connect to Redis with sync_connect: true" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)
        error = %Redix.ConnectionError{reason: :nxdomain}
        assert Redix.start_link(host: "nonexistent", sync_connect: true) == {:error, error}
        assert_receive {:EXIT, _pid, ^error}, 1000
      end)
    end

    test "when unable to connect to Redis with sync_connect: false" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)
        {:ok, pid} = Redix.start_link(host: "nonexistent", sync_connect: false)
        refute_receive {:EXIT, ^pid, :nxdomain}, 200
      end)
    end

    test "using a redis:// url" do
      {:ok, pid} = Redix.start_link("redis://localhost:6379/3")
      assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
    end

    test "name registration" do
      {:ok, pid} = Redix.start_link(name: :redix_server)
      assert Process.whereis(:redix_server) == pid
      assert Redix.command(:redix_server, ["PING"]) == {:ok, "PONG"}
    end

    test "passing options along with a Redis URI" do
      {:ok, pid} = Redix.start_link("redis://localhost", name: :redix_uri)
      assert Process.whereis(:redix_uri) == pid
    end
  end

  test "child_spec/1" do
    default_spec = %{
      id: Redix,
      start: {Redix, :start_link, []},
      type: :worker
    }

    args_path = [:start, Access.elem(2)]

    assert Redix.child_spec("redis://localhost") ==
             put_in(default_spec, args_path, ["redis://localhost"])

    assert Redix.child_spec([]) == put_in(default_spec, args_path, [[]])
    assert Redix.child_spec(name: :redix) == put_in(default_spec, args_path, [[name: :redix]])

    assert Redix.child_spec({"redis://localhost", name: :redix}) ==
             put_in(default_spec, args_path, ["redis://localhost", [name: :redix]])
  end

  describe "stop/1" do
    test "stops the connection" do
      {:ok, pid} = Redix.start_link()
      ref = Process.monitor(pid)
      assert Redix.stop(pid) == :ok

      assert_receive {:DOWN, ^ref, _, _, :normal}, 500
    end

    test "closes the socket as well" do
      {:ok, pid} = Redix.start_link(sync_connect: true)

      # This is a hack to get the socket. If I'll have a better idea, good for me :).
      {_, data} = :sys.get_state(pid)

      assert Port.info(data.socket) != nil
      assert Redix.stop(pid) == :ok
      assert Port.info(data.socket) == nil
    end
  end

  describe "command/2" do
    setup :connect

    test "PING", %{conn: c} do
      assert Redix.command(c, ["PING"]) == {:ok, "PONG"}
    end

    test "transactions - MULTI/EXEC", %{conn: c} do
      assert Redix.command(c, ["MULTI"]) == {:ok, "OK"}
      assert Redix.command(c, ["INCR", "multifoo"]) == {:ok, "QUEUED"}
      assert Redix.command(c, ["INCR", "multibar"]) == {:ok, "QUEUED"}
      assert Redix.command(c, ["INCRBY", "multifoo", 4]) == {:ok, "QUEUED"}
      assert Redix.command(c, ["EXEC"]) == {:ok, [1, 1, 5]}
    end

    test "transactions - MULTI/DISCARD", %{conn: c} do
      Redix.command!(c, ["SET", "discarding", "foo"])

      assert Redix.command(c, ["MULTI"]) == {:ok, "OK"}
      assert Redix.command(c, ["SET", "discarding", "bar"]) == {:ok, "QUEUED"}
      # Discarding
      assert Redix.command(c, ["DISCARD"]) == {:ok, "OK"}
      assert Redix.command(c, ["GET", "discarding"]) == {:ok, "foo"}
    end

    test "Lua scripting - EVAL", %{conn: c} do
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

    test "Redis errors", %{conn: c} do
      {:ok, _} = Redix.command(c, ~w(SET errs foo))

      assert_raise Redix.Error, "ERR value is not an integer or out of range", fn ->
        Redix.command(c, ~w(INCR errs))
      end
    end

    test "passing an empty list returns an error", %{conn: c} do
      message = "got an empty command ([]), which is not a valid Redis command"
      assert_raise ArgumentError, message, fn -> Redix.command(c, []) end
    end

    test "timeout", %{conn: c} do
      assert {:error, %ConnectionError{reason: :timeout}} = Redix.command(c, ~W(PING), timeout: 0)
    end

    test "Redix process crashes while waiting", %{conn: conn} do
      Process.flag(:trap_exit, true)

      pid =
        spawn_link(fn ->
          Redix.command(conn, ~w(BLPOP mid_command_disconnection 0))
        end)

      # We sleep to allow the task to issue the command to Redix.
      Process.sleep(100)

      Process.exit(conn, :kill)

      assert_receive {:EXIT, ^conn, :killed}
      assert_receive {:EXIT, ^pid, :killed}
    end

    test "passing a non-list as the command", %{conn: c} do
      message = "expected a list of binaries as each Redis command, got: \"PING\""

      assert_raise ArgumentError, message, fn ->
        Redix.command(c, "PING")
      end
    end
  end

  describe "pipeline/2" do
    setup :connect

    test "basic interaction", %{conn: c} do
      commands = [
        ["SET", "pipe", "10"],
        ["INCR", "pipe"],
        ["GET", "pipe"]
      ]

      assert Redix.pipeline(c, commands) == {:ok, ["OK", 11, "11"]}
    end

    test "a lot of commands so that TCP gets stressed", %{conn: c} do
      assert {:ok, "OK"} = Redix.command(c, ~w(SET stress_pipeline foo))

      ncommands = 10000
      commands = List.duplicate(~w(GET stress_pipeline), ncommands)

      # Let's do it twice to be sure the server can handle the data.
      {:ok, results} = Redix.pipeline(c, commands)
      assert length(results) == ncommands
      {:ok, results} = Redix.pipeline(c, commands)
      assert length(results) == ncommands
    end

    test "a single command should still return a list of results", %{conn: c} do
      assert Redix.pipeline(c, [["PING"]]) == {:ok, ["PONG"]}
    end

    test "Redis errors in the response", %{conn: c} do
      msg = "ERR value is not an integer or out of range"
      assert {:ok, resp} = Redix.pipeline(c, [~w(SET pipeline_errs foo), ~w(INCR pipeline_errs)])
      assert resp == ["OK", %Error{message: msg}]
    end

    test "passing an empty list of commands raises an error", %{conn: c} do
      msg = "no commands passed to the pipeline"
      assert_raise ArgumentError, msg, fn -> Redix.pipeline(c, []) end
    end

    test "passing one or more empty commands returns an error", %{conn: c} do
      message = "got an empty command ([]), which is not a valid Redis command"

      assert_raise ArgumentError, message, fn ->
        Redix.pipeline(c, [[]])
      end

      assert_raise ArgumentError, message, fn ->
        Redix.pipeline(c, [["PING"], [], ["PING"]])
      end
    end

    test "passing a PubSub command causes an error", %{conn: c} do
      assert_raise ArgumentError, ~r{Redix doesn't support Pub/Sub}, fn ->
        Redix.pipeline(c, [["PING"], ["SUBSCRIBE", "foo"]])
      end
    end

    test "timeout", %{conn: c} do
      assert {:error, %ConnectionError{reason: :timeout}} =
               Redix.pipeline(c, [~w(PING), ~w(PING)], timeout: 0)
    end

    test "commands must be lists of binaries", %{conn: c} do
      message = "expected a list of Redis commands, got: \"PING\""

      assert_raise ArgumentError, message, fn ->
        Redix.pipeline(c, "PING")
      end

      message = "expected a list of binaries as each Redis command, got: \"PING\""

      assert_raise ArgumentError, message, fn ->
        Redix.pipeline(c, ["PING"])
      end
    end
  end

  describe "command!/2" do
    setup :connect

    test "simple commands", %{conn: c} do
      assert Redix.command!(c, ["PING"]) == "PONG"
      assert Redix.command!(c, ["SET", "bang", "foo"]) == "OK"
      assert Redix.command!(c, ["GET", "bang"]) == "foo"
    end

    test "Redis errors", %{conn: c} do
      assert_raise Redix.Error, ~r/ERR unknown command .NONEXISTENT./, fn ->
        Redix.command!(c, ["NONEXISTENT"])
      end

      "OK" = Redix.command!(c, ["SET", "bang_errors", "foo"])

      assert_raise Redix.Error, "ERR value is not an integer or out of range", fn ->
        Redix.command!(c, ["INCR", "bang_errors"])
      end
    end

    test "connection errors", %{conn: c} do
      assert_raise Redix.ConnectionError, ":timeout", fn ->
        Redix.command!(c, ["PING"], timeout: 0)
      end
    end
  end

  describe "pipeline!/2" do
    setup :connect

    test "simple commands", %{conn: c} do
      assert Redix.pipeline!(c, [~w(SET ppbang foo), ~w(GET ppbang)]) == ~w(OK foo)
    end

    test "Redis errors in the list of results", %{conn: c} do
      commands = [~w(SET ppbang_errors foo), ~w(INCR ppbang_errors)]

      msg = "ERR value is not an integer or out of range"
      assert Redix.pipeline!(c, commands) == ["OK", %Redix.Error{message: msg}]
    end

    test "connection errors", %{conn: c} do
      assert_raise Redix.ConnectionError, ":timeout", fn ->
        Redix.pipeline!(c, [["PING"]], timeout: 0)
      end
    end
  end

  describe "transaction_pipeline/3" do
    setup :connect

    test "non-bang version", %{conn: conn} do
      commands = [~w(SET transaction_pipeline_key 1), ~w(GET transaction_pipeline_key)]
      assert Redix.transaction_pipeline(conn, commands) == {:ok, ["OK", "1"]}
    end

    test "bang version", %{conn: conn} do
      commands = [~w(SET transaction_pipeline_key 1), ~w(GET transaction_pipeline_key)]
      assert Redix.transaction_pipeline!(conn, commands) == ["OK", "1"]
    end
  end

  describe "noreply_* functions" do
    setup :connect

    test "noreply_pipeline/3", %{conn: conn} do
      commands = [~w(INCR noreply_pl_mykey), ~w(INCR noreply_pl_mykey)]
      assert Redix.noreply_pipeline(conn, commands) == :ok
      assert Redix.command!(conn, ~w(GET noreply_pl_mykey)) == "2"
    end

    test "noreply_command/3", %{conn: conn} do
      assert Redix.noreply_command(conn, ["SET", "noreply_cmd_mykey", "myvalue"]) == :ok
      assert Redix.command!(conn, ["GET", "noreply_cmd_mykey"]) == "myvalue"
    end
  end

  describe "timeouts and network errors" do
    setup :connect

    test "client suicide and reconnections", %{conn: c} do
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

    test "timeouts", %{conn: c} do
      assert {:error, %ConnectionError{reason: :timeout}} = Redix.command(c, ~w(PING), timeout: 0)

      # Let's check that the Redix connection doesn't reply anyways, even if the
      # timeout happened.
      refute_receive {_ref, _message}
    end

    test "mid-command disconnections", %{conn: conn} do
      {:ok, kill_conn} = Redix.start_link()

      capture_log(fn ->
        task = Task.async(fn -> Redix.command(conn, ~w(BLPOP mid_command_disconnection 0)) end)

        # Give the task the time to issue the command to Redis, then kill the connection.
        Process.sleep(50)
        Redix.command!(kill_conn, ~w(CLIENT KILL TYPE normal SKIPME yes))

        assert Task.await(task, 100) == {:error, %ConnectionError{reason: :disconnected}}
      end)
    end

    test "no leaking messages when timeout happen at the same time as disconnections", %{conn: c} do
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
  end

  test ":exit_on_disconnection option" do
    {:ok, c} = Redix.start_link(exit_on_disconnection: true)
    Process.flag(:trap_exit, true)

    capture_log(fn ->
      Redix.command!(c, ~w(QUIT))
      assert_receive {:EXIT, ^c, %ConnectionError{reason: :tcp_closed}}
    end)
  end

  defp connect(_context) do
    {:ok, conn} = Redix.start_link()
    {:ok, %{conn: conn}}
  end
end
