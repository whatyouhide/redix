defmodule RedixTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Redix.{
    ConnectionError,
    Error
  }

  @with_auth_port 16379
  @with_acl_port 6385

  setup_all do
    {:ok, conn} = Redix.start_link(sync_connect: true)
    Redix.command!(conn, ["FLUSHALL"])
    :ok = Redix.stop(conn)
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

    test "specifying a non-existing database" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)
        {:ok, pid} = Redix.start_link(database: 1000)

        assert_receive {:EXIT, ^pid, %Error{message: message}}, 500
        assert message in ["ERR invalid DB index", "ERR DB index is out of range"]
      end)
    end

    test "specifying a password when no password is set" do
      log =
        capture_log(fn ->
          Process.flag(:trap_exit, true)
          {:ok, pid} = Redix.start_link(password: "foo")

          assert_receive {:EXIT, ^pid, %Error{message: message}}, 500

          assert message =~ "ERR Client sent AUTH" or
                   message =~ "ERR AUTH <password> called without any password"
        end)

      assert log =~ "ERR Client sent AUTH" or
               log =~ "ERR AUTH <password> called without any password"
    end

    test "specifying an invalid user/password when ACL is set" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)

        {:ok, pid} =
          Redix.start_link(port: @with_acl_port, username: "nonexistent", password: "foo")

        assert_receive {:EXIT, ^pid, %Error{message: message}}, 500

        assert message =~ "WRONGPASS invalid username-password pair"
      end)
    end

    test "specifying a valid user/password when ACL is set" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)

        # Readonly user
        {:ok, c} =
          Redix.start_link(port: @with_acl_port, username: "readonly", password: "ropass")

        assert {:error, %Error{message: message}} = Redix.command(c, ~w(SET abc my_value))
        assert message =~ "NOPERM this user has no permissions to run the 'set' command"
        assert Redix.command(c, ~w(GET abc)) == {:ok, nil}

        # Superuser
        {:ok, c} =
          Redix.start_link(port: @with_acl_port, username: "superuser", password: "superpass")

        assert Redix.command(c, ~w(SET def my_value)) == {:ok, "OK"}
        assert Redix.command(c, ~w(GET def)) == {:ok, "my_value"}
      end)
    end

    test "specifying a string password when a password is set" do
      {:ok, pid} = Redix.start_link(port: @with_auth_port, password: "some-password")
      assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
    end

    test "overriding username when a URI is used" do
      {:ok, pid} =
        Redix.start_link("redis://ignored:some-password@localhost:#{@with_auth_port}",
          username: nil
        )

      assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
    end

    test "specifying a user/password when Redis version is < 6.0.0 (no ACL support)" do
      log_output =
        capture_log(fn ->
          # We warn but fall back to ignoring the username.
          {:ok, pid} =
            Redix.start_link(
              port: @with_auth_port,
              username: "discarded",
              password: "some-password",
              sync_connect: true
            )

          assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
        end)

      assert log_output =~ "a username was provided to connect to Redis"
    end

    test "specifying a mfa password when a password is set" do
      System.put_env("REDIX_MFA_PASSWORD", "some-password")

      {:ok, pid} =
        Redix.start_link(
          port: @with_auth_port,
          password: {System, :get_env, ["REDIX_MFA_PASSWORD"]}
        )

      assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
    after
      System.delete_env("REDIX_MFA_PASSWORD")
    end

    test "when unable to connect to Redis with sync_connect: true" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)

        assert {:error, %Redix.ConnectionError{reason: reason}} =
                 Redix.start_link(host: "nonexistent", sync_connect: true)

        assert_receive {:EXIT, _pid, %Redix.ConnectionError{}}, 1000

        # Apparently somewhere the error reason is :nxdomain but some other times it's
        # :timeout.
        assert reason in [:nxdomain, :timeout]
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

    test "using a rediss:// url, ignoring certificate" do
      {:ok, pid} =
        Redix.start_link("rediss://localhost:6384/3",
          socket_opts: [verify: :verify_none, reuse_sessions: false]
        )

      assert Redix.command(pid, ["PING"]) == {:ok, "PONG"}
    end

    test "using a rediss:// url, unknown certificate" do
      capture_log(fn ->
        Process.flag(:trap_exit, true)

        assert {:error, error} =
                 Redix.start_link("rediss://localhost:6384/3",
                   socket_opts: [reuse_sessions: false],
                   sync_connect: true
                 )

        assert %Redix.ConnectionError{reason: {:tls_alert, _}} = error
        assert_receive {:EXIT, _pid, ^error}, 1000
      end)
    end

    test "name registration with atom name" do
      {:ok, pid} = Redix.start_link(name: :redix_server)
      assert Process.whereis(:redix_server) == pid
      assert Redix.command(:redix_server, ["PING"]) == {:ok, "PONG"}
    end

    test "name registration with invalid name" do
      assert_raise ArgumentError, ~r/expected :name option to be one of the following/, fn ->
        Redix.start_link(name: "not a valid name")
      end
    end

    test "name registration with :global name" do
      {:ok, _pid} = Redix.start_link(name: {:global, :redix_server})
      assert Redix.command({:global, :redix_server}, ["PING"]) == {:ok, "PONG"}
    end

    test "name registration with :via registry name" do
      name = {:via, :global, :redix_server_via}
      {:ok, _pid} = Redix.start_link(name: name)
      assert Redix.command(name, ["PING"]) == {:ok, "PONG"}
    end

    test "passing options along with a Redis URI" do
      {:ok, pid} = Redix.start_link("redis://localhost", name: :redix_uri)
      assert Process.whereis(:redix_uri) == pid
    end

    test "using gen_statem options" do
      fullsweep_after = Enum.random(0..50000)
      {:ok, pid} = Redix.start_link(spawn_opt: [fullsweep_after: fullsweep_after])
      {:garbage_collection, info} = Process.info(pid, :garbage_collection)
      assert info[:fullsweep_after] == fullsweep_after
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

      message = "ERR value is not an integer or out of range"
      assert Redix.command(c, ~w(INCR errs)) == {:error, %Redix.Error{message: message}}
    end

    test "passing an empty list returns an error", %{conn: c} do
      message = "got an empty command ([]), which is not a valid Redis command"
      assert_raise ArgumentError, message, fn -> Redix.command(c, []) end
    end

    test "timeout", %{conn: c} do
      assert {:error, %ConnectionError{reason: :timeout}} = Redix.command(c, ~W(PING), timeout: 0)
    end

    test "Redix process crashes while waiting" do
      # We manually start a connection without start_supervised/1 here.
      {:ok, conn} = Redix.start_link()

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

    test "passing CLIENT REPLY commands causes an error", %{conn: c} do
      for command <- [~w(CLIENT REPLY ON), ~w(CLIENT reply Off), ~w(client reply skip)] do
        assert_raise ArgumentError, ~r{CLIENT REPLY commands are forbidden}, fn ->
          Redix.pipeline(c, [command])
        end
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

      assert Redix.transaction_pipeline(conn, commands, timeout: 0) ==
               {:error, %ConnectionError{reason: :timeout}}
    end

    test "bang version", %{conn: conn} do
      commands = [~w(SET transaction_pipeline_key 1), ~w(GET transaction_pipeline_key)]
      assert Redix.transaction_pipeline!(conn, commands) == ["OK", "1"]

      assert_raise Redix.ConnectionError, fn ->
        Redix.transaction_pipeline!(conn, commands, timeout: 0)
      end
    end
  end

  describe "noreply_* functions" do
    setup :connect

    test "noreply_pipeline/3", %{conn: conn} do
      commands = [~w(INCR noreply_pl_mykey), ~w(INCR noreply_pl_mykey)]
      assert Redix.noreply_pipeline(conn, commands) == :ok
      assert Redix.command!(conn, ~w(GET noreply_pl_mykey)) == "2"

      assert Redix.noreply_pipeline(conn, commands, timeout: 0) ==
               {:error, %ConnectionError{reason: :timeout}}
    end

    test "noreply_pipeline!/3", %{conn: conn} do
      commands = [~w(INCR noreply_pl_bang_mykey), ~w(INCR noreply_pl_bang_mykey)]
      assert Redix.noreply_pipeline!(conn, commands) == :ok
      assert Redix.command!(conn, ~w(GET noreply_pl_bang_mykey)) == "2"

      assert_raise Redix.ConnectionError, fn ->
        Redix.noreply_pipeline!(conn, commands, timeout: 0)
      end
    end

    test "noreply_command/3", %{conn: conn} do
      assert Redix.noreply_command(conn, ["SET", "noreply_cmd_mykey", "myvalue"]) == :ok
      assert Redix.command!(conn, ["GET", "noreply_cmd_mykey"]) == "myvalue"

      assert Redix.noreply_command(conn, ["SET", "noreply_cmd_mykey", "myvalue"], timeout: 0) ==
               {:error, %ConnectionError{reason: :timeout}}
    end

    test "noreply_command!/3", %{conn: conn} do
      assert Redix.noreply_command!(conn, ["SET", "noreply_cmd_bang_mykey", "myvalue"]) == :ok
      assert Redix.command!(conn, ["GET", "noreply_cmd_bang_mykey"]) == "myvalue"

      assert_raise Redix.ConnectionError, fn ->
        Redix.noreply_command!(conn, ["SET", "noreply_cmd_bang_mykey", "myvalue"], timeout: 0)
      end
    end

    # Regression for https://github.com/whatyouhide/redix/issues/192
    @tag :capture_log
    test "throw a human-readable error when the server disabled CLIENT commands and users " <>
           "of Redix ignore function results" do
      conn = start_supervised!({Redix, port: 6386})
      Process.flag(:trap_exit, true)
      key = Base.encode16(:crypto.strong_rand_bytes(6))

      # Here we send a command with noreply. Redis returns an error here since CLIENT commands
      # (issued under the hood) are disabled, but we are simulating a user ignoring the results
      # of the noreply_command/2 calls. When issuing more normal commands, Redix has trouble
      # finding the command in the ETS queue when popping the response and used to fail
      # with a hard-to-understand error (see the issue linked above).
      _ = Redix.noreply_command(conn, ["INCR", key])

      assert {%RuntimeError{} = error, _stacktrace} = catch_exit(Redix.command!(conn, ["PING"]))

      assert Exception.message(error) =~
               "failed to find an original command in the commands queue"
    end

    # Regression for https://github.com/whatyouhide/redix/issues/192
    test "return an error when the server disabled CLIENT commands" do
      conn = start_supervised!({Redix, port: 6386})
      key = Base.encode16(:crypto.strong_rand_bytes(6))

      assert {:error, %Redix.Error{} = error} = Redix.noreply_command(conn, ["INCR", key])
      assert error.message =~ "unknown command `CLIENT`"
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

    test "no leaking messages when timeout happens at the same time as disconnections", %{
      conn: conn
    } do
      {:ok, kill_conn} = Redix.start_link()

      capture_log(fn ->
        {_pid, ref} =
          Process.spawn(
            fn ->
              error = %ConnectionError{reason: :timeout}
              assert Redix.command(conn, ~w(BLPOP my_list 0), timeout: 0) == {:error, error}

              # The fact that we timed out should be respected here, even if the
              # connection is killed (no {:error, :disconnected} message should
              # arrive).
              refute_receive {_ref, _message}
            end,
            [:link, :monitor]
          )

        # Give the process time to issue the command to Redis, then kill the connection.
        Process.sleep(50)
        Redix.command!(kill_conn, ~w(CLIENT KILL TYPE normal SKIPME yes))

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

  describe "Telemetry" do
    setup :connect

    test "emits events when starting a pipeline with pipeline/3", %{conn: c} do
      {test_name, _arity} = __ENV__.function

      parent = self()
      ref = make_ref()

      handler = fn event, measurements, meta, _config ->
        if meta.connection == c do
          assert event == [:redix, :pipeline, :start]
          assert is_integer(measurements.system_time)
          assert meta.commands == [["PING"]]
          assert is_nil(meta.connection_name)
        end

        send(parent, ref)
      end

      :telemetry.attach(to_string(test_name), [:redix, :pipeline, :start], handler, :no_config)

      assert {:ok, ["PONG"]} = Redix.pipeline(c, [["PING"]])

      assert_receive ^ref

      :telemetry.detach(to_string(test_name))
    end

    test "emits events on successful pipelines with pipeline/3", %{conn: c} do
      {test_name, _arity} = __ENV__.function

      parent = self()
      ref = make_ref()

      handler = fn event, measurements, meta, _config ->
        if meta.connection == c do
          assert event == [:redix, :pipeline, :stop]
          assert is_integer(measurements.duration) and measurements.duration > 0
          assert meta.commands == [["PING"]]
          assert is_nil(meta.connection_name)
        end

        send(parent, ref)
      end

      :telemetry.attach(to_string(test_name), [:redix, :pipeline, :stop], handler, :no_config)

      assert {:ok, ["PONG"]} = Redix.pipeline(c, [["PING"]])

      assert_receive ^ref

      :telemetry.detach(to_string(test_name))
    end

    test "emits events on error pipelines with pipeline/3", %{conn: c} do
      {test_name, _arity} = __ENV__.function

      parent = self()
      ref = make_ref()

      handler = fn event, measurements, meta, _config ->
        if meta.connection == c do
          assert event == [:redix, :pipeline, :stop]
          assert is_integer(measurements.duration)
          assert meta.commands == [["PING"], ["PING"]]
          assert meta.kind == :error
          assert meta.reason == %ConnectionError{reason: :timeout}
          assert is_nil(meta.connection_name)
        end

        send(parent, ref)
      end

      :telemetry.attach(to_string(test_name), [:redix, :pipeline, :stop], handler, :no_config)

      assert {:error, %ConnectionError{reason: :timeout}} =
               Redix.pipeline(c, [~w(PING), ~w(PING)], timeout: 0)

      assert_receive ^ref

      :telemetry.detach(to_string(test_name))
    end

    test "emits connection-related events on disconnections and reconnections" do
      {test_name, _arity} = __ENV__.function

      parent = self()
      ref = make_ref()

      handler = fn event, measurements, meta, _config ->
        # We need to run this test only if was called for this Redix connection so that
        # we can run in parallel with the pubsub tests.
        if meta.connection_name == :redix_telemetry_test do
          send(parent, {ref, event, measurements, meta})
        end
      end

      events = [[:redix, :connection], [:redix, :disconnection]]
      :ok = :telemetry.attach_many(to_string(test_name), events, handler, :no_config)

      {:ok, c} = Redix.start_link(name: :redix_telemetry_test)

      assert_receive {^ref, [:redix, :connection], measurements, meta}, 1000
      assert measurements == %{}

      assert %{
               address: "localhost:6379",
               connection: ^c,
               reconnection: false,
               connection_name: :redix_telemetry_test
             } = meta

      capture_log(fn ->
        assert {:ok, _} = Redix.command(c, ~w(QUIT))

        assert_receive {^ref, [:redix, :disconnection], measurements, meta}, 1000
        assert measurements == %{}

        assert %{
                 address: "localhost:6379",
                 connection: ^c,
                 connection_name: :redix_telemetry_test
               } = meta

        assert_receive {^ref, [:redix, :connection], measurements, meta}, 5000
        assert measurements == %{}

        assert %{
                 address: "localhost:6379",
                 connection: ^c,
                 reconnection: true,
                 connection_name: :redix_telemetry_test
               } = meta
      end)
    end

    test "the :connection_name metadata field is filled in when the connection is named" do
      {test_name, _arity} = __ENV__.function
      {:ok, c} = Redix.start_link(name: test_name)

      parent = self()
      ref = make_ref()

      handler = fn event, measurements, meta, _config ->
        if meta.connection == c do
          send(parent, {ref, event, measurements, meta})
        end
      end

      :telemetry.attach_many(
        to_string(test_name),
        [[:redix, :pipeline, :start], [:redix, :pipeline, :stop]],
        handler,
        :no_config
      )

      assert {:ok, ["PONG"]} = Redix.pipeline(test_name, [["PING"]])

      assert_receive {^ref, [:redix, :pipeline, :start], measurements, meta}
      assert is_integer(measurements.system_time)
      assert meta.commands == [["PING"]]
      assert meta.connection_name == test_name

      assert_receive {^ref, [:redix, :pipeline, :stop], measurements, meta}
      assert is_integer(measurements.duration) and measurements.duration > 0
      assert meta.commands == [["PING"]]
      assert meta.connection_name == test_name

      :telemetry.detach(to_string(test_name))
    end

    test "supports the :telemetry_metadata option", %{conn: c} do
      {test_name, _arity} = __ENV__.function

      parent = self()
      ref = make_ref()

      handler = fn event, measurements, meta, _config ->
        if meta.connection == c do
          send(parent, {ref, event, measurements, meta})
        end
      end

      :telemetry.attach_many(
        to_string(test_name),
        [[:redix, :pipeline, :start], [:redix, :pipeline, :stop]],
        handler,
        :no_config
      )

      assert {:ok, ["PONG"]} = Redix.pipeline(c, [["PING"]], telemetry_metadata: %{extra: 42})

      assert_receive {^ref, [:redix, :pipeline, :start], measurements, meta}
      assert is_integer(measurements.system_time)
      assert meta.commands == [["PING"]]
      assert meta.extra_metadata == %{extra: 42}

      assert_receive {^ref, [:redix, :pipeline, :stop], measurements, meta}
      assert is_integer(measurements.duration) and measurements.duration > 0
      assert meta.commands == [["PING"]]
      assert meta.extra_metadata == %{extra: 42}

      :telemetry.detach(to_string(test_name))
    end
  end

  defp connect(context) do
    conn = start_supervised!(Supervisor.child_spec(Redix, id: context.test))
    {:ok, %{conn: conn}}
  end
end
