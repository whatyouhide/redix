defmodule RedixTest do
  use ExUnit.Case, async: true
  import Redix.TestHelpers
  alias Redix.Error

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
  test "start_link/1: nil options don't mess start_link up" do
    assert {:ok, pid} = Redix.start_link(host: nil, port: nil)
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
    assert {:ok, pid} = Redix.start_link(name: :redix_server)
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

  test "pipeline/2: Redis errors in the response", %{conn: c} do
    msg = "ERR value is not an integer or out of range"
    assert {:ok, resp} = Redix.pipeline(c, [~w(SET pipeline_errs foo), ~w(INCR pipeline_errs)])
    assert resp == ["OK", %Error{message: msg}]
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

  ## PubSub

  test "subscribe/3: one channel", %{conn: c} do
    assert :ok = Redix.subscribe(c, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
  end

  test "subscribe/3: multiple channels", %{conn: c} do
    assert :ok = Redix.subscribe(c, ["foo", "bar"], self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}
  end

  test "psubscribe/3: one pattern", %{conn: c} do
    assert :ok = Redix.psubscribe(c, "foo*", self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
  end

  test "psubscribe/3: multiple patterns", %{conn: c} do
    assert :ok = Redix.psubscribe(c, ["foo*", "bar*"], self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :psubscribe, "bar*", _}
  end

  test "subscribe/3: sending to a pid", %{conn: c} do
    task = Task.async fn ->
      assert_receive {:redix_pubsub, :subscribe, "foo", _}
    end

    assert :ok = Redix.subscribe(c, "foo", task.pid)
    Task.await(task)
  end

  test "unsubscribe/3: single channel", %{conn: c} do
    assert :ok = Redix.subscribe(c, "foo", self())
    assert :ok = Redix.unsubscribe(c, "foo", self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}
  end

  test "unsubscribe/3: multiple channels", %{conn: c} do
    assert :ok = Redix.subscribe(c, ~w(foo bar), self())
    assert :ok = Redix.unsubscribe(c, ~w(foo bar), self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}
    assert_receive {:redix_pubsub, :unsubscribe, "bar", _}
  end

  test "punsubscribe/3: single channel", %{conn: c} do
    assert :ok = Redix.psubscribe(c, "foo*", self())
    assert :ok = Redix.punsubscribe(c, "foo*", self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}
  end

  test "punsubscribe/3: multiple channels", %{conn: c} do
    assert :ok = Redix.psubscribe(c, ~w(foo* bar?), self())
    assert :ok = Redix.punsubscribe(c, ~w(foo* bar?), self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :punsubscribe, "bar?", _}
  end

  test "pubsub: subscribing to channels and receiving messages", %{conn: c} do
    {:ok, pubsub} = Redix.start_link

    Redix.subscribe(pubsub, ~w(foo bar), self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert_receive {:redix_pubsub, :subscribe, "bar", _}

    Redix.pipeline!(c, [~w(PUBLISH foo foo), ~w(PUBLISH bar bar), ~w(PUBLISH baz baz)])
    assert_receive {:redix_pubsub, :message, "foo", "foo"}
    assert_receive {:redix_pubsub, :message, "bar", "bar"}
    refute_receive {:redix_pubsub, :message, "baz", "baz"}

    Redix.unsubscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, :unsubscribe, "foo", _}

    Redix.pipeline!(c, [~w(PUBLISH foo foo), ~w(PUBLISH bar bar)])
    refute_receive {:redix_pubsub, :message, "foo", "foo"}
    assert_receive {:redix_pubsub, :message, "bar", "bar"}
  end

  test "pubsub: subscribing to patterns and receiving messages", %{conn: c} do
    {:ok, pubsub} = Redix.start_link

    Redix.psubscribe(pubsub, ~w(foo* ba?), self())
    assert_receive {:redix_pubsub, :psubscribe, "foo*", _}
    assert_receive {:redix_pubsub, :psubscribe, "ba?", _}

    Redix.pipeline!(c, [~w(PUBLISH foo_1 foo_1),
                        ~w(PUBLISH foo_2 foo_2),
                        ~w(PUBLISH bar bar),
                        ~w(PUBLISH barfoo barfoo)])

    assert_receive {:redix_pubsub, :pmessage, "foo_1", {"foo*", "foo_1"}}
    assert_receive {:redix_pubsub, :pmessage, "foo_2", {"foo*", "foo_2"}}
    assert_receive {:redix_pubsub, :pmessage, "bar", {"ba?", "bar"}}
    refute_receive {:redix_pubsub, :pmessage, "barfoo", {_, "barfoo"}}

    Redix.punsubscribe(pubsub, "foo*", self())
    assert_receive {:redix_pubsub, :punsubscribe, "foo*", _}

    Redix.pipeline!(c, [~w(PUBLISH foo_x foo_x), ~w(PUBLISH baz baz)])

    refute_receive {:redix_pubsub, :pmessage, "foo_x", {"foo*", "foo_x"}}
    assert_receive {:redix_pubsub, :pmessage, "baz", {"ba?", "baz"}}
  end

  test "pubsub: subscribing multiple times to the same channel has no effects", %{conn: c} do
    {:ok, pubsub} = Redix.start_link

    Redix.subscribe(pubsub, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}

    Redix.subscribe(pubsub, ["foo", "bar"], self())
    refute_receive {:redix_pubsub, :subscribe, "foo", _} # already subscribed to "foo"
    assert_receive {:redix_pubsub, :subscribe, "bar", _}

    # Let's be sure we only receive *one* message on the "foo" channel.
    Redix.command!(c, ~w(PUBLISH foo hello))
    assert_receive {:redix_pubsub, :message, "hello", _}
    refute_receive {:redix_pubsub, :message, "hello", _}
  end

  test "pubsub: once you (p)subscribe, you go in pubsub mode (with subscribe/3)", %{conn: c} do
    assert :ok = Redix.subscribe(c, "foo", self())
    assert_receive {:redix_pubsub, :subscribe, "foo", _}
    assert Redix.command(c, ["PING"]) == {:error, :pubsub_mode}
  end

  test "pubsub: once you (p)subscribe, you go in pubsub mode (with psubscribe/3)", %{conn: c} do
    assert :ok = Redix.psubscribe(c, "fo*", self())
    assert_receive {:redix_pubsub, :psubscribe, "fo*", _}
    assert Redix.command(c, ["PING"]) == {:error, :pubsub_mode}
  end

  test "pubsub: you can't unsubscribe if you're not in pubsub mode", %{conn: c} do
    assert Redix.unsubscribe(c, "foo", self()) == {:error, :not_pubsub_mode}
    assert Redix.punsubscribe(c, "fo*", self()) == {:error, :not_pubsub_mode}
  end

  test "pubsub?/1: returns true if the conn is in pubsub mode, false otherwise", %{conn: c} do
    refute Redix.pubsub?(c)
    Redix.subscribe(c, "foo", self())
    assert Redix.pubsub?(c)
  end
end
