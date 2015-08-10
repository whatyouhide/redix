defmodule RecsTest do
  use ExUnit.Case, async: true

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

  test "command/2", %{conn: c} do
    assert Recs.command(c, ["SET", "foo", "1"]) == "OK"
    assert Recs.command(c, ["GET", "foo"]) == "1"
  end

  test "pipeline/2", %{conn: c} do
    commands = [
      ["SET", "pipe", "10"],
      ["INCR", "pipe"],
      ["GET", "pipe"],
    ]
    assert Recs.pipeline(c, commands) == ["OK", 11, "11"]
  end
end
