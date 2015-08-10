defmodule RecsTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn} = Recs.start_link(host: "localhost", port: 6379)
    {:ok, %{conn: conn}}
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
