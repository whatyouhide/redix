defmodule Redix.TelemetryTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  describe "attach_default_handler/0" do
    test "attaches an handler that logs disconnections and reconnections" do
      Redix.Telemetry.attach_default_handler()

      {:ok, conn} = Redix.start_link()

      client_id = Redix.command!(conn, ["CLIENT", "ID"])

      addr =
        conn
        |> Redix.command!(["CLIENT", "LIST"])
        |> String.split("\n", trim: true)
        |> Enum.find(&(&1 =~ "id=#{client_id}"))
        |> String.split()
        |> Enum.find_value(fn
          "addr=" <> addr -> addr
          _other -> nil
        end)

      log =
        capture_log(fn ->
          assert Redix.command!(conn, ["CLIENT", "KILL", addr]) == "OK"
          assert wait_for_reconnection(conn, 1000) == :ok
        end)

      assert log =~ ~r/Connection .* disconnected from Redis at localhost:6379/
      assert log =~ ~r/Connection .* reconnected to Redis at localhost:6379/
    end

    test "attaches an handler that logs failed connections" do
      Redix.Telemetry.attach_default_handler()

      log =
        capture_log(fn ->
          {:ok, _conn} = Redix.start_link("redis://localhost:9999")
          # Sleep just a bit to let it log the first failed connection message.
          Process.sleep(50)
        end)

      assert log =~ ~r/Connection .* failed to connect to Redis at localhost:9999/
    end

    test "attaches an handler that logs failed connections for sentinels" do
      Redix.Telemetry.attach_default_handler()

      log =
        capture_log(fn ->
          {:ok, _conn} =
            Redix.start_link(sentinel: [sentinels: ["redis://localhost:9999"], group: "main"])

          # Sleep just a bit to let it log the first failed connection message.
          Process.sleep(50)
        end)

      assert log =~ ~r/Connection .* failed to connect to sentinel at localhost:9999/
    end
  end

  defp wait_for_reconnection(conn, timeout) do
    case Redix.command(conn, ["PING"]) do
      {:ok, "PONG"} ->
        :ok

      {:error, _reason} when timeout > 25 ->
        Process.sleep(25)
        wait_for_reconnection(conn, timeout - 25)

      {:error, reason} ->
        {:error, reason}
    end
  end
end
