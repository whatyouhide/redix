defmodule Redix.Connection.Auth do
  @moduledoc false

  alias Redix.Protocol

  @doc """
  Authenticates and selects the right database based on the state `s`.

  This function checks the state `s` to see if in the options a password and/or
  a database are specified. If a password is specified, then this function will
  send the appropriate `AUTH` command to the Redis connection in `s.socket`. If
  a database is specified, then the appropriate `SELECT` command will be issued.
  """
  @spec auth_and_select_db(Redix.Connection.state) ::
    {:ok, Redix.Connection.state} | {:stop, term, Redix.Connection.state}
  def auth_and_select_db(s) do
    case auth(s, s.opts[:password]) do
      {:ok, s} ->
        case select_db(s, s.opts[:database]) do
          {:ok, s} ->
            :ok = :inet.setopts(s.socket, active: :once)
            {:ok, s}
          o ->
            o
        end
      o ->
        o
    end
  end

  defp auth(s, nil) do
    {:ok, s}
  end

  defp auth(%{socket: socket} = s, password) when is_binary(password) do
    case :gen_tcp.send(socket, Protocol.pack(["AUTH", password])) do
      :ok ->
        case wait_for_response(s) do
          {:ok, "OK", s} ->
            s = put_in(s.opts[:password], :redacted)
            {:ok, s}
          {:ok, error, s} ->
            {:stop, error, s}
          {:error, reason} ->
            {:stop, reason, s}
        end
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp select_db(s, nil) do
    {:ok, s}
  end

  defp select_db(%{socket: socket} = s, db) do
    case :gen_tcp.send(socket, Protocol.pack(["SELECT", db])) do
      :ok ->
        case wait_for_response(s) do
          {:ok, "OK", s} ->
            {:ok, s}
          {:ok, error, s} ->
            {:stop, error, s}
          {:error, reason} ->
            {:stop, reason, s}
        end
      {:error, reason} ->
        {:stop, reason, s}
    end
  end

  defp wait_for_response(%{socket: socket} = s) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        data = s.tail <> data
        case Protocol.parse(data) do
          {:ok, value, rest} ->
            {:ok, value, %{s | tail: rest}}
          {:error, :incomplete} ->
            wait_for_response(%{s | tail: data})
        end
      {:error, _} = err ->
        err
    end
  end

end
