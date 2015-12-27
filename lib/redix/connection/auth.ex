defmodule Redix.Connection.Auth do
  @moduledoc false

  alias Redix.Protocol

  @doc """
  Authenticates and selects the right database based on the state `state`.

  This function checks the state `state` to see if in the options a password
  and/or a database are specified. If a password is specified, then this
  function will send the appropriate `AUTH` command to the Redis connection in
  `state.socket`. If a database is specified, then the appropriate `SELECT`
  command will be issued.
  """
  @spec auth_and_select_db(Redix.Connection.state) ::
    {:ok, Redix.Connection.state} | {:stop, term, Redix.Connection.state}
  def auth_and_select_db(state) do
    case auth(state, state.opts[:password]) do
      {:ok, state} ->
        case select_db(state, state.opts[:database]) do
          {:ok, state} ->
            :ok = :inet.setopts(state.socket, active: :once)
            {:ok, state}
          o ->
            o
        end
      o ->
        o
    end
  end

  defp auth(state, nil) do
    {:ok, state}
  end

  defp auth(%{socket: socket} = state, password) when is_binary(password) do
    case :gen_tcp.send(socket, Protocol.pack(["AUTH", password])) do
      :ok ->
        case wait_for_response(state) do
          {:ok, "OK", state} ->
            {:ok, state}
          {:ok, error, state} ->
            {:stop, error, state}
          {:error, reason} ->
            {:stop, reason, state}
        end
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp select_db(state, nil) do
    {:ok, state}
  end

  defp select_db(%{socket: socket} = state, db) do
    case :gen_tcp.send(socket, Protocol.pack(["SELECT", db])) do
      :ok ->
        case wait_for_response(state) do
          {:ok, "OK", state} ->
            {:ok, state}
          {:ok, error, state} ->
            {:stop, error, state}
          {:error, reason} ->
            {:stop, reason, state}
        end
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp wait_for_response(%{socket: socket} = state) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        data = state.tail <> data
        case Protocol.parse(data) do
          {:ok, value, rest} ->
            {:ok, value, %{state | tail: rest}}
          {:error, :incomplete} ->
            wait_for_response(%{state | tail: data})
        end
      {:error, _} = err ->
        err
    end
  end

end
