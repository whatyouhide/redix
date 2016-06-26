defmodule Redix.Auth do
  @moduledoc false

  alias Redix.Protocol

  @doc """
  Authenticates and selects the right database based on the state `state`.

  This function checks the given options to see if a password and/or a database
  are specified. If a password is specified, then this function will send the
  appropriate `AUTH` command to the Redis connection in `state.socket`. If a
  database is specified, then the appropriate `SELECT` command will be issued.

  The socket is expected to be in passive mode, and will be returned in passive
  mode to the caller.
  """
  @spec auth_and_select_db(:gen_tcp.socket, Keyword.t) :: {:ok, binary} | {:error, term}
  def auth_and_select_db(socket, opts) do
    # TODO: this *begs* for `with` once we depend on ~> 1.2.
    case auth(socket, opts[:password]) do
      {:ok, tail} ->
        select_db(socket, opts[:database], tail)
      {:error, _reason} = error ->
        error
    end
  end

  @spec auth(:gen_tcp.socket, nil | binary) :: {:ok | binary} | {:error, term}
  defp auth(socket, password)

  defp auth(_socket, nil) do
    {:ok, ""}
  end

  defp auth(socket, password) when is_binary(password) do
    case :gen_tcp.send(socket, Protocol.pack(["AUTH", password])) do
      :ok ->
        case blocking_recv(socket, "") do
          {:ok, "OK", tail} ->
            {:ok, tail}
          {:ok, error, _tail} ->
            {:error, error}
          {:error, _reason} = error ->
            error
        end
      {:error, _reason} = error ->
        error
    end
  end

  @spec select_db(:gen_tcp.socket, nil | non_neg_integer | binary, binary) :: {:ok, binary} | {:error, term}
  defp select_db(socket, db, tail)

  defp select_db(_socket, nil, tail) do
    {:ok, tail}
  end

  defp select_db(socket, db, tail) do
    case :gen_tcp.send(socket, Protocol.pack(["SELECT", db])) do
      :ok ->
        case blocking_recv(socket, tail) do
          {:ok, "OK", tail} ->
            {:ok, tail}
          {:ok, error, _state} ->
            {:error, error}
          {:error, _reason} = error ->
            error
        end
      {:error, _reason} = error ->
        error
    end
  end

  @spec blocking_recv(:gen_tcp.socket, binary, nil | (binary -> term)) :: {:ok, term, binary} | {:error, term}
  defp blocking_recv(socket, tail, continuation \\ nil) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        parser = continuation || &Protocol.parse/1
        case parser.(tail <> data) do
          {:ok, _resp, _rest} = result ->
            result
          {:continuation, continuation} ->
            blocking_recv(socket, "", continuation)
        end
      {:error, _reason} = error ->
        error
    end
  end
end
