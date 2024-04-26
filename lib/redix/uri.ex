defmodule Redix.URI do
  @moduledoc """
  This module provides functions to work with a Redis URI.

  This is generally intended for library developers using Redix under the hood.
  """

  @doc """
  Returns start options from a Redis URI.

  A **Redis URI** looks like this:

      redis://[username:password@]host[:port][/db]

  > #### Valkey {: .tip}
  >
  > URIs also work with [Valkey](https://valkey.io/), a Redis-compatible in-memory key-value
  > store. Use `valkey://` as the scheme instead of `redis://`.

  ## Examples

      iex> Redix.URI.to_start_options("redis://example.com")
      [host: "example.com"]

      iex> Redix.URI.to_start_options("rediss://username:password@example.com:5000/3")
      [ssl: true, database: 3, password: "password", username: "username", port: 5000, host: "example.com"]

  """
  @doc since: "1.2.0"
  @spec to_start_options(binary()) :: Keyword.t()
  def to_start_options(uri) when is_binary(uri) do
    %URI{host: host, port: port, scheme: scheme} = uri = URI.parse(uri)

    unless scheme in ["redis", "rediss", "valkey"] do
      raise ArgumentError,
            "expected scheme to be redis://, valkey://, or rediss://, got: #{scheme}://"
    end

    {username, password} = username_and_password(uri)

    []
    |> put_if_not_nil(:host, host)
    |> put_if_not_nil(:port, port)
    |> put_if_not_nil(:username, username)
    |> put_if_not_nil(:password, password)
    |> put_if_not_nil(:database, database(uri))
    |> enable_ssl_if_secure_scheme(scheme)
  end

  defp username_and_password(%URI{userinfo: nil}) do
    {nil, nil}
  end

  defp username_and_password(%URI{userinfo: userinfo}) do
    case String.split(userinfo, ":", parts: 2) do
      ["", password] ->
        {nil, password}

      [username, password] ->
        {username, password}

      _other ->
        raise ArgumentError,
              "expected password in the Redis URI to be given as redis://:PASSWORD@HOST or " <>
                "redis://USERNAME:PASSWORD@HOST"
    end
  end

  defp database(%URI{path: path}) when path in [nil, "", "/"] do
    nil
  end

  defp database(%URI{path: "/" <> path = full_path}) do
    case Integer.parse(path) do
      {db, rest} when rest == "" or binary_part(rest, 0, 1) == "/" ->
        db

      _other ->
        raise ArgumentError, "expected database to be an integer, got: #{inspect(full_path)}"
    end
  end

  defp put_if_not_nil(opts, _key, nil), do: opts
  defp put_if_not_nil(opts, key, value), do: Keyword.put(opts, key, value)

  defp enable_ssl_if_secure_scheme(opts, "rediss"), do: Keyword.put(opts, :ssl, true)
  defp enable_ssl_if_secure_scheme(opts, _scheme), do: opts
end
