defmodule Redix.URI do
  @moduledoc false

  @spec opts_from_uri(binary) :: Keyword.t()
  def opts_from_uri(uri) when is_binary(uri) do
    %URI{host: host, port: port, scheme: scheme} = uri = URI.parse(uri)

    unless scheme in ["redis", "rediss"] do
      raise ArgumentError, "expected scheme to be redis:// or rediss://, got: #{scheme}://"
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
