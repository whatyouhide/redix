defmodule Redix.URI do
  @moduledoc false

  @spec opts_from_uri(binary) :: Keyword.t()
  def opts_from_uri(uri) when is_binary(uri) do
    %URI{host: host, port: port, scheme: scheme} = uri = URI.parse(uri)

    unless scheme == "redis" do
      raise ArgumentError, "expected scheme to be redis://, got: #{scheme}://"
    end

    []
    |> put_if_not_nil(:host, host)
    |> put_if_not_nil(:port, port)
    |> put_if_not_nil(:password, password(uri))
    |> put_if_not_nil(:database, database(uri))
  end

  defp password(%URI{userinfo: nil}) do
    nil
  end

  defp password(%URI{userinfo: userinfo}) do
    [_user, password] = String.split(userinfo, ":", parts: 2)
    password
  end

  defp database(%URI{path: path}) when path in [nil, "/"], do: nil
  defp database(%URI{path: "/" <> db}), do: String.to_integer(db)

  defp put_if_not_nil(opts, _key, nil), do: opts
  defp put_if_not_nil(opts, key, value), do: Keyword.put(opts, key, value)
end
