defmodule Red.URI do
  @moduledoc false

  defmodule URIError do
    defexception [:message]
  end

  @spec opts_from_uri(binary) :: Keyword.t
  def opts_from_uri(uri) when is_binary(uri) do
    uri = URI.parse(uri)
    ensure_scheme_is_redis!(uri)
    %URI{host: host, port: port} = uri

    [
      host: host,
      port: port,
      password: password(uri),
      database: db(uri),
    ]
  end

  defp ensure_scheme_is_redis!(%URI{scheme: "redis"}),
    do: :ok
  defp ensure_scheme_is_redis!(%URI{scheme: scheme}),
    do: raise(URIError, message: "scheme is not redis:// but '#{scheme}://'")

  defp password(%URI{userinfo: nil}) do
    nil
  end

  defp password(%URI{userinfo: userinfo}) do
    [_user, password] = String.split(userinfo, ":", parts: 2)
    password
  end

  defp db(%URI{path: nil}), do: nil
  defp db(%URI{path: "/"}), do: nil
  defp db(%URI{path: "/" <> db}), do: String.to_integer(db)
end
