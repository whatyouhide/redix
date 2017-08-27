defmodule Redix.URI do
  @moduledoc false

  defmodule URIError do
    @moduledoc """
    Error in parsing a Redis URI or error in the content of the URI.
    """
    defexception [:message]
  end

  @spec opts_from_uri(binary | URI.t | nil) :: Keyword.t
  def opts_from_uri(uri)

  def opts_from_uri(nil) do
    []
  end

  def opts_from_uri(uri) when is_binary(uri) do
    uri |> URI.parse() |> opts_from_uri()
  end

  def opts_from_uri(%URI{host: host, port: port, scheme: scheme} = uri) do
    unless scheme == "redis" do
      raise URIError, message: "expected scheme to be redis://, got: #{scheme}://"
    end

    reject_nils([
      host: host,
      port: port,
      password: password(uri),
      database: database(uri),
    ])
  end

  defp password(%URI{userinfo: nil}) do
    nil
  end

  defp password(%URI{userinfo: userinfo}) do
    [_user, password] = String.split(userinfo, ":", parts: 2)
    password
  end

  defp database(%URI{path: nil}), do: nil
  defp database(%URI{path: "/"}), do: nil
  defp database(%URI{path: "/" <> db}), do: String.to_integer(db)

  defp reject_nils(opts) when is_list(opts) do
    Enum.reject(opts, &match?({_, nil}, &1))
  end
end
