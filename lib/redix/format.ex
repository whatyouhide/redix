defmodule Redix.Format do
  @moduledoc false

  # Used for formatting things to print or log or anything like that.

  @spec format_host_and_port(host, :inet.port_number()) :: String.t()
        when host: {:local, String.t()} | charlist() | binary() | :inet.ip_address()
  def format_host_and_port(host, port)

  def format_host_and_port({:local, path}, 0) when is_binary(path), do: path

  def format_host_and_port(host, port) when is_binary(host) and is_integer(port),
    do: "#{host}:#{port}"

  def format_host_and_port(host, port) when is_list(host),
    do: format_host_and_port(IO.chardata_to_string(host), port)

  def format_host_and_port(host, port) when is_tuple(host) do
    case :inet.ntoa(host) do
      {:error, :einval} ->
        raise ArgumentError, "invalid host: #{inspect(host)}"

      host ->
        format_host_and_port(host, port)
    end
  end
end
