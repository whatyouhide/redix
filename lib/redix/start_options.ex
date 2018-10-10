defmodule Redix.StartOptions do
  @moduledoc false

  @default_log_options [
    disconnection: :error,
    failed_connection: :error,
    reconnection: :info
  ]

  @default_options [
    socket_opts: [],
    ssl: false,
    sync_connect: false,
    backoff_initial: 500,
    backoff_max: 30000,
    log: @default_log_options,
    exit_on_disconnection: false
  ]

  @allowed_options [:host, :port, :database, :password, :name] ++ Keyword.keys(@default_options)

  def sanitize(options) when is_list(options) do
    @default_options
    |> Keyword.merge(options)
    |> assert_only_known_options()
    |> sanitize_host_and_port()
    |> fill_default_log_options()
  end

  defp assert_only_known_options(options) do
    Enum.each(options, fn {key, _value} ->
      unless key in @allowed_options do
        raise ArgumentError, "unknown option: #{inspect(key)}"
      end
    end)

    options
  end

  defp fill_default_log_options(options) do
    Keyword.update!(options, :log, &Keyword.merge(@default_log_options, &1))
  end

  defp sanitize_host_and_port(options) do
    {host, port} =
      case {Keyword.get(options, :host, "localhost"), Keyword.fetch(options, :port)} do
        {{:local, _unix_socket_path}, {:ok, port}} when port != 0 ->
          raise ArgumentError,
                "when using Unix domain sockets, the port must be 0, got: #{inspect(port)}"

        {{:local, _unix_socket_path} = host, :error} ->
          {host, 0}

        {_host, {:ok, port}} when not is_integer(port) ->
          raise ArgumentError,
                "expected an integer as the value of the :port option, got: #{inspect(port)}"

        {host, {:ok, port}} when is_binary(host) ->
          {String.to_charlist(host), port}

        {host, :error} when is_binary(host) ->
          {String.to_charlist(host), 6379}
      end

    Keyword.merge(options, host: host, port: port)
  end
end
