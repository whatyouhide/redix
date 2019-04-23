defmodule Redix.StartOptions do
  @moduledoc false

  @default_sentinel_options [
    timeout: 500,
    socket_opts: [],
    ssl: false,
    role: :primary
  ]

  @default_options [
    socket_opts: [],
    ssl: false,
    sync_connect: false,
    backoff_initial: 500,
    backoff_max: 30000,
    exit_on_disconnection: false,
    timeout: 5000
  ]

  @allowed_options [:host, :port, :database, :password, :name, :sentinel] ++
                     Keyword.keys(@default_options)

  def sanitize(options) when is_list(options) do
    # TODO: remove support for :log in Redix v0.11+.
    options =
      if Keyword.has_key?(options, :log) do
        IO.warn(
          "The :log option has been deprecated in favour of using :telemetry. " <>
            "See https://hexdocs.pm/redix/telemetry.html for more information."
        )

        Keyword.delete(options, :log)
      else
        options
      end

    @default_options
    |> Keyword.merge(options)
    |> assert_only_known_options()
    |> maybe_sanitize_sentinel_opts()
    |> maybe_sanitize_host_and_port()
  end

  defp assert_only_known_options(options) do
    Enum.each(options, fn {key, _value} ->
      unless key in @allowed_options do
        raise ArgumentError, "unknown option: #{inspect(key)}"
      end
    end)

    options
  end

  defp maybe_sanitize_sentinel_opts(options) do
    case Keyword.fetch(options, :sentinel) do
      {:ok, sentinel_opts} ->
        Keyword.put(options, :sentinel, sanitize_sentinel_opts(sentinel_opts))

      :error ->
        options
    end
  end

  defp sanitize_sentinel_opts(sentinel_opts) do
    sentinel_opts =
      case Keyword.fetch(sentinel_opts, :sentinels) do
        {:ok, sentinels} when is_list(sentinels) and sentinels != [] ->
          sentinels = Enum.map(sentinels, &normalize_sentinel_address(&1, sentinel_opts))
          Keyword.replace!(sentinel_opts, :sentinels, sentinels)

        {:ok, sentinels} ->
          raise ArgumentError,
                "the :sentinels option inside :sentinel must be a non-empty list, got: " <>
                  inspect(sentinels)

        :error ->
          raise ArgumentError, "the :sentinels option is required inside :sentinel"
      end

    unless Keyword.has_key?(sentinel_opts, :group) do
      raise ArgumentError, "the :group option is required inside :sentinel"
    end

    if Keyword.has_key?(sentinel_opts, :host) or Keyword.has_key?(sentinel_opts, :port) do
      raise ArgumentError, ":host or :port can't be passed as option if :sentinel is used"
    end

    Keyword.merge(@default_sentinel_options, sentinel_opts)
  end

  defp normalize_sentinel_address(sentinel_uri, sentinel_opts) when is_binary(sentinel_uri) do
    sentinel_uri |> Redix.URI.opts_from_uri() |> normalize_sentinel_address(sentinel_opts)
  end

  defp normalize_sentinel_address(opts, sentinel_opts) when is_list(opts) do
    opts =
      if opts[:host] do
        Keyword.update!(opts, :host, &to_charlist/1)
      else
        raise ArgumentError, "a host should be specified for each sentinel"
      end

    unless opts[:port] do
      raise ArgumentError, "a port should be specified for each sentinel"
    end

    Keyword.merge(Keyword.take(sentinel_opts, [:password]), opts)
  end

  defp normalize_sentinel_address(other, _sentinel_opts) do
    raise ArgumentError,
          "sentinel address should be specified as a URI or a keyword list, got: " <>
            inspect(other)
  end

  defp maybe_sanitize_host_and_port(options) do
    if Keyword.has_key?(options, :sentinel) do
      options
    else
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
end
