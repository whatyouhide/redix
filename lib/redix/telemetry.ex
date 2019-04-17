defmodule Redix.Telemetry do
  @moduledoc false

  require Logger

  @default_log_options [
    disconnection: :error,
    failed_connection: :error,
    reconnection: :info
  ]

  def attach_default_handler() do
    events = [
      [:redix, :disconnection],
      [:redix, :reconnection],
      [:redix, :failed_connection]
    ]

    # Avoid warnings if :telemetry is not available.
    telemetry = :telemetry
    telemetry.attach_many("redix-default-telemetry-handler", events, &handle_event/4, :no_config)
  end

  def handle_event([:redix, event], _measurements, metadata, :no_config) do
    handle_event_with_log_opts(event, metadata, @default_log_options)
  end

  defp handle_event_with_log_opts(event, metadata, log_opts) do
    level = Keyword.fetch!(log_opts, event)

    message =
      case event do
        :failed_connection ->
          human_reason = Exception.message(metadata.reason)
          "Failed to connect to Redis (#{metadata.address}): #{human_reason}"

        :disconnection ->
          human_reason = Exception.message(reason: metadata.reason)
          "Disconnected from Redis (#{metadata.address}): #{human_reason}"

        :reconnection ->
          "Reconnected to Redis (#{metadata.address})"
      end

    :ok = Logger.log(level, message)
  end

  @spec execute(keyword(), atom(), map()) :: :ok
  def execute(event, log_opts, metadata) do
    metadata = Map.put(metadata, :connection, self())
    telemetry_execute(event, log_opts, metadata)
  end

  if Code.ensure_compiled?(:telemetry) do
    defp telemetry_execute(event, _log_opts, metadata) do
      :ok = :telemetry.execute([:redix, event], _measurements = %{}, metadata)
    end
  else
    # This approach is deprecated. We show the deprecation if users specifically
    # pass the :log option to start_link/1,2.
    defp telemetry_execute(event, log_opts, metadata) do
      :ok = handle_event_with_log_opts(event, metadata, log_opts)
    end
  end
end
