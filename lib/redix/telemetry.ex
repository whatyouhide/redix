defmodule Redix.Telemetry do
  @moduledoc false

  require Logger

  def attach_default_handler() do
    events = [
      [:redix, :disconnection],
      [:redix, :reconnection],
      [:redix, :failed_connection]
    ]

    :telemetry.attach_many("redix-default-telemetry-handler", events, &handle_event/4, :no_config)
  end

  def handle_event([:redix, event], _measurements, metadata, :no_config)
      when event in [:failed_connection, :disconnection, :reconnection] do
    sentinel_address = Map.get(metadata, :sentinel_address)

    case event do
      :failed_connection when is_binary(sentinel_address) ->
        human_reason = Exception.message(metadata.reason)
        _ = Logger.error("Failed to connect to sentinel #{sentinel_address}: #{human_reason}")

      :failed_connection ->
        human_reason = Exception.message(metadata.reason)
        _ = Logger.error("Failed to connect to Redis (#{metadata.address}): #{human_reason}")

      :disconnection ->
        human_reason = Exception.message(metadata.reason)
        _ = Logger.error("Disconnected from Redis (#{metadata.address}): #{human_reason}")

      :reconnection ->
        _ = Logger.info("Reconnected to Redis (#{metadata.address})")
    end
  end

  def execute(event, metadata) when is_atom(event) and is_map(metadata) do
    metadata = Map.put(metadata, :connection, self())
    :ok = :telemetry.execute([:redix, event], _measurements = %{}, metadata)
  end
end
