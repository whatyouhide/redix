# Telemetry

Since version v0.10.0, Redix uses [Telemetry][telemetry] for instrumentation and for having an extensible way of doing logging. Telemetry is a metrics and instrumentation library for Erlang and Elixir applications that is based on publishing events through a common interface and attaching handlers to handle those events. For more information about the library itself, see [its README][telemetry].

Before version v0.10.0, `Redix.start_link/1` and `Redix.PubSub.start_link/1` supported a `:log` option to control logging. For example, if you wanted to log disconnections at the `:error` level and reconnections and the `:debug` level, you would do:

    Redix.start_link(log: [disconnection: :error, reconnection: :debug])

The `:log` option is now deprecated in favour of either using the default Redix event handler or writing your own.

For information on the Telemetry events that Redix emits, see `Redix.Telemetry`.

## Writing your own handler

If you want control on how Redix events are logged or on what level they're logged at, you can use your own event handler. For example, you can create a module to handle these events:

    defmodule MyApp.RedixTelemetryHandler do
      require Logger

      def handle_event([:redix, event], _measurements, metadata, _config) do
        case event do
          :disconnection ->
            human_reason = Exception.message(metadata.reason)
            Logger.warn("Disconnected from #{metadata.address}: #{human_reason}")
            
          :failed_connection ->
            human_reason = Exception.message(metadata.reason)
            Logger.warn("Failed to connect to #{metadata.address}: #{human_reason}")

          :reconnection ->
            Logger.debug("Reconnected to #{metadata.address}")
        end
      end
    end

Once you have a module like this, you can attach it when your application starts:

    events = [
      [:redix, :disconnection],
      [:redix, :failed_connection],
      [:redix, :reconnection]
    ]

    :telemetry.attach_many(
      "my-redix-log-handler",
      events,
      &MyApp.RedixTelemetryHandler.handle_event/4,
      :config_not_needed_here
    )

[telemetry]: https://github.com/beam-telemetry/telemetry