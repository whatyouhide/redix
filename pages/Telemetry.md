# Telemetry

Since version v0.10.0, Redix uses [Telemetry][telemetry] for instrumentation and for having an extensible way of doing logging. Telemetry is a metrics and instrumentation library for Erlang and Elixir applications that is based on publishing events through a common interface and attaching handlers to handle those events. For more information about the library itself, see [its README][telemetry].

Telemetry is an optional dependency for Redix, so if you want to use it, add it explicitly to your dependencies:

    defp deps do
      [
        # ...
        {:redix, ">= 0.0.0"},
        {:telemetry, ">= 0.0.0"}
      ]

Before version v0.10.0, `Redix.start_link/1` and `Redix.PubSub.start_link/1` supported a `:log` option to control logging. For example, if you wanted to log disconnections at the `:error` level and reconnections and the `:debug` level, you would do:

    Redix.start_link(log: [disconnection: :error, reconnection: :debug])

The `:log` option is now deprecated in favour of either using the default Redix event handler or writing your own. See below for more information.

## Events

Redix connections (both `Redix` and `Redix.PubSub`) execute the following Telemetry events:

  * `[:redix, :disconnection]` - executed when the connection is lost with the Redis server. There are no measurements associated with this event. Metadata are:

    * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.
    * `:address` - the address the connection was connected to.
    * `:conn` - the PID of the Redix connection that emitted the event.

  * `[:redix, :failed_connection]` - executed when Redix can't connect to the specified Redis server, either when starting up the connection or after a disconnection. There are no measurements associated with this event. Metadata are:

    * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.
    * `:address` or `:sentinel_address` - the address the connection was trying to connect to (either a Redis server or a Redis Sentinel instance).
    * `:conn` - the PID of the Redix connection that emitted the event.

  * `[:redix, :reconnection]` - executed when a Redix connection that had disconnected reconnects to a Redis server. There are no measurements associated with this event. Metadata are:

    * `:address` - the address the connection successfully reconnected to.
    * `:conn` - the PID of the Redix connection that emitted the event.

More events might be added in the future and that won't be considered a breaking change, so if you're writing a handler for Redix events be sure to ignore events that are not known. All future Redix events will start with the `:redix` atom, like the ones above.

## Default handler for logging

By default, Redix provides a Telemetry event handler that performs logging. Events will be logged as follows:

  * `[:redix, :disconnection]` and `[:redix, :failed_connection]` are logged at the `:error` level.

  * `[:redix, :reconnection]` is logged at the `:info` level.

These are reasonable defaults that work for most applications. The default event handler does not support customizing the log levels. To use this default log handler, call this when starting your application:

    Redix.attach_default_telemetry_handler()

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