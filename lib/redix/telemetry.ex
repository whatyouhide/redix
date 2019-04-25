defmodule Redix.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  Redix connections (both `Redix` and `Redix.PubSub`) execute the
  following Telemetry events:

    * `[:redix, :disconnection]` - executed when the connection is lost
      with the Redis server. There are no measurements associated with
      this event. Metadata are:

      * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.
      * `:address` - the address the connection was connected to.
      * `:connection` - the PID or registered name of the Redix connection
        that emitted the event.

    * `[:redix, :failed_connection]` - executed when Redix can't connect to
      the specified Redis server, either when starting up the connection or
      after a disconnection. There are no measurements associated with this event.
      Metadata are:

      * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.
      * `:address` or `:sentinel_address` - the address the connection was trying
        to connect to (either a Redis server or a Redis Sentinel instance).
      * `:connection` - the PID or registered name of the Redix connection
        that emitted the event.

    * `[:redix, :reconnection]` - executed when a Redix connection that had
      disconnected reconnects to a Redis server. There are no measurements
      associated with this event. Metadata are:

      * `:address` - the address the connection successfully reconnected to.
      * `:connection` - the PID or registered name of the Redix connection
          that emitted the event.

  `Redix` connections execute the following Telemetry events when commands or
  pipelines of any kind are executed.

    * `[:redix, :pipeline]` - executed when a pipeline (or command, which is a
      pipeline with just one command) is successfully sent to the server and
      a reply comes from the server. Measurements are:

      * `:elapsed_time` (integer) - the elapsed time that it took to send the
        pipeline to the server and get a reply. The elapsed time is expressed in
        the `:native` time unit. See `System.convert_time_unit/3`.

      Metadata are:

      * `:connection` - the connection that emitted the event. If the connection
        was registered with a name, the name is used here, otherwise the PID.
      * `:commands` - the commands sent to the server. This is always a list of
        commands, so even if you do `Redix.command(conn, ["PING"])` than the
        list of commands will be `[["PING"]]`.
      * `:start_time` - the system time when the pipeline was issued. This could be
        useful for tracing. The time unit is `:native`, see `System.convert_time_unit/3`.

    * `[:redix, :pipeline, :error]` - executed when there's an error talking to
      the server. There are no measurements. Metadata are:

      * `:connection` - the connection that emitted the event. If the connection
        was registered with a name, the name is used here, otherwise the PID.
      * `:commands` - the commands sent to the server. This is always a list of
        commands, so even if you do `Redix.command(conn, ["PING"])` than the list
        of commands will be `[["PING"]]`.
      * `:start_time` - the system time when the pipeline was issued. This could
        be useful for tracing. The time unit is `:native`, see `System.convert_time_unit/3`.
      * `:reason` - the error reason.

  More events might be added in the future and that won't be considered a breaking
  change, so if you're writing a handler for Redix events be sure to ignore events
  that are not known. All future Redix events will start with the `:redix` atom,
  like the ones above.

  A default handler that logs these events appropriately is provided, see
  `attach_default_handler/0`. Otherwise, you can write your own handler to
  instrument or log events, see the [Telemetry page](telemetry.html) in the docs.
  """

  require Logger

  @doc """
  Attaches the default Redix-provided Telemetry handler.

  This function attaches a default Redix-provided handler that logs
  (using Elixir's `Logger`) the following events:

    * `[:redix, :disconnection]` - logged at the `:error` level
    * `[:redix, :failed_connection]` - logged at the `:error` level
    * `[:redix, :reconnection]` - logged at the `:info` level

  See the module documentation for more information. If you want to
  attach your own handler, look at the [Telemetry page](telemetry.html)
  in the documentation.

  ## Examples

      :ok = Redix.Telemetry.attach_default_handler()

  """
  @spec attach_default_handler() :: :ok | {:error, :already_exists}
  def attach_default_handler() do
    events = [
      [:redix, :disconnection],
      [:redix, :reconnection],
      [:redix, :failed_connection]
    ]

    :telemetry.attach_many("redix-default-telemetry-handler", events, &handle_event/4, :no_config)
  end

  # This function handles only log-related events (disconnections, reconnections, and so on).
  @doc false
  @spec handle_event([atom()], map(), map(), :no_config) :: :ok
  def handle_event([:redix, event], _measurements, metadata, :no_config)
      when event in [:failed_connection, :disconnection, :reconnection] do
    sentinel_address = Map.get(metadata, :sentinel_address)

    case event do
      :failed_connection when is_binary(sentinel_address) ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(metadata.connection)} failed to connect to sentinel " <>
              "at #{sentinel_address}: #{Exception.message(metadata.reason)}"
          end)

      :failed_connection ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(metadata.connection)} failed to connect to Redis " <>
              "at #{metadata.address}: #{Exception.message(metadata.reason)}"
          end)

      :disconnection ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(metadata.connection)} disconnected from Redis " <>
              "at #{metadata.address}: #{Exception.message(metadata.reason)}"
          end)

      :reconnection ->
        _ =
          Logger.info(fn ->
            "Connection #{inspect(metadata.connection)} reconnected to Redis " <>
              "at #{metadata.address}"
          end)
    end
  end
end
