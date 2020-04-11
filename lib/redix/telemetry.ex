defmodule Redix.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  Redix connections (both `Redix` and `Redix.PubSub`) execute the
  following Telemetry events:

    * `[:redix, :connection]` - executed when a Redix connection establishes the
      connection to Redis. There are no measurements associated with this event.
      Metadata are:

      * `:address` - the address the connection successfully connected to.
      * `:connection` - the PID or registered name of the Redix connection
          that emitted the event.
      * `:reconnection` - a boolean that specifies whether this was a first
        connection to Redis or a reconnection after a disconnection. This can
        be useful for more granular logging.
      * `:extra` - whatever is passed in `:telemetry_extra` when the connection
        is started.

    * `[:redix, :disconnection]` - executed when the connection is lost
      with the Redis server. There are no measurements associated with
      this event. Metadata are:

      * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.
      * `:address` - the address the connection was connected to.
      * `:connection` - the PID or registered name of the Redix connection
        that emitted the event.
      * `:extra` - whatever is passed in `:telemetry_extra` when the connection
        is started.

    * `[:redix, :failed_connection]` - executed when Redix can't connect to
      the specified Redis server, either when starting up the connection or
      after a disconnection. There are no measurements associated with this event.
      Metadata are:

      * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.
      * `:address` or `:sentinel_address` - the address the connection was trying
        to connect to (either a Redis server or a Redis Sentinel instance).
      * `:connection` - the PID or registered name of the Redix connection
        that emitted the event.
      * `:extra` - whatever is passed in `:telemetry_extra` when the connection
        is started.

  `Redix` connections execute the following Telemetry events when commands or
  pipelines of any kind are executed.

    * `[:redix, :pipeline, :start]` - executed right before a pipeline (or command,
      which is a pipeline with just one command) gets send to the Redis server.
      Measurements are:

      * `:system_time` (integer) - the system time (in the `:native` time unit)
        at the time the event is emitted. See `System.system_time/0`.

      Metadata are:

      * `:connection` - the connection that emitted the event. If the connection
        was registered with a name, the name is used here, otherwise the PID.
      * `:commands` - the commands sent to the server. This is always a list of
        commands, so even if you do `Redix.command(conn, ["PING"])` than the
        list of commands will be `[["PING"]]`.

    * `[:redix, :pipeline, :stop]` - executed a response to a pipeline returns
      from the Redis server, regardless of whether it's an error response or a
      successful response. Measurements are:

      * `:duration` - the duration (in the `:native` time unit, see `t:System.time_unit/0`)
        of back-and-forth between client and server.

      Metadata are:

      * `:connection` - the connection that emitted the event. If the connection
        was registered with a name, the name is used here, otherwise the PID.
      * `:commands` - the commands sent to the server. This is always a list of
        commands, so even if you do `Redix.command(conn, ["PING"])` than the list
        of commands will be `[["PING"]]`.

      If the response is an error, the following metadata will also be present:

      * `:kind` - this is the atom `:error`
      * `:reason` - the error reason (such as a `Redix.ConnectionError` struct).

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
    * `[:redix, :connection]` - logged at the `:info` level if it's a
      reconnection, not logged if it's the first connection.

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
      [:redix, :connection],
      [:redix, :failed_connection]
    ]

    :telemetry.attach_many("redix-default-telemetry-handler", events, &handle_event/4, :no_config)
  end

  # This function handles only log-related events (disconnections, reconnections, and so on).
  @doc false
  @spec handle_event([atom()], map(), map(), :no_config) :: :ok
  def handle_event([:redix, event], _measurements, metadata, :no_config)
      when event in [:failed_connection, :disconnection, :connection] do
    case {event, metadata} do
      {:failed_connection, %{sentinel_address: sentinel_address}}
      when is_binary(sentinel_address) ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(metadata.connection)} failed to connect to sentinel " <>
              "at #{sentinel_address}: #{Exception.message(metadata.reason)}"
          end)

      {:failed_connection, _metadata} ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(metadata.connection)} failed to connect to Redis " <>
              "at #{metadata.address}: #{Exception.message(metadata.reason)}"
          end)

      {:disconnection, _metadata} ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(metadata.connection)} disconnected from Redis " <>
              "at #{metadata.address}: #{Exception.message(metadata.reason)}"
          end)

      {:connection, %{reconnection: true}} ->
        _ =
          Logger.info(fn ->
            "Connection #{inspect(metadata.connection)} reconnected to Redis " <>
              "at #{metadata.address}"
          end)

      {:connection, %{reconnection: false}} ->
        :ok
    end
  end
end
