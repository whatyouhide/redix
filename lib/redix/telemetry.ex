defmodule Redix.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  Redix connections (both `Redix` and `Redix.PubSub`) execute the
  following Telemetry events:

    * `[:redix, :connection]` - executed when a Redix connection establishes the
      connection to Redis. There are no measurements associated with this event.
      Metadata are:

      * `:connection` - the PID of the Redix connection that emitted the event.
      * `:connection_name` - the name (passed to the `:name` option when the
      connection is started) of the Redix connection that emitted the event.
      `nil` if the connection was not registered with a name.
      * `:address` - the address the connection successfully connected to.
      * `:reconnection` - a boolean that specifies whether this was a first
        connection to Redis or a reconnection after a disconnection. This can
        be useful for more granular logging.

    * `[:redix, :disconnection]` - executed when the connection is lost
      with the Redis server. There are no measurements associated with
      this event. Metadata are:

      * `:connection` - the PID of the Redix connection that emitted the event.
      * `:connection_name` - the name (passed to the `:name` option when the
      * `:address` - the address the connection was connected to.
        connection is started) of the Redix connection that emitted the event.
        `nil` if the connection was not registered with a name.
      * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.

    * `[:redix, :failed_connection]` - executed when Redix can't connect to
      the specified Redis server, either when starting up the connection or
      after a disconnection. There are no measurements associated with this event.
      Metadata are:

      * `:connection` - the PID of the Redix connection that emitted the event.
      * `:connection_name` - the name (passed to the `:name` option when the
        connection is started) of the Redix connection that emitted the event.
        `nil` if the connection was not registered with a name.
      * `:address` or `:sentinel_address` - the address the connection was trying
      to connect to (either a Redis server or a Redis Sentinel instance).
      * `:reason` - the disconnection reason as a `Redix.ConnectionError` struct.

  `Redix` connections execute the following Telemetry events when commands or
  pipelines of any kind are executed.

    * `[:redix, :pipeline, :start]` - executed right before a pipeline (or command,
      which is a pipeline with just one command) is sent to the Redis server.
      Measurements are:

      * `:system_time` (integer) - the system time (in the `:native` time unit)
        at the time the event is emitted. See `System.system_time/0`.

      Metadata are:

      * `:connection` - the PID of the Redix connection used to send the pipeline.
      * `:connection_name` - the name of the Redix connection used to sent the pipeline.
        This is `nil` if the connection was not registered with a name or if the
        pipeline function was called with a PID directly (for example, if you did
        `Process.whereis/1` manually).
      * `:commands` - the commands sent to the server. This is always a list of
        commands, so even if you do `Redix.command(conn, ["PING"])` then the
        list of commands will be `[["PING"]]`.
      * `:extra_metadata` - any term set by users via the `:telemetry_metadata` option
        in `Redix.pipeline/3` and other functions.

    * `[:redix, :pipeline, :stop]` - executed a response to a pipeline returns
      from the Redis server, regardless of whether it's an error response or a
      successful response. Measurements are:

      * `:duration` - the duration (in the `:native` time unit, see `t:System.time_unit/0`)
        of back-and-forth between client and server.

      Metadata are:

      * `:connection` - the PID of the Redix connection used to send the pipeline.
      * `:connection_name` - the name of the Redix connection used to sent the pipeline.
        This is `nil` if the connection was not registered with a name or if the
        pipeline function was called with a PID directly (for example, if you did
        `Process.whereis/1` manually).
      * `:commands` - the commands sent to the server. This is always a list of
        commands, so even if you do `Redix.command(conn, ["PING"])` then the
        list of commands will be `[["PING"]]`.
      * `:extra_metadata` - any term set by users via the `:telemetry_metadata` option
        in `Redix.pipeline/3` and other functions.

      If the response is an error, the following metadata will also be present:

      * `:kind` - the atom `:error`.
      * `:reason` - the error reason (such as a `Redix.ConnectionError` struct).

  ## Cluster events

  `Redix.Cluster` connections execute the following Telemetry events:

    * `[:redix, :cluster, :topology_change]` - executed when the cluster topology
      is successfully refreshed. There are no measurements. Metadata are:

      * `:cluster` - the name of the cluster (the atom passed as `:name`).
      * `:nodes` - the list of primary node addresses (as `"host:port"` strings).

    * `[:redix, :cluster, :failed_topology_refresh]` - executed when the cluster
      manager fails to refresh the topology (no reachable node). There are no
      measurements. Metadata are:

      * `:cluster` - the name of the cluster.
      * `:reason` - the error reason (such as `:no_reachable_node`).

    * `[:redix, :cluster, :node_connection_failed]` - executed when the cluster
      manager fails to establish a connection to a specific node. There are no
      measurements. Metadata are:

      * `:cluster` - the name of the cluster.
      * `:address` - the node address (as a `"host:port"` string).
      * `:reason` - the error reason.

    * `[:redix, :cluster, :redirection]` - executed when a command receives a
      `MOVED` or `ASK` redirection from a cluster node. There are no measurements.
      Metadata are:

      * `:cluster` - the name of the cluster.
      * `:type` - either `:moved` or `:ask`.
      * `:slot` - the hash slot being redirected.
      * `:target_address` - the target node address (as a `"host:port"` string).

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
    * `[:redix, :cluster, :failed_topology_refresh]` - logged at the `:error` level
    * `[:redix, :cluster, :node_connection_failed]` - logged at the `:warning` level
    * `[:redix, :cluster, :redirection]` - logged at the `:info` level

  See the module documentation for more information. If you want to
  attach your own handler, look at the [Telemetry page](telemetry.html)
  in the documentation.

  ## Examples

      :ok = Redix.Telemetry.attach_default_handler()

  """
  @spec attach_default_handler() :: :ok | {:error, :already_exists}
  def attach_default_handler do
    events = [
      [:redix, :disconnection],
      [:redix, :connection],
      [:redix, :failed_connection],
      [:redix, :cluster, :topology_change],
      [:redix, :cluster, :failed_topology_refresh],
      [:redix, :cluster, :node_connection_failed],
      [:redix, :cluster, :redirection]
    ]

    :telemetry.attach_many(
      "redix-default-telemetry-handler",
      events,
      &__MODULE__.handle_event/4,
      :no_config
    )
  end

  # This function handles only log-related events (disconnections, reconnections, and so on).
  @doc false
  @spec handle_event([atom()], map(), map(), :no_config) :: :ok
  def handle_event([:redix, event], _measurements, metadata, :no_config)
      when event in [:failed_connection, :disconnection, :connection] do
    connection_name = metadata.connection_name || metadata.connection

    case {event, metadata} do
      {:failed_connection, %{sentinel_address: sentinel_address}}
      when is_binary(sentinel_address) ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(connection_name)} failed to connect to sentinel " <>
              "at #{sentinel_address}: #{Exception.message(metadata.reason)}"
          end)

      {:failed_connection, _metadata} ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(connection_name)} failed to connect to Redis " <>
              "at #{metadata.address}: #{Exception.message(metadata.reason)}"
          end)

      {:disconnection, _metadata} ->
        _ =
          Logger.error(fn ->
            "Connection #{inspect(connection_name)} disconnected from Redis " <>
              "at #{metadata.address}: #{Exception.message(metadata.reason)}"
          end)

      {:connection, %{reconnection: true}} ->
        _ =
          Logger.info(fn ->
            "Connection #{inspect(connection_name)} reconnected to Redis " <>
              "at #{metadata.address}"
          end)

      {:connection, %{reconnection: false}} ->
        :ok
    end
  end

  def handle_event([:redix, :cluster, event], _measurements, metadata, :no_config) do
    cluster = metadata.cluster

    case event do
      :topology_change ->
        :ok

      :failed_topology_refresh ->
        _ =
          Logger.error(fn ->
            "Cluster #{inspect(cluster)} failed to refresh topology: #{inspect(metadata.reason)}"
          end)

      :node_connection_failed ->
        _ =
          Logger.warning(fn ->
            "Cluster #{inspect(cluster)} failed to connect to node " <>
              "#{metadata.address}: #{inspect(metadata.reason)}"
          end)

      :redirection ->
        _ =
          Logger.info(fn ->
            "Cluster #{inspect(cluster)} #{metadata.type} redirection " <>
              "for slot #{metadata.slot} to #{metadata.target_address}"
          end)
    end
  end
end
