defmodule Redix.StartOptions do
  @moduledoc false

  @default_timeout 5_000

  start_link_opts_schema = [
    host: [
      type: {:custom, __MODULE__, :__validate_host__, []},
      doc: """
      the host where the Redis server is running. If you are using a Redis URI, you cannot
      use this option. Defaults to `"localhost`".
      """,
      type_doc: "`t:String.t/0`"
    ],
    port: [
      type: :non_neg_integer,
      doc: """
      the port on which the Redis server is running. If you are using a Redis URI, you cannot
      use this option. Defaults to `6379`.
      """
    ],
    database: [
      type: {:or, [:non_neg_integer, :string]},
      doc: """
      the database to connect to. Defaults to `nil`, meaning Redix doesn't connect to a
      specific database (the default in this case is database `0`). When this option is provided,
      all Redix does is issue a `SELECT` command to Redis in order to select the given database.
      """,
      type_doc: "`t:String.t/0` or `t:non_neg_integer/0`"
    ],
    username: [
      type: {:or, [:string, {:in, [nil]}]},
      doc: """
      the username to connect to Redis. Defaults to `nil`, meaning no username is used.
      Redis supports usernames only since Redis 6 (see the [ACL
      documentation](https://redis.io/topics/acl)). If a username is provided (either via
      options or via URIs) and the Redis version used doesn't support ACL, then Redix falls
      back to using just the password and emits a warning. In future Redix versions, Redix
      will raise if a username is passed and the Redis version used doesn't support ACL.
      """
    ],
    password: [
      type: {:or, [:string, :mfa]},
      type_doc: "`t:Redix.password/0`",
      doc: """
      the password used to connect to Redis. Defaults to
      `nil`, meaning no password is used. When this option is provided, all Redix
      does is issue an `AUTH` command to Redis in order to authenticate. MFAs are also
      supported in the form of `{module, function, arguments}`. This can be used
      to fetch the password dynamically on every reconnection but most importantly to
      hide the password from crash reports in case the Redix connection crashes for
      any reason. For example, you can set this option to:
      `{System, :fetch_env!, ["REDIX_PASSWORD"]}`.
      """
    ],
    timeout: [
      type: :timeout,
      default: @default_timeout,
      doc: """
      connection timeout (in milliseconds) directly passed to the network layer.
      """
    ],
    sync_connect: [
      type: :boolean,
      default: false,
      doc: """
      decides whether Redix should initiate the network connection to the Redis server *before*
      or *after* returning from `start_link/1`. This option also changes some reconnection
      semantics; read the "Reconnections" page in the documentation for more information.
      """
    ],
    exit_on_disconnection: [
      type: :boolean,
      default: false,
      doc: """
      if `true`, the Redix server will exit if it fails to connect or disconnects from Redis.
      Note that setting this option to `true` means that the `:backoff_initial` and
      `:backoff_max` options will be ignored.
      """
    ],
    backoff_initial: [
      type: :non_neg_integer,
      default: 500,
      doc: """
      the initial backoff time (in milliseconds), which is the time that the Redix process
      will wait before attempting to reconnect to Redis after a disconnection or failed first
      connection. See the "Reconnections" page in the docs for more information.
      """
    ],
    backoff_max: [
      type: :timeout,
      default: 30_000,
      doc: """
      the maximum length (in milliseconds) of the time interval used between reconnection
      attempts. See the "Reconnections" page in the docs for more information.
      """
    ],
    ssl: [
      type: :boolean,
      default: false,
      doc: """
      if `true`, connect through SSL, otherwise through TCP. The `:socket_opts` option applies
      to both SSL and TCP, so it can be used for things like certificates. See `:ssl.connect/4`.
      """
    ],
    name: [
      type: :any,
      doc: """
      Redix is bound to the same registration rules as a `GenServer`. See the `GenServer`
      documentation for more information.
      """
    ],
    socket_opts: [
      type: {:list, :any},
      default: [],
      doc: """
      specifies a list of options that are passed to the network layer when connecting to
      the Redis server. Some socket options (like `:active` or `:binary`) will be
      overridden by Redix so that it functions properly.

      If `ssl: true`, then these are added to the default: `[verify: :verify_peer, depth: 3]`.
      If the `CAStore` dependency is available, the `:cacertfile` option is added
      to the SSL options by default as well.
      """
    ],
    hibernate_after: [
      type: :non_neg_integer,
      doc: """
      if present, the Redix connection process awaits any message for the given number
      of milliseconds and if no message is received, the process goes into hibernation
      automatically (by calling `:proc_lib.hibernate/3`). See `t::gen_statem.start_opt/0`.
      Not present by default.
      """
    ],
    spawn_opt: [
      type: :keyword_list,
      doc: """
      if present, its value is passed as options to the Redix connection process as in
      `Process.spawn/4`. See `t::gen_statem.start_opt/0`. Not present by default.
      """
    ],
    debug: [
      type: :keyword_list,
      doc: """
      if present, the corresponding function in the
      [`:sys` module](http://www.erlang.org/doc/man/sys.html) is invoked.
      """
    ],
    fetch_client_id_on_connect: [
      type: :boolean,
      default: false,
      doc: """
      if `true`, Redix will fetch the client ID after connecting to Redis and before
      subscribing to any topic. You can then read the client ID of the pub/sub connection
      with `get_client_id/1`. This option uses the `CLIENT ID` command under the hood,
      which is available since Redis 5.0.0. *This option is available since v1.4.1*.
      """
    ],
    sentinel: [
      type: :keyword_list,
      doc: """
      options to use Redis Sentinel. If this option is present, you cannot use the `:host` and
      `:port` options. See the [*Sentinel Options* section below](#start_link/1-sentinel-options).
      """,
      subsection: "### Sentinel Options",
      keys: [
        sentinels: [
          type: {:custom, __MODULE__, :__validate_sentinels__, []},
          required: true,
          type_doc: "list of `t:String.t/0` or `t:keyword/0`",
          doc: """
          a list of sentinel addresses. Each element in this list is the address
          of a sentinel to be contacted in order to obtain the address of a primary. The address of
          a sentinel can be passed as a Redis URI (see the "Using a Redis URI" section) or
          a keyword list with `:host`, `:port`, `:password` options (same as when connecting to a
          Redis instance directly). Note that the password can either be passed in the sentinel
          address or globally — see the `:password` option below.
          """
        ],
        group: [
          type: :string,
          required: true,
          doc: """
          the name of the group that identifies the primary in the sentinel configuration.
          """
        ],
        role: [
          type: {:in, [:primary, :replica]},
          default: :primary,
          type_doc: "`t:Redix.sentinel_role/0`",
          doc: """
          if `:primary`, the connection will be established
          with the primary for the given group. If `:replica`, Redix will ask the sentinel for all
          the available replicas for the given group and try to connect to one of them
          **at random**.
          """
        ],
        socket_opts: [
          type: :keyword_list,
          default: [],
          doc: """
          socket options for connecting to each sentinel. Same as the `:socket_opts` option
          described above.
          """
        ],
        timeout: [
          type: :timeout,
          default: 500,
          doc: """
          the timeout (in milliseconds or `:infinity`) that will be used to
          interact with the sentinels. This timeout will be used as the timeout when connecting to
          each sentinel and when asking sentinels for a primary. The Redis documentation suggests
          to keep this timeout short so that connection to Redis can happen quickly.
          """
        ],
        ssl: [
          type: :boolean,
          default: false,
          doc: """
          whether to use SSL to connect to each sentinel.
          """
        ],
        password: [
          type: {:or, [:string, :mfa]},
          type_doc: "`t:Redix.password/0`",
          doc: """
          if you don't want to specify a password for each sentinel you
          list, you can use this option to specify a password that will be used to authenticate
          on sentinels if they don't specify a password. This option is recommended over passing
          a password for each sentinel because in the future we might do sentinel auto-discovery,
          which means authentication can only be done through a global password that works for all
          sentinels.
          """
        ]
      ]
    ]
  ]

  @redix_start_link_opts_schema start_link_opts_schema
                                |> Keyword.drop([:fetch_client_id_on_connect])
                                |> NimbleOptions.new!()
  @redix_pubsub_start_link_opts_schema NimbleOptions.new!(start_link_opts_schema)

  @spec options_docs(:redix | :redix_pubsub) :: String.t()
  def options_docs(:redix), do: NimbleOptions.docs(@redix_start_link_opts_schema)
  def options_docs(:redix_pubsub), do: NimbleOptions.docs(@redix_pubsub_start_link_opts_schema)

  @spec sanitize(:redix | :redix_pubsub, keyword()) :: keyword()
  def sanitize(conn_type, options) when is_list(options) do
    schema =
      case conn_type do
        :redix -> @redix_start_link_opts_schema
        :redix_pubsub -> @redix_pubsub_start_link_opts_schema
      end

    options
    |> NimbleOptions.validate!(schema)
    |> maybe_sanitize_sentinel_opts()
    |> maybe_sanitize_host_and_port()
  end

  defp maybe_sanitize_sentinel_opts(options) do
    case Keyword.fetch(options, :sentinel) do
      {:ok, sentinel_opts} ->
        if Keyword.has_key?(options, :host) or Keyword.has_key?(options, :port) do
          raise ArgumentError, ":host or :port can't be passed as option if :sentinel is used"
        end

        sentinel_opts =
          Keyword.update!(
            sentinel_opts,
            :sentinels,
            &Enum.map(&1, fn opts ->
              Keyword.merge(Keyword.take(sentinel_opts, [:password]), opts)
            end)
          )

        Keyword.replace!(options, :sentinel, sentinel_opts)

      :error ->
        options
    end
  end

  defp maybe_sanitize_host_and_port(options) do
    if Keyword.has_key?(options, :sentinel) do
      options
    else
      {host, port} =
        case {Keyword.get(options, :host, "localhost"), Keyword.fetch(options, :port)} do
          {{:local, _unix_socket_path} = host, {:ok, 0}} ->
            {host, 0}

          {{:local, _unix_socket_path}, {:ok, non_zero_port}} ->
            raise ArgumentError,
                  "when using Unix domain sockets, the port must be 0, got: #{inspect(non_zero_port)}"

          {{:local, _unix_socket_path} = host, :error} ->
            {host, 0}

          {host, {:ok, port}} when is_binary(host) ->
            {String.to_charlist(host), port}

          {host, :error} when is_binary(host) ->
            {String.to_charlist(host), 6379}
        end

      Keyword.merge(options, host: host, port: port)
    end
  end

  def __validate_sentinels__([_ | _] = sentinels) do
    sentinels = Enum.map(sentinels, &normalize_sentinel_address/1)
    {:ok, sentinels}
  end

  def __validate_sentinels__([]) do
    {:error, "expected :sentinels to be a non-empty list"}
  end

  def __validate_sentinels__(other) do
    {:error, "expected :sentinels to be a non-empty list, got: #{inspect(other)}"}
  end

  def __validate_host__(host) when is_binary(host) do
    {:ok, host}
  end

  def __validate_host__({:local, path} = value) when is_binary(path) do
    {:ok, value}
  end

  defp normalize_sentinel_address(sentinel_uri) when is_binary(sentinel_uri) do
    sentinel_uri |> Redix.URI.to_start_options() |> normalize_sentinel_address()
  end

  defp normalize_sentinel_address(opts) when is_list(opts) do
    unless opts[:port] do
      raise ArgumentError, "a port should be specified for each sentinel"
    end

    if opts[:host] do
      Keyword.update!(opts, :host, &to_charlist/1)
    else
      raise ArgumentError, "a host should be specified for each sentinel"
    end
  end

  defp normalize_sentinel_address(other) do
    raise ArgumentError,
          "sentinel address should be specified as a URI or a keyword list, got: " <>
            inspect(other)
  end
end
