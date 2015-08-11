defmodule Rex do
  @type command :: [binary]

  @default_opts [
    host: "localhost",
    port: 6379,
    socket_opts: [],
  ]

  @redis_opts ~w(host port password database)a

  @doc """
  Starts a connection to Redis.

  This function returns `{:ok, pid}` if the connection is successful. The actual
  TCP connection to the Redis server happens asynchronously: when `start_link/1`
  is called, a pid is returned right away and the connection process starts. All
  the calls to Redis that happen during the connection have to wait for the
  connection to be established before being issued.

  This function accepts a single argument: `uri_or_opts`. As the name suggests,
  this argument can either be a Redis URI or a list of options.

  ## URI

  In case `uri_or_opts` is a Redis URI, it must be in the form:

      redis://[:password@]host[:port][/db]

  Here are some examples of valid URIs:

      redis://localhost
      redis://:secret@localhost:6397
      redis://example.com:6380/1

  Usernames before the password are ignored, so the these two URIs are
  equivalent:

      redis://:secret@localhost
      redis://myuser:secret@localhost

  The only mandatory thing when using is the host. All other elements (password,
  port, database) are optional and their default value can be found in the
  "Options" section below.

  ## Options

  The following options can be used to specify the connection parameters
  (instead of a URI as described above):

    * `:host` - (string) the host where the Redis server is running. Defaults to
      `"localhost"`.
    * `:port` - (integer) the port on which the Redis server is
      running. Defaults to `6379`.
    * `:password` - (string) the password used to connect to Redis. Defaults to
      `nil`, meaning no password is used. When this option is provided, all Rex
      does is issue an `AUTH` command to Redis in order to authenticate.
    * `:database` - (integer or string) the database to connect to. Defaults to
      `nil`, meaning don't connect to any database (Redis connects to database
      `0` by default). When this option is provided, all Rex does is issue a
      `SELECT` command to Redis in order to select the given database.

  The following options can be specified in the list of options or as a second
  argument when using a URI (using `start_link/2` instead of `start_link/1`):

    * `:socket_opts` - (list of options) this option specifies a list of options
      that are passed to `:gen_tcp.connect/4` when connecting to the Redis
      server. Some socket options (like `:active` or `:binary`) will be
      overridden by Rex so that it functions properly.

  In addition to these options, all options accepted by `GenServer.start_link/3`
  are forwarded to it. For example, a Rex connection can be registered with a
  name:

      Rex.start_link(name: :rex)
      Process.whereis(:rex)
      #=> #PID<...>

  ## Examples

      iex> Rex.start_link
      {:ok, #PID<...>}

      iex> Rex.start_link(host: "example.com", port: 9999, password: "secret")
      {:ok, #PID<...>}

      iex> Rex.start_link(database: 3, name: :rex_3)
      {:ok, #PID<...>}

  """
  @spec start_link(Keyword.t | binary) :: GenServer.on_start
  def start_link(uri_or_opts \\ [])

  def start_link(uri) when is_binary(uri) do
    uri |> Rex.URI.opts_from_uri |> start_link
  end

  def start_link(opts) do
    {_redis_opts, connection_opts} = Keyword.split(opts, @redis_opts)
    opts = merge_with_default_opts(opts)
    Connection.start_link(Rex.Connection, opts, connection_opts)
  end

  @doc """
  Works like `start_link/1` but accepts a Redis URI *and* a list of options.

  The options you can pass are the ones not about the connection (e.g.,
  `:socket_opts` or `:name`). Read the docs for `start_link/1` for more
  information.

  ## Examples

      iex> Rex.start_link("redis://foo.com/2", name: :rex_foo)
      {:ok, #PID<...>}

  """
  @spec start_link(binary, Keyword.t) :: GenServer.on_start
  def start_link(uri, opts) when is_binary(uri) and is_list(opts) do
    uri |> Rex.URI.opts_from_uri |> Keyword.merge(opts) |> start_link
  end

  @doc """
  Closes the connection to the Redis server.

  This function is asynchronous: it returns `:ok` as soon as it's called and
  performs the closing of the connection after that.

  ## Examples

      iex> Rex.stop(conn)
      :ok

  """
  @spec stop(pid) :: :ok
  def stop(conn) do
    Connection.cast(conn, :stop)
  end

  @doc """
  Issues a command on the Redis server.

  This function sends `command` to the Redis server and returns the response
  returned by Redis. `pid` must be the pid of a Rex connection. `command` must
  be a list of strings making up the Redis command and its arguments.

  ## Examples

      iex> Rex.command(conn, ["SET", "mykey", "foo"])
      "OK"
      iex> Rex.command(conn, ["GET", "mykey"])
      "foo"

  """
  @spec command(pid, command) :: Rex.Protocol.redis_value
  def command(conn, args) do
    Connection.call(conn, {:command, args})
  end

  @doc """
  Issues a pipeline of commands on the Redis server.

  `commands` must be a list of commands, where each command is a list of strings
  making up the command and its arguments. The commands will be sent as a single
  "block" to Redis, and a list of ordered responses (one for each command) will
  be returned.

  ## Examples

      iex> Rex.command(conn, [~w(INCR mykey), ~w(INCR mykey), ~w(DECR mykey)])
      [1, 2, 1]

  """
  @spec pipeline(pid, [command]) :: [Rex.Protocol.redis_value]
  def pipeline(conn, commands) do
    Connection.call(conn, {:pipeline, commands})
  end

  defp merge_with_default_opts(opts) do
    Keyword.merge @default_opts, opts, fn
      _k, default_val, nil ->
        default_val
      _k, _default_val, val ->
        val
    end
  end
end
