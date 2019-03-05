# Real-world usage

Redix is a low-level driver, but it's still built to handle most stuff thrown at it.

Redix is built to handle multiple Elixir processes sending commands to Redis through it at the same time. It takes advantage of TCP being a **full-duplex** protocol (bytes are sent in both directions, often at the same time) so that the TCP stream has bytes flowing in both directions (to and from Redis). For example, if two Elixir processes send a `PING` command to Redis via `Redix.command/2`, Redix will send both commands to Redis but will concurrently start listening for the reply to these commands; at a given point, both a `PING` command as well as the `PONG` response to a previous `PING` could be flowing in the TCP stream of the socket that Redix is using.

There's a few different ways to use Redix and to pool connections for better high-load support.

## Single named Redix instance

For many applications, a single global Redix instance is enough. This is true especially for applications where requests to Redis are not mapping one-to-one to things like user requests (that is, a request for each user). A common pattern in these cases is to have a named Redix process started under the supervision tree:

```elixir
children = [
  {Redix, name: :redix}
]
```

Once Redix is started and registered, you can use it with the given name from anywhere:

```elixir
Redix.command(:redix, ["PING"])
#=> {:ok, "PONG"}
```

Note that this pattern extends to more than one global (named) Redix: for example, you could have a Redix process for handling big and infrequent requests and another one to handle short and frequent requests.

## Name-based pool

When you want to have a pool of connections, you can start many connections and register them by name. Say you want to have a pool of five Redis connections. You can start these connections in a supervisor under your supervision tree and then create a wrapper module that calls connections from the pool. The wrapper can use any strategy to choose which connection to use, for example a random strategy.

```elixir
defmodule MyApp.Redix do
  @pool_size 5

  def child_spec(_args) do
    # Specs for the Redix connections.
    children =
      for i <- 0..(@pool_size - 1) do
        Supervisor.child_spec({Redix, name: :"redix_#{i}"}, id: {Redix, i})
      end

    # Spec for the supervisor that will supervise the Redix connections.
    %{
      id: RedixSupervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  def command(command) do
    Redix.command(:"redix_#{random_index()}", command)
  end

  defp random_index() do
    rem(System.unique_integer([:positive]), @pool_size)
  end
end
```

You can then start the Redix connections and their supervisor in the application's supervision tree:

```elixir
def start(_type, _args) do
  children = [
    MyApp.Redix,
    # ...other children
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

And then use the new wrapper in your application:

```elixir
MyApp.Redix.command(["PING"])
#=> {:ok, "PONG"}
```

### Caveats of the name-based pool

The name-based pool works well enough for many use cases but it has a few caveats.

The first one is that the load of requests to Redis is distributed fairly among the connections in the pool, but is not distributed in a "smart" way. For example, you might want to send less requests to connections that are behaving in a worse way, such as slower connections. This avoids bottling up connections that are already slow by sending more requests to them and distributes the load more evenly.

The other caveat is that you need to think about possible race conditions when using this kind of pool since every time you issue a command you could be using a different connections. If you issue commands from the same process, things will work since the process will block until it receives a reply so we know that Redis received and processed the command before we can issue a new one. However, if you issue commands from different processes, you can't be sure of the order that they get processed by Redis. After all, this is often true when doing things from different processes and is not particularly Redix specific.
