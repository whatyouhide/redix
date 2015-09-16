# Redix

[![Build Status](https://travis-ci.org/whatyouhide/redix.svg?branch=v0.1.0)](https://travis-ci.org/whatyouhide/redix)

> Superfast, pipelined, resilient Redis client for Elixir.

Redix is a [Redis][redis] client written in pure Elixir with focus on speed,
correctness and resiliency (that is, being able to automatically reconnect to
Redis in case of network errors).

## Installation

Add the `:redix` dependency to your `mix.exs` file:

```elixir
defp dependencies do
  [{:redix, ">= 0.0.0"}]
end
```

Then run `$ mix deps.get` to install it.

## Why

As for the *why* you would want to use Redix over the more battle-tested redo or
eredis Erlang clients, well, it's pure Elixir :). Also, it appears to be
slightly faster than the above-metioned clients (see the
[section about speed](#speed)). It surely lacks on battle-testing, but we can
make up for that by using it a lot!

## Usage

Redix is very simple in that it doesn't wrap Redis commands with Elixir
functions: it only provides two functions (with their bang! variants),
`command/3` and `pipeline/3`. A Redis *command* is expressed as a list of
strings making up the command and its arguments.

Connections are started via `start_link/0`, `start_link/1` or
`start_link/2`. These functions accept Redix-specific options as well as all the
options accepted by `GenServer.start_link/3` (e.g., `:name` for registering the
connection process under a name).

```elixir
{:ok, conn} = Redix.start_link
{:ok, conn} = Redix.start_link(host: "example.com", port: 5000)
{:ok, conn} = Redix.start_link("redis://localhost:6379/3", name: :redix)
```

Commands can be sent using `Redix.command/2-3`:

```elixir
Redix.command(conn, ~w(SET mykey foo))
#=> {:ok, "OK"}
Redix.command(conn, ~w(GET mykey))
#=> {:ok, "foo"}
```

Pipelines are just lists of commands sent all at once to Redis for which Redis
replies with a list of responses. They can be used in Redix via
`Redix.pipeline/2-3`:

```elixir
Redix.pipeline(conn, [~w(INCR foo), ~w(INCR foo), ~w(INCR foo 2)])
#=> {:ok, [1, 2, 4]}
```

`command/2-3` and `pipeline/2-3` always return `{:ok, result}` or `{:error,
reason}`. If you want to access the result directly and raise in case there's an
error, bang! variants are provided:

```elixir
Redix.command!(conn, ["PING"])
#=> "PONG"

Redix.pipeline!(conn, [~w(SET mykey foo), ~w(GET mykey)])
#=> ["OK", "foo"]
```

A note about Redis errors: in the non-bang functions, they're returned as
`Redix.Error` structs with a `:message` field which contains the original error
message.

```elixir
Redix.command(conn, ~w(FOO))
#=> {:error, %Redix.Error{message: "ERR unknown command 'FOO'"}}

# pipeline/2 returns {:ok, _} instead of {:error, _} even if there are errors in
# the list of responses so that it doesn't have to walk the entire list of
# responses.
Redix.pipeline(conn, [~w(PING), ~w(FOO)])
#=> {:ok, ["PONG", %Redix.Error{message: "ERR unknown command 'FOO'"}]}
```

In `command!/2-3`, they're raised as exceptions:

```elixir
Redix.command!(conn, ~w(FOO))
#=> ** (Redix.Error) ERR unknown command 'FOO'
```

`command!/2-3` and `pipeline!/2-3` raise `Redix.ConnectionError` in case there's an
error related to the Redis connection (e.g., the connection is closed while
Redix is waiting to reconnect).

Transactions are supported naturally as they're just made up of commands:

```elixir
Redix.command(conn, ~w(MULTI))
#=> {:ok, "OK"}
Redix.command(conn, ~w(INCR counter))
#=> {:ok, "QUEUED"}
Redix.command(conn, ~w(INCR counter))
#=> {:ok, "QUEUED"}
Redix.command(conn, ~w(EXEC))
#=> {:ok, [1, 2]}
```

#### PubSub

Redix supports the [PubSub functionality][redis-pubsub] provided by Redis.

PubSub connections are different than regular Redix connections and have to be
started using `Redix.PubSub.start_link/1-2`. Clients can then subscribe processes to channels (or patterns) using `Redix.PubSub.subscribe/4` (or `Redix.PubSub.psubscribe/4`). After that, recipient processes will receive (Elixir) messages for:

  * successful subscriptions
  * messages published on subscribed channels or patterns
  * successful unsubscriptions (`Redix.PubSub.unsubscribe/4` and `Redix.PubSub.punsubscribe/4`)
  * disconnections and reconnections of the PubSub client

Here's an example usage of the PubSub functionality:

```elixir
{:ok, conn} = Redix.PubSub.start_link

:ok = Redix.PubSub.subscribe(conn, "ch1", self())

# We receive a message for each channel we subscribe to. We are receiving it
# because we passed self() to subscribe/3.
receive do msg -> msg end
#=> {:redix_pubsub, :subscribe, "ch1", 1}

{:ok, other_conn} = Redix.start_link
{:ok, _} = Redix.command(other_conn, ~w(PUBLISH ch1 hello_world))

receive do msg -> msg end
#=> {:redix_pubsub, :message, "hello_world", "ch1"}
```

#### Resiliency

Redix takes full advantage of the [connection][connection] library by James
Fish to provide a resilient behaviour when dealing with the network connection
to Redis. For example, if the connection to Redis drops, Redix will
automatically try to reconnect to it periodically at a given "backoff" interval
(which is configurable). Look at the documentation for the `Redix` module for
more information on the available options and on the exact behaviour.

## Speed

I'm by no means a benchmarking expert, but I ran some benchmarks (out of
curiosity) against Redis clients for Erlang like [redo][redo] and
[eredis][eredis]. You can find these benchmarks in the `./bench` directory. I
used [benchfella][benchfella] to make them.

It appears from the benchmarks that:

  * Redix is roughly as fast as redo and eredis (with a < 10% margin) for single
    commands sent to Redis.
  * Redix is slightly faster than redo and eredis when sending a few pipelined
    commands (a few means 5 in the benchmarks), with a margin of 15%-20%.
  * Redix is substantially faster than eredis (~35% faster) and redo (~300%
    faster) when sending a lot of pipelined commands (where "a lot" means 10k
    commands).

For now, I'm quite happy with these benchmarks and hope we can make them even
better in the future.

## Using Redix in the Real World™

Redix is "low level" in some ways as it tries to be as close to the Redis API as
possible and at the same time remain as flexible as it can. A Redix connection
is basically just a GenServer, so it fits nicely in OTP supervision trees and
the such. Most of the times you will want to supervise Redix connections and
often to use a pool of them: for example, if you have a web application that
uses Redis, you wouldn't want to connect to Redis each time a new request
arrives to the server.

This section provides some sample code to serve as an example for how to
supervise a pool of Redix connections. Pooling will be done using the very
famous [poolboy][poolboy] Erlang library.

A simple way to do this is create a supervisor for the poolboy pool that we can
later start when we start our application. The code for this supervisor would
look something like this.

```elixir
defmodule MyApp.RedixPool do
  use Supervisor

  @redis_connection_params host: "example.com", password: "secret"

  def start_link do
    Supervisor.start_link(__MODULE__, [])
  end

  def init([]) do
    pool_opts = [
      name: {:local, :redix_poolboy},
      worker_module: Redix,
      size: 10,
      max_overflow: 5,
    ]

    children = [
      :poolboy.child_spec(:redix_poolboy, pool_opts, @redis_connection_params)
    ]

    supervise(children, strategy: :one_for_one, name: __MODULE__)
  end

  def command(command) do
    :poolboy.transaction(:redix_poolboy, &Redix.command(&1, command))
  end

  def pipeline(commands) do
    :poolboy.transaction(:redix_poolboy, &Redix.pipeline(&1, commands))
  end
end
```

Using this is straightforward:

```elixir
MyApp.RedixPool.command(~w(PING)) #=> {:ok, "PONG"}
```

Now, all you need to do to is add the `MyApp.RedixPool` supervisor to the
supervision tree of your application (using something like
`supervisor(MyApp.RedixPool, [])` in the list of children to supervise).

Just as a short aside, note that you could also use `:poolboy.child_spec/3`
directly in the list of supervised children of your application, without
creating a dedicated supervisor for the pool of Redix connection. Here, we did
this for clarity but both strategies make sense in different scenarios.

## Contributing

Clone the repository and run `$ mix test` to make sure everything is
working. For tests to pass, you must have a Redis server running on `localhost`,
port `6379`.

## License

MIT &copy; 2015 Andrea Leopardi, see the [license file](LICENSE.txt).


[redis]: http://redis.io
[redis-pubsub]: http://redis.io/topics/pubsub
[connection]: https://github.com/fishcakez/connection
[redo]: https://github.com/heroku/redo
[eredis]: https://github.com/wooga/eredis
[benchfella]: https://github.com/alco/benchfella
[poolboy]: https://github.com/devinus/poolboy
