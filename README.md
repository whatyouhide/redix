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

Clients can subscribe arbitrary processes to channels (or patterns) using
`Redix.subscribe/4` (or `Redix.psubscribe/4`). After that, the recipient
processes will receive (Elixir) messages for successful subscriptions, messages
published on subscribed channels, and successful unsubscriptions (through
`Redix.unsubscribe/4` or `Redix.punsubscribe/4`).

Here's an example usage of the PubSub functionality:

```elixir
# We start a regular connection
{:ok, conn} = Redix.start_link

# By calling subscribe/3 we go into "PubSub mode".
:ok = Redix.subscribe(conn, "ch1", self())

# We receive a message for each channel we subscribe to. We are receiving it
# because we passed self() to subscribe/3.
receive do msg -> msg end
#=> {:redix_pubsub, :subscribe, "ch1", 1}

# Calling `command/2` or `pipeline/2` in PubSub mode returns an error.
Redix.command(conn, ["PING"])
{:error, :pubsub_mode}

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

## Sample Poolboy usage

If you are a beginner and want to use this lib in your project it is maybe not
apparent how you use it. This is a small walkthrough on how to use the lib
with a supervisor to get connection pooling wrapped in a public API.

To start with you need to make a module that implements `Supervisor`.
```
defmodule Redix.Poolboy do
  use Supervisor

  @connection_params Application.get_env(:redix, :poolboy)[:server]

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    worker_pool_options = [
      name: {:local, :redix_poolboy},
      worker_module: Redix, #worker process
      size: 10,
      max_overflow: 5
    ]

    children = [
      :poolboy.child_spec(:redix_poolboy, worker_pool_options, @connection_params)
    ]

    opts = [strategy: :one_for_one, name: __MODULE__]

    supervise(children, opts)
  end


  def command(command) do
    :poolboy.transaction(:redix_poolboy, fn(conn) -> Redix.command(conn, command) end)
  end

  def pipeline(command) do
      :poolboy.transaction(:redix_poolboy, fn(conn) -> Redix.pipeline(conn, command) end)
    end
end
```

This module can be put anywhere, but it is suggested to put it in `/lib/redix/redix_poolboy.ex`
or a similar place. After putting the module in place you have to do two more things
before it can be used.

The first thing you need to do is to actually tell your application that this supervisor
should be started. When making a new project with `mix` you will get a `your_app_name.ex`
file placed in the `/lib` folder. In this file you need to add `supervisor(Redix.Poolboy, [])`
inside the `children = []` block.

Secondly you need to configure the server url to connect to. This config follows the exact same
pattern as the configuration described earlier in the readme and must be put under the namespace
`:redix, :poolboy`.

Sample config:
```
config :redix, :poolboy,
  server: [host: "localhost", port: 6379]
```

Do note that this way of configuring is not compatible with umbrella apps or multiple Redis backends.
If you need multiple instances, you need to make multiple instances of the supervisor or to move the config
to the `supervisor()` call.



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
