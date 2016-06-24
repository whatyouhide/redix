# Redix

[![Build Status](https://travis-ci.org/whatyouhide/redix.svg?branch=master)](https://travis-ci.org/whatyouhide/redix)

> Superfast, pipelined, resilient Redis client for Elixir.

![](http://i.imgur.com/ZG2RXsb.png)

Redix is a [Redis][redis] client written in pure Elixir with focus on speed,
correctness and resiliency (that is, being able to automatically reconnect to
Redis in case of network errors).

Note that this README refers to the `master` branch of Redix, not the latest
released version on Hex. See [the documentation](http://hexdocs.pm/redix) for
the documentation of the version you're using.

## Installation

Add the `:redix` dependency to your `mix.exs` file:

```elixir
defp deps() do
  [{:redix, ">= 0.0.0"}]
end
```

and add `:redix` to your list of applications:

```elixir
defp application() do
  [applications: [:logger, :redix]]
end
```

Then, run `mix deps.get` in your shell to fetch the new dependency.

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
{:ok, conn} = Redix.start_link()
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
`Redix.pipeline/2,3`:

```elixir
Redix.pipeline(conn, [~w(INCR foo), ~w(INCR foo), ~w(INCRBY foo 2)])
#=> {:ok, [1, 2, 4]}
```

`Redix.command/2,3` and `Redix.pipeline/2,3` always return `{:ok, result}` or
`{:error, reason}`. If you want to access the result directly and raise in case
there's an error, bang! variants are provided:

```elixir
Redix.command!(conn, ~w(PING))
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

In `Redix.command!/2,3`, Redis errors are raised as exceptions:

```elixir
Redix.command!(conn, ~w(FOO))
#=> ** (Redix.Error) ERR unknown command 'FOO'
```

`Redix.command!/2,3` and `Redix.pipeline!/2,3` raise `Redix.ConnectionError` in
case there's an error related to the Redis connection (e.g., the connection is
closed while Redix is waiting to reconnect).

#### Resiliency

Redix takes full advantage of the [connection][connection] library by James
Fish to provide a resilient behaviour when dealing with the network connection
to Redis. For example, if the connection to Redis drops, Redix will
automatically try to reconnect to it periodically at a given "backoff" interval
(which is configurable). Look at the documentation for the `Redix` module and to
the ["Reconnections" page][docs-reconnections] in the documentation for more
information on the available options and on the exact behaviour regarding
reconnections.

#### Pub/Sub

Redix doesn't support the Pub/Sub features of Redis. For that, there's
[`redix_pubsub`][redix-pubsub] :).

## Using Redix in the Real World™

Redix is low-level, but it's still built to handle most things thrown at
it. Most people tend to use pooling with Redix (through something like
[poolboy][poolboy]), but for many applications, that can be avoided with little
to no impact on performance. Read the
["Real world usage" page][docs-real-world-usage] in the documentation for more
information on this.

## Contributing

Clone the repository and run `$ mix test` to make sure everything is
working. For tests to pass, you must have a Redis server running on `localhost`,
port `6379`. Both may be configured using the environment variables `REDIX_TEST_HOST` and
`REDIX_TEST_PORT` respectively. Tests will wipe clean all the databases on the running Redis
server, as they call `FLUSHALL` multiple times, so *be careful*.

## License

MIT &copy; 2015 Andrea Leopardi, see the [license file](LICENSE.txt).


[redis]: http://redis.io
[connection]: https://github.com/fishcakez/connection
[poolboy]: https://github.com/devinus/poolboy
[redix-pubsub]: https://github.com/whatyouhide/redix_pubsub
[docs-reconnections]: http://hexdocs.pm/redix/reconnections.html
[docs-real-world-usage]: http://hexdocs.pm/redix/real-world-usage.html
