# Redix

[![hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/redix)
[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)][docs]
[![CI](https://github.com/whatyouhide/redix/actions/workflows/main.yml/badge.svg)](https://github.com/whatyouhide/redix/actions/workflows/main.yml)
[![Coverage Status](https://coveralls.io/repos/github/whatyouhide/redix/badge.svg?branch=main)](https://coveralls.io/github/whatyouhide/redix?branch=main)

> Fast, pipelined, resilient Redis client for Elixir.

![DALL·E Golden Retriever](https://github.com/user-attachments/assets/cd7f8c7a-49ba-46e5-8d35-2b0fa371fdb9)

Redix is a [Redis][redis] and [Valkey][valkey] client written in pure Elixir with focus on speed, correctness, and resiliency (that is, being able to automatically reconnect to Redis in case of network errors).

This README refers to the main branch of Redix, not the latest released version on Hex. Make sure to check [the documentation][docs] for the version you're using.

## Features

  * Idiomatic interface for sending commands to Redis
  * Pipelining
  * Resiliency (automatic reconnections)
  * Pub/Sub
  * SSL
  * Redis Sentinel

## Installation

Add the `:redix` dependency to your `mix.exs` file. If you plan on connecting to a Redis server [over SSL][docs-ssl] you may want to add the optional [`:castore`][castore] dependency as well:

```elixir
defp deps do
  [
    {:redix, "~> 1.1"},
    {:castore, ">= 0.0.0"}
  ]
end
```

Then, run `mix deps.get` in your shell to fetch the new dependencies.

## Usage

Redix is simple: it doesn't wrap Redis commands with Elixir functions. It only provides functions to send any Redis command to the Redis server. A Redis *command* is expressed as a list of strings making up the command and its arguments.

Connections are started via `start_link/0,1,2`:

```elixir
{:ok, conn} = Redix.start_link(host: "example.com", port: 5000)
{:ok, conn} = Redix.start_link("redis://localhost:6379/3", name: :redix)
```

Commands can be sent using `Redix.command/2,3`:

```elixir
Redix.command(conn, ["SET", "mykey", "foo"])
#=> {:ok, "OK"}
Redix.command(conn, ["GET", "mykey"])
#=> {:ok, "foo"}
```

Pipelines are just lists of commands sent all at once to Redis for which Redis replies with a list of responses. They can be used in Redix via `Redix.pipeline/2,3`:

```elixir
Redix.pipeline(conn, [["INCR", "foo"], ["INCR", "foo"], ["INCRBY", "foo", "2"]])
#=> {:ok, [1, 2, 4]}
```

`Redix.command/2,3` and `Redix.pipeline/2,3` always return `{:ok, result}` or `{:error, reason}`. If you want to access the result directly and raise in case there's an error, bang! variants are provided:

```elixir
Redix.command!(conn, ["PING"])
#=> "PONG"

Redix.pipeline!(conn, [["SET", "mykey", "foo"], ["GET", "mykey"]])
#=> ["OK", "foo"]
```

#### Resiliency

Redix is resilient against network errors. For example, if the connection to Redis drops, Redix will automatically try to reconnect periodically at a given "backoff" interval. Look at the documentation for the `Redix` module and at the ["Reconnections" page][docs-reconnections] in the documentation for more information on the available options and on the exact reconnection behaviour.

#### Redis Sentinel

Redix supports [Redis Sentinel][redis-sentinel] out of the box. You can specify a list of sentinels to connect to when starting a `Redix` (or `Redix.PubSub`) connection. Every time that connection will need to connect to a Redis server (the first time or after a disconnection), it will try to connect to one of the sentinels in order to ask that sentinel for the current primary or a replica.

```elixir
sentinels = ["redis://sent1.example.com:26379", "redis://sent2.example.com:26379"]
{:ok, primary} = Redix.start_link(sentinel: [sentinels: sentinels, group: "main"])
```

##### Terminology

Redix doesn't support the use of the terms "master" and "slave" that are usually used with Redis Sentinel. I don't think those are good terms to use, period. Instead, Redix uses the terms "primary" and "replica". If you're interested in the discussions around this, [this][redis-terminology-issue] issue in the Redis repository might be interesting to you.

#### Pub/Sub

A `Redix.PubSub` process can be started via `Redix.PubSub.start_link/2`:

```elixir
{:ok, pubsub} = Redix.PubSub.start_link()
```

Most communication with the `Redix.PubSub` process happens via Elixir messages (that simulate a Pub/Sub interaction with the pub/sub server).

```elixir
{:ok, pubsub} = Redix.PubSub.start_link()

Redix.PubSub.subscribe(pubsub, "my_channel", self())
#=> {:ok, ref}
```

Confirmation of subscriptions is delivered as an Elixir message:

```elixir
receive do
  {:redix_pubsub, ^pubsub, ^ref, :subscribed, %{channel: "my_channel"}} -> :ok
end
```

If someone publishes a message on a channel we're subscribed to:

```elixir
receive do
  {:redix_pubsub, ^pubsub, ^ref, :message, %{channel: "my_channel", payload: "hello"}} ->
    IO.puts("Received a message!")
end
```

## Using Redix in the Real World™

Redix is low-level, but it's still built to handle most things thrown at it. For many applications, you can avoid pooling with little to no impact on performance. Read the ["Real world usage" page][docs-real-world-usage] in the documentation for more information on this and pooling strategies that work better with Redix.

## Contributing

To run the Redix test suite you will have to have Redis running locally. Redix requires a somewhat complex setup for running tests (because it needs a few instances running, for pub/sub and sentinel). For this reason, in this repository you'll find a `docker-compose.yml` file so that you can use [Docker][docker] and [docker compose][docker-compose] to spin up all the necessary Redis instances with just one command. Make sure you have Docker installed and then just run:

```bash
docker compose up
```

Now, you're ready to run tests with the `$ mix test` command.

## License

Redix is released under the MIT license. See the [license file](LICENSE.txt).

[docs]: http://hexdocs.pm/redix
[redis]: http://redis.io
[redis-sentinel]: https://redis.io/topics/sentinel
[castore]: https://github.com/ericmj/castore
[docs-ssl]: https://hexdocs.pm/redix/Redix.html#module-ssl
[docs-reconnections]: http://hexdocs.pm/redix/reconnections.html
[docs-real-world-usage]: http://hexdocs.pm/redix/real-world-usage.html
[docker]: https://www.docker.com
[docker-compose]: https://docs.docker.com/compose/
[redis-terminology-issue]: https://github.com/antirez/redis/issues/5335
[valkey]: https://valkey.io/
