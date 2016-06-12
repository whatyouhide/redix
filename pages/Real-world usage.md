# Real-world usage

Redix is a low-level driver, but it's still built to handle most stuff thrown at it.

There's a tendency to use pooling when using Redix (through something like
[poolboy][poolboy]). This makes Redix more "scalable" (e.g., the size of the
pool is configurable), but it doesn't really take advantage of Redix.

Redix is built to handle multiple Elixir processes sending commands to Redis
through it at the same time. It takes advantage of TCP being a **full-duplex**
protocol (bytes are sent in both directions, often at the same time) so that the
TCP stream has bytes flowing in both directions (to and from Redis). For
example, if two Elixir processes send a `PING` command to Redis via
`Redix.command/2`, Redix will send both commands to Redis but will concurrently
start listening for the reply to these commands; at a given point, both a `PING`
command as well as the `PONG` response to a previous `PING` could be flowing in
the TCP stream of the socket that Redix is using.

When pooling with something like `:poolboy`, the pattern is to have a pool of
Redixes that can be checked out and back in from the pool when needed:

```elixir
:poolboy.checkout(:my_redix_pool, fn(redix_pid) ->
  Redix.command(redix_pid, ~w(PING))
end)
```

In the example above, we only use the `redix_pid` we check out of the pool from
the process that checks it out of the pool: as stated earlier, this means we're
not taking advantage of the concurrency that Redix provides.

## Global Redix

For many applications, a single global Redix instance is enough (especially if
they're not web applications that hit Redis on every request). A common pattern
is to have a named Redix process started under the supervision tree:

```elixir
children = [
  worker(Redix, [[], [name: :redix]]),
  # ...
]
```

and then called globally (e.g., `Redix.command(:redix, ~w(PING))`).

Note that this pattern extends to more than one global (named) Redix: for
example, you could have a Redix process for handling big and infrequent requests
and another one to handle short and frequent requests.

## Pooling strategies

A common pattern is to hit Redis on each request in web applications. Instead of
using something like poolboy to handle pooling in such cases, a strategy I
personally like is to have a "manual" pool of Redix processes available, much
smaller than the "usual" poolboy pool. A strategy to do this is to have `n`
named Redix processes, and to distribute the load between the Redixes using
their name.

For example, we can start five Redix processes under our supervision tree and
name them `:redix_0` to `:redix_4`:

```elixir
# Create the redix children list of workers:
pool_size = 5
redix_workers = for i <- 0..(pool_size - 1) do
  worker(Redix, [[], [name: :"redix_#{i}"]], id: {Redix, i})
end
```

Then, we can build a simple wrapper module around Redix which will dispatch to
one of the five Redix processes (with whatever strategy makes sense, e.g.,
randomly):

```elixir
defmodule MyApp.Redix do
  def command(command) do
    Redix.command(:"redix_#{random_index()}", command)
  end

  defp random_index() do
    rem(System.unique_integer([:positive]), 5)
  end
end
```

And then to use the new wrapper in your appplication:

```elixir
MyApp.Redix.command(~w(PING))
```
