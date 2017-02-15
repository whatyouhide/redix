# Reconnections

Redix tries to be as resilient as possible. When the connection to Redis drops
for some reason, a Redix process will try to reconnect to the Redis server.

If there are pending requests to Redix (e.g., `Redix.command/2`s that haven't
returned yet) when a disconnection happens, the `Redix` functions will return
`{:error, %Redix.ConnectionError{reason: :disconnected}}` to the caller. The
caller is responsible to retry the request if interested.

The first reconnection attempts happens after a backoff interval decided by the
`:backoff_initial` option.  If this attempt succeeds, then Redix will start to
function normally again. If this attempt fails, then subsequent reconnection
attempts are made until one of them succeeds. The backoff interval between these
subsequent reconnection attempts is increased exponentially (currently, with a
fixed factor of `1.5`). This means that the first attempt will be made after `n`
milliseconds, the second one after `n * 1.5` milliseconds, the third one after
`n * 1.5 * 1.5` milliseconds, and so on. Since this growth is exponential, it
won't take many attempts before this backoff interval becomes very large: for
this reason, `Redix.start_link/2` also accepts a `:backoff_max` option. This
option specifies the maximum backoff interval (in milliseconds) that should be
used. The `:backoff_max` option can be used to simulate constant backoff after
some exponential backoff attempts: for example, by passing `backoff_max: 5_000`
and `backoff_initial: 5_000`, attempts will be made regularly every 5 seconds.

## Synchronous or asynchronous connection

The `:sync_connect` option passed to `Redix.start_link/2` decides whether Redix
should initiate the TCP connection to the Redis server *before* or *after*
`Redix.start_link/2` returns. This option also changes the behaviour of Redix
when the TCP connection can't be initiated at all.

When `:sync_connect` is `false`, then a failed attempt to initially connect to
the Redis server is treated exactly as a disconnection: attempts to reconnect
are made as described above. This behaviour should be used when Redix is not a
vital part of your application: your application should be prepared to handle
Redis being down (for example, using the non "bang" variants to issue commands
to Redis and handling `{:error, _}`).

When `:sync_connect` is `true`, then a failed attempt to initiate the connection
to Redis will cause the Redix process to fail and exit. This should be used when
Redix is a vital part of your application: for example, this should be used if
you plan to use a Redix process under your application's supervision tree,
placed *before* the parts of your application that depend on it in the tree (so
that this way, the application won't be started until a connection to Redis has
been established). With `sync_connect: true`, disconnections after the TCP
connection has been established will behave exactly as above (with reconnection
attempts at given intervals) and your application should still handle the
possibility of Redis being down. If your application can't function properly
without Redix, then it should fail as soon as Redix reports an error (e.g., by
using "bang" variants of functions, like `Redix.command!/2`). You may feel the
need to be notified as soon as Redis goes down if it's a vital part of your
application: still, it's of little use to be notified right away because your
application will be notified as soon as it interacts with Redix. A possible
middle ground is to periodically `PING` the Redis server, so that errors may be
detected sooner.
