# Reconnections

Redix tries to be as resilient as possible. When the connection to Redis drops for some reason, a Redix process will try to reconnect to the Redis server.

If there are pending requests to Redix when a disconnection happens, the `Redix` functions will return `{:error, %Redix.ConnectionError{reason: :disconnected}}` to the caller. The caller is responsible to retry the request if interested.

The first reconnection attempts happens after a backoff interval decided by the `:backoff_initial` option. If this attempt succeeds, then Redix will start to function normally again. If this attempt fails, then subsequent reconnection attempts are made until one of them succeeds. The backoff interval between these subsequent reconnection attempts is increased exponentially (with a fixed factor of `1.5`). This means that the first attempt will be made after `n` milliseconds, the second one after `n * 1.5` milliseconds, the third one after `n * 1.5 * 1.5` milliseconds, and so on. Since this growth is exponential, it won't take many attempts before this backoff interval becomes large: because of this, `Redix.start_link/2` also accepts a `:backoff_max` option. which specifies the maximum backoff interval that should be used. The `:backoff_max` option can be used to simulate constant backoff after some exponential backoff attempts: for example, by passing `backoff_max: 5_000` and `backoff_initial: 5_000`, attempts will be made regularly every 5 seconds.

## Synchronous or asynchronous connection

The `:sync_connect` option passed to `Redix.start_link/2` decides whether Redix should initiate the TCP connection to the Redis server *before* or *after* `Redix.start_link/2` returns. This option also changes the behaviour of Redix when the TCP connection can't be initiated at all.

When `:sync_connect` is `false`, then a failed attempt to initially connect to the Redis server is treated exactly as a disconnection: attempts to reconnect are made as described above. This behaviour should be used when Redix is not a vital part of your application: your application should be prepared to handle Redis being down (for example, using the non "bang" variants to issue commands to Redis and handling `{:error, _}` tuples).

When `:sync_connect` is `true`, then a failed attempt to initiate the connection to Redis will cause the Redix process to fail and exit. This might be what you want if Redis is vital to your application.

### If Redis is vital to your application

You should use `sync_connect: true` if Redis is a vital part of your application: for example, if you plan to use a Redix process under your application's supervision tree, placed *before* the parts of your application that depend on it in the tree (so that this way, the application won't be started until a connection to Redis has been established). With `sync_connect: true`, disconnections after the TCP connection has been established will behave exactly as above (with reconnection attempts at given intervals). However, if your application can't function properly without Redix, then you want to use `exit_on_disconnection: true`. With this option, the connection will crash when a disconnection happens. With `:sync_connect` and `:exit_on_disconnection`, you can isolate the part of your application that can't work without Redis under a supervisor and bring that part down when Redix crashes:

```elixir
isolated_children = [
  {Redix, sync_connect: true, exit_on_disconnection: true},
  MyApp.MyGenServer
]

isolated_supervisor = %{
  id: MyChildSupervisor,
  type: :supervisor,
  start: {Supervisor, :start_link, [isolated_children, [strategy: :rest_for_one]]}, 
}

children = [
  MyApp.Child1,
  isolated_supervisor,
  MyApp.Child2
]

Supervisor.start_link(children, strategy: :one_for_one)
```
