# Changelog

## v0.10.4

### Bug fixes and improvements

  * Fix the default Telemetry handler for Redis Sentinel events (wasn't properly fixed in v0.10.3).
  * Fix a compile-time warning about the [castore](https://github.com/elixir-mint/castore) library.

## v0.10.3

### Bug fixes and improvements

  * Use more secure SSL default options and optionally use [castore](https://github.com/elixir-mint/castore) if available as a certificate store.
  * Fix the default Telemetry handler for Redis Sentinel events.

## v0.10.2

### Bug fixes and improvements

  * Allow a discarded username when using Redis URIs.
  * Fix the `Redix.command/0` type which was `[binary()]` but which should have been `[String.Chars.t()]` since we call `to_string/1` on each command.

## v0.10.1

### Bug fixes and improvements

  * Improve password checking in Redis URIs.
  * Fix a bug when naming Redix connections with something other than a local name.

## v0.10.0

### Bug fixes and improvements

  * Add support for [Telemetry](https://github.com/beam-telemetry/telemetry) and publish the following events: `[:redix, :pipeline]`, `[:redix, :pipeline, :error]`, `[:redix, :disconnection]`, `[:redix, :reconnection]`, `[:redix, failed_connection]`.
  * Deprecate the `:log` option in `Redix.start_link/1` and `Redix.PubSub.start_link/1` in favour of Telemetry events and a default log handler that can be activated with `Redix.Telemetry.attach_default_handler/0`. See the documentation for [`Redix.Telemetry`](https://hexdocs.pm/redix/0.10.0/Redix.Telemetry.html). This is a hard deprecation that shows a warning. Support for the `:log` option will be removed in the next version.
  * Fix a few minor bugs in `Redix.PubSub`.

## v0.9.3

### Bug fixes and improvements

  * Fix a bug related to quickly reconnecting PIDs in `Redix.PubSub`.
  * Improve error messages here and there.

## v0.9.2

### Bug fixes and improvements

  * Add support for URLs with the `rediss` scheme.
  * Fix a bug where we used the wrong logging level in some places.

## v0.9.1

### Bug fixes and improvements

  * Fix a bad return type from a `gen_statem` callback (#120).
  * Improve logging for Redis Sentinel.

## v0.9.0

### Breaking changes

  * Bring `Redix.PubSub` into Redix. Pub/Sub functionality lived in a separate library, [redix_pubsub](https://github.com/whatyouhide/redix_pubsub). Now, that functionality has been moved into Redix. This means that if you use redix_pubsub and upgrade your Redix version to 0.9, you will use the redix_pubsub version of `Redix.PubSub`. If you also upgrade your redix_pubsub version, redix_pubsub will warn and avoid compiling `Redix.PubSub` so you can use the latest version in Redix. In general, if you upgrade Redix to 0.9 or later just drop the redix_pubsub dependency and make sure your application works with the latest `Redix.PubSub` API (the message format changed slightly in recent versions).

  * Add support for Redis Sentinel.

  * Don't raise `Redix.Error` errors on non-bang variants of functions. This means that for example `Redix.command/3` won't raise a `Redix.Error` exception in case of Redis errors (like wront typing) and will return that error instead. In general, if you're pattern matching on `{:error, _}` to handle **connection errors** (for example, to retry after a while), now specifically match on `{:error, %Redix.ConnectionError{}}`. If you want to handle all possible errors the same way, keep matching on `{:error, _}`.

### Bug fixes and improvements

  * Fix a bug that wouldn't let you use Redis URIs without host or port.
  * Don't ignore the `:timeout` option when connecting to Redis.

## v0.8.2

### Bug fixes and improvements

  * Fix an error when setting up SSL buffers (#106).

## v0.8.1

### Bug fixes and improvements

  * Re-introduce `start_link/2` with two lists of options, but deprecate it. It will be removed in the next Redix version.

## v0.8.0

### Breaking changes

  * Drop support for Elixir < 1.6.

  * Unify `start_link` options: there's no more separation between "Redis options" and "connection options". Now, all the options are passed in together. You can still pass a Redis URI as the first argument. This is a breaking change because now calling `start_link/2` with two kewyord lists breaks. Note that `start_link/2` with two keyword lists still works, but emits a warning and is deprecated.

### Bug fixes and improvements

  * Rewrite the connection using [`gen_statem`](http://erlang.org/doc/man/gen_statem.html) in order to drop the dependency to [Connection](https://github.com/fishcakez/connection).

  * Add `Redix.transaction_pipeline/3` and `Redix.transaction_pipeline!/3`.

  * Use a timeout when connecting to Redis (which sometimes could get stuck).

  * Add support for SSL 🔐

  * Add `Redix.noreply_command/3` and `Redix.noreply_pipeline/3` (plus their bang `!` variants).

## v0.7.1

  * Add support for Unix domain sockets by passing `host: {:local, path}`.

## v0.7.0

### Breaking changes

  * Drop support for Elixir < 1.3.

  * Remove `Redix.format_error/1`.

### Bug fixes and improvements

  * Add `Redix.child_spec/1` for use with the child spec changes in Elixir 1.5.

## v0.6.1

  * Fix some deprecation warnings around `String.to_char_list/1`.

## v0.6.0

### Breaking changes

  * Start using `Redix.ConnectionError` when returning errors instead of just an atom. This is a breaking change since now `Redix.command/2` and the other functions return `{:error, %Redix.ConnectionError{reason: reason}}` instead of `{:error, reason}`. If you're matching on specific error reasons, make sure to update your code; if you're formatting errors through `Redix.format_error/1`, you can now use `Exception.message/1` on the `Redix.ConnectionError` structs.

## v0.5.2

  * Fix some TCP error handling during the connection setup phase.

## v0.5.1

  * Fix `Redix.stop/1` to be synchronous and not leave zombie processes.

## v0.5.0

  * Drop support for Elixir < 1.2 and OTP 17 or earlier.

## v0.4.0

  * Add [@lexmag](https://github.com/lexmag) to the maintainers :tada:

  * Handle timeouts nicely by returning `{:error, :timeout}` instead of exiting (which is the default `GenServer` behaviour).

  * Remove support for specifying a maximum number of reconnection attempts when connecting to Redis (it was the `:max_reconnection_attempts` option).

  * Use exponential backoff when reconnecting.

  * Don't reconnect right away after the connection to Redis is lost, but wait for a cooldown time first.

  * Add support for `:backoff_initial` and `:backoff_max` options in `Redix.start_link/2`. These options are used for controlling the backoff behaviour of a `Redix` connection.

  * Add support for the `:sync_connect` option when connecting to Redis.

  * Add support for the `:exit_on_disconnection` option when connecting to Redis.

  * Add support for the `:log` option when connecting to Redis.

  * Raise `ArgumentError` exceptions instead of `Redix.ConnectionError` exceptions for stuff like empty commands.

  * Raise `Redix.Error` exceptions from `Redix.command/3` instead of returning them wrapped in `{:error, _}`.

  * Expose `Redix.format_error/1`.

  * Add a "Reconnections" page in the documentation.

  * Extract the Pub/Sub functionality into [a separate project](https://github.com/whatyouhide/redix_pubsub).


## v0.3.6

  * Fixed a bug in the integer parsing in `Redix.Protocol`.

## v0.3.5

  * `Redix.Protocol` now uses continuations under the hood for a faster parsing experience.

  * A bug in `Redix.Protocol` that caused massive memory leaks was fixed. This bug originated upstream in Elixir itself, and I submitted a fix for it [here](https://github.com/elixir-lang/elixir/pull/4350).

  * Some improvements were made to error reporting in the Redix logging.

## v0.3.4

  * Fix a bug in the connection that was replacing the provided Redis password with `:redacted` upon successful connection, making it impossible to reconnect in case of failure (because of the original password now being unavailable).

## v0.3.3

  * Fix basically the same bug that was almost fixed in `v0.3.2`, but this time for real!

## v0.3.2

  * Fix a bug in the protocol that failed to parse integers in some cases.

## v0.3.1

  * Restructure the Redix architecture to use two Elixir processes per connection instead of one (a process that packs commands and sends them on the socket and a process that listens from the socket and replies to waiting clients); this should speed up Redix when it comes to multiple clients concurrently issueing requests to Redis.

## v0.3.0

### Breaking changes

  * Change the behaviour for an empty list of command passed to `Redix.pipeline/2` (`Redix.pipeline(conn, [])`), which now raises a `Redix.ConnectionError` complaining about the empty command. Before this release, the behaviour was just a connection timeout.

  * Change the behaviour of empty commands passed to `Redix.command/2` or `Redix.pipeline/2` (for example, `Redix.command(conn, [])` or `Redix.pipeline(conn, [["PING"], []])`); empty commands now return `{:error, :empty_command}`. The previous behaviour was just a connection timeout.

  * Remove `Redix.start_link/1` in favour of just `Redix.start_link/2`: now Redis options are separated from the connection options. Redis options can be passed as a Redis URI as well.

### Bug fixes and improvements

  * Change the error messages for most of the `Redix.ConnectionError` exceptions from simple atoms to more meaningful messages.

## v0.2.1

  * Fix a bug with single-element lists, that were parsed as single elements (and not lists with a single element in them) by `Redix.Protocol.parse_multi/2`. See [whatyouhide/redix#11](https://github.com/whatyouhide/redix/issues/11).

## v0.2.0

  * Rename `Redix.NetworkError` to `Redix.ConnectionError` (as it's more generic and more flexible).

  * Add support for PubSub. The following functions have been added to the `Redix` module:
    * `Redix.subscribe/4`
    * `Redix.subscribe!/4`
    * `Redix.psubscribe/4`
    * `Redix.psubscribe!/4`
    * `Redix.unsubscribe/4`
    * `Redix.unsubscribe!/4`
    * `Redix.punsubscribe/4`
    * `Redix.punsubscribe!/4`
    * `Redix.pubsub?/2`

## v0.1.0

Initial release.
