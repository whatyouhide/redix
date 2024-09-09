# Changelog

## v1.5.2

### Bug fixes

  * Fix a bug with `Redix.transaction_pipeline/2`, which would return `{:ok, Redix.Error.t()}` in some cases. Those cases now return `{:error, Redix.Error.t()}`, which is what was documented in the spec.
  * Fix an issue with sentinels reporting their peers with IPv4 or IPv6 addresses. In these cases, Redix wouldn't be able to connect—that has been fixed.

## v1.5.1

### Bug fixes

  * Fix a race condition that would cause connections to stop and not reconnect in cases where the network would fail *after* establishing the connection but *before* issuing potential `AUTH` or `SELECT` commands. This is a recommended upgrade for everyone.

## v1.5.0

### New features

  * Add support for the `valkey://` scheme when using URIs.

## v1.4.2

### Bug fixes and improvements

  * Speed up `Redix.Protocol` a little bit for common responses (`"OK"` and friends).
  * Fix a bug where `:tcp_closed`/`:ssl_closed` and `:tcp_error`/`:ssl_error` messages wouldn't arrive to the socket owner, and Redix would get stuck in a disconnected state when sending would error out. See the discussion in [#265](https://github.com/whatyouhide/redix/issues/265).

## v1.4.1

### Bug fixes and improvements

  * `Redix.PubSub.get_client_id/1` is not available only behind the `:fetch_client_id_on_connect` option that you can pass to `Redix.PubSub.start_link/1`. This option defaults to `false`, so that this version of Redix is compatible with Redis v4 or earlier out of the box. To opt in into the behavior desired for [client-side caching](https://redis.io/docs/manual/client-side-caching/) and use `Redix.PubSub.get_client_id/1`, pass `fetch_client_id_on_connect: true` to `Redix.PubSub.start_link/1`.

## v1.4.0

### Bug fixes and improvements

  * Introduce `Redix.PubSub.get_client/1`, which can be used to implement [client-side caching](https://redis.io/docs/manual/client-side-caching/).

## v1.3.0

### Bug fixes and improvements

  * Improve EXITs that happen during calls to Redix functions.
  * Remove call to deprecated `Logger.warn/2`.
  * Support MFA for `:password` in the `:sentinel` option.
  * Add the `Redix.password/0` type.
  * Add the `Redix.sentinel_role/0` type.

## v1.2.4

### Bug fixes and improvements

  * Remove Dialyzer PLTs from the Hex package. This has no functional impact whatsoever on the library. The PLTs were accidentally published together with the Hex package, which just results in an unnecessarily large Hex package.

## v1.2.3

### Bug fixes and improvements

  * Fix a bug with validating the `:socket_opts` option, which required a keyword list and thus wouldn't support *valid* options such as `:inet6`.

## v1.2.2

### Bug fixes and improvements

  * Make parsing large bulk strings *a lot* faster. See [the pull request](https://github.com/whatyouhide/redix/pull/247) for benchmarks. This causes no functional changes, just a speed improvement.

## v1.2.1

### Bug fixes and improvements

  * Relaxed the version requirement for the `:castore` dependency to support `~> 1.0`.

## v1.2.0

### New features

  * Add `:telemetry_metadata` option to Redis calls. This can be used to provide custom metadata for Telemetry events.
  * Mark **Redis sentinel** support as *not*-experimental anymore.
  * Make `Redix.URI` part of the public API.

### Bug fixes and improvements

  * Handle Redis servers that disable the `CLIENT` command.
  * Bump Elixir requirement to 1.11+.
  * Raise an error if the `:timeout` option (supported by many of the function in the `Redix` module) is something other than a non-negative integer or `:infinity`. Before, `timeout: nil` was accidentally supported (but not documented) and would use a default timeout.

## v1.1.5

### Bug fixes and improvements

  * Fix formatting of Unix domain sockets when logging
  * Use `Logger` instead of `IO.warn/2` when warning about ACLs, so that it can be silenced more easily.
  * Allow the `:port` option to be set explicitly to `0` when using Unix domain sockets
  * Support empty string as database when using Redis URIs due to changes to how URIs are handled in Elixir

## v1.1.4

### Bug fixes and improvements

  * Support version 1.0 and over for the Telemetry dependency.

## v1.1.3

### Bug fixes and improvements

  * The `.formatter.exs` file included in this repo had some filesystem permission problems. This version fixes those.

## v1.1.2

Version v1.1.1 was accidentally published with local code (from the maintainer's machine) in it instead of the code from the main Git branch. We're all humans! Version v1.1.1 has been retired.

## v1.1.1

### Bug fixes and improvements

  * Version v1.1.0 started using ACLs and issuing `AUTH <username> <password>` when a username was provided (either via options or via URI). This broke previous documented behavior, where Redix used to ignore usernames. With this bug fix, Redix now falls back to `AUTH <password>` if `AUTH <username> <password>` fails because of the wrong number of arguments, which indicates a version of Redis earlier than version 6 (when ACLs were introduced).

## v1.1.0

### Bug fixes and improvements

  * Improve handling of databases in URIs.
  * Add support for [ACL](https://redis.io/topics/acl), introduced in Redis 6.

## v1.0.0

No bug fixes or improvements. Just enough years passed for this to become 1.0.0!

## v0.11.2

### Bug fixes and improvements

  * Fix a connection process crash that would very rarely happen when connecting to sentinel nodes with the wrong password or wrong database would fail to due a TCP/SSL connection issue.

## v0.11.1

### Bug fixes and improvements

  * Allow `nil` as a valid value for the `:password` start option again. v0.11.0 broke this feature.

## v0.11.0

### Breaking changes

  * Use the new Telemetry event conventions for pipeline-related events. The new events are `[:redix, :pipeline, :start]` and `[:redix, :pipeline, :stop]`. They both have new measurements associated with them.
  * Remove the `[:redix, :reconnection]` Telemetry event in favor or `[:redix, :connection]`, which is emitted anytime there's a successful connection to a Redis server.
  * Remove support for the deprecated `:log` start option (which was deprecated on v0.10.0).

### Bug fixes and improvements

  * Add the `:connection_metadata` name to all connection/disconnection-related Telemetry events.
  * Allow a `{module, function, arguments}` tuple as the value of the `:password` start option. This is useful to avoid password leaks in case of process crashes (and crash reports).
  * Bump minimum Elixir requirement to Elixir `~> 1.7`.

## v0.10.7

### Bug fixes and improvements

  * Fix a crash in `Redix.PubSub` when non-subscribed processes attempted to unsubscribe.

## v0.10.6

### Bug fixes and improvements

  * Fix a bug that caused a memory leak in some cases for Redix pub/sub connections.

## v0.10.5

### Bug fixes and improvements

  * Fix default option replacement for SSL in OTP 22.2.
  * Allow `:gen_statem.start_link/3,4` options in `Redix.start_link/2` and `Redix.PubSub.start_link/2`.
  * Change default SSL depth from 2 to 3 (see [this issue](https://github.com/whatyouhide/redix/issues/162)).

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

  * Don't raise `Redix.Error` errors on non-bang variants of functions. This means that for example `Redix.command/3` won't raise a `Redix.Error` exception in case of Redis errors (like wrong typing) and will return that error instead. In general, if you're pattern matching on `{:error, _}` to handle **connection errors** (for example, to retry after a while), now specifically match on `{:error, %Redix.ConnectionError{}}`. If you want to handle all possible errors the same way, keep matching on `{:error, _}`.

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

  * Restructure the Redix architecture to use two Elixir processes per connection instead of one (a process that packs commands and sends them on the socket and a process that listens from the socket and replies to waiting clients); this should speed up Redix when it comes to multiple clients concurrently issuing requests to Redis.

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
