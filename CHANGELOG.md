# Changelog

## v0.3.6

* Fixed a bug in the integer parsing in `Redix.Protocol`.

## v0.3.5

* `Redix.Protocol` now uses continuations under the hood for a faster parsing
  experience.
* A bug in `Redix.Protocol` that caused massive memory leaks was fixed. This bug
  originated upstream in Elixir itself, and I submitted a fix for it
  [here](https://github.com/elixir-lang/elixir/pull/4350).
* Some improvements where made to error reporting in the Redix logging.

## v0.3.4

* Fix a bug in the connection that was replacing the provided Redis password
  with `:redacted` upon successful connection, making it impossible to reconnect
  in case of failure (because of the original password now being unavailable).

## v0.3.3

* Fix basically the same bug that was almost fixed in `v0.3.2`, but this time
  for real!

## v0.3.2

* Fix a bug in the protocol that failed to parse integers in some cases.

## v0.3.1

* Restructure the Redix architecture to use two Elixir processes per connection
  instead of one (a process that packs commands and sends them on the socket and
  a process that listens from the socket and replies to waiting clients); this
  should speed up Redix when it comes to multiple clients concurrently issueing
  requests to Redis.

## v0.3.0

* Change the behaviour for an empty list of command passed to `Redix.pipeline/2`
  (`Redix.pipeline(conn, [])`), which now raises a `Redix.ConnectionError`
  complaining about the empty command. Before this release, the behaviour was
  just a connection timeout.
* Change the behaviour of empty commands passed to `Redix.command/2` or
  `Redix.pipeline/2` (e.g., `Redix.command(conn, [])` or `Redix.pipeline(conn,
  [["PING"], []])`); empty commands now return `{:error, :empty_command}`. The
  previous behaviour was just a connection timeout.
* Remove `Redix.start_link/1` in favour of just `Redix.start_link/2`: now Redis
  options are separated from the connection options. Redis options can be passed
  as a Redis URI as well.
* Changed the error messages for most of the `Redix.ConnectionError` exceptions
  from simple atoms to more meaningful messages.

## v0.2.1

* Fix a bug with single-element lists, that were parsed as single elements (and
  not lists with a single element in them) by
  `Redix.Protocol.parse_multi/2`. See
  [whatyouhide/redix#11](https://github.com/whatyouhide/redix/issues/11).

## v0.2.0

* Rename `Redix.NetworkError` to `Redix.ConnectionError` (as it's more generic
  and more flexible).
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
