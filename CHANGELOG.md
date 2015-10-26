# Changelog

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
