# Changelog

## v0.2.0-dev

* Rename `Redix.NetworkError` to `Redix.ConnectionError` (as it's more generic
  and more flexible).
* Add support for PubSub. The following functions have been added to the `Redix` module:
  * `Redix.subscribe/4`
  * `Redix.psubscribe/4`
  * `Redix.unsubscribe/4`
  * `Redix.punsubscribe/4`
  * `Redix.pubsub?/2`

## v0.1.0

Initial release.
