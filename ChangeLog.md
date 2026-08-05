# Changelog
All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

### Changed

- `GET_POSITION` now returns the net open amount in lots, as the broker API requires ("net open amount as in
  BrokerBuy2"). It used to return the raw contract quantity. **Scripts that relied on the old unit must be adapted.**
- `BrokerBuy2` returns -2 when the exchange did not confirm the order (network/timeout) instead of 0. A transport
  failure does not mean the order was rejected - it may well be live - and 0 would invite a duplicate order.

### Added

- `BrokerTrade`, so that Zorro can follow the fill state of orders resting on the book.

### Fixed

- Subscribing an illiquid asset no longer fails. Zorro subscribes an asset by calling `BrokerAsset` with
  `pPrice == NULL`, and an asset that answers 0 to that call triggers Error 053 and gets its trading disabled. The
  subscription call no longer depends on a tick having arrived - the bookTicker stream only pushes on a best bid/ask
  change, which for a low cap can take minutes. In addition the price is seeded from a REST snapshot on subscribe and
  a REST snapshot is used as a fallback whenever the stream stays silent.
- Read timeout of the stream manager was 0.3x the configured value (a `timeout / 0.01` counter combined with a 3 ms
  sleep), so the effective wait was 1.5 s instead of 5 s. Replaced by a real deadline.
- A cached tick price is no longer served forever after the WebSocket session dies - ticks older than `maxTickAge`
  (60 s) are refreshed over REST instead.
- `BrokerSell2` ignored the position mode and always sent `positionSide=LONG/SHORT`, which Binance rejects in One-way
  mode with -4061, making it impossible to close a trade. One-way mode now sends `positionSide=BOTH` + `reduceOnly`.
- `BrokerSell2` dropped the trade id to symbol mapping before the closing order was confirmed. A failed close left the
  trade impossible to close ever again.
- Orders that rest on the book (a GTC limit order answers with `NEW`) were reported as failures while staying open on
  the exchange. They are now reported as pending trades with a fill amount of 0.
- A partially filled IOC order (`EXPIRED` with a non-zero executed quantity) was reported as a failure although the
  fill is a real position. Partial fills are now reported with their fill amount.
- `BrokerSell2` returned the id of the closing order. Zorro expects the original trade id unless the broker really
  assigned a new one to the remainder.
- `getLastFundingRate()` called `back()` on an empty vector for symbols without funding history (undefined behaviour).
- `BrokerAccount` rounded the balance to whole units and reported neither the trade value nor the margin.
- `BrokerAsset` returned success for an unknown symbol, read an uninitialized `*pPip` and dereferenced `pLotAmount`
  without a null check.
- Signed requests used the local clock without any correction, so a clock drift larger than recvWindow made every
  request fail with -1021. The offset against the exchange clock is now measured and applied.
- `getPositionRisk()` used a non-existent V1 endpoint.
- Historical prices dropped the last candle even when it was already closed.
- Non-JSON error responses (gateway HTML, plain text) threw a parse error instead of reporting the HTTP status.
- The exchange info was re-downloaded every 60 seconds, now every 15 minutes, and hot paths no longer copy the whole
  multi hundred symbol structure just to read one value.
- `NEW` from Zorro's `trading.h` collided with `futures::OrderStatus::NEW`.

## [1.0.0](https://github.com/stawe-org/tools_zorro_binance_plugin/releases/tag/1.0.0) (2025-11-13)

- Initial release