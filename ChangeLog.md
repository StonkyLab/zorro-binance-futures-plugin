# Changelog
All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

## 2.0.0 (2026-08-29)

### Changed

- `recvWindow` of signed requests lowered from the 60 s maximum to the exchange default of 5 s. With the clock
  synchronization in place the wide window only meant that a badly delayed order could still be executed.
- `GET_POSITION` now returns the net open amount in lots, as the broker API requires ("net open amount as in
  BrokerBuy2"). It used to return the raw contract quantity. **Scripts that relied on the old unit must be adapted.**
- `BrokerBuy2` no longer answers a failed send with a plain "rejected". A failure whose outcome is unknown (the
  transport, the HTTP 5xx family, and the API codes -1006 / -1007) is reconciled: the order is queried back by its client order id,
  an order still resting on the book is cancelled - which the broker API requires after answering -2 - and whatever
  filled is reported as the trade.
- `BrokerBuy2` returns -2 when the exchange did not confirm the order (network/timeout) instead of 0. A transport
  failure does not mean the order was rejected - it may well be live - and 0 would invite a duplicate order.

### Added

- `GET_COMPLIANCE` returns "no hedging" for One-way accounts, so Zorro nets opposite entries instead of believing it
  holds two independent positions.
- `BrokerTime` detects a lost connection: it reports the exchange as unreachable when neither recent traffic nor a
  probe request succeeds, which is what lets Zorro stop trading and reconnect.
- `BrokerTrade`, so that Zorro can follow the fill state of orders resting on the book.

### Fixed

- **A partially filled entry left the trade record wrong.** The record kept the REQUESTED size, so an order for 10
  lots that filled 3 and was then fully closed still counted 7 lots as open and `BrokerTrade` kept reporting an open
  position that no longer existed. Closed lots are now counted cumulatively against the real entry fill.
- The store was written by opening the target file directly, which truncates it first - a crash or a full disk in
  the middle of the write destroyed the trade to symbol mapping. It is written to a temporary file and renamed over
  the target now, and a failed write is reported to Zorro instead of only being logged.
- Blocking socket operations had no bound at all, so a black holed connection could stall the Zorro thread until the
  operating system gave up, which is minutes. Every read and write of a request is now bounded by the `SET_WAIT`
  time. The connect itself still falls back to the Windows connect timeout.
- `setCredentials` replaced the HTTP session without synchronization while the background instrument updater was
  using it - a data race, and the `reset()` before the assignment even left a window with a null session.
- The hedge flag was only ever set to true. After the account was switched to One-way mode the plugin kept sending
  hedge parameters and every order failed with -4061 until Zorro was restarted.
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

### Notes

- The plugin API level reported by `BrokerOpen` stays at 2, that is the Zorro broker API generation the plugin
  implements, not the plugin's own version.

## 1.0.0 (2025-11-13)

- Initial release