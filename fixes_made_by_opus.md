# Fixes made by Claude Opus

Record of the changes made during the review sessions of 2026-08-29, covering both Zorro plugins and the two C++
API libraries under them. Kept in the Binance repository, but the content spans all four repositories.

Everything below is committed; nothing was pushed.

| Repository | Commits |
|---|---|
| `zorro-binance-futures-plugin` | `25db27e`, `92afc6d`, `c85878e`, `5690ad4`, `b1fb647` |
| `binance-cpp-api` | `c910141`, `98b6ffc` |
| `zorro-bybit-futures-plugin` | `8e409c1`, `b6373c1`, `89b8f95`, `46942b0`, `0e2e9b1`, `bc2a128` |
| `bybit-cpp-api` | `027a4b3`/`35a6896`, `0ea1ac7`, `32cbcf2` (merge with origin/main) |

---

## 1. The reported bug: subscribing an illiquid asset failed

The original complaint - "waiting for a tick price, which on a low cap can take minutes, so the plugin falsely
reports a failed subscribe" - had three independent causes.

- **The subscription call was implemented in the wrong place.** Zorro subscribes an asset by calling `BrokerAsset`
  with `pPrice == NULL`, and an asset that answers 0 to that call triggers Error 053 and gets its trading disabled
  for the rest of the session. Both plugins ran the price-reading path for that call and returned 0 when no tick had
  arrived. The subscription call now only verifies that the symbol exists and that its stream is running.
- **The read timeout was 0.3x of what it claimed.** `maxNumTries = timeout / 0.01` combined with a 3 ms sleep gave
  1.5 s instead of the configured 5 s, in both stream managers. Replaced by a real `steady_clock` deadline.
- **Nothing ever seeded the price cache.** The `bookTicker` (Binance) and `tickers` (Bybit) streams only push when
  something changes. The current quote is now seeded from the REST snapshot on subscribe, and a REST snapshot is
  also used as a fallback whenever the stream stays silent.

Related, and arguably more dangerous than the reported symptom: **a cached quote was served forever after the
WebSocket session died**. There was no staleness check at all, so a dead stream kept feeding hours-old prices into
trading decisions. Quotes older than `maxTickAge` (60 s) are now refreshed over REST instead.

## 2. Order handling

- **Bybit: the plugin did not compile.** Every member access still used the `m_` prefixed field names that
  `bybit-cpp-api` had dropped - roughly 60 errors. This had to be fixed before anything else could be verified.
- **Bybit: the order state loop waited for the v3-era status `Active`**, which v5 never reports for a resting order.
  A GTC limit order therefore polled for 5 s and was then reported as a failure while it was live on the exchange.
- **Resting orders were reported as failures.** A GTC limit order answers `NEW`, which both plugins treated as a
  rejection while the order stayed on the book. They are now reported as pending trades with a fill amount of 0.
- **Partially filled IOC orders were reported as failures** although the fill is a real position (Binance: `EXPIRED`
  with a non-zero executed quantity, Bybit: `PartiallyFilledCanceled`).
- **`BrokerSell2` could make a trade impossible to close.** It dropped the trade id to symbol mapping *before* the
  closing order was confirmed, so a failed close left the plugin unable to ever address that trade again. Bybit
  additionally tried to erase the same record a second time, logging a bogus "Could not find Asset for trade id" on
  every single close.
- **Binance: `BrokerSell2` ignored the position mode** and always sent `positionSide=LONG/SHORT`, which the exchange
  rejects in One-way mode with -4061 - closing a trade was impossible on such an account.
- **One-way closes are sent with `reduceOnly`**, so an oversized close cannot flip the position.
- **`BrokerSell2` returned the id of the closing order.** Zorro expects the original trade id unless the broker
  really assigned a new one to the remainder.
- **`BrokerTrade` was missing entirely**, so Zorro had no way to learn that a resting order had been filled. It is
  implemented in both plugins. On Bybit the realtime order endpoint only serves orders that are still open, so for
  an order that already filled it falls back to the execution list.

### The trade record was wrong after a partial entry fill

Found by the external audit and confirmed: the record stored the size that was **requested**, not the size that
**filled**, and closes were subtracted from it. An order for 10 lots that filled 3 and was then fully closed still
counted 7 lots as open, and `BrokerTrade` kept reporting a position that no longer existed.

Closed lots are now counted cumulatively against the real entry fill instead of being subtracted from the requested
size. The record is retired once nothing is left open, which also stops the file from growing forever.

### Unknown outcomes are no longer reported as rejections

A failed send used to be indistinguishable from a rejected order. Both venues document responses whose execution
state is genuinely unknown (HTTP 5xx, Binance `-1006`/`-1007`, Bybit `10000`/`10016`/`170007`), and the request may
still have been executed - reporting it as a rejection invites a duplicate order.

Both libraries now throw a typed `UnknownOutcomeError` (`TransportError` for the transport level, `ExecutionUnknown`
for the venue answers above). `BrokerBuy2` reconciles such a failure: it queries the order back by its client order
id, cancels it if it is still resting - which the broker API requires after answering -2 - and reports whatever
filled as the trade. `-2` is returned only when even the reconciliation cannot reach the exchange.

## 3. Conformance with the Zorro broker API

Verified against the manual, not from memory. Several of these corrected my own earlier assumptions.

- **`BrokerTime` always reported the market as open** once the client object existed, so Zorro could never notice a
  broken connection. It now goes by the time of the last successful response and, when the traffic went quiet, by a
  rate limited probe request.
- **`GET_POSITION` returned raw contracts**, while the API specifies "net open amount as in BrokerBuy2", i.e. lots.
  Now converted using the per symbol lot size. **This is a breaking change for scripts** and is why the plugin
  version went to 2.0.0.
- **`GET_COMPLIANCE` was not implemented.** It now reports flag 2 ("no hedging") for One-way accounts, so Zorro nets
  opposite entries instead of believing it holds two independent positions.
- **`BrokerAccount` reported neither the trade value nor the bound margin**, and Binance rounded the balance to
  whole units.
- **`BrokerAsset` reported success for an unknown symbol**, read an uninitialized `*pPip` and dereferenced
  `pLotAmount` without a null check. Bybit additionally reported the last traded price where Zorro expects the ask,
  which is what the returned spread is relative to.
- **`BrokerBuy2` returns `-2`** when the exchange did not confirm, instead of `0` which reads as a rejection.
- **`SET_WAIT` was stored and ignored.** It now bounds both the price reads and the REST requests.

## 4. Robustness in the libraries

- **Signed requests used the uncorrected local clock.** Any drift beyond `recvWindow` failed every request
  (Binance -1021). The offset against the exchange clock is now measured and applied, re-synchronized every 30 min.
- **`recvWindow` lowered to the venue default of 5 s** (from 60 s on Binance, 25 s on Bybit). The wide window only
  meant that a badly delayed order could still be executed.
- **Blocking socket operations had no bound at all**, so a black holed connection could stall the Zorro thread for
  as long as the operating system allows. Every read and write is now bounded by the `SET_WAIT` time.
- **`setCredentials` was a data race.** It replaced the shared HTTP session while background threads were issuing
  requests, and the `reset()` before the assignment even left a window with a null pointer. Both are atomic
  `shared_ptr`s handed out by value now. This also affects the trading bots that use these libraries.
- **`getLastFundingRate()` called `back()` on an empty vector** for symbols without funding history - undefined
  behaviour, not an exception, so the `try/catch` around it did not help.
- **`getPositionRisk()` used a Binance V1 endpoint that does not exist.**
- **Non-JSON error bodies** (gateway HTML, plain text) threw a parse error instead of reporting the HTTP status.
- **Beast's default 8 MB body limit** rejected the larger public data archives.
- **Historical prices dropped the last candle** even when it had already closed.
- **Ticker cache entries were keyed by the symbol from the message body**, which a Bybit delta message does not
  carry, while lookups used the symbol from the topic.
- **`getOpenOrder()` parsed the response body twice**, and sent empty `orderId`/`orderLinkId` parameters which the
  venue rejects.
- **Exchange/instrument info was re-downloaded every 60 seconds** (a multi-megabyte payload); now every 15 minutes,
  and the hot paths no longer copy the whole several-hundred-symbol structure to read a single value.
- **The trade store was written by truncating the target file**, so a crash mid-write destroyed the trade to symbol
  mapping. It is written to a temporary file and renamed over the target now, and a failed write reaches the
  operator instead of only the log.

## 5. Build and repository hygiene

- **`NEW` from Zorro's `trading.h` collided with `futures::OrderStatus::NEW`** - a latent compile error that would
  have hit the first time anyone referenced that enum value.
- **Boost headers must be included before `trading.h`**, which defines `and`/`or`/`not` as macros and breaks any
  Boost header included after it. Documented at the include site in the Bybit plugin.
- **Both Visual Studio projects were dead.** They pointed at `binance_cpp_api` / `bybit_cpp_api` and `vk_cpp_common`,
  directories that no longer exist, and listed template headers (`framework.h`, `pch.h`) that were never in the
  repositories. The Bybit project additionally did not link zlib although it compiles the zlib-using REST client,
  and carried the Binance project's GUID and root namespace.
- **C++23** for both plugins, matching what `stonky-cpp-common` already required and propagated.
- **READMEs** now document the real dependencies (the submodules moved organization, and OpenSSL, zlib and
  magic_enum were missing entirely), the supported position modes, and the fact that no broker side stop loss is
  placed - `dStopDist` is deliberately ignored, so a stop only works while Zorro is running.
- **Dead release links removed** from the changelogs: they pointed at a different organization and at a tag name
  that does not exist either (the tags are `v1.0.0`).
- **Version bumped to 2.0.0** in both plugins. The plugin API level returned by `BrokerOpen` stays at 2 - that is
  the Zorro broker API generation, not the plugin's own version.

## 6. Merge with the parallel Bybit work

While this was in progress, `bybit-cpp-api` had received eight upstream commits that fixed several of the same
audit findings independently. Resolved in favour of the upstream implementation wherever it overlapped:

- **Socket stall timeouts:** upstream's version kept, because it sets the options on the open socket before connect
  and therefore bounds the connect as well on POSIX. Made configurable so `SET_WAIT` can drive it.
- **Ambiguous outcomes:** upstream detects them by matching the exception message text in the execution gateway.
  That was left untouched and the typed exceptions were added next to it, so callers outside the gateway can catch
  by type. **The message wording must not be changed - the gateway parses it.**
- **Pagination and the `settleCoin` overload:** taken from upstream unchanged.

Fixed while merging: the new upstream stall-timeout code included `<sys/socket.h>` and `<sys/time.h>` unguarded and
passed a `timeval` to `setsockopt`, neither of which builds on MSVC - and the Zorro plugin compiles that file.

## 7. What was NOT done

- **`SET_ORDERTYPE` semantics.** The current Zorro manual documents `0` as "broker default (highest fill
  probability)", `1` as AON, `2` as GTC. Both plugins implement the older convention from the bundled `trading.h`
  (`0` = FOK, `1` = IOC, `2` = GTC), which inverts the meaning of the default: Zorro asks for the highest fill
  probability and gets FOK, the lowest. Left as is by decision of the operator, pending a check against the latest
  Zorro release.
- **Broker side stop loss.** `dStopDist` remains ignored; documented in both READMEs.
- **Per-account persistence.** The trade store and the order counter are still shared across accounts and plugin
  copies, so two accounts can collide on trade ids.
- **One WebSocket connection per symbol** on Binance, where a multiplexed stream would do.
- **Live `pVolume` and historical `T6.fVol` have different meanings** (top of book depth vs traded volume).
- **No automated tests.** The order state machine, partial fills, restart and reconnect paths have no coverage; the
  existing test executable needs real credentials and has no assertions. `binance-cpp-api/test/connector.cpp` does
  not even compile, but it is dead code that is not part of the CMake build.
- **Connect phase timeout** on Windows still falls back to the operating system default.

## 8. Verification status

- Both plugins syntax-check under GCC 15 with C++23, in the x64 and the 32-bit configuration, using a stub of the
  Windows headers. Both libraries and their test executables build from a clean CMake configure with
  `ENABLE_TESTS=ON`.
- Every file referenced by the Visual Studio projects was verified to exist, as were the include directories.
- Claims about the broker API were checked against the Zorro manual, and venue behaviour against the exchange docs.

**Not verified: no MSVC build, no linking, and no run against a live or test account.** In particular the order
paths (partial fills, unknown outcomes, reconciliation, cancel after -2) have only been reasoned about and type
checked, never executed against an exchange. A `TradeTest` run on a demo or small live account is the missing step.
