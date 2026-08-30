# Fixes made by Claude Opus - second round

Follow-up to [`audit_gpt_sol_2.md`](audit_gpt_sol_2.md), the external review of the changes recorded in
[`fixes_made_by_opus.md`](fixes_made_by_opus.md). Kept in the Binance repository, content spans all four
repositories.

Everything below is committed; nothing was pushed.

| Repository | Commit |
|---|---|
| `zorro-binance-futures-plugin` | `8959f4b` |
| `binance-cpp-api` | `28bac5c` |
| `zorro-bybit-futures-plugin` | `7866695` |
| `bybit-cpp-api` | `b50b985` |

---

## Verdict on the audit

All findings were reproduced in the code before anything was changed. Two of them were defects introduced by the
previous round of fixes, not pre-existing ones.

| Finding | Verdict |
|---|---|
| P0-1 Pending and unknown close orders are not tracked | Confirmed |
| P0-2 Reconciliation can return 0 while the order is still live | Confirmed |
| P0-2b A generic exception is read as "the order does not exist" | Confirmed |
| P0-2c Bybit answers -2 without attempting a cancel | Confirmed |
| P1-1 Bybit `BrokerTrade` ignores closed lots for a terminal entry | Confirmed - introduced by the previous round |
| P1-2 Binance does not treat HTTP 408 as an unknown outcome | Confirmed |
| P2-1 `BrokerAccount` reads the account id as a currency | Confirmed - known, now documented |
| P2-2 A clean build with `ENABLE_TESTS=ON` fails | Confirmed - and it invalidated a claim in the first summary |

## What was fixed

### Closing orders always reach a terminal state

The core of P0-1. A closing order used to be sent GTC, so a limit close rested on the book; its client id was never
persisted, `BrokerTrade` only ever queried the entry order, and a fill that arrived later was therefore never
booked. Zorro then either held a trade the exchange no longer had, or sent another close for lots that were already
gone.

Closes are now sent **IOC**: they fill what they can immediately and expire with the rest, so `BrokerSell2` always
returns with a state the exchange will not change anymore. Nothing is left resting that nothing tracks.

For the case where the outcome cannot be established at all - the response is lost, the venue is unreachable - the
close order's client id is remembered in the trade record (`pendingCloses`) and settled by `BrokerTrade` on a later
call, which books its fill against the trade. The trade is reported as not closed in the meantime, so Zorro retries;
`reduceOnly` keeps a repeated close from flipping the position.

**This is a deliberate behaviour change:** a limit close that cannot be filled at its price right away is not kept
working, it expires and Zorro is told the trade was not closed. That is the trade-off for never having an untracked
order on the book, and it matches how the plugin was originally used (market orders only).

### An unknown outcome is settled, never guessed

The core of P0-2. Cancelling is asynchronous on both venues, so the single query issued after the cancel could still
answer "working" with nothing filled - which the code read as "did not fill" and reported as a rejection, while the
order was live on the exchange.

Both plugins now share a `resolveOrderOutcome()` helper that polls the order until the exchange settles it, sending
one cancel when it is still working. An order that does not settle within the budget is reported as unknown (`-2`),
never as a rejection.

**Correction:** this document originally also claimed that Bybit's "poll budget exhausted" paths were routed through
the same helper. They were not - that edit was lost before the commit and the two paths still answered `-2` without
attempting a cancel. It was found by the next audit and fixed in the third round.

### Only the venue may say "the order does not exist"

P0-2b. Reconciliation treated every non-transport exception as proof that the order never reached the exchange. An
authentication failure, a rate limit or a malformed answer proves nothing of the sort.

A dedicated `OrderNotFound` exception was added to both libraries, thrown only for the venue's own answer (Binance
`-2013`, Bybit `110001` / `170213`). That one returns 0. Everything else keeps the outcome unknown. On Bybit the
message wording is deliberately unchanged, because the execution gateway of the trading bot recognizes ambiguous
outcomes by parsing it.

### Bybit `BrokerTrade`: one calculation for every state

P1-1, a defect introduced by the previous round. The branch for a terminal entry order returned the raw entry fill
and skipped the `entry fill - closed` calculation, so an IOC entry that filled 3 of 10 lots and was then fully
closed still reported 3 open lots on a flat position. The early return is gone; every state goes through the same
calculation, retires the trade when nothing is left, and reports `-1`.

### Smaller items

- **HTTP 408** joins the unknown-outcome set on Binance - it is a timeout waiting for the backend, so the request
  may well have reached it.
- **`stonky_common` is linked `PUBLIC`** in both libraries. Its `PUBLIC` include directories never reached
  consumers, so a clean CMake configure with `ENABLE_TESTS=ON` could not build the test executables. Verified by an
  actual build this time, not by a syntax check with manual include flags.
- **`BrokerAccount`'s `Account` parameter** is documented in both READMEs as the margin asset rather than an account
  identifier, with the failure mode spelled out.
- The claim in `fixes_made_by_opus.md` that the test executables build was corrected - it held only for the manual
  include flags used to verify it, not for a clean CMake build.

## Version and changelog

The version stays at **2.0.0**. It was never tagged, released or pushed, so these fixes were folded into the
existing 2.0.0 changelog section instead of inventing a 2.0.1.

## Still open

Carried over from the audit as acceptable for this scope, unchanged:

- No broker side stop loss; `dStopDist` is ignored.
- The trade store and the order id counter are shared across accounts and plugin copies. Safe only while one copy
  serves one account.
- `SET_ORDERTYPE` follows the older convention of the bundled `trading.h` (`0` = FOK, `1` = IOC, `2` = GTC), not the
  current manual (`0` = broker default, `1` = AON). Left by decision of the operator, pending a check against the
  latest Zorro release.
- One WebSocket connection per symbol on Binance.
- Live `pVolume` and historical `T6.fVol` mean different things.
- The connect phase falls back to the Windows connect timeout.
- No automated tests of the order state machine.

## Verification status

- Both plugins syntax-check under GCC 15 with C++23, in the x64 and the 32-bit configuration, using a stub of the
  Windows headers.
- Both libraries **and** their test executables build from a clean CMake configure with `ENABLE_TESTS=ON`. This was
  run for real this time.
- Every audit finding was reproduced in the source before being fixed.

**Not verified: no MSVC build, no linking, no run against a live or test account.** The order paths this round
touches - IOC closes, polling to a terminal state, cancel before `-2`, the pending-close reconciliation - have only
been reasoned about and type checked. They are exactly the paths that need the exchange to be exercised.

The audit's suggested matrix is the right minimum and is still outstanding:

- market open and close, long and short;
- partial IOC entry followed by a full close;
- GTC entry: no fill, partial fill, full fill, cancel;
- limit close that cannot fill at its price (now expected to expire and report "not closed");
- timeout after sending an entry and after sending a close;
- two trades, restart Zorro, close both.
