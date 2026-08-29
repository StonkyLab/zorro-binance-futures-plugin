# Binance Futures Zorro Plugin
Binance Futures Plugin for Zorro Trader

# Instructions

- Build as the x86 (32 bit) architecture & place the output dynamic library into Zorro/Plugin folder.
- Plugin logs all issues into Zorro/Log/binance_futures.log file.

# Notes and limitations

- **No broker side stop loss.** `dStopDist` is ignored, no stop order is placed on the exchange. The stop loss is
  handled by Zorro and therefore only works while Zorro is running.
- Both **Hedge** and **One-way** position modes are supported, the mode is detected at login.
- **Demo mode is not supported**, the plugin refuses to log in with `Type == "Demo"`.
- **The `Account` field is read as the margin asset, not as an account id.** Zorro passes the account name or
  number from its account list, this plugin interprets it as the coin whose wallet balance is reported (`USDT` when
  the field is empty). Leave it empty or set it to the margin asset; an arbitrary account identifier there makes
  `BrokerAccount` fail with "Account currency not found".
- A limit ENTRY order that rests on the book is reported to Zorro as a pending trade with a fill amount of 0;
  `BrokerTrade` then reports its fill state. **Closing orders are always sent IOC**, so they never rest on the
  book - a close that could be left hanging would keep filling with nothing tracking it.
- Prices come from the `bookTicker` WebSocket stream. Because that stream only pushes when the best bid/ask changes,
  the plugin seeds and refreshes them from the REST snapshot endpoint, which keeps illiquid symbols usable.
- The trade id to symbol mapping is persisted in `Zorro/Data/binance_open_trades.json` and survives a restart.

# Custom exported functions

- ```DLLFUNC_C int AssetMinuteCandle(char *Asset, int previous, T6 *candle);```
- ```DLLFUNC_C int AssetMinuteCandleREST(char *Asset, T6 *candles, int maxCandles);```
- ```DLLFUNC_C int GetLastFundingRate(char *Asset, double *fundingTime, double *fundingRate);```
- ```DLLFUNC_C int PreloadMinuteCandles(char **Assets, int numAssets, int numCandles);```
- ```DLLFUNC_C int GetPreloadedMinuteCandles(char *Asset, T6 *candles, int maxCandles, int &numRead);```
- ```DLLFUNC_C int GetMaxPositionValue(char *Asset, double *amount);```
- ```DLLFUNC_C int GetPositionLimits(char *Asset, double *lotSize, double *marketLotSize);```
- ```DLLFUNC_C int ChangeInitialLeverage(char *Asset, int targetLeverage, int *leverage, double *maxNotionalValue);```

# Dependencies

Sources, pulled in as git submodules - clone with `git clone --recurse-submodules`:

- [StonkyLab/binance-cpp-api](https://github.com/StonkyLab/binance-cpp-api) - Binance REST and WebSocket client
    - [StonkyLab/stonky-cpp-common](https://github.com/StonkyLab/stonky-cpp-common) - shared utilities, nested
      submodule of the above

Libraries, expected to be provided by the toolchain (e.g. vcpkg):

- [Boost](https://github.com/boostorg/boost) 1.88 or newer - header only, Beast and Asio
- [OpenSSL](https://github.com/openssl/openssl)
- [nlohmann/json](https://github.com/nlohmann/json)
- [spdlog](https://github.com/gabime/spdlog) - used in its header only variant
- [magic_enum](https://github.com/Neargye/magic_enum)

Build environment:

- CMake 4.0 or newer
- A compiler with C++23 support. The plugin itself is C++20, but `stonky-cpp-common` requires C++23
  (`std::jthread`, `std::stop_token`) and propagates that to everything built against it.