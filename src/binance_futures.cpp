/**
Binance Futures Zorro Plugin

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#include "stonky/binance/binance_futures_rest_client.h"
#include "stonky/binance/binance_http_session.h"
#include "stonky/binance/binance_ws_stream_manager.h"
#include "stonky/binance/binance.h"
#include "stonky/utils/utils.h"
#include "stonky/utils/registry.h"
#include "binance_futures.h"
#include <wtypes.h>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <algorithm>
#include <fstream>
#include <optional>
#include "magic_enum/magic_enum.hpp"

#define PLUGIN_VERSION    2
#define PLUGIN_VERSION_STR "1.0.0"
#define PLUGIN_VERSION_RELEASE_DATE "13-november-2025"
#undef min
/// Zorro's trading.h defines NEW as a chart flag, which collides with futures::OrderStatus::NEW
#undef NEW

#define ZORRO_REG_KEY "SOFTWARE\\Zorro"
#define LAST_ORDER_ID_KEY "BinanceLastOrderId"
#define OPEN_TRADES_FILE R"(./Data/binance_open_trades.json)"

using namespace std::chrono_literals;
using namespace stonky::binance;

/// Maximum time spent waiting for a price of an asset before the REST snapshot fallback kicks in
static constexpr int STREAM_READ_TIMEOUT_S = 5;

/// A cached tick older than this is considered unusable and is refreshed over REST. Must be generous enough for
/// illiquid symbols whose best bid/ask legitimately does not move for a long time.
static constexpr int MAX_TICK_AGE_S = 60;

/// Period of the background refresh of the exchange info (symbol filters, precisions, ...)
static constexpr int EXCHANGE_UPDATE_PERIOD_S = 900;

static std::string currentSymbol;
static std::string accountCurrency;
static int lastOrderId = 0;
static int orderType = 0;
static bool hedge = false;
static double lotAmount = 1.0;
static int loopMs = 50; // Zorro loop time, kept for GET_DELAY only
static int waitMs = STREAM_READ_TIMEOUT_S * 1000; // Maximum broker response time, drives the stream read timeout
std::atomic exchangeUpdaterRunning = false;
std::thread exchangeUpdater;

std::shared_ptr<futures::RESTClient> restClient;
std::unique_ptr<futures::WSStreamManager> streamManager;

std::map<std::string, futures::EventCandlestick> lastBars;
std::map<std::string, std::vector<Candle> > lastCandles;

// #define EXPERIMENTAL

#ifdef _WIN64
std::map<std::string, double> lotAmounts;
#endif

enum ExchangeStatus {
	Unavailable = 0,
	Closed,
	Open
};

__int64 convertTime(const DATE date) {
	return static_cast<__int64>((date - 25569.) * 24. * 60. * 60.);
}

DATE convertTime(const __int64 t64) {
	if (t64 == 0) return 0.;
	return (25569. + static_cast<double>(t64 / 1000) / (24. * 60. * 60.));
}

DLLFUNC_C int BrokerOpen(char *Name, FARPROC fpError, FARPROC fpProgress) {
	strcpy_s(Name, 32, "BinanceFutures");
	reinterpret_cast<FARPROC &>(BrokerError) = fpError;
	reinterpret_cast<FARPROC &>(BrokerProgress) = fpProgress;

	return PLUGIN_VERSION;
}

void logFunction(const stonky::LogSeverity severity, const std::string &errmsg) {
	switch (severity) {
		case stonky::LogSeverity::Info:
			spdlog::info(errmsg);
			break;
		case stonky::LogSeverity::Warning:
			spdlog::warn(errmsg);
			break;
		case stonky::LogSeverity::Critical:
			spdlog::critical(errmsg);
			break;
		case stonky::LogSeverity::Error:
			spdlog::error(errmsg);
			break;
		case stonky::LogSeverity::Debug:
			spdlog::debug(errmsg);
			break;
		case stonky::LogSeverity::Trace:
			spdlog::trace(errmsg);
			break;
	}
}

void exchangeUpdaterFunc() {
	exchangeUpdaterRunning = true;

	/// Refresh right away, then periodically. The exchange info is a multi megabyte payload, downloading it more
	/// often than the symbol filters can realistically change is a waste of bandwidth and API weight.
	int numPass = EXCHANGE_UPDATE_PERIOD_S;

	while (exchangeUpdaterRunning) {
		if (numPass >= EXCHANGE_UPDATE_PERIOD_S) {
			numPass = 0;

			try {
				if (restClient) {
					restClient->updateExchangeInfo(true);
				}
			} catch (std::exception &e) {
				spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
			}
		}
		std::this_thread::sleep_for(1s);
		numPass++;
	}
}

void startExchangeUpdater() {
	exchangeUpdater = std::thread(&exchangeUpdaterFunc);
}

void stopExchangeUpdater() {
	exchangeUpdaterRunning = false;

	if (exchangeUpdater.joinable()) {
		exchangeUpdater.join();
	}
}

void writeLastOrderId() {
	if (const bool success = stonky::writeInRegistry(HKEY_CURRENT_USER, ZORRO_REG_KEY, LAST_ORDER_ID_KEY, lastOrderId); !
		success) {
		spdlog::error("Cannot store BinanceLastOrderId");
	}
}

void readLastOrderId() {
	DWORD bnbLastOrderId;
	const bool success = stonky::readDwordValueRegistry(HKEY_CURRENT_USER, ZORRO_REG_KEY, LAST_ORDER_ID_KEY,
	                                                &bnbLastOrderId);
	if (success && bnbLastOrderId != 0) {
		lastOrderId = static_cast<int>(bnbLastOrderId);
	} else {
		time_t Time;
		time(&Time);
		lastOrderId = static_cast<int>(Time);
		writeLastOrderId();
	}
}

/// Zorro identifies a trade by the id returned from BrokerBuy2, the exchange needs the symbol for every subsequent
/// operation. The mapping is persisted so that it survives a Zorro restart.
struct OpenTrade {
	std::string asset;
	int lots{0}; /// Remaining open lots, 0 for records written by older plugin versions
};

/// Ids of recently closed trades, so that BrokerTrade can tell "fully closed" (-1) apart from "unknown" (NAY)
static constexpr std::size_t MAX_CLOSED_TRADES = 200;

struct TradeStore {
	std::map<int, OpenTrade> open;
	std::vector<int> closed; /// Oldest first, capped at MAX_CLOSED_TRADES
};

TradeStore loadTrades() {
	TradeStore store;

	try {
		std::ifstream ifs(OPEN_TRADES_FILE);

		if (!ifs.is_open()) {
			return store;
		}

		const nlohmann::json json = nlohmann::json::parse(ifs, nullptr, false);

		if (json.is_discarded()) {
			spdlog::error(fmt::format("Malformed json file, path: {}, {}", OPEN_TRADES_FILE, MAKE_FILELINE));
			return store;
		}

		if (const auto it = json.find("openTrades"); it != json.end() && it->is_object()) {
			for (const auto &[key, value]: it->items()) {
				OpenTrade openTrade;

				if (value.is_string()) {
					/// Legacy format - the symbol was stored alone, the open size is unknown
					openTrade.asset = value.get<std::string>();
				} else if (value.is_object()) {
					if (const auto assetIt = value.find("asset"); assetIt != value.end()) {
						openTrade.asset = assetIt->get<std::string>();
					}

					if (const auto lotsIt = value.find("lots"); lotsIt != value.end()) {
						openTrade.lots = lotsIt->get<int>();
					}
				}

				if (!openTrade.asset.empty()) {
					store.open.insert_or_assign(std::stoi(key), openTrade);
				}
			}
		}

		if (const auto it = json.find("closedTrades"); it != json.end() && it->is_array()) {
			store.closed = it->get<std::vector<int> >();
		}
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
	}

	return store;
}

void saveTrades(const TradeStore &store) {
	try {
		auto tradesJson = nlohmann::json::object();

		for (const auto &[tradeId, openTrade]: store.open) {
			tradesJson[std::to_string(tradeId)] = {{"asset", openTrade.asset}, {"lots", openTrade.lots}};
		}

		nlohmann::json json;
		json["openTrades"] = tradesJson;
		json["closedTrades"] = store.closed;

		if (std::ofstream ofs(OPEN_TRADES_FILE); ofs.is_open()) {
			ofs << json.dump(4);
		} else {
			spdlog::error(fmt::format("Couldn't save json file, path: {}, {}", OPEN_TRADES_FILE, MAKE_FILELINE));
		}
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
	}
}

/// BrokerTrade is polled per open trade, so the store is kept in memory and only written through on a change
static std::optional<TradeStore> tradeStoreCache;

TradeStore &tradeStore() {
	if (!tradeStoreCache) {
		tradeStoreCache = loadTrades();
	}

	return *tradeStoreCache;
}

std::optional<OpenTrade> findOpenTrade(const int tradeId) {
	const auto &store = tradeStore();

	if (const auto it = store.open.find(tradeId); it != store.open.end()) {
		return it->second;
	}

	spdlog::error(fmt::format("Could not find Asset for trade id: {}, {}", tradeId, MAKE_FILELINE));
	return {};
}

bool wasTradeClosed(const int tradeId) {
	const auto &store = tradeStore();
	return std::ranges::find(store.closed, tradeId) != store.closed.end();
}

void storeOpenTrade(const int tradeId, const std::string &asset, const int lots) {
	auto &store = tradeStore();
	store.open.insert_or_assign(tradeId, OpenTrade{asset, lots});
	saveTrades(store);
}

/**
 * Account for lots that have just been closed. Once nothing is left open the record moves to the capped list of
 * closed trades, which both keeps the file from growing forever and lets BrokerTrade report a closed trade.
 * @param tradeId
 * @param closedLots absolute number of closed lots
 */
void reduceOpenTrade(const int tradeId, const int closedLots) {
	auto &store = tradeStore();
	const auto it = store.open.find(tradeId);

	if (it == store.open.end()) {
		return;
	}

	if (const auto remaining = std::abs(it->second.lots) - std::abs(closedLots); remaining > 0) {
		it->second.lots = it->second.lots < 0 ? -remaining : remaining;
	} else {
		store.open.erase(it);
		store.closed.push_back(tradeId);

		if (store.closed.size() > MAX_CLOSED_TRADES) {
			store.closed.erase(store.closed.begin(),
			                   store.closed.begin() + static_cast<long>(store.closed.size() - MAX_CLOSED_TRADES));
		}
	}

	saveTrades(store);
}

DLLFUNC_C int BrokerLogin(char *User, char *Pwd, char *Type, char *Account) {
	if (!User) {
		stopExchangeUpdater();
		streamManager.reset();
		restClient.reset();
		/// Drop the cached trade store, it is re-read from disk on the next login
		tradeStoreCache.reset();
		spdlog::info("Logout");
		spdlog::shutdown();
		return 1;
	}
	if (static_cast<std::string>(Type) == "Demo") {
		const auto msg = "Demo mode not supported by this plugin.";
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
		BrokerError(msg);
		return 0;
	}
	if (!restClient) {
		const auto logger = spdlog::basic_logger_mt("binance_logger", R"(./Log/binance_futures.log)");
		spdlog::set_default_logger(logger);
		spdlog::flush_on(spdlog::level::info);
		logger->set_pattern("%+", spdlog::pattern_time_type::utc);

		if (!std::string_view(User).empty() && !std::string_view(Pwd).empty()) {
			restClient = std::make_shared<futures::RESTClient>(User, Pwd);
			startExchangeUpdater();
			readLastOrderId();
			spdlog::info("Logged into account: " + std::string(Account));
			const std::string msg = "Plugin version: " + std::string(PLUGIN_VERSION_STR) + ",  release date: " +
			                        std::string(PLUGIN_VERSION_RELEASE_DATE);
			spdlog::info("Plugin version: " + msg);
			BrokerError(msg.c_str());
		} else {
			const auto msg = "Missing or Incomplete Account credentials.";
			spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
			BrokerError(msg);
			return 0;
		}
	} else {
		restClient->setCredentials(User, Pwd);
	}
	try {
		if (!streamManager) {
			streamManager = std::make_unique<futures::WSStreamManager>(restClient);
			streamManager->setLoggerCallback(&logFunction);
			streamManager->setTimeout(STREAM_READ_TIMEOUT_S);
			streamManager->setMaxTickAge(MAX_TICK_AGE_S);
		}
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		return 0;
	}

	try {
		const auto account = restClient->getAccountInfo();

		if (const auto positionMode = restClient->getPositionMode(); positionMode == PositionMode::Hedge) {
			hedge = true;
		}

		return 1;
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		return 0;
	}
}

DLLFUNC_C int
BrokerAsset(char *Asset, double *pPrice, double *pSpread, double *pVolume, double *pPip, double *pPipCost,
            double *pLotAmount, double *pMarginCost, double *pRollLong, double *pRollShort) {
	const auto currentS = std::time(nullptr);
	const auto currentMinute = stonky::getMsTimestamp(stonky::currentTime()).count() / 60000;

	/// NOTE: Do not log normal state, this function is called ver often!
	if (!Asset || !*Asset) {
		return 0;
	}

	if (!restClient || !streamManager) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance WS stream manager instance not initialized."));
		return 0;
	}

	/// Zorro subscribes an asset by calling with pPrice == NULL and only afterwards asks for prices
	const bool subscribing = pPrice == nullptr;

	if (pPip != nullptr || subscribing) {
		/// Contract parameters of the asset
		try {
			const auto symbolInfo = restClient->getSymbolInfo(Asset);

			if (!symbolInfo) {
				const auto msg = "Unknown asset: " + std::string(Asset);
				spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
				BrokerError(msg.c_str());
				return 0;
			}

			double tickSize = 0.0;
			double stepSize = 0.0;

			for (const auto &fEl: symbolInfo->filters) {
				if (fEl.filterType == futures::SymbolFilter::LOT_SIZE) {
					stepSize = fEl.stepSize;
				} else if (fEl.filterType == futures::SymbolFilter::PRICE_FILTER) {
					tickSize = fEl.tickSize;
				}
			}

			if (tickSize <= 0.0 || stepSize <= 0.0) {
				const auto msg = "Incomplete asset info for: " + std::string(Asset);
				spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
				BrokerError(msg.c_str());
				return 0;
			}

			if (pPip) {
				*pPip = tickSize;
			}

			if (pLotAmount) {
				*pLotAmount = stepSize;
			}

			if (pPipCost) {
				*pPipCost = tickSize * stepSize;
			}

#ifdef _WIN64
			lotAmounts.insert_or_assign(Asset, stepSize);
#endif
		} catch (std::exception &e) {
			spdlog::error(fmt::format("{}: {}\n", MAKE_FILELINE, e.what()));
			BrokerError("Cannot acquire asset info from server.");
			return 0;
		}
	}

	try {
		/// Check if the Book Ticker Stream is subscribed for the Asset
		streamManager->subscribeBookTickerStream(Asset);

		if (subscribing) {
			/// Subscription call - the asset exists and its stream is running, that is all Zorro asks for here.
			/// It must NOT depend on a tick having arrived: an asset that returns 0 after subscription triggers
			/// Error 053 and gets its trading disabled, while an illiquid symbol can stay silent for minutes.
			return 1;
		}

#ifdef EXPERIMENTAL
		/// TODO: Candle interval must correspond to BAR size (but how to set it?)
		streamManager->subscribeCandlestickStream(Asset, CandleInterval::_1m);

		auto candlestick = streamManager->readEventCandlestick(Asset, CandleInterval::_1m);

		if (candlestick) {
			const auto candleMinute = candlestick.value().k.t / 60000;
			auto timeStruct = std::gmtime(&currentS);
			if (timeStruct->tm_sec == 0) {
				if (currentMinute == candleMinute) {
					/// Time mismatch - take previous candle
					const auto it = lastBars.find(Asset);
					if (it != lastBars.end()) {
						candlestick.emplace(it->second);
					}
				}
			} else {
				if (currentMinute == candleMinute) {
					lastBars.insert_or_assign(Asset, candlestick.value());
				}
			}
		}

#endif

		/// Reading falls back to a REST snapshot when the stream stays silent, which is the normal case for an
		/// illiquid symbol - the best bid/ask simply does not change for minutes.
		if (const auto tickPrice = streamManager->readEventTickPrice(Asset)) {
			const auto &tickerPrice = *tickPrice;

			if (tickerPrice.a <= 0.0 || tickerPrice.b <= 0.0) {
				const auto msg = "Invalid bid/ask for Asset: " + std::string(Asset);
				spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
				return 0;
			}

			if (pPrice) {
				*pPrice = tickerPrice.a;
			}
			if (pSpread) {
				*pSpread = tickerPrice.a - tickerPrice.b;
			}
			if (pVolume) {
#ifdef EXPERIMENTAL
				if (candlestick) {
					*pVolume = candlestick->k.v;
				} else {
					*pVolume = tickerPrice.A + tickerPrice.B;
				}
#else
				*pVolume = tickerPrice.A + tickerPrice.B;
#endif
			}

			return 1;
		}
		const auto msg = "Could not read Book Ticker Stream for Asset: " + std::string(Asset) + ", reading timeout";
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		BrokerError("Cannot acquire asset info from server.");
	}

	return 0;
}

DLLFUNC_C int BrokerAccount(char *Account, double *pdBalance, double *pdTradeVal, double *pdMarginVal) {
	if (!restClient) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance Rest Client instance not initialized."));
		return 0;
	}

	if (!Account || !*Account) {
		accountCurrency = "USDT";
	} else {
		accountCurrency = Account;
	}

	try {
		const auto account = restClient->getAccountInfo();

		for (const auto &el: account.assets) {
			if (el.asset != accountCurrency) {
				continue;
			}

			if (pdBalance) {
				*pdBalance = el.walletBalance;
			}

			/// Value of open positions and the margin they bind - without those Zorro cannot track equity
			if (pdTradeVal) {
				*pdTradeVal = el.unrealizedProfit;
			}

			if (pdMarginVal) {
				*pdMarginVal = el.initialMargin;
			}

			return 1;
		}

		const auto msg = "Account currency not found: " + accountCurrency;
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
		BrokerError(msg.c_str());
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		BrokerError("Cannot acquire account info from server.");
	}

	return 0;
}

DLLFUNC_C int BrokerTime(DATE *pTimeGMT) {
	if (!restClient) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance Rest Client instance not initialized."));
		return ExchangeStatus::Unavailable;
	}

#ifdef ENABLE_SERVER_TIME
	// Off by default, to save time, the response takes roughly 0.35 s
	std::int64_t timeInMs = restClient->getServerTime();

	if (pTimeGMT) {
		*pTimeGMT = convertTime(timeInMs);
	}
#endif

	/// Binance never closes
	return ExchangeStatus::Open;
}

DLLFUNC_C int GetLastFundingRate(char *Asset, double *fundingTime, double *fundingRate) {
	if (!Asset || !restClient) {
		return 0;
	}

	try {
		const auto rate = restClient->getLastFundingRate(Asset);
		if (fundingRate && fundingTime) {
			*fundingTime = convertTime(rate.fundingTime);
			*fundingRate = rate.fundingRate;
			return 1;
		}
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
	}
	return 0;
}

DLLFUNC_C int AssetMinuteCandleREST(char *Asset, T6 *candles, int maxCandles) {
	if (!Asset || !restClient) {
		return 0;
	}

	if (candles) {
		const std::time_t from = std::time(nullptr) * 1000 - maxCandles * 60000;
		const std::time_t to = std::time(nullptr) * 1000;

		try {
			const auto bnbCandles = restClient->getHistoricalPrices(Asset, CandleInterval::_1m, from, to, maxCandles);
			const auto maxElements = std::min(maxCandles, static_cast<int>(bnbCandles.size()));

			for (auto i = 0; i < maxElements; i++) {
				candles[i].fOpen = bnbCandles[i].open;
				candles[i].fHigh = bnbCandles[i].high;
				candles[i].fLow = bnbCandles[i].low;
				candles[i].fClose = bnbCandles[i].close;
				candles[i].fVol = bnbCandles[i].volume;
				candles[i].time = convertTime(bnbCandles[i].closeTime);
			}
			return 1;
		} catch (std::exception &e) {
			spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		}
	}

	return 0;
}

DLLFUNC_C int AssetMinuteCandle(char *Asset, int previous, T6 *candle) {
	if (!Asset || !streamManager) {
		return 0;
	}

	if (candle) {
		if (const auto candleEvent = streamManager->readEventCandlestick(
			Asset, CandleInterval::_1m, static_cast<bool>(previous))) {
			candle->fVol = candleEvent->k.v;
			candle->fOpen = candleEvent->k.o;
			candle->fHigh = candleEvent->k.h;
			candle->fLow = candleEvent->k.l;
			candle->fClose = candleEvent->k.c;
			candle->time = convertTime(candleEvent->k.T);
			return 1;
		}
	}

	return 0;
}

DLLFUNC_C int PreloadMinuteCandles(char **Assets, int numAssets, int numCandles) {
	if (!Assets) {
		return 0;
	}

	if (!restClient) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance Rest Client instance not initialized."));
		return 0;
	}

	std::vector<std::string> symbols;

	for (auto i = 0; i < numAssets; i++) {
		symbols.emplace_back(Assets[i]);
	}

	const std::time_t from = std::time(nullptr) * 1000 - numCandles * 60000;
	const std::time_t to = std::time(nullptr) * 1000;

	try {
		lastCandles = restClient->getHistoricalPrices(symbols, CandleInterval::_1m, from, to, numCandles);
		return 1;
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
	}
	return 0;
}

DLLFUNC_C int GetPreloadedMinuteCandles(char *Asset, T6 *candles, int maxCandles, int &numRead) {
	if (!Asset || !candles) {
		return 0;
	}

	if (const auto it = lastCandles.find(Asset); it != lastCandles.end()) {
		const auto maxElements = std::min(maxCandles, static_cast<int>(it->second.size()));
		numRead = maxElements;

		for (auto i = 0; i < maxElements; i++) {
			candles[i].fOpen = it->second[i].open;
			candles[i].fHigh = it->second[i].high;
			candles[i].fLow = it->second[i].low;
			candles[i].fClose = it->second[i].close;
			candles[i].fVol = it->second[i].volume;
			candles[i].time = convertTime(it->second[i].closeTime);
		}
		return 1;
	}
	return 0;
}

DLLFUNC_C int GetMaxPositionValue(char *Asset, double *amount) {
	if (!Asset || !restClient) {
		return 0;
	}

	try {
		const auto positionRisk = restClient->getPositionRisk(Asset);

		if (positionRisk.empty()) {
			spdlog::critical(fmt::format("{}: {}\n", MAKE_FILELINE, "Unknown error"));
			return 0;
		}

		if (amount) {
			*amount = positionRisk[0].maxNotionalValue;
			return 1;
		}
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}\n", MAKE_FILELINE, e.what()));
	}

	return 0;
}

DLLFUNC_C int GetPositionLimits(char *Asset, double *lotSize, double *marketLotSize) {
	if (!Asset || !restClient) {
		return 0;
	}

	try {
		const auto symbolInfo = restClient->getSymbolInfo(Asset);

		if (!symbolInfo) {
			const auto msg = "Unknown asset: " + std::string(Asset);
			spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
			BrokerError(msg.c_str());
			return 0;
		}

		for (const auto &fEl: symbolInfo->filters) {
			if (fEl.filterType == futures::SymbolFilter::LOT_SIZE) {
				if (lotSize) {
					*lotSize = fEl.maxQty;
				}
			} else if (fEl.filterType == futures::SymbolFilter::MARKET_LOT_SIZE) {
				if (marketLotSize) {
					*marketLotSize = fEl.maxQty;
				}
			}
		}
		return 1;
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}\n", MAKE_FILELINE, e.what()));
		BrokerError("Cannot acquire asset info from server.");
	}

	return 0;
}

DLLFUNC_C int ChangeInitialLeverage(char *Asset, int targetLeverage, int *leverage, double *maxNotionalValue) {
	if (!Asset || !restClient) {
		return 0;
	}

	try {
		const auto [fst, snd] = restClient->changeInitialLeverage(Asset, targetLeverage);

		if (leverage) {
			*leverage = fst;
		}

		if (maxNotionalValue) {
			*maxNotionalValue = snd;
		}

		return 1;
	} catch (std::exception &e) {
		std::string msg = "Cannot change initial leverage";
		spdlog::error(fmt::format("{}, {}: {}\n", msg, MAKE_FILELINE, e.what()));
		BrokerError(msg.c_str());
	}

	return 0;
}

DLLFUNC_C int BrokerHistory2(char *Asset, DATE tStart, DATE tEnd, int nTickMinutes, int nTicks, T6 *ticks) {
	if (!Asset || !ticks || !nTicks) {
		return 0;
	}

	if (!restClient) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance Rest Client instance not initialized."));
		return 0;
	}

	try {
		auto candleInterval = CandleInterval::_1m;

		if (!Binance::isValidCandleResolution(nTickMinutes, candleInterval)) {
			std::string msg = "Invalid data resolution: " + std::to_string(nTickMinutes) + " minutes.";
			spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
			BrokerError(msg.c_str());
			return 0;
		}

		const auto msInInterval = Binance::numberOfMsForCandleInterval(candleInterval);
		auto candles = restClient->getHistoricalPrices(Asset, candleInterval, convertTime(tStart) * 1000 - msInInterval,
		                                               convertTime(tEnd) * 1000 - msInInterval, -1);

		if (candles.empty()) {
			std::string msg = "No historical data.";
			spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
			BrokerError(msg.c_str());
			return 0;
		}

		const auto maxCandles = std::min(nTicks, static_cast<int>((candles).size()));
		std::ranges::reverse(candles);

		/// From most recent to oldest.
		for (int i = 0; i < maxCandles; i++, ticks++) {
			ticks->fOpen = static_cast<float>(candles[i].open);
			ticks->fHigh = static_cast<float>(candles[i].high);
			ticks->fLow = static_cast<float>(candles[i].low);
			ticks->fClose = static_cast<float>(candles[i].close);
			ticks->fVol = static_cast<float>(candles[i].volume);

			/// Zorro uses reversed order in time series so that's why...
			ticks->time = convertTime(candles[i].openTime + msInInterval);
		}

		return maxCandles;
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		BrokerError("Cannot acquire historical data from server.");
	}

	return 0;
}

DLLFUNC_C int BrokerBuy2(char *Asset, int Amount, double dStopDist, double Limit, double *pPrice, int *pFill) {
	if (!restClient) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance Rest Client instance not initialized."));
		return 0;
	}

	if (!Asset || !*Asset || Amount == 0) {
		return 0;
	}

	try {
#ifdef _WIN64
		if (auto it = lotAmounts.find(Asset); it != lotAmounts.end()) {
			lotAmount = it->second;
		} else {
			std::string msg = "Cannot find lot amount size for asset: " + std::string(Asset);
			spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
			return 0;
		}
#endif
		spdlog::info("New Order for asset: " + std::string(Asset) + ", amount: " + std::to_string(Amount) + ", size: " +
		             std::to_string(lotAmount * std::abs(Amount)) + ", stop dist:" + std::to_string(Limit) +
		             ", limit: " +
		             std::to_string(Limit));

		futures::Order order;
		order.symbol = Asset;

		if (Amount > 0) {
			order.side = Side::BUY;
		} else {
			order.side = Side::SELL;
		}

		hedge
			? (Amount > 0
				   ? order.positionSide = futures::PositionSide::LONG
				   : order.positionSide = futures::PositionSide::SHORT)
			: order.positionSide = futures::PositionSide::BOTH;

		if (orderType == 1) {
			order.timeInForce = TimeInForce::IOC;
		} else if (orderType == 2) {
			order.timeInForce = TimeInForce::GTC;
		} else {
			order.timeInForce = TimeInForce::FOK;
		}

		/// NOTE: dStopDist is deliberately ignored - this plugin does not place a broker side stop order, so the
		/// stop loss stays with Zorro and is only executed while Zorro is running. See README.
		if (Limit > 0.) {
			order.price = Limit;
			order.type = futures::OrderType::LIMIT;
		} else {
			order.type = futures::OrderType::MARKET;
		}

		order.quantity = lotAmount * std::abs(Amount);
		order.newOrderRespType = OrderRespType::RESULT;

		readLastOrderId();
		order.newClientOrderId = std::to_string(lastOrderId++);
		writeLastOrderId();

		const futures::OrderResponse orderResponse = restClient->sendOrder(order);
		const auto filledLots = static_cast<int>(std::round(orderResponse.executedQty / lotAmount));

		/// A GTC limit order normally comes back as NEW - it rests on the book and must NOT be reported as a
		/// failure, otherwise it stays open on the exchange while Zorro believes nothing happened. A partially
		/// filled IOC comes back as EXPIRED with a non-zero executed quantity - the fill is a real position and
		/// must be reported as well.
		if (orderResponse.orderStatus == futures::OrderStatus::FILLED ||
		    orderResponse.orderStatus == futures::OrderStatus::PARTIALLY_FILLED ||
		    orderResponse.orderStatus == futures::OrderStatus::NEW ||
		    filledLots > 0) {
			const auto tradeId = std::stoi(orderResponse.clientOrderId);

			if (pPrice && orderResponse.avgPrice > 0.0) {
				*pPrice = orderResponse.avgPrice;
			}

			if (pFill) {
				*pFill = filledLots;
			}

			spdlog::info("Order placed for asset: " + std::string(Asset) + ", status: " +
			             std::string(magic_enum::enum_name(orderResponse.orderStatus)) + ", filled size: " +
			             std::to_string(orderResponse.executedQty / lotAmount) + ", price: " +
			             std::to_string(orderResponse.avgPrice) + ", clientId: " + orderResponse.clientOrderId);

			storeOpenTrade(tradeId, Asset, Amount);
			return tradeId;
		}

		std::string msg =
				"Cannot place order: " + std::string(Asset) + ", size: " + std::to_string(Amount) +
				", reason: " + std::string(magic_enum::enum_name(orderResponse.orderStatus));
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
		BrokerError(msg.c_str());
	} catch (const TransportError &e) {
		/// The order may well have reached the exchange - reporting a rejection here would risk a duplicate order.
		/// -2 tells Zorro that the broker did not confirm within the wait time.
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		BrokerError("No confirmation from server, order state unknown.");
		return -2;
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		BrokerError("Cannot send order to server.");
	}

	return 0;
}

DLLFUNC_C int
BrokerSell2(int nTradeId, int nAmount, double Limit, double *pClose, double *pCost, double *pProfit, int *pFill) {
	if (!restClient) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance Rest Client instance not initialized."));
		return 0;
	}

	if (nAmount == 0) {
		return 0;
	}

	try {
		/// Do NOT drop the mapping here - if the closing order fails the trade would become impossible to close
		const auto openTrade = findOpenTrade(nTradeId);

		if (!openTrade) {
			return 0;
		}

		const auto asset = openTrade->asset;

#ifdef _WIN64
		if (auto it = lotAmounts.find(asset); it != lotAmounts.end()) {
			lotAmount = it->second;
		} else {
			std::string msg = "Cannot find lot amount size for asset: " + std::string(asset);
			spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
			return 0;
		}
#endif
		futures::Order order;
		order.symbol = asset;

		if (nAmount > 0) {
			order.side = Side::SELL;
			order.positionSide = futures::PositionSide::LONG;
		} else {
			order.side = Side::BUY;
			order.positionSide = futures::PositionSide::SHORT;
		}

		/// In One-way mode Binance rejects any positionSide other than BOTH (-4061), the closing intent is
		/// expressed by reduceOnly instead
		if (!hedge) {
			order.positionSide = futures::PositionSide::BOTH;
			order.reduceOnly = true;
		}

		if (Limit > 0.) {
			order.price = Limit;
			order.type = futures::OrderType::LIMIT;
		} else {
			order.type = futures::OrderType::MARKET;
		}

		order.quantity = lotAmount * std::abs(nAmount);
		order.newOrderRespType = OrderRespType::RESULT;

		readLastOrderId();
		order.newClientOrderId = std::to_string(lastOrderId++);
		writeLastOrderId();

		order.timeInForce = TimeInForce::GTC;

		const futures::OrderResponse orderResponse = restClient->sendOrder(order);
		const auto filledLots = static_cast<int>(std::round(orderResponse.executedQty / lotAmount));

		/// A partial close (IOC leftovers expired) still closed real lots and must be booked
		if (orderResponse.orderStatus == futures::OrderStatus::FILLED ||
		    orderResponse.orderStatus == futures::OrderStatus::PARTIALLY_FILLED ||
		    orderResponse.orderStatus == futures::OrderStatus::NEW ||
		    filledLots > 0) {
			if (pFill) {
				*pFill = filledLots;
			}

			if (pClose && orderResponse.avgPrice > 0.0) {
				*pClose = orderResponse.avgPrice;
			}

			spdlog::info("Closing order placed for asset: " + std::string(asset) + ", status: " +
			             std::string(magic_enum::enum_name(orderResponse.orderStatus)) + ", filled size: " +
			             std::to_string(orderResponse.executedQty / lotAmount) + ", price: " +
			             std::to_string(orderResponse.avgPrice) + ", clientId: " + orderResponse.clientOrderId);

			/// Book the closed lots against the record, it is dropped once nothing is left open
			if (filledLots > 0) {
				reduceOpenTrade(nTradeId, filledLots);
			}

			/// Zorro keeps addressing the remainder by the original id
			return nTradeId;
		}

		std::string msg =
				"Cannot place order: " + std::string(asset) + ", size: " + std::to_string(nAmount) +
				", reason: " + std::string(magic_enum::enum_name(orderResponse.orderStatus));
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
		BrokerError(msg.c_str());
	} catch (const TransportError &e) {
		/// BrokerSell2 has no return code for "unknown state", so Zorro will retry. A repeated close is bounded by
		/// the position itself - reduceOnly in One-way mode, position reducing side/positionSide in Hedge mode.
		spdlog::error(fmt::format("{}: closing order not confirmed, state unknown: {}", MAKE_FILELINE, e.what()));
		BrokerError("No confirmation from server, close state unknown.");
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
		BrokerError("Cannot close trade.");
	}

	return 0;
}

/**
 * Report the fill state of an order/trade back to Zorro. Without it Zorro cannot tell whether a resting limit order
 * has been filled in the meantime.
 *
 * Return value (Zorro broker API): current fill amount in lots as in BrokerBuy2, -1 when the trade was completely
 * closed, NAY when the state is unavailable, NAY-1 when the order was cancelled or removed by the broker.
 */
DLLFUNC_C int BrokerTrade(int nTradeId, double *pOpen, double *pClose, double *pCost, double *pProfit) {
	if (!restClient) {
		spdlog::critical(fmt::format("{}: {}", MAKE_FILELINE, "Binance Rest Client instance not initialized."));
		return NAY;
	}

	const auto openTrade = findOpenTrade(nTradeId);

	if (!openTrade) {
		/// Nothing open under this id - either it was closed through this plugin, or it is simply not known here
		return wasTradeClosed(nTradeId) ? -1 : NAY;
	}

	try {
#ifdef _WIN64
		if (const auto it = lotAmounts.find(openTrade->asset); it != lotAmounts.end()) {
			lotAmount = it->second;
		} else {
			spdlog::error(fmt::format("{}: Cannot find lot amount size for asset: {}", MAKE_FILELINE,
			                          openTrade->asset));
			return NAY;
		}
#endif
		const auto orderResponse = restClient->queryOrder(openTrade->asset, std::to_string(nTradeId));
		const auto filledLots = static_cast<int>(std::round(orderResponse.executedQty / lotAmount));

		if (pOpen && orderResponse.avgPrice > 0.0) {
			*pOpen = orderResponse.avgPrice;
		}

		/// The queried order carries the size it was opened with, the record carries what is still open after
		/// partial closes. Legacy records have no size, there the order is the only source of truth.
		const auto openLots = openTrade->lots != 0
			                      ? std::min(std::abs(openTrade->lots), filledLots)
			                      : filledLots;

		switch (orderResponse.orderStatus) {
			case futures::OrderStatus::FILLED:
			case futures::OrderStatus::PARTIALLY_FILLED:
				return openLots;
			case futures::OrderStatus::NEW:
			case futures::OrderStatus::PENDING_CANCEL:
				/// Still resting on the book, nothing filled yet
				return 0;
			default:
				/// CANCELED, REJECTED, EXPIRED - whatever got filled before is a real position, if nothing was
				/// filled the order is gone from the broker
				return openLots > 0 ? openLots : NAY - 1;
		}
	} catch (std::exception &e) {
		spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
	}

	return NAY;
}

DLLFUNC_C double BrokerCommand(int Command, DWORD dwParameter) {
	switch (Command) {
		case SET_ORDERTYPE:
			return orderType = dwParameter;
		case SET_DELAY:
			loopMs = dwParameter;
		case GET_DELAY:
			return loopMs;
		case SET_AMOUNT:
#ifndef _WIN64
			lotAmount = *(double *) dwParameter;
#endif
			return 1;
		case SET_WAIT:
			waitMs = dwParameter;

			/// Drives how long a price read may block before falling back to the REST snapshot. Clamped so that a
			/// generous Zorro setting cannot stall the whole asset loop.
			if (streamManager) {
				streamManager->setTimeout(std::clamp(waitMs / 1000, 1, STREAM_READ_TIMEOUT_S));
			}
		case GET_WAIT:
			return waitMs;
		case SET_SYMBOL:
			currentSymbol = reinterpret_cast<char *>(dwParameter);
			return 1;
		case GET_POSITION:
			if (restClient) {
				const char *symbol = reinterpret_cast<char *>(dwParameter);
				try {
					const auto positions = restClient->getPosition(symbol);

					double totalPositionAmt = 0;

					for (const auto &position: positions) {
						totalPositionAmt += position.positionAmt;
					}

					/// Zorro expects the net open amount in the same unit as BrokerBuy2, i.e. in lots, not in
					/// contracts. The lot size is per symbol, the global lotAmount belongs to the last order.
					double symbolLotAmount = lotAmount;
#ifdef _WIN64
					if (const auto it = lotAmounts.find(symbol); it != lotAmounts.end()) {
						symbolLotAmount = it->second;
					} else {
						spdlog::error(fmt::format("{}: Cannot find lot amount size for asset: {}", MAKE_FILELINE,
						                          symbol));
						return 0;
					}
#endif
					if (symbolLotAmount <= 0.0) {
						return 0;
					}

					return totalPositionAmt / symbolLotAmount;
				} catch (std::exception &e) {
					spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
					BrokerError(std::string("Cannot get position of " + std::string(symbol)).c_str());
				}
			}
			break;
		case GET_BROKERZONE:
			return 0; //return 0 for UTC
		case GET_MAXREQUESTS:
			return 10;
		case GET_MAXTICKS:
			return 250;
		case DO_CANCEL:
			if (restClient) {
				try {
					futures::OrderResponse orderResponse = restClient->cancelOrder(currentSymbol,
						std::to_string(dwParameter));

					if (orderResponse.orderStatus == futures::OrderStatus::CANCELED) {
						spdlog::info("Order canceled for asset: " + std::string(currentSymbol) + ", order id: " +
						             orderResponse.clientOrderId);
						return 1;
					}
					std::string msg =
							"Cannot cancel order for asset: " + std::string(currentSymbol) + ", order id: " +
							orderResponse.clientOrderId + ", reason: " + std::string(magic_enum::enum_name(orderResponse.orderStatus));

					spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, msg));
					BrokerError(msg.c_str());
					return 0;
				} catch (std::exception &e) {
					spdlog::error(fmt::format("{}: {}", MAKE_FILELINE, e.what()));
					BrokerError(std::string(
						"Cannot cancel order id " + std::to_string(dwParameter)).c_str());
				}
			}
			return 0;

		default:
			return 0;
	}

	return 0;
}
