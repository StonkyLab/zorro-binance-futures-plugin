/**
Binance Futures Zorro Plugin

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef BINANCE_FUTURES_ZORRO_PLUGIN_H
#define BINANCE_FUTURES_ZORRO_PLUGIN_H

#include <wtypes.h>
#include "stdafx.h"
#include <zorro/trading.h>

#ifdef BINANCE_FUTURES_DLL_EXPORTS
#define DLLFUNC extern __declspec(dllexport)
#define DLLFUNC_C extern "C" __declspec(dllexport)
#else
#define DLLFUNC extern __declspec(dllimport)
#define DLLFUNC_C extern "C" __declspec(dllimport)
#endif

int (__cdecl *BrokerError)(const char *txt) = nullptr;

int (__cdecl *BrokerProgress)(const int percent) = nullptr;

// zorro functions
DLLFUNC_C int BrokerOpen(char *Name, FARPROC fpError, FARPROC fpProgress);
DLLFUNC_C int BrokerLogin(char *User, char *Pwd, char *Type, char *Account);
DLLFUNC_C int BrokerTime(DATE *pTimeGMT);
DLLFUNC_C int BrokerAsset(char *Asset, double *pPrice, double *pSpread, double *pVolume, double *pPip, double *pPipCost,
                          double *pLotAmount, double *pMarginCost, double *pRollLong, double *pRollShort);
DLLFUNC_C int BrokerHistory2(char *Asset, DATE tStart, DATE tEnd, int nTickMinutes, int nTicks,
                             T6 *ticks);  // only supports stocks, no option history available.
DLLFUNC_C int BrokerBuy2(char *Asset, int Amount, double dStopDist, double Limit, double *pPrice, int *pFill);
DLLFUNC_C double BrokerCommand(int Command, DWORD dwParameter);
DLLFUNC_C int BrokerAccount(char *Account, double *pdBalance, double *pdTradeVal, double *pdMarginVal);
DLLFUNC_C int BrokerSell2(int nTradeId, int nAmount, double Limit, double *pClose, double *pCost, double *pProfit, int *pFill);
DLLFUNC_C int BrokerTrade(int nTradeId, double *pOpen, double *pClose, double *pCost, double *pProfit);

DLLFUNC_C int AssetMinuteCandle(char *Asset, int previous, T6 *candle);
DLLFUNC_C int AssetMinuteCandleREST(char *Asset, T6 *candles, int maxCandles);
DLLFUNC_C int GetLastFundingRate(char *Asset, double *fundingTime, double *fundingRate);
DLLFUNC_C int PreloadMinuteCandles(char **Assets, int numAssets, int numCandles);
DLLFUNC_C int GetPreloadedMinuteCandles(char *Asset, T6 *candles, int maxCandles, int &numRead);
DLLFUNC_C int GetMaxPositionValue(char *Asset, double *amount);
DLLFUNC_C int GetPositionLimits(char *Asset, double *lotSize, double *marketLotSize);
DLLFUNC_C int ChangeInitialLeverage(char *Asset, int targetLeverage, int *leverage, double *maxNotionalValue);

#endif //BINANCE_FUTURES_ZORRO_PLUGIN_H