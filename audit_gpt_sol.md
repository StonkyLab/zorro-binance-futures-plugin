# Audit Binance Futures pluginu pro Zorro

Datum auditu: 28. srpna 2026

## Shrnutí

Plugin bych v současném stavu ještě nenasazoval na účet s významnějším kapitálem. Audit odhalil dvě kritické a několik vysokých rizik, především v zacházení s neznámým výsledkem objednávky, evidenci částečných fillů a close orderů a v zotavení po ztrátě spojení.

Nejvyšší prioritu mají:

1. Rozlišování zamítnuté objednávky od objednávky s neznámým výsledkem.
2. Perzistentní stavový automat pro entry a close ordery.
3. Skutečné síťové timeouty a korektní implementace `BrokerTime`.
4. Atomická persistence oddělená podle účtu.

## Kritické nálezy

### P0-1: Neznámý výsledek objednávky je často považován za zamítnutí

`checkResponse` v [`binance_futures_rest_client.cpp`](binance-cpp-api/src/binance_futures_rest_client.cpp#L114) převádí každý neúspěšný HTTP status na obecnou `std::runtime_error`. `BrokerBuy2` zachytí jako neznámý výsledek pouze `TransportError`; ostatní výjimky skončí návratovou hodnotou `0`, tedy „order rejected“ ([`binance_futures.cpp`](src/binance_futures.cpp#L964)).

Binance však u HTTP 408, některých variant HTTP 503 a chyb `-1006` a `-1007` uvádí, že stav exekuce je neznámý. Order mohl být na burze přijat a před opakováním je nutné ověřit jeho stav přes user data stream nebo dotazem podle `clientOrderId`.

Další související problémy:

- REST sockety nemají connect, write ani read deadline ([`binance_http_session.cpp`](binance-cpp-api/src/binance_http_session.cpp#L201)). Synchronní operace proto může blokovat Zorro neomezeně dlouho.
- `SET_WAIT` ovlivňuje pouze čtení cen z WebSocket vrstvy, nikoliv odesílání objednávek ([`binance_futures.cpp`](src/binance_futures.cpp#L1166)).
- Při `TransportError` vrátí `BrokerBuy2` hodnotu `-2`, ale známý `newClientOrderId` se následně nedohledá ani nezruší ([`binance_futures.cpp`](src/binance_futures.cpp#L925)).
- Zorro Broker API požaduje, aby plugin po návratu `-2` neznámý order zrušil.

Důsledek: osiřelé nebo duplicitní objednávky a pozice; při síťové poruše také možné zamrznutí hlavního vlákna Zorra.

Doporučení:

- Persistovat `clientOrderId` ještě před odesláním objednávky.
- Zavést samostatnou kategorii chyby `ExecutionUnknown` pro HTTP 408, relevantní HTTP 503 a Binance `-1006/-1007`.
- Po neznámém výsledku dotazovat order podle `clientOrderId`; pokud existuje a není terminální, zrušit jej a znovu ověřit konečný fill.
- Nastavit connect/write/read deadline odvozené od `SET_WAIT`.
- Neopakovat objednávku, dokud předchozí pokus nemá známý terminální stav.

Reference:

- [Binance Futures General Info](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)
- [Binance Futures Error Codes](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/error-code)
- [Zorro Broker API](https://zorro-project.com/manual/en/brokerplugin.htm)

### P0-2: Evidence partial fillů a čekajících close orderů je chybná

Při úspěšném nebo částečně vyplněném vstupu se do lokálního store uloží původně požadovaný `Amount`, nikoliv skutečný počet vyplněných lotů ([odeslání a výpočet fillu](src/binance_futures.cpp#L929), [uložení](src/binance_futures.cpp#L955)).

Příklad:

1. IOC požadavek je na 10 lotů.
2. Burza vyplní pouze 3 loty.
3. Store přesto eviduje 10 lotů.
4. Následné zavření všech 3 lotů odečte 3 od 10 a store eviduje 7.
5. `BrokerTrade` dotáže původní vstupní order, zjistí kumulativní fill 3 a znovu hlásí 3 otevřené loty, přestože je pozice na burze nulová.

Čekající close limit ordery mají další problémy:

- `BrokerSell2` nastavuje pro limitní close vždy `GTC`, bez ohledu na `SET_ORDERTYPE` ([`binance_futures.cpp`](src/binance_futures.cpp#L1040)).
- Nový `clientOrderId` close orderu se nepersistuje.
- Při odpovědi `NEW` je vráceno původní trade ID a fill `0`, ale plugin nemá mechanismus pro sledování tohoto close orderu.
- `BrokerTrade` později dotazuje pouze původní vstupní order ([`binance_futures.cpp`](src/binance_futures.cpp#L1120)).

Plugin proto nedokáže zjistit, že čekající close order byl později částečně nebo úplně vyplněn. Zorro může obchod dál považovat za otevřený a odeslat další close order.

Doporučení:

- Nahradit `OpenTrade { asset, lots }` explicitním stavem obsahujícím alespoň původně požadované množství, kumulativní entry fill, kumulativně zavřené množství a seznam aktivních close orderů.
- Persistovat exchange `orderId` i `clientOrderId` každého entry a close orderu.
- Aktualizovat stav podle kumulativních fillů, nikoliv pouze podle okamžité odpovědi na `sendOrder`.
- Pro `BrokerTrade` kombinovat stav orderů s aktuální pozicí; počítat také s manuálním zavřením, likvidací a změnou pozice mimo plugin.
- Zabránit vytvoření dalšího close orderu, dokud existuje předchozí nepotvrzený nebo otevřený close order.

## Vysoká rizika

### P1-1: `BrokerTime` hlásí burzu jako otevřenou i bez spojení

[`BrokerTime`](src/binance_futures.cpp#L587) kontroluje pouze existenci objektu `restClient`. Pokud byl klient jednou vytvořen, funkce vždy vrátí `ExchangeStatus::Open`, tedy hodnotu `2`, bez ping požadavku, kontroly poslední úspěšné REST odpovědi nebo stavu streamu.

Zorro používá `BrokerTime` pro detekci výpadku spojení, pozastavení obchodování a opakovaný login. Při výpadku sítě nebo Binance proto nemusí korektně přejít do offline režimu ani obnovit spojení.

Doporučení:

- Udržovat timestamp poslední úspěšné REST nebo WebSocket komunikace.
- Vrátit `0`, pokud je spojení prokazatelně ztracené nebo nebyla po definovanou dobu přijata platná odpověď.
- Volitelně provést krátký server-time nebo ping request s tvrdým timeoutem.

### P1-2: Persistence obchodů není atomická ani oddělená podle účtu

Store se zapisuje přímým otevřením cílového souboru [`binance_open_trades.json`](src/binance_futures.cpp#L240). Otevření soubor nejprve zkrátí; pád procesu, výpadek disku nebo přerušení zápisu tak může zanechat neplatný či prázdný JSON. `loadTrades` při neplatném JSON vrátí prázdný store ([`binance_futures.cpp`](src/binance_futures.cpp#L190)).

Selhání zápisu je pouze zalogováno. `BrokerBuy2` přesto vrátí trade ID jako úspěšné, takže po restartu nemusí existovat mapování potřebné pro `BrokerSell2`.

Soubor i registry key jsou navíc společné pro všechny účty a kopie pluginu ([konstanty](src/binance_futures.cpp#L32)). Přepnutí API klíčů nebo paralelní použití více účtů může způsobit kolize trade ID a načtení stavu jiného účtu.

Doporučení:

- Zapisovat do dočasného souboru, flushnout data a atomicky jej přejmenovat přes původní soubor.
- Udržovat záložní kopii posledního validního store.
- Oddělit store a order counter podle stabilního, nevratného identifikátoru účtu; nepoužívat samotný API secret ani jej neukládat.
- Serializovat přístup napříč vlákny a procesy.
- Pokud nelze kritický stav objednávky bezpečně persistovat, nepovažovat operaci za dokončenou bez následné rekonciliace.

### P1-3: One-way mode není plně kompatibilní s logickým modelem Zorra

Plugin v One-way mode správně posílá `positionSide=BOTH`, ale neimplementuje `GET_COMPLIANCE` a Zorru nesdělí, že burzovní účet nepodporuje současnou long a short pozici. `BrokerBuy2` rovněž nebrání otevření opačné strany ([`binance_futures.cpp`](src/binance_futures.cpp#L899)).

Opačný vstup na burze znetuje existující pozici, zatímco lokální trade store dál eviduje dva samostatné obchody. Deklarace v README, že One-way mode je podporován, je proto bez dalších omezení příliš silná.

Doporučení:

- Implementovat `GET_COMPLIANCE` nebo jinak zajistit, že Zorro používá kompatibilní non-hedge režim.
- Před opačným vstupem ověřit aktuální net position a jasně definovat, zda jde o close, reverse, nebo nový obchod.
- Doplnit integrační testy pro více logických obchodů stejného symbolu a pro změnu směru.

### P1-4: Opakovaný login obsahuje race condition a „sticky“ hedge mode

Zorro může při ztrátě spojení volat `BrokerLogin` opakovaně. Pokud už `restClient` existuje, zavolá se `setCredentials` ([`binance_futures.cpp`](src/binance_futures.cpp#L362)). Tato funkce bez synchronizace nahrazuje `shared_ptr<HTTPSession>` ([`binance_futures_rest_client.cpp`](binance-cpp-api/src/binance_futures_rest_client.cpp#L146)), zatímco background updater může stejný pointer současně číst ([`binance_futures.cpp`](src/binance_futures.cpp#L118)). Jde o datový race s nedefinovaným chováním.

Globální `hedge` se navíc při loginu pouze nastaví na `true`, pokud Binance vrátí Hedge mode, ale před dalším zjištěním se nikdy nenastaví zpět na `false` ([`binance_futures.cpp`](src/binance_futures.cpp#L380)). Po změně účtu nebo position mode proto může plugin dál posílat hedge parametry a dostávat Binance chybu `-4061`.

Doporučení:

- Před změnou credentials zastavit updater a operace používající starou session, případně použít synchronizovaný immutable snapshot klienta.
- Nastavit `hedge = (positionMode == PositionMode::Hedge)` při každém úspěšném loginu.
- Při neúspěšném loginu uklidit částečně vytvořený stav, případně jej explicitně označit jako disconnected.

## Střední a provozní rizika

### P2-1: `BrokerAccount` zaměňuje account ID za měnu

[`BrokerAccount`](src/binance_futures.cpp#L540) interpretuje parametr `Account` jako symbol margin assetu. Podle Zorro Broker API ale tento parametr představuje jméno nebo číslo účtu. Standardní hodnota například `0` proto způsobí hledání assetu `0` a chybu „Account currency not found“.

Výchozí `USDT` funguje pouze tehdy, když Zorro předá prázdný nebo nulový account string, případně když uživatel nestandardně použije `USDT` jako account ID.

Doporučení:

- Pro jediné API konto parametr ignorovat a používat explicitně nakonfigurovanou account currency.
- Pokud má plugin podporovat více margin assetů, oddělit jejich konfiguraci od Zorro account ID.

### P2-2: `recvWindow` je nastaveno na maximálních 60 sekund

[`addTimestampToTargetPath`](binance-cpp-api/src/binance_http_session.cpp#L314) přidává ke každému signed requestu `recvWindow=60000`. Binance Futures používá výchozí hodnotu 5000 ms a zdůrazňuje, že `recvWindow` určuje, jak dlouho smí být opožděný request ještě proveden.

Při správně fungující synchronizaci času není minutové okno nutné a umožňuje provedení výrazně zastaralé objednávky.

Doporučení: použít konfigurovatelnou hodnotu přibližně 5000 ms a držet ji nezávisle na síťovém response timeoutu.

### P2-3: WebSocket vrstva vytváří jedno spojení pro každý symbol

Každé volání `bookTicker` vytváří samostatný `WebSocketSession` ([`binance_futures_ws_client.cpp`](binance-cpp-api/src/binance_futures_ws_client.cpp#L129)). Pro větší asset universe to znamená desítky až stovky TCP/TLS spojení a reconnect pokusů.

Aktuální Binance API podporuje multiplexované streamy i all-bookTicker stream. Pro tento plugin by byl vhodnější jeden multiplexovaný stream nebo centrální stream s filtrací symbolů.

### P2-4: Live a historický volume údaj nemají stejnou sémantiku

`BrokerAsset` vrací jako `pVolume` součet či akumulaci best bid/ask quantities z `bookTicker` ([`binance_futures.cpp`](src/binance_futures.cpp#L516)). `BrokerHistory2` naproti tomu vrací skutečný zobchodovaný kline volume.

Zorro doporučuje, aby live `pVolume` a historické `T6.fVol` měly konzistentní význam. Současná implementace navíc opakovaným sčítáním viditelné top-of-book likvidity nevytváří skutečný obchodní objem.

### P2-5: Chybí automatické testy order state machine

`ENABLE_TESTS` sestaví pouze ruční integrační executable. Testy obsahují hardcoded placeholder pro credentials a nemají assertions ani CTest registraci ([`binance-cpp-api/CMakeLists.txt`](binance-cpp-api/CMakeLists.txt#L58), [`test/main.cpp`](binance-cpp-api/test/main.cpp#L45)).

Nejdůležitější chování — partial fills, HTTP 503, timeout po odeslání requestu, restart procesu, čekající close a změna position mode — není automaticky testováno.

Doporučení: oddělit transport za rozhraní a vytvořit deterministický mock server nebo fake REST klient pro testování stavového automatu bez reálného účtu.

## Známé provozní omezení

Plugin neposílá stop-loss na burzu a `dStopDist` záměrně ignoruje ([`binance_futures.cpp`](src/binance_futures.cpp#L913)). Stop je obsluhován pouze Zorrem. Při pádu Zorra, VPS, síťového spojení nebo pluginu proto zůstane pozice bez ochranného orderu na burze.

Nejde o skrytou chybu — omezení je uvedeno v README — ale pro live provoz je to významné reziduální riziko i po opravě ostatních nálezů.

## Pozitivní zjištění

- REST i WebSocket spojení zapínají peer certificate verification a hostname verification.
- API secret se v kontrolovaných logovacích cestách přímo nevypisuje.
- Signed requesty kompenzují rozdíl mezi lokálním a Binance časem.
- Market data cache odmítá příliš staré tickery a používá REST snapshot fallback.
- GTC entry order ve stavu `NEW` už není chybně hlášen jako zamítnutý.
- Používá se deterministický numeric `newClientOrderId`, což umožňuje budoucí rekonciliaci neznámých výsledků.
- Repozitář během auditu neobsahoval commitnuté API ani privátní klíče.

## Provedené ověření

- Pracovní strom byl před auditem čistý; audit zdrojové soubory neměnil.
- `binance_api` a `stonky_common` se úspěšně sestavily s GCC 15 a C++23.
- `cppcheck` nenašel další zásadní defekty. Upozornil především na stínění `orderId` v `OrderResponse` a na upozornění v externím `date.h`.
- Starší WebSocket cesta `/ws/...`, kterou plugin používá, i nová dokumentovaná `/public/ws/...` dne 28. srpna 2026 úspěšně prošly Binance handshake s odpovědí `101 Switching Protocols`.
- Plnou Windows x86 DLL nebylo v linuxovém auditním prostředí možné sestavit a spustit v Zorro Traderu.
- Nebyly prováděny obchodní požadavky ani testy s reálnými API credentials.

## Doporučené pořadí nápravy

1. Implementovat `ExecutionUnknown`, síťové deadline a rekonciliaci/cancel podle předem persistovaného `clientOrderId`.
2. Nahradit současný `TradeStore` stavovým automatem, který eviduje entry i close ordery a kumulativní filly.
3. Zavést atomickou a per-account persistenci.
4. Opravit `BrokerTime`, login lifecycle, synchronizaci session a reset position mode.
5. Vyjasnit a vynutit podporovaný Zorro režim pro Binance One-way účty.
6. Doplnit mockované automatické testy všech chybových a restart scénářů.
7. Teprve poté provést end-to-end test na Binance demo/testnet prostředí a Zorro `TradeTest`.
