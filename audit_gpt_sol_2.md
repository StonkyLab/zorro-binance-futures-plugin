# Kontrola oprav Binance a Bybit Futures pluginů pro Zorro

Datum kontroly: 29. srpna 2026

Kontrolované repozitáře:

- `/home/vitakot/Work/stonky-lab/zorro-binance-futures-plugin`
- `/home/vitakot/Work/stonky-lab/zorro-bybit-futures-plugin`
- vložené knihovny `binance-cpp-api` a `bybit-cpp-api`

Podklad: `fixes_made_by_opus.md` a změny od commitů uvedených v tomto souboru.

## Celkový verdikt

Opravy jsou výrazný posun a velká část z nich je technicky kvalitní. Přesto oba pluginy ještě nejsou „good enough“
pro bezobslužné obchodování s významnějším kapitálem. Pro testnet nebo malý, aktivně hlídaný účet již vhodné jsou.

Orientační hodnocení:

- Binance plugin: **7/10**
- Bybit plugin: **6/10**

Důvodem nejsou chybějící procenta test coverage. Zůstaly konkrétní chyby ve sledování close orderů a v některých
větvích rekonciliace neznámého výsledku. Ty mohou vést k fantomovému obchodu v Zorru, duplicitnímu close orderu nebo
živému orderu, který plugin považuje za nevyplněný.

## Blokující nálezy

### P0-1: Čekající a neznámé close ordery se stále nesledují

Původní audit požadoval evidovat nejen entry order, ale také všechny close ordery a jejich kumulativní filly. Oprava
zavedla kumulativní pole `closed`, ale aktualizuje ho pouze f illem dostupným přímo v synchronní odpovědi
`BrokerSell2`.

Binance:

- Close order dostane nové `newClientOrderId`, ale toto ID se do `TradeStore` neuloží
  ([`src/binance_futures.cpp`](src/binance_futures.cpp#L1207)).
- Stav `NEW` je považován za úspěšně umístěný close order
  ([`src/binance_futures.cpp`](src/binance_futures.cpp#L1216)).
- Do `closed` se započítá pouze množství vyplněné v okamžité odpovědi
  ([`src/binance_futures.cpp`](src/binance_futures.cpp#L1233)).
- `BrokerTrade` později dotazuje pouze původní entry order podle původního trade ID
  ([`src/binance_futures.cpp`](src/binance_futures.cpp#L1290)).

Bybit má stejný model:

- Close order dostane nové `orderLinkId`, které se nepersistuje
  ([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1057)).
- `New` close order je vrácen Zorru jako platný, ale započítá se jen okamžitý fill
  ([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1089)).
- `BrokerTrade` sleduje pouze entry `orderLinkId`
  ([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1160)).

Důsledky:

1. Limitní GTC close je nejprve `NEW` a `pFill` je 0.
2. Order se později na burze vyplní.
3. Lokální `closed` se nezmění, protože plugin close order znovu nedotazuje.
4. `BrokerTrade` dál hlásí původní entry fill jako otevřenou pozici.
5. Zorro může držet fantomový obchod nebo odeslat další close.

Stejná nekonzistence vznikne, když close order proběhne na burze, ale jeho odpověď se ztratí. `BrokerSell2` při
`UnknownOutcomeError` close order podle jeho známého client ID nerekonciluje. Opakovaný reduce-only close sice brání
překlopení reálné pozice, ale neopraví lokální evidenci prvního fillu.

Původní kritický nález P0-2 proto není plně vyřešen.

### P0-2: Reconciliation může vrátit 0, i když je order stále živý

Oba pluginy po neznámém výsledku entry order vyhledají a pokusí se jej zrušit. To je správný směr. Po cancelu však
provedou pouze jediný okamžitý re-query a neověří, že výsledný stav je terminální.

Binance cesta:

- živý order je rozpoznán a odešle se cancel
  ([`src/binance_futures.cpp`](src/binance_futures.cpp#L971));
- selhání cancelu se pouze zaloguje a pokračuje se
  ([`src/binance_futures.cpp`](src/binance_futures.cpp#L976));
- po jediném re-query se při nulovém fillu vrátí 0, i když odpověď může být stále `NEW`
  ([`src/binance_futures.cpp`](src/binance_futures.cpp#L983),
  [`src/binance_futures.cpp`](src/binance_futures.cpp#L1006)).

Bybit cesta funguje analogicky
([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L790),
[`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L800)).

U Bybitu je tento problém obzvlášť pravděpodobný: API výslovně uvádí, že potvrzení cancel requestu je asynchronní a
konečný stav se má potvrdit order streamem nebo následným pollingem.

Další problém v obou implementacích: obecná `std::exception` během reconciliation je interpretována jako „order není
na burze“ a vede k návratu 0. Chyba autentizace, rate limit, neplatná odpověď nebo jiná chyba dotazu ale existenci
orderu nerozhoduje.

Bybit navíc po normálně přijatém `placeOrder`, jehož stav se během poll budgetu nepodařilo zjistit, vrací `-2` bez
pokusu order zrušit
([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L935),
[`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L973)).

Zorro Broker API přitom požaduje, aby plugin před návratem `-2` neznámý order zrušil. Pokud není možné potvrdit jeho
zrušení, nesmí být stav prezentován jako běžné zamítnutí.

Reference:

- [Zorro Broker API](https://zorro-project.com/manual/en/brokerplugin.htm)
- [Bybit Cancel Order](https://bybit-exchange.github.io/docs/v5/order/cancel-order)
- [Bybit Place Order](https://bybit-exchange.github.io/docs/v5/order/create-order)

### P1-1: Bybit `BrokerTrade` ignoruje close u terminálního částečného entry fillu

Bybit `BrokerTrade` má zvláštní časnou návratovou větev pro entry order, jehož stav již není považován za live
([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1164)).

Pokud je například derivátový IOC order zrušen s nenulovým `cumExecQty`, funkce vrátí celý entry fill na řádku 1168.
Neprovede následující výpočet `entryLots - openTrade->closed`, který začíná až na řádku 1202.

Scénář:

1. Entry IOC požaduje 10 lotů.
2. Vyplní se 3 loty a zbytek skončí jako `Cancelled`.
3. `BrokerSell2` všech 3 vyplněných lotů zavře a `closed` je 3.
4. `BrokerTrade` najde terminální entry order a vrátí původní fill 3 bez odečtení `closed`.
5. Zorro stále vidí 3 otevřené loty, přestože burzovní pozice je nulová.

Bybit dokumentuje, že derivátový order ve stavu `Cancelled` může mít nenulové vyplněné množství:
[Bybit enum definitions](https://bybit-exchange.github.io/docs/v5/enum).

## Vysoká a střední reziduální rizika

### P1-2: Binance nerozlišuje všechny relevantní timeoutové odpovědi

`isExecutionUnknown` označuje jako neznámý výsledek všechny HTTP 5xx a API kódy `-1006` a `-1007`, ale samotný
HTTP 408 bez těchto API kódů nikoliv
([`binance_futures_rest_client.cpp`](binance-cpp-api/src/binance_futures_rest_client.cpp#L128)).

Binance Futures dokumentuje HTTP 408 jako timeout při čekání na backend. Pro order request je bezpečnější tento stav
rovněž rekoncilovat, ne jej obecnou výjimkou převést na zamítnutí.

Reference: [Binance Futures General Info](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info).

### P2-1: `BrokerAccount` stále interpretuje account ID jako měnu

Zorro předává do `BrokerAccount` jméno nebo číslo účtu. Oba pluginy tento parametr používají jako symbol měny:

- Binance: [`src/binance_futures.cpp`](src/binance_futures.cpp#L611)
- Bybit: [`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L571)

S prázdným Account polem nebo hodnotou `USDT` to funguje, ale obecný account identifikátor způsobí hledání
neexistující měny. Pro současný způsob použití může být toto omezení přijatelné, mělo by však být jasně uvedeno v
konfiguraci nebo README.

### P2-2: Čistý build testovacích executable neprojde

Samotné statické knihovny `binance_api` a `bybit_api` se s GCC 15 a C++23 úspěšně sestavily. Při konfiguraci s
`ENABLE_TESTS=ON` ale následně selhaly testovací executable:

- `binance_test`: nenalezeno `stonky/interface/i_json.h`
- `bybit_test`, `bybit_benchmark`, `bybit_exec_probe`: nenalezeno `stonky/utils/magic_enum_wrapper.hpp`

Důvodem je, že veřejné hlavičky API knihoven používají hlavičky `stonky_common`, ale závislost je linkována jako
`PRIVATE`
([`binance-cpp-api/CMakeLists.txt`](binance-cpp-api/CMakeLists.txt#L64),
[`bybit-cpp-api/CMakeLists.txt`](../zorro-bybit-futures-plugin/bybit-cpp-api/CMakeLists.txt#L83)).

Nejde o překážku live provozu a není nutné kvůli tomu budovat rozsáhlou testovací infrastrukturu. Znamená to však,
že tvrzení ve `fixes_made_by_opus.md`, že oba testovací programy buildí, v čistém samostatném CMake buildu neplatí.

## Co bylo opraveno dobře

Následující opravy byly v kódu skutečně nalezeny a jejich provedení je pro zamýšlený rozsah rozumné:

- Subscription illiquid symbolu už nezávisí na příchodu prvního WebSocket ticku.
- Cenová cache se při subscription seeduje z REST a stale cena má REST fallback.
- Timeout price readu používá skutečný `steady_clock` deadline.
- REST transport rozlišuje `UnknownOutcomeError` od běžného zamítnutí.
- Signed requesty kompenzují rozdíl lokálního a burzovního času.
- `recvWindow` bylo sníženo na rozumných 5 sekund.
- Read/write socket operace pluginů na Windows mají timeout odvozený od `SET_WAIT`.
- Výměna credentials používá atomický `shared_ptr` a odstraňuje původní data race.
- `BrokerTime` kontroluje čerstvou komunikaci a při tichu provádí omezený probe.
- Binance znovu zjišťuje position mode při loginu a One-way close používá `BOTH + reduceOnly`.
- `GET_POSITION` vrací loty namísto raw contracts.
- `GET_COMPLIANCE` je implementován alespoň pro jednoznačně zjištěný position mode.
- Zápis trade store přes dočasný soubor a `std::filesystem::rename` je pro tento rozsah přiměřená oprava původního
  přímého truncating zápisu. MSVC STL při rename používá replace-existing chování.
- Balance již není zaokrouhlován na celé jednotky a plugin vrací unrealized PnL a vázanou margin.
- Visual Studio projekty ukazují na existující zdroje a Bybit linkuje zlib.
- Dokumentace otevřeně přiznává chybějící broker-side stop loss.

## Přijatelná známá omezení

Následující body není pro „good enough“ release nutné opravovat, pokud odpovídají reálnému způsobu nasazení:

- žádný broker-side stop loss;
- persistence a order counter nejsou oddělené podle účtu;
- jeden WebSocket na symbol u Binance;
- rozdílný význam live a historického volume;
- connect timeout na Windows zůstává závislý na operačním systému;
- současná historická interpretace `SET_ORDERTYPE`;
- absence rozsáhlého unit-test coverage.

Per-account persistence je přijatelná pouze tehdy, pokud jedna kopie pluginu používá jeden účet a souběžně neběží
další kopie zapisující do stejného store a registry counteru.

## Minimální release gate

Před běžným live provozem doporučuji pouze tyto cílené kroky:

1. **Close ordery:** ukládat jejich client ID a kumulativní fill. Jednodušší alternativa pro první stabilní release
   je close limit ordery vůbec nedržet jako GTC, ale používat market/IOC a před návratem vždy dojít do terminálního
   stavu.
2. **Unknown entry:** po cancelu pollovat do terminálního stavu. Pokud je cancel nebo query neprůkazný, nikdy nevracet
   `0`; stav musí zůstat neznámý a order nesmí být považován za zamítnutý.
3. **Unknown close:** rekoncilovat podle předem známého close client ID a započítat všechny zjištěné filly.
4. **Bybit partial/cancel:** odstranit časný return v `BrokerTrade` a pro všechny terminální stavy použít stejný
   výpočet `entry fill - closed`.
5. Projít malou sadu manuálních nebo integračních scénářů, není nutná plošná testovací sada:
   - market open a close long/short;
   - partial IOC entry a úplný close;
   - GTC entry: bez fillu, partial fill, full fill a cancel;
   - GTC close, pokud má zůstat podporován;
   - timeout po odeslání entry i close orderu;
   - dva obchody, restart Zorra a následný close obou obchodů.

Po opravě prvních čtyř bodů a úspěšném průchodu této malé matice bych oba pluginy považoval za prakticky „good
enough“ bez požadavku na rozsáhlé test coverage.

## Provedené ověření

- Prošel jsem `fixes_made_by_opus.md` a reálné diffy obou pluginů i vložených API knihoven.
- Kritické order cesty byly porovnány s aktuální dokumentací Zorro, Binance Futures a Bybit V5.
- `binance_api` se úspěšně sestavilo s GCC 15 a C++23.
- `bybit_api` se úspěšně sestavilo s GCC 15 a C++23.
- Buildy s `ENABLE_TESTS=ON` odhalily výše popsané chybějící transitivní include cesty.
- `cppcheck` nenašel další nový zásadní defekt; analýzu částečně omezily externí include a Windows-specifické části.
- Nebyl proveden MSVC link, spuštění DLL v Zorru ani obchodní test s credentials.
- Kontrola zdrojové soubory pluginů nezměnila.
