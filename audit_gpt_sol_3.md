# Kontrola druhého kola oprav Binance a Bybit Futures pluginů pro Zorro

Datum kontroly: 29. srpna 2026

Kontrolované repozitáře a opravné commity:

- `zorro-binance-futures-plugin`: `8959f4b`
- `binance-cpp-api`: `28bac5c`
- `zorro-bybit-futures-plugin`: `7866695`
- `bybit-cpp-api`: `b50b985`

Podklad: [`fixes_made_by_opus_2.md`](fixes_made_by_opus_2.md), který odpovídá uživatelem zmíněnému
`fixes_made_by_opus_v2.md`.

## Celkový verdikt

Druhé kolo oprav je technicky kvalitní a oba pluginy výrazně posunulo. Tvrzení reportu, že byly vyřešeny všechny
nálezy předchozího auditu, ale neodpovídá skutečnému Bybit kódu.

Orientační hodnocení:

- Binance plugin: **8/10**
- Bybit plugin: **6,5/10**

Binance je při jednom účtu, jedné instanci pluginu a omezeném počtu současných pozic podmíněně „good enough“ po
krátkém testnet smoke testu. Bybit bych před běžným live provozem ještě neuvolnil: zůstaly v něm dvě konkrétní cesty,
které mohou ponechat živý order na burze nebo jej nesprávně vyhodnotit jako nevyplněný.

Nejde o požadavek na vysoké test coverage. Jde o několik úzkých větví stavového automatu, jejichž chyba může vytvořit
orphan order nebo rozjet lokální stav Zorra proti skutečné burzovní pozici.

## Závažné nálezy

### P0-1: Bybit stále může vrátit `-2` bez pokusu o cancel

Report tvrdí, že Bybit cesta po vyčerpání poll budgetu nyní prochází přes společný `resolveOrderOutcome()` a před
návratem `-2` se pokusí order zrušit
([`fixes_made_by_opus_2.md`](fixes_made_by_opus_2.md#L61)). Skutečný kód však obsahuje dvě přímé návratové větve:

1. `placeOrder` vrátí přijatý order, ale `pollOrderState` během deseti pokusů nenajde žádný stav. `BrokerBuy2` vrátí
   `-2` přímo a `resolveOrderOutcome` ani `cancelOrder` nezavolá
   ([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1011)).
2. Polling vidí neterminální stav, například `Created` nebo `PendingCancel`, ale stav se během budgetu nezmění.
   Funkce opět vrátí `-2` přímo
   ([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1049)).

To je přesně P0-2c z předchozího auditu, který měl být podle reportu opraven. Zorro u návratu `-2` výslovně požaduje,
aby se plugin pokusil neznámý order zrušit. Jinak může zůstat živý orphan order a další pokus strategie otevře
duplicitní pozici.

Reference: [Zorro Broker API](https://zorro-project.com/manual/en/brokerplugin.htm).

Minimální oprava: obě přímé `return -2` větve vést přes `reconcileUnknownOrder(Asset, tradeId, Amount, pPrice,
pFill)`, případně přímo přes `resolveOrderOutcome(..., true)`, a `-2` vrátit až tehdy, když se nepodařilo potvrdit ani
terminální stav, ani zrušení.

### P0-2: Prázdná Bybit realtime odpověď je stále vyhodnocena příliš definitivně

`resolveOrderOutcome` se při prázdném výsledku `getOpenOrder` dotáže na executions. Poté okamžitě vrátí součet fillů,
a to i tehdy, když je seznam executions prázdný
([`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L860)).

Prázdný realtime dotaz a zatím prázdná execution history ale nerozlišují tyto stavy:

- order nikdy nevznikl;
- nově přijatý, dosud nevyplněný order ještě není v dotazovacím endpointu vidět;
- realtime endpoint má dočasné zpoždění;
- order byl terminálně zrušen bez fillu.

Bybit u `/v5/order/realtime` výslovně upozorňuje na možná zpoždění při vysoké volatilitě. Cache terminálních orderů se
navíc po restartu služby maže. Samotný prázdný seznam tedy není ekvivalentem explicitního `OrderNotFound`.

Reference:

- [Bybit Get Open & Closed Orders](https://bybit-exchange.github.io/docs/v5/order/open-order)
- [Bybit Get Trade History](https://bybit-exchange.github.io/docs/v5/order/execution)

Důsledek u neznámého entry:

1. `placeOrder` se na burze provede, ale odpověď se ztratí.
2. První realtime query ještě order nevidí.
3. Order zatím nemá fill, takže executions jsou prázdné.
4. Plugin vrátí 0 jako běžné nevyplnění.
5. Původní order může zůstat živý a Zorro jej již nesleduje.

Minimální oprava: při prázdném orderu a nulových executions pokračovat v pollingu a pokusit se o cancel podle známého
`orderLinkId`. Pokud ani po budgetu nevznikne jednoznačný terminální výsledek, vrátit unknown, nikoli 0. Nulu lze
bezpečně vrátit po explicitním terminálním stavu nebo jednoznačném `OrderNotFound`.

### P1-1: Retry neznámého close může zavřít část jiné lokální pozice

Oba pluginy nyní správně persistují ID close orderu, jehož výsledek nebylo možné zjistit, do `pendingCloses`. Samotný
`BrokerSell2` ale před odesláním nového close nekontroluje, zda už pro stejný trade nějaký pending close existuje:

- Binance: [`src/binance_futures.cpp`](src/binance_futures.cpp#L1224)
- Bybit: [`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1095)

Pending close se řeší až z `BrokerTrade`:

- Binance: [`src/binance_futures.cpp`](src/binance_futures.cpp#L1372)
- Bybit: [`src/bybit_futures.cpp`](../zorro-bybit-futures-plugin/src/bybit_futures.cpp#L1247)

Když `BrokerSell2` vrátí 0, Zorro nebo strategie může close zopakovat dříve, než proběhne `BrokerTrade`. Komentář v
reportu s tímto retry výslovně počítá. `reduceOnly` sice zabrání překlopení celkové burzovní pozice, ale nezachovává
přiřazení agregované burzovní pozice jednotlivým Zorro trade ID.

Příklad:

1. Na stejném symbolu existují dva lokální long obchody A a B, každý po jednom lotu.
2. Close A se na burze vyplní, ale jeho odpověď se ztratí a ID se uloží do `pendingCloses`.
3. Retry A odešle další reduce-only close. Protože na účtu stále existuje long z obchodu B, může se vyplnit i tento
   druhý close.
4. Plugin druhý fill zaúčtuje proti A a A vyřadí. B zůstane v Zorru otevřený, přestože burzovní pozice může být nula.

Pro jediný současný obchod daného směru na symbolu je riziko výrazně menší, protože reduce-only skutečně zabrání
flipnutí účtu. Při více obchodech stejného směru ale nejde o dostatečnou ochranu.

Minimální oprava: na začátku `BrokerSell2` nejprve vyřešit všechny existující `pendingCloses`. Dokud některý zůstává
unknown, neposílat další close. Po jejich vyřešení znovu načíst trade record a až poté případně zavřít zbývající
množství.

## Menší nález

### P2-1: Rekoncilovaný fill ztrácí skutečnou cenu

`resolveOrderOutcome()` v obou pluginech vrací pouze počet vyplněných lotů. `reconcileUnknownOrder` sice stále přijímá
`pPrice`, ale při nalezeném fillu jej nenastaví. Stejně tak může `pClose` zůstat nulové, když se close vyřeší až přes
helper.

Zorro v takovém případě použije odhad podle aktuální ceny. Neohrožuje to velikost pozice ani bezpečnost orderu, ale
P&L a vykázaná fill cena mohou být v timeoutovém scénáři méně přesné. Pro první „good enough“ release je to
přijatelné; vhodné je doplnit helperu strukturovaný výsledek obsahující lots i průměrnou cenu.

## Co bylo opraveno dobře

Následující změny jsou ve zdrojích skutečně přítomné a jejich provedení je pro zamýšlený rozsah rozumné:

- Close ordery jsou IOC, takže běžně nezůstávají jako nesledované GTC ordery na burze.
- Neznámé close ordery mají persistované client ID v `pendingCloses` a `BrokerTrade` je umí později zpracovat.
- Binance `resolveOrderOutcome` po cancelu polluje až do terminálního stavu a při neprůkazném výsledku vrací unknown.
- Obecná chyba query se již neinterpretuje jako důkaz, že order neexistuje.
- Obě API knihovny mají samostatný typ `OrderNotFound`.
- Binance správně zahrnuje HTTP 408 mezi neznámé výsledky exekučního požadavku.
- Bybit `BrokerTrade` už používá výpočet `entry fill - closed` i pro terminální částečně vyplněný entry order.
- Limit close, který se ihned nevyplní, nyní předvídatelně expiruje jako IOC. Jde o rozumnou a zdokumentovanou změnu
  chování pro plugin používaný primárně s market ordery.
- `stonky_common` je v obou API knihovnách linkováno jako `PUBLIC`, takže jeho veřejné include adresáře dostanou i
  konzumenti.
- README otevřeně dokumentuje interpretaci `BrokerAccount(Account)` jako margin asset a další přijatá omezení.

## Ověření

Při tomto auditu byly z čistých build adresářů spuštěny:

```text
cmake -S binance-cpp-api -B <clean-dir> -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build <clean-dir>

cmake -S bybit-cpp-api -B <clean-dir> -DENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build <clean-dir>
```

Úspěšně se sestavily:

- `binance_api`
- `binance_test`
- `bybit_api`
- `bybit_test`
- `bybit_benchmark`
- `bybit_exec_probe`

Tím je tvrzení druhého reportu o opravě čistého CMake buildu potvrzeno. Build skončil pouze varováními z vložené
`date.h` a Boost.Asio, bez chyb.

Nebyl proveden:

- MSVC build a link výsledných Windows DLL;
- live ani testnet order;
- skutečný síťový timeout po odeslání orderu;
- automatizovaný test stavového automatu.

Absence rozsáhlého unit-test coverage není pro tento projekt blokující. Chybějící testnet ověření změněných order
cest ale stále je relevantní, protože syntax check ani sestavení nepotvrdí časování a viditelnost orderů na burze.

## Pragmatický release gate

Před běžným live provozem doporučuji pouze následující cílené kroky:

1. Opravit obě Bybit větve, které vracejí `-2` bez cancelu.
2. Při prázdném Bybit realtime orderu a nulových executions nevracet ihned 0.
3. Neposílat nový close, dokud pro stejné trade ID existuje nerozřešený `pendingClose`.
4. Udělat MSVC Release build cílových 32bit/64bit DLL podle skutečně používané konfigurace Zorra.
5. Na testnetu nebo s minimální velikostí projít tuto malou matici:
   - market open a close, long i short;
   - partial IOC entry a následný úplný close;
   - timeout nebo simulovaná ztráta odpovědi po entry orderu a ověření cancelu;
   - timeout po close orderu, jeho pozdější reconciliation a retry;
   - dva současné obchody stejného směru na jednom symbolu;
   - restart Zorra s uloženým `pendingCloses`.

Po prvních třech opravách a úspěšném průchodu této krátké matice bych oba pluginy považoval za prakticky „good
enough“. Není nutné zavádět plošné test coverage ani stavět rozsáhlou testovací infrastrukturu.

