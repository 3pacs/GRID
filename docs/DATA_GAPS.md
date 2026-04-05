# GRID Data Gaps & Sources Plan

> **Last updated:** 2026-04-05
> **Current state:** 121 pullers, 80+ sources, 1,281 features
> **Paid accounts:** Tiingo Pro, QuiverQuant Pro

## What We Already Have (Don't Rebuild)

- **FX:** FRED already pulls EUR, JPY, CAD, CHF, GBP, CNY, Trade-Weighted USD
- **Fundamentals:** Tiingo pulls P/E, P/B, PEG, market cap, enterprise value daily
- **Political intel:** QuiverQuant Pro covers congressional, insider, lobbying, dark pool, gov contracts
- **Macro:** 60+ FRED series covering yields, credit, monetary, consumer, industrial
- **Crypto prices:** CoinGecko + DexScreener + PumpFun
- **Geopolitical:** GDELT + FARA + FOIA cables

---

## GAP 1: Earnings Data (CRITICAL — empty feature family)

The `earnings` family is defined in schema but has ZERO data. Tiingo has fundamentals but not earnings announcements/surprises.

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **Financial Modeling Prep** | Free (250/day) | EPS actual/estimate, revenue, guidance, dates, transcripts | https://financialmodelingprep.com — email signup |
| **Alpha Vantage** | Free (25/day) | Earnings calendar, EPS, surprise %, quarterly | https://www.alphavantage.co — get API key |
| **Polygon.io** | Free (5/min) | Earnings, dividends, splits, financials | https://polygon.io — email signup |
| **Tiingo** (already paid) | Included | Check if your tier includes earnings endpoints | Already have key |
| **yfinance** | Free | `.earnings_dates`, `.quarterly_earnings` | No signup — already installed |

**Recommendation:** Start with **yfinance** (free, already installed, no signup). Add **FMP** for depth. Build `ingestion/altdata/earnings_puller.py`.

---

## GAP 2: Crypto On-Chain Metrics (CRITICAL — blind to whale/DeFi signals)

Currently price-only. Can't see whale movements, liquidations, TVL, staking flows.

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **Etherscan API** | Free (5/sec) | ETH transfers, token txns, contract events, gas | https://etherscan.io/apis — email signup |
| **Blockchain.com** | Free | BTC hash rate, difficulty, mempool, whale alerts | https://www.blockchain.com/api — no signup |
| **DeFi Llama** | Free (no key) | TVL by protocol/chain, yields, stablecoin flows, bridges | https://defillama.com/docs/api — no signup needed |
| **CryptoQuant** | Free (limited) | Exchange flows, miner flows, SOPR, NUPL | https://cryptoquant.com — email signup |
| **Dune Analytics** | Free (limited) | Custom SQL on Ethereum/Solana/Base chains | https://dune.com — email signup |
| **Flipside Crypto** | Free (limited) | SQL on 20+ chains, pre-built dashboards | https://flipsidecrypto.xyz — email signup |
| **Solscan API** | Free | Solana txns, token flows, DeFi activity | https://docs.solscan.io — no signup |

**Recommendation:** Start with **DeFi Llama** (free, no key, covers TVL/yields/stablecoins across all chains). Add **Etherscan** for whale tracking. Add **CryptoQuant** for exchange flow signals.

---

## GAP 3: Breadth Data (IMPORTANT — thin feature family)

Advance/decline, new highs/lows, McClellan oscillator — barely populated.

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **FRED** (already have) | Free | ADVN, DECL, HIGHN, LOWN on NYSE | Already have key |
| **yfinance** | Free | `^ADV`, `^DEC` indices | Already installed |
| **Barchart OnDemand** | Free (limited) | Market breadth, sector breadth | https://www.barchart.com/ondemand — email signup |

**Recommendation:** Add these FRED series to the existing puller: `ADVFN`, `DECFN`, `NHIGH`, `NLOW`. Derive McClellan from advance/decline. Zero new code needed — just add series IDs.

---

## GAP 4: Credit/Consumer Health (IMPORTANT — can't forecast consumer cycles)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **FRED** (already have) | Free | DRCCLACBS (credit card delinquency), DRSFRMACBS (mortgage delinquency), TDSP (debt service ratio) | Already have key |
| **New York Fed** | Free | Household Debt & Credit Report (quarterly), auto/student/mortgage delinquency | https://www.newyorkfed.org/microeconomics/hhdc — no signup, CSV download |
| **Equifax/TransUnion** | $$$$ | Real-time credit stress | Not worth it — FRED proxies are fine |

**Recommendation:** Just add more FRED series. These are free and already supported:
- `DRCCLACBS` — Credit card delinquency rate
- `DRSFRMACBS` — Mortgage delinquency rate
- `TDSP` — Household debt service ratio
- `CCLACBW027SBOG` — Consumer credit outstanding (already pulling)
- `DRBLACBS` — Business loan delinquency

---

## GAP 5: Options Greeks & Vol Surface (IMPORTANT — daily snapshots only)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **CBOE DataShop** | Free (delayed) | VIX term structure, SKEW, PUT/CALL ratios | https://datashop.cboe.com — email signup |
| **Polygon.io** | Free (5/min) | Options snapshots with Greeks, OI by strike | https://polygon.io — already mentioned above |
| **Tradier** | Free (delayed) | Full options chains with Greeks, vol surface | https://tradier.com — email signup |
| **TD Ameritrade/Schwab** | Free (with account) | Real-time options chains, Greeks | Requires brokerage account |
| **yfinance** | Free | Basic options chains (no Greeks) | Already installed |

**Recommendation:** **Tradier** gives free delayed options with full Greeks. **CBOE DataShop** for VIX term structure. Both are free.

---

## GAP 6: Emerging Markets Depth (IMPORTANT — blind to EM crises)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **FRED** (already have) | Free | EM bond indices (BAMLEM*), EM currency indices | Already have key |
| **World Bank API** | Free (no key) | GDP, inflation, trade for 200+ countries | https://data.worldbank.org — no signup |
| **Trading Economics** | Free (limited) | Economic indicators for 196 countries, calendars | https://tradingeconomics.com — email signup |
| **DBnomics** (already have) | Free | 70+ statistical offices, 700M+ series | Already pulling |
| **CEIC** | $$$$ | Comprehensive EM data | Too expensive |

**Recommendation:** Add FRED EM series: `BAMLEMHBHYCRPIOAS` (EM High Yield), `BAMLEMCLLOTRUSD` (EM Corporate). Add **World Bank API** for GDP/inflation across all countries. Both free.

---

## GAP 7: Shipping & Logistics (IMPORTANT — supply chain blind spots)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **FRED** (already have) | Free | HARPEX (container freight), BDIY (Baltic Dry — already pulling) | Already have key |
| **MarineTraffic API** | Free (limited) | Port congestion, vessel movements | https://www.marinetraffic.com/en/ais-api-services — email signup |
| **Freightos Baltic Index** | Free (delayed) | Container shipping rates, 12 trade lanes | https://terminal.freightos.com — email signup |
| **Port of LA/Long Beach** | Free | Monthly TEU volumes, vessel calls | https://www.portoflosangeles.org/business/statistics — CSV download |
| **FlightRadar24** | Free (limited) | Air cargo activity proxy | https://www.flightradar24.com — no signup for basic |

**Recommendation:** Add FRED series `HARPEX`. Download **Port of LA** monthly TEUs (free CSV). Add **Freightos** for container rates.

---

## GAP 8: Job Market / Labor (NICE-TO-HAVE — leading indicator)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **FRED** (already have) | Free | JTSJOL (job openings), JTSQUR (quits rate), JTSHIR (hiring rate) | Already have key |
| **Indeed Hiring Lab** | Free | Job postings index by sector/metro, CSV download | https://www.hiringlab.org/data/ — no signup |
| **Revelio Labs** | $$$$ | Workforce analytics | Too expensive |
| **BLS** (already have) | Free | JOLTS, CES, LAUS | Already have key |

**Recommendation:** Add FRED series `JTSJOL`, `JTSQUR`, `JTSHIR` (JOLTS data). Download **Indeed Hiring Lab** CSVs monthly. All free.

---

## GAP 9: Web Traffic / App Data (NICE-TO-HAVE — company health proxy)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **SimilarWeb** | $$$$ | Website traffic, engagement | Too expensive |
| **Cloudflare Radar** | Free | Internet traffic trends by country/protocol | https://radar.cloudflare.com — no signup, public API |
| **Tranco List** | Free | Top 1M websites ranked daily | https://tranco-list.eu — no signup |
| **BuiltWith** | Free (limited) | Technology adoption trends | https://builtwith.com — email signup |
| **Google Trends** (already have) | Free | Search interest as proxy | Already pulling |

**Recommendation:** **Cloudflare Radar** for macro internet activity. Google Trends already covers company-level. Don't pay for SimilarWeb.

---

## GAP 10: Satellite/Geospatial (NICE-TO-HAVE — factory/agriculture signals)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **VIIRS** (already have) | Free | Nighttime lights (monthly) | Already pulling |
| **Copernicus/Sentinel Hub** | Free (limited) | SAR, optical, crop health, flood detection | https://scihub.copernicus.eu — email signup |
| **NASA FIRMS** | Free | Active fire detection (wildfire/industrial) | https://firms.modaps.eosdis.nasa.gov — email signup |
| **USGS EarthExplorer** | Free | Landsat imagery, land use change | https://earthexplorer.usgs.gov — email signup |

**Recommendation:** Add **NASA FIRMS** for wildfire/disaster detection. Copernicus for crop monitoring. Both free.

---

## GAP 11: Real Estate / Housing (NICE-TO-HAVE — cycle indicator)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **FRED** (already have) | Free | CSUSHPINSA (Case-Shiller), MORTGAGE30US, HOUST (starts), PERMIT | Already pulling most |
| **Zillow ZTRAX** | Free (academic) | Home values, rents, inventory by metro | https://www.zillow.com/research/data/ — CSV download |
| **Redfin Data Center** | Free | Median price, days on market, inventory | https://www.redfin.com/news/data-center/ — CSV download |

**Recommendation:** Add FRED `CSUSHPINSA` (Case-Shiller), `MORTGAGE30US`. Download **Redfin** monthly CSVs. All free.

---

## GAP 12: Startup/VC Activity (NICE-TO-HAVE — innovation cycle)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **Crunchbase** | Free (limited) | Funding rounds, company profiles, investors | https://www.crunchbase.com — email signup |
| **PitchBook** | $$$$ | VC/PE data | Too expensive |
| **AngelList/Wellfound** | Free (limited) | Startup hiring, valuations | https://wellfound.com — email signup |
| **Y Combinator** | Free | Batch companies, exits | Public data, scrape-friendly |

**Recommendation:** **Crunchbase** free tier for funding round signals. Low priority.

---

## GAP 13: ESG / Governance (NICE-TO-HAVE — regulatory risk)

| Source | Cost | What You Get | Signup |
|--------|------|-------------|--------|
| **SEC EDGAR** (already have) | Free | Proxy statements (DEF 14A), governance filings | Already pulling |
| **MSCI ESG** | $$$$ | ESG ratings | Too expensive |
| **Sustainalytics** | $$$$ | ESG risk | Too expensive |
| **CDP** | Free (delayed) | Climate disclosure data | https://www.cdp.net — email signup |

**Recommendation:** We already pull EDGAR — add DEF 14A proxy statement parsing for governance changes. Free.

---

## FRED Rate Limit Warning

FRED free tier: **120 requests/minute**, with 0.25s delay between calls. With 80+ series, a full pull takes ~20 seconds. Adding more series is fine for daily pulls but painful for backfills.

**For anything real-time or high-volume, use Tiingo (already paid) or Polygon.io (free 5/min, paid unlimited).** FRED is best for macro series that update daily/weekly/monthly — not for anything you need fast.

Tiingo Pro gives you most of what FRED has (yields, macro) plus equities, fundamentals, and news — with much better rate limits. Use FRED only for series Tiingo doesn't carry (JOLTS, breadth, ICSA, financial conditions indices).

## Quick Wins (Already Done — Added to FRED Puller)

These series were added to `FRED_SERIES_LIST` in `ingestion/fred.py`:

```python
# Add to FRED_SERIES_LIST in ingestion/fred.py

# Breadth
"ADVFN",            # NYSE Advancing Issues
"DECFN",            # NYSE Declining Issues

# Consumer credit health
"DRCCLACBS",        # Credit card delinquency rate
"DRSFRMACBS",       # Mortgage delinquency rate
"TDSP",             # Household debt service ratio
"DRBLACBS",         # Business loan delinquency

# Labor market depth
"JTSJOL",           # Job openings (JOLTS)
"JTSQUR",           # Quits rate
"JTSHIR",           # Hiring rate

# Housing
"CSUSHPINSA",       # Case-Shiller Home Price Index
"MORTGAGE30US",     # 30-Year Mortgage Rate
"MSACSR",           # Monthly Supply of New Houses

# EM bonds
"BAMLEMHBHYCRPIOAS",  # EM High Yield OAS
"BAMLEMCLLOTRUSD",    # EM Corporate Total Return

# Shipping
"HARPEX",           # Harper Petersen Container Index

# Financial stress depth
"DRTSCIS",          # Already have — verify
"CFNAI",            # Chicago Fed National Activity Index
```

---

## New Pullers to Build (Free, Just Need Signup)

| Priority | Puller | Source | Signup URL | Lines of Code |
|----------|--------|--------|-----------|---------------|
| 1 | `earnings_puller.py` | yfinance + FMP | https://financialmodelingprep.com | ~200 |
| 2 | `defi_llama_puller.py` | DeFi Llama | None needed | ~150 |
| 3 | `etherscan_puller.py` | Etherscan | https://etherscan.io/apis | ~200 |
| 4 | `tradier_options_puller.py` | Tradier | https://tradier.com | ~250 |
| 5 | `world_bank_puller.py` | World Bank | None needed | ~150 |
| 6 | `cloudflare_radar_puller.py` | Cloudflare Radar | None needed | ~120 |
| 7 | `nasa_firms_puller.py` | NASA FIRMS | https://firms.modaps.eosdis.nasa.gov | ~150 |
| 8 | `cryptoquant_puller.py` | CryptoQuant | https://cryptoquant.com | ~200 |
| 9 | `redfin_puller.py` | Redfin CSV | None needed | ~120 |
| 10 | `indeed_hiring_puller.py` | Indeed Hiring Lab | None needed | ~100 |

---

## APIs You Should Sign Up For

**Do these now (5 minutes each, all free):**

1. **Financial Modeling Prep** — https://financialmodelingprep.com (earnings data, 250 calls/day free)
2. **Etherscan** — https://etherscan.io/apis (ETH on-chain, 5 calls/sec free)
3. **Tradier** — https://tradier.com (options with Greeks, free delayed data)
4. **CryptoQuant** — https://cryptoquant.com (exchange flows, free tier)
5. **Polygon.io** — https://polygon.io (stocks + options + crypto, 5 calls/min free)
6. **NASA FIRMS** — https://firms.modaps.eosdis.nasa.gov (fire/disaster data)

**Optional (nice-to-have):**
7. Crunchbase — https://www.crunchbase.com (startup/VC)
8. CBOE DataShop — https://datashop.cboe.com (VIX term structure)
9. MarineTraffic — https://www.marinetraffic.com (port congestion)
10. Copernicus — https://scihub.copernicus.eu (satellite imagery)

---

## What We're Already Paying For But Not Fully Using

### Tiingo Pro
- **Using:** Prices, fundamentals (P/E, P/B, PEG, market cap), news sentiment
- **NOT using:** Statement-level financials (income statement, balance sheet, cash flow)
- **Action:** Check if earnings/EPS is available on your tier. If yes, add to `tiingo_fundamentals_pull.py`

### QuiverQuant Pro
- **Using:** Congressional trading, insider filings, lobbying, gov contracts
- **NOT using:** Possibly: corporate flights, Wikipedia edits, patent filings (check your tier)
- **Action:** Check QuiverQuant dashboard for additional datasets on your plan

---

## Data Volume Estimates

| Source | Estimated Size | Growth Rate |
|--------|---------------|-------------|
| ICIJ (loaded) | ~5 GB | Static (periodic new leaks) |
| Actor network (auto-discovery) | 100K-500K actors | ~10K/month from all sources |
| Embeddings (pgvector) | ~2 GB initial | ~500 MB/month |
| FRED (all series) | ~500 MB | ~5 MB/day |
| Crypto on-chain | ~10 GB/year | ~30 MB/day |
| Options chains | ~5 GB/year | ~20 MB/day |
| Everything else | ~3 GB/year | ~10 MB/day |

**Total projected:** ~25 GB year 1, growing ~2 GB/month. Any modern HDD handles this easily.
