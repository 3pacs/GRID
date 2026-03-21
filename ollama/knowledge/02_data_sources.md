# GRID Data Sources

## US Macro (Core)

| Source | Series | Update | What It Tells You |
|--------|--------|--------|-------------------|
| FRED | 20+ series (DFF, T10Y2Y, T10Y3M, VIXCLS, CPIAUCSL, UNRATE, etc.) | Daily/Monthly | Baseline US economic conditions |
| BLS | Employment, CPI, PPI | Monthly | Labor market and inflation |
| EDGAR/SEC | Form 4 insider transactions, 13F holdings | Daily/Quarterly | Smart money positioning |
| SEC Velocity | 8-K filing rates by sector | Weekly | Corporate stress/activity |

## Market Data (Yahoo Finance)

30+ tickers updated daily:
- **Equity Indices**: ^GSPC (S&P 500), ^DJI, ^IXIC, ^RUT, ^VIX
- **Sector ETFs**: XLK, XLF, XLE, XLV, XLI, XLY, XLP, XLU, XLRE, XLB, XLC
- **Bond ETFs**: TLT, IEF, SHY, LQD, HYG, JNK, EMB, MUB
- **Commodity ETFs**: GLD, SLV, USO, DBA, PDBC
- **Currencies**: UUP, FXE, FXY, EEM
- **VIX Term Structure**: ^VIX9D, ^VIX3M, ^VIX6M
- **Futures**: HG=F (copper), GC=F (gold), SI=F (silver), CL=F (crude)

## International (18 Sources)

| Source | Coverage | Key Signals |
|--------|----------|-------------|
| ECB SDW | Euro area | M3, bank lending, TARGET2, yield curves |
| OECD SDMX | 44 countries | Composite Leading Indicators (1970+), MEI |
| BIS | Global | Credit-to-GDP gap, cross-border banking flows |
| AKShare | China | M2, TSF (Total Social Financing), industrial production, PMI |
| BCB Brazil | Brazil | SELIC, IPCA inflation, credit growth |
| KOSIS Korea | South Korea | Exports (EARLIEST global trade read), IIP |
| MAS Singapore | Singapore | SORA, FX reserves |
| RBI India | India | Repo rate, IIP, FX reserves |
| ABS Australia | Australia | CPI, unemployment, iron ore exports |
| IMF | Global | WEO, IFS, COFER reserves |
| Eurostat | EU | GDP, HICP, employment, trade |
| DBnomics | 100+ providers | Unified API for any central bank |

## Trade & Complexity

| Source | What It Measures |
|--------|-----------------|
| UN Comtrade v2 | Bilateral trade flows by HS code, 200+ countries, 1962+ |
| CEPII BACI | Cleaned bilateral trade data at HS6 level |
| Harvard Atlas ECI | Economic Complexity Index — how sophisticated a country's exports are |
| WIOD | World Input-Output tables — global value chain participation |

## Physical Economy (Leading Indicators)

| Source | What It Measures | Why It Matters |
|--------|-----------------|----------------|
| NASA VIIRS | Nighttime lights intensity (2012+) | Real economic activity that can't be faked |
| EU KLEMS | Total factor productivity by industry (1970+) | Long-run growth potential |
| USPTO PatentsView | Patent applications by technology class (1976+) | 2-3 year lead on capex cycles |
| USDA NASS | Crop yields, planted acres, crop condition | Agricultural supply shocks |
| OFR | Financial stress: credit/funding/leverage scores | Systemic risk early warning |

## Alternative Data

| Source | What It Measures | Update |
|--------|-----------------|--------|
| Opportunity Insights | Consumer spending by income quartile (2020+) | Weekly |
| GDELT | Global news tone, event counts, conflict volume | Daily |
| NOAA AIS | Port vessel arrivals, congestion | Monthly |
