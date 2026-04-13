---
title: GRID Catalog — 100 New Pullers
parent: CATALOG.md
---

# PULLERS — 100 new data sources

See `CATALOG.md` for scoring rubric and reading order. `docs/MODULE_CATALOG.md` has the canonical list of ~104 existing pullers — check before building.

**Format:** `[Type | Tier | Status] · L/C · Source · Cost · PIT · Coverage · Overlap`

---

## A. China / Asia macro (#1-10)

GRID's single biggest regional blind spot. At swing/quarterly/LEAPS horizons, China's growth impulse is the dominant input to commodities, EM, and global risk cycles. Existing dormant modules (`akshare_macro.py`, `kosis.py`) should be **reactivated** as part of this work.

### 1. China LGFV + trust product default tracker `[P · Tier A · NEW]`
Daily scrape of Wind/CSMAR for local government financing vehicle bond issuance, defaults, and extensions. Trust products are the Chinese shadow banking canary.
**Why ≥1%:** Early warning on China credit impulse turning negative. Leads RMB, copper, iron ore, AUD, EM equity by 2-4 weeks.
**L/C:** Lever — LGFV defaults close the municipal infrastructure funding valve. **Source:** Wind/CSMAR scrape (or akshare free tier) · **Cost:** M · **PIT:** easy (daily release) · **Coverage:** ~15% (commodities, EM, AUD)
**Location:** `ingestion/international/china_lgfv.py` (new module) + reactivate `ingestion/international/akshare_macro.py` as pull substrate
**Overlap:** none.

### 2. China Li Keqiang Index — electricity + rail freight + bank loans `[P · Tier A · ACTIVATE]`
Daily State Grid electricity consumption, China Railway freight volume, and PBoC bank loan data. The "Li Keqiang Index" is the credible growth nowcast when official GDP is suspect.
**Why ≥1%:** Truth-tier China growth signal. Leads reported GDP by 60-90 days. Conditions every commodities + EM call.
**L/C:** L+C — electricity is a pure demand condition; freight + loans name the credit valve. **Source:** State Grid + China Railway + PBoC (via akshare or direct scrape) · **Cost:** M · **PIT:** easy · **Coverage:** ~25%
**Location:** reactivate `ingestion/international/akshare_macro.py` + new submodule `ingestion/international/china_li_keqiang.py`
**Overlap:** reactivates dormant `ingestion/international/akshare_macro.py`.

### 3. PBoC open market operations + MLF renewals `[P · Tier A · NEW]`
Daily 7-day reverse repo volumes, MLF renewal amounts vs maturities, required reserve ratio changes. The PBoC's actual liquidity posture (not statements).
**Why ≥1%:** PBoC is a top-3 global liquidity lever. MLF renewal shortfalls precede CNY and risk asset moves by days.
**L/C:** Lever — PBoC is the named actor, MLF is the valve. **Source:** PBoC website + akshare · **Cost:** S · **PIT:** easy · **Coverage:** ~15%
**Location:** `ingestion/international/pboc_omo.py` (new module)
**Overlap:** partial with `ingestion/altdata/fed_liquidity.py` concept but China-side.

### 4. China real estate inventory + new home sales (70 cities) `[P · Tier B · NEW]`
Monthly NBS 70-city new/secondary home price index + inventory days-on-market from Centaline/Soufun. Real estate is ~25% of Chinese GDP and the epicenter of credit risk.
**Why ≥1%:** Leads Chinese bank equity, CNY, copper, luxury goods, and global real estate by 30-60 days.
**L/C:** L+C — developer defaults (lever) amplified by inventory condition. **Source:** NBS + Centaline scrape · **Cost:** M · **PIT:** tricky (monthly release with revisions) · **Coverage:** ~10%
**Location:** `ingestion/international/china_housing.py` (new module)
**Overlap:** none.

### 5. Korea 20-day export flash `[P · Tier A · ACTIVATE]`
Daily Korea Customs 20-day export data by product category. Korea's exports are the world's most reliable semi + memory + auto + chemicals nowcast.
**Why ≥1%:** Leads global manufacturing PMI, semiconductor cycle, and Asian FX by 2-3 weeks. Ultra-reliable historical signal.
**L/C:** Condition — demand nowcast, amplifies any semi or cycle lever. **Source:** Korea Customs Service + kosis · **Cost:** S · **PIT:** easy (10-day release cadence) · **Coverage:** ~15%
**Location:** reactivate `ingestion/international/kosis.py` with new `korea_export_flash` method
**Overlap:** reactivates dormant `ingestion/international/kosis.py`.

### 6. Japan Ministry of Finance FX intervention + JGB auction reports `[P · Tier A · NEW]`
Daily MOF FX intervention announcements + weekly JGB auction bid-to-cover + BOJ rinban (bond purchase) operations. Japan is the world's carry-funding currency.
**Why ≥1%:** MOF intervention is a **mechanical lever** that breaks global carry trades. BOJ YCC adjustments cascade into USTs and global duration.
**L/C:** Lever — MOF/BOJ are named, JPY liquidity valve is explicit. **Source:** MOF + BOJ websites · **Cost:** S · **PIT:** easy (daily) · **Coverage:** ~20% (JPY crosses + carry-sensitive risk assets)
**Location:** `ingestion/international/boj_mof.py` (new module; complements existing `jquants.py`)
**Overlap:** partial with `ingestion/international/jquants.py` (equities-focused, not macro ops).

### 7. China dollar bond issuance + HY spread (Markit iBoxx ADBI) `[P · Tier A · NEW]`
Daily primary market issuance calendar + secondary spread index for Chinese USD bonds. The cross-border credit valve.
**Why ≥1%:** Primary market freeze is the earliest warning of China credit stress propagating to global markets. Leads HY credit spreads by weeks.
**L/C:** Lever — issuer access to USD funding. **Source:** Markit iBoxx + Dealogic · **Cost:** M · **PIT:** easy (daily) · **Coverage:** ~15%
**Location:** `ingestion/altdata/china_dollar_bonds.py` (new module; altdata because it's cross-border USD-denominated)
**Overlap:** none.

### 8. India RBI forex reserves composition + OMO calendar `[P · Tier B · EXTEND]`
Weekly RBI forex reserves by currency + OMO purchase/sale calendar. India is now large enough that RBI interventions move AUD, EM, and energy demand.
**Why ≥1%:** RBI intervention telegraphs INR direction, which feeds back to global energy imports and Asian FX baskets.
**L/C:** Lever — RBI is named, valve is INR liquidity. **Source:** RBI weekly statistical supplement · **Cost:** S · **PIT:** easy · **Coverage:** ~8%
**Location:** extend `ingestion/international/rbi.py` with `fetch_reserves_composition()` and `fetch_omo_calendar()` methods
**Overlap:** extends existing `ingestion/international/rbi.py`.

### 9. Taiwan monthly export orders + semiconductor foundry utilization `[P · Tier A · NEW]`
TAIEX monthly export orders broken down by product (semis, electronics, optics) + TSMC/UMC/Vanguard utilization reports. The global semi cycle's ground truth.
**Why ≥1%:** Taiwan export orders are the **leading indicator** for the AI capex + memory cycle. Leads SOX, TSMC ADR, Nvidia, and semi-equipment by 30-60 days.
**L/C:** Condition — amplifies any tech/AI lever. **Source:** TAIEX + Taiwan MoEA · **Cost:** M · **PIT:** easy (monthly release) · **Coverage:** ~12%
**Location:** `ingestion/international/taiwan_exports.py` (new module)
**Overlap:** none.

### 10. ASEAN + Australia coal/LNG contract tracker `[P · Tier C · NEW]`
Monthly Indonesian Newcastle coal prices, Australian LNG long-term contract renewals, Vietnamese thermal coal imports. Energy flow between Asia's exporters and importers.
**Why ≥1%:** Energy contract renegotiations signal shifts in Asian demand and affect AUD, coal equities, LNG shippers, and Japanese utilities.
**L/C:** Condition — commodity flow detail. **Source:** Argus / Platts scrape + regional customs · **Cost:** M · **PIT:** tricky (contract data often delayed) · **Coverage:** ~5%
**Location:** `ingestion/altdata/asean_energy.py` (new module)
**Overlap:** none.

---

## B. Europe macro (#11-20)

ECB / TTF / German equities / European credit are all under-represented. GRID has `ecb.py` but the granular energy + credit + banking layer is thin. Dormant `eurostat.py` should be reactivated.

### 11. European gas storage + TTF curve (GIE + ICE) `[P · Tier A · NEW]`
Daily Gas Infrastructure Europe storage levels (country-by-country) + ICE TTF forward curve (M+1 through M+12). The European energy liquidity gauge.
**Why ≥1%:** TTF drives European inflation → ECB policy → EUR → DAX → European credit → cyclicals globally. Storage levels are a mechanical lever on price.
**L/C:** L+C — storage shortfall is a named valve; TTF curve amplifies. **Source:** GIE Agg + ICE TTF settlements · **Cost:** S · **PIT:** easy (daily) · **Coverage:** ~15%
**Location:** `ingestion/altdata/ttf_gas.py` (new module; altdata because it's European commodity forward curve)
**Overlap:** none; complements `ingestion/altdata/eia_puller.py` (US nat gas only).

### 12. ECB TLTRO / TPI repayment calendar + usage `[P · Tier A · EXTEND]`
Weekly ECB targeted longer-term refinancing operation repayments + Transmission Protection Instrument usage. ECB's actual liquidity posture behind the rate decisions.
**Why ≥1%:** TLTRO repayment cliffs are mechanical drains on bank liquidity; TPI usage signals peripheral spread stress. Both move EUR and peripheral debt.
**L/C:** Lever — ECB is the actor, TLTRO is the valve. **Source:** ECB weekly financial statement · **Cost:** S · **PIT:** easy · **Coverage:** ~12%
**Location:** extend `ingestion/international/ecb.py` with `fetch_tltro_repayments()` + `fetch_tpi_usage()` methods
**Overlap:** extends existing `ingestion/international/ecb.py`.

### 13. European AT1 (CoCo) bond prices + bank CDS basis `[P · Tier A · NEW]`
Daily iBoxx EUR Contingent Convertible AT1 index + European bank CDS vs cash bond basis. AT1 is the thin end of the European banking wedge.
**Why ≥1%:** AT1 leads bank equity by days when stress builds (see Credit Suisse 2023). CDS-cash basis wides are early warnings.
**L/C:** Condition — tail risk amplifier on any Europe banking lever. **Source:** Markit iBoxx + CDS feed (paid) · **Cost:** M · **PIT:** easy · **Coverage:** ~8% (European banks + global financials)
**Location:** `ingestion/altdata/european_at1.py` (new module)
**Overlap:** none.

### 14. Eurostat flash PMI + flash GDP nowcast `[P · Tier B · ACTIVATE]`
Monthly Eurostat flash PMI + flash GDP releases, country-level breakdown. Dormant `eurostat.py` should be reactivated for this.
**Why ≥1%:** Flash data hits 2-3 weeks ahead of full releases and drives ECB expectations + EUR repricing.
**L/C:** Condition — demand nowcast. **Source:** Eurostat SDMX bulk download · **Cost:** S (if reactivating dormant module) · **PIT:** easy · **Coverage:** ~10%
**Location:** reactivate `ingestion/international/eurostat.py` and add `fetch_flash_pmi()` + `fetch_flash_gdp()` methods
**Overlap:** reactivates dormant `ingestion/international/eurostat.py`.

### 15. German IFO + ZEW + sector PMIs `[P · Tier B · NEW]`
Monthly IFO business climate, ZEW financial sentiment, and sector-level PMI. German cycle leads European equity + auto + chemicals.
**Why ≥1%:** German industrial cycle is the European proxy for global trade. IFO drops precede DAX drawdowns.
**L/C:** Condition — broad European cycle. **Source:** IFO Institute + ZEW website · **Cost:** S · **PIT:** easy · **Coverage:** ~8%
**Location:** `ingestion/international/germany_sentiment.py` (new module)
**Overlap:** none.

### 16. European power price curves + carbon credit (EUA) `[P · Tier B · NEW]`
Daily day-ahead + forward power prices for DE/FR/NL/IT + EU ETS carbon allowance prices. The energy transition + industrial cost input.
**Why ≥1%:** Power price differentials drive European industrial location, energy-intensive stocks (aluminum, steel, chemicals), and utility margins.
**L/C:** Condition — input cost to everything European industrial. **Source:** EEX + ICE EUA futures · **Cost:** M · **PIT:** easy · **Coverage:** ~7%
**Location:** `ingestion/altdata/european_power_carbon.py` (new module)
**Overlap:** none.

### 17. UK gilt auctions + DMO issuance calendar `[P · Tier C · NEW]`
Weekly UK Debt Management Office gilt auction results (bid-to-cover, tail) + forward issuance calendar. The UK sovereign credit valve.
**Why ≥1%:** UK gilt stress is a 2022-style tail risk that moves GBP, European rates, and risk sentiment globally. Usually quiet, occasionally catastrophic.
**L/C:** Lever — DMO is the named actor. **Source:** DMO website · **Cost:** S · **PIT:** easy · **Coverage:** ~4%
**Location:** `ingestion/international/uk_dmo.py` (new module)
**Overlap:** none.

### 18. Swiss National Bank FX reserves + sight deposits `[P · Tier C · NEW]`
Weekly SNB FX reserves + sight deposit stats. Switzerland quietly manages one of the world's largest FX books; SNB surprises are historic tail events (2015).
**Why ≥1%:** SNB policy shifts are low-probability/high-impact. Tracking sight deposits gives advance notice.
**L/C:** Lever — SNB is the actor. **Source:** SNB weekly stats · **Cost:** S · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/international/snb.py` (new module)
**Overlap:** none.

### 19. European sovereign CDS spread + term structure `[P · Tier B · NEW]`
Daily 5Y + 10Y CDS for IT/ES/FR/DE/GR + term structure slope. Peripheral spread widening is the textbook early warning of EU fragmentation stress.
**Why ≥1%:** Sovereign CDS movements telegraph EUR + bank equity moves 5-15 days ahead during stress episodes.
**L/C:** Condition — tail amplifier. **Source:** Markit CDS (paid) or FINRA ATS substitute · **Cost:** M · **PIT:** easy · **Coverage:** ~8%
**Location:** `ingestion/altdata/eu_sovereign_cds.py` (new module)
**Overlap:** partial with `ingestion/altdata/finra_ats.py` concept.

### 20. European bank deposit flows (ECB MFI statistics) `[P · Tier B · EXTEND]`
Monthly ECB Monetary Financial Institutions statistics — household + NFC deposits by country. The European analog to US H.8 bank reserves migration.
**Why ≥1%:** Deposit flight from peripheral banks is a direct lever on European credit stress and bank equity.
**L/C:** Lever — depositors are the distributed actor, bank liquidity is the valve. **Source:** ECB SDW · **Cost:** S · **PIT:** tricky (monthly with ~1 month lag) · **Coverage:** ~6%
**Location:** extend `ingestion/international/ecb.py` with `fetch_mfi_deposits()` method
**Overlap:** extends `ingestion/international/ecb.py`.

---

## C. Liquidity plumbing (#21-30)

Fed balance sheet components (TGA / RRP / reserves / SOMA), FX swap basis, SOFR dispersion, bank reserves migration. Conditions every other prediction — multiplicative value.

### 21. Fed H.4.1 granular decomposition (TGA / RRP / reserves / SOMA maturity profile) `[P · Tier A · EXTEND]`
Weekly H.4.1 parsed into Treasury General Account balance, Reverse Repo facility usage, bank reserves, and SOMA holdings by maturity bucket. Track **drains and migrations**, not just the headline total.
**Why ≥1%:** RRP → reserves → money market flow is the real risk-asset liquidity channel. Conditions every prediction. Multiplicative across the oracle.
**L/C:** Lever — Fed is the actor, reserves + RRP are the valves. **Source:** NY Fed H.4.1 + SOMA holdings file · **Cost:** S · **PIT:** easy · **Coverage:** ~30%
**Location:** extend `ingestion/altdata/fed_liquidity.py` with `fetch_h41_components()` + `fetch_soma_maturity_profile()` methods
**Overlap:** extends `ingestion/altdata/fed_liquidity.py` (currently the "net liquidity equation" — probably doesn't decompose components).

### 22. FX swap basis + cross-currency basis (EUR/USD, USD/JPY, GBP/USD) `[P · Tier A · NEW]`
Daily 3-month FX swap basis for G10 pairs. Wides beyond −25bps historically precede risk-off by days.
**Why ≥1%:** Dollar funding stress is an early warning for equity, credit, and EM. A fast, liquid indicator with decades of track record.
**L/C:** Condition — amplifies any risk-off lever; can also be the lever itself in acute stress. **Source:** BIS OTC derivatives stats + ICE/CME basis feeds · **Cost:** M · **PIT:** easy (daily) · **Coverage:** ~15%
**Location:** `ingestion/altdata/fx_swap_basis.py` (new module)
**Overlap:** none.

### 23. SOFR dispersion + Fed SRF (Standing Repo Facility) usage `[P · Tier A · NEW]`
Daily SOFR 1st/25th/75th/99th percentile dispersion + Fed SRF borrowing. Dispersion spikes before repo stress.
**Why ≥1%:** SRF usage > 0 is a screaming red flag for funding stress. SOFR dispersion is the quietest reliable early warning.
**L/C:** Lever — primary dealers are the actors, repo liquidity is the valve. **Source:** NY Fed SOFR stats + SRF daily reports · **Cost:** S · **PIT:** easy · **Coverage:** ~12%
**Location:** extend `ingestion/altdata/repo_market.py` with `fetch_sofr_dispersion()` + `fetch_srf_usage()` — new module only if granularity mismatch
**Overlap:** partial with `ingestion/altdata/repo_market.py` — check granularity.

### 24. Primary dealer Treasury positioning (FR 2004 weekly) `[P · Tier A · NEW]`
Weekly FR 2004 primary dealer net positions across Treasury, agency, MBS, and corporate bonds. Dealer inventory changes lead yield moves by days.
**Why ≥1%:** Dealer positioning is a near-mechanical lever on rates. Few quants systematize this despite it being public.
**L/C:** Lever — primary dealers are named actors, inventory is the valve. **Source:** NY Fed FR 2004 release · **Cost:** S · **PIT:** easy (weekly) · **Coverage:** ~15% (rates, duration, credit)
**Location:** `ingestion/altdata/primary_dealer.py` (new module)
**Overlap:** none.

### 25. Treasury auction tail + bid-to-cover statistics `[P · Tier A · NEW]`
Per-auction tail (yield vs when-issued), bid-to-cover ratio, indirect bidder share, dealer take. A poor 10Y or 30Y auction is a real-time pricing event.
**Why ≥1%:** Auction tails move the curve within minutes and are a direct signal of foreign appetite for USTs. Leads dollar + risk by hours to days.
**L/C:** Condition — demand signal amplifies rates theses. **Source:** TreasuryDirect + Bloomberg substitute · **Cost:** S · **PIT:** easy (per auction) · **Coverage:** ~8%
**Location:** `ingestion/altdata/treasury_auctions.py` (new module)
**Overlap:** none.

### 26. Discount window + BTFP usage `[P · Tier B · NEW]`
Weekly Fed discount window primary/secondary credit + Bank Term Funding Program usage (if revived). Banks borrowing from the window = acute stress.
**Why ≥1%:** Discount window usage > $5B is a historical stress signal. Rare but extreme informational value when it triggers.
**L/C:** Lever — borrowing banks are the actors. **Source:** H.4.1 auxiliary · **Cost:** S · **PIT:** easy · **Coverage:** ~5%
**Location:** `ingestion/altdata/discount_window.py` (new module; could also live as a method on `fed_liquidity.py`)
**Overlap:** none.

### 27. H.8 bank balance sheet (loans, deposits, securities by size class) `[P · Tier A · NEW]`
Weekly H.8 broken into large-bank vs small-bank by loan type and deposit flight. Deposit migration between size classes is early warning of credit tightening.
**Why ≥1%:** Deposits fleeing small banks → credit contraction → real estate, regional bank, and SME stress. Leads by 4-8 weeks.
**L/C:** Lever — depositor actors, bank liquidity valve. **Source:** Fed H.8 release · **Cost:** S · **PIT:** easy (weekly) · **Coverage:** ~10%
**Location:** `ingestion/altdata/h8_bank_balance.py` (new module)
**Overlap:** none.

### 28. FHLB advances outstanding + Federal Home Loan Bank issuance `[P · Tier B · NEW]`
Weekly FHLB advances + issuance. FHLB is "the lender of next-to-last resort" for banks; advance spikes mean banks are running to the GSE for liquidity.
**Why ≥1%:** FHLB advance surges lead bank stress by 2-4 weeks. Underwatched.
**L/C:** Lever — member banks are the actors. **Source:** FHLB Office of Finance reports · **Cost:** M · **PIT:** tricky (weekly but inconsistent publication) · **Coverage:** ~5%
**Location:** `ingestion/altdata/fhlb_advances.py` (new module)
**Overlap:** none.

### 29. TIC flows (Treasury International Capital) `[P · Tier B · NEW]`
Monthly TIC foreign holdings and flows — who's buying/selling USTs. Two-month lag but irreplaceable for understanding foreign demand.
**Why ≥1%:** Structural foreign demand shifts (China, Japan, Saudi) feed into long-end rates and dollar over quarters. LEAPS-relevant.
**L/C:** Lever — named sovereign buyers. **Source:** Treasury TIC monthly release · **Cost:** S · **PIT:** hard (~2 month lag with revisions) · **Coverage:** ~8% (mostly LEAPS + duration)
**Location:** `ingestion/altdata/tic_flows.py` (new module)
**Overlap:** none.

### 30. Money market fund composition (T-bills vs RRP vs repo) `[P · Tier A · NEW]`
Daily OFR/ICI money market fund portfolio mix. MMF allocation between T-bills, RRP, and private repo is the direct RRP drain mechanism.
**Why ≥1%:** MMFs have >$6T AUM. Their allocation shifts between RRP and T-bills is the mechanism that drains/fills the reserves bucket.
**L/C:** Lever — MMFs are collective actors, RRP/T-bill valves. **Source:** OFR + ICI daily/weekly data · **Cost:** M · **PIT:** easy · **Coverage:** ~15%
**Location:** `ingestion/altdata/mmf_portfolio.py` (new module)
**Overlap:** none.

---

## D. Positioning + flows (#31-40)

CFTC COT extremes, 13F flows, primary dealer Treasury positions, prime broker client exposure, structured-flow calendar. Swing-horizon alpha lives here.

### 31. Prime broker net exposure notes (GS / MS / JPM weekly) `[P · Tier A · NEW]`
Scraped or excerpted weekly prime broker client letters (GS FICC, MS Quant Insight, JPM Positioning Intelligence). Report hedge fund aggregate gross + net + factor exposures.
**Why ≥1%:** When PB net hits 95th percentile, mean reversion is near-certain at 2-4 week horizon. Famously reliable.
**L/C:** Lever — hedge fund actors, positioning is the valve. **Source:** Client letter scrape (gray area) + public commentary · **Cost:** M · **PIT:** tricky (gated content) · **Coverage:** ~20% (equity factor + index)
**Location:** `ingestion/altdata/prime_broker_exposure.py` (new module)
**Overlap:** none.

### 32. Sovereign wealth fund rebalancing calendar + disclosed holdings `[P · Tier A · EXTEND]`
Quarterly GIC, Temasek, Norway NBIM, ADIA, QIA, CIC holdings disclosures + rebalancing heuristics (e.g., Norway's 60/40 band). Mechanical quarterly flows of $100B+.
**Why ≥1%:** Rebalancing is a fully predictable mechanical lever. Post-quarter rebalance windows see reliable flow patterns.
**L/C:** Lever — SWFs are the actors, policy bands are the mechanism. **Source:** SWF filings + 13F for US holdings · **Cost:** M · **PIT:** tricky (filings lag 45-60d) · **Coverage:** ~12%
**Location:** new puller at `ingestion/altdata/swf_holdings.py` + extend `intelligence/swf_network.py` for rebalance forecast logic
**Overlap:** extends `intelligence/swf_network.py` (exists per MODULE_CATALOG).

### 33. MSCI / Russell / S&P index rebalance calendar + inclusion forecast `[P · Tier A · NEW]`
Systematized forward calendar of MSCI semi-annual reviews, Russell reconstitution, S&P index changes, and forecasted additions/deletions. Forced flows of hundreds of billions on known dates.
**Why ≥1%:** Index inclusion drives 5-15% single-name moves with ~90% historical reliability. Known to everyone, systematized by few.
**L/C:** Lever — passive funds are collective actors, index rules are the valve. **Source:** MSCI/Russell/S&P public schedules + prediction services · **Cost:** M · **PIT:** easy · **Coverage:** ~10% (single names in review windows)
**Location:** `ingestion/altdata/index_rebalance.py` (new module)
**Overlap:** none.

### 34. Leveraged ETF AUM + daily rebalance demand estimator `[P · Tier B · NEW]`
Daily AUM snapshots for TQQQ/SQQQ/TMF/TMV/SOXL/SOXS/SPXL/SPXS + return-driven rebalance calculation. End-of-day mechanical flows.
**Why ≥1%:** Large daily moves trigger amplified rebalance demand. Predictable at 15:45-16:00 ET window.
**L/C:** Lever — ETF issuers forced to rebalance. **Source:** ETF.com + issuer daily files · **Cost:** S · **PIT:** easy · **Coverage:** ~5% (intraday index effects)
**Location:** `ingestion/altdata/levered_etf.py` (new module)
**Overlap:** none. Note: short-horizon, less important for swing/LEAPS.

### 35. CFTC COT extremes + non-commercial positioning z-scores `[P · Tier A · EXTEND]`
Weekly CFTC Commitments of Traders — non-commercial net positions, z-scores vs 3Y rolling, and pattern-matched turns. Not just raw positions.
**Why ≥1%:** COT extremes (>2σ) have historical mean-reversion hit rates of ~70% at 4-8 week horizon.
**L/C:** Lever — speculator positioning is the mechanism. **Source:** CFTC weekly COT (Tuesday release) · **Cost:** S · **PIT:** easy (weekly) · **Coverage:** ~15%
**Location:** extend `ingestion/altdata/cftc_cot.py` with z-score and regime-aware extremes; intelligence logic in `intelligence/cot_extremes.py`
**Overlap:** extends existing `ingestion/altdata/cftc_cot.py`. Audit granularity.

### 36. 13F quarterly delta tracking (500 largest funds) `[P · Tier A · EXTEND]`
Quarter-over-quarter position delta for the 500 largest US 13F filers, clustered to surface consensus rotations before they become obvious.
**Why ≥1%:** 13F delta clustering surfaces crowded trades and rotations 4-8 weeks before sell-side coverage catches up.
**L/C:** Lever — institutional actors named explicitly. **Source:** SEC EDGAR 13F · **Cost:** M · **PIT:** hard (T+45 filing lag) · **Coverage:** ~12%
**Location:** extend `ingestion/altdata/institutional_flows.py` (ingestion) + new `intelligence/thirteen_f_delta.py` (analytics)
**Overlap:** extends `ingestion/altdata/institutional_flows.py`.

### 37. ETF creation/redemption imbalances (daily) `[P · Tier B · NEW]`
Daily creation/redemption basket data for major ETFs (SPY, QQQ, HYG, LQD, EEM, XLE, XLF, GLD, TLT). Creations = buying pressure; redemptions = selling pressure.
**Why ≥1%:** Creation/redemption asymmetry leads underlying flows and reveals institutional conviction. Under-exploited signal.
**L/C:** Condition — flow confirmation. **Source:** NYSE Arca + issuer creation baskets · **Cost:** S · **PIT:** easy (next-day) · **Coverage:** ~10%
**Location:** extend `ingestion/altdata/institutional_flows.py` with create/redeem basket fetch (or new `ingestion/altdata/etf_flow_baskets.py`)
**Overlap:** partial with `institutional_flows.py` ETF coverage.

### 38. Short interest + borrow rate composite (FINRA + Interactive Brokers) `[P · Tier A · NEW]`
Bi-monthly FINRA short interest + daily borrow rates from broker feeds. The short side of positioning.
**Why ≥1%:** Rising borrow rates + high short interest = squeeze potential. Meaningful tail signal for select names.
**L/C:** Lever — short sellers' positioning. **Source:** FINRA short interest + IBKR/S3 borrow feed · **Cost:** M · **PIT:** easy · **Coverage:** ~8% (single names)
**Location:** `ingestion/altdata/short_borrow.py` (new module)
**Overlap:** none.

### 39. Structured product + autocall issuance flow `[P · Tier C · NEW]`
Weekly structured product issuance volumes (mostly AR/KI/Phoenix types) + major autocall barrier levels. Dealer hedging flow from structured products is real but opaque.
**Why ≥1%:** Autocall barrier knocks drive real hedging unwinds in SPX and single-name underliers.
**L/C:** Condition — dealer hedge flow amplifier. **Source:** SRP (paid) + SEC Rule 424 prospectus scrape · **Cost:** L · **PIT:** tricky · **Coverage:** ~5%
**Location:** `ingestion/altdata/structured_products.py` (new module)
**Overlap:** none.

### 40. Institutional futures roll calendar + impact estimator `[P · Tier B · NEW]`
Quarterly E-mini, Treasury futures, crude, gold roll windows with estimated impact. CTAs and systematic macro rebalance during roll weeks.
**Why ≥1%:** Roll-week dislocations create predictable slippage and mean-reversion opportunities in rate and index futures.
**L/C:** Condition — mechanical flow. **Source:** CME + ICE roll schedules · **Cost:** S · **PIT:** easy · **Coverage:** ~5%
**Location:** `ingestion/altdata/futures_roll_calendar.py` (new module)
**Overlap:** none.

---

## E. Credit markets (#41-50)

TRACE corporate bond prints, CLO equity tranches, leveraged loan index, muni spreads, sovereign CDS term structure, AT1 bond prices. Credit leads equity at quarterly horizons.

### 41. FINRA TRACE corporate bond trade prints (daily) `[P · Tier A · NEW]`
Every public corporate bond trade print with size, price, and dealer-vs-customer flag. Institutional rotation shows up in credit 2-3 days before it hits equity.
**Why ≥1%:** Institutional money rotates via credit first. TRACE prints reveal the rotation in near-real-time.
**L/C:** Lever — institutional actors, bond market valve. **Source:** FINRA TRACE bulk download (free with 15-min delay) · **Cost:** M · **PIT:** easy · **Coverage:** ~15% (credit + equity rotation)
**Location:** `ingestion/altdata/trace_bonds.py` (new module)
**Overlap:** none.

### 42. Leveraged loan index (LL100 / LSTA) + primary market calendar `[P · Tier A · NEW]`
Daily Morningstar LSTA leveraged loan index + forward primary market calendar (deals pricing next 2 weeks). Leveraged loans lead HY credit.
**Why ≥1%:** LL market is a purer credit-stress signal than HY bonds because it's floating-rate and held by CLOs. First to freeze, first to widen.
**L/C:** Lever — CLO warehouse funding valve. **Source:** Morningstar LSTA feed (paid) or Deal Catalyst · **Cost:** M · **PIT:** easy · **Coverage:** ~10%
**Location:** `ingestion/altdata/leveraged_loans.py` (new module)
**Overlap:** none.

### 43. CLO equity tranche prices + AAA-BB spreads `[P · Tier B · NEW]`
Weekly CLO equity tranche pricing (Citi/BarCap indexes) + AAA-BB tranche spread. CLO equity is the most stressed credit tranche.
**Why ≥1%:** CLO equity P&L leads private credit stress by weeks. Strong predictor of mid-market credit turns.
**L/C:** Condition — credit stress amplifier. **Source:** Dealer pricing runs (paid/gray) · **Cost:** L · **PIT:** tricky · **Coverage:** ~6%
**Location:** `ingestion/altdata/clo_tranches.py` (new module)
**Overlap:** none.

### 44. CDX.HY + CDX.IG series rolls + basis trade `[P · Tier A · NEW]`
Daily CDX HY + IG indexes + index-vs-intrinsic basis + series roll calendar. Credit index basis is a clean mispricing + rotation signal.
**Why ≥1%:** CDX widening leads equity drawdowns by days. Basis > 10bps signals stress or arbitrage pressure.
**L/C:** Condition — credit risk amplifier. **Source:** Markit CDX (paid) or ICE indices · **Cost:** M · **PIT:** easy · **Coverage:** ~12%
**Location:** `ingestion/altdata/cdx_indices.py` (new module)
**Overlap:** none.

### 45. Sovereign CDS term structure (US / IT / BR / TR / CN) `[P · Tier B · NEW]`
Daily 1Y / 5Y / 10Y sovereign CDS for major risk sovereigns. Term structure inversions (1Y > 5Y) are acute stress signals.
**Why ≥1%:** Sovereign CDS inversion precedes currency + sovereign bond crises by days to weeks.
**L/C:** Condition — tail risk amplifier. **Source:** Markit (paid) or alternative CDS feeds · **Cost:** M · **PIT:** easy · **Coverage:** ~8%
**Location:** `ingestion/altdata/sovereign_cds.py` (new module)
**Overlap:** none.

### 46. Muni bond spread widening + primary issuance calendar `[P · Tier C · NEW]`
Weekly Bloomberg muni index yields (AAA GO vs Treasury) + primary market calendar. Muni stress is a US fiscal early warning.
**Why ≥1%:** Muni widening precedes broader US credit stress during risk-off episodes. Specific states (IL, NJ, CA) are tail risks.
**L/C:** Condition — fiscal stress amplifier. **Source:** MSRB EMMA + Bloomberg muni feeds · **Cost:** M · **PIT:** easy · **Coverage:** ~4%
**Location:** `ingestion/altdata/muni_bonds.py` (new module)
**Overlap:** none.

### 47. Business Development Company (BDC) NAV discount tracker `[P · Tier A · NEW]`
Daily premium/discount of BDC share prices to last-reported NAV, plus NAV trajectory. BDCs are the public window into private credit quality.
**Why ≥1%:** BDC discount > 10% + declining NAV is an early warning for private credit stress — a $2T opaque market.
**L/C:** Condition — private credit health proxy. **Source:** BDC filings + share prices · **Cost:** S · **PIT:** easy (daily) · **Coverage:** ~6%
**Location:** `ingestion/altdata/bdc_nav.py` (new module)
**Overlap:** none.

### 48. Moody's / S&P rating watch + action calendar `[P · Tier B · NEW]`
Daily rating actions, outlook changes, and CreditWatch placements. Rating transitions are mechanical triggers for forced selling in investment-grade portfolios.
**Why ≥1%:** Fallen angel (IG → HY) transitions are mechanical selling events worth 2-5% on single names.
**L/C:** Lever — agencies are named actors. **Source:** Moody's + S&P press releases · **Cost:** M · **PIT:** easy (same-day) · **Coverage:** ~6%
**Location:** `ingestion/altdata/rating_actions.py` (new module)
**Overlap:** none.

### 49. Repo fail deliveries + specials list `[P · Tier B · NEW]`
Weekly NY Fed repo fails + specific CUSIPs trading "special" in the repo market. Repo fails = funding stress; specials = short-positioning crowding.
**Why ≥1%:** Fails surge above $100B is a reliable stress precursor. Specials identify which Treasury tenors are crowded short.
**L/C:** Condition — funding stress + positioning tell. **Source:** NY Fed repo data · **Cost:** M · **PIT:** easy · **Coverage:** ~5%
**Location:** extend `ingestion/altdata/repo_market.py` with `fetch_repo_fails()` + `fetch_specials()` methods
**Overlap:** partial with `repo_market.py` — check specifics.

### 50. OIS spread + Treasury swap spreads `[P · Tier B · NEW]`
Daily OIS-SOFR spreads + Treasury swap spreads across tenors. Swap spreads are a clean signal of rate-market dislocations and bank funding risk.
**Why ≥1%:** Negative swap spreads at long tenors are structural but their movements signal flow imbalances worth monitoring.
**L/C:** Condition — rate market plumbing. **Source:** ICE swap settlement + Bloomberg · **Cost:** M · **PIT:** easy · **Coverage:** ~6%
**Location:** `ingestion/altdata/swap_spreads.py` (new module)
**Overlap:** none.

---

## F. Commodities physical (#51-60)

LME warehouse stocks + cancellation ratios, crack spreads, Shanghai rebar, iron ore port stocks, lithium chemicals, uranium spot, carbon credits. Physical truth beats reported stats.

### 51. LME warehouse stocks + cancellation ratios (Cu/Al/Zn/Ni/Pb) `[P · Tier A · NEW]`
Daily LME warehouse stock levels + cancellation ratios by metal. Cancelled warrants signal imminent physical delivery (demand) or warehouse games.
**Why ≥1%:** High cancellation ratios (>40%) lead price moves by days. Reliable signal across industrial metals.
**L/C:** Lever — physical buyers are the actors. **Source:** LME daily stocks reports · **Cost:** S · **PIT:** easy · **Coverage:** ~10% (metals + mining equities)
**Location:** `ingestion/physical/lme_stocks.py` (new module)
**Overlap:** none.

### 52. Iron ore port stocks (Qingdao / Dalian) + daily throughput `[P · Tier A · NEW]`
Daily Mysteel-tracked port stocks at 45 Chinese ports + vessel discharge rates. The cleanest iron ore demand signal available.
**Why ≥1%:** Port stock drawdowns lead SGX iron ore futures by days. Critical for BHP/RIO/VALE/FMG.
**L/C:** Condition — physical demand nowcast. **Source:** Mysteel (paid) or CEIC scrape · **Cost:** M · **PIT:** easy · **Coverage:** ~6%
**Location:** `ingestion/physical/iron_ore_ports.py` (new module)
**Overlap:** none.

### 53. Crude oil floating storage + VLCC demurrage rates `[P · Tier B · NEW]`
Weekly floating storage (Vortexa / Kpler) + VLCC day rates. Floating storage builds = oversupply; demurrage spikes = demand panic.
**Why ≥1%:** Floating storage is the structural oversupply signal. Demurrage spikes are short-term demand shocks.
**L/C:** Condition — supply/demand imbalance. **Source:** Vortexa + Kpler (paid) or satellite tanker tracking · **Cost:** L · **PIT:** easy · **Coverage:** ~8%
**Location:** `ingestion/physical/floating_storage.py` (new module) + possibly extend `ingestion/altdata/noaa_ais.py` for AIS-based fallback
**Overlap:** partial with `ingestion/altdata/noaa_ais.py` if AIS-based.

### 54. Refinery utilization + crack spreads (3-2-1, 2-1-1) `[P · Tier A · NEW]`
Weekly EIA refinery utilization by PADD + daily crack spreads. Refinery economics lead crude product prices and refiner equity.
**Why ≥1%:** Crack spreads are a clean refiner equity signal and feed into gasoline/diesel consumer inflation.
**L/C:** Condition — refiner margin. **Source:** EIA weekly + CME crack futures · **Cost:** S · **PIT:** easy · **Coverage:** ~7%
**Location:** extend `ingestion/altdata/eia_puller.py` with `fetch_refinery_util()` + `fetch_crack_spreads()` methods
**Overlap:** extends `ingestion/altdata/eia_puller.py`.

### 55. US natural gas pipeline nominations + storage burn rate `[P · Tier B · EXTEND]`
Daily pipeline nominations (EIA-914 granular) + storage fill/draw weekly. The clean US nat gas supply/demand nowcast.
**Why ≥1%:** Nomination surges precede Henry Hub moves. Storage burn rates during cold snaps are pricing levers.
**L/C:** Condition — physical demand. **Source:** EIA-914 + pipeline EBBs · **Cost:** M · **PIT:** easy · **Coverage:** ~5%
**Location:** extend `ingestion/altdata/eia_puller.py` with `fetch_pipeline_nominations()` method
**Overlap:** extends `ingestion/altdata/eia_puller.py`.

### 56. Lithium chemicals pricing (Fastmarkets + CNY benchmarks) `[P · Tier C · NEW]`
Weekly lithium hydroxide + carbonate prices (China + rest of world). Lithium cycles drive battery + EV equity + miner performance.
**Why ≥1%:** Lithium is the canary for EV/battery demand. Leads miners (ALB, SQM) and downstream equities by weeks.
**L/C:** Condition — EV demand nowcast. **Source:** Fastmarkets (paid) or Shanghai Metals Market · **Cost:** M · **PIT:** tricky (weekly, some paid) · **Coverage:** ~3%
**Location:** `ingestion/physical/lithium_prices.py` (new module)
**Overlap:** none.

### 57. Uranium spot + UxC index `[P · Tier C · NEW]`
Weekly UxC U3O8 spot price + long-term contract price + Sprott Physical Uranium Trust NAV. Uranium is a rare structural supply-constrained market.
**Why ≥1%:** Uranium trades on its own cycle; price leads CCJ/NXE/UEC and broader nuclear renaissance thesis.
**L/C:** Condition — nuclear cycle. **Source:** UxC (paid) + Sprott NAV · **Cost:** M · **PIT:** easy · **Coverage:** ~2%
**Location:** `ingestion/physical/uranium_spot.py` (new module)
**Overlap:** none.

### 58. Carbon credit prices (EUA / CCA / UKA / RGGI) `[P · Tier C · NEW]`
Daily carbon allowance prices for EU ETS, California, UK, and RGGI. Carbon prices affect utility equities, carbon-intensive industrials, and ESG flows.
**Why ≥1%:** EUA price moves drive European utility equity and steel/cement margins.
**L/C:** Condition — regulatory cost. **Source:** ICE EUA + CCA futures · **Cost:** S · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/altdata/carbon_credits.py` (new module; altdata because it's global forward curve data, not physical)
**Overlap:** none.

### 59. Shanghai rebar futures + construction steel PMI `[P · Tier B · NEW]`
Daily SHFE rebar futures + weekly construction steel PMI. The clean Chinese construction + real estate demand pulse.
**Why ≥1%:** Rebar leads iron ore and Chinese real estate equity. Ground truth for stimulus efficacy.
**L/C:** Condition — Chinese construction demand. **Source:** SHFE + akshare · **Cost:** S · **PIT:** easy · **Coverage:** ~5%
**Location:** `ingestion/international/shfe_rebar.py` (new module) — depends on `akshare_macro.py` reactivation
**Overlap:** reactivates akshare if dormant.

### 60. US grain stocks + export sales (USDA WASDE complement) `[P · Tier C · EXTEND]`
Weekly USDA grain export sales + monthly WASDE supply/demand updates + ocean freight rates. Agricultural cycle + food inflation nowcast.
**Why ≥1%:** WASDE revisions move grain futures 3-5% and drive fertilizer + ag equipment equities.
**L/C:** Condition — ag cycle. **Source:** USDA NASS + FAS export sales · **Cost:** S · **PIT:** easy · **Coverage:** ~3%
**Location:** extend `ingestion/physical/usda_nass.py` with `fetch_export_sales()` + `fetch_wasde_deltas()` methods
**Overlap:** extends existing `ingestion/physical/usda_nass.py`.

---

## G. Corporate filings (#61-70)

8-K clustering, S-1 velocity, auditor changes, going-concern flags, lock-up expiries, secondary offering dilution, buyback execution rates, SPAC redemptions. Quarterly-horizon alpha.

### 61. 8-K unusual clustering + item category tracker `[P · Tier A · EXTEND]`
Daily 8-K filings with item code categorization (1.01 entry into material definitive agreement, 2.02 results, 5.02 officer departure, 8.01 other) + cluster detection when a company files 3+ within 30 days.
**Why ≥1%:** 8-K clusters precede large corporate events (M&A, restructurings, legal). Cluster detection beats reading each filing.
**L/C:** Lever — filing actors named. **Source:** SEC EDGAR 8-K feed · **Cost:** M · **PIT:** easy (same-day) · **Coverage:** ~8%
**Location:** extend `ingestion/altdata/sec_velocity.py` (already tracks 8-K velocity per MODULE_CATALOG) with item categorization + cluster detection; analytics in `intelligence/filing_cluster.py`
**Overlap:** extends existing `ingestion/altdata/sec_velocity.py`.

### 62. S-1 IPO pipeline + SPAC de-SPAC forward calendar `[P · Tier B · NEW]`
Active S-1 registrations + withdrawn S-1s + SPAC de-SPAC merger forward calendar. IPO pipeline velocity is a risk-appetite gauge.
**Why ≥1%:** S-1 withdrawals cluster at market tops. IPO pricing vs range forecasts risk-on/off. SPAC redemption rates signal retail capitulation.
**L/C:** Condition — risk appetite proxy. **Source:** SEC EDGAR + IPOScoop + SPAC Research · **Cost:** M · **PIT:** easy · **Coverage:** ~5%
**Location:** `ingestion/altdata/ipo_pipeline.py` (new module)
**Overlap:** none.

### 63. Auditor changes + PCAOB inspection findings `[P · Tier B · NEW]`
8-K Item 4.01 auditor resignations/dismissals + PCAOB inspection reports flagging audit deficiencies. Auditor changes are a major forensic red flag.
**Why ≥1%:** Auditor changes precede restatements and fraud disclosures by 3-12 months with documented predictive power.
**L/C:** Lever — named auditor actor. **Source:** SEC 8-K Item 4.01 + PCAOB reports · **Cost:** S · **PIT:** easy · **Coverage:** ~3% (but high precision when triggered)
**Location:** `ingestion/altdata/auditor_changes.py` (new module)
**Overlap:** none.

### 64. Going-concern language detector (10-K / 10-Q) `[P · Tier B · NEW]`
NLP scan of 10-K/10-Q filings for substantial doubt going-concern language. First appearance is a high-precision distressed equity signal.
**Why ≥1%:** Going-concern addition → average 30% drawdown within 90 days on academic samples.
**L/C:** Lever — auditor is the actor. **Source:** SEC EDGAR + NLP parser · **Cost:** M · **PIT:** easy (filing day) · **Coverage:** ~3%
**Location:** `ingestion/altdata/going_concern.py` (new module; NLP logic could be a method in `intelligence/earnings_transcript_analyzer.py` if reusing that pipeline)
**Overlap:** none.

### 65. Lock-up expiration calendar (pre/post IPO) `[P · Tier B · NEW]`
Forward calendar of 180-day lock-up expirations from recent IPOs + insider sell pressure forecast. Mechanical selling event with 70% historical drawdown pattern.
**Why ≥1%:** Lock-up expiration causes 5-15% drawdowns on ~70% of recent IPOs, starting ~5 days before expiry.
**L/C:** Lever — insider actors with mechanical calendar. **Source:** S-1 terms + IPO tracker · **Cost:** S · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/altdata/lockup_calendar.py` (new module)
**Overlap:** none.

### 66. Secondary offering dilution + ATM program tracker `[P · Tier B · NEW]`
Daily S-3 shelf registrations + ATM (at-the-market) program announcements + actual equity issuance. Dilution is mechanical selling pressure.
**Why ≥1%:** ATM programs and secondary offerings are direct share supply increases that suppress price 2-5% per dilution.
**L/C:** Lever — issuer is the actor. **Source:** SEC S-3 + Form 144 + 10-Q ATM disclosures · **Cost:** M · **PIT:** easy · **Coverage:** ~5%
**Location:** `ingestion/altdata/secondary_offerings.py` (new module)
**Overlap:** none.

### 67. Share buyback execution rate vs authorization `[P · Tier A · NEW]`
Quarterly 10-Q buyback spend vs board authorization. The ratio tells you who's actually returning cash vs announcing it for headlines.
**Why ≥1%:** Companies with high execution rates outperform announcers 2-3% quarterly on average. Underwatched.
**L/C:** Lever — corporate treasurers are the actors. **Source:** 10-Q buyback disclosures · **Cost:** M · **PIT:** easy (quarterly filing) · **Coverage:** ~8%
**Location:** `ingestion/altdata/buyback_execution.py` (new module)
**Overlap:** none.

### 68. SPAC redemption rates + trust cash levels `[P · Tier C · NEW]`
Per-SPAC redemption percentages at de-SPAC votes + trust account cash levels. High redemption = retail exhaustion signal.
**Why ≥1%:** SPAC redemption > 90% is a broad retail capitulation signal that has historically marked intermediate bottoms.
**L/C:** Condition — retail sentiment indicator. **Source:** SEC 8-K de-SPAC filings + SPAC Research · **Cost:** S · **PIT:** easy · **Coverage:** ~2%
**Location:** `ingestion/altdata/spac_redemptions.py` (new module)
**Overlap:** none.

### 69. CFO departures + comp committee changes `[P · Tier B · NEW]`
Form 8-K Item 5.02 officer changes + Schedule 14A comp committee restructurings. CFO departures under 3-year tenure are a strong earnings-quality red flag.
**Why ≥1%:** CFO resignations with no stated reason have 40% hit rate for negative surprises in next 2 quarters.
**L/C:** Lever — named CFO actor. **Source:** SEC 8-K Item 5.02 parser · **Cost:** S · **PIT:** easy · **Coverage:** ~4%
**Location:** `ingestion/altdata/cfo_departures.py` (new module)
**Overlap:** none.

### 70. Short seller report velocity (Hindenburg, Muddy Waters, Kerrisdale, etc.) `[P · Tier B · NEW]`
Tracks published reports from known short sellers with scraped full-text + target tickers. Public short seller reports average 15% drawdown in 5 days.
**Why ≥1%:** Short seller reports move names 10-25% in 5 days with well-documented base rates.
**L/C:** Lever — named short seller actor. **Source:** Twitter monitoring + direct scraping · **Cost:** M · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/altdata/short_seller_reports.py` (new module)
**Overlap:** none.

---

## H. Labor + consumer (#71-80)

WARN Act notices, Indeed job velocity, H1B applications, ADP intraday, credit card spending, TSA throughput, restaurant reservations, used car prices. Nowcast layer.

### 71. WARN Act layoff notices (state-level aggregation) `[P · Tier A · NEW]`
Daily state WARN Act database scraping — companies filing for mass layoffs. Leads BLS nonfarm payrolls by 30-60 days.
**Why ≥1%:** WARN filings are a legally-required early warning of layoffs. Aggregated spikes precede cyclical equity drawdowns.
**L/C:** Lever — named companies, layoff is the valve. **Source:** CA EDD, NY DOL, TX TWC, IL IDES scrapers + 40 other state DOLs · **Cost:** L (50 states to scrape) · **PIT:** tricky (state-by-state release lag) · **Coverage:** ~10%
**Location:** `ingestion/altdata/warn_layoffs.py` (new module)
**Overlap:** none.

### 72. Indeed job posting velocity by sector + wage offers `[P · Tier A · NEW]`
Weekly Indeed Hiring Lab job posting index by sector + median wage offers. Real-time labor demand + wage pressure.
**Why ≥1%:** Indeed job postings lead BLS JOLTS by 4-6 weeks. Wage offers lead CPI wage component.
**L/C:** Condition — labor market nowcast. **Source:** Indeed Hiring Lab free data + indeed_hiring_puller.py if already exists · **Cost:** S · **PIT:** easy · **Coverage:** ~8%
**Location:** check `ingestion/altdata/indeed_hiring_puller.py` (may exist per recent puller list); extend if yes, new `ingestion/altdata/indeed_velocity.py` if no
**Overlap:** partial with potential existing `indeed_hiring_puller.py`.

### 73. H1B LCA filings + USCIS approval rates `[P · Tier C · NEW]`
Weekly H1B Labor Condition Applications + monthly USCIS approval/denial rates. Leading indicator of tech hiring plans and immigration policy stance.
**Why ≥1%:** H1B filings precede tech hiring announcements. Approval rate changes are policy signals.
**L/C:** Condition — sector-specific labor demand. **Source:** DOL iCERT LCA database + USCIS H1B reports · **Cost:** M · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/altdata/h1b_filings.py` (new module)
**Overlap:** none.

### 74. ADP Employment Report intraday + real-time private payroll `[P · Tier B · NEW]`
Monthly ADP + real-time ADP-tracked private payroll data. ADP tracks 26M workers — not just the monthly report but the daily throughput.
**Why ≥1%:** ADP Research Institute has daily private payroll data before the monthly print. Some is public.
**L/C:** Condition — employment nowcast. **Source:** ADP Research Institute releases + NFIB small business data · **Cost:** M · **PIT:** easy · **Coverage:** ~6%
**Location:** `ingestion/altdata/adp_realtime.py` (new module)
**Overlap:** none.

### 75. Credit card spending aggregators (Earnest / Second Measure / Bloomberg 2ndM) `[P · Tier A · NEW]`
Daily credit card panel data for consumer-facing tickers. Directly feeds earnings nowcasts for retail/restaurant/travel.
**Why ≥1%:** Card data has well-documented 2-3% alpha on consumer earnings surprises. A standard alt-data category.
**L/C:** Condition — consumer demand nowcast. **Source:** Earnest + Second Measure + Affinity (paid) or alt · **Cost:** L (paid source) · **PIT:** easy · **Coverage:** ~8% (consumer names)
**Location:** `ingestion/altdata/card_spending.py` (new module)
**Overlap:** none.

### 76. TSA passenger throughput + hotel occupancy `[P · Tier B · NEW]`
Daily TSA throughput + STR weekly hotel RevPAR. Real-time travel + leisure demand.
**Why ≥1%:** TSA passenger count leads airline + hotel earnings by 30-60 days with high reliability.
**L/C:** Condition — travel demand nowcast. **Source:** TSA daily + STR weekly · **Cost:** S · **PIT:** easy · **Coverage:** ~4%
**Location:** `ingestion/altdata/tsa_hotel.py` (new module)
**Overlap:** none.

### 77. OpenTable restaurant reservations + Apple/Google mobility `[P · Tier C · EXTEND]`
Daily OpenTable seated diners vs 2019 + Apple Mobility Trends + Google Community Mobility. Consumer leisure + mobility nowcast.
**Why ≥1%:** Mobility data leads restaurant + retail earnings; regional patterns predict sector rotation.
**L/C:** Condition — real economy nowcast. **Source:** OpenTable open data + Apple/Google mobility · **Cost:** S · **PIT:** easy · **Coverage:** ~3%
**Location:** extend `ingestion/altdata/opportunity.py` (Opportunity Insights Economic Tracker already aggregates mobility); new submodule for OpenTable
**Overlap:** partial with `ingestion/altdata/opportunity.py`.

### 78. Used car prices (Manheim / Kelley Blue Book) `[P · Tier C · NEW]`
Weekly Manheim Used Vehicle Value Index + KBB retail price trends. Used car prices feed core CPI and consumer delinquency trends.
**Why ≥1%:** Manheim leads CPI used cars component by ~1 month. Also leads subprime auto delinquency trends.
**L/C:** Condition — consumer inflation input. **Source:** Cox Automotive + KBB · **Cost:** S · **PIT:** easy · **Coverage:** ~2%
**Location:** `ingestion/altdata/used_cars.py` (new module)
**Overlap:** none.

### 79. Mortgage application velocity + purchase index (MBA) `[P · Tier B · NEW]`
Weekly MBA Mortgage Application Index (purchase + refi) + rate-lock volumes. Housing demand nowcast that leads existing home sales.
**Why ≥1%:** MBA purchase index leads existing home sales by 4-6 weeks and drives homebuilder + mortgage finance equities.
**L/C:** Condition — housing demand. **Source:** MBA weekly release · **Cost:** S · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/altdata/mba_mortgage.py` (new module)
**Overlap:** none.

### 80. Subprime auto + consumer credit delinquency `[P · Tier B · NEW]`
Monthly asset-backed securities trust reports (Santander Consumer, GM Financial, Ally) — delinquency rates on subprime auto + consumer unsecured. Consumer credit quality canary.
**Why ≥1%:** Subprime delinquencies rise 60-90 days before broader consumer stress and credit card charge-offs.
**L/C:** Lever — borrower actors, payment valve. **Source:** ABS trust reports via EDGAR + Bloomberg · **Cost:** M · **PIT:** easy · **Coverage:** ~4%
**Location:** `ingestion/altdata/consumer_credit_delinquency.py` (new module)
**Overlap:** none.

---

## I. Industrial + logistics (#81-90)

Cass Freight, Drewry Container, Harpex charters, US rail intermodal, Cape/Panamax spreads, truck tonnage, port congestion. Cycle + inflation inputs.

### 81. Cass Freight Index + US truck tonnage (ATA) `[P · Tier A · NEW]`
Monthly Cass Freight Shipments + Expenditures Index + ATA Truck Tonnage Index. Leading indicators of US industrial activity and CPI goods.
**Why ≥1%:** Cass + ATA lead US ISM manufacturing and transport equity earnings by 30-60 days.
**L/C:** Condition — freight demand nowcast. **Source:** Cass Information Systems + ATA · **Cost:** S · **PIT:** easy · **Coverage:** ~5%
**Location:** `ingestion/altdata/cass_freight.py` (new module)
**Overlap:** none. Note `baltic_dry.py` + `supply_chain.py` exist but cover different facets.

### 82. Drewry Container Index + Shanghai Containerized Freight `[P · Tier A · NEW]`
Weekly Drewry + SCFI container freight rates. Container rate spikes are an inflation lever; crashes are a disinflation lever.
**Why ≥1%:** Container rates lead retailer margin pressure + CPI goods by 6-10 weeks. Clean inflation signal.
**L/C:** Condition — supply chain cost. **Source:** Drewry weekly + SSE SCFI · **Cost:** S · **PIT:** easy · **Coverage:** ~4%
**Location:** `ingestion/altdata/container_rates.py` (new module; complements existing `baltic_dry.py` and `supply_chain.py`)
**Overlap:** none.

### 83. Harpex charter rates (container ship charters) `[P · Tier C · NEW]`
Weekly Harpex Charter Rate Index. Charter rates lead spot freight rates by 4-8 weeks.
**Why ≥1%:** Harpex is a leading indicator for container freight rates and shipping equity cycle turns.
**L/C:** Condition — shipping capacity leading indicator. **Source:** Harper Petersen · **Cost:** S · **PIT:** easy · **Coverage:** ~2%
**Location:** `ingestion/altdata/harpex_charters.py` (new module)
**Overlap:** none.

### 84. US rail intermodal + carload (AAR weekly) `[P · Tier B · NEW]`
Weekly Association of American Railroads rail carload + intermodal data by commodity type. Ground truth for US industrial activity.
**Why ≥1%:** Rail carloads lead industrial production reports by 2-3 weeks with high correlation.
**L/C:** Condition — industrial nowcast. **Source:** AAR Weekly Rail Traffic · **Cost:** S · **PIT:** easy · **Coverage:** ~5%
**Location:** `ingestion/altdata/aar_rail.py` (new module)
**Overlap:** none.

### 85. Capesize / Panamax / Supramax freight rate spreads `[P · Tier B · EXTEND]`
Daily BDI sub-indices + cross-vessel spreads. Cape moves iron ore + coal; Panamax moves grains; Supramax is a mixed signal.
**Why ≥1%:** Cape vs Panamax spread shifts signal shifts in bulk commodity demand mix (metals vs ags).
**L/C:** Condition — granular shipping demand. **Source:** Baltic Exchange daily · **Cost:** S · **PIT:** easy · **Coverage:** ~4%
**Location:** extend `ingestion/altdata/baltic_dry.py` with sub-index + spread computation
**Overlap:** extends existing `baltic_dry.py`.

### 86. Port of LA / LB TEU throughput + congestion `[P · Tier B · NEW]`
Weekly Port of LA + LB container throughput + queue depth + wait time. West Coast port activity is a real-time US consumer demand proxy.
**Why ≥1%:** Port volumes lead retailer inventory + CPI goods pricing power.
**L/C:** Condition — consumer demand nowcast. **Source:** Port of LA + Port of LB public data + satellite/AIS · **Cost:** M · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/altdata/port_la_lb.py` (new module; could share infra with `noaa_ais.py`)
**Overlap:** partial with AIS-based shipping tracking.

### 87. Diesel demand (EIA weekly product supplied) `[P · Tier C · EXTEND]`
Weekly EIA distillate product supplied. Real-time trucking + industrial demand signal. Also feeds crack spreads.
**Why ≥1%:** Diesel demand is a direct industrial + freight activity signal that leads ISM by 1-2 weeks.
**L/C:** Condition — industrial demand. **Source:** EIA Weekly Petroleum Status Report · **Cost:** S · **PIT:** easy · **Coverage:** ~3%
**Location:** extend `ingestion/altdata/eia_puller.py` with `fetch_diesel_demand()` method
**Overlap:** extends `eia_puller.py`.

### 88. Panama Canal transit delays + Suez Canal traffic `[P · Tier C · NEW]`
Daily Panama Canal Authority transit slot assignments + drought-driven capacity cuts + Suez Canal vessel counts. Chokepoint data matters during stress episodes.
**Why ≥1%:** Panama Canal drought events (2023-24) drove freight rate spikes. Suez disruptions (2021 Ever Given, 2024 Houthi) moved markets.
**L/C:** Condition — tail amplifier for freight costs. **Source:** Panama Canal Authority + Suez Canal Authority + AIS · **Cost:** M · **PIT:** easy · **Coverage:** ~2% (but high event impact)
**Location:** `ingestion/altdata/canal_chokepoints.py` (new module)
**Overlap:** none.

### 89. Semiconductor equipment bookings (SEMI monthly) `[P · Tier A · NEW]`
Monthly SEMI Book-to-Bill ratio + regional equipment bookings. Leading indicator for the semi capex cycle and memory glut/tightness.
**Why ≥1%:** SEMI book-to-bill leads semi equipment equity (AMAT, LRCX, KLAC, ASML) and memory prices by 3-6 months.
**L/C:** Condition — semi cycle. **Source:** SEMI.org monthly release · **Cost:** S · **PIT:** easy · **Coverage:** ~6%
**Location:** `ingestion/altdata/semi_bookings.py` (new module)
**Overlap:** none.

### 90. DRAM + NAND spot prices (DRAMeXchange / TrendForce) `[P · Tier B · NEW]`
Weekly DRAM + NAND spot + contract prices. Memory cycle is distinct from logic cycle and matters for Micron/Samsung/SK Hynix/Western Digital.
**Why ≥1%:** DRAM spot leads contract, which leads memory maker earnings.
**L/C:** Condition — memory cycle. **Source:** TrendForce + DRAMeXchange · **Cost:** M · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/altdata/dram_nand_prices.py` (new module)
**Overlap:** none.

---

## J. OSINT + tail risk (#91-100)

Taiwan Strait AIS+ADS-B, PLA exercises, ship tracking, insurance CAT rates, satellite NDVI, construction activity, deforestation rates, reservoir levels. Structural tail-risk pricing.

### 91. Taiwan Strait OSINT (ADS-B + AIS + PLA exercise calendar) `[P · Tier A · NEW]`
Daily ADS-B flight tracking over Taiwan Strait, AIS vessel positions in Taiwan's ADIZ, PLA exercise announcements + commercial satellite imagery of PLA bases.
**Why ≥1%:** Baseline tail-risk monitor for TSMC / AI semi / global tech supply chain. Escalation anomalies would move markets 10-20%.
**L/C:** Lever — PLA is named actor. **Source:** ADS-B Exchange + MarineTraffic + OSINT Discord feeds · **Cost:** L · **PIT:** easy · **Coverage:** ~3% baseline, ~20% when triggered
**Location:** `ingestion/altdata/taiwan_strait_osint.py` (new module; can reuse `noaa_ais.py` substrate)
**Overlap:** partial with `ingestion/altdata/noaa_ais.py` for AIS data.

### 92. Red Sea + Gulf of Aden AIS tracking + Houthi attack log `[P · Tier B · NEW]`
Daily AIS vessel counts through Bab el-Mandeb + Houthi attack log from Ambrey Analytics. Red Sea disruption is the 2024-recurring shipping tail risk.
**Why ≥1%:** Red Sea disruptions drove container rates +300% in Q1 2024. Ongoing tracker for freight + insurance premiums.
**L/C:** Lever — Houthi actors, shipping lane valve. **Source:** Ambrey Analytics + MarineTraffic · **Cost:** M · **PIT:** easy · **Coverage:** ~2-8%
**Location:** `ingestion/altdata/red_sea_osint.py` (new module; reuses AIS infra)
**Overlap:** partial with AIS infra.

### 93. Satellite-derived economic activity (night lights + thermal + construction) `[P · Tier B · EXTEND]`
NASA Black Marble nighttime lights + MODIS thermal anomalies + construction site counts via Planet Labs. Physical economic activity ground truth.
**Why ≥1%:** Satellite economic activity beats official GDP for real-time China/EM growth and calls out stat manipulation.
**L/C:** Condition — ground truth for macro. **Source:** NASA Black Marble + Planet Labs (paid) + VIIRS · **Cost:** L · **PIT:** tricky (satellite data release lag) · **Coverage:** ~5%
**Location:** extend `ingestion/physical/viirs.py` with Black Marble + construction detection; optionally new `ingestion/physical/planet_labs.py` for commercial imagery
**Overlap:** extends `ingestion/physical/viirs.py`.

### 94. Reservoir levels + hydro generation (US + China) `[P · Tier C · NEW]`
Daily reservoir levels from USBR + Chinese Three Gorges. Hydro generation affects power prices + aluminum smelter economics.
**Why ≥1%:** Reservoir levels drive aluminum smelter curtailments in China (historical market mover) and US Western power prices.
**L/C:** Condition — energy supply constraint. **Source:** USBR + EIA hydro + Chinese reservoir data · **Cost:** M · **PIT:** easy · **Coverage:** ~2%
**Location:** `ingestion/physical/reservoirs.py` (new module)
**Overlap:** none.

### 95. Crop NDVI health indices (corn/soy/wheat) `[P · Tier C · NEW]`
Weekly USDA + MODIS NDVI crop health by region. Crop stress leads USDA yield forecast revisions.
**Why ≥1%:** NDVI anomalies precede WASDE revisions by 4-8 weeks, moving grain futures and fertilizer + ag equipment equities.
**L/C:** Condition — crop supply risk. **Source:** USDA FAS + NASA MODIS · **Cost:** M · **PIT:** easy · **Coverage:** ~3%
**Location:** `ingestion/physical/crop_ndvi.py` (new module; could complement existing `usda_nass.py`)
**Overlap:** none.

### 96. Insurance catastrophe + reinsurance rate tracker `[P · Tier B · NEW]`
Quarterly reinsurance rate renewals (Guy Carpenter + Artemis) + Artemis CAT bond tracker + Swiss Re sigma events database. Climate risk pricing for P&C + reinsurers + mortgage credit.
**Why ≥1%:** Reinsurance rate hikes drive P&C equity multiples and tighten mortgage credit in high-risk zones.
**L/C:** Lever — reinsurers are named actors. **Source:** Artemis.bm + Guy Carpenter reports · **Cost:** M · **PIT:** tricky · **Coverage:** ~3%
**Location:** `ingestion/altdata/reinsurance.py` (new module)
**Overlap:** none.

### 97. Wildfire smoke + air quality (PM2.5) by metro `[P · Tier C · NEW]`
EPA AirNow + NOAA smoke forecast integration. Wildfire smoke affects consumer mobility, retail, outdoor events, and insurance.
**Why ≥1%:** Sustained smoke events reduce regional retail + dining + mobility 5-15%.
**L/C:** Condition — regional demand suppression. **Source:** EPA AirNow + NOAA HRRR-Smoke · **Cost:** S · **PIT:** easy · **Coverage:** ~1%
**Location:** `ingestion/altdata/air_quality.py` (new module)
**Overlap:** none.

### 98. Global flight tracking (paid Flightradar24 + OpenSky) `[P · Tier C · NEW]`
Daily global commercial flight counts, cargo flight routing changes, corporate jet tracking. Cargo flight routing changes precede supply chain reconfiguration.
**Why ≥1%:** Corporate jet tracking identifies M&A pre-announcement; cargo routing tracks supply chain stress.
**L/C:** Condition — activity proxy. **Source:** OpenSky Network (free) + Flightradar24 (paid) · **Cost:** M · **PIT:** easy · **Coverage:** ~2%
**Location:** `ingestion/altdata/flight_tracking.py` (new module)
**Overlap:** none.

### 99. Sanctions watchlist + OFAC updates (real-time) `[P · Tier B · NEW]`
Real-time OFAC SDN list updates + EU sanctions + UK OFSI + Treasury designations. Sanctions events move specific commodities, banking, and cross-border payment equities.
**Why ≥1%:** Sanctions announcements move affected equities 5-20% in hours. Direct regulatory lever.
**L/C:** Lever — government actors, sanctioned entities. **Source:** OFAC + EU + OFSI feeds · **Cost:** M · **PIT:** easy · **Coverage:** ~4%
**Location:** `ingestion/altdata/sanctions_watch.py` (new module; complements existing `export_controls.py` for BIS Entity List)
**Overlap:** partial with existing `ingestion/altdata/export_controls.py`.

### 100. Cyber breach disclosures (SEC 8-K Item 1.05 cybersecurity) `[P · Tier C · NEW]`
Real-time 8-K Item 1.05 cyber incident disclosures (required under 2023 SEC rule). Material cyber incidents cause 3-10% drawdowns.
**Why ≥1%:** SEC 8-K Item 1.05 is a new filing category (2023+) with well-documented price impact.
**L/C:** Lever — attacker/defender actors, disclosure is the valve. **Source:** SEC EDGAR 8-K Item 1.05 parser · **Cost:** S · **PIT:** easy · **Coverage:** ~2%
**Location:** `ingestion/altdata/cyber_incidents.py` (new module; NLP could share infra with `sec_velocity.py`)
**Overlap:** partial with `sec_velocity.py` event categorization.

---

_End of PULLERS.md. 100 entries. See INTELLIGENCE.md for the 100 analytics modules._
