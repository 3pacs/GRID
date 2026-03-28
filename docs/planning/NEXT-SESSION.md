# Next Session Plan — Complete Instructions

Everything the next agent needs to pick up and execute. No ambiguity, no guessing.

---

## PRIORITY 1: Fix Data Quality Issues (30 min)

### 1a. Fix WorldNews resolver mapping
**Problem:** 33 wn_* features have data in raw_series but aren't resolving to resolved_series. The series_id format from the WorldNews puller doesn't match entity_map.
**Steps:**
1. `SELECT DISTINCT series_id FROM raw_series WHERE series_id LIKE 'wn_%' LIMIT 20` — see actual format
2. Add mappings to `grid/normalization/entity_map.py` in NEW_MAPPINGS_V2
3. Run resolver: `from normalization.resolver import Resolver; Resolver(get_engine()).resolve_pending()`
4. Verify: `SELECT COUNT(*) FROM resolved_series rs JOIN feature_registry fr ON rs.feature_id = fr.id WHERE fr.name LIKE 'wn_%'`
**Expected:** 33 features go from zero to having data

### 1b. Fix FRED fedfred date parsing
**Problem:** `get_series_observations()` returns dates in the value column. The fill script tries to `float()` a date string.
**Steps:**
1. Read `scripts/fill_missing_features.py` line ~155 — the DataFrame iteration
2. The fedfred library returns a DataFrame with `date` index and `value` column, but the column name may vary
3. Fix: `df.reset_index()` then iterate with proper column names
4. Rerun: `python scripts/fill_missing_features.py --batch fred`
**Expected:** RRPONTSYD, NEWORDER, DTWEXBGS, NASDAQCOM series populate

### 1c. Fix analyst ratings int64 serialization
**Problem:** yfinance returns numpy int64 which json.dumps can't serialize
**Steps:**
1. In `scripts/fill_missing_features.py` `pull_analyst_ratings()`, wrap values: `int(float(val))`
2. In the `raw_payload` JSON dump, use `default=str` parameter
3. Rerun: `python scripts/fill_missing_features.py --batch analyst`
**Expected:** 24 analyst rating features populated

### 1d. Run resolver after all fixes
```bash
cd /home/grid/grid_v4/grid_repo/grid && source ../../venv/bin/activate
PYTHONPATH=/data/grid_v4/grid_repo/grid python3 -c "
from normalization.resolver import Resolver
from db import get_engine
r = Resolver(db_engine=get_engine())
result = r.resolve_pending()
print(result)
"
```

---

## PRIORITY 2: Fill Remaining 124 Zero-Data Features (1-2 hours)

### 2a. Computed features (12 features) — derive from existing data
These need no external data, just computation from what's already in resolved_series:
- `copper_gold_ratio` — copper_futures_close / gold_futures_close
- `copper_gold_slope` — 63-day slope of the ratio
- `sp500_mom_3m` — (sp500_close / sp500_close_63d_ago - 1) × 100
- `sp500_mom_12_1` — 12m return minus 1m return
- `hy_spread_proxy` — HYG/LQD price ratio (or use BAMLH0A0HYM2 from FRED)
- `hy_spread_3m_chg` — 63-day change in HY spread
- `dxy_index` — use DTWEXBGS from FRED or UUP proxy
- `dxy_3m_chg` — 63-day change in DXY
- `conf_board_lei_slope` — 63-day slope of USSLIND
- `spy_macd` — EMA(12) - EMA(26) signal line of SPY close
- `vix_3m_ratio` — vix_spot / vix3m_spot (both now in DB from CBOE pull)
- `sp500_adline`, `sp500_adline_slope`, `sp500_pct_above_200ma` — need constituent data, DEFER

**Steps:**
1. `python scripts/fill_missing_features.py --batch computed` — already has most of these
2. Add vix_3m_ratio computation (vix_spot and vix3m_spot are now in raw_series from CBOE)
3. Verify: check resolved_series for each feature

### 2b. International macro (20 features) — use existing international pullers
These have dedicated pullers in `ingestion/international/` that were never run:
- Brazil (3): `python -c "from ingestion.international.bcb import BCBPuller; BCBPuller(get_engine()).pull_all()"`
- China (4): `python -c "from ingestion.international.akshare_macro import AKSharePuller; AKSharePuller(get_engine()).pull_all()"`
- Korea (2): `python -c "from ingestion.international.kosis import KOSISPuller; KOSISPuller(get_engine()).pull_all()"`
- OECD (3): `python -c "from ingestion.international.oecd import OECDPuller; OECDPuller(get_engine()).pull_all()"`
- ECB (1): `python -c "from ingestion.international.ecb import ECBPuller; ECBPuller(get_engine()).pull_all()"`
- ISM (2): ISM PMI is in FRED as MANEMP proxy — map it
- Opportunity Insights (4): `python -c "from ingestion.altdata.opportunity import OpportunityPuller; OpportunityPuller(get_engine()).pull_all()"`
- Conf Board LEI: Already in FRED as USSLIND — compute slope

**Steps:** Run each puller, check for API key requirements, run resolver after

### 2c. EIA electricity (7 features) — fix EIA API v2 format
The crude/natgas series pulled fine but electricity series returned 0 rows. The series IDs may use a different v2 endpoint path.
**Steps:**
1. Check `https://api.eia.gov/v2/electricity/rto/daily-region-data/data/` vs `seriesid/` format
2. Test: `curl "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/?api_key=KEY&data[0]=generation&frequency=monthly&facets[fueltypeid][]=COW&facets[location][]=US-99"`
3. Fix `scripts/fill_missing_features.py` EIA series IDs to use correct v2 facets
4. Rerun: `python scripts/fill_missing_features.py --batch eia`

### 2d. Systemic risk (3 features) — OFR Financial Stress
**Problem:** OFR API returned 400. Their endpoint format changed.
**Steps:**
1. Check current OFR API: `curl "https://data.financialresearch.gov/v1/series/timeseries?mnemonic=STLFSI2"`
2. Try FRED alternative: STLFSI2 (St. Louis Fed Financial Stress Index) as proxy
3. If neither works, scrape from OFR website directly

### 2e. Trade (2 features) — Comtrade API
**Steps:**
1. Check if Comtrade API key is configured: `from config import settings; print(settings.COMTRADE_API_KEY)`
2. If not, use FRED proxies: BOPGSTB (trade balance), EXPGS (exports)
3. Run: `python -c "from ingestion.trade.comtrade import ComtradePuller; ComtradePuller(get_engine()).pull_all()"`

### 2f. Alternative (4 features) — VIIRS + Patents
- VIIRS nightlights: Need NASA FIRMS API or NOAA DMSP downloads
- Patents: Need USPTO PatentsView API (free, no key)
**Steps:**
1. `python -c "from ingestion.physical.viirs import VIIRSPuller; VIIRSPuller(get_engine()).pull_all()"`
2. `python -c "from ingestion.physical.patents import PatentsPuller; PatentsPuller(get_engine()).pull_all()"`
3. If pullers fail, check API endpoints and fix

### 2g. Crypto DeFi (5 features) — Pump.fun
Pump.fun API may have changed. Check DexScreener API for graduated token data.
**Steps:**
1. `python -c "from ingestion.pumpfun import PumpFunPuller; PumpFunPuller(get_engine()).pull_all()"`
2. If Pump.fun API is down, use DexScreener "new pairs" endpoint as proxy
3. Check: `curl "https://api.dexscreener.com/token-boosts/latest/v1" | head -c 500`

### 2h. Rates (4 features)
- `real_ffr`: FRED REAINTRATREARAT1YE (already attempted, fix date parsing)
- `repo_volume`: FRED RRPONTSYD (same fix)
- `euro_bund_10y`: ECB puller
- `singapore_sora`: MAS puller — `from ingestion.international.mas import MASPuller`

### 2i. GDELT avg tone + Polymarket BTC
- GDELT: The bulk API endpoint may need different params. Try: `https://api.gdeltproject.org/api/v2/doc/doc?query=*&mode=tonechart&timespan=30d`
- Polymarket: API returns nested JSON, the price extraction regex was wrong. Fix parsing.

---

## PRIORITY 3: Crucix iOS Interface (1-2 hours)

### 3a. Add Crucix button to PWA bottom nav
**File:** `pwa/src/App.jsx` or `pwa/src/components/BottomNav.jsx`
**Steps:**
1. Find the "More" menu or bottom nav component
2. Add a button/link that opens the Crucix interface
3. Crucix URL: check `config.py` or `docker-compose.yml` for Crucix port (likely :3001 or similar)

### 3b. Reformat Crucix for iOS
**Location:** `/home/grid/grid_v4/Crucix/` (separate app)
**Steps:**
1. Audit Crucix frontend files — likely React or Vue
2. Apply iOS-optimized styles: safe-area-inset, -webkit-overflow-scrolling, viewport meta
3. Dark theme matching GRID PWA (#080C10 background)
4. Touch targets ≥ 44px, bottom nav compatible with iOS home indicator
5. 3D globe needs WebGL check — fallback to 2D map on older iOS

---

## PRIORITY 4: Hermes Email System (1 hour)

### 4a. Set up hermes@stepdad.finance
**Options (pick one):**
- **Cloudflare Email Routing** → forward to Gmail, send via Gmail SMTP (easiest)
- **Postfix on server** → full self-hosted (most control)
**Steps:**
1. Cloudflare dashboard → Email → Routing → Add `hermes@stepdad.finance` → forward to stepdadfinance@gmail.com
2. For sending: use Gmail SMTP (already configured) with FROM: hermes@stepdad.finance
3. Update `config.py`: ALERT_EMAIL_FROM = "hermes@stepdad.finance"

### 4b. Implement sender allowlist
**File:** Create `alerts/email_guard.py`
**Steps:**
1. ALLOWED_SENDERS list in config (or DB table)
2. Inbound email processing function that checks sender against allowlist
3. If sender not in allowlist: silently drop or auto-reply "not monitored"
4. If sender allowed: pass content to Hermes LLM for processing
5. Add ability for user to add/remove allowed senders via API or config

---

## PRIORITY 5: Oracle Calibration & Improvement (ongoing)

### 5a. Fix confidence normalization
**Problem:** Everything at 95% confidence because signal_strength / 5.0 is too generous
**Steps:**
1. In `oracle/engine.py` `generate_predictions()`, change normalization:
   - `confidence = min(0.95, max(0.05, sigmoid(signal_strength - 2.0)))` where sigmoid squashes to 0-1
   - This centers at signal_strength=2.0 → 50% confidence
2. Add confidence calibration after first scoring cycle

### 5b. Add regime-aware model switching
**Steps:**
1. Track which regime each prediction was made in
2. After scoring, compute per-regime hit rates per model
3. When generating predictions, weight models by their regime-specific performance
4. Store regime at prediction time in oracle_predictions

### 5c. Hermes taxonomy awareness
**Steps:**
1. Add a daily Hermes prompt that reviews signal counts, value distributions, and freshness
2. If signal count drops >50% from trailing 7-day average → alert
3. If signal count spikes >200% → check for data quality issues
4. Log all anomalies to `operator_issues` table

---

## PRIORITY 6: Living Graph Renderers (2 hours)

### 6a. Wire PhaseSpace to real regime data
**Steps:**
1. Create API endpoint: `GET /api/v1/regime/trajectory` that returns PCA coordinates over time
2. Source: run PCA on resolved_series features, project each date to PC1/PC2
3. Color by regime state from decision_journal
4. Frontend: LivingGraph component already has PhaseSpace renderer

### 6b. Wire Orbital to real sector data
**Steps:**
1. Create API endpoint: `GET /api/v1/flows/orbital-data?period=6M`
2. Source: compute 30-day rolling relative performance vs SPY for each sector ETF
3. Return snapshots array for time scrubber
4. Frontend: LivingGraph Orbital renderer is ready

### 6c. Wire ForceNetwork to feature correlation
**Steps:**
1. Use existing `/api/v1/discovery/smart-heatmap` endpoint
2. Transform correlation matrix into nodes + links format
3. Add feature importance as node size
4. Frontend: LivingGraph ForceNetwork renderer is ready

---

## PRIORITY 7: Remaining Roadmap Items

### 7a. Flows page rework (from next-session plans)
- Add "Market Flow Summary" narrative at top
- Group sectors by flow direction: INFLOWS → NEUTRAL → OUTFLOWS
- Each card leads with insight, not data

### 7b. Watchlist first-principles redesign
- Mini briefing card per ticker: price + sector + influence + options + regime context
- "Why I'm watching this" editable notes
- Auto-suggest tickers based on sector map gaps

### 7c. LLM prompt optimization via orthogonality
- Already have analysis/prompt_optimizer.py — wire into all LLM prompts
- Before building any prompt, run orthogonality analysis
- Keep only features that are independent (low mutual correlation)

### 7d. Hypothesis results UI in Discovery
- Show 67+ tested hypotheses in the PWA
- Filter by sector, verdict, correlation strength
- "Promote to Feature" button for PASSED hypotheses

### 7e. AstroGrid Phase 13 (in progress)
- 3D planetary orrery
- Ephemeris calculator
- Market-astro correlation engine
- Celestial narrative synthesis

---

## PRIORITY 8: Testing & Hardening

### 8a. Run full test suite
```bash
cd /home/grid/grid_v4/grid_repo/grid && python -m pytest tests/ -v
```
- Fix any broken tests from today's changes
- Add tests for oracle/engine.py (critical path)
- Add tests for alerts/hundredx_digest.py sanity checks

### 8b. Rebuild PWA
```bash
cd /home/grid/grid_v4/grid_repo/grid/pwa && npm run build
```
- Verify LivingGraph component renders
- Check viz router loads correctly

### 8c. Restart API to pick up new routers
```bash
sudo systemctl restart grid-api
```
- Verify /api/v1/viz/rules returns the 11 visualization rules
- Verify /api/v1/viz/recommend works

---

## Quick Reference — File Locations

| What | Where |
|------|-------|
| Oracle engine | `grid/oracle/engine.py` |
| Oracle report | `grid/oracle/report.py` |
| Oracle runner | `grid/oracle/run_cycle.py` |
| 100x digest | `grid/alerts/hundredx_digest.py` |
| Viz intelligence | `grid/analysis/viz_intelligence.py` |
| Viz API | `grid/api/routers/viz.py` |
| LivingGraph component | `grid/pwa/src/components/LivingGraph.jsx` |
| Options puller (fixed) | `grid/ingestion/options.py` |
| Options scanner (fixed) | `grid/discovery/options_scanner.py` |
| Entity map (expanded) | `grid/normalization/entity_map.py` |
| Fill missing features | `grid/scripts/fill_missing_features.py` |
| Bulk historical pull | `grid/scripts/bulk_historical_pull.py` |
| Web scraper | `grid/ingestion/web_scraper.py` |
| Hermes operator (updated) | `grid/scripts/hermes_operator.py` |
| GSD state | `.planning/STATE.md` |
| GSD roadmap | `.planning/ROADMAP.md` |
| This plan | `.planning/NEXT-SESSION.md` |

## Hermes Schedule (what runs automatically)

| Task | Interval | Module |
|------|----------|--------|
| Market briefing | Hourly | ollama/market_briefing.py |
| Capital flow research | 4 hours | analysis/capital_flows.py |
| 100x digest | 4 hours | alerts/hundredx_digest.py |
| Oracle cycle | 6 hours | oracle/engine.py + oracle/report.py |
| Options pull | Daily | ingestion/options.py |
| Daily digest | Daily 07:00 UTC | alerts/email.py |
| Taxonomy audit | Nightly 02:30 | scripts/taxonomy_audit.py |
| Price fallback | 6 hours | ingestion/price_fallback.py |
| CoinGecko | Daily | ingestion/coingecko.py |
| Social sentiment | Daily | ingestion/social_sentiment.py |
| Google Trends | Daily | ingestion/altdata/google_trends.py |
| Wiki history | Daily | ingestion/wiki_history.py |
