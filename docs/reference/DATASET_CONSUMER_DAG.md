# Dataset → Consumer Dependency DAG (W2, first-release journeys)

**Scope:** read-only inventory, source-only. Traced against `origin/main` at `3fe3f5ef` from the
worktree `GRID-fable-wt-contracts-dag`. No database connections, SSH, `.env` reads, or
production/CI queries were used — every row below is a citation into checked-in source. Row
counts and last-fetch times are never reported because they cannot be measured this way.

**Provenance note (added while assembling this doc):** a separate integration commit
`44019a43` ("Merge branch `fix/trust-convergence-honesty` into `integration/data-integrity-20260918g`")
exists in this repo's local history but is **not** part of the `3fe3f5ef` base this document was
traced against. It touches many of the same router files cited below —
`watchlist_core.py`, `watchlist_helpers.py`, `watchlist_overview.py`, `watchlist_analysis.py`,
`oracle.py`, `derivatives.py`, `options.py`, `chat.py`, `canvas.py`, `intel.py`, and others — and is
reported by the remediation leads (see the `NOTE (lead)` annotations inline below, several of
which reference "composed tree g") to remove a number of the literal defaults flagged here. This
document has **not** been re-verified against that commit; treat every edge tagged with a
`NOTE (lead)` below as **pending re-check against the post-`44019a43` baseline** before anyone is
assigned a duplicate fix.

**Persona-scoping finding (applies to every journey below):** `pwa/src/app.jsx:11` defines
`const DAD_VIEWS = new Set(['home', 'ten-year', 'ticker-lookup'])`. A "simple/dad" (non-technical)
session is bounced back to `home` for anything outside that set (`app.jsx:314-319`). That means
**only Journeys 1 and 2, and the `ten-year` half of Journey 3, are reachable by the actual
first-release non-technical end user.** Journeys 3's `portfolio`/`edge-scanner` views, Journey 4
in full, and Journey 5 in full are operator/analyst-only (`group: 'trading' | 'research' |
'operations'`, `nav: 'drawer'` in `pwa/src/routes.js`). They are traced fully below because
operators are real users of the product, but this fact changes who is actually exposed to the
defects found.

Classification legend (exactly one per edge): `missing-module`, `unwired-code`,
`disabled-or-unregistered-schedule`, `source-or-auth-failure` (only for a hardcoded dead endpoint
visible in code), `parser-drift-suspected`, `consumer-query-mismatch` (writer/reader column names
differ), `unknown-runtime` (code looks wired; effective runtime state can't be determined from
source alone — preferred over guessing).

---

## Journey 1 — Home / market overview

**Route:** `home` is special-cased in `pwa/src/app.jsx:47-48`
(`extraRouteComponents.home = lazyView('./views/Home.jsx')`), forced for dad users
(`DAD_VIEWS`, `app.jsx:~11, ~311`) → `pwa/src/views/Home.jsx` → `WidgetGrid`
(`pwa/src/components/home/widgets.jsx:313-340`, registry: `verdict`, `ticker_pulse`, `watchlist`,
`macro_regime`, `news`, `money_flow`).

A second, operator-only "market overview" surface exists at `routes.js` id `dashboard` →
`pwa/src/views/Dashboard.jsx` (`group: 'worldView'`, but not in `DAD_VIEWS`). Both are traced.

### 1a. `Home.jsx` / `widgets.jsx` (dad-facing — reachable by the real end user)

| # | View | API call | Router · fn | Tables read | Writer | Schedule | Missing-data behavior | Classification |
|---|---|---|---|---|---|---|---|---|
| 1 | Home.jsx `AlertsPanel.loadAlerts` | `api.listAlerts()` → `GET /api/v1/alerts` (`api.js:930`) | `api/routers/price_alerts.py:186 list_alerts` | `sd_price_alerts` (`price_alerts.py:196-199`) | INSERT via `create_alert_record` (chat.py compose path); `triggered_at`/`last_price` only written by `scripts/check_price_alerts.py` | **No scheduler/systemd/cron hook found** for `scripts/check_price_alerts.py`; `docs/planning/LEVER-PACKAGE.md:709-711` states it "is built and never scheduled" | Backend returns `{"alerts": [], "error": str(exc)}` (`price_alerts.py:~201-202`); `Home.jsx:47` reads only `res.alerts`, swallows the error silently | disabled-or-unregistered-schedule *(NOTE lead: #528 `fix/wire-alerts-digest-scheduler` restores this wiring)* |
| 2 | Home.jsx compose / VerdictCard | `api.compose(question, history)` → `POST /api/v1/chat/compose` (`api.js:905-909`) | `api/routers/chat.py:2127 compose_layout` | LLM planner; price via `price_alerts.current_price` on alert intent (`chat.py:~2148-2151`) | n/a | n/a | LLM down → honest `CARD_BUSY_MESSAGE` (`chat.py:1155-1158, 2213-2217`) | unknown-runtime |
| 3 | widgets.jsx VerdictCard `askStream` | `api.askStream` → `POST /api/v1/chat/ask/stream` (`api.js:945-953`) | `chat.py:2740 ask_grid_stream` (`_stream_verdict` `~2660-2696`) | `_build_context_block` aggregation | n/a | n/a | Full failure streams `CARD_BUSY_MESSAGE` (`chat.py:2694, 2736`); frontend `widgets.jsx:110` `ErrorState` (`plain.js:57`) | unknown-runtime |
| 4 | widgets.jsx TickerPulseCard | `api.getTickerQuote(ticker)` → `GET /api/v1/watchlist/{ticker}/quote` (`api.js:575-577`) | `watchlist_overview.py:378 get_ticker_quote` | `resolved_series`+`feature_registry` (`watchlist_overview.py:436-449`); `options_daily_signals` (`452-457`) | `resolved_series` via `normalization/resolver.py:174-180 _flush_batch` from `raw_series`; options via `ingestion/options.py:449` | Resolver as Hermes step `scripts/hermes_operator.py:1541-1548`; options daily pull `ingestion/scheduler.py:1318-1330` | Dead query → `_log_query_failure` + live-fetch fallback (`watchlist_overview.py:437-444, ~456-467`); both fail → `widgets.jsx:148` `ErrorState`; non-numeric → empty (`widgets.jsx:162`) | unknown-runtime |
| 5 | widgets.jsx WatchlistCard | `api.getWatchlist()` → `GET /api/v1/watchlist/` (`api.js:547-550`) | `watchlist_core.py:32 list_watchlist` | `watchlist` (`44-51`) | user `POST` (`watchlist_core.py:654`) | n/a (user-driven) | Error → `ErrorState` (`widgets.jsx:177`); empty → "No stocks saved yet." (`179`) | unknown-runtime |
| 6 | widgets.jsx MacroRegimeCard | `api.getCurrent()` → `GET /api/v1/regime/current` (`api.js:282`) | `api/routers/regime.py:146 get_current` | `model_registry` (`155-158`); `decision_journal` (`160-171`, fallback `180-186`); `regime_history.data_as_of` (`_regime_data_as_of`, `29-40`) | `decision_journal` INSERT `journal/log.py:115` from `scripts/auto_regime.py:1073-1078`; `regime_history` UPSERT `auto_regime.py:679-689` | `auto_regime.run()` from `ingestion/scheduler.py:1179-1187` ("the only writer of `regime_history`") | No journal row → hardcoded `state="UNCALIBRATED", confidence=0.0, baseline_comparison="No data — run auto_regime..."` (`regime.py:188-199`, **literal 0.0**); frontend `plain.js:41-50 plainRegime()` has no "uncalibrated" branch and renders flat prose `The market read is "uncalibrated".` instead of the `widgets.jsx:239` Empty state (which only fires when `regime` is falsy, not for this sentinel) | consumer-query-mismatch *(NOTE lead: `regime.py` + `api/schemas/regime.py` are in the #534/#538 claimed set; re-check against composed tree)* |
| 7 | widgets.jsx NewsCard | `api.getNewsMomentum()` → `GET /api/v1/physics/momentum` (`api.js:694-696`) | `api/routers/physics.py:62 momentum` | GDELT actor-tone + price features via `PITStore.get_feature_matrix` (`physics/momentum.py:108-152`) | `raw_series` via `ingestion/altdata/gdelt.py _pull_actor_tones` (`314, 326, 690`); resolver as row 4 | GDELT daily puller group `ingestion/scheduler.py:421-424` | Insufficient data → honest `_unavailable_result()`: `available: False, direction="unavailable", summary="News momentum isn't available right now..."` (`physics/momentum.py:251-271`); frontend `widgets.jsx:257` renders the same "It's quiet — no big news right now." Empty state whether the backend said explicitly-unavailable or genuinely-quiet — the widget never reads `available` | consumer-query-mismatch |
| 8 | widgets.jsx MoneyFlowCard | `api.getSectorFlows()` → `GET /api/v1/flows/sectors` (`api.js:721`) | `api/routers/flows.py:458 get_sectors` | `resolved_series`/`feature_registry` via PITStore in `_compute_sectors_payload` (`flows.py:59-124`); `options_daily_signals` (`123-125`) | In-process warm loop `_sector_warm_cycle` (`flows.py:396-418`); persisted via `AnalyticalSnapshotStore.save_snapshot` category `sector_flows` (`301-334`) | `start_sector_flow_warm_thread()` from `api/main.py:205-206` at process startup (in-process thread, not systemd/cron) | Cold cache → honest `{"sectors": {}, "stale": True, "unavailable": True}` (`flows.py:478`); `widgets.jsx:270` checks `unavailable`/`stale` → "Money-flow data is still warming up." (`288`) | unknown-runtime |

### 1b. `Dashboard.jsx` (operator-only — `routes.js` id `dashboard`, not in `DAD_VIEWS`)

| # | View | API call | Router · fn | Tables read | Writer | Schedule | Missing-data behavior | Classification |
|---|---|---|---|---|---|---|---|---|
| 9 | Dashboard `loadData` (regime) | `api.getCurrent()` → `/api/v1/regime/current` | same as row 6 | same as row 6 | same as row 6 | same as row 6 | `Dashboard.jsx:260-263 .catch(() => null)` — stays stale silently, no message on top of row 6's mismatch | consumer-query-mismatch |
| 10 | Dashboard `loadData` (status) | `api.getStatus()` → `/api/v1/system/status` (`api.js:265`) | `system.py:213 status` | `feature_registry` COUNT (`233-238`); `hypothesis_registry` COUNT (`240`); `model_registry` PRODUCTION (`243-245`); `decision_journal` COUNTs (`249-256`); `pg_database_size()` (`222-224`) | aggregate/read-only | n/a | `.catch(() => null)` (`Dashboard.jsx:261`) | unknown-runtime |
| 11 | Dashboard `loadData` (thesis) | `api.getThesis()` → `/api/v1/intelligence/thesis` (`api.js:1050`) | `intelligence_thesis.py:30-31 _build_thesis` (`:49`, not traced further) | not traced | not traced | not traced | `.then(t => setThesis if !t.error).catch(() => {})` (`Dashboard.jsx:268`); UI text "No thesis available. Click Refresh to generate." (`383`) indistinguishable from an actual error | unknown-runtime |
| 12 | Dashboard `loadData` (intel dashboard) | `api.getIntelDashboard()` → `/api/v1/intelligence/dashboard` (`api.js:1001`) | `intelligence_risk.py:1083 → _build_dashboard_snapshot` (`532-590+`) | `source_trust_scores_cached` (`trust_scorer.load_trust_scores_cached`, `intelligence_risk.py:561`); live recompute fallback (`565-568`) | `flows/refresh_trust_scores.py:87-91` nightly | **No reference found** in `ingestion/scheduler.py`, `hermes_operator.py`, or any `server_setup/*.service`; `python -m flows.refresh_trust_scores register` (`:17`) implies an external Prefect-style orchestrator not in this repo | **Literal silent defaults:** `"overall_confidence": 0.5` at init (`intelligence_risk.py:542`); same `0.5` on the exception path (`1107-1116`); per-source `s.get("trust_score", 0.5)` (`583`) | disabled-or-unregistered-schedule *(NOTE lead: `intelligence_risk.py` is in the #534/#535 claimed set; re-check against composed tree)* |
| 13 | Dashboard `loadData` (aggregated flows) | `api.getAggregatedFlows()` → `/api/v1/flows/aggregated` (`api.js:761-765`) | `flows.py:2130` (not read) | not traced | not traced | not traced | `.catch(() => {})` (`Dashboard.jsx:270`); "No flow data" indistinguishable from error | unknown-runtime |
| 14 | Dashboard `loadData` (watchlist prices) | `api.getWatchlistPrices()` → `/api/v1/watchlist/prices` (`api.js:581-583`) | `watchlist_core.py:106` | in-memory `_price_cache` (`112`, `_get_cached_prices()` in `watchlist_helpers.py`) | Only via `POST /api/v1/watchlist/refresh-prices` (`watchlist_core.py:66-99`) | **No call to `refresh-prices` or `preload_watchlist` found** in any scheduler; Dashboard never calls either | Never warmed → honest `{"prices": {}, "fresh": false, "cached": false}` (`113-114`); PULSE row shows `--` (`Dashboard.jsx:568-589`) until a WebSocket push or manual refresh | disabled-or-unregistered-schedule |
| 15 | Dashboard `loadData` (watchlist enriched) | `api.getWatchlistEnriched(8)` → `/api/v1/watchlist/enriched?limit=8` (`api.js:565-567`) | `watchlist_core.py:325` | `watchlist` (`341-345`); `options_daily_signals` (`371-375`) | user CRUD; options puller (row 4) | as row 4 | Empty → `{"items": [], "suggestions": []}` (`346-347`); section hidden entirely (`Dashboard.jsx:410`) | unknown-runtime |
| 16 | Dashboard `AudioBriefingPlayer` | `api.listFlowBriefings()` → `/api/v1/flows/briefing/list` (`api.js:795-797`) | `flows.py:2753 list_briefings` → `intelligence/audio_briefing.list_all_briefings` | not traced | manual "New" button only (`api.getFlowBriefing(true)` → `flows.py:2689`; `Dashboard.jsx:138`) | **None found** | `.catch(() => {})` (`118-125`); "No briefing yet. Click 'New'..." (`219-221`) indistinguishable from error | unknown-runtime |
| 17 | Dashboard `LessonsWidget` | `api.getPostmortemLessons(n,days,force)` → `/api/v1/postmortem-lessons` (`api.js:1015-1019`) | `postmortem_lessons.py:148` | lessons cache table + postmortems (`_generate`, `~135-144`) | `_write_cache`; postmortems by `intelligence/postmortem.py` | not traced | Failure → `{"lessons": None, "generated_at": None, "error": ..., "cached": False}` (`178-190`); surfaced honestly via `ErrorState` (`LessonsWidget.jsx:75-77, 160-176`) | unknown-runtime |

**Journey 1 findings:**
1. `scripts/check_price_alerts.py` (the trigger checker for the *actual* dad-facing alerts panel) is unregistered — corroborated independently by `docs/planning/LEVER-PACKAGE.md:709-711`.
2. Two backend modules already emit honest unavailable sentinels (`regime.py:188` `UNCALIBRATED`/`0.0`; `physics/momentum.py:251` `available:false`) that the frontend mappers (`plain.js:41-50`, `widgets.jsx:248-257`) don't distinguish from real values/genuine quiet — on a page the real end user sees.
3. `intelligence_risk.py` hardcodes `overall_confidence: 0.5` on both init and exception (operator-only Dashboard, not dad-facing).
4. Multiple `Dashboard.jsx .catch(() => null/{})` sites make "never loaded", "empty", and "error" indistinguishable.

---

## Journey 2 — Ticker investigation

**Routes:** `ticker-lookup` special-cased in `app.jsx` (`extraRouteComponents['ticker-lookup']` →
`pwa/src/views/TickerLookup.jsx`, one of the 3 `DAD_VIEWS`). Child route `watchlist-analysis` →
`pwa/src/views/WatchlistAnalysis.jsx` (operator-only path per `app.jsx:318-321`, reached from
Dashboard/CrossReference/IntelDashboard/EdgeScanner/CommandPalette/IntelligenceSearch — never
from `TickerLookup.jsx`). `api/routers/watchlist.py:35-39` mounts `watchlist_core`,
`watchlist_analysis`, `watchlist_overview` under `/api/v1/watchlist`; `dad.py:26` mounts
`/api/v1/dad`.

| # | View | API call | Router · fn | Tables read | Writer | Schedule | Missing-data behavior | Classification |
|---|---|---|---|---|---|---|---|---|
| 1 | TickerLookup (dad-facing) | `api.getDadTickerGold(ticker)` → `GET /api/v1/dad/ticker/{ticker}/gold` (`api.js:332-337`) | `dad.py:1901 get_dad_ticker_gold` → `_build_compact_dad_response` (`dad.py:1719`) | `dad_ticker_summary_cache` (`dad.py:440-449`); DuckDB `ticker_summary`/`ticker_best_evidence`/`ticker_file_summary`/`ticker_sheet_summary` (`dad.py:1518-1576`); `raw_series`/`source_catalog` Finviz (`612-628`); `resolved_series`/`feature_registry` (`938-998`); `options_daily_signals` (`1048-1057`); `signal_sources`, TradingView `raw_series`, `decision_journal` (`1132-1191`) | **No writer in repo for the DuckDB workbook** — path hardcoded to a one-off extract `/data/agent-home/anikdang/dad-stock-analysis/work/extract-20260617/dad_stock_research.duckdb` (`dad.py:28-31`, overridable via `GRID_DAD_STOCK_RESEARCH_DB`); Finviz rows written by `_store_finviz_snapshot` (`668-722`) only on demand | Workbook: manual/one-off extract only, no builder anywhere in the repo; Finviz: on-demand refresh only | `_empty_workbook_context` → `status="unavailable"` (`dad.py:1497-1504`), UI shows "Database offline" (`TickerLookup.jsx:45-46`) — honest; `_gold_from_summary` with no summary → `score: 0, verdict: "No workbook history yet"` (`dad.py:788-793`) — honest zero | unwired-code (richest data source depends on an external one-time DuckDB extract nothing in the codebase can regenerate) |
| 2 | TickerLookup (dad-facing) | `api.getDadTickerEvidence` → `GET /api/v1/dad/ticker/{ticker}/evidence` (`api.js:339-345`) | `dad.py:1921 → _build_evidence_payload (1761) → _load_workbook_context (1485)` | same DuckDB workbook tables | same — none | same — manual only | "No workbook evidence rows found for this ticker." (`TickerLookup.jsx:225-230`) — honest | unwired-code |
| 3 | TickerLookup (dad-facing) | `api.getDadTickerChart` → `GET /api/v1/dad/ticker/{ticker}/chart` (`api.js:347-353`) | `dad.py:1935 → _build_chart_payload (1792) → _grid_market_context (916) + _source_freshness (874) + _latest_signal_context (1125)` | `resolved_series`/`feature_registry` (`938-949`); fallback `raw_series` where `source_catalog.name='yfinance'` (`952-967`); freshness rollup across `yfinance, finviz_fundamentals, TradingView, Social_Smart_Money, SEC_INSIDER, Unusual_Whales` (`874-913`); `signal_sources`, TradingView `raw_series`, `decision_journal` (`1125-1207`) | Price: `ingestion/yfinance_pull.py` (`YFinancePuller`); TradingView: inbound webhook `api/routers/tradingview.py:1-9,29-56`; regime/`decision_journal`: `scripts/auto_regime.py:run()` via `journal/log.py:44-115` | yfinance daily (`ingestion/scheduler.py:1251-1272`); regime "runs after all data is fresh" (`ingestion/scheduler.py:1177-1209`); TradingView not scheduled (webhook-only) | `SourceFreshness` → "Freshness data unavailable." (`TickerLookup.jsx:212-218`); chart topline "Price loading" / "Needs market history" (`572-573`) — honest | unknown-runtime |
| 4 | TickerLookup (dad-facing) | `api.getDadTickerFinviz` → `GET /api/v1/dad/ticker/{ticker}/finviz` (`api.js:355-360`) | `dad.py:1949 → _build_finviz_payload → _get_finviz_profile (748-784)` | `raw_series` filtered `source_catalog.name='finviz_fundamentals'`, `series_id LIKE 'finviz.{ticker}.%'` (`612-628`) | Same router: `_fetch_finviz_snapshot` (live scrape of finviz.com, `589-609`) + `_store_finviz_snapshot` (`668-722`), only when `refresh=True` and state is missing/aging/stale | **No entry in `ingestion/scheduler.py`**; refresh only fires on UI click (`TickerLookup.jsx:557`) | `FinvizPanel` shows `finviz?.status \|\| 'unavailable'` and "No Finviz snapshot rows found for this ticker yet." (`171-196`) — honest | disabled-or-unregistered-schedule |
| 5 | TickerLookup (dad-facing) | `api.getDadTickerOptions` → `GET /api/v1/dad/ticker/{ticker}/options` (`api.js:362-368`) | `dad.py:1962 → _build_options_payload → _options_history_context (1078-1122)` | `options_daily_signals` (`1084-1096`) | `ingestion/options.py OptionsPuller` INSERT (`449`) | Daily "Options chain pull" (`ingestion/scheduler.py:1319-1338`) | `status: "missing", latest: None` (`1114-1122`) — honest | unknown-runtime |
| 6 | TickerLookup (dad-facing) | `api.streamDadTickerGold` → `GET /api/v1/dad/ticker/{ticker}/gold/stream` (SSE, `api.js:370-405`) | `dad.py:1976`, yields compact/evidence/chart/finviz/options/done (`1985-2003`) | see rows 1-5 | see rows 1-5 | see rows 1-5 | Exception → `event: error` (`2004-2006`); UI falls back to `hydrateDetails()` (`TickerLookup.jsx:399-405`) — honest degrade | unknown-runtime |
| 7 | WatchlistAnalysis (operator-only) | `api.getTickerAnalysis(ticker, period)` → `GET /api/v1/watchlist/{ticker}/analysis` (`api.js:561-564`) | `watchlist_analysis.py:57 get_ticker_analysis` | `watchlist` (`92-96`); `resolved_series`/`feature_registry` (`112-224`); `options_daily_signals` (`228-251`); `decision_journal` (`255-269`); `raw_series`/`source_catalog` TradingView (`273-295`) | yfinance pull (scheduled) with in-request live `yfinance.Ticker().history()` fallback that writes back via `_cache_price_to_db` (`132-159`); options puller; auto_regime; TradingView webhook | see rows 3/5 | Each sub-block's `try/except` sets an **empty default silently** — `price_history = []` (`:129`), `options = []` (`:251`), `regime = None` (`:269`) — logged only at `log.debug` (`:128`), so a query failure is indistinguishable from "no data" in the API response and in the logs. UI shows a hard error only if `error && !data && !dataLoading && !enrichedData` (`WatchlistAnalysis.jsx:1215-1222`); otherwise sections just render nothing | unknown-runtime *(flag: debug-level swallowing at `watchlist_analysis.py:127-128, 158-159, 222-223, 249-250` would hide schema drift)* |
| 8 | WatchlistAnalysis (operator-only) | `api.getTickerOverview(ticker)` → `GET /api/v1/watchlist/{ticker}/overview` (`api.js:571-573`) | `watchlist_overview.py:30 get_ticker_overview` | `resolved_series`/`feature_registry` (`62-67`); `options_daily_signals` (`81-87`); `decision_journal` (`102-106`); related features (`129-144`); static `analysis.sector_map.SECTOR_MAP` (`154-184`) | Sentiment is a hardcoded rule-based heuristic (`188-206`); AI narrative via `llm.router.get_llm(Tier.LOCAL)` (`270-283`) with Ollama fallback (`285-299`); on LLM failure, falls back to a **templated** rule-based narrative (`323-364`) | n/a | On LLM failure, `AIOverviewCard` still labels the templated fallback "AI Generated · {timestamp}" (`WatchlistAnalysis.jsx:148-159`) regardless of which path produced it — mislabeling, not an outage; `as_of`/`generated_at` = `datetime.utcnow()` **at response time** (`watchlist_overview.py:374`), not the data's observation date | unknown-runtime *(honesty gap: fallback narrative labelled AI-generated; "freshness" is request time, not data time — file:line above)* |
| 9 | WatchlistAnalysis (operator-only) | `api.getTickerEdge(ticker)` → `GET /api/v1/watchlist/{ticker}/edge` (`api.js:590-592`) | `watchlist_overview.py:503 get_ticker_edge` | `signal_sources` via `intelligence/trust_scorer.py::get_insider_edge` (`1245-1300+`) and directly for whale flow (`source_type='scanner'`, `587-593`), smart money (`'social'`, `608-614`), prediction markets (`IN ('prediction','polymarket')`, `628-634`); `investigation_leads` (existence-checked, `683-697`); `detect_convergence` reads `signal_sources` (`trust_scorer.py:1373-1412`) | All registered: congressional (`ingestion/altdata/congressional.py`, scheduler `458-463`), insider Form 4 (`insider_filings.py`, scheduler `1300-1310`), dark pool (`dark_pool.py`, scheduler `844-848`), unusual whales (`unusual_whales.py`, scheduler `477-480`), smart money (`smart_money.py`, scheduler `489-492`) | as above (all scheduled) | **Literal 0.5 defaults:** `convergence = {"direction": "neutral", "source_count": 0, "confidence": 0.5}` (`watchlist_overview.py:702`, re-applied via `round(best.get("combined_confidence", 0.5), 2)` at `:711`); congressional `trust_score` default `0.5` (`:545`); smart-money `float(r[3]) if r[3] else 0.5` (`:626`, coerces a measured `0` to `0.5`); prediction-market `probability` default `0.5` (`:644`). **Consumer-query-mismatch on top:** `get_insider_edge` (`trust_scorer.py:1245-1300`) returns dicts keyed `"signal_type"`, but the router reads `sig.get("direction", "BUY")` (`watchlist_overview.py:541, 557`) and `latest_dp.get("direction")` (`:575`) — the key never exists, so congressional/insider/dark-pool direction is **always** the default `"BUY"`/`"accumulation"`; `detect_convergence` (`trust_scorer.py:1373-1462`) also returns `"signal_type"`, router reads `best.get("direction", "neutral")` (`:709`) — `direction` in `/edge` is **always** `"neutral"`. UI `TrustBar` (`WatchlistAnalysis.jsx:240-260`) and the "X sources bullish · Y% confidence" pill (`333-343`) render all of this indistinguishably from computed values | consumer-query-mismatch *(NOTE lead: this endpoint is inside #537/#546 `fix/trust-convergence-honesty` scope — composed tree `44019a43` is reported to change it; also a parallel worktree `GRID-wt-watchlist-edge` at `d9abaf90` exists for the same field — check both before assigning further fixes; this trace is against main `3fe3f5ef`)* |
| 10 | WatchlistAnalysis (operator-only) | `api.getGEXProfile(ticker)` → `GET /api/v1/derivatives/gex/{ticker}` (`api.js:663-665`) | `derivatives.py:81 get_gex` → `physics/dealer_gamma.py::DealerGammaEngine.compute_gex_profile` | `options_snapshots` (`physics/dealer_gamma.py:375-418`) | `ingestion/options.py` INSERT `options_snapshots` (`354`, DDL `188-208`) | Daily (`ingestion/scheduler.py:1319-1338`) | Returns `{"error": ...}` (`derivatives.py:93-95`); UI never sets `gexData`, panel silently omitted (`WatchlistAnalysis.jsx:1170-1174, 1346-1360`) | unknown-runtime |
| 11 | WatchlistAnalysis (operator-only) | `api.getVannaCharm(ticker)` → `GET /api/v1/derivatives/vanna-charm/{ticker}` (`api.js:666-668`) | `derivatives.py:172` (same engine/table/writer as row 10) | `options_snapshots` | same | same | Errors at `derivatives.py:184-185, 248-250`; UI omits `VannaCharmViz` silently (`WatchlistAnalysis.jsx:1363-1372`) | unknown-runtime |
| 12 | WatchlistAnalysis (operator-only) | `api.getFlowTimeline(ticker, days)` → `GET /api/v1/derivatives/flow-timeline/{ticker}` (`api.js:669-671`) | `derivatives.py:854` | `options_daily_signals` (`877-883`); fallback live `compute_gex_profile` on `options_snapshots` (`921-933`) | as rows 5/10 | as rows 5/10 | Both empty → HTTP 200 with `history: [], gamma_flip_crossings: []` (`941-947`), no explicit no-data flag; UI omits `FlowTimeline` silently (`WatchlistAnalysis.jsx:1375-1388`) | unknown-runtime |

**Journey 2 findings:**
1. The workbook DuckDB (rows 1, 2, 6) is the single largest gap: it depends on a hardcoded one-time extract path with no builder/writer/schedule anywhere in the repo, yet it is one of only three views the real end user (dad) can ever reach. It degrades honestly.
2. Confidence-basis literal `0.5` defaults on `main` at `watchlist_overview.py:544-545, 626, 644, 702, 711` (smart money even coerces a measured `0` to `0.5`), plus a column-name mismatch that pins `direction` to a constant — this endpoint is operator-only, not dad-facing, but it feeds the primary trust/edge UI.
3. `dad.py` computes freshness from real timestamps; `watchlist_overview.py:374` stamps request time instead and calls it `generated_at`.
4. `TickerLookup.jsx` (dad-facing) consistently shows explicit "unavailable" text; `WatchlistAnalysis.jsx` (operator-only) silently omits whole panels (GEX, Vanna/Charm, Flow Timeline, Options Intel) instead.

---

## Journey 3 — Watchlist / portfolio / edge

**Scoping:** `Portfolio.jsx` (`portfolio`) and `EdgeScanner.jsx` (`edge-scanner`) are in the
`trading` drawer (`routes.js:452-475`) — operator-only. `WatchlistAnalysis.jsx` is likewise
outside `DAD_VIEWS`. The one exception: `pwa/src/components/home/widgets.jsx:171-190`'s
`watchlist` widget can be composed onto the dad-facing Home by the chat planner
(`api/routers/chat.py compose`, `~272-2124`) — raw watchlist names can reach the real end user,
but no `portfolio` widget exists for dad users. `TenYearPortfolio.jsx` (`ten-year`) *is* a
`DAD_VIEWS` member and is the actual portfolio-shaped surface the real user sees.

| # | View | API call | Router · fn | Tables read | Writer | Schedule | Missing-data behavior | Classification |
|---|---|---|---|---|---|---|---|---|
| 1 | Portfolio.jsx (operator-only) | `api.getPortfolio()` → `GET /api/v1/watchlist/portfolio` (`api.js:584`) | `watchlist_core.py:117-118 get_portfolio` | `watchlist` (`139-142`); `options_recommendations` (`283-290`) | `watchlist` user `POST` (`654-696`); `options_recommendations` INSERT `trading/options_recommender.py:686` | `intelligence/scheduler.py:173-287` (`_options_recommendations`, daily 07:00, `grid-intelligence.service`) and `scripts/hermes_operator.py:756-768` (every 4h) | **Literal hardcoded defaults:** `ESTIMATED_PORTFOLIO = 125_000` (`watchlist_core.py:213`) — the displayed "portfolio value" is a constant, not a computed figure; `pnl_1d = ... if pct_1d is not None else 0` (`:225`); empty watchlist → all-zero shape (`:150-163`) | unwired-code (the headline dollar figure is a constant) *(NOTE lead: #537 `fix/portfolio-dollar-truth` territory; composed tree `44019a43` reported to change this; trace is against main)* |
| 2 | EdgeScanner.jsx (operator-only) | `api.getMarketEdges(10)` → `GET /api/v1/intelligence/edges` (`api.js:1428`) | `api/routers/intelligence_edges.py:17-27 get_market_edges` | `signal_sources` (`intelligence/market_edge_scanner.py:1172-1179`) | `ingestion/altdata/congressional.py:422`, `insider_filings.py:653/696`, `dark_pool.py:348`, `unusual_whales.py:397` | `ingestion/scheduler.py:460` (Congress_Trading), `:472` (SEC_Insider), `:845` (DarkPool) | Exception → honest `build_market_edge_snapshot(None, ...)` with `opportunities: []` and an explicit `"error"` field (`intelligence_edges.py:24-26`); `public_data_only: True` (`market_edge_scanner.py:1349`) | unknown-runtime |
| 3 | WatchlistAnalysis `InsiderEdgePanel` (operator-only) | same as Journey 2 row 9 (`GET /api/v1/watchlist/{ticker}/edge`) | `watchlist_overview.py:503-504 get_ticker_edge` | `signal_sources`, `investigation_leads` (`677-693`, writer not located — likely `intelligence/sleuth.py`, unconfirmed) | see Journey 2 row 9 | see Journey 2 row 9 | Every `try/except` swallows and logs (`watchlist_overview.py:573, 591, 663, 675, 706`); response keeps empty lists; frontend `if (!edgeData) return null` (`WatchlistAnalysis.jsx:298`) | consumer-query-mismatch *(same defect and NOTE lead as Journey 2 row 9 — do not duplicate the fix)* |
| 4 | TenYearPortfolio.jsx (dad-facing) | `api.getTenYearPortfolio({capital, years})` → `GET /api/v1/ten-year-portfolio/weekly` (`api.js:301`) | `api/routers/ten_year_portfolio.py:262-263` | `raw_series` (`104+ _fetch_raw_rows`, YF adjusted close); `resolved_series` (`71+`) | `ingestion/yfinance_pull.py:272` INSERT `raw_series` | 4×/day (`ingestion/scheduler.py:1526-1528` → `run_daily_pulls:1087` → `YFinancePuller:1251-1259`), `grid-scheduler.service` | Honest: `{"status": "empty", "message": "No eligible Yahoo adjusted-close price history found."}` (`:286-289`); `{"status": "error", "error": "Ten-year portfolio query failed."}` (`:290-293`) | unknown-runtime |
| 5 | `widgets.jsx` WatchlistCard (dad-reachable via chat compose) | same as Journey 1 row 5 | `watchlist_core.py:32-33` | `watchlist` (`41-49`) | user `POST` | not scheduled | `ErrorState` with retry (`widgets.jsx:177`); "No stocks saved yet." (`181-182`) | unknown-runtime |
| 6 | Dashboard.jsx (operator-only) | `api.getWatchlistPrices()` (`api.js:581`) + `api.getWatchlistEnriched(8)` (`api.js:565`) | `watchlist_core.py:106-107`, `:325-326` | `watchlist` (`:341`), `options_daily_signals` (`370-376`), `resolved_series`+`feature_registry` (`399-404`) | `get_watchlist_prices` reads a cache; refresh only via `POST /refresh-prices` (user/WS-triggered) | **No scheduler entry found** for automatic price refresh | `Dashboard.jsx:245/271-272 .catch(() => null)` / `.catch(() => {})` — fully silent | unwired-code (no scheduled price refresh located; may be a frontend poll this trace didn't find) |
| 7 | `ActorProfileDrawer.jsx` (from `SectorDive.jsx:964`, operator-only) | `api.getActorTrustCog(actorId)` → `GET /api/v1/actors/{actorId}/trust-cog` (`api.js:747`) | `actor_detail.py:381-382` | `lever_pullers` ⋈ `actor_analytics`, `actor_credibility` (`intelligence/actor_trust_cog.py:279-283`) | `actor_trust_cog.py:237` UPDATE inside `score_all_actors` (`188`) | Weekly Sunday 05:00 (`intelligence/scheduler.py:504`, `grid-intelligence.service`) | Honest `{"found": False, "note": ...}` (`actor_detail.py:403-407`); `{"error": "lookup_failed"}` (`399-400`) | unknown-runtime |
| 8 | `IntelDashboard.jsx:31-32` (operator-only) | `api.getTrustScores()` / `api.getConvergenceAlerts()` → both `GET /api/v1/intelligence/dashboard` (`api.js:1021, 1027`, reading `data.trust.top_sources` / `data.trust.convergence_events`) | `intelligence_risk.py:1083 → _build_dashboard_snapshot (532-587)` | `source_trust_scores_cached` (`trust_scorer.py:1116-1152`); fallback live recompute over `signal_sources` (`trust_scorer.py:909`) | `trust_scorer.py:1081-1102` INSERT, invoked by `flows/refresh_trust_scores.py:87` | **Nightly Prefect cron `"30 2 * * *"` declared in `scripts/serve_trust_scores.py:19-28`, whose own docstring (`:9`) names `grid-prefect-trust-scores.service` — that unit does not exist under `server_setup/` (28 `.service` files checked, none match)** | Cache stale >24h → live recompute (`intelligence_risk.py:571-574`); if that throws, `snapshot["trust"]` keeps `{"top_sources": [], "convergence_events": []}` (`:542`) and `overall_confidence` stays hardcoded `0.5` (`:547`) silently; `snapshot["errors"]` (`:588`) is discarded by the api.js wrappers (`api.js:1021-1030`) | disabled-or-unregistered-schedule (named systemd unit does not exist) |

**Undetermined for this journey:** the `investigation_leads` writer was not located; `options_daily_signals`/`resolved_series` writers for `list_watchlist_enriched` are covered under Journey 2; whether `POST /api/v1/watchlist/refresh-prices` is invoked automatically anywhere could not be confirmed from either scheduler module.

---

## Journey 4 — Research status (Hermes / autoresearch / hypothesis)

**Scoping:** `Operator.jsx` (`routes.js:535-541`, group `operations`) and `Discovery.jsx`
(`routes.js:338-344`, group `research`) are operator/analyst-only — not in `DAD_VIEWS`. **A
research-status view does exist** (contrary to the possibility the task flagged): Operator.jsx
surfaces Hermes cycle/issue state, Discovery.jsx surfaces hypothesis run state. Grepping all of
`pwa/src/views/` for "hermes"/"autoresearch"/"hypothesis" turned up no other matching view.

| # | View | API call | Router · fn | Tables read | Writer | Schedule | Missing-data behavior | Classification |
|---|---|---|---|---|---|---|---|---|
| 1 | Operator.jsx:52 | `getStatus()` → `GET /api/v1/system/status` (`api.js:265`) | `system.py:213 status()` | server/DB resource checks | n/a | n/a | Card gated on `status && !loading`; subfields checked `!= null` (`Operator.jsx:258-297`) | unknown-runtime |
| 2 | Operator.jsx:53 | `getHermesStatus()` → `GET /api/v1/system/hermes-status?limit=20` (`api.js:611`) | `system.py:1058 hermes_status()` | `analytical_snapshots WHERE subcategory='hermes_operator'` (`1074-1078`); `operator_issues` (`996-1002`); `analytical_snapshots WHERE snapshot_type='hermes_cycle'` (`1022-1029`) | `scripts/hermes_fixers.py:1707 save_cycle_snapshot()` → `category="pipeline_summary", subcategory="hermes_operator"`, called from `hermes_operator.py:2490` | `server_setup/grid-hermes.service` (`ExecStart=python3 scripts/hermes_operator.py`, `Restart=always`), 5-min loop (`hermes_operator.py:2811-2825`) | `Operator.jsx:84 hermes = hermesStatus \|\| {}`; `:85 hermesState = hermes.operator_state \|\| {}`; `:86 isOnline = Boolean(hermes.running \|\| ...)`; `:88-91` cascading `\|\|` fallbacks for `lastCycleTime` → `fmtDate` returns `'-'` when null; `:137 consecutive_failures ?? 0`; `:142 cycle_count ?? '-'` | unknown-runtime (base call) |
| 2a | same call, "live" branch | — | `system.py:1124-1137` (`if _hermes_state is None: ... else: live`) | — | `_hermes_state` global set via `set_hermes_state()` (`system.py:973-976`), called only from `scripts/hermes_operator.py:2717-2718` | — | The live branch can only fire if `hermes_operator.py` runs inside the same Python process as the FastAPI app; it is deployed as the separate `grid-hermes.service` unit vs the API's own unit → this branch is dead code in the deployed topology | **unwired-code** |
| 2b | same call, embedded `snapshots` sub-list | — | `system.py:1019-1048`, queries columns `snapshot_timestamp`, `snapshot_type` (`1022-1029`) | `AnalyticalSnapshotStore.save_snapshot()` DDL (`store/snapshots.py:95-105`) defines `snapshot_date`, `category`, `subcategory`, `created_at` — **no `snapshot_type`/`snapshot_timestamp` columns exist** | n/a | n/a | Query throws (undefined column), caught by `except Exception: log.debug` (`system.py:1047-1048`) → `snapshots: []`. Not read by `Operator.jsx` but *is* read by `Settings.jsx:547` (`hermesStatus?.snapshots \|\| []`, Hermes tab) — that second consumer is silently starved | **consumer-query-mismatch** |
| 3 | Operator.jsx:54,73-77 | `getOperatorIssues(30, category, severity)` → `GET /api/v1/snapshots/issues` (`api.js:1121-1125`) | `api/routers/snapshots.py:123 get_operator_issues()` | `operator_issues` (`151-158`) | `scripts/hermes_health.py:65 log_issue()` INSERT (`122-130`), called from `hermes_fixers.py` (multiple sites) and `hermes_data_integrity.py` inside `run_cycle()` | grid-hermes 5-min loop | `Operator.jsx:61 setIssues(issuesRes?.issues \|\| issuesRes \|\| [])`; empty → "No issues found" (`359-363`) | wired; unknown-runtime |
| 4 | Operator.jsx:55 | `getSnapshotLatest('pipeline_summary', 10)` → `GET /api/v1/snapshots/latest/pipeline_summary?n=10` (`api.js:1106-1108`) | `snapshots.py:29 get_latest_snapshots()` → `store/snapshots.py:504 get_latest()` | `analytical_snapshots WHERE category = :cat` (`518-529`) | same `save_cycle_snapshot()` as row 2 — columns match here | same as row 2 | `Operator.jsx:62 setRecentCycles(cyclesRes?.snapshots \|\| cyclesRes \|\| [])`; empty → section hidden, no empty-state text | wired; unknown-runtime |
| 5 | Operator.jsx:56 | `getHealth()` → `GET /api/v1/system/health` (`api.js:613`) | `system.py:68 health()` | subsystem checks | n/a | n/a | Gated `health && !loading` (`Operator.jsx:154`) | unknown-runtime |
| 6 | Operator.jsx:57 | `getFreshness()` → `GET /api/v1/system/freshness` (`api.js:612`) | `system.py:377 freshness()` | family freshness | n/a | n/a | Gated `freshness && !loading && freshness.families` (`Operator.jsx:216`) | unknown-runtime |
| 7 | Discovery.jsx:41 | `getHypothesisResults(params)` → `GET /api/v1/discovery/hypotheses/results` (`api.js:599-602`) | `api/routers/discovery.py:155 get_hypothesis_results()` | `hypothesis_registry` ⋈ `validation_results` (`full_period_metrics, overall_verdict, run_timestamp`, `172-181`) | `hypothesis_registry` INSERT `scripts/autoresearch.py:565-573`; `validation_results` by `validation/backtest.py WalkForwardBacktest`; columns match | `run_autoresearch()` via `maybe_run_autoresearch()` (`hermes_fixers.py:1680-1699`, gated every 12h), called from `run_cycle()` at `hermes_operator.py:1839-1842` | `Discovery.jsx:43 catch {}` swallows; `:47 if (!loaded \|\| results.length === 0) return null` — section renders nothing | wired; unknown-runtime |
| 8 (write) | Discovery.jsx:129 | `promoteHypothesis(h.id)` → `POST /api/v1/discovery/hypotheses/{id}/promote` (`api.js:596-598`) | write path, not traced | — | — | manual | `catch → addNotification('error')` (`132-134`) | n/a (write action, not a dataset read) |
| 9 | Discovery.jsx:167 | `getJobs()` → `GET /api/v1/discovery/jobs` (`api.js:497`) | `discovery.py:117 get_jobs()` | in-memory job dict (not persisted) | `trigger_orthogonality`/`trigger_clustering` background tasks, manual button only (`discovery.py:76-113`) | manual script/button only, no schedule | `Discovery.jsx:171 setJobs(j.jobs \|\| [])` | disabled-or-unregistered-schedule (manual-trigger only) |
| 10 | Discovery.jsx:168-169 | `getResults('orthogonality'/'clustering')` → `GET /api/v1/discovery/results/{type}` (`api.js:498`) | `discovery.py:129, :142` | in-memory jobs filtered `status=="complete"` | `discovery/orthogonality.py`, `discovery/clustering.py` | manual only | `Discovery.jsx:172-173` stays `null` if absent | disabled-or-unregistered-schedule |
| 11 | Discovery.jsx:183 | `getHypotheses(params)` → `GET /api/v1/discovery/hypotheses` (`api.js:499-502`) | `discovery.py:261 get_hypotheses()` | `hypothesis_registry` (`271-279`) | same as row 7 | same as row 7 | `Discovery.jsx:185-187` error notification; empty list renders nothing | wired; unknown-runtime |

**Other views checked:** `CatalystTimeline.jsx` uses "hypothesis" only as an event-type label
(lines 28, 57, 84) — not a run-state view. `Settings.jsx` Hermes tab
(`Settings.jsx:156,205,223,237-242,545-575,1076`) is a **second, independent consumer** of
`getHermesStatus()`; it renders `sched.autoresearch` with a hardcoded fallback label
`'weekdays 2 AM'` (`Settings.jsx:553`) and is silently starved by the same `snapshots` mismatch
as row 2b above.

**Journey 4 verdict:** a research-status view exists (Operator.jsx + Discovery.jsx),
**operator-only**. The writer → table → router → view pipeline is mostly wired with matching
columns (rows 2/4, 3, 7/11). Two concrete static defects: (a) `system.py:1124-1137`'s live-state
branch is dead code in the deployed topology; (b) `system.py:1019-1048` queries columns that do
not exist in the snapshot table's DDL, starving both Operator.jsx and Settings.jsx's Hermes tab.
None of these views exposes hypothesis skip/failure reasons, evaluation version, or
out-of-sample results as first-class fields — noted as a gap for a later slice, not fixed here.

---

## Journey 5 — Data health / source drill-down

**Scoping:** `PipelineHealth.jsx` (`pipeline-health`), `SystemLogs.jsx` (`system`), and
`Snapshots.jsx` (`snapshots`) are all `group: 'operations', nav: 'drawer'`
(`routes.js:526-559`) — operator-only, not in `DAD_VIEWS`.

| # | View | API call | Router · fn | Tables read | Writer | Schedule | Missing-data behavior | Classification |
|---|---|---|---|---|---|---|---|---|
| 1 | PipelineHealth.jsx:322 | `api.getPipelineHealth()` → `GET /api/v1/system/pipeline-health` (`api.js:614`) | `api/routers/system.py:509 pipeline_health()` | `source_catalog`, `raw_series` (per-source last-pull/rows/series-count, `529-562`); `feature_registry`+`resolved_series` (coverage-by-family, `626-638`); `server_log` (recent errors, `653-659`); `raw_series`/`feature_registry`/`resolved_series` (resolver status, `670-707`) | **Computed live at request time — no materializer.** Freshness/status derived in-handler from `raw_series.pull_timestamp`/`source_catalog.last_pull_at` (`564-593`); `source_catalog.last_pull_at` is written by `ingestion/scheduler.py:199` after a successful pull | `source_catalog.last_pull_at` writer runs continuously under `grid-scheduler.service`; the health computation itself is unscheduled — it runs only when the view polls every 60s (`PipelineHealth.jsx:332`) | Any exception inside the handler's `try` → `system.py:711-712` logs a warning and returns `PipelineHealthResponse` built from **empty locals**: `sources=[]`, `coverage={}`, `recent_errors=[]`, `resolver=ResolverStatus()` (`api/schemas/system.py:158-163`) — HTTP 200, not an error shape; `PipelineHealth.jsx:324` only shows the red error card when `res.error` is truthy, which never happens here, so the view renders "Total Sources: 0 / Healthy: 0 / Stale: 0 / Broken: 0" (`387-402`) and "No sources match filter" (`487-493`) — **indistinguishable from a genuinely-empty-but-healthy system** | unknown-runtime *(silent zero-default on backend query failure, not surfaced as "unavailable" — `system.py:711-712`)* |
| 2 | SystemLogs.jsx:19 (logs tab) | `api.getLogs(source, 100)` → `GET /api/v1/system/logs` (`api.js:266-268`) | `system.py:734 get_logs()` | No DB table — flat log files via `subprocess.run(["tail", ...])` (`741-746`) | n/a | n/a | Honest: `useAsyncData.js:51-57` converts the api.js `{error:true}` marker into a thrown error; `SystemLogs.jsx:126-127` shows `ErrorState title="Logs unavailable"`; empty → "No logs available" (`147-149`) | unknown-runtime (not a source/freshness signal) |
| 3 | SystemLogs.jsx:46 (config tab) | `api.getConfig()` → `GET /api/v1/config` (`api.js:505`) | `api/routers/config.py:48 get_config()` | None — `settings.model_dump()` (`50-53`) | n/a | n/a | `SystemLogs.jsx:46 api.getConfig().catch(() => null)` — on failure the Config tab body renders nothing, no error badge (`155`) | unknown-runtime |
| 4 | SystemLogs.jsx:47 (sources tab) | `api.getSources()` → `GET /api/v1/config/sources` (`api.js:509`) | `api/routers/config.py:78 get_sources()` | `source_catalog` (`86-89`), incl. `last_pull_at`, `trust_score`, `active`, `priority_rank` (`94-96`) | `ingestion/scheduler.py:199` updates `last_pull_at`; `active`/`priority_rank`/`trust_score` written by the operator toggle (see below) and pullers/`normalization/resolver.py` (not traced) | `grid-scheduler.service` continuous; endpoint itself is a plain read | `SystemLogs.jsx:47 api.getSources().catch(() => [])`; empty → "No sources found" (`199-200`) — **identical to "zero sources configured"** | unknown-runtime |
| 5 (write) | SystemLogs.jsx:57 (Enable/Disable toggle) | `api.updateSource(id, {active})` → `PUT /api/v1/config/sources/{id}` (`api.js:510-515`) | `api/routers/config.py:102 update_source()` | writes `source_catalog` (`active`, `priority_rank`, `trust_score`, `111-127`) | operator-triggered write | manual only | On failure only `console.warn(...)` — no user-visible error; toggle silently no-ops (`55-59`) | n/a (write action, not a dataset read) |
| 6 | Snapshots.jsx:40 | `api.getSnapshotLatest(category, 1)` → `GET /api/v1/snapshots/latest/{category}?n=1` (`api.js:1106-1108`) | `snapshots.py:29 get_latest_snapshots()` | `analytical_snapshots WHERE category = :cat` (`store/snapshots.py:518-529`); data-health tab = `category='pipeline_summary'` | `hermes_fixers.py:1707 save_cycle_snapshot()` — same writer as Journey 4 row 2 | grid-hermes 5-min loop | **Consumer-query-mismatch (response shape):** router returns a bare `list[dict]` (`snapshots.py:29-48`), never `{"snapshots": [...]}`; `Snapshots.jsx:43 setLatest(latestRes?.snapshots?.[0] \|\| latestRes \|\| null)` falls through to the whole array; `extractMetrics` (`64-77`) then filters everything out. LATEST SNAPSHOT card renders date `-` (`144`) and zero metric tiles with **no error**, even though the backend returned valid data | **consumer-query-mismatch** |
| 7 | Snapshots.jsx:41 | `api.getSnapshotHistory(category)` → `GET /api/v1/snapshots/history/{category}` (`api.js:1109-1115`) | `snapshots.py:51 get_snapshot_history()` | `analytical_snapshots` (`store/snapshots.py:570-582`) | same as row 6 | same as row 6 | `historyRes?.snapshots \|\| historyRes \|\| []` (`44`) works by accident here (bare array); empty → "No snapshots found for {category}" (`248-251`) — honest | unknown-runtime (no defect) |
| 8 | Snapshots.jsx:56 (Compare) | `api.compareSnapshots(category, dateA, dateB)` → `GET /api/v1/snapshots/compare/{category}` (`api.js:1116-1120`) | `snapshots.py:70 compare_snapshots()` | `analytical_snapshots` (`store/snapshots.py:611-628`) | same | same | Missing date → `store/snapshots.py:630-636` builds `missing`, router raises 404 (`84-86`); frontend `setError(...)` (`56-60`, rendered `135`) — honest end-to-end | unknown-runtime (no defect) |

**Health/freshness endpoints in `api.js` NOT used by these three views:** `getFreshness()`
(`api.js:612`, `system.py:377`), `getHealth()` (`api.js:613`, `system.py:68`), `getStatus()`
(`api.js:265`), `getHermesStatus()` (`api.js:611`) — all four are called elsewhere (Journeys 1/4),
just not by PipelineHealth/SystemLogs/Snapshots. Not claimed dead; other views weren't in scope.

---

## Classification counts by journey

| Journey | missing-module | unwired-code | disabled-or-unregistered-schedule | source-or-auth-failure | parser-drift-suspected | consumer-query-mismatch | unknown-runtime | Total (read edges) |
|---|---|---|---|---|---|---|---|---|
| 1. Home / market overview | 0 | 0 | 3 | 0 | 0 | 3 | 11 | 17 |
| 2. Ticker investigation | 0 | 2 | 1 | 0 | 0 | 1 | 8 | 12 |
| 3. Watchlist / portfolio / edge | 0 | 2 | 1 | 0 | 0 | 1 | 4 | 8 |
| 4. Research status | 0 | 1 | 2 | 0 | 0 | 1 | 8 | 12 |
| 5. Data health / source drill-down | 0 | 0 | 0 | 0 | 0 | 1 | 6 | 7 |
| **Total** | **0** | **5** | **7** | **0** | **0** | **7** | **37** | **56** |

Write-only actions (`promoteHypothesis`, `updateSource`) are listed in their journeys' tables for
completeness but excluded from these counts and from the JSON sidecar, which covers dataset
*read* dependencies only.

---

## Contract extension proposal (not implemented)

Read from branch `feat/availability-provenance-contract` at commit `721992fd` via
`git show 721992fd:store/availability.py` and `git show 721992fd:docs/reference/AVAILABILITY_CONTRACT.md`
(not checked out). The contract defines exactly three payload kinds — `available` (with
`Provenance`: `source`, `as_of`, `vintage`, `basis`, `estimated`), `partial` (available plus a
named `missing` list), and `unavailable` (`available:false`, `status`, `reason`, every measured
field `None`) — plus `Freshness` (`as_of`, `age_days`, `stale`, `stale_after_days`) and helpers
(`measured_or_none`, `mean_of_available`, `cacheable`) that stop a `None` from ever becoming a
`0`/`0.5`/`"NEUTRAL"`.

Against the twelve requested per-field dimensions:

| Dimension | Carried today? | Where |
|---|---|---|
| state (available / partial / unavailable) | **Yes** | `available`/`status` in `available()`, `partial()`, `unavailable()` |
| value + unit | Value: yes (arbitrary `**fields`). **Unit: no** — no unit is modeled anywhere in the dataclasses | `available(**fields)` |
| observation period | **Partial** — `as_of` is a single observation *date*, not a start/end period | `Provenance.as_of` |
| published timestamp (when the source released the data) | **No** — not modeled; conflated with `as_of`/`vintage` | — |
| available/acquired timestamp | **Yes**, as `vintage` ("when that observation was pulled/computed") | `Provenance.vintage` |
| ingested timestamp | **No** — not distinct from `vintage`; no separate `ingested_at` | — |
| revision/vintage | **Yes** — explicit `vintage` field, plus doc guidance on `resolved_series` multi-vintage reads (`store/pit.py` `DISTINCT ON`) | `Provenance.vintage`; contract doc "Reading source tables" |
| source reference | **Yes** — `source` (e.g. `"raw_series:WALCL"`) | `Provenance.source` |
| calculation version | **No** — `basis` names the method/parameters (e.g. `"sigma=0.25"`) but there is no versioned identifier for the calculation code itself | — |
| coverage | **Partial** — `partial()`'s `missing: list[str]` names gaps; `mean_of_available()` returns a sample size the *caller* may publish, but there is no first-class coverage field on an `available` payload | `partial(missing=...)` |
| stale reason | **No** — `Freshness` carries `stale: bool` + `age_days` + `stale_after_days`, but no text explaining *why* something is stale (only `unavailable()`'s `reason` covers missing data, not staleness) | `Freshness` dataclass |
| provenance (measured/derived/modeled) as an axis separate from validity/availability | **Yes, explicitly** — `estimated: bool` + `basis`, orthogonal to `available`/`status` | `Provenance.estimated`, `Provenance.basis` |

**Best first adopter: `api/routers/watchlist_overview.py::get_ticker_edge`** (the
`/api/v1/watchlist/{ticker}/edge` endpoint backing `WatchlistAnalysis.jsx`'s `InsiderEdgePanel`
and `TrustBar`, Journey 2 row 9 / Journey 3 row 3 above). This function is the single richest
concentration of the exact anti-patterns `AVAILABILITY_CONTRACT.md` names by example — a literal
`confidence: 0.5` on a missing convergence signal (`watchlist_overview.py:702, 711`), a
`trust_score` default of `0.5` for congressional trades (`:545`), smart-money code that coerces a
*measured* `0` into `0.5` (`:626`), and a `probability` default of `0.5` for prediction markets
(`:644`) — plus a `direction`/`signal_type` column mismatch that silently pins the displayed
direction to a constant. It sits on the production path for every operator-facing trust/edge
decision in the app (reached from Dashboard, CrossReference, IntelDashboard, EdgeScanner,
CommandPalette, and IntelligenceSearch), it already has open remediation attention in this exact
spot (`NOTE (lead)` above: #537/#546, composed tree `44019a43`, and a parallel worktree at
`d9abaf90`), and adopting the contract here — replacing every bare `0.5`/`"neutral"` fallback with
`unavailable()`/`partial()` and fixing the `signal_type`/`direction` read — would both fix a real
defect and give the in-flight remediation lanes a shared vocabulary to converge on rather than
three independent patches racing each other.
