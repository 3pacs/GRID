# Fake-data audit — Cluster C (trading / signals / watchlist routers)

Repo: `C:\Users\owner\dev\GRID-wt-fake-data-audit` @ `audit/fake-data-cleanup-20260917` (= origin/main `fe6765bc`, deployed as `grid-api` on grid-svr).
Scope: 29 routers under `api/routers/` plus helper modules under `trading/`, `oracle/`, `valuation/`, `derivatives*`, `strategy/`, `backtest/`, `store/`, `features/`, `physics/`, `discovery/`, `alpha_research/` followed where a response value's origin was unclear.

Categories: **1** = fabricated fallback · **2** = placeholder metric · **3** = misleading provenance · **4** = hardcoded values as observations.

---

## Findings table

### HIGH

| # | file:line | snippet | endpoint(s) | cat | sev | conf | honest replacement |
|---|---|---|---|---|---|---|---|
| H1 | `api/routers/watchlist_core.py:218,235-240,307-309` | `ESTIMATED_PORTFOLIO = 125_000  # estimated portfolio value` … `alloc_value = ESTIMATED_PORTFOLIO * it["weight"]` … `"total_value": ESTIMATED_PORTFOLIO` | `GET /api/v1/watchlist/portfolio` | 1,4 | HIGH | CONFIRMED | Return `total_value: null` and dollar P&L `null` until real position sizes exist; expose only percentage returns, which are measured. |
| H2 | `api/routers/watchlist_core.py:233,237` | `pct_1m = pct_1w * 4.0 / 1.0  # rough extrapolation from 1w` → `pnl_1m = round(alloc_value * (pct_1m or 0), 2)` | `GET /api/v1/watchlist/portfolio` | 2 | HIGH | CONFIRMED | Drop `total_pnl_1m`/`pct_1m` unless a real 1-month close is loaded; never extrapolate a 1-week return ×4. |
| H3 | `api/routers/derivatives.py:787-821` | `fomc_dates = [date(2026,1,28), …]` / `cpi_dates = [… ]  # typically ~10th-15th of each month` appended as `{"type":"cpi","label":"CPI Release"}` | `GET /api/v1/derivatives/flow-timeline/{ticker}` (`catalysts[]`) | 4 | HIGH | CONFIRMED | Read FOMC/CPI dates from an ingested calendar table; return `catalysts: []` with `catalyst_source: "unavailable"` when it is empty. |
| H4 | `trading/contagion_to_ticket.py:59,396-400,429` | `DEFAULT_CONFIDENCE_NO_HISTORY: float = 0.55` … `else: confidence = DEFAULT_CONFIDENCE_NO_HISTORY` … `kelly = compute_kelly_fraction(confidence)` | `GET /api/v1/trade-tickets/recent`, `GET /api/v1/contagion/{prediction_id}/tickets` | 2 | HIGH | CONFIRMED | With `accuracy_n == 0`, emit `confidence: null`, `kelly_size: 0` and `confidence_basis: "no_backtest_history"` instead of a 0.55 that sizes the trade. |
| H5 | `trading/contagion_to_ticket.py:377,386-388` + `trading/options_recommender.py:292-309` | `iv_atm = float(signal.get("iv_atm") or 0.30)` → `estimate_premium(spot, iv_atm, dte)` → `entry_premium/target_premium/stop_premium` | same as H4 | 1,2 | HIGH | CONFIRMED | Skip the ticket when `iv_atm` is missing; when premiums are modelled rather than quoted, tag them `premium_basis: "modelled_1sigma"` in the ticket payload. |
| H6 | `trading/options_recommender.py:1152-1177` | `base_prob = 0.30 + (score - 5.0) * 0.06` … `return max(0.10, min(0.65, base_prob))` → `kelly_fraction`, `expected_return`, `suggested_contracts` | `GET/POST /api/v1/options/recommendations*`, `POST /api/v1/trading/options-recommendations` | 2 | HIGH | CONFIRMED | Derive win probability from the recorded `options_recommendations.outcome` history for that score bucket; return `null` (and no Kelly size) until that history exists. |
| H7 | `trading/options_recommender.py:1047-1107` (esp. `1064-1065`) | `r = 0.05` / `sigma = self._get_atm_iv(db, ticker) or 0.25` → Black-Scholes price returned as the recommendation's `entry_price` | same as H6 | 1 | HIGH | CONFIRMED | Reject the recommendation when no bid/ask snapshot exists, or return `entry_price_basis: "model"` plus the σ/r used, alongside `entry_bid`/`entry_ask` = null. |
| H8 | `trading/options_recommender.py:1091,1110` | `if not gex_profile: return entry_price * 2.0` — docstring says "Set target from GEX expected move" | same as H6 | 2 | HIGH | CONFIRMED | Return `target_price: null` (and no `target_return_pct`/`expected_return`) when no GEX profile is available. |
| H9 | `physics/dealer_gamma.py:382-390` + `:193`, consumed at `api/routers/derivatives.py:893-899` | `if not rows: … return self._load_chain(ticker, latest[0])` while `compute_gex_profile` still returns `"snap_date": str(snap_date)` (the **requested** date) | `GET /api/v1/derivatives/gex/{ticker}`, `/walls/{ticker}`, `/vanna-charm/{ticker}`, `/regime`, `/flow-timeline/{ticker}` | 3 | HIGH | CONFIRMED | **Look-ahead**: in `flow-timeline` each historical `sig_date` silently reuses the *latest* chain, so a future chain is dated as a past bar. Return `snap_date` = the date the chain actually came from and refuse the substitution when `snap_date` was explicitly passed. |
| H10 | `api/routers/watchlist_overview.py:573` | `"volume_vs_avg": dp_meta.get("volume_vs_avg", 1.0)` | `GET /api/v1/watchlist/{ticker}/edge` (`dark_pool`) | 1 | HIGH | CONFIRMED | Omit `volume_vs_avg` (or `null`) when the metadata lacks it — 1.0 reads as "exactly average dark-pool volume", a measurement that was never taken. |
| H11 | `api/routers/watchlist_overview.py:644-645` | `"probability": meta.get("probability", 0.5), "change_24h": meta.get("change_24h", 0.0)` | `GET /api/v1/watchlist/{ticker}/edge` (`prediction_markets`) | 1 | HIGH | CONFIRMED | Emit `null`; a fabricated 50% market-implied probability is a tradeable-looking number that no market quoted. |
| H12 | `valuation/derivatives_support.py:50,102,151` | `if self.short_float_pct is None: return 50.0  # Neutral if no data` (and the same at 102, 151) → `derivatives_support_score`, `support_regime` | `GET /api/v1/valuation/derivatives/{ticker}`, `GET /api/v1/valuation/analyze/{ticker}` | 2 | HIGH | CONFIRMED | Return `null` sub-scores, re-weight the composite over the layers that do have data, and expose `layers_with_data` so "NEUTRAL" cannot mean "we measured nothing". |

### MED

| # | file:line | snippet | endpoint(s) | cat | sev | conf | honest replacement |
|---|---|---|---|---|---|---|---|
| M1 | `api/routers/options.py:104` (+ `176`, `191`) | `from trading.options_recommender import generate_recommendations` — **no such module-level symbol exists** (only `OptionsRecommender.generate_recommendations`, `options_recommender.py:590`) → every call raises `ImportError` → `_load_saved_recommendations` with `"reason": "Options recommender module is not installed"` | `GET /api/v1/options/recommendations`, `POST /api/v1/options/recommendations/refresh` | 3 | MED | CONFIRMED | Fix the import to the class; until then the "refresh" endpoint never refreshes and the reason text is false — it should say the persisted rows' age and `fresh_scan: false`. |
| M2 | `api/routers/ten_year_portfolio.py:280` | `"source": "raw_series:yfinance_adjusted_close"` — but 26 of 31 tickers load from `resolved_series` `<ticker>_full` (`_load_price_history:170-185`), and `_full` falls back to raw close (`normalization/entity_map.py:840-878`) | `GET /api/v1/ten-year-portfolio/weekly`, `/export.xlsx`, `/workbook/*` | 3 | MED | CONFIRMED | Report per-ticker `{source, price_basis}` actually used; only claim `adjusted_close` for rows resolved from an `adj_close` vintage. |
| M3 | `api/routers/derivatives.py:396-402,469` | `# Get spot price approximation (highest OI call strike)` → `"spot": spot` | `GET /api/v1/derivatives/term-structure/{ticker}` | 2 | MED | CONFIRMED | Rename to `atm_reference_strike` or read the real close from `resolved_series`; `spot` must be a price, not a strike. |
| M4 | `api/routers/derivatives.py:441-461` | `otm_pct = 0.05  # 25-delta approximations` → `"iv_25d_put"`, `"iv_25d_call"` | same as M3 | 2 | MED | CONFIRMED | Rename the fields `iv_5pct_otm_put/call`, or compute real 25-delta strikes from the chain's deltas. |
| M5 | `api/routers/derivatives.py:892-899` | `except Exception: net_gex = 0; regime_raw = "neutral"` → appended to `history[]` | `GET /api/v1/derivatives/flow-timeline/{ticker}` | 1 | MED | CONFIRMED | Skip the bar (or emit `net_gex: null, regime: null`) rather than plotting a zero-GEX neutral day that was never computed. |
| M6 | `physics/dealer_gamma.py:468` | `"snap_date": str(snap_date or date.today())` while each ticker's numbers came from its own (possibly older) latest snapshot | `GET /api/v1/derivatives/overview` | 3 | MED | CONFIRMED | Report `min`/`max` of the per-ticker snapshot dates actually used, not `today()`. |
| M7 | `api/routers/signals.py:183` | `series[name].append(float(value) if value is not None else 0.0)` | `GET /api/v1/signals/timeseries` | 1 | MED | CONFIRMED | Emit `null` for missing observations so sparklines show a gap instead of a spike to zero. |
| M8 | `api/routers/signal_registry.py:162-169` | `score = 50` … returned as top-level `"score"` even when no model produced a result | `POST /api/v1/ensemble/predict` | 2 | MED | CONFIRMED | Return `score: null` with `consensus: {}` when `ok` is empty; 50 is indistinguishable from a real balanced consensus. |
| M9 | `api/routers/discovery.py:733` | `z = ((last_row - mean) / std).fillna(0)` → `z_scores[].zscore` | `GET /api/v1/discovery/smart-heatmap` | 1 | MED | CONFIRMED | Drop the feature from `z_scores` when the z is undefined; 0 reads as "exactly at its mean". |
| M10 | `api/routers/discovery.py:632-637` | `if idx == 0: interp = f"{var_pct:.0%} of variance explained by risk-on/risk-off"` / `elif idx == 1: … "rates/duration factor"` | `GET /api/v1/discovery/correlation-matrix` (`pca.components[].interpretation`) | 3,4 | MED | CONFIRMED | Derive the label from the actual top loadings, or return only `top_features` and let the client label it. |
| M11 | `api/routers/discovery.py:467-474` | `DISPLAY_NAMES = {"treasury_10y": "TLT (10Y)", "hy_spread": "HYG (spread)", "gold_price": "GLD", "crude_oil": "OIL", …}` | same as M10 (`features[]`, `matrix`) | 3 | MED | CONFIRMED | `treasury_10y` is a yield and `hy_spread` a spread — labelling them with ETF tickers implies price series. Use "UST 10Y yield", "HY OAS". |
| M12 | `api/routers/watchlist_overview.py:328-331` | `pct = price_info["pct_1d"]` … `f" The stock is {direction} {abs(pct):.1f}% on the day."` — but `pct_1d` is a **fraction** (`watchlist_helpers.py:245`) | `GET /api/v1/watchlist/{ticker}/overview` (`sections[].body`, `overview`) | 3 | MED | CONFIRMED | Unit mislabel: a +2.34% day prints "up 0.0%". Multiply by 100 (or store percent consistently across the price stack). |
| M13 | `api/routers/watchlist_analysis.py:137,148-157` | `hist = yf.Ticker(ticker_upper).history(period=yf_period)` — yfinance ≥0.2 defaults `auto_adjust=True`, so these are **adjusted** closes served as `price_source: "yfinance"` and written back via `_cache_price_to_db` | `GET /api/v1/watchlist/{ticker}/analysis`; contaminates `raw_series` read by `/prices`, `/enriched`, `/quote`, price alerts | 3 | MED | CONFIRMED | Pass `auto_adjust=False`, matching the explicit comments in `watchlist_helpers.py:234-237` and `:330-337`; mixing bases in one series silently shifts historical price levels. |
| M14 | `api/routers/watchlist_helpers.py:347,377` | `now_iso = datetime.now(timezone.utc).isoformat()` … `"updated_at": now_iso` for every ticker of a `period="5d"` download | `POST /api/v1/watchlist/refresh-prices`, `GET /api/v1/watchlist/prices`, `/portfolio` | 3 | MED | CONFIRMED | Use the close's own bar timestamp; `updated_at` currently claims "now" for a Friday close served on a Sunday. |
| M15 | `api/routers/watchlist_core.py:268-272` | `beta_map = {"stock": 1.1, "crypto": 1.8, "etf": 1.0, …}` → `risk_metrics.beta_weighted` | `GET /api/v1/watchlist/portfolio` | 2 | MED | CONFIRMED | Compute beta from the actual return series vs SPY, or label the field `beta_proxy_by_asset_class`. |
| M16 | `api/routers/watchlist_core.py:236` | `pnl_1d = round(alloc_value * pct_1d, 2) if pct_1d is not None else 0` (summed into `total_pnl_1d`) | `GET /api/v1/watchlist/portfolio` | 2 | MED | CONFIRMED | Exclude the position from the total and report `positions_missing_price` rather than counting an unknown move as zero P&L. |
| M17 | `valuation/intrinsic.py:170,194-195,232` | `values = {… "filing_date": as_of …}` ; `values["data_freshness"] = "STALE"` is then **dropped** by `filtered = {k: v for k, v in values.items() if k in valid_fields}` (`FinancialInputs` has no such field) | `GET /api/v1/valuation/analyze/{ticker}`, `/history/{ticker}` | 3 | MED | CONFIRMED | `valuation_date` is stamped *today* for statements that may be a year old and `data_freshness` is permanently `"CURRENT"`. Carry the real max `obs_date` and the STALE flag through to `ValuationResult`. |
| M18 | `valuation/intrinsic.py:117-121,311-312,488-489` | `_DEFAULT_COC = 0.10` / `_DCF_GROWTH_RATE = 0.03` / `_DCF_DISCOUNT_RATE = 0.10`, and `maintenance_capex = abs(capex) * 0.70  # heuristic` (`:302`) → `dcf_ps`, `epv_ps`, `owner_earnings_ps` | `GET /api/v1/valuation/analyze/{ticker}` | 2 | MED | CONFIRMED | `ValuationResult` has no assumptions field — add `assumptions: {growth, discount, coc, maintenance_capex_frac}` to every response so a fixed 3%/10% is visible to the reader. |
| M19 | `alpha_research/realized_alpha.py:85,406-411` | `_SPY_FALLBACK = "sp500_full"` … `log.warning("SPY benchmark falling back to {n} (index level, no dividends)")` — logged server-side only; the API field stays `mean_spy` | `GET /api/v1/alpha/realized` | 3 | MED | CONFIRMED | Persist and return the resolved `benchmark_feature` name; "alpha vs SPY" computed against a price index understates the benchmark by its dividend yield. |
| M20 | `discovery/options_scanner.py:765-817` (esp. `783`, `804`, `807`, `812`) | `return composite_score * 5  # Rough fallback` … `otm_cost_pct = iv_atm * 0.5` … `base_payoff = (expected_move_pct / (otm_cost_pct*100)) * iv_leverage * 10` → `is_100x` (`:74-78`) | `GET /api/v1/options/scan`, `/api/v1/options/100x`, `/api/v1/derivatives/scan` | 2 | MED | CONFIRMED | `is_100x` is a boolean assertion built entirely from tuning constants. Expose the inputs and rename the endpoint/field (`heuristic_payoff_flag`), or price the actual OTM contract. |
| M21 | `api/routers/watchlist_overview.py:558-559,602-605` | `"shares": meta.get("shares", 0), "value": meta.get("value", 0)` ; `"strike": meta.get("strike", 0), "premium": meta.get("premium", 0)` | `GET /api/v1/watchlist/{ticker}/edge` (`insider[]`, `whale_flow[]`) | 2 | MED | CONFIRMED | Use `null`; a "0 shares / $0" insider row or a "$0 strike" whale print reads as an observed trade of zero size. |
| M22 | `api/routers/watchlist_overview.py:545,626,702,711` | `round(sig.get("trust_score", 0.5), 2)` ; `round(float(r[3]) if r[3] else 0.5, 2)` ; `convergence: dict = {"direction": "neutral", "source_count": 0, "confidence": 0.5}` (kept as-is when `detect_convergence` raises) | `GET /api/v1/watchlist/{ticker}/edge` | 2 | MED | CONFIRMED | `null` trust scores; on a convergence exception return `convergence: null` + `convergence_error`, not a 0.5 confidence that survives the failure silently. |
| M23 | `api/routers/prediction_backtest.py:128-131` | `except Exception: stats[table] = 0` | `GET /api/v1/pm-backtest/stats` | 2 | MED | CONFIRMED | Return `null` with an `errors` list; 0 rows and "the count query failed" are different facts. |
| M24 | `trading/options_recommender.py:211-268` (`pick_strike`, `pick_expiry`), used by `trading/contagion_to_ticket.py:384-385` | `raw = spot * (1 - offset) if is_short else spot * (1 + offset)` (2% OTM) ; `days_to_fri = (4 - target_d.weekday()) % 7` — "close enough to a listed monthly cycle for a ticket card" | `GET /api/v1/trade-tickets/recent`, `/api/v1/contagion/{id}/tickets` | 2 | MED | CONFIRMED | The `strike`/`expiry` on the ticket may not be a listed contract. Validate against `options_snapshots` and drop the ticket, or mark `contract_verified: false`. |
| M25 | `api/routers/valuation.py:484-490` | `SELECT value FROM raw_series WHERE series_id = :sid … ORDER BY obs_date DESC LIMIT 1` → `valuation["live_price"]` | `GET /api/v1/valuation/catalyst-timeline/{ticker}` | 3 | MED | CONFIRMED | It is the last stored daily close, possibly days old. Rename to `last_close` and return its `obs_date`. |
| M26 | `api/routers/tps.py:139-145` | `"as_of": entries[0]["as_of_date"] if entries else today.isoformat()` | `GET /api/v1/tps/today` | 3 | MED | CONFIRMED | With no snapshots (or an offset past the end) the envelope claims today's date. Return `as_of: null`. |
| M27 | `api/routers/tps.py:123-126` | `except Exception: total = len(rows)` → drives `has_more` | `GET /api/v1/tps/today` | 2 | MED | CONFIRMED | Return `total: null, has_more: null` when the count query fails, rather than a total equal to the page size. |
| M28 | `api/routers/ten_year_portfolio.py:345-353` | `"method_signals": [{"label": "Long Term Chart", "strength": 1}, …], "formula_functions": []` under `workbook_summary` | `GET /api/v1/ten-year-portfolio/export.xlsx` | 4 | MED | CONFIRMED | Nothing is measured here — either compute the signal strengths from the recommendation or drop `workbook_summary` from the model export. |

### LOW

| # | file:line | snippet | endpoint(s) | cat | sev | conf | honest replacement |
|---|---|---|---|---|---|---|---|
| L1 | `api/routers/watchlist_core.py:111-114` | `return {"prices": cached, "fresh": True, "cached": True}` for a cache up to `_PRICE_CACHE_TTL = 300` s old | `GET /api/v1/watchlist/prices` | 3 | LOW | CONFIRMED | Return `cache_age_seconds` instead of a flat `fresh: true`. |
| L2 | `strategy/engine.py:198-200` + `:19-52` | `except Exception: … return []` → `get_active_strategies` serves all four `DEFAULT_STRATEGIES` | `GET /api/v1/strategy/active`, `/for-regime/{state}` | 1 | LOW | CONFIRMED | A DB outage is indistinguishable from "no override configured". `source: "default"` is honest about origin; add `db_reachable: false`. |
| L3 | `api/routers/postmortem_lessons.py:135` | `confidence_in_analysis=full.get("confidence_in_analysis", 0.5)` | `GET /api/v1/postmortem-lessons` (indirect, via LLM synthesis input) | 2 | LOW | PLAUSIBLE | Skip the post-mortem rather than feeding a fabricated 0.5 confidence into the lessons prompt. |
| L4 | `api/routers/options.py:180-193` | the DB-failure branch also returns `"reason": "Options recommender module is not installed"` | `GET /api/v1/options/recommendations` | 3 | LOW | CONFIRMED | Distinguish "engine unavailable" from "persisted-row query failed". |
| L5 | `valuation/intrinsic.py:36-39` | `"""… All dollar values in millions unless noted."""` — `ingestion/altdata/fmp_puller.py:511,545,578` stores `float(val)` straight from FMP (absolute dollars) | — (internal) | 3 | LOW | CONFIRMED | Docstring only; the per-share arithmetic is self-consistent because `shares_outstanding = market_cap / price` uses the same scale. Fix the comment. |

---

## Ruled-out constants (considered, excluded)

- `strategy/engine.py:19-52` `DEFAULT_STRATEGIES` — curated static map, labelled to the client as `source: "default"` with `id: null`, `assigned_at: ""` (vs `source: "database"` at `:239`). Honest.
- `api/routers/derivatives.py:112-128` regime `interpretations` dict, `:582-611` narrative strings — UI copy keyed off a computed regime.
- `api/routers/derivatives.py:625-629` `stale: True` + `note: "Inline fallback — LLM briefing not yet generated"` — an explicitly-labelled degraded path; this is the pattern the rest of the codebase should copy.
- `api/routers/valuation.py:512-525` `_milestone_invalidation` `type_map` — curated human-readable invalidation copy, not a metric.
- `api/routers/valuation.py:541-546` 5% stop in `_prediction_invalidation` — the arbitrary constant is printed in the string itself ("5% below entry $X"), so the reader can see it.
- `api/routers/watchlist_overview.py:192-206`, `:473-481` and `watchlist_core.py:485` P/C 0.7/1.3 and IV 0.4 sentiment cut-offs — classification thresholds, not fabricated values.
- `api/routers/conviction.py:91-96` `_DEFAULT_ACCOUNT_SIZE_USD`, `_DEFAULT_TOP_K`, `_ALLOWED_UNIVERSES` — request defaults, echoed back as query params.
- `oracle/scoreboard.py:29` partials counted as 0.5 — a stated scoring convention applied consistently; `:188` `total_pnl … or 0.0` is a SUM over zero rows.
- `strategy/ten_year_portfolio.py:415-478` Monte Carlo — seeded, but self-labelled `"method": "seeded_normal_proxy_from_historical_cagr_and_volatility"` and the export's own plan text says "risk context, not a guarantee"; inputs (`cagr`, `annual_volatility`) are measured at `:261,266`. The `0.20` default at `:448`/`:355` is unreachable in practice (`compute_chart_metrics` always sets `annual_volatility`).
- `physics/dealer_gamma.py:228-229,283` IV fallback `0.25` — `_load_chain` filters `implied_vol > 0` (`:377`), so the branch is defensive only. (The same constant in `options_recommender.py:904` is **not** guarded — see H7's neighbourhood.)
- `physics/dealer_gamma.py:114` `risk_free_rate=0.05` — a documented model input, consistent across the Greeks stack.
- `trading/hyperliquid.py`, `trading/robinhood.py` — connectors return `{"error": …}` on failure and label unexecuted orders `status: "dry_run"` / `"rejected"`; no fabricated balances or fills.
- `alpha_research/realized_alpha.py:38,226,352` `cost_bps` — an assumption, but it is a request parameter *and* echoed in every persisted row and API entry.
- `features/importance.py:592` `np.random.default_rng(seed=42)` — permutation-importance shuffling; randomness is the method, and the seed makes it reproducible.
- `api/routers/models.py`, `model_comparison.py`, `trials.py`, `tradingview.py`, `contracts.py`, `backtest.py`, `snapshots.py`, `price_alerts.py`, `realized_alpha.py`, `trade_tickets.py`, `strategy.py`, `oracle.py`, `conviction.py`, `watchlist.py` — no router-level fabrication found; all serve DB rows or propagate errors.

---

## Files reviewed (read in full)

| file | lines |
|---|---|
| `api/routers/trading.py` | 645 |
| `api/routers/trade_tickets.py` | 94 |
| `api/routers/oracle.py` | 463 |
| `api/routers/options.py` | 526 |
| `api/routers/derivatives.py` | 992 |
| `api/routers/valuation.py` | 547 |
| `api/routers/price_alerts.py` | 220 |
| `api/routers/realized_alpha.py` | 53 |
| `api/routers/strategy.py` | 108 |
| `api/routers/backtest.py` | 178 |
| `api/routers/prediction_backtest.py` | 195 |
| `api/routers/postmortem_lessons.py` | 196 |
| `api/routers/model_comparison.py` | 134 |
| `api/routers/models.py` | 311 |
| `api/routers/conviction.py` | 588 |
| `api/routers/signals.py` | 392 |
| `api/routers/signal_registry.py` | 171 |
| `api/routers/snapshots.py` | 181 |
| `api/routers/discovery.py` | 758 |
| `api/routers/watchlist.py` | 39 |
| `api/routers/watchlist_analysis.py` | 300 |
| `api/routers/watchlist_core.py` | 782 |
| `api/routers/watchlist_helpers.py` | 636 |
| `api/routers/watchlist_overview.py` | 764 |
| `api/routers/ten_year_portfolio.py` | 410 |
| `api/routers/trials.py` | 186 |
| `api/routers/tradingview.py` | 226 |
| `api/routers/contracts.py` | 76 |
| `api/routers/tps.py` | 214 |

**All 29 in-scope routers were read end to end (10,385 lines).**

Helper modules read in full: `physics/dealer_gamma.py` (493), `strategy/engine.py` (257).

Helper modules read in the regions that produce a response value (targeted, not end to end):
`trading/options_recommender.py` (1712 — lines 50-120, 211-310, 355-460, 520-600, 720-920, 1020-1210, 1560-1712),
`trading/contagion_to_ticket.py` (704 — 280-440, 470-560),
`strategy/ten_year_portfolio.py` (244-540, 620-680),
`valuation/intrinsic.py` (35-330, 480-505), `valuation/derivatives_support.py` (37-265),
`alpha_research/realized_alpha.py` (1-120, 220-460, 790-858),
`discovery/options_scanner.py` (regions around 57-80, 390-435, 765-820),
`oracle/scoreboard.py` (150-230), `normalization/entity_map.py` (840-1000),
`ingestion/altdata/fmp_puller.py` (units only).

## Files NOT fully reviewed

- `trading/options_recommender.py`, `trading/contagion_to_ticket.py`, `discovery/options_scanner.py`, `strategy/ten_year_portfolio.py`, `alpha_research/realized_alpha.py`, `valuation/*` — read only where a served field's origin was in question; time-boxed. The 5-layer sanity pipeline (`options_recommender.py:1220-1565`) was skimmed, not audited; it may contain further placeholder verdicts.
- `intelligence/*` (`decision_gateway`, `universe_ranker`, `pair_conviction`, `llm_narrator`, `signal_health_monitor`, `trust_scorer`, `lever_pullers`, `actor_network`, `long_plays`) — reached by `conviction.py` and `watchlist_overview.py:/edge`, but outside the helper directories named in the brief. `conviction.py` itself is a clean pass-through; the numbers it serves were **not** traced to source. Recommend a follow-up pass.
- `inference/live.py`, `analysis/backtest_scanner.py`, `analysis/sector_map.py`, `backtest/engine.py`, `backtest/paper_trade.py`, `store/snapshots.py`, `oracle/engine.py`, `oracle/calibration.py`, `trading/paper_engine.py`, `trading/wallet_manager.py`, `trading/prediction_markets.py`, `trading/prediction_backtest.py`, `features/importance.py` — pattern-scanned (`random`, `demo`, `sample`, literal defaults, `except → return {…}`) with no hits that reach a response; not read line by line.
