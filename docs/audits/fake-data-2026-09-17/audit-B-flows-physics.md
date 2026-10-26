# Audit B — flows / physics / intelligence routers: fabricated production data

Repo: `C:\Users\owner\dev\GRID-wt-fake-data-audit` @ `audit/fake-data-cleanup-20260917` (= origin/main `fe6765bc`, deployed as `grid-api`).
Scope: 18 routers under `api/routers/`, read in full, plus helper modules followed where a response value's origin was unclear.

Categories: **1** = fabricated fallback · **2** = placeholder metric · **3** = misleading provenance · **4** = hardcoded values presented as observations.

---

## Findings table (sorted by severity)

| # | File:line | Endpoint(s) | Cat | Sev | Conf | Issue → honest replacement |
|---|---|---|---|---|---|---|
| H1 | `intelligence/sector_health.py:478-486` | `GET /api/v1/sectors/{sector}/health` | 1,3 | HIGH | CONFIRMED | Any DB error sets all six components to 0.5 → score exactly `50.0`, trend `"stable"`, `as_of=now()`, narrative names a "strongest lever" — no error flag. Return `{"score": null, "status": "unavailable", "reason": ...}` instead of a neutral score. |
| H2 | `analysis/money_flow.py:976-991` | `GET /api/v1/flows/money-map` | 2,3 | HIGH | CONFIRMED | Fed→equities edge volume = `abs(net_liquidity_change)*0.5`, Fed→bonds `*0.3`, both stamped `"confidence": "confirmed"`. The 50/30 split is invented. Drop the split or emit the raw net-liquidity change once with `confidence: "estimated"` and the ratio in metadata. |
| H3 | `analysis/money_flow.py:980` | `GET /api/v1/flows/money-map` | 1,2 | HIGH | CONFIRMED | `"change": _safe_pct_change(abs_vol, abs_vol * 0.9)` is `(v-0.9v)/0.9v` — a constant `+0.111111` on every Fed flow edge, presented as a measured change. Set `change: null` when no prior period is available. |
| H4 | `api/routers/flows.py:2410-2436` | `GET /api/v1/flows/waterfall` | 1,3 | HIGH | CONFIRMED | When no edge carries flow, `total_flow = current_value * 0.3` ("default 30% pass-through") and the chain link still reports the edge's own `confidence` (can be `confirmed`). Emit `value: null, confidence: "unmodeled"` and stop the chain. |
| H5 | `analysis/money_flow_engine/layer_credit.py:94-107, 124-137, 212-227, 240-250` | `/flows/junction-points`, `/flows/layers`, `/flows/flow-map-v2`, `/flows/waterfall` | 4,3 | HIGH | CONFIRMED | Node `value` is a hardcoded market size (HY $1.5T, IG $5T, repo $4.5T, CDS $10T) while `confidence`/`source` come from the *live spread series* — a literal is served as `confirmed` `FRED:BAMLH0A0HYM2` data. Put the market size in `metadata` and make `value` the measured spread, or label the node `confidence: "static_reference"`. |
| H6 | `api/routers/surfacer.py:1575, 1602, 1649-1654` | `GET /api/v1/surfacer/candidates` | 2,4 | HIGH | CONFIRMED | `score_parts.backtest` is the literal `70`/`45` (oracle) or `35` (every signal candidate); `tradability` is `80/45`/`35/65/30`. No backtest exists behind the "backtest" number. Either compute from `oracle_predictions` verdict history or rename to `prior_weight` and mark it static. |
| H7 | `api/routers/surfacer.py:1630-1631, 1689` | `GET /api/v1/surfacer/candidates` | 1,2 | HIGH | CONFIRMED | Missing `signal_data.confidence` → invented `0.30`; missing hypothesis confidence → `0.35`; both surface as `candidate.confidence` and `score_parts.confidence` (30%/35%). Return `confidence: null` and let the gate report "confidence unknown". |
| H8 | `intelligence/earnings_intel.py:397-398` | `POST /api/v1/earnings/predict/{ticker}`, `GET /api/v1/earnings/calendar` | 2 | HIGH | CONFIRMED | With no options IV row, `opt_move` defaults to `2.0` (%) and is 70% of `predicted_move`. The published expected move is then mostly a magic number. Skip the options term and mark `predicted_move_basis: "history_only"`. |
| H9 | `api/routers/flows.py:1660, 1666, 1671, 1703-1710, 1849` | `GET /api/v1/flows/sankey` | 2,1 | HIGH | CONFIRMED | Missing prices default to `0`, so `flow_pct` is reported as a real relative performance and `spy_30d` as `0.0`; link `value` is `max(0.1, abs(perf)*1000)` — a price-change scalar presented as capital flow volume. Omit nodes/links with no price and rename `value` to `magnitude_index`. |
| H10 | `api/routers/dad.py:1022-1032` (via `1807` `_range_to_days`) | `GET /api/v1/dad/ticker/{t}/chart?range=…` | 3,2 | HIGH | CONFIRMED | `metrics` always uses the keys `high_52w`, `low_52w`, `pct_from_52w_high`, `return_1y_pct` but is computed over the *requested* window, so `range=1M` labels a 31-day high as the 52-week high and a 31-day return as 1Y. Key the metrics by the actual window (`high_window`, `return_window_pct`, `window_days`). |
| H11 | `api/routers/flows.py:1264-1286`, `1581-1586` | `GET /api/v1/flows/sectors/{sector}/detail`, `/flows/sector/{sector}` | 4,3 | HIGH | CONFIRMED | The V1 static maps (`_ACTIVIST_HOLDERS`, `_SUPPLY_CHAIN`, `_REGULATOR_THREATS`, `_GLP1_PRESSURE`) emit edges with `"confidence": "confirmed"` and invented `strength` (0.8/0.6/0.5/0.55), and `_LINEAGE_CHAINS` is returned as `connections.lineage` — the payload never tells the client any of it is curated/static. Add `"provenance": "curated_static"`, an `as_of` for the curation date, and drop the numeric `strength` (or mark it editorial). |
| H12 | `api/routers/explain.py:49-60, 657-667, 686-696` | `GET /api/v1/actors/{actor_id}/explain` | 2 | HIGH | CONFIRMED | `evidence[].strength` = hardcoded `TYPE_WEIGHTS` × recency decay, then rendered as `"Most probable driver: insider trade (55%)"` — a fixed prior presented as a probability. Rename to `rank_score`, stop percent-formatting it, and say "ranked by source-type prior". |
| M1 | `api/routers/flows.py:2522-2534` | `GET /api/v1/flows/orthogonality` | 2 | MED | CONFIRMED | `correlation_matrix` values are `1 - |z_i - z_j| / (|z_i|+|z_j|)` — a z-score similarity proxy, not a correlation. Rename to `zscore_similarity` or compute real pairwise correlation from the series history. |
| M2 | `api/routers/flows.py:743, 786-804, 929` | `/flows/sectors/{sector}/detail` | 2,1 | MED | CONFIRMED | `dark_pool_signal` initialises to `"neutral"` and stays there when `dark_pool_weekly` is missing/empty/erroring — indistinguishable from an observed neutral reading. Use `None`/`"unavailable"`. |
| M3 | `api/routers/contagion.py:528-536, 587-594` | `GET /api/v1/sectors/{sector}/contagion-matrix` | 1,2 | MED | CONFIRMED | A failed simulation is cached for 1h as `{"ranked_impact": []}`, and every unmatched ticker cell becomes `margin_impact_pct: 0.0`, `severity: "none"` — a failure renders as "no impact". Use `null` + `severity: "unmodeled"` and don't cache failures. |
| M4 | `api/routers/geo.py:62-92` | `/geo/flows`, `/geo/actors`, `/geo/signals/density` | 2,4 | MED | CONFIRMED | Any jurisdiction code (or a name containing "fed"/"treasury") resolves to a hardcoded financial-centre lat/lng (all US entities → 40.7128/-74.0060) with no precision flag. Add `geo_precision: "exact"\|"country_proxy"` per point. |
| M5 | `api/routers/geo.py:155-171` | `GET /api/v1/geo/flows` | 4,3 | MED | CONFIRMED | Every `dollar_flows` row is drawn as an arc terminating at New York (`us_default`) with `to_name` falling back to `"US Markets"`; the destination was never observed. Omit the arc, or emit a single-point marker. |
| M6 | `api/routers/supply_chain_helpers.py:354-389` | `GET /api/v1/actors/{actor_id}/supply_chain` | 1,4 | MED | CONFIRMED | Fallback graph invents `relationship: "customer"` edges between *subsector peers* (i.e. competitors) — only `confidence: "inferred"` + `citation` disclose it. Label the relationship `"sector_peer"` or drop the downstream fallback. |
| M7 | `api/routers/supply_chain_helpers.py:321, 335, 409-410, 450-476` | same | 2 | MED | CONFIRMED | `annual_usd_total` and `upstream/downstream_annual_usd_total` are `0.0` when no dollar data exists (fallback path always; DB path whenever `annual_usd` is NULL) — a $0 flow presented as observed. Use `null`. |
| M8 | `api/routers/physics.py:386-401, 437-447` + `physics/news_energy.py:511-527` | `GET /api/v1/physics/dashboard`, `/physics/news-energy` | 1,3 | MED | CONFIRMED | `_empty_result` returns `total_news_energy: 0.0`, `coherence: 0.0`, `regime_signal.equilibrium: True`, and the dashboard turns that into `energy_conservation.state = "equilibrium"` and "Overall market state: equilibrium." when there is simply no news data. Emit `null` energies and `state: "unknown"`. |
| M9 | `api/routers/capital_flow.py:609-617` | `GET /api/v1/actors/{id}/capital_flow` | 2,3 | MED | CONFIRMED | `buyback_yield` silently switches denominator from market cap to revenue when market cap is missing — two different metrics under one field name. Emit `buyback_yield: null` plus a separate `buybacks_to_revenue`. |
| M10 | `api/routers/capital_flow.py:587-594` | same | 2 | MED | CONFIRMED | `dividend_payout` divides by `ni_proxy = rev - cogs - opex - tax` and `fcf_conversion` by a similar proxy; the response names them as the canonical ratios with no "proxy" marker. Suffix the field names (`dividend_payout_ni_proxy`) or add `ratio_basis`. |
| M11 | `api/routers/capital_flow.py:471-500, 1124-1131` | same | 3 | MED | CONFIRMED | When FX conversion fails, the local-currency amount stays in `amount_usd` (only `amount_local == amount_usd` hints at it) while `fx_conversions_applied` counts successes only — a converted row and a failed row are indistinguishable. Add an explicit `fx_status: "converted"\|"unconverted"` per flow. |
| M12 | `api/routers/feed.py:281-299` | `GET /api/v1/feed/live?entities=…` | 3 | MED | CONFIRMED | If an entity filter returns <10 rows, unrelated global signals are appended with no marker, so the client renders them as entity matches. Tag each item `match: "entity"\|"backfill"`. |
| M13 | `api/routers/explain.py:719-749, 780-784` | `/api/v1/actors/{id}/explain` | 3 | MED | CONFIRMED | `provenance.sources_checked` is incremented before each collector runs, including collectors that immediately return `[]` because the table doesn't exist — the field overstates coverage the docstring claims it reports. Count only lenses whose table was present and queried, and list the skipped ones. |
| M14 | `api/routers/surfacer.py:2200-2243` | `GET /api/v1/surfacer/candidates` | 3 | MED | CONFIRMED | A DB exception is logged and swallowed; the response is a normal 200 with `candidates: []`, `generated_at: now()` and a brief reading "Stand down / Nothing cleared the front page". Add `status: "degraded"` + `error` (or 503) so an outage is not reported as a clean market read. |
| M15 | `api/routers/surfacer.py:723-727, 1678-1682, 1774` | same | 3 | MED | CONFIRMED | Regime defaults to the string `"NEUTRAL"` when unknown; it is echoed in `candidate.calibration.regime` and used to pick regime-conditional Brier history, so a candidate can be "calibrated" against the wrong regime. Use `null` and skip regime-conditional lookup. |
| M16 | `api/routers/surfacer.py:423, 433, 444, 452` | same | 2 | MED | CONFIRMED | Every evidence item with no weight/confidence gets `weight: 0.5`. Use `null`. |
| M17 | `api/routers/dad.py:678-715, 645-654` | `GET /api/v1/dad/ticker/{t}/finviz`, `/gold` | 2,4 | MED | CONFIRMED | Text Finviz fields (Sector, Industry) are written into `raw_series.value` as `0.0` with `pull_status='SUCCESS'`, then served back as `numeric_value: 0.0`. Skip non-numeric fields or store NULL and emit `numeric_value: null`. |
| M18 | `api/routers/dad.py:1234-1360` | `/dad/ticker/{t}/gold`, `/gold/stream` | 2 | MED | CONFIRMED | `decision_stack.score`/`stance` come from hand-picked point values (`gold*0.35`, `+15/+8/-4`, `+5/+4/+3`, `-min(12, n*3)` …) presented as a 0-100 decision score. Keep the gate cards but drop the aggregate number, or label it `heuristic_score` with the weights inlined. |
| M19 | `intelligence/sentiment_scorer.py:779-799` | `GET /api/v1/briefing/sentiment` | 2 | MED | CONFIRMED | With no usable components, `score` is `0.0` and `label` `"NEUTRAL"` (only the `context` sentence says "Insufficient data"). Emit `score: null, label: "UNAVAILABLE"`. |
| M20 | `intelligence/earnings_intel.py:361, 465` | `/earnings/predict/{t}`, `/earnings/scorecard` | 2 | MED | CONFIRMED | No earnings history → `beat_rate` treated as `0.5` inside the score; no scored predictions → `overall_pct: 0.0` presented as 0% accuracy. Use `null` and skip the term. |
| M21 | `analysis/money_flow_engine/layer_institutional.py:26-36, 88-98, 106-119` | `/flows/junction-points`, `/flows/layers` | 4 | MED | CONFIRMED | SWF AUM table ("as of 2024-Q4 estimates", $5.3T total) and a $200B pension-rebalancing magnitude are hardcoded node values; they carry `confidence:"estimated"` at node level but roll un-flagged into `layer.aggregate_value` and `global_liquidity_total`. Exclude estimated nodes from aggregates or return `aggregate_value_estimated_share`. |
| M22 | `analysis/money_flow_engine/layer_sovereign.py:176-205` | `/flows/junction-points` | 3,4 | MED | CONFIRMED | `tariff_impact` sets `value = wiki_tariff` (a Wikipedia-derived index) but declares `unit: "USD"`; with no data it falls back to a literal `80_000_000_000.0`. Fix the unit and return `null` for the no-data case. |
| M23 | `analysis/flow_aggregator.py:710-750` | `GET /api/v1/flows/aggregated` | 2 | MED | CONFIRMED | Pairwise sector rotation is an outer product of net in/outflow shares; no pairwise flow is observed, yet `signals[].label` reads "Money leaving X, entering Y: $12,345,678". Keep `estimated_usd` but make the label say "implied by simultaneous net flows". |
| M24 | `timeseries/timesfm_forecaster.py:247-257` (also `339-362`) | `POST /api/v1/forecasts/generate`, `/forecasts/batch` | 2,3 | MED | CONFIRMED | When the model returns no quantiles, `lower_bound`/`upper_bound`/`forecast_std` are silently replaced by `±1.96 × trailing-30-obs stdev` and still labelled with the TimesFM `model_version`. Add `interval_source: "model_quantiles"\|"empirical_sigma"`. |
| M25 | `api/routers/flows.py:1494-1504, 1522-1528, 1544-1551` | `/flows/sectors/{sector}/detail` | 2 | MED | CONFIRMED | Convergence edge `strength` is the literal `0.65`; SEC-filing edges `0.9` with `confidence:"confirmed"`; chokepoint edges `0.6`. These are UI weights, not measurements. Omit `strength` or carry the source score through. |
| M26 | `api/routers/regime.py:266-272` | `GET /api/v1/regime/all-active` | 2 | MED | CONFIRMED | `float(row[1]) if row[1] else 0.0` turns a NULL `state_confidence`/`transition_probability` into an observed `0.0`, and the list is then sorted by it. Use `null`. |
| L1 | `api/routers/regime.py:187-201` | `GET /api/v1/regime/current` | 3 | LOW | CONFIRMED | The no-data response sets `as_of = now()` (and `model_version: "none"`), implying a reading was taken now. Use `as_of: null`. |
| L2 | `api/routers/celestial.py:85, 121, 130` | `GET /api/v1/signals/celestial` | 3 | LOW | CONFIRMED | `as_of` is always `date.today()`, including the empty and error payloads; per-feature `obs_date` is the only real timestamp. Set `as_of` to the max `obs_date`. |
| L3 | `api/routers/feed.py:156, 213` | `/feed/rss`, `/feed/atom` | 3 | LOW | CONFIRMED | Rows with a NULL `created_at` get `pubDate`/`updated` = now, so old items surface as brand new in readers. Skip the item or use a sentinel. |
| L4 | `api/routers/dad.py:357-368` | `/dad/ticker/{t}/gold`, `/gold/stream` | 3 | LOW | CONFIRMED | `tradingview.symbol` is always `NASDAQ:<ticker>` regardless of the real venue (NYSE names are mislabelled). Resolve the exchange or return only the search URL. |
| L5 | `intelligence/sentiment_scorer.py:694-713` | `GET /api/v1/briefing/sentiment` | 2 | LOW | CONFIRMED | `fear_greed` is a linear rescale of VIX alone presented as a 0-100 Fear/Greed reading (the word "proxy" appears only inside `detail`). Rename the component `vix_fear_proxy`. |
| L6 | `api/routers/physics.py:326-335` | `GET /api/v1/physics/dashboard` | 2 | LOW | CONFIRMED | `te_val = (ke_val or 0) + (pe_val or 0)` with `status: "ok"` — a missing component is silently counted as zero energy. Require both, else `total_energy: null, status: "partial"`. |
| L7 | `api/routers/dad.py:796-824` | `/dad/ticker/{t}/gold` | 2 | LOW | CONFIRMED | `gold.score = evidence_score*2.5 + file_count*8 + sheet_count*2 + min(mentions,30)` — arbitrary multipliers rendered as a 0-100 "workbook conviction" score. Publish the raw counts and keep the verdict qualitative. |

---

## Finding details (snippets)

**H1** `intelligence/sector_health.py:478-486` → `GET /api/v1/sectors/{sector}/health` (router `api/routers/sector_health.py:46` is a pure passthrough + 600s cache)
```python
    except Exception as exc:
        log.warning("sector_health: computation failed for {s}: {e}", ...)
        components = {k: NEUTRAL for k in WEIGHTS}
        score = round(100.0 * NEUTRAL, 2)      # -> 50.0
        trend = "stable"
```
Individual sub-scores also fall back to `NEUTRAL` per component (module docstring lines 21-22), so a sector with zero underlying data still returns `score: 50.0`, a narrative naming a "strongest lever", and `as_of: now()`.

**H2/H3** `analysis/money_flow.py:976-991` → `GET /api/v1/flows/money-map`
```python
                    flows.append({
                        "from": "fed", "to": "equities",
                        "volume": abs_vol * 0.5,
                        "change": _safe_pct_change(abs_vol, abs_vol * 0.9),
                        "label": f"Fed liquidity {'injection' ...}",
                        "confidence": "confirmed",
                    })
```
`_safe_pct_change` (line 212) is `(current-previous)/abs(previous)` → constant `0.111111`.
Related, lower-severity: line 1037-1048 `est_monthly_flow = bs_usd * 0.01` ("1% monthly churn") — at least tagged `estimated`.

**H4** `api/routers/flows.py:2410-2436` → `GET /api/v1/flows/waterfall`
```python
        total_flow = sum(e.value_usd for e in relevant_edges)
        if total_flow <= 0 and current_value > 0:
            # Estimate attenuation
            total_flow = current_value * 0.3  # default 30% pass-through
        ...
        edge_conf = best_edge.confidence if best_edge else "estimated"
        chain.append({... "value": round(total_flow, 2), "confidence": edge_conf})
```

**H5** `analysis/money_flow_engine/layer_credit.py:94-107`
```python
    # HY bond market ~$1.5T. Spread is the stress indicator.
    spread_raw = value
    _HY_MARKET_SIZE = 1_500_000_000_000
    return FlowNode(id="hy_spread", ..., value=_HY_MARKET_SIZE,
                    confidence=confidence,             # from the live OAS series
                    unit="USD", source=f"FRED:{_SERIES_HY_SPREAD}", ...)
```
Same shape at 124-137 (IG $5T), 212-227 (repo $4.5T), 240-250 (CDS $10T). Also 146-177: money-market node falls back to `_EST_MONEY_MARKET_FUNDS = 6_000_000_000_000` (this one is honestly tagged `confidence:"estimated"`, `source:"estimate"`).

**H6/H7** `api/routers/surfacer.py`
```python
1575:    backtest = 70 if str(data.get("verdict") or "").lower() in {"hit", "partial"} else 45
1602:            "tradability": 80 if ticker else 45,
1631:    confidence = _unit(raw_confidence if confidence_known else 0.30)
1652:            "backtest": 35,
1689:    confidence = _unit(raw_confidence if raw_confidence is not None else 0.35)
```

**H8** `intelligence/earnings_intel.py:396-398`
```python
        hist_avg = abs(signals.get("historical_surprise_avg", 0))
        opt_move = signals.get("expected_move_options", 2.0)
        predicted_move = (hist_avg * 0.3 + opt_move * 0.7) if opt_move else hist_avg
```

**H9** `api/routers/flows.py:1660-1712`
```python
1660:    spy_change = price_changes.get("spy_full", 0)
1666:        sector_perf = price_changes.get(etf_key, 0) - spy_change
1671:        flow_value = max(0.1, abs(sector_perf) * 1000)
1704:                actor_flow = max(0.05, abs(actor_perf) * 1000) * actor.get("weight", 0.1)
```

**H10** `api/routers/dad.py:1022-1032`
```python
        metrics = {
            "latest_price": latest, "first_price": first,
            "return_1y_pct": ((latest - first) / first * 100) if first else None,
            "high_52w": high, "low_52w": low,
            "pct_from_52w_high": ((latest - high) / high * 100) if high else None,
```
`prices` covers `days = _range_to_days(range_name)` (`dad.py:203-214`, 31…3650 days) supplied by `GET /dad/ticker/{t}/chart?range=`.

**H11** `api/routers/flows.py:1264-1286` and `1581-1586`
```python
    for slug, info in _ACTIVIST_HOLDERS.items():
        for tk in info["targets"]:
            if tk in ticker_set:
                add_edge(slug, tk, info["kind"], 0.8, info["evidence"], "confirmed")
    ...
    payload = {"nodes": nodes, "edges": edges, "clusters": clusters,
               "lineage": _LINEAGE_CHAINS.get(sector_name, [])}
```
The module comment at 959-960 documents the intent ("V1 is hardcoded"), but nothing in the HTTP payload distinguishes these edges from the DB-derived ones — they are the *only* edges labelled `"confidence": "confirmed"` alongside genuine 13F-derived ones.

**H12** `api/routers/explain.py:657-667, 686-696`
```python
def _score(evidence, pivot, window_days) -> float:
    base = TYPE_WEIGHTS.get(evidence.get("type", ""), 0.2)
    ...
    return round(base * recency, 4)
...
        strength = int(round(ev.get("strength", 0) * 100))
        pieces.append(f"{label} ({strength}%)")
    return f"{move_part} Most probable driver: {lead}{reinforcers}."
```

**M1** `api/routers/flows.py:2526-2529`
```python
            # Simple correlation proxy from z-score similarity
            corr = 1.0 - abs(zscores[i] - zscores[j]) / (abs(zscores[i]) + abs(zscores[j]) + 1e-9)
            correlation_matrix[key] = round(float(corr), 3)
```

**M3** `api/routers/contagion.py:528-536, 587`
```python
    except Exception as exc:
        log.warning("contagion matrix: sim failed for scenario {s}: {e}", ...)
        result = {"ranked_impact": []}
    _scenario_sim_cache.set(sid, result)          # failure cached for 1h
...
            margin = float(match["margin_impact_pct"]) if match else 0.0
```

**M6** `api/routers/supply_chain_helpers.py:374-389`
```python
                edges.append({
                    "source": ticker, "target": pt,
                    "relationship": "customer",
                    "annual_usd": None, ...
                    "confidence": "inferred",
                    "citation": "sector_map subsector overlap",
                })
```
`pt` is a *peer in the same subsector*, i.e. a competitor, not a customer.

**M8** `physics/news_energy.py:511-527` (reached from `api/routers/physics.py:339, 386-401`)
```python
        return {..., "total_news_energy": 0.0,
                "coherence": {"coherence": 0.0, "dominant_direction": "neutral", ...},
                "regime_signal": {"equilibrium": True, "violations": 0, ...},
                "summary": reason}
```
`physics.py:393-401` then computes `eq_state = "equilibrium"` and appends `"Overall market state: equilibrium."`.

**M12** `api/routers/feed.py:281-299`
```python
        # If entity filter returned few results, backfill with recent global signals
        if entities and len(rows) < 10:
            ...
            rows = list(rows) + [r for r in backfill if r[0] not in existing_ids]
```

**M14** `api/routers/surfacer.py:2210-2211, 2237-2243`
```python
    except Exception as exc:
        log.warning("surfacer candidate query unavailable: {e}", e=str(exc))
    ...
    return {"generated_at": datetime.now(timezone.utc).isoformat(),
            "candidates": selected, "thesis": thesis, "meta": meta, "brief": brief}
```

**M17** `api/routers/dad.py:678-682`
```python
            if raw_value is None or parsed is None:
                continue
            numeric_value = parsed if isinstance(parsed, (int, float)) else 0.0
```
…then `INSERT INTO raw_series (... value ...) ... 'SUCCESS'` at 693-714, read back as `numeric_value` at 651.

**M21** `analysis/money_flow_engine/layer_institutional.py:29-36, 106-119`
```python
_SWF_ESTIMATES = (
    {"id": "norway_gpfg", "label": "Norway GPFG", "aum_usd": 1_700_000_000_000}, ...)
...
    return FlowNode(id="sovereign_wealth", ..., value=_SWF_TOTAL_USD,
                    confidence="estimated", source="estimated:SWF_AUM", ...)
```
Peer constants in the same package: `layer_corporate.py:31,36,44,48,49`; `layer_crypto.py:37-40,70`; `layer_retail.py:32`; `layer_sovereign.py:31,34`.

**M24** `timeseries/timesfm_forecaster.py:247-257`
```python
        else:
            # Fallback: estimate intervals from point forecast volatility
            series_std = float(np.nanstd(series[-min(30, len(series)):]))
            lower = [p - 1.96 * series_std for p in point]
            upper = [p + 1.96 * series_std for p in point]
        forecast_std = [(u - l) / 3.92 for u, l in zip(upper, lower)]
```

---

## Ruled-out constants (considered, not reported)

- **Threshold/classification constants** — `flows.py` dark-pool 0.40/0.55 and PCR 0.7/1.3 cutoffs; `contagion.py:469-478` severity buckets; `explain.py` recency decay shape; `surfacer.py` `_freshness` 24/96h bands and `_canonical_horizon_days`; `sector_health.py` `WEIGHTS` and `TREND_EPS`; `physics.py` Hurst 0.45/0.55 bands; `capital_flow.py` `_trichotomy` 50 bp tolerance. These classify measured values rather than substituting for them.
- **Curated, self-declaring catalogs** — `contagion.py:45-136` `SCENARIO_CATALOG` (endpoint is explicitly "curated preset scenario catalog"; the shock magnitudes are user-selectable *inputs*, not observations); `dad.py:83-126` `DAD_STAT_CATALOG` (UI prompts); `physics/conventions` (day-count/annualization conventions); `explain.py` `_IDENT_RE` / table-name whitelists.
- **Honest empty/degraded payloads (good examples to copy)** — `flows.py:478` `{"sectors": {}, "stale": True, "unavailable": True}`; `flows.py:337-393` age-gated snapshot seeding with `snapshot_age_s`; `capital_flow.py:979-1000` `_fallback_payload` (all nulls, `label:"pending"`, `provenance.source:"fallback"`); `divergence.py:82-93` `provenance.source:"fallback"`, `reason:"table_missing"`; `ollama/celestial_briefing.py:466-493` explicit `stale` + `note`; `regime.py:118-143` `_regime_data_as_of` deliberately reports unknown input age as unknown; `forecasts.py` returns 503/404 instead of a synthetic forecast; `briefing.py:175` `accuracy: None` when nothing is scored.
- **Presentation-only numbers** — graph node `size` (`flows.py:1177, 1212, 1222, 1235, 1245`), `_downsample_points`, response `limit`/`offset` echoes, XML escaping, and all UI copy strings.
- **Enum labels / display maps** — `geo.py FINANCIAL_CENTERS` *as a lookup table* is legitimate reference data; the finding (M4/M5) is about serving it as an entity's observed position, not about the table itself. Same for `FINVIZ_FIELD_MAP` and `CELESTIAL_PATTERNS`.
- **Tests** — nothing under `tests/` was inspected or reported.

---

## Files reviewed (full read)

| File | Lines |
|---|---|
| `api/routers/flows.py` | 2803 |
| `api/routers/capital_flow.py` | 1135 |
| `api/routers/contagion.py` | 605 |
| `api/routers/divergence.py` | 192 |
| `api/routers/sector_health.py` | 59 |
| `api/routers/geo.py` | 257 |
| `api/routers/physics.py` | 452 |
| `api/routers/supply_chain.py` | 105 |
| `api/routers/supply_chain_helpers.py` | 597 |
| `api/routers/regime.py` | 502 |
| `api/routers/forecasts.py` | 230 |
| `api/routers/earnings.py` | 134 |
| `api/routers/celestial.py` | 189 |
| `api/routers/briefing.py` | 211 |
| `api/routers/dad.py` | 2265 |
| `api/routers/feed.py` | 331 |
| `api/routers/explain.py` | 834 |
| `api/routers/surfacer.py` | 2243 |
| **Total** | **13,144** |

Helper modules followed (targeted reads at the call sites that produce response values, not full reads): `intelligence/sector_health.py`, `intelligence/earnings_intel.py`, `intelligence/sentiment_scorer.py`, `analysis/money_flow.py`, `analysis/flow_aggregator.py`, `analysis/money_flow_engine/{layer_credit,layer_institutional,layer_sovereign}.py`, `physics/news_energy.py`, `ollama/celestial_briefing.py`, `timeseries/timesfm_forecaster.py`.

## Files NOT fully reviewed

None of the 18 assigned routers were skipped. Helper modules were read only around the code paths that feed HTTP responses, so the following remain **partially** reviewed and could hold further findings:

- `analysis/money_flow.py` (1770) — read the central-bank table, the flow-edge builder, and `_safe_pct_change`; sectors/actor drill-down sections skimmed by grep.
- `analysis/flow_aggregator.py` (1146) — read the rotation matrix and confidence weighting; `aggregate_by_sector`/`by_actor_tier`/`compute_flow_momentum` not line-read.
- `analysis/money_flow_engine/*` (3391 across 12 files) — read `layer_credit`, `layer_institutional`, `layer_sovereign` and grepped the rest; `layer_corporate`, `layer_crypto`, `layer_retail`, `layer_market`, `layer_monetary`, `flow_inference` contain the same hardcoded-magnitude pattern (constants listed under M21) and deserve the same treatment.
- `intelligence/chain_contagion.py` (804) — not read; `DEFAULT_PASS_THROUGH` and the margin-impact model behind `/contagion/simulate` are unaudited.
- `intelligence/earnings_intel.py` (partial), `intelligence/sentiment_scorer.py` (partial), `intelligence/cds_tracker.py`, `intelligence/lever_pullers.py`, `intelligence/trust_scorer.py`, `intelligence/audio_briefing.py`, `intelligence/image_gen.py`, `analysis/research_agent.py`, `analysis/sector_map.py` — reached from the audited routers but out of the assigned scope.
