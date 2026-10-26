# Audit D — fabricated data in chat / system / astrogrid / canvas / misc routers

Repo: `C:\Users\owner\dev\GRID-wt-fake-data-audit`
Branch `audit/fake-data-cleanup-20260917` @ `fe6765bc` (= origin/main, deployed as `grid-api` on grid-svr)
Scope: production-path fabricated data reaching an HTTP/SSE response.
**Read-only audit. I modified no repo file.**

> ⚠️ **Working tree was clean at the start of this audit but is not now.** `pwa/src/views/TrendTracker.jsx` was modified at `2026-09-17 15:11:16 -0700`, during this session, by something outside this audit (+83 / −308). I did not make this change and I have deliberately **not** reverted it — it is uncommitted work and looks intentional. The change is a *remediation* of exactly this audit's subject matter: it deletes `generatePlaceholderData()` (hardcoded fake trends: "Tech Sector Death Cross", "Regime Shift: GROWTH -> FRAGILE", with invented strengths/confidences) and removes a chart that drew a 12-month trajectory from `Math.random()` noise and presented it as measured history, replacing both with an honest `ErrorState`. Confirm whoever made it intended it before committing or discarding.

Categories:
1. **FABRICATED FALLBACK** — on failure/empty, invented rows or numbers instead of an honest empty state
2. **PLACEHOLDER METRIC** — arbitrary constant/approximation presented as a measured metric
3. **MISLEADING PROVENANCE** — `source` / `as_of` / `freshness` / `confidence`-label claims that don't match what produced the value
4. **HARDCODED VALUES PRESENTED AS OBSERVATIONS**

---

## Findings table (sorted by severity)

| # | file:line | Endpoint(s) | Cat | Sev | Conf |
|---|---|---|---|---|---|
| H1 | `api/routers/canvas.py:901` (+1012, 1067, 1089, 1311, 1467, 1533) | `/api/v1/canvas/graph`, `/canvas/node/actor/{id}`, `/canvas/expand/{type}/{id}` | 2 | HIGH | CONFIRMED |
| H2 | `api/routers/system.py:1643,1667-1675,1683-1686` | `/api/v1/system/architecture` | 4 | HIGH | CONFIRMED |
| H3 | `api/routers/system.py:1622-1633` | `/api/v1/system/architecture` | 1 | HIGH | CONFIRMED |
| H4 | `api/routers/astrogrid_helpers.py:1039,1043,1047,1088` | `/api/v1/astrogrid/snapshot`, `/astrogrid/overview` | 2 | HIGH | CONFIRMED |
| H5 | `api/routers/astrogrid_helpers.py:1752-1762` | `/api/v1/astrogrid/scorecard` | 2 | HIGH | CONFIRMED |
| H6 | `api/routers/chat.py:1785,1845,1959` | `/api/v1/chat/ask` | 2 | HIGH | CONFIRMED |
| H7 | `api/routers/canvas.py:422` | `/api/v1/canvas/graph`, `/canvas/expand/{type}/{id}` | 2 | HIGH | CONFIRMED |
| H8 | `api/routers/canvas.py:1892` (+1932, 1971, 2011, 2048, 2086, 2125) | `/api/v1/canvas/dots` | 2 | HIGH | CONFIRMED |
| H9 | `knowledge/tree.py:221-252` (esp. 230) | `/api/v1/knowledge`, `/knowledge/{id}`; written by `/api/v1/ollama/ask` | 2 | HIGH | CONFIRMED |
| H10 | `api/routers/astrogrid_predictions.py:101-102,115-117` | `/api/v1/astrogrid/guru/ask` (**unauthenticated**), `/astrogrid/predictions` | 1 | HIGH | CONFIRMED |
| H11 | `oracle/publish.py:162-164` | written by `/astrogrid/predictions`, `/guru/ask`; read via `/astrogrid/scorecard`, oracle endpoints | 2 | HIGH | CONFIRMED |
| H12 | `api/routers/astrogrid_celestial.py:584-593` | `/api/v1/astrogrid/nakshatra` | 2 | HIGH | CONFIRMED |
| H13 | `store/astrogrid.py:2378` | `/astrogrid/predictions/score`, `/learning-loop/run` → `/predictions/scoreboard`, `/backtest/summary` | 1 | HIGH | CONFIRMED |
| H14 | `store/astrogrid.py:1793` + `:2777` | `/astrogrid/backtest/run`, `/backtest/summary`, `/review/latest`, `/learning-loop/run` | 2 | HIGH | CONFIRMED |
| M1 | `api/routers/astrogrid_helpers.py:1121-1122 + 1149-1151` | `/api/v1/astrogrid/snapshot` | 3 | MED | CONFIRMED |
| M2 | `api/routers/astrogrid_helpers.py:657-662` | `/api/v1/astrogrid/predictions`, `/astrogrid/guru/ask` | 2 | MED | CONFIRMED |
| M3 | `api/routers/astrogrid_helpers.py:1704-1713` | `/api/v1/astrogrid/scorecard` | 2 | MED | CONFIRMED |
| M4 | `api/routers/astrogrid_helpers.py:1716-1726` | `/api/v1/astrogrid/scorecard` | 2 | MED | CONFIRMED |
| M5 | `api/routers/astrogrid_helpers.py:1921,1946-1953,1962-1967` | `/api/v1/astrogrid/scorecard` | 1 | MED | CONFIRMED |
| M6 | `api/routers/system.py:984-991` | `/api/v1/system/hermes-status`, `/system/hermes-history` | 4 | MED | CONFIRMED |
| M7 | `api/routers/system.py:595-612` | `/api/v1/system/pipeline-health` | 2 | MED | CONFIRMED |
| M8 | `api/routers/system.py:619` (+`api/schemas/system.py:126`) | `/api/v1/system/pipeline-health` | 2 | MED | CONFIRMED |
| M9 | `api/routers/system.py:550-561, 672-687, 1485-1496` | `/api/v1/system/pipeline-health`, `/system/architecture` | 2 | MED | CONFIRMED |
| M10 | `api/routers/system.py:1562-1564, 1575-1577` | `/api/v1/system/architecture` | 2 | MED | CONFIRMED |
| M11 | `api/routers/canvas.py:930` | `/api/v1/canvas/graph?center=all` | 3 | MED | CONFIRMED |
| M12 | `api/routers/canvas.py:985` | `/api/v1/canvas/graph?center=all` | 2 | MED | CONFIRMED |
| M13 | `api/routers/canvas.py:509,530` (+778-782) | `/api/v1/canvas/graph`, `/canvas/expand/{type}/{id}` | 2 | MED | CONFIRMED |
| M14 | `api/routers/canvas_expand.py:752` | `/api/v1/canvas/boards/{id}/expand/{node_id}`, `/canvas/boards/{id}` | 3 | MED | CONFIRMED |
| M15 | `api/routers/canvas_llm.py:231` | `/api/v1/canvas/explain` | 3 | MED | CONFIRMED |
| M16 | `api/routers/canvas_investigate.py:440` | `/api/v1/canvas/investigate` | 3 | MED | CONFIRMED |
| M17 | `api/routers/canvas.py:2143` (+1914, 1953) | `/api/v1/canvas/dots` | 3 | MED | CONFIRMED |
| M18 | `api/routers/canvas_expand.py:840` (+191-192) | `/api/v1/canvas/boards/{id}/expand/{node_id}` | 2 | MED | CONFIRMED |
| M19 | `api/routers/canvas_investigate.py:359,389` | `/api/v1/canvas/investigate`, `/canvas/boards/{id}` | 2 | MED | CONFIRMED |
| M20 | `analysis/viz_intelligence.py:556` | `/api/v1/viz/weights` | 1 | MED | CONFIRMED |
| M21 | `api/routers/mcp_export.py:284` | `/api/v1/mcp/data-freshness` | 3 | MED | CONFIRMED |
| M22 | `api/routers/mcp_export.py:82` | `/api/v1/mcp/trust-score` | 3 | MED | CONFIRMED |
| M23 | `api/routers/ollama.py:110` | `/api/v1/ollama/status` | 3 | MED | CONFIRMED |
| M24 | `api/routers/workflows.py:92` | `POST /api/v1/workflows/{name}/run` | 3 | MED | CONFIRMED |
| M25 | `a2a/agent_card.py:131,145,177` | `GET /.well-known/agent.json` | 4 | MED | CONFIRMED |
| M26 | `api/routers/a2a.py:187` (`a2a/server.py:33`) | `/.well-known/agent.json`, `/a2a/tasks*` | 3 | MED | CONFIRMED |
| M27 | `api/routers/astrogrid_celestial.py:151,160-161` | `/api/v1/astrogrid/correlations` | 3 | MED | CONFIRMED |
| M28 | `api/routers/astrogrid_celestial.py:80-81,98` | `/api/v1/astrogrid/ephemeris` | 3 | MED | CONFIRMED |
| M29 | `api/routers/astrogrid_celestial.py:656-657` | `/api/v1/astrogrid/solar/activity` | 3 | MED | CONFIRMED |
| M30 | `api/routers/astrogrid_core.py:206` | `/api/v1/astrogrid/scorecard` | 3 | MED | CONFIRMED |
| M31 | `oracle/publish.py:86-92` | `/astrogrid/predictions`, `/guru/ask` → oracle read endpoints | 1 | MED | CONFIRMED |
| M32 | `oracle/publish.py:160` | same write path as M31 | 4 | MED | CONFIRMED |
| M33 | `store/astrogrid.py:1091-1097`, `:1346` | `/astrogrid/review/generate`, `/review/latest`, `/weights/proposals`, `/learning-loop/run` | 2 | MED | CONFIRMED |
| M34 | `store/astrogrid.py:1391-1394` | `/api/v1/astrogrid/learning-loop/run`, `/backtest/run` | 3 | MED | CONFIRMED |
| M35 | `oracle/scoreboard.py:25-28` (also `store/astrogrid.py:1636,1655`) | `/astrogrid/scorecard`, `/predictions/scoreboard` | 3 | MED | CONFIRMED |
| M36 | `api/routers/astrogrid_celestial.py:542-544,553` (`ingestion/celestial/vedic.py:72-77`) | `/astrogrid/nakshatra`, `/overview`, `/ephemeris`, `/compare`, `/briefing` | 2 | MED | CONFIRMED |
| M37 | `ingestion/celestial/planetary.py:120-126` | `/astrogrid/overview`, `/ephemeris`, `/compare`, `/briefing`, `/retrograde`, `/timeline` | 2 | MED | CONFIRMED |
| M38 | `ingestion/celestial/planetary.py:172-180` | `/astrogrid/overview`, `/briefing`, `/ephemeris`, `/compare` | 2 | MED | CONFIRMED |
| L1 | `api/routers/astrogrid_helpers.py:743` | `/api/v1/astrogrid/snapshot` | 4 | LOW | CONFIRMED |
| L2 | `api/routers/astrogrid_helpers.py:877` | `/api/v1/astrogrid/snapshot` | 2 | LOW | CONFIRMED |
| L3 | `api/routers/astrogrid_helpers.py:634-654` → `astrogrid_predictions.py:241` | `/api/v1/astrogrid/postmortems`, `/astrogrid/predictions/{id}` | 3 | LOW | CONFIRMED |
| L4 | `api/routers/system.py:1698-1728` | `/api/v1/system/architecture` | 4 | LOW | CONFIRMED |
| L5 | `api/routers/canvas.py:589,627` (+`canvas_expand.py:545,579,872`) | `/api/v1/canvas/graph`, `/canvas/boards/{id}/expand/{node_id}` | 3 | LOW | CONFIRMED |
| L6 | `api/routers/canvas_expand.py:613,649,900` | `/api/v1/canvas/boards/{id}/expand/{node_id}` | 3 | LOW | CONFIRMED |
| L7 | `api/routers/viz.py:83` | `/api/v1/viz/weights` | 2 | LOW | CONFIRMED |
| L8 | `events/consumer.py:112` | `/api/v1/events/topics` | 3 | LOW | PLAUSIBLE |
| L9 | `api/routers/astrogrid_predictions.py:118` (also 108, 113) | `/astrogrid/guru/ask`, `/astrogrid/predictions` | 4 | LOW | CONFIRMED |
| L10 | `api/routers/astrogrid_celestial.py:511-516` | `/api/v1/astrogrid/eclipses` | 3 | LOW | CONFIRMED |
| L11 | `api/routers/astrogrid_celestial.py:294-304` | `/api/v1/astrogrid/briefing` | 3 | LOW | PLAUSIBLE |
| L12 | `ingestion/celestial/chinese.py:168-175` | `/astrogrid/overview`, `/briefing`, `/ephemeris`, `/compare` | 4 | LOW | CONFIRMED |

---

## HIGH findings — detail

### H1 — Actor influence/trust: an invented `0.5` that outranks every real value
`api/routers/canvas.py:901`
```python
"influence": float(row["influence_score"] or 50) / 100.0,
"trust_score": float(row["trust_score"] or 50) / 100.0,
```
`actors.influence_score` is declared `NUMERIC DEFAULT 0.5` on a **0–1** scale (`intelligence/actors/db.py:37`; `intelligence/actors/models.py:26` `# 0-1, computed`). The router treats it as 0–100: a **real** score of 0.8 is served as `0.008`, while a **missing** score is served as `0.5`. The fabricated default is ~60x larger than the largest genuine value. `_limit_canvas_nodes` (`canvas.py:257-286`) sorts on this field, so the placeholder decides which nodes survive the `limit`. `canvas_investigate.py:193` reads the same column with no `/100`, confirming the scale contradiction inside one feature.
**Honest replacement:** return `influence_score` unchanged (0–1) and emit `null` when NULL so the client renders "no score".

### H2 — 14 subsystems hardcoded to `"status": "healthy"`
`api/routers/system.py:1667-1675` (and 1643, 1683-1686)
```python
{"id": "trust_scorer", "label": "Trust Scoring", "type": "engine", "status": "healthy"},
{"id": "cross_reference", "label": "Cross-Reference", "type": "engine", "status": "healthy"},
{"id": "sleuth", "label": "Sleuth (Investigator)", "type": "engine", "status": "healthy"},
```
14 unconditional literals. `pwa/src/views/AppArchitecture.jsx:115` and `:371` render `node.status` as a coloured health light, so these are user-facing green lights for subsystems that are never probed. They also silently suppress the `gaps` list (`system.py:1747-1755` only collects `new`/`broken`), so a dead engine can never surface as a gap.
**Honest replacement:** probe each subsystem (or emit `"status": "unknown"`) instead of asserting health.

### H3 — `/system/architecture` invents ten pullers with invented counts when the DB query returns nothing
`api/routers/system.py:1622-1633`
```python
"children": puller_nodes if puller_nodes else [
    {"id": "fred", "label": "FRED (35 series)", "type": "puller", "status": "unknown"},
    {"id": "yfinance", "label": "yFinance (50 tickers)", "type": "puller", "status": "unknown"},
```
`_get_puller_stats` returns `[]` on any exception or empty `source_catalog` (`system.py:1541-1543`). The fallback ships ten fabricated puller rows carrying invented series counts ("35 series", "50 tickers"). `stats.total_pullers` is still `len(puller_nodes)` = 0, so the same payload self-contradicts.
**Honest replacement:** return an empty `children` list and an explicit `ingestion_status: "unavailable"`.

### H4 — AstroGrid seer confidence is three hardcoded literals
`api/routers/astrogrid_helpers.py:1036-1047`
```python
if pressure >= 6 and pressure > release:
    ...
    confidence = 0.72
elif release >= pressure + 2:
    ...
    confidence = 0.69
else:
    ...
    confidence = 0.6
```
Returned at `:1088` as `seer.confidence` with a `confidence_band` whose thresholds (`>= 0.7` high, `>= 0.62` medium, else low — `:1078-1083`) are tuned exactly to map those three literals onto high/medium/low. Nothing is measured. This value then flows through `_prediction_confidence` into the persisted prediction and out to the oracle publisher (`publish_astrogrid_prediction`).
**Honest replacement:** drop `confidence`/`confidence_band`, or replace with a backtested hit rate for the pressure/release bucket.

### H5 — Scorecard `confidence` is a data-coverage heuristic dressed as forecast confidence
`api/routers/astrogrid_helpers.py:1752-1762`
```python
stale_penalty = 0.0
if latest_date and (date.today() - latest_date).days > 3:
    stale_penalty = 0.18
live_boost = 0.08 if has_live_price else 0.0
history_boost = min(history_points / 60.0, 0.35)
return round(max(0.15, min(0.92, 0.28 + history_boost + live_boost - stale_penalty)), 4)
```
Every constant (0.28, 0.18, 0.08, /60, 0.35, 0.15, 0.92) is arbitrary. Emitted per item at `:1896` as `confidence` alongside genuine prices and percent changes.
**Honest replacement:** rename to `coverage_score` and publish the inputs (`history_points`, `latest_date`, `has_live_price`) — which the payload already carries — or drop it.

### H6 — `/chat/ask` confidence is a constant, rendered to the user as a percentage
`api/routers/chat.py:1785`, `:1845`, `:1959`
```python
confidence = 0.5  # base
...
confidence = 0.75 if context_text else 0.5
...
confidence = 0.3 if context_text else 0.1
```
`pwa/src/components/ChatPanel.jsx:595` renders `confidence: {(msg.confidence * 100).toFixed(0)}%`, i.e. the user is shown "confidence: 75%" for an answer whose only input to that number was *whether the context string was non-empty*. It is not derived from the model, the firewall verdict, or the sanity warnings — note the firewall can reject the answer (`chat.py:1906` replaces `answer` with sanitized output) and `confidence` still reports 0.75.
**Honest replacement:** drop the field, or derive it from the firewall's verified-claim ratio (`fw.claim_count` / `fw.flagged_count`, already computed at `chat.py:1880`).

### H7 — Every actor-connection edge carries a fabricated `confidence: 0.7`
`api/routers/canvas.py:422`
```python
"type": "connection",
"label": r["relationship"] or "connected",
"strength": float(r["strength"] or 0.5),
"confidence": 0.7,
```
`actor_connections` is never queried for a confidence column — 0.7 is a literal on every edge. The `or 0.5` on `strength` compounds it.
**Honest replacement:** omit `confidence` for these edges and send `strength: null` when the row has none.

### H8 — `/canvas/dots` confidence values are arbitrary affine functions of row counts
`api/routers/canvas.py:1892`
```python
"confidence": min(0.9, 0.3 + cnt * 0.05),
```
Seven different hand-tuned formulas (`:1892, 1932, 1971, 2011, 2048, 2086, 2125`), no calibration anywhere in the path. A ticker with 12 insider prints reads "confidence 0.9" purely because 12 × 0.05 saturates the cap.
**Honest replacement:** drop `confidence` and return the raw counts already present in `evidence`, or rename to `heuristic_score` with the formula disclosed.

### H9 — Knowledge-base `confidence` rewards an answer for containing more `$` and `%` tokens
`knowledge/tree.py:221-252`
```python
score = 0.5  # base
word_count = len(answer.split())
if word_count > 200:
    score += 0.15
...
specifics = len(re.findall(r"\d+\.?\d*%|\d{4}-\d{2}|\$[\d,.]+", answer))
score += min(specifics * 0.05, 0.2)
```
Persisted to `knowledge_tree.confidence` by `POST /api/v1/ollama/ask` and served unlabeled on every row from `GET /api/v1/knowledge` and `/knowledge/{entry_id}`. It is a text-shape heuristic, not a model confidence — and its "specificity bonus" scores *more invented numbers* as *more confident*, which is precisely backwards for a hallucination-prone path.
**Honest replacement:** drop the field from the API, or rename to `answer_heuristic_score` and return `null` when the backend supplies no real confidence.

### H10 — The Guru invents a relative-strength trading call from zero data
`api/routers/astrogrid_predictions.py:101-102` and `:115-117`
```python
top = bullish_items[0] if bullish_items else {}
weak = bearish_items[0] if bearish_items else {}
...
selected = str(top.get("symbol") or (target_symbols[0] if target_symbols else target_group)).upper()
call = f"buy {selected}" if selected and selected != "HYBRID" else "press the best mapped leader"
setup = f"{selected} has the cleanest mapped relative-strength read in {target_group}"
```
`market_overlay_snapshot` defaults to `{}`, so `_ranked_overlay_items` returns `[]` and the endpoint still emits "buy X" plus the assertion that X "has the cleanest mapped relative-strength read" — a comparative claim manufactured with no comparison performed. **`POST /astrogrid/guru/ask` has no `Depends(require_auth)`** (`astrogrid_predictions.py:284-288`), so this is reachable unauthenticated.
**Honest replacement:** when `bullish_items`/`bearish_items` are empty, return `call: null` with `status: "no_scoreable_data"` and a reason, not a directive.

### H11 — One placeholder number published as three independent oracle metrics
`oracle/publish.py:162-164`
```python
"confidence": float(payload.get("confidence") or 0.5),
"signal_strength": float(payload.get("confidence") or 0.5),
"coherence": float(payload.get("confidence") or 0.5),
```
Three nominally independent metrics are the same value, and that value is `0.5` whenever the caller omitted `seer.confidence` (see M2). The `or 0.5` also silently rewrites a legitimate `0.0` confidence to 0.5. `oracle/scoreboard.py:162` then feeds these into the calibration report served at `/astrogrid/scorecard` (`evaluation.by_symbol[].calibration`), so placeholders are treated as stated probabilities and scored for reliability.
**Honest replacement:** store `NULL` for all three when the caller supplies no confidence, populate `signal_strength`/`coherence` from their own inputs or drop them, and exclude null-confidence rows from calibration.

### H12 — `/nakshatra` labels ~27-day returns as daily returns
`api/routers/astrogrid_celestial.py:584-588`, emitted at `:593`
```python
returns = [
    (vals[i] - vals[i - 1]) / vals[i - 1] * 100.0
    for i in range(1, len(vals))
    if vals[i - 1] > 0
]
...
"avg_daily_return_pct": round(float(np.mean(arr)), 4),
```
The SQL at `:570-580` selects only days whose `nakshatra_index` matches today's, so consecutive `vals` are roughly one sidereal month apart. The differences are ~27-day returns published under a `_daily_` name — off by more than an order of magnitude. `median_daily_return_pct` and `positive_pct` inherit the error.
**Honest replacement:** rename to `avg_return_between_nakshatra_visits_pct`, or join each matching date to its own next-trading-day close for a real daily return.

### H13 — Unmapped prediction targets are scored against SPY
`store/astrogrid.py:2378`
```python
symbols = [symbol for symbol in target_symbols if symbol in _HYBRID_LOOKUP_BY_SYMBOL] or ["SPY"]
```
A prediction about an asset outside the scoreable universe gets **SPY's** return recorded as its `realized_return`, and a hit/miss verdict derived from it. Only `raw_payload.symbols` hints at the substitution; the accuracy aggregates surfaced by `/predictions/scoreboard`, `/backtest/summary` and `/review/latest` do not.
**Honest replacement:** return `None` — `score_predictions` already has a `skipped_no_price` counter for exactly this case.

### H14 — Backtests coerce NULL and neutral outcomes to a measured 0.0
`store/astrogrid.py:1793`
```python
signed_return = float(row[12] or 0.0) * sign if sign else 0.0
signed_alpha = float(row[13] or 0.0) * sign if sign else 0.0
```
averaged at `store/astrogrid.py:2777`
```python
avg_signed_alpha_local = (
    sum(float(item["signed_alpha"]) for item in items) / total_local if total_local else 0.0
)
```
`realized_return` and `alpha_vs_benchmark` are nullable; an unscored row becomes a measured 0.0%, and every `neutral`-direction prediction is forced to exactly 0.0. Both dilute `avg_signed_return` / `avg_signed_alpha` toward zero — the numbers that decide the "best variant by group" claim and the automated weight proposals.
**Honest replacement:** skip rows with NULL return/alpha and neutral-direction rows, and report `n_used` alongside each average.

---

## MED findings — detail

### M1 — `/astrogrid/snapshot` always claims `resolved_series` as a source
`api/routers/astrogrid_helpers.py:1121-1122` then `:1149-1151`
```python
if solar_features["solar_cycle_phase"] is None:
    solar_features["solar_cycle_phase"] = round(_solar_cycle_phase(target), 6)
...
source_parts = ["analysis.ephemeris"]
if any(value is not None for value in solar_features.values()):
    source_parts.append("resolved_series")
```
`_solar_cycle_phase` is pure date arithmetic off a hardcoded Solar-Cycle-25 start (`ingestion/celestial/solar.py:59-67`) — not an observation. Because it unconditionally fills the slot *before* the `any()` check, `solar_features` is never all-`None`, so the response `source` field claims `resolved_series` even when **zero** solar values came from the DB.
**Honest replacement:** compute `source_parts` from the values actually returned by `_get_latest_resolved`, before the formula fallback fills the gap.

### M2 — Prediction confidence silently defaults to 0.5 and is published to the oracle
`api/routers/astrogrid_helpers.py:657-662`
```python
def _prediction_confidence(req: AstrogridPredictionRequest) -> float:
    try:
        value = float((req.seer or {}).get("confidence"))
    except (TypeError, ValueError):
        return 0.5
```
Used at `astrogrid_predictions.py:180`, persisted in `prediction_payload` and sent to `publish_astrogrid_prediction` as the prediction's stated confidence. A request with no seer block yields a 50%-confidence prediction of record.
**Honest replacement:** make confidence required, or persist `null` and exclude unconfident predictions from scoring.

### M3 — `change_20d_pct` can be computed from a 2-day-old baseline
`api/routers/astrogrid_helpers.py:1704-1713`
```python
cutoff = reference_date - timedelta(days=lookback_days)
for obs_date, value in reversed(history):
    if obs_date <= cutoff:
        return value
return history[0][1] if history else None
```
When no observation is old enough, it silently returns the **oldest available** point. A symbol with 3 days of history gets a `change_20d_pct` (`:1856`) that is really a 3-day change, which then drives `momentum_score`, `bias` and `trend`.
**Honest replacement:** return `None` when no observation predates the cutoff so the percent change is omitted rather than mislabeled.

### M4 — `momentum_score` treats missing changes as 0.0% (flat), not unknown
`api/routers/astrogrid_helpers.py:1721-1726`
```python
weighted = (
    (change_1d or 0.0) * 0.2
    + (change_5d or 0.0) * 0.35
    + (change_20d or 0.0) * 0.45
)
return round(math.tanh(weighted / 12.0), 4)
```
A symbol with only a 1-day change is scored as if the 5-day and 20-day moves were exactly flat — 80% of the weight is fabricated zeroes. The weights and `/12.0` divisor are themselves uncalibrated constants.
**Honest replacement:** renormalize the weights over the non-null components, and return `null` when nothing is available.

### M5 — Empty scorecard renders as a genuine neutral reading
`api/routers/astrogrid_helpers.py:1946-1953`, `:1921`, `:1962-1967`
```python
composite_score = (
    round(sum(float(item["momentum_score"]) for item in covered) / len(covered), 4)
    if covered
    else 0.0
)
```
With zero covered symbols, `composite_score` is `0.0` and `bias` becomes `_momentum_bias(0.0)` = `"wait"` — indistinguishable from a genuinely balanced market. `crypto_score`/`macro_score` default to `0.0` the same way when a group is absent.
**Honest replacement:** emit `null` for the scores and `"unavailable"` for the bias when `covered` is empty (`coverage_ratio` is already there to justify it).

### M6 — Hermes schedule is a hardcoded dict served as operator status
`api/routers/system.py:984-991`
```python
schedule_info = {
    "cycle_interval": "5 minutes",
    "pipeline_interval": "6 hours",
    "autoresearch": "weekdays 2 AM",
    "daily_briefing": "weekdays 6 AM",
```
Returned as `schedule` from `/system/hermes-status` and `/system/hermes-history` beside genuinely-queried task rows. Nothing reads the real scheduler config, so these silently lie if the schedule changes.
**Honest replacement:** read the live schedule off the running `OperatorState` (already shared via `set_hermes_state`) or omit the block when Hermes isn't reporting.

### M7 — `next_scheduled` is `last_pull + a guessed interval`
`api/routers/system.py:595-612`
```python
# Compute next_scheduled (approximate)
...
delta = delta_map.get(freq, timedelta(days=1))
next_dt = last_pull + delta
```
The interval comes from the hardcoded `_SOURCE_SCHEDULE` map (`:479-506`), not from the scheduler. Served as `next_scheduled` (an ISO timestamp) with no "approximate" marker, so the operator UI shows a specific future time that nothing will honour.
**Honest replacement:** read the real next-run time from the scheduler, or rename the field `estimated_next_pull`.

### M8 — `rows_last_pull` actually reports rows in the last 48 hours
`api/routers/system.py:619` (schema `api/schemas/system.py:126`)
```python
rows_last_pull=recent_rows,
```
`recent_rows` comes from `COUNT(*) ... WHERE rs.pull_timestamp >= NOW() - INTERVAL '48 hours'` (`system.py:544-548`). Rendered verbatim in `pwa/src/views/PipelineHealth.jsx:468` and used as a sort key at `:301`. A daily source that ran twice shows double its true last-pull row count.
**Honest replacement:** rename to `rows_last_48h`, or count rows at `MAX(pull_timestamp)` only.

### M9 — `series_count` and `resolver.pending` are sampled, presented as totals
`api/routers/system.py:550-561` and `:1485-1496` (`LIMIT :series_limit`, `_SERIES_COUNT_SAMPLE_LIMIT = 50_000`); `:672-687` (`_RESOLVER_PENDING_SAMPLE_LIMIT = 100_000`)
```python
"  SELECT COUNT(DISTINCT sampled.series_id) AS series_count "
"  FROM ( SELECT rs.series_id FROM raw_series rs WHERE rs.source_id = sc.id "
"         ORDER BY rs.pull_timestamp DESC LIMIT :series_limit ) sampled"
```
Both are bounded samples of the newest rows, surfaced as plain counts (`series_count`, `resolver_status.pending`) and embedded in human labels (`f"{name} ({series_count} series)"` at `:1534`). For any source past the sample window the number is a floor, not a count.
**Honest replacement:** label them `series_count_sampled` / `pending_sampled` with the sample limit in the payload.

### M10 — `total_resolved` / `total_raw` are Postgres estimates formatted as exact counts
`api/routers/system.py:1562-1564`, `:1575-1577`
```python
r = conn.execute(text(
    "SELECT reltuples::bigint FROM pg_class WHERE relname = 'resolved_series'"
)).fetchone()
return r[0] if r and r[0] > 0 else 0
```
`reltuples` is an ANALYZE-time estimate. It is rendered comma-formatted as fact — `f"PIT Query Engine ({resolved_count:,} rows)"` (`:1651`). Worse, a never-analyzed table has `reltuples = -1`, so the `> 0` guard reports **0 rows** for a fully populated table, which then flips the node's status to `"new"` (`:1652`) and lists it as a gap.
**Honest replacement:** return `COUNT(*)` (or keep `reltuples` but name the field `approx_*`) and treat `-1` as unknown rather than zero.

### M11 — Power-map edges stamped `confidence: "confirmed"`
`api/routers/canvas.py:930`
```python
"type": "connection",
"label": cr["relationship"] or "",
"strength": float(cr["strength"] or 0.5),
"confidence": "confirmed",
```
The same relationship type from the same table is labeled `0.7` at `canvas.py:422` and `"confirmed"` here — proof neither is data-derived. `"confirmed"` is the top rung of the router's own `VALID_CONFIDENCE_LABELS` (`canvas_graph.py:87`).
**Honest replacement:** carry the row's real provenance, or omit the field.

### M12 — Derived co-signal edges get a constant strength and a "derived" label
`api/routers/canvas.py:985`
```python
"type": "co_signal",
"label": ticker,
"strength": 0.4,
"confidence": "derived",
```
Synthesized from "both actors touched ticker X within 90 days"; 0.4 is applied to every pair regardless of overlap count or recency.
**Honest replacement:** derive strength from the actual co-occurrence count, or ship the count and let the client scale.

### M13 — Signal confidence coerced to 0.5 / relabeled `"estimated"`
`api/routers/canvas.py:509` and `:530` (duplicated at `:778-782`)
```python
conf_map = {"confirmed": 1.0, "derived": 0.8, "estimated": 0.6,
            "rumored": 0.3, "inferred": 0.5}
conf_val = conf_map.get(conf_raw.lower(), 0.5)
```
The enum→number map is defensible, but the `0.5` fallback invents a number for unknown labels. `confidence_label` also defaults to `"estimated"` while the DB column default is `'derived'` (`schema.sql:1685`), so a row written with the schema default is re-reported under a different, weaker provenance.
**Honest replacement:** `confidence: null` + `confidence_label: "unknown"` for unmapped values.

### M14 — News nodes attributed to "GRID" when the real source is unknown
`api/routers/canvas_expand.py:752`
```python
"title": nm["headline"],
"source": nm["source"] or "GRID",
"published_at": str(nm["signal_date"]) if nm["signal_date"] else None,
```
A third-party headline with a NULL source is shown to the analyst as sourced by GRID itself; `:758` reuses the same value as the edge label. It is persisted into `canvas_nodes`, mirrored into `investigation_boards.graph_state`, and resurfaces from `GET /api/v1/canvas/boards/{id}`.
**Honest replacement:** leave `source` null and render "source unknown" client-side.

### M15 — `/canvas/explain` returns a templated stub graded with a confidence when no LLM ran
`api/routers/canvas_llm.py:231`
```python
# Fallback when LLM produces no parseable output
return ExplainResponse(
    explanation=(
        f"Connection between {source_label} and {target_label} — "
        f"{n_connections} direct links, {n_signals} shared signals."
    ),
    confidence="estimated",
```
`llm/router.py:141` `get_llm()` never raises — on total LLM outage it returns `_NullClient` (`llm/router.py:1309-1318`) whose `generate()` returns `None`. So the honest "LLM unavailable" branch at `canvas_llm.py:198-208` is effectively unreachable for the common outage case, and control always lands here: a confidence-graded explanation with nothing in the response schema to signal that no model was consulted.
**Honest replacement:** add `llm_available` / `generated_by` to `ExplainResponse` and return `confidence: null` whenever `generate()` returned falsy.

### M16 — `llm_research_started: true` for an event with no consumer
`api/routers/canvas_investigate.py:440`
```python
from events.producer import emit
emit("canvas", {
    "event_type": "investigation_started",
    ...
})
return True
```
`"investigation_started"` appears exactly once in the repo — here. `emit` also returns `True` from its PG-NOTIFY fallback (`events/producer.py:120`), so the flag means "a message was published into the void", not "research started".
**Honest replacement:** rename to `research_event_emitted`, or return `false` until a worker acknowledges the job.

### M17 — `/canvas/dots` reports a complete scan while two of seven sections always fail
`api/routers/canvas.py:2143`
```python
"connections": connections,
"total": len(connections),
"lookback_days": days,
"generated_at": datetime.now(timezone.utc).isoformat(),
```
Each of the seven blocks is wrapped in `try/except … log.debug` (`:1898, 1938, 1977, 2017, 2054, 2092, 2131`), so a failed scan is indistinguishable from "nothing found". Two are permanently broken: `:1914` and `:1953` use `ABS(EXTRACT(EPOCH FROM a.signal_date - b.signal_date))`, but `signal_data.signal_date` is `DATE` (`schema.sql:1678`), so `date - date` yields an integer and `EXTRACT(EPOCH FROM <integer>)` errors in Postgres. `whale_convergence` and `lobbying_insider` therefore never appear, yet `total` presents the truncated list as the full result. (`canvas_expand.py:697` does the same day-diff correctly.)
**Honest replacement:** collect per-section status and return `sections_failed: [...]` alongside `total`.

### M18 — Lever-puller influence/trust defaulted to 0.5
`api/routers/canvas_expand.py:840` (also `:191-192`)
```python
"influence_rank": float(lm["influence_rank"]) if lm["influence_rank"] else 0.5,
"trust_score": float(lm["trust_score"]) if lm["trust_score"] else 0.5,
```
**Honest replacement:** pass `null` through so the badge can show "unrated".

### M19 — Hardcoded edge strengths on auto-investigate boards
`api/routers/canvas_investigate.py:359`, `:389`
```python
_place_edge(conn, board_id, canvas_nid, nid, "signal", 0.8)
```
0.8 for "an actor name substring-matched a signal" and 0.9 for "a wealth flow exists" — literals unrelated to amount, recency, or match quality. Written to `canvas_edges.data`, mirrored into `graph_state`, served by `GET /api/v1/canvas/boards/{id}`.
**Honest replacement:** derive from the underlying quantity, or store no strength.

### M20 — A total data outage renders as maximum freshness
`analysis/viz_intelligence.py:556`
```python
else:
    hours_since_update = 0.0  # No data = assume fresh (conservative)

decay = math.exp(-hours_since_update / schedule.freshness_half_life_hours)
```
The comment is exactly backwards — assuming fresh is the anti-conservative choice. When `get_engine()` fails or the `source_catalog` query raises (both swallowed with `log.warning` at `:519` and `:537`), or a source has no `last_pull_at`, every family is treated as zero hours old and returns its **peak** weight. The docstring at `:505` advertises "Queries source_catalog.last_pull_at for actual freshness". The lookup key is `schedule.source` matched against `LOWER(source_catalog.name)`, so a naming mismatch lands in the same always-fresh branch.
**Honest replacement:** return `weight: null` with `freshness: "unknown"` when no `last_pull_at` was found or the query failed.

### M21 — A source released *today* is reported as `"empty"`
`api/routers/mcp_export.py:284`
```python
"status": "fresh" if staleness_days and staleness_days <= 7
          else "stale" if staleness_days and staleness_days <= 30
          else "dead" if staleness_days
          else "empty",
```
`staleness_days == 0` is falsy, so every same-day source falls through all three branches to `"empty"` — indistinguishable from a source with no rows at all. The freshness report inverts its own signal for the best-maintained sources, and `stale_count` is computed off it.
**Honest replacement:** branch on `staleness_days is None` first, then compare the integer.

### M22 — `window_days` is echoed back but never applied
`api/routers/mcp_export.py:82`
```python
"influence_score": _safe_float(row[5]),
"updated_at": _safe_iso(row[6]),
"window_days": window_days,
```
`window_days` is accepted (`Query(default=90, ge=1, le=365)`) and returned beside `trust_score`, but the SQL at `:60-67` is an unfiltered `SELECT ... FROM actors`. A caller requesting a 7-day trust score receives the all-time stored score tagged `"window_days": 7`.
**Honest replacement:** compute over the requested window, or drop the parameter and return `updated_at` as the only provenance.

### M23 — `/ollama/status` reports a boot-time cached availability flag as live
`api/routers/ollama.py:110`
```python
client = _get_client()
result: dict[str, Any] = {
    "available": client.is_available,
    "model": client.model,
```
`get_client()` (`ollama/client.py:587`) is a module-level singleton; `is_available` is set once by the constructor probe and only refreshed by an explicit `health_check()`, which this handler calls **only** on the `settings.LLAMACPP_ENABLED and hasattr(client, "get_metrics")` branch (`:117`). On the OpenAI and Ollama backends a server that died after boot keeps reporting `available: true` for the life of the grid-api process. `model`/`embed_model` are the configured names, not what the server reports loaded.
**Honest replacement:** call `client.health_check()` in the handler and return its live result.

### M24 — `POST /workflows/{name}/run` says "accepted"/"triggered" and runs nothing
`api/routers/workflows.py:92`
```python
# Return the workflow info — actual execution is dispatched via CLI
return {
    "status": "accepted",
    "name": name,
    ...
    "message": f"Workflow '{name}' execution triggered. "
```
Nothing is executed or queued; the handler only loads metadata and logs. The docstring even claims "Execute a workflow by name (synchronous — may take a while)". A UI or agent will believe a job is in flight.
**Honest replacement:** return HTTP 501, or `{"status": "not_dispatched", "run_with": "python cli.py run <name>"}`.

### M25 — Agent Card advertises hardcoded entity counts to external agents
`a2a/agent_card.py:145` (also `:131`, `:177`)
```python
description=(
    "Query the financial actor network — 495 named actors "
    "with wealth flow tracking, congressional trades, "
    "lobbying disclosure, and campaign finance mapping."
),
```
"495 named actors" and "464+ data sources" are string literals in a machine-readable capability document consumed by third-party agents, with no update path from `SELECT COUNT(*) FROM actors` / `source_catalog`.
**Honest replacement:** render the counts from the DB at card-build time, or remove the numbers.

### M26 — Agent Card advertises six skills that no handler backs
`api/routers/a2a.py:187` → `a2a/server.py:33`
```python
def _get_task_manager() -> A2ATaskManager:
    """Return a cached A2ATaskManager singleton."""
    global _task_manager
    if _task_manager is None:
        from a2a.server import A2ATaskManager
        _task_manager = A2ATaskManager()
    return _task_manager
```
`A2ATaskManager.register_handler` is defined at `a2a/server.py:33` and **never called anywhere in the production tree** (verified by repo-wide grep excluding `tests/`). The manager is always constructed with an empty registry, so all six advertised skills (`forecast`, `oracle_prediction`, `regime_detection`, `signal_analysis`, `actor_network`, `options_flow`) accept a task, log "no handler for skill", and leave it `submitted` forever — while the card presents them as live capabilities.
**Honest replacement:** register real handlers at startup, or reject unhandled `skill_id`s with 501 and publish only handler-backed skills.

### M27 — `/correlations` echoes a `lookback_days` it never applies
`api/routers/astrogrid_celestial.py:151`, `:160-161`
```python
results = ace.get_cached_or_compute()
...
"count": len(results),
"lookback_days": lookback_days,
```
`get_cached_or_compute()` takes no window argument; `compute_correlations` always uses its own default `lookback_days=504` (`analysis/astro_correlations.py:187`), and a cached batch up to 24h old may be returned instead. A request for `lookback_days=90` receives 504-day correlations labelled 90.
**Honest replacement:** pass the parameter through, or drop the field and return the batch's real `computed_at` and window.

### M28 — `/ephemeris` reports `source: "computed"` after a swallowed DB failure
`api/routers/astrogrid_celestial.py:80-81`, `:98`
```python
except Exception as e:
    log.warning("Celestial: DB ephemeris lookup failed: {e}", e=str(e))
...
"source": "computed" if not db_data else "db+computed",
```
A DB outage and a legitimately empty date are indistinguishable to the client — both yield `"source": "computed"` with no degradation flag.
**Honest replacement:** add `"db_lookup": "ok" | "failed"` and mark `source` as `computed_degraded` when the query raised.

### M29 — `/solar/activity` mixes an 11-year-cycle formula into a "from resolved_series" payload
`api/routers/astrogrid_celestial.py:656-657`
```python
if solar_features["solar_cycle_phase"] is None:
    solar_features["solar_cycle_phase"] = round(_solar_cycle_phase(today), 6)
```
The endpoint docstring says "Current solar weather from resolved_series", but `_solar_cycle_phase` is `(days since 2019-12-01) % 4017.75 / 4017.75` (`ingestion/celestial/solar.py:59-67`) — a calendar fraction. It lands in the same `features` dict as NOAA-sourced values; the only tell is a `None` in `obs_dates`. (Same formula, worse handling, in M1.)
**Honest replacement:** return it under a separate `derived` key with `"source": "cycle_25_linear_model"`.

### M30 — `/scorecard` persists a possibly-stale close stamped with today's date
`api/routers/astrogrid_core.py:206`
```python
if live_quote and live_quote.get("price") is not None:
    _cache_price_to_db(
        engine, asset["lookup_ticker"], float(live_quote["price"]), date.today()
    )
```
`_batch_fetch_prices` (`api/routers/watchlist_helpers.py:338-378`) uses `period="5d"` and returns the last non-null close with `updated_at = now`. Called on a weekend or holiday for SPY/QQQ/TLT/GLD/CL, Friday's close is written into `raw_series` as Sunday's observation, then re-served as `latest` and reused by the scorer — a GET endpoint silently corrupting the time series.
**Honest replacement:** cache under the quote's actual bar date from the yfinance index, not `date.today()`.

### M31 — Oracle publish invents a `NEUTRAL` regime when context building fails
`oracle/publish.py:86-92`
```python
except Exception:
    context = {
        "regime": "NEUTRAL",
        "fci_regime": "NEUTRAL",
        "vix_level": None,
        "signal_contributions": {},
    }
```
`vix_level` is honestly `None`, but the two regime fields become invented labels indistinguishable from a genuinely neutral reading. They are stored on the prediction and reused as backtest regime context (`store/astrogrid.py:1802-1805`), so per-regime performance breakdowns absorb them.
**Honest replacement:** `"regime": None, "fci_regime": None, "context_status": "unavailable"`.

### M32 — `entry_price` hardcoded to 0.0
`oracle/publish.py:160`
```python
"direction": _prediction_direction(payload),
"entry_price": 0.0,
"expiry": _prediction_expiry(payload),
```
A literal 0.0 in a price column on `oracle_predictions`, read by the oracle scoreboard/detail endpoints. Anything computing a move from it produces nonsense. The column plainly accepts NULL — `target_price` is passed as NULL two lines up.
**Honest replacement:** insert `NULL`, or look up the real close for `as_of_ts`.

### M33 — Review confidence is a stack of arbitrary constants
`store/astrogrid.py:1091-1097`
```python
confidence = 0.45
if hit_count + miss_count >= 10:
    confidence += 0.15
if best_alpha and best_alpha > 0.02:
    confidence += 0.15
if top_grid:
    confidence += 0.05
```
plus `store/astrogrid.py:1346` `"confidence": float(review_payload.get("confidence") or 0.5),`. `calibrate_confidence_default` returns the value unchanged whenever the reliability table is missing or the engine is unavailable (`intelligence/confidence_calibration.py:246-252`), so in practice the published number is just the constant stack.
**Honest replacement:** publish the evidence counts (`scored_n`, `best_alpha`) and omit `confidence` until a reliability curve exists for `astrogrid_mystical`.

### M34 — Learning loop silently re-runs the backtest over a different window
`store/astrogrid.py:1391-1394`
```python
if not any(((run.get("summary") or {}).get("total_predictions") or 0) for run in backtest_summary.get("runs", [])):
    fallback_window = self._scored_prediction_date_range(horizon_label=horizon_label)
    if fallback_window and fallback_window[0] and fallback_window[1]:
        backtest_summary = self.run_backtests(
```
The caller's `backtest_window_days` is silently replaced by the full scored history, and the response never states which window produced the numbers (`store/astrogrid.py:1864` returns `{"runs": [...], "count": ...}` with no window echoed).
**Honest replacement:** include `window_start` / `window_end` and a `window_source: "requested" | "fallback_full_history"` flag on each run.

### M35 — Accuracy reported as 0.0 when nothing has been scored
`oracle/scoreboard.py:25-28` (same pattern at `store/astrogrid.py:1636`, `:1655`)
```python
def _prediction_accuracy(hits: int, misses: int, partials: int) -> float:
    scored = hits + misses + partials
    if scored <= 0:
        return 0.0
```
"No outcomes yet" and "everything missed" both render as 0% accuracy — surfaced as `summary.oracle_accuracy` and `evaluation.overall.accuracy` on `/astrogrid/scorecard`.
**Honest replacement:** return `None` when `scored == 0` so the UI can show "not yet scored".

### M36 — Nakshatra computed from a tropical longitude but labelled sidereal
`api/routers/astrogrid_celestial.py:542-544`, `:553`
```python
moon_lon = _moon_sidereal_longitude(today)
nak_idx = _nakshatra_from_longitude(moon_lon)
nak_name = _NAKSHATRA_NAMES[nak_idx]
...
"moon_sidereal_longitude": round(moon_lon, 2),
```
`_moon_sidereal_longitude` (`ingestion/celestial/vedic.py:72-77`) is `(218.3165 + 13.1764·days) % 360` — the *tropical* mean longitude with **no ayanamsha subtracted**, while the companion `_sun_sidereal_longitude` in the same file (`:89`) does subtract ~23.85°. The ~24° offset is nearly two full nakshatras (13°20′ each), so the reported nakshatra is frequently wrong. Two-decimal output also implies precision a mean-motion model (ignoring perturbations of several degrees) cannot support. Propagates to `/overview`, `/ephemeris`, `/compare`, `/briefing`.
**Honest replacement:** subtract the same Lahiri ayanamsha used for the Sun, round to whole degrees, and name the field `moon_sidereal_longitude_approx`.

### M37 — Mercury retrograde is guessed outside the table and silently stops inside it
`ingestion/celestial/planetary.py:120-126`
```python
if d < date(2020, 1, 1) or d > date(2030, 12, 31):
    ref_start = date(2020, 2, 17)
    days_since = (d - ref_start).days
    cycle_pos = days_since % 115.88
    # Retrograde roughly days 0-24 of each cycle
    return cycle_pos < 24.0
```
Beyond 2030 the boolean is a modular guess with no relation to Mercury's actual motion, returned identically to the table-backed value. Separately, `/retrograde` (`astrogrid_celestial.py:427`) and `/timeline` (`:194`) read `_MERCURY_RETROGRADES` directly with **no** fallback, so after 2030-12-25 they will report "no retrogrades" as fact.
**Honest replacement:** return `None`/`"unknown"` outside the table's coverage and expose a `coverage_end` date on `/retrograde`.

### M38 — `mars_volatility_index` is an orb score named like a volatility measurement
`ingestion/celestial/planetary.py:172-180`
```python
# Score: higher when Mars is in hard aspect to Jupiter or Saturn
score = 0.0
for sep in [mars_jup, mars_sat]:
    for target in [0, 90, 180]:
        closeness = abs(sep - target)
        if closeness < 15:
            score += (15 - closeness) / 15.0
return min(score / 2.0, 1.0)  # Normalise to 0-1
```
The 15° window, the linear ramp and the `/2` normaliser are invented. It is displayed on `/briefing` as "Mars volatility index: …" (`astrogrid_celestial.py:330`) and grouped under `planetary` on `/overview` (`astrogrid_core.py:76`) — presented as a measured volatility reading. It is also computed from `_geo_longitude`, a linear mean-longitude model (same file, `:78-104`), not a real geocentric position.
**Honest replacement:** rename to `mars_hard_aspect_proximity_score` and document it as a synthetic 0–1 geometry score.

---

## LOW findings — detail

### L1 — Every celestial body stamped `track_mode: "reliable"`
`api/routers/astrogrid_helpers.py:743`
```python
"track_mode": "reliable",
"precision": "computed",
"source": "analysis.ephemeris",
```
A blanket quality assertion applied identically to the Sun and to Pluto/Rahu/Ketu. (`precision` and `source` are accurate.)
**Honest replacement:** drop `track_mode`, or set it per body from the ephemeris model's actual accuracy class.

### L2 — Unknown regime label becomes a neutral 0.0 signal value
`api/routers/astrogrid_helpers.py:877`
```python
"value": market_bias if market_bias is not None else 0.0,
```
If the regime label isn't in `_MARKET_REGIME_BIAS` (`:325-338`), the signal value is 0.0 (neutral) while the label still shows the real regime — a fabricated neutral rather than "unmapped".
**Honest replacement:** omit the `market_regime` signal entry when the bias is unmapped.

### L3 — `_build_postmortem_stub` is stored in the `postmortem_*` columns at prediction time
`api/routers/astrogrid_helpers.py:634-654` → `astrogrid_predictions.py:241`
```python
summary = (
    f"Pending {horizon} read on "
    f"{', '.join(target_symbols) if target_symbols else req.target_universe}: "
    f"{req.call}. Break if {req.invalidation.lower()}."
)
```
**Answering the specific question posed: no, the stub is not served to users as a real post-mortem.** It is written into `prediction_postmortem` with `state = 'pending'` (`store/astrogrid.py:719`), `list_postmortems` selects `pp.state` (`store/astrogrid.py:2231`), and `astrogrid_web/app.js:2107` renders that state next to the summary — and the summary text itself begins "Pending". The residual issue is narrower: `dominant_grid_drivers` / `dominant_mystical_drivers` are derived from the request's own snapshot at creation time, yet sit in fields whose names imply post-outcome attribution, and `GET /api/v1/astrogrid/postmortems` mixes pending stubs with real ones with no filter.
**Honest replacement:** default `/postmortems` to `state != 'pending'` (or accept a `state` filter), and name the creation-time fields `drivers_at_entry`.

### L4 — Static architecture edges served from an "introspection" endpoint
`api/routers/system.py:1698-1728`
```python
data_flows = [
    {"from": "ingestion", "to": "resolver", "label": "raw_series", "color": "#22C55E"},
    {"from": "resolver", "to": "pit_engine", "label": "resolved_series", "color": "#22C55E"},
```
A hand-maintained diagram, not observed topology, returned from an endpoint documented as "a meta-view of the running system". Borderline — included because the docstring makes a claim the data doesn't support.
**Honest replacement:** label the block `static_architecture_reference` in the payload.

### L5 — NULL flow confidence relabeled, NULL amount collapsed to `0`
`api/routers/canvas.py:589` (also `:627`, `canvas_expand.py:545`, `:579`, `:872`)
```python
"amount": float(r["amount_estimate"] or 0),
"confidence": r["confidence"] or "estimated",
```
`amount` renders as "$0" rather than "unknown".
**Honest replacement:** pass `null` through for both.

### L6 — Insider / congressional nodes hardcoded to `confidence: "confirmed"`
`api/routers/canvas_expand.py:613` (also `:649`, and `:900` `"rumored"`)
```python
"magnitude": min(val / 1e6, 10) if val else 0,
"confidence": "confirmed",
```
Defensible in intent (Form 4 / STOCK Act filings are disclosures), but the label is applied blindly without reading any provenance column, so a stale or unverified row is still stamped "confirmed". `magnitude` here is also a rescaled, capped derivative sharing a field name with raw magnitudes elsewhere.
**Honest replacement:** derive the label from the source table's ingest metadata.

### L7 — Unknown viz families invent `weight: 0.5`, `half_life_hours: 24`
`api/routers/viz.py:83`
```python
"weight": weights.get(f, 0.5),
"cadence": WEIGHT_SCHEDULES[f].cadence if f in WEIGHT_SCHEDULES else "unknown",
"pulse_on_update": WEIGHT_SCHEDULES[f].pulse_on_update if f in WEIGHT_SCHEDULES else False,
"half_life_hours": WEIGHT_SCHEDULES[f].freshness_half_life_hours if f in WEIGHT_SCHEDULES else 24,
```
Presented next to genuinely computed weights under a docstring claiming "Weights reflect freshness".
**Honest replacement:** omit unknown families, or return `weight: null, known: false`.

### L8 — `available: true` for a topic that doesn't exist
`events/consumer.py:112`
```python
return {
    "topic": topic,
    "partitions": len(partitions),
    "available": True,
}
```
`partitions_for_topic` returns `None` for a nonexistent topic; the `or set()` at `:109` turns that into `partitions: 0` while still asserting availability. This flips `redpanda_available` and suppresses the honest `fallback: "pg_notify"` marker in `api/routers/sse.py:327-333`. Marked PLAUSIBLE — depends on broker client behaviour not verified against a live broker.
**Honest replacement:** `"available": len(partitions) > 0`.

### L9 — Hardcoded 4%/8% levels presented as this read's invalidation
`api/routers/astrogrid_predictions.py:118` (also `:108`, `:113`)
```python
invalidation = f"stop the read if {selected} gives back 4% on swing or 8% on macro horizon"
```
Literals embedded in a sentence that reads as an instrument-specific risk level, unrelated to the asset's volatility or to the scorer's own `_horizon_thresholds`.
**Honest replacement:** derive from the symbol's realised volatility, or state it plainly as a fixed house rule.

### L10 — `/eclipses` labels every window `"+-5 days"` regardless of what was found
`api/routers/astrogrid_celestial.py:511-516`
```python
past_with_returns.append({
    "date": str(ecl),
    "type": ecl_type,
    "spy_return_pct": round(ret, 2),
    "window": "+-5 days",
})
```
The guard accepts any `len(vals) >= 2`, so the return may span as little as two adjacent rows while still carrying the fixed ±5-day label.
**Honest replacement:** emit the actual `first_obs_date` / `last_obs_date` and `n_obs`.

### L11 — `/briefing` serves an arbitrarily old cached narrative as current
`api/routers/astrogrid_celestial.py:294-304`
```python
if row:
    return {
        "briefing": row[0],
        "generated_at": str(row[1]),
        "source": "cached",
    }
```
No age bound on the query; a months-old briefing describing a different moon phase is returned as the current celestial state. `generated_at` and `source: "cached"` are honest, which is why this is LOW.
**Honest replacement:** add `AND generated_at::date = CURRENT_DATE` (falling through to the computed branch), or return a `stale: true` flag.

### L12 — `iching_hexagram_of_day` is date arithmetic served as a celestial feature
`ingestion/celestial/chinese.py:168-175`
```python
upper = ((y % 100) + m + day) % 8
# Lower trigram from (year + month * day) mod 8
lower = ((y % 100) + m * day) % 8
# Hexagram = upper * 8 + lower + 1 (1-indexed)
hexagram = upper * 8 + lower + 1
```
The docstring concedes it is "a deterministic hash of the date", yet it is returned inside the `chinese` category next to genuine calendar values and counted in `/ephemeris`'s `feature_count`.
**Honest replacement:** rename to `iching_hexagram_deterministic_hash`, or drop it from the ephemeris feature set.

---

## Ruled-out constants (considered and excluded)

**chat.py**
- `CARD_BUSY_MESSAGE` (`:1155`), `_build_rule_based_response`'s "I don't have enough live data…" (`:1386`) — honest unavailable copy.
- `_REGIME_DATA_STALE_AFTER_DAYS = 5` (`:333`) — documented threshold, and `_gather_regime_context` (`:336-382`) is exemplary: it distinguishes `obs_date` / `data_as_of` / `created_at` and instructs the model not to present stale data as today's.
- Timeframe instruction map (`:1805-1811`), `_WIDGET_CATALOG` (`:1974-2001`), `_ALERT_NAME_TO_TICKER` (`:2391-2399`), `_GAP_PATTERNS` (`:2348-2360`) — curated static catalogs/prompt text, no fabricated observations.
- `_validate_compose_layout`'s "always at least one widget" fallback (`:2102-2105`) — produces a *layout*, and the widget fetches its own real data.
- `GRID_SYSTEM_CONTEXT` "50+ live data feeds, 10 thesis models" (`:950`) — an unverified self-description, but it is prompt text, not a response field. Noted, not filed.
- The compose path's honest refusals (`cannot_fulfill`, capability-gap logging, `CARD_BUSY_MESSAGE`) are a genuinely good pattern — the dad path refuses rather than fabricates.

**main.py**
- The line-~200 "persisted payload / placeholder" logic is a comment pointing at `api/routers/flows.py`. That code is honest: `_load_persisted_sectors_snapshot` (`flows.py:337-393`) refuses to seed a snapshot older than `_SECTOR_STALE_TTL`, and tags what it does serve with `snapshot_age_s` plus the original `computed_at`.
- The line-~360 warning is the `GRID_ALLOWED_ORIGINS` CORS fallback — a config default, correctly logged.

**system.py**
- `_SOURCE_TYPE_MAP` / `_SOURCE_SCHEDULE` (`:456-506`) as *classification and staleness thresholds* — legitimate config. Only their use to fabricate `next_scheduled` (M7) is filed.
- GREEN/YELLOW/RED freshness cutoffs (`:403-408`) — documented in the docstring.
- `checks["ws_clients"] = -1` on failure (`:151`) — an explicit sentinel, not a fake count.
- `api_key_fields` descriptions (`:1235-1260`) — static label text; the `configured`/`missing` status is really computed.

**astrogrid_helpers.py**
- `_PHASE_NAMES`, `_BODY_META`, `_ELEMENT_BY_SIGN`, `_PUBLIC_LENS_LABELS`, `_NAKSHATRA_*`, `_ZODIAC_ANIMALS` — enum/label tables.
- `_interpret_kp` (`:1655-1670`) — a standard Kp banding, and it returns `"No data"` for `None` rather than inventing a level.
- `_fallback_interpretation` (`:1438-1480`) — a model-outage fallback done right: `used_llm: False`, `backend: "fallback"`, `model: None`, and "No interpreted reading." rather than invented prose.
- `_build_scorecard_item`'s `source` field (`:1906`) — accurately assembled from what actually contributed, and `"unresolved"` when nothing did. The correct pattern, which makes M1 the outlier.
- `_MARKET_REGIME_BIAS` (`:325-338`) — a curated label→bias map; kept out of the findings because the client is given the label alongside the number.

**astrogrid routers / celestial ingestion**
- `_MERCURY_RETROGRADES`, `_LUNAR_ECLIPSES`, `_SOLAR_ECLIPSES`, `_CNY_DATES`, `_FLYING_STAR_PERIODS`, `_KP_TO_AP` — real reference tables with cited provenance (USNO/JPL, NOAA standard mapping). Table *lookup* is honest; only the out-of-range guesswork (M37) is filed.
- `SYNODIC_MONTH = 29.53059`, `_REF_NEW_MOON`, `SIDEREAL_MONTH`, `_MOON_RATE` — documented astronomical constants.
- `oracle/astrogrid_universe.py:20-147` `_ASTROGRID_SCOREABLE_UNIVERSE` — hardcoded, but it is a *contract* (symbol → feature/benchmark mapping), and `enrich_astrogrid_scoreable_universe` attaches real `history_points` / `latest_obs_date` / `status` from the DB. `GET /universe`'s error path (`astrogrid_core.py:271-279`) downgrades to `status="unknown"`, `scoreable_now=False`, `reason_if_not="coverage check unavailable"` — a model of honest degradation, and the pattern the rest of this codebase should copy.
- `_MIN_SCOREABLE_HISTORY_POINTS = 60`, `_STALE_HISTORY_DAYS = 14`, `_AUTO_PUBLISH_CONFIDENCE`, `_REVIEW_REWRITE_RATIO`, `_horizon_thresholds`, `_NEUTRAL_MOVE_BAND` — documented scoring thresholds.
- `GET /overview`'s except at `astrogrid_core.py:148-150` and `/correlations`' at `astrogrid_celestial.py:167-169` — honest error objects, no invented rows.
- `build_prediction_scoreboard` / `build_oracle_ticker_rollup` counts — genuinely aggregated from stored verdicts; the `partials * 0.5` weighting is a stated convention. (Only the `0.0`-for-empty convention is filed, M35.)
- `GET /lunar/calendar`, `POST /compare`, `GET /timeline` full/new-moon detection — mean-synodic approximations (±~0.5 day) consistent with the module's documented model.
- `analysis/astro_correlations.py` bootstrap p-values — a real permutation test; the returned number is what it claims. Flagged for a *methodology* review rather than this audit: the `p <= 0.05` filter (`:281`) is applied to the best of 61 lags × all pairs with no multiple-comparison correction, so "significant" is overstated.
- `"Entertainment and research only. Not financial advice."` disclaimer (`astrogrid_predictions.py:163`), `tone_notes` — honest UI copy.

**canvas group**
- `canvas_core.py` is **dead code** — `canvas.py:44-49` states it is intentionally not mounted (its `/boards` routes collide with the facade's), and nothing else imports it. Its `_auto_seed_board` fabrications (`canvas_core.py:225` `"source": nr[1] or "GRID"`, the hardcoded SPX/VIX/DXY/US10Y rows at `:185-190`) would be findings if reachable.
- `_LAYER_SOURCE_MAP` / `_LAYER_CATEGORY_MAP` (`canvas.py:124-144`), `_ACTOR_ID_PREFIXES` (`:179`) — routing/formatting config.
- `/dots` SQL `HAVING` thresholds — documented detection criteria, not emitted as metrics.
- `VALID_CONFIDENCE_LABELS` / `VALID_EVIDENCE_TYPES` (`canvas_graph.py:87-89`) — request-validation enums.
- `PredictionRequest.confidence = 0.5` (`canvas_predict.py:34`) — a *client-supplied* value echoed back; `threshold = 2.0` (`:153`) is the stated test criterion stored in `test_criteria`.
- Layout geometry throughout (`_circular_positions` radii, `R_BASE`, `avg_x + 200`, fixed 500/300 coordinates).
- `_CANVAS_GRAPH_CACHE_TTL_SECONDS = 60` (`canvas.py:91`) — the cached payload keeps its original `generated_at`, which is correct.
- All of `canvas_graph.py` and `canvas_board_store.py` — CRUD/serialization; their `or "note"` / `or 0.0` defaults normalize client-submitted payloads.
- `"No direct connection found (2-hop search not yet implemented)"` (`canvas_expand.py:1034`) — honest copy.

**misc group**
- `api/routers/search.py:15-47` `_VIEWS` — a static navigation registry; no relevance score is ever emitted (the docstring's "sorted by relevance" isn't reflected in any response field).
- `api/routers/viz.py:96-241` `VizSpec` endpoints — rendering instructions only; values come from the referenced `data_endpoint`s.
- `analysis/viz_intelligence.py:113-128` `WEIGHT_SCHEDULES` — documented cadence/half-life config; only its fallback use (M20/L7) is filed.
- `ollama/market_briefing.py:544` `_generate_fallback_briefing` and `analysis/capital_flows.py:822` `_fallback_narrative` — both label themselves ("AI analysis unavailable. Data summary only.", "(Data Only — LLM Unavailable)") and render only real rows; the cached path preserves the original `generated_at`.
- `llamacpp/client.py:449-484` `health_check` — `slots_idle`/`slots_processing` default to `None`, populated only from live JSON. `ollama/client.py:320-340` `list_models` really queries `/api/tags` and returns `[]` on failure. No hardcoded model list exists on any serving path.
- `ollama/reasoner.py` — returns `None` when unavailable; router converts to HTTP 503.
- `agents/backtest.py:86-94`, `:193-194` — empty result sets return zeroed counts with `has_data: False`.
- `store/blob.py:206-220` — real MinIO `list_buckets()`, honest `available: False` + error.
- `api/routers/sse.py` — the `connected` frame carries only the channel list; keepalives are bare `: keepalive` comments. **No synthetic activity events are injected.**
- `api/routers/vault.py:304` `d["rank"]` — a genuine Postgres `ts_rank(...)`.
- `api/routers/journal.py`, `notifications.py`, `blob.py`, `config.py`, `knowledge.py` (router layer) — DB/config pass-through with honest 404/503 paths.
- SQL bind placeholders and validation allowlists throughout — excluded per scope.

---

## Files reviewed

Fully read, line-for-line:

| File | Lines |
|---|---|
| `api/routers/chat.py` | 2775 |
| `api/routers/system.py` | 1791 |
| `api/routers/astrogrid_helpers.py` | 2002 |
| `api/main.py` | 771 |
| `api/routers/canvas.py` | 2147 |
| `api/routers/canvas_expand.py` | 1150 |
| `api/routers/canvas_board_store.py` | 622 |
| `api/routers/canvas_investigate.py` | 469 |
| `api/routers/canvas_graph.py` | 463 |
| `api/routers/canvas_core.py` | 393 (dead code — not mounted) |
| `api/routers/canvas_predict.py` | 297 |
| `api/routers/canvas_llm.py` | 274 |
| `api/routers/vault.py` | 532 |
| `api/routers/mcp_export.py` | 450 |
| `api/routers/ollama.py` | 342 |
| `api/routers/search.py` | 288 |
| `api/routers/viz.py` | 241 |
| `api/routers/a2a.py` | 193 |
| `api/routers/agents.py` | 191 |
| `api/routers/notifications.py` | 183 |
| `api/routers/config.py` | 180 |
| `api/routers/workflows.py` | 176 |
| `api/routers/sse.py` | 160 |
| `api/routers/journal.py` | 158 |
| `api/routers/blob.py` | 107 |
| `api/routers/knowledge.py` | 87 |
| `api/routers/astrogrid_celestial.py` | 686 |
| `api/routers/astrogrid_predictions.py` | 512 |
| `api/routers/astrogrid_core.py` | 306 |
| `api/routers/astrogrid.py` | 36 |

Helpers followed for provenance (read in part, as needed to trace a response value):
`analysis/ephemeris.py`, `analysis/thesis_scorer.py`, `analysis/viz_intelligence.py`, `analysis/capital_flows.py`, `ingestion/celestial/solar.py`, `physics/dealer_gamma.py`, `store/astrogrid.py`, `store/blob.py`, `intelligence/actors/db.py`, `intelligence/actors/models.py`, `knowledge/tree.py`, `llm/router.py`, `ollama/client.py`, `llamacpp/client.py`, `ollama/market_briefing.py`, `ollama/reasoner.py`, `a2a/agent_card.py`, `a2a/server.py`, `events/bus.py`, `events/consumer.py`, `events/producer.py`, `workflows/loader.py`, `agents/scheduler.py`, `agents/backtest.py`, `api/routers/flows.py`, `api/schemas/system.py`, `schema.sql`, plus PWA consumers (`pwa/src/components/ChatPanel.jsx`, `pwa/src/views/AppArchitecture.jsx`, `pwa/src/views/PipelineHealth.jsx`, `astrogrid_web/app.js`) to confirm which values are rendered to users.

## Files NOT fully reviewed

None in scope. Every file on the assigned list was read in full.

Helper modules audited as part of tracing astrogrid response values (they own several findings above): `store/astrogrid.py` (~1,300 of 2,815 lines — every path reachable from an audited endpoint; the DDL/schema and interpretation-persistence blocks were skipped as unreachable from a response body), `oracle/publish.py` (`publish_astrogrid_prediction`), `oracle/scoreboard.py`, `oracle/publisher_gate.py`, `oracle/astrogrid_universe.py`, `ingestion/celestial/{lunar,planetary,vedic,chinese}.py` (full), `ingestion/celestial/solar.py:1-140`.

---

## Cross-cutting observations

1. **`confidence` is the single worst-behaved field in this surface.** Nine independent findings (H4, H5, H6, H7, H8, H9, H11, M2, M13, M33) are all a hardcoded or heuristic number emitted under the name `confidence`. None is calibrated against outcomes; several are rendered directly to users as a percentage. A repo-wide policy — `confidence` is null unless it comes from a scored track record — would close most of this class at once.
2. **"Empty" and "zero" are conflated throughout.** M5, M21, M35, H14 and L8 are all the same bug shape: a missing measurement is substituted with `0.0` / `0` / a falsy-check default, making "no data" indistinguishable from a real neutral or failing reading. M20's inline comment ("No data = assume fresh (conservative)") shows the reasoning error explicitly.
3. **Three endpoints echo a query parameter they never apply** (M22 `window_days`, M27 `lookback_days`, M34 the backtest window). Each returns data from a different window than the one requested, labelled with the requested value.
4. **Auth gap worth separate triage (out of fake-data scope):** `POST /api/v1/astrogrid/guru/ask`, `GET /astrogrid/predictions/latest` and `GET /astrogrid/postmortems` carry no `Depends(require_auth)`, unlike every sibling endpoint — verified at `astrogrid_predictions.py:284-288`, `:315-319`, `:327-331`. This makes H10's fabricated trading call publicly reachable.

Out-of-scope notes worth flagging for whoever owns those files:
- `astrogrid_api/astrogrid_celestial.py` and `astrogrid_api/astrogrid_predictions.py` are near-duplicates of the `api/routers/` versions (e.g. `astrogrid_api/astrogrid_predictions.py:370` repeats the postmortem-stub write). Any fix applied under `api/routers/` needs mirroring or the duplicate should be deleted.
- `api/routers/dad.py` (2265 lines) builds the actual stepdad.finance numbers; `chat.py`'s composer only chooses *which* widgets render, so the dad-path values themselves were out of this audit's scope.
