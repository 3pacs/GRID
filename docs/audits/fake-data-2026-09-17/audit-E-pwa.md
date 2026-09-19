# Audit E — Fabricated data in the GRID React PWA

**Checkout:** `C:\Users\owner\dev\GRID-wt-fake-data-audit`
**Branch:** `audit/fake-data-cleanup-20260917` @ `fe6765bc` (origin/main) — the `pwa/` build served as stepdad.finance / GRID PWA on grid-svr, built 2026-09-17.
**Scope:** every production file under `pwa/src/views/` (incl. `canvas_lenses/`, `mobile/`), `pwa/src/components/` (incl. `home/`, `flows/`), `pwa/src/hooks/`, `pwa/src/canvas/`, `pwa/src/api.js`, `pwa/src/app.jsx`.
**Excluded:** `pwa/src/__tests__/`, `*.test.*` (fixtures, not production).
**Method:** 144 files / 72,945 lines read in full (not grepped), across 8 parallel passes, plus an independent cross-check grep sweep and manual re-verification of every HIGH/MED finding's line numbers and render path.

**Headline:** the codebase is far cleaner than the scope suggested. Only **5 HIGH** findings exist, concentrated in **3 views** (`TrendTracker.jsx`, `RiskMap.jsx`, `TenYearPortfolio.jsx`) plus one panel (`ActorProfileDrawer.jsx`). The PR-#419 / #432 / #446 / #457 / #434 / #447 cleanups **all held** — every file named in those PRs verified clean.

---

## Findings table (sorted by severity)

| # | File:Line | Route / View user sees | Cat | Sev | Conf |
|---|---|---|---|---|---|
| 1 | `pwa/src/views/TrendTracker.jsx:54-223`, invoked `:693`, `:697` | `#trends` — "Trends" (drawer, Markets) | 1 + 4 | **HIGH** | CONFIRMED |
| 2 | `pwa/src/views/RiskMap.jsx:571-592`, rendered `:1039` | `#risk` — "Risk" (primary tab) → RISK TIMELINE (30D) | 1 + 4 | **HIGH** | CONFIRMED |
| 3 | `pwa/src/views/TrendTracker.jsx:552-570` | `#trends` → "Comparison" tab → TREND STRENGTH OVER TIME | 4 | **HIGH** | CONFIRMED |
| 4 | `pwa/src/components/ActorProfileDrawer.jsx:126-184`, rendered `:265` | ActorProfileDrawer (from SectorDive / canvas) → CONFIDENCE STACK | 2 | **HIGH** | CONFIRMED |
| 5 | `pwa/src/views/TenYearPortfolio.jsx:99-119`, wired `:399-407`, rendered `:723-730` | `#ten-year` — "10-Year" (primary tab) → Planner chat | 4 | **HIGH** | CONFIRMED |
| 6 | `pwa/src/views/TrendTracker.jsx:222`, rendered `:1089-1093` | `#trends` → footer "Generated: …" | 3 | MED | CONFIRMED |
| 7 | `pwa/src/views/PipelineHealth.jsx:375-379`, rendered `:472-475` | `#pipeline-health` — "Pipeline" (drawer, Operations) | 2 | MED | CONFIRMED |
| 8 | `pwa/src/views/TenYearPortfolio.jsx:51-56`, rendered `:724-729` | `#ten-year` → first chat bubble | 3 | MED | CONFIRMED |
| 9 | `pwa/src/views/TenYearPortfolio.jsx:331-334`, rendered `:578-581` | `#ten-year` → "Dad Method Oracle" header | 3 | MED | CONFIRMED |
| 10 | `pwa/src/views/SectorDive.jsx:1507`, `:1516` | `#sector-dive/{sector}` → Intelligence Feed | 3 | LOW | CONFIRMED |
| 11 | `pwa/src/components/ChatPanel.jsx:121-125`, rendered `:528-530` | Global "Ask GRID" panel (every view) | 3 | LOW | CONFIRMED |
| 12 | `pwa/src/views/CrossReference.jsx:224-243`, rendered `:1436-1455` | `#cross-reference` — "Truth" (primary tab) → TICKER IMPACT | 4 | LOW | CONFIRMED |
| 13 | `pwa/src/views/Regime.jsx:291` | `#regime` — "Regime" (drawer) → REGIME EXPLAINED | 4 | LOW | CONFIRMED |

---

### Finding 1 — TrendTracker renders 11 invented trends on empty **and** on error

**File:** `pwa/src/views/TrendTracker.jsx:54-223` (generator), invoked at `:693` and `:697`
**Route/View:** `#trends` — "Trends", drawer nav, Markets group
**Category:** 1 FABRICATED FALLBACK + 4 HARDCODED VALUES PRESENTED AS OBSERVATIONS
**Severity:** HIGH · **Confidence:** CONFIRMED

```js
// line 52
// ── Placeholder data (used until API is wired) ──────────────────────────
function generatePlaceholderData() {
    const trends = [
        {
            name: 'Tech Sector Death Cross',
            category: 'momentum', direction: 'bearish', strength: 0.72,
            description: 'XLK 50-day MA crossed below 200-day MA, first time since October 2023',
```

```js
// lines 684-698
            try {
                result = await api.getTrends(days);
            } catch {
                result = null;                       // <-- API error silently swallowed
            }
            if (result && result.trends && result.trends.length > 0) {
                setData(result);
            } else {
                setData(generatePlaceholderData());  // line 693 — empty/short response
            }
        } catch (err) {
            setError(err.message);
            setData(generatePlaceholderData());      // line 697 — fetch error
        }
```

This is the single worst item in the PWA. Eleven fully fabricated trend objects carry invented headlines ("Tech Sector Death Cross", "Regime Shift: GROWTH -> FRAGILE", "BTC-NASDAQ Decoupling"), invented price levels ("VIX (22.4) trading above VIX3M (19.8)"), invented dates ("first time since October 2023"), and invented `strength`/`confidence` values (0.45–0.72) that drive the Bullish/Bearish/Avg-Strength score row, the category cards, the trend cards and the narrative panel. The `days` lookback selector re-runs the same fabrication.

Aggravating factor: the **inner** `catch` at line 686 sets `result = null` and never rethrows, so an API failure falls through to line 693 and the user sees fabricated trends with **no error banner at all** — the honest `setError` path at line 697 is only reachable if something outside `api.getTrends` throws. The comment at line 52 claims "used until API is wired", but `api.getTrends` (`api.js:1076` → `/api/v1/intelligence/trends`) and the backend route (`api/routers/intelligence_actors.py:871`) both exist, so this is stale scaffolding, not a pre-wiring stub. Git history confirms: `75c01ff1 chore: stub TrendTracker + IntelDashboard (agents building full versions)` — IntelDashboard was later cleaned (PR #419), TrendTracker never was.

**Honest replacement:** delete `generatePlaceholderData()`; on empty result render an empty state ("No trends detected for this lookback") and on fetch error surface `ErrorState` with a retry, letting the inner catch propagate so the error is actually shown.

---

### Finding 2 — RiskMap fabricates a 30-day random-walk risk history on every successful load

**File:** `pwa/src/views/RiskMap.jsx:571-592`, rendered at `:1039`
**Route/View:** `#risk` — "Risk", primary tab bar → "RISK TIMELINE (30D)" card
**Category:** 1 FABRICATED FALLBACK + 4 HARDCODED VALUES PRESENTED AS OBSERVATIONS
**Severity:** HIGH · **Confidence:** CONFIRMED

```js
        // Generate synthetic timeline from current snapshot (in a real system
        // this would come from stored historical risk scores)
        const now = new Date();
        ...
            for (let i = 29; i >= 0; i--) {
                const d = new Date(now);
                d.setDate(d.getDate() - i);
                val = Math.max(0.05, Math.min(0.95, val + (Math.random() - 0.5) * 0.12));
                points.push({ date: d, value: val });
            }
            // Pin last point to actual
            points[points.length - 1].value = currentScore;
```

Worse than a fallback: this runs on every **successful** `api.getRiskMap()` load. Six risk categories x 29 fabricated daily points get real calendar dates on a `%b %d` time axis, a "CONVERGENCE" shaded band computed off the fake series, hover tooltips giving per-category Low/Moderate/High/Critical readings for those invented days, and a click handler (`:1040-1044`) that stores a fabricated point in `timelineTooltip` and presents it as a selected historical date. Only the final point is pinned to the real current score. The code comment states outright that real history is not available.

**Honest replacement:** fetch stored historical risk scores from a backend history endpoint and render the timeline only when that series exists; otherwise show "No 30-day history available yet" and keep only the real current-snapshot cards.

---

### Finding 3 — TrendTracker draws a `Math.random()` 12-month strength history for *real* API trends too

**File:** `pwa/src/views/TrendTracker.jsx:552-570`
**Route/View:** `#trends` → "Comparison" tab → "TREND STRENGTH OVER TIME"
**Category:** 4 HARDCODED VALUES PRESENTED AS OBSERVATIONS
**Severity:** HIGH · **Confidence:** CONFIRMED

```js
        // Generate synthetic strength trajectory for each trend
        visible.forEach((trend, vi) => {
            const baseStrength = trend.strength;
            ...
                if (trend.direction === 'bullish') {
                    val = baseStrength * (0.2 + 0.8 * progress) + (Math.random() - 0.5) * 0.08;
                } else if (trend.direction === 'bearish') {
                    val = baseStrength * (0.3 + 0.7 * progress) + (Math.random() - 0.5) * 0.08;
```

Distinct from Finding 1 and independently harmful: this runs on whatever `trends` array is in state, so **even when the API returns genuine trends** the comparison chart invents a 12-month trajectory for each one. Rendered as a D3 line+area under real calendar-style labels `['12m','11m',…,'Now']` (line 543) and the section title "TREND STRENGTH OVER TIME" (line 998). A user reads it as measured history; it is noise anchored to the current value.

**Honest replacement:** fetch a real historical strength series and render the chart only when it exists; otherwise drop the line/area and show only the current strength value.

---

### Finding 4 — ActorProfileDrawer reverse-engineers "confidence stack" multipliers from frontend constants

**File:** `pwa/src/components/ActorProfileDrawer.jsx:126-184`, rendered at `:265`
**Route/View:** ActorProfileDrawer → Overview tab → "CONFIDENCE STACK (7d, {TICKER})" panel (opens from SectorDive ownership/power network and canvas actor selection)
**Category:** 2 PLACEHOLDER METRIC
**Severity:** HIGH · **Confidence:** CONFIRMED

```js
    const multipliers = [
        { label: "catalyst", value: data.catalyst_proximity,
          factor: 1 - 0.5 * (data.catalyst_proximity || 0), ... },
        { label: "liquidity", value: data.liquidity_level_percentile / 100,
          factor: data.liquidity_state === "EXPANSION_STRONG" ? 1.20
                : data.liquidity_state === "EXPANSION" ? 1.10
                : data.liquidity_state === "TIGHTENING" ? 0.85
                : data.liquidity_state === "CRISIS" ? 0.60 : 1.00, ... },
```

The panel presents itself as the model's confidence decomposition, but every `factor` is computed client-side from arbitrary coefficients (`0.5`, `0.4`, `0.05`, `1.20/1.10/0.85/0.60`, `0.80`, `0.85/1.10/1.05`) that the backend never sent. Each renders as `×{factor.toFixed(2)}` (line 265) with a bar whose width also comes from hardcoded `maxAmplify`/`maxDampen` caps (`0.20`, `0.50`, `0.40`, `0.15`, `0.10`). The API supplies the *inputs* (`catalyst_proximity`, `liquidity_state`, `fci_score`, …) but not these multipliers, so the user is shown seven invented numbers presented as the SWEEP model's actual per-factor contributions.

**Honest replacement:** render this panel only if the payload includes the backend's own applied `multipliers[].factor` decomposition; otherwise drop it and show just the real `confidence` / `confidence_lower` / `confidence_upper` already in the payload.

---

### Finding 5 — TenYearPortfolio's "Planner chat" is a local keyword matcher presented as an AI assistant

**File:** `pwa/src/views/TenYearPortfolio.jsx:99-119`, wired at `:399-407`, rendered at `:723-730`
**Route/View:** `#ten-year` — "10-Year", primary tab → Planner chat card
**Category:** 4 HARDCODED VALUES PRESENTED AS OBSERVATIONS
**Severity:** HIGH · **Confidence:** CONFIRMED

```js
function plannerReply(text, workbookPlan, activeProfile) {
    const lower = text.toLowerCase();
    if (lower.includes('monte') || lower.includes('risk')) { ... }
    if (lower.includes('upload') || lower.includes('excel')) { ... }
    ...
    return 'Queued for the planner: I will fold that into the weekly prompt, ...';
}
```

```js
        const reply = plannerReply(text, workbookPlan, activeProfile);
        setChatMessages(messages => [ ...messages.slice(-5),
            { role: 'user', text }, { role: 'assistant', text: reply } ]);
```

No network call is made. Replies are `if/else` string literals rendered in an `assistant` bubble with a `<Bot>` icon, and the catch-all reply ("Queued for the planner: I will fold that into the weekly prompt…") asserts work that nothing performs. On the 10-Year page — the app's default landing tab — this reads as a live planning agent.

**Honest replacement:** wire it to a real backend chat endpoint, or relabel the card "Quick reference", drop the bot icon and the `assistant` role, and remove the "queued"/"I will" phrasing.

---

### Finding 6 — Fabricated TrendTracker data is stamped with a fresh `new Date()`

**File:** `pwa/src/views/TrendTracker.jsx:222`, rendered at `:1089-1093`
**Route/View:** `#trends` footer
**Category:** 3 MISLEADING PROVENANCE · **Severity:** MED · **Confidence:** CONFIRMED

```js
    return { trends, category_summaries, narrative, generated_at: new Date().toISOString() };
```

```jsx
            {data.generated_at && (
                <div style={{...}}>Generated: {formatDateTime(data.generated_at)}</div>
```

The placeholder payload self-certifies as generated "just now", which is what makes Finding 1 convincing rather than obviously stale.

**Honest replacement:** never stamp client-synthesized data with `new Date()`; show a "Generated" timestamp only from the API payload.

---

### Finding 7 — PipelineHealth freshness bar width is a 3-value constant lookup

**File:** `pwa/src/views/PipelineHealth.jsx:375-379`, rendered at `:472-475`
**Route/View:** `#pipeline-health` — "Pipeline", drawer, Operations group
**Category:** 2 PLACEHOLDER METRIC · **Severity:** MED · **Confidence:** CONFIRMED

```js
    const freshnessPct = (src) => {
        if (src.freshness === 'green') return 100;
        if (src.freshness === 'yellow') return 55;
        return 15;
    };
```

Rendered as a quantified progress bar in the sources table. Two "green" sources with very different real ages render identical 100%-width bars, so bar length reads as a measured freshness quantity it is not.

**Honest replacement:** drive the width from the real age/staleness value (e.g. `src.age_hours` against the source's threshold), or drop the bar and keep only the categorical freshness badge the payload already provides.

---

### Finding 8 — "Oracle heartbeat is live" canned greeting

**File:** `pwa/src/views/TenYearPortfolio.jsx:51-56`, rendered at `:724-729`
**Route/View:** `#ten-year` → first chat bubble
**Category:** 3 MISLEADING PROVENANCE · **Severity:** MED · **Confidence:** CONFIRMED

```js
const INITIAL_CHAT_MESSAGES = [
    { role: 'assistant',
      text: 'Oracle heartbeat is live. The $1M 10-year prompt, Monte Carlo ranges, workbook intake, exports, and API checklist are staged on this page.' },
];
```

Asserts a live backend "Oracle" process that the frontend never contacts.

**Honest replacement:** neutral static copy that does not claim liveness.

---

### Finding 9 — "Heartbeat {time}" is a local 10-second `setInterval`, not a backend signal

**File:** `pwa/src/views/TenYearPortfolio.jsx:331-334`, rendered at `:578-581`
**Route/View:** `#ten-year` → "Dad Method Oracle" panel header
**Category:** 3 MISLEADING PROVENANCE · **Severity:** MED · **Confidence:** CONFIRMED

```js
    const timer = window.setInterval(() => setHeartbeatAt(new Date()), 10000);
```

```jsx
                    <div className="ty-heartbeat">
                        <HeartPulse size={15} />
                        <span>Heartbeat {formatTime(heartbeatAt)}</span>
```

A pulsing-heart icon plus an always-current timestamp reads as a live health signal; it is a wall clock. It is also bumped on chat submit (`:409`), reinforcing the illusion.

**Honest replacement:** bind it to a real `/health` ping or the last successful fetch timestamp, or remove it.

---

### Finding 10 — SectorDive defaults missing provenance to `'confirmed'`

**File:** `pwa/src/views/SectorDive.jsx:1507` and `:1516`
**Route/View:** `#sector-dive/{sector}` → Intelligence Feed
**Category:** 3 MISLEADING PROVENANCE · **Severity:** LOW · **Confidence:** CONFIRMED

```js
                confidence: t.confidence || 'confirmed',
```

Applied to both `insider` and `congress` rows. Drives a colored confidence dot and its `title` tooltip, asserting the strongest provenance tier when the payload supplied none. Underlying row data (ticker, actor, action, date) is real.

**Honest replacement:** default to `null`/`'unknown'` and render a neutral gray dot with a "confidence not reported" tooltip.

---

### Finding 11 — Always-green status dot on the global "Ask GRID" panel

**File:** `pwa/src/components/ChatPanel.jsx:121-125`, rendered at `:528-530`
**Route/View:** global floating chat panel, reachable from every view
**Category:** 3 MISLEADING PROVENANCE · **Severity:** LOW · **Confidence:** CONFIRMED

```js
    headerDot: {
        width: '8px', height: '8px', borderRadius: '50%',
        background: colors.green, display: 'inline-block',
    },
```

Unconditionally green — no code path changes its colour — so it reads "online" even after the last `askGRID` call errored.

**Honest replacement:** bind the colour to the last request's success/failure, or remove the dot.

---

### Finding 12 — "TICKER IMPACT" is a hand-picked static map, not a computed relationship

**File:** `pwa/src/views/CrossReference.jsx:224-243`, rendered at `:1436-1455`
**Route/View:** `#cross-reference` — "Truth", primary tab → matrix cell detail panel
**Category:** 4 HARDCODED VALUES PRESENTED AS OBSERVATIONS · **Severity:** LOW · **Confidence:** CONFIRMED

```js
const TICKER_IMPACT = {
    'GDP|China': ['FXI', 'KWEB', 'EEM', 'BABA', 'HG', 'FCX'],
    'GDP|US': ['SPY', 'QQQ', 'IWM', 'DIA'],
```

No fabricated numbers, but the section heading "TICKER IMPACT" implies a computed causal link the code does not compute. Covers only 13 of 45 category|region combos; the rest correctly render nothing.

**Honest replacement:** relabel "RELATED TICKERS (curated)", or derive the list from the check's actual source-series metadata.

---

### Finding 13 — Literal "37+ data sources" inside an otherwise live paragraph

**File:** `pwa/src/views/Regime.jsx:291`
**Route/View:** `#regime` — "Regime", drawer → ACTION tab → "REGIME EXPLAINED" card
**Category:** 4 HARDCODED VALUES PRESENTED AS OBSERVATIONS · **Severity:** LOW · **Confidence:** CONFIRMED

```jsx
                    GRID analyzes <strong style={{ color: '#C8D8E8' }}>37+ data sources</strong> across
                    economics, markets, sentiment, and alternative data to classify the current market
```

The surrounding paragraph interpolates real `regime.*` fields, so a reader cannot distinguish the literal count from the live figures beside it.

**Honest replacement:** drop the specific count, or render the real source total the backend already exposes (surfaced as `features_total` in Settings/Operator).

---

## Verification of earlier PRs

All previously-cleaned files were re-read in full and are **confirmed clean**:

| PR | File | Verdict |
|---|---|---|
| #419 | `views/CorrelationMatrix.jsx` (739 ln) | **Clean.** All data from `api.getDiscoveryCorrelationMatrix()`. Notably `ScatterPlot` (`:117-186`) joins the two series on `obs_date` rather than array index, with an inline comment explaining that index pairing would plot points that never co-occurred, and falls back to an honest `'no-overlap'` state — exactly the category-3 anti-pattern, handled correctly. |
| #419 | `views/IntelDashboard.jsx` (434 ln) | **Clean.** Every card and detail reads from `api.getTrustScores` / `getConvergenceAlerts` / `getCrossReference` / `getLatestBriefing` / `getSpiderStats`; honest empty states. The `'LIVE'` badge at `:176` is gated on `briefing` being truthy, else `'--'`. |
| #446/#457 | `views/CrossReference.jsx` (1719 ln) | **Clean of the fabrication pattern.** Ungapped cells are explicitly filled `classification: 'noData'` with "Not available yet" copy (`:292-312`); the file header comment at `:9` states the rule. Only the LOW `TICKER_IMPACT` nit (Finding 12) remains. |
| #434/#447 | `useAsyncData` / `LoadingSkeleton` / `ErrorState` batches | **Clean.** `hooks/useAsyncData.js` is honest infrastructure; 25 views/components adopt the helpers. Views that do not import them (e.g. `Signals`, `TPS`, `Surfacer`, `EdgeScanner`) were each verified to implement equivalent inline honest empty/error states. |
| #432 | (files folded into the above sets) | No residue found. |

`views/RegimeAnalog.jsx` (555 ln) was specifically checked for hardcoded historical analog events and z-scores — **none found**; `matches.episodes`, `forecast.outcomes`, `regime.axes`, `timesfm.forecasts` all come from `api.getRegimeAnalogs(20)`.

`components/flows/FreshnessIndicator.jsx` was specifically checked — its `'LIVE'` label (`:9`) is a display synonym for the API's `confirmed` classification, and all callers (`JunctionDashboard.jsx:85,114`, `FlowTooltip.jsx:38,47`) pass `confidence` straight from the payload. **Not a finding.**

`api.js` (1585 ln) was read in full: on network/HTTP error it returns `{ error: true, status, message }` and **never** synthesizes a fallback payload or derives values the server did not send. `Math.random()` at `:1375` is retry-backoff jitter.

**Fixture imports:** no production file in scope imports from `__tests__`, `*.test.*`, `__mocks__`, or a fixture module (verified by grep across all 144 files). The only cross-reference is the reverse direction — `pwa/src/__tests__/dashboard.test.jsx` mocking `StatusDot.jsx`, which is normal.

---

## Ruled-out items

Considered and excluded — not fabrication:

- **Force-layout / particle seeding `Math.random()`** — `ActorUniverse.jsx:576-580` (3D shell scatter), `InfluenceNetwork.jsx:84`, `AttentionRadar.jsx:470-471`, `BubbleUniverse.jsx:95-96`, `CanvasStore.js:546-547` (node x/y position fallback), `SendToCanvas.jsx:59-60` (drop placement), `AppArchitecture.jsx:250-253` (particle phase/speed/size), `canvas_lenses/CapitalLens.jsx:371`. All seed layout/animation; the physics sim resolves positions and every rendered size/colour/label comes from real fields.
- **Animation selection `Math.random()`** — `ActorNetwork.jsx:950,953,992,1006` and `PowerMap.jsx:310,332`: pick *which already-fetched real flow record* animates next, and jitter transition duration. No amounts, tickers or dates invented.
- **Loading-skeleton shimmer** — `TrialGems.jsx:118` (`width: ${60 + Math.random() * 35}%`): randomized skeleton bar widths in a loading placeholder, never a data value.
- **Retry jitter** — `api.js:1375`.
- **Colour / enum / label maps** — `nodeStyles.js` `NODE_COLORS` / `EDGE_TYPE_COLORS` / `EDGE_RELATIONSHIP_COLORS` (header cites the backend source of truth), `RISK_COLORS`, `CATEGORY_LABELS`, `LEVEL_EXPLANATIONS`, `TIER_COLORS`, `FLOW_COLORS`, `REGIME_COLORS`, `GRADE_COLORS`, `CONF_OPACITY`, `DIVERGENCE_COLORS`, `VERDICT_COLORS`, `SCORE_COLORS`, and the `zToColor` / `corrToColor` / `stressColor` / `perfColor` / `scoreColor` threshold ramps.
- **Static nav / UI config** — `routes.js`, `DadNav.jsx` tabs, `ContextMenu.jsx` menus, `WidgetManager.jsx` `WIDGET_CATALOG`, `LayerControls.jsx` `LAYERS`, `CommandPalette.jsx` `QUICK_COMMANDS`, `app.jsx` preload path lists, `Archive`/`MoneyFlow`/`AssociationsLegacy` `TABS`, `GeoFlows`/`Timeline` `PERIODS`, `IntelSubmit` `INTEL_TYPES`, `Vault`/`Knowledge` filter enums.
- **Documentation / help / tour copy** — `ViewHelp.jsx` `HELP`, `Onboarding.jsx` `STEPS`, `Backtest.jsx:474-506` portfolio-constraint policy tables (static policy text, no live-data styling — worth a separate check that these constants have not drifted from the backend risk config, but not a fabrication), `TenYearPortfolio` `SIMPLE_PROFILE_BLURBS` / `DEFAULT_PLAN_STEPS` (labeled "Ready checklist").
- **Documented curated maps** — `CorrelationMatrix.jsx` `RAW_FEATURE_NAME_BY_DISPLAY`, `MomentumSparks.jsx` ticker aliases, `MarketPulse.jsx` `INVERSE_SIGNALS` / `FAMILY_LABELS`, `GrandLoop.jsx` `LOOP_NODES` / `LOOP_EDGES` (topology definitions; all rendered values come from `useFlowLayers()` and nodes without live data are skipped), `CrossReference.jsx` `DATA_GAPS` (rendered in a "Data Gaps" tab and honestly labelled as sources ingested-but-not-yet-cross-referenced).
- **Chart axis / scale defaults** — d3 `.domain()` / `.ticks()` computed from the real data's own min/max throughout; `Flows.jsx:76` `InfluenceBar` `value * 100 * 3` visual scaling (the true percentage is printed as text immediately beside it); `VannaCharmViz.jsx` linear OpEx decay projection (a disclosed deterministic function of the real `charm_exposure` field, labelled "Decay Timeline").
- **Honest empty/error copy** — `SweepPanel.jsx` ("No persisted sweep at this horizon yet…"), `SupplyLens` / `CapitalLens` (both explicitly detect `data?.provenance?.source === 'fallback'` and show "not mapped yet" instead of drawing nodes), `RiskView.jsx` ("Conviction scorer unavailable"), `Signals.jsx` ("awaiting ingestion module"), `EdgeScanner.jsx` ("No synthetic placeholders" for withheld sectors), `TPS.jsx` (header comment: "No fake metrics: rows with score == null render a 'low coverage' badge"), `Hyperspace.jsx` ("Could not load logs"), `EmptyState.jsx`, `ErrorState.jsx`, `IntelSubmit`'s `useAsyncData(..., { fallback: [] })`.
- **Input `placeholder=` text** throughout.
- **Display-threshold bucketing of real values** — `SectorDive.jsx:1498` (`>= 0.6 ? 'derived' : 'inferred'` on a real numeric confidence), `ConfidenceMeter.jsx` colour thresholds, `RegimeThermometer.jsx` `regimeToPosition`.
- **Dead / unreachable code (not user-visible)** — `components/FearGreedGauge.jsx` (178 ln) blends VIX / put-call / regime confidence with arbitrary hardcoded weights and *would* be a HIGH placeholder metric, but is **never imported anywhere** in `pwa/src`; `components/LivingGraph.jsx` (478 ln) likewise never imported; `CrossReference.jsx` `officialTrend` / `physicalTrend` / `historicalAnalog` are read in JSX (`:1386-1409`, `:1476-1480`) but never written anywhere, so the sparkline/divergence/analog blocks never render; `useWebSocket.js:17` `lastMessage` is always null and never consumed. **Flagged so they are not accidentally wired up later.**
- **Non-fabrication nits noted in passing (out of scope)** — `flows/FlowSankey8.jsx:115` references an undefined `globalIdx`; `DashboardFlows.jsx:388` comment says "trust score dot" but the dot encodes `d.acceleration` (no label reaches the screen, so not a mislabel finding).

---

## Files reviewed

**144 files / 72,945 lines — all read in full, top to bottom.** No file was skipped, truncated, or grep-only. Line counts via `wc -l`.

**views/ (74 files)**
`ActorNetwork.jsx` 2081 · `ActorUniverse.jsx` 1360 · `Agents.jsx` 477 · `AppArchitecture.jsx` 847 · `Archive.jsx` 418 · `Associations.jsx` 776 · `AssociationsLegacy.jsx` 1028 · `AttentionRadar.jsx` 749 · `Backtest.jsx` 511 · `Briefings.jsx` 563 · `Canvas.jsx` 4 · `CatalystTimeline.jsx` 563 · `CorrelationMatrix.jsx` 739 · `CrossReference.jsx` 1719 · `Dashboard.jsx` 594 · `Discovery.jsx` 463 · `EarningsCalendar.jsx` 474 · `EdgeScanner.jsx` 1501 · `Flows.jsx` 918 · `GeoFlows.jsx` 399 · `Globe.jsx` 735 · `GlobeView.jsx` 1 · `Heatmap.jsx` 498 · `Home.jsx` 388 · `Hyperspace.jsx` 179 · `InfluenceNetwork.jsx` 677 · `IntelDashboard.jsx` 434 · `IntelModeration.jsx` 193 · `IntelSubmit.jsx` 280 · `IntelligenceSearchView.jsx` 313 · `Journal.jsx` 169 · `JournalEntry.jsx` 213 · `Knowledge.jsx` 386 · `LeverMap.jsx` 836 · `Login.jsx` 264 · `MarketDiary.jsx` 592 · `MilestoneTracker.jsx` 809 · `Models.jsx` 218 · `MoneyFlow.jsx` 141 · `Operator.jsx` 467 · `Options.jsx` 1028 · `Physics.jsx` 609 · `PipelineHealth.jsx` 572 · `Portfolio.jsx` 513 · `Predictions.jsx` 821 · `Regime.jsx` 620 · `RegimeAnalog.jsx` 555 · `RiskMap.jsx` 1083 · `RiskView.jsx` 468 · `SectorDive.jsx` 1793 · `Settings.jsx` 1103 · `Signals.jsx` 431 · `Snapshots.jsx` 285 · `SpiderStats.jsx` 266 · `Strategies.jsx` 576 · `Strategy.jsx` 201 · `Surfacer.jsx` 1215 · `SystemLogs.jsx` 230 · `TPS.jsx` 356 · `TenYearPortfolio.jsx` 1678 · `Thesis.jsx` 613 · `TickerLookup.jsx` 1309 · `Timeline.jsx` 1282 · `TrendTracker.jsx` 1096 · `TrialGems.jsx` 549 · `Valuation.jsx` 652 · `Vault.jsx` 257 · `WatchlistAnalysis.jsx` 1524 · `WeightSliders.jsx` 425 · `WhyView.jsx` 1137 · `Workflows.jsx` 216 · `canvas_lenses/CapitalLens.jsx` 668 · `canvas_lenses/SupplyLens.jsx` 394 · `mobile/MobileDashboard.jsx` 724

**components/ (52 files)**
`ActorProfileDrawer.jsx` 2021 · `CapitalFlowAnalysis.jsx` 608 · `CapitalFlowSankey.jsx` 465 · `ChartControls.jsx` 153 · `ChatPanel.jsx` 647 · `CommandPalette.jsx` 511 · `ConfidenceMeter.jsx` 33 · `DadNav.jsx` 114 · `DashboardFlows.jsx` 579 · `DecisionModal.jsx` 56 · `EgoGraph.jsx` 493 · `EmptyState.jsx` 67 · `ErrorState.jsx` 51 · `FearGreedGauge.jsx` 178 · `FlowTimeline.jsx` 586 · `GEXProfile.jsx` 654 · `IntelligenceSearch.jsx` 509 · `KillSwitch.jsx` 78 · `LessonsWidget.jsx` 225 · `LivingGraph.jsx` 478 · `LoadingSkeleton.jsx` 92 · `MarketPulse.jsx` 269 · `MomentumSparks.jsx` 120 · `NavBar.jsx` 506 · `Onboarding.jsx` 537 · `PowerMap.jsx` 475 · `PriceChart.jsx` 394 · `RegimeCard.jsx` 77 · `RegimeThermometer.jsx` 141 · `SendToCanvas.jsx` 178 · `SignalCard.jsx` 64 · `StatusDot.jsx` 29 · `TimeframeComparison.jsx` 155 · `TransitionGauge.jsx` 40 · `VannaCharmViz.jsx` 531 · `ViewErrorBoundary.jsx` 97 · `ViewHelp.jsx` 310 · `WidgetManager.jsx` 255 · `flows/BubbleUniverse.jsx` 220 · `flows/CDSDashboard.jsx` 192 · `flows/FlowSankey8.jsx` 385 · `flows/FlowTooltip.jsx` 68 · `flows/FlowWaterfall.jsx` 219 · `flows/FreshnessIndicator.jsx` 55 · `flows/GrandLoop.jsx` 402 · `flows/JunctionCard.jsx` 90 · `flows/JunctionDashboard.jsx` 151 · `flows/OrthogonalityView.jsx` 190 · `flows/useFlowData.js` 161 · `home/answerFormat.jsx` 138 · `home/plain.js` 60 · `home/widgets.jsx` 397

**hooks/ (6 files)**
`useAsyncData.js` 82 · `useDevice.js` 57 · `useEventStream.js` 104 · `useForceLayout.js` 178 · `useFullScreen.js` 114 · `useWebSocket.js` 22

**canvas/ (12 files)**
`CanvasStore.js` 667 · `CommunityHulls.jsx` 266 · `ContextMenu.jsx` 205 · `GothamCanvas.jsx` 1605 · `LayerControls.jsx` 113 · `SigmaGraph.jsx` 267 · `TemporalScrubber.jsx` 407 · `nodeStyles.js` 226 · `hooks/useCommunities.js` 150 · `hooks/useKeyboardShortcuts.js` 142 · `panels/DetailPanel.jsx` 1238 · `panels/SweepPanel.jsx` 154

**root (2 files)**
`api.js` 1585 · `app.jsx` 633

### Files NOT fully reviewed

None. Every file in scope was read end to end.

**Read but out of declared scope** (consulted for context only, no findings drawn): `pwa/src/routes.js` (route/view name resolution), `pwa/src/utils/interpret.js` and `pwa/src/utils/formatTime.js` (checked for fabricated constants leaking into in-scope views — both are pure null-guarded formatters, clean), `api/routers/intelligence_actors.py:871` (confirming the `/trends` endpoint exists, for Finding 1).
