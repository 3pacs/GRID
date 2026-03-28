# GSD Plan — Next Sprint

**Date:** 2026-03-28
**Focus:** Watchlist as the primary operator interface
**Principle:** Every feature serves one buyer decision: "What should I do with this position?"

---

## North Star

The watchlist is the operator's daily driver. When you open GRID, the watchlist
shows your positions and candidates with live prices, AI context, and actionable
flow data. Clicking a ticker gives you everything you need to decide: hold, add,
trim, or exit.

---

## Phase A: Watchlist Core (PARTIALLY DONE)

What shipped tonight:
- [x] Live yfinance price fallback when DB data is stale
- [x] Click ticker → detail page with AI overview + sentiment
- [x] Capital flow path (Market → Sector → Subsector → Ticker)
- [x] Delete button on each card
- [x] Routing wired in app.jsx

### Remaining (HIGH priority)

**A1. Add ticker search + autocomplete**
- The add-ticker input on Dashboard is a raw text field
- Need: search-as-you-type against yfinance, show name + asset type
- On select: auto-fills ticker, display_name, asset_type

**A2. Price refresh on load**
- Enriched watchlist should batch-fetch live prices for all tickers on page load
- Not one-by-one — use yfinance.download() with a list of tickers
- Cache for 5 minutes to avoid hammering yfinance

**A3. Watchlist ordering**
- Allow drag-to-reorder or sort by: change %, sector, added date
- Persist order in DB (add `position` column to watchlist table)

**A4. Quick actions from list view**
- Swipe or long-press on mobile → quick action sheet (remove, notes, alert)
- Desktop: hover reveals action buttons

---

## Phase B: Ticker Detail Page (HIGH priority)

The detail page is the money view. When you click a ticker, this is what
convinces you to act or wait.

**B1. AI Overview improvements**
- Current: single LLM call with rule-based fallback
- Need: structured sections (Price Action | Options Flow | Sector Context | Risk)
- Include: "What changed since last time you looked" if we have prior views
- Streaming response for perceived speed

**B2. Interactive price chart**
- Current: 90-day sparkline from resolved_series
- Need: D3 candlestick or line chart with:
  - Timeframe toggle (1W, 1M, 3M, 6M, 1Y)
  - Volume bars
  - Key levels overlay (max pain, support/resistance from options)
  - Regime bands (colored background by market regime)

**B3. Capital flow deep dive**
- Current: simple SVG path showing hierarchy
- Need: animated flow showing relative weight changes over time
- Show: peer comparison within subsector (e.g., TSM vs other semis)
- Link to full Sankey view filtered to this sector

**B4. Options intelligence card**
- Current: raw numbers (PCR, IV, max pain)
- Need: visual positioning chart (spot vs max pain vs key strikes)
- Unusual activity detection (OI spikes, IV skew changes)
- "What the options market is saying" — one-sentence LLM interpretation

**B5. Related signals**
- Show correlated features that moved recently
- Highlight: "BTC moved +3% yesterday → historically this leads XLK by 2 days"
- Pull from hypothesis_registry PASSED hypotheses

---

## Phase C: Server Health + Data Quality (MEDIUM priority)

Issues found by GSD scan tonight:

**C1. Fix active server errors**
- [ ] Eurostat pulling 0/3 series — investigate endpoint change
- [ ] server_log git branch divergence — already fixed by re-clone
- [ ] Missing API keys: KOSIS, COMTRADE, USDA_NASS, GDELT

**C2. Clean up server disk**
- [ ] Delete `grid_repo_old` (29 GB) — all content now in grid_repo
- [ ] Delete loose RTF files on server home directory
- [ ] Rotate the exposed GitHub PAT in git remote URLs

**C3. Data coverage push**
- Macro at 57%, FX at 75% — tonight's fixes help but need:
- [ ] Run the updated scheduler to ingest new FRED series + FX pairs
- [ ] Verify seed_v2 features actually insert (schema CHECK constraint)
- [ ] Wire entity_map for new FRED series → feature_registry names
- [ ] Run NYFedPuller manually to confirm GDP nowcast data flows

**C4. Test the full pipeline on server**
- [ ] Trigger a manual ingestion run for new sources
- [ ] Verify resolved_series gets new data
- [ ] Check watchlist prices update from new sources

---

## Phase D: Polish + Ship (LOWER priority)

**D1. PWA build + deploy**
- Build pwa with `npm run build`
- Deploy pwa_dist to server
- Verify watchlist works on mobile (touch targets, responsive)

**D2. Update CLAUDE.md**
- Add watchlist architecture notes
- Update phase status (13 in progress, 14 partially done)
- Document new endpoints

**D3. Notification system for watchlist**
- Price alert thresholds per ticker
- Regime change alerts
- Options unusual activity alerts
- Delivery: PWA push notification + email

---

## Execution Order

```
NOW (this session):
  A1  — Ticker search/autocomplete
  A2  — Batch price refresh
  B2  — Interactive price chart
  C2  — Server disk cleanup

NEXT SESSION:
  B1  — AI overview improvements
  B3  — Capital flow deep dive
  B4  — Options intelligence card
  C1  — Fix server errors
  C3  — Data coverage verification

LATER:
  A3  — Watchlist ordering
  A4  — Quick actions
  B5  — Related signals
  C4  — Pipeline test
  D1-D3 — Polish
```

---

## Constraints

- Codex agent owns `astrogrid/`, `astrogrid_web/`, `astrogrid_shared/` — do not touch
- Check `.coordination.md` before and after every commit
- All backend changes must have tests or at minimum not break the 552-test suite
- yfinance calls must be rate-limited (batch, cache 5 min)
- LLM calls must degrade gracefully (rule-based fallback always)
