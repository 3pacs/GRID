# Shared Read Contract: GRID → AstroGrid

AstroGrid reads from GRID through these explicit API endpoints only.
AstroGrid NEVER writes to GRID tables. AstroGrid stores derived state in `astrogrid.*`.

---

## Contract v1 — Market/Regime/Flow Overlays

### 1. Regime State
```
GET /api/v1/regime/current
→ {state, confidence, drivers, posture, timestamp}
```
AstroGrid uses this to overlay market regime on celestial views.

### 2. Unified Thesis
```
GET /api/v1/intelligence/thesis
→ {overall_direction, conviction, key_drivers, risk_factors, theses[], narrative}
```
AstroGrid feeds this into Oracle/Atlas as the "market voice."

### 3. Capital Flows (aggregated)
```
GET /api/v1/flows/money-map
→ {layers[], flows[], intelligence{}, levers[], global_liquidity{}, global_policy{}}
```
AstroGrid maps sector flows to celestial bodies/sectors.

### 4. Sector Flows
```
GET /api/v1/flows/aggregated?days=30&period=weekly
→ {by_sector{}, by_actor_tier{}, rotation_matrix{}, time_series[]}
```
For cross-sector rotation overlays.

### 5. Sector Map
```
GET /api/v1/flows/sectors
→ {sectors: {SectorName: {etf, etf_z, sector_stress, actors[], subsectors[]}}}
```
AstroGrid uses this for canonical sector and actor context.

### 6. Sector Detail
```
GET /api/v1/flows/sectors/{sector_name}/detail
→ {sector, etf, price, change_1m, subsectors{}, sector_metrics{}, intelligence{}}
```
AstroGrid uses this for top-actor, subsector, and sector-intelligence overlays.

### 7. Key Market Features
```
GET /api/v1/signals/snapshot
→ {features: [{name, family, value, obs_date, zscore}]}
```
AstroGrid maps features to celestial windows (e.g., VIX → Mars volatility).

### 8. Active Patterns
```
GET /api/v1/intelligence/patterns/active
→ [{pattern, ticker, next_expected, confidence, hit_rate}]
```
AstroGrid can overlay predicted events on celestial timeline.

### 9. Cross-Reference Red Flags
```
GET /api/v1/intelligence/cross-reference
→ {checks[], red_flags[], narrative}
```
For "truth overlay" on celestial map.

---

## Rules

1. All endpoints require JWT auth (same token as GRID)
2. AstroGrid caches responses (suggested TTL: 5-15 min)
3. If an endpoint is unavailable, AstroGrid degrades gracefully (show celestial data without market overlay)
4. GRID may add fields to responses without breaking the contract
5. GRID will NOT remove fields without 7-day deprecation notice in this doc
6. Payload changes get logged in `.coordination.md`

## Versioning

This is contract v1. Breaking changes create v2 endpoints.
Current: all endpoints are v1 (`/api/v1/...`).
