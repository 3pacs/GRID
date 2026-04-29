# TODO: 200-Signal Catalog (parked for tomorrow)

**Status:** Deferred to next session
**Requested:** 2026-04-13
**Owner:** session branch `claude/review-cryexc-app-T2icl`

## Objective

Brainstorm 200 candidate signals (pullers + intelligence modules) that each
plausibly deliver ≥1% certainty gain on a GRID trade. Expected hit rate after
falsification testing: 10-25%, yielding 20-50 shipped signals — would roughly
double GRID's active signal surface.

## Scope

- **~100 new data sources (pullers)** — concrete APIs/scrapes/feeds to ingest
- **~100 new intelligence modules** — analytics/inference engines built on top
  of existing + new data
- **No constraint** to use only existing 48 pullers / 14 intelligence modules.
  The universe is wide open.

## Required fields per entry

1. ID, Name, Type (Puller/Intel/Hybrid)
2. Domain (macro/positioning/flows/sentiment/etc.)
3. One-line description
4. Why it plausibly gives ≥1% (lever named, coverage estimate)
5. Lever vs Condition classification (per GRID Prediction Causation Standard)
6. PIT feasibility (easy/tricky/hard — with reason)
7. Source (API, scrape, paid, free, OSINT)
8. Build cost (S/M/L)
9. Confidence tier (A = ship after quick test, B = needs falsification,
   C = speculative)
10. Overlap flag with existing GRID modules

## Today's partial output (shortlist — ~40 entries)

See session message in branch `claude/review-cryexc-app-T2icl` with subject
"what you'd pull and which intelligence would bring a 1% gain" —  contains the
high-conviction Tier A picks across pullers and intelligence. Use as the
starting seed for tomorrow's full 200.

## Structure for tomorrow

```
.planning/signals/
├── CATALOG.md              (index, scoring rubric, tiering)
├── PULLERS.md              (~100 new data sources)
├── INTELLIGENCE.md         (~100 new analytics engines)
└── SHORTLIST-TIER-A.md     (highest-conviction subset, ship order)
```
