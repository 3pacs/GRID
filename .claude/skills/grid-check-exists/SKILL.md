---
name: grid-check-exists
description: "Before proposing to build a new GRID module, search the existing 405-module codebase for similar implementations. Use this whenever a session is about to propose a new intelligence module, puller, analytics engine, or oracle feature. Searches intelligence/, analysis/, physics/, features/, discovery/, trading/, oracle/, and ingestion/ for name and content matches, then cross-references docs/MODULE_CATALOG.md."
---

# grid-check-exists

**Purpose:** prevent duplicative work. CLAUDE.md documents only the 14 core intelligence modules, but the real codebase has 405 modules (46 in intelligence/, 104 in ingestion/, 16 in analysis/, plus physics/features/discovery/trading/oracle). Any session that proposes to "build a new sentiment tracker" or "build a network mapper" or "build a vol surface" is very likely duplicating existing code.

Call this skill **before** writing any new module.

## When to Use

- A task is about to propose creating a new file in `intelligence/`, `analysis/`, `physics/`, `features/`, `discovery/`, `trading/`, `oracle/`, or `ingestion/`.
- A user asks "does GRID have X?"
- You catch yourself about to write `Write` on a net-new file path in those directories.
- Before any `git commit` that introduces a new Python module in the above directories.

## Usage

```
/grid-check-exists <keyword>
```

Examples:
- `/grid-check-exists vol surface`
- `/grid-check-exists insider cluster`
- `/grid-check-exists hypothesis`
- `/grid-check-exists catalyst calendar`
- `/grid-check-exists transfer entropy`

## Execution

Run these searches in order and report findings compactly:

```bash
KEYWORD="$1"

echo "## 1. Filename matches across module directories"
ls intelligence/ analysis/ physics/ features/ discovery/ trading/ oracle/ ingestion/ ingestion/altdata/ ingestion/international/ 2>/dev/null \
  | grep -i "$KEYWORD" \
  | head -30

echo ""
echo "## 2. Grep content matches (class / def / string literal)"
grep -rIl -iE "(class|def).*${KEYWORD// /.*}|[\"']${KEYWORD}[\"']" \
    intelligence/ analysis/ physics/ features/ discovery/ trading/ oracle/ \
    2>/dev/null | head -15

echo ""
echo "## 3. MODULE_CATALOG.md mentions"
grep -inE "${KEYWORD// /.*}" docs/MODULE_CATALOG.md 2>/dev/null | head -15

echo ""
echo "## 4. Session roadmap mentions"
grep -inE "${KEYWORD// /.*}" docs/planning/SESSION-ROADMAP-2026-04-13.md 2>/dev/null | head -10

echo ""
echo "## 5. Total file counts (for sanity)"
for d in intelligence analysis physics features discovery trading oracle ingestion/altdata; do
    count=$(ls "$d"/*.py 2>/dev/null | wc -l)
    echo "  $d/: $count .py files"
done
```

## How to Interpret Results

- **Any filename match** → open the top 50 lines of the file before proposing anything new. Treat the task as "extend and wire," not "build new."
- **Content matches without filename matches** → a similar concept lives inside another module. Read that module's public API.
- **MODULE_CATALOG hits but no file matches** → the catalog may be stale; run `ls` directly to confirm.
- **Zero matches in all five sections** → genuinely new. Still grep broader terms before committing to a new file.

## Known "I almost built it but it already exists" list (from 2026-04-13 session)

Before proposing any of these, know they already exist:

| Capability | Existing location | Status |
|---|---|---|
| Vol surface / SVI / skew / butterfly checks | `analysis/vol_surface.py` | Built, not wired into scanner/recommender |
| Earnings call tone / Q&A split / guidance extraction | `intelligence/earnings_transcript_analyzer.py` | Extensible |
| LLM hypothesis generation with kill criteria | `intelligence/hypothesis_engine.py` | Extensible |
| Brier / reliability / calibration tracking | `intelligence/prediction_calibration.py` | Not persisted, not per-horizon |
| Signal inventory and backlinking | `intelligence/signal_registry.py`, `signal_backlinker.py`, `signal_extractor.py` | Reuse |
| Sector network mappers (banking/energy/pharma/defense/tech/real_estate/commodities/defi) | `intelligence/banking_network.py` et al. | Already built per sector |
| Vanna / charm computation | `physics/dealer_gamma.py:248-250` | Computed but never scored |
| Per-ticker GEX | `physics/dealer_gamma.py` | Built but assumes net-short dealers |
| Actor network (495 actors) | `intelligence/actor_network.py` | Extend with temporal decay + governance graph |
| PIT query with as_of_date + vintage policy | `store/pit.py:43-132` | Do not reinvent |
| Feature permutation importance + regime correlation | `features/importance.py` | Not per-horizon |
| Regime discovery (PCA + GMM) | `discovery/clustering.py` | Static labels; no transition matrix |
| USD flow normalization + sector/actor aggregation | `intelligence/dollar_flows.py` + `analysis/flow_aggregator.py` + `analysis/flow_thesis.py` | Extensible |

Full list in `docs/planning/SESSION-ROADMAP-2026-04-13.md#1-session-start-pre-read-read-this-first`.

## See Also

- `/grid-orient` — rebuild `.claude/CODEBASE_INDEX.md` after major changes
- `docs/MODULE_CATALOG.md` — canonical 405-module inventory
- `docs/planning/SESSION-ROADMAP-2026-04-13.md` — full session findings and known gaps
