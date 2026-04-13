#!/bin/bash
# smoke_endpoints.sh — regression gate for GRID backend deploys.
#
# Exits 0 only if every critical endpoint returns plausible data. A wave is not
# "done" until this script passes. Called by scripts/deploy.py --smoke and by
# the Hermes post-deploy hook.
#
# Usage:
#   bash scripts/smoke_endpoints.sh           # run all tests, fail fast
#   bash scripts/smoke_endpoints.sh --verbose # show every assertion
#   bash scripts/smoke_endpoints.sh --quiet   # only print failures
#
# Runs against the local grid-api process (systemd service) via direct Python
# import — bypasses HTTP auth gating since we're on the server. Catches:
#   - sector_map loading (YAML shim, 3,533 actors expected)
#   - supply_chain endpoint with chokepoint scoring
#   - capital_flow endpoint with real XBRL data + percentile enrichment
#   - contagion simulator with preset scenarios
#   - actor_detail drawer payload
#   - sector_health composite
#   - contagion → trade ticket bridge
#   - explain endpoint (hero query)
#
# Exit codes:
#   0  all endpoints passed
#   1  sector_map failed to load
#   2  supply_chain broken
#   3  capital_flow broken
#   4  contagion broken
#   5  actor_detail broken
#   6  sector_health broken
#   7  trade tickets broken
#   8  explain broken
#   9  environment setup failed (venv missing, import failure)

set -uo pipefail

VERBOSE=0
QUIET=0
for arg in "$@"; do
  case "$arg" in
    --verbose|-v) VERBOSE=1 ;;
    --quiet|-q)   QUIET=1 ;;
    -h|--help)
      grep '^# ' "$0" | sed 's/^# \?//'
      exit 0
      ;;
  esac
done

log() {
  if [[ $QUIET -eq 0 ]]; then
    echo "$@"
  fi
}
fail() {
  echo "FAIL: $1" >&2
  exit "$2"
}

# Find the GRID repo root. We might be called from anywhere.
if [[ -n "${GRID_ROOT:-}" ]]; then
  REPO="$GRID_ROOT"
elif [[ -d /data/grid_v4/astrogrid_dedup ]]; then
  REPO=/data/grid_v4/astrogrid_dedup
elif [[ -d /home/grid/grid_v4/grid_repo ]]; then
  REPO=/home/grid/grid_v4/grid_repo
else
  here="$(cd "$(dirname "$0")" && pwd)"
  while [[ "$here" != "/" ]]; do
    if [[ -f "$here/analysis/sector_map.py" ]]; then
      REPO="$here"
      break
    fi
    here="$(dirname "$here")"
  done
fi
if [[ -z "${REPO:-}" ]] || [[ ! -f "$REPO/analysis/sector_map.py" ]]; then
  fail "cannot locate GRID repo root (set GRID_ROOT env var to override)" 9
fi

VENV=""
for candidate in ~/grid_v4/venv /home/grid/grid_v4/venv /data/grid_v4/venv "$REPO/.venv"; do
  if [[ -x "$candidate/bin/python3" ]]; then
    VENV="$candidate"
    break
  fi
done
if [[ -z "$VENV" ]]; then
  fail "cannot find venv (looked for ~/grid_v4/venv and siblings)" 9
fi

log "==> GRID smoke test"
log "    repo: $REPO"
log "    venv: $VENV"

cd "$REPO"
# shellcheck disable=SC1091
source "$VENV/bin/activate"

STARTED=$(date -u +%Y-%m-%dT%H:%M:%SZ)

SMOKE_VERBOSE=$VERBOSE SMOKE_QUIET=$QUIET python3 - <<'PY'
"""Inline Python test harness. Runs every endpoint, prints per-test result."""
import asyncio
import os
import sys
import traceback

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

verbose = os.environ.get("SMOKE_VERBOSE") == "1"
quiet = os.environ.get("SMOKE_QUIET") == "1"


def info(msg):
    if not quiet:
        print(msg)


def vinfo(msg):
    if verbose:
        print("    " + msg)


def fail(code, msg):
    print(f"FAIL [{code}]: {msg}", file=sys.stderr)
    sys.exit(code)


# 1. sector_map shim loads 3533 actors
try:
    from analysis.sector_map import SECTOR_MAP, JUNCTION_POINTS
    n_sectors = len(SECTOR_MAP)
    n_actors = sum(
        len(sub["actors"])
        for s in SECTOR_MAP.values()
        for sub in s.get("subsectors", {}).values()
    )
    if n_sectors < 15:
        fail(1, f"sector_map only has {n_sectors} sectors (expected >=15)")
    if n_actors < 3000:
        fail(1, f"sector_map only has {n_actors} actors (expected >=3000)")
    info(f"[OK] sector_map: {n_sectors} sectors, {n_actors} actors, {len(JUNCTION_POINTS)} junctions")
except SystemExit:
    raise
except Exception as e:
    traceback.print_exc()
    fail(1, f"sector_map load failed: {e}")

# 2. supply_chain endpoint
try:
    from api.routers.supply_chain import get_supply_chain
    r = asyncio.run(get_supply_chain("NVDA", "both", 3, "smoke_test"))
    n = len(r["nodes"])
    e = len(r["edges"])
    ch = len(r["chokepoints"])
    if n < 20:
        fail(2, f"NVDA supply returned only {n} nodes (expected >=20)")
    if e < 20:
        fail(2, f"NVDA supply returned only {e} edges (expected >=20)")
    if r["provenance"]["source"] == "fallback":
        fail(2, "NVDA supply returned fallback (expected db)")
    vinfo(f"NVDA supply: n={n} e={e} chokes={ch}")
    info("[OK] supply_chain endpoint")
except SystemExit:
    raise
except Exception as ex:
    traceback.print_exc()
    fail(2, f"supply_chain broken: {ex}")

# 3. capital_flow endpoint with percentile enrichment
try:
    from api.routers.capital_flow import get_capital_flow
    r = asyncio.run(get_capital_flow("aapl", 4, "annual", "smoke_test"))
    periods = r["periods"]
    if not periods:
        fail(3, "AAPL capital_flow returned zero periods")
    rev = periods[0]["totals"]["inflow_usd"]
    if rev < 300_000_000_000:
        fail(3, f"AAPL latest revenue ${rev / 1e9:.0f}B below plausible threshold $300B")
    summary = r.get("summary", {})
    if summary.get("latest_revenue_usd") is None:
        fail(3, "AAPL summary missing latest_revenue_usd")
    vinfo(f"AAPL cap: rev=${rev/1e9:.1f}B periods={len(periods)}")

    r2 = asyncio.run(get_capital_flow("msft", 4, "annual", "smoke_test"))
    pct = r2["periods"][0]["ratios"].get("_percentiles", {}).get("gross_margin")
    vinfo(f"MSFT gm percentile: {pct}")

    info("[OK] capital_flow endpoint")
except SystemExit:
    raise
except Exception as ex:
    traceback.print_exc()
    fail(3, f"capital_flow broken: {ex}")

# 4. contagion simulator + scenario catalog
try:
    from api.routers.contagion import simulate, get_scenarios
    r = asyncio.run(simulate("tsmc", "price_increase", 0.30, 4, "smoke_test"))
    victims = r["summary"]["total_actors_affected"]
    if victims < 10:
        fail(4, f"tsmc shock only affected {victims} actors (expected >=10)")
    if r.get("prediction_id") is None:
        fail(4, "contagion did not persist prediction_id")
    vinfo(f"tsmc shock: {victims} victims, pid={r['prediction_id']}")

    scns = asyncio.run(get_scenarios("smoke_test"))
    if len(scns) < 5:
        fail(4, f"scenario catalog only has {len(scns)} entries (expected >=5)")
    vinfo(f"scenarios: {len(scns)}")

    info("[OK] contagion simulator + scenarios")
except SystemExit:
    raise
except Exception as ex:
    traceback.print_exc()
    fail(4, f"contagion broken: {ex}")

# 5. actor_detail drawer
try:
    from api.routers.actor_detail import get_actor_detail_for_drawer
    r = asyncio.run(get_actor_detail_for_drawer("AAPL", "smoke_test"))
    if r.get("type") != "company":
        fail(5, f"AAPL type={r.get('type')} (expected 'company')")
    mcap = r.get("market_cap")
    if mcap is None or mcap < 1_000_000_000_000:
        fail(5, f"AAPL market_cap={mcap} (expected >= $1T)")
    vinfo(f"AAPL drawer: type={r.get('type')} mcap=${mcap/1e12:.2f}T")
    info("[OK] actor_detail drawer")
except SystemExit:
    raise
except Exception as ex:
    traceback.print_exc()
    fail(5, f"actor_detail broken: {ex}")

# 6. sector_health composite
try:
    from api.routers.sector_health import get_sector_health
    r = asyncio.run(get_sector_health("Technology", "smoke_test"))
    score = r.get("score")
    if score is None or not (0 <= score <= 100):
        fail(6, f"sector_health Technology score={score} out of range")
    vinfo(f"Tech health: {score:.1f} ({r.get('trend_30d')})")
    info("[OK] sector_health composite")
except SystemExit:
    raise
except Exception as ex:
    traceback.print_exc()
    fail(6, f"sector_health broken: {ex}")

# 7. contagion -> trade ticket bridge
try:
    from trading.contagion_to_ticket import generate_tickets_for_recent_predictions
    from api.dependencies import get_db_engine
    tickets = generate_tickets_for_recent_predictions(get_db_engine(), since_hours=168)
    vinfo(f"tickets (last 168h): {len(tickets)}")
    info("[OK] contagion -> trade ticket bridge")
except SystemExit:
    raise
except Exception as ex:
    traceback.print_exc()
    fail(7, f"trade tickets broken: {ex}")

# 8. explain endpoint (hero query)
try:
    from datetime import date
    from api.routers.explain import get_actor_explain
    r = asyncio.run(get_actor_explain("aapl", str(date.today()), 5, "smoke_test"))
    if "evidence" not in r:
        fail(8, "explain response missing evidence field")
    vinfo(f"AAPL explain: {len(r['evidence'])} evidence items")
    info("[OK] explain endpoint")
except SystemExit:
    raise
except Exception as ex:
    traceback.print_exc()
    fail(8, f"explain broken: {ex}")

info("")
info("=" * 40)
info("ALL SMOKE TESTS PASSED")
info("=" * 40)
PY

RC=$?

FINISHED=$(date -u +%Y-%m-%dT%H:%M:%SZ)
log ""
log "==> started:  $STARTED"
log "    finished: $FINISHED"
log "    exit:     $RC"

exit $RC
