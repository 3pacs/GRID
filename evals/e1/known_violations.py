"""Real violations the E1 gates found on main, tracked as strict, named xfails.

A gate is never weakened to pass. When it finds a genuine defect on main, the
failing case is marked ``@known_violation("<ID>")``: a *strict* xfail, so

* the suite stays green while the defect is open and recorded here, and
* the day the code is fixed, the xfail turns into an XPASS **failure** until
  the entry is deleted from this registry (and ``MANIFEST.sha256`` re-pinned).

``test_manifest_guard.py`` checks every ID here is used by at least one gate
and every use names an ID here. Each entry names the code location, what is
wrong and why it matters. Report: ``GRID-E1-GATES-V1-20260930.md``.

Closed (entry removed, gate now enforced): E1-V1, E1-V2, E1-V5 (suite
``e1-v1.1``); E1-V3 (registry entries with no callable pull method), E1-V4
(raw_series writers without pull_status / with non-schema columns / rewriting
stored rows) and E1-V6 (coingecko writing resolved_series directly) -- suite
``e1-v1.2``.

Open: E1-V7a..o (suite ``e1-v1.3``), direct resolved_series writers that
backdate or rewrite vintages (evals/e1/vintage_scan.py, DATA-FIX DFa).
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest


@dataclass(frozen=True)
class Violation:
    gate: str
    title: str
    location: str
    detail: str


KNOWN: dict[str, Violation] = {}


# E1-V7: direct resolved_series writers that backdate vintages or rewrite them
# in place (evals/e1/vintage_scan.py). A writer that stamps release_date /
# vintage_date with the row's obs_date makes history fetched on one day look,
# to every point-in-time reader (store/pit.py, the regime state vector,
# realized_alpha), as if GRID had held it on each past day. One ID per writer.
_V7_GATE = "provenance (resolved_series vintages)"
_V7_BACKDATE = (
    "release_date = vintage_date = obs_date: every historical row it loads claims to have been known on its own "
    "observation day, although it was fetched when the script ran. PIT reads before the run date see values GRID "
    "did not have (look-ahead in any replay), and there is no raw_series row to say when the value really arrived."
)
_V7: dict[str, tuple[str, str, str]] = {
    "E1-V7a": ("analysis/research_agent.py::fill_missing_stocks",
               "yfinance ~504-day close history inserted straight into resolved_series",
               _V7_BACKDATE),
    "E1-V7b": ("scripts/backfill_celestial_ephemeris.py::flush_batches (resolved_batch od/rd/vd = d)",
               "ephemeris backfill stamps each day's resolved row with that day as release and vintage",
               _V7_BACKDATE + " Ephemeris values are computable in advance, so the honest fix may be an explicit, "
               "reviewed same-value exemption rather than a code change; until then it is tracked."),
    "E1-V7c": ("scripts/backfill_resolved_series_from_ticker_metrics.sql (tmd.obs_date AS release_date)",
               "TwelveData ticker_metrics_daily bridge sets release_date from obs_date",
               "release_date = obs_date (vintage_date = as_of, or obs_date when as_of is NULL): rows bridged from "
               "ticker_metrics_daily claim release on their observation day, bypassing raw_series and the resolver."),
    "E1-V7d": ("scripts/bridge_crucix.py::ins", "Crucix bridge writes backdated resolved rows", _V7_BACKDATE),
    "E1-V7e": ("scripts/load_alt_data.py::insert_obs", "alt-data loader writes backdated resolved rows",
               _V7_BACKDATE),
    "E1-V7f": ("scripts/load_more_data.py (two yfinance period='2y' loaders)",
               "yfinance 2-year loaders write backdated resolved rows", _V7_BACKDATE),
    "E1-V7g": ("scripts/load_ticker_deep.py::ins", "ticker-deep loader writes backdated resolved rows",
               _V7_BACKDATE),
    "E1-V7h": ("scripts/load_wave2.py::ins", "wave-2 loader writes backdated resolved rows", _V7_BACKDATE),
    "E1-V7i": ("scripts/load_wave3.py::ins", "wave-3 loader writes backdated resolved rows", _V7_BACKDATE),
    "E1-V7j": ("scripts/load_yfinance.py (yfinance period='2y' loader)",
               "yfinance 2-year loader writes backdated resolved rows", _V7_BACKDATE),
    "E1-V7k": ("scripts/migrate_and_load.py::insert_resolved", "bulk GDELT/EIA/NY Fed loader writes backdated resolved rows",
               _V7_BACKDATE),
    "E1-V7l": ("scripts/parse_edgar.py (quarterly XBRL aggregate INSERT ... SELECT)",
               "EDGAR aggregate rewrites stored vintages with ON CONFLICT ... DO UPDATE",
               "Release and vintage come from the filing date (honest), but ON CONFLICT (feature_id, obs_date, "
               "vintage_date) DO UPDATE SET value rewrites a stored vintage in place, so a re-run silently changes "
               "what every past read saw instead of appending a new vintage."),
    "E1-V7m": ("scripts/parse_eia.py::ins", "EIA bulk parser writes backdated resolved rows", _V7_BACKDATE),
    "E1-V7n": ("scripts/parse_gdelt.py (daily GDELT features)", "GDELT parser writes backdated resolved rows",
               _V7_BACKDATE),
    "E1-V7o": ("scripts/pull_intraday.py (intraday feature push)", "intraday puller writes backdated resolved rows",
               _V7_BACKDATE),
}
KNOWN.update({vid: Violation(gate=_V7_GATE, title=title, location=loc, detail=detail)
              for vid, (loc, title, detail) in _V7.items()})


def known_violation(vid: str):
    """Strict xfail for a registered violation (unknown IDs fail at import).

    Only an ``AssertionError`` (the gate's own check) counts as the known
    failure; any other exception is a real error, not the tracked violation.
    """
    v = KNOWN[vid]
    return pytest.mark.xfail(strict=True, raises=AssertionError, reason=f"{vid} [{v.gate}] {v.title} -- {v.location}")
