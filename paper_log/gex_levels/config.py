"""Constants pinned by ``docs/paper_log/gex-levels-v1-preregistration.md``.

Every number in this file traces to a specific line of the pre-registration
or is an explicitly-documented implementation choice the pre-registration
left open (fixed seed, exact query shape, etc.). None of it may change
after the first logged session without a new pre-registration (v2) — see
the pre-registration's own header.
"""

from __future__ import annotations

from pathlib import Path
from zoneinfo import ZoneInfo

# ── Identity ──────────────────────────────────────────────────────────────

TICKER = "SPY"
VIX_TICKER = "^VIX"
EASTERN = ZoneInfo("America/New_York")

# Repo-relative path to the pre-registration, used only to resolve it in a
# dev checkout (``resolve_prereg_sha256`` in storage.py). Production never
# re-reads this file — it carries the hash below as a pinned constant.
PREREG_RELATIVE_PATH = Path("docs/paper_log/gex-levels-v1-preregistration.md")
# The pre-registration has been amended twice, both times before any
# session was logged — the document's own rule ("nothing below may change
# after the first logged session") explicitly permits this, and each
# amendment says so itself. PREREG_COMMIT/PREREG_SHA256 below are pinned
# to the CURRENT amended text — the only text that will ever back a real
# (non-smoke) record. History, in case anything needs to be traced back:
#   47c05fc8 — original registration
#   2951e4cc — Amendment 1 items 1-4 (tested walls, narrower
#              engine_unavailable, data_unavailable/no_preopen, wording)
#   07ef4a3c — Amendment 1 item 5 (tested walls computed contract by
#              contract, matching what tested_walls.py already did; notes
#              the pinned engine must compute regime the same way as flip)
PREREG_COMMIT = "07ef4a3c"

# SHA-256 of the pre-registration file's exact committed bytes (LF line
# endings), computed once and pinned here so the value written into the
# first JSONL record never depends on a working copy's line endings or on
# git being available at run time:
#
#   git show 07ef4a3c:docs/paper_log/gex-levels-v1-preregistration.md | sha256sum
#
# Verified 2026-09-24 to match both `git show <commit>:<path>` and the
# working copy (repo core.autocrlf=true did not rewrite this particular
# file — confirmed independently rather than assumed). Recompute and
# update both PREREG_COMMIT and PREREG_SHA256 together if the
# pre-registration is ever amended again before the first logged session
# — never let one change without the other.
PREREG_SHA256 = "8e4ce28ba9fc3479449f04e278532273224bce14edacfae14ab060329b8087f6"

# ── Schedule / lateness gate ─────────────────────────────────────────────
# "A session counts only if its pre-open record was written before 09:30
# America/New_York that day." / "Refuse to write a session's pre-open
# record at/after 09:30 America/New_York (write it as excluded
# late_preopen)."

from datetime import time as _time  # noqa: E402

PREOPEN_DEADLINE_ET = _time(9, 30)

# ── Cross-checks / thresholds ────────────────────────────────────────────

# "Cross-check against the engine's spot; if they differ by more than
# 0.25%, the session is excluded (reason ref_mismatch)."
REF_MISMATCH_THRESHOLD_PCT = 0.0025

# "A placebo within 0.10% of any real level is dropped."
PLACEBO_COLLISION_THRESHOLD_PCT = 0.0010

# Amendment 1: tested walls "must be at least 0.5% from P0, on the correct
# side" — put wall among strikes <= (1 - this) * P0, call wall among
# strikes >= (1 + this) * P0.
TESTED_WALL_MIN_DISTANCE_PCT = 0.005

# "more than one trading day old" -> stale_chain
MAX_CHAIN_AGE_TRADING_DAYS = 1

# "more than 10% of the expected 5-minute bars are missing" -> bars_missing
MAX_MISSING_BARS_PCT = 0.10

# ">10% of sessions are excluded (not counting market_closed) by the 30th
# session" -> advisory only (status() surfaces it; nothing in this codebase
# auto-halts the job — see status.py docstring).
STOP_REVIEW_SESSION_COUNT = 30
STOP_REVIEW_EXCLUSION_PCT = 0.10

# ── Costs (H3) ────────────────────────────────────────────────────────────

# "1 basis point adverse slippage per side, no commission. Notional $1,000
# per trade."
SLIPPAGE_BP_PER_SIDE = 1.0
NOTIONAL_USD = 1_000.0

# ── Hypothesis tests ──────────────────────────────────────────────────────

VALID_SESSIONS_REQUIRED = 60

# "Three tests, Bonferroni-corrected: a hypothesis passes only with
# one-sided p < 0.0167." (0.05 / 3 = 0.016666...)
BONFERRONI_ALPHA = 0.0167

NEWEY_WEST_LAGS = 5

# "shuffling real/placebo labels within each session, 10,000 draws."
H2_PERMUTATION_DRAWS = 10_000
# Not specified by the pre-registration beyond "fixed seed" — pinned here,
# documented, and never changed within v1 so every `evaluate` run (interim
# or final) reproduces the identical permutation draws.
H2_PERMUTATION_SEED = 20260924

H1_MIN_GROUP_SESSIONS = 10
H2_MIN_REAL_REACHES = 20
H3_MIN_TRADES = 20

# ── Regimes (as returned by physics.dealer_gamma.DealerGammaEngine) ───────

REGIME_LONG_GAMMA = "LONG_GAMMA"
REGIME_SHORT_GAMMA = "SHORT_GAMMA"
REGIME_NEUTRAL = "NEUTRAL"
VALID_REGIMES = {REGIME_LONG_GAMMA, REGIME_SHORT_GAMMA, REGIME_NEUTRAL}

LEVEL_NAMES = ("gamma_flip", "put_wall", "call_wall")
H3_LEVEL_NAMES = ("put_wall", "call_wall")

# ── Exclusion reason codes ────────────────────────────────────────────────

EXCL_NO_CHAIN = "no_chain"
EXCL_STALE_CHAIN = "stale_chain"
EXCL_ENGINE_UNAVAILABLE = "engine_unavailable"
EXCL_REF_MISMATCH = "ref_mismatch"
EXCL_LATE_PREOPEN = "late_preopen"
EXCL_DATA_UNAVAILABLE = "data_unavailable"  # Amendment 1
EXCL_NO_PREOPEN = "no_preopen"  # Amendment 1
EXCL_BARS_MISSING = "bars_missing"
EXCL_MARKET_CLOSED = "market_closed"

ALL_EXCLUSION_CODES = (
    EXCL_NO_CHAIN,
    EXCL_STALE_CHAIN,
    EXCL_ENGINE_UNAVAILABLE,
    EXCL_REF_MISMATCH,
    EXCL_LATE_PREOPEN,
    EXCL_DATA_UNAVAILABLE,
    EXCL_NO_PREOPEN,
    EXCL_BARS_MISSING,
    EXCL_MARKET_CLOSED,
)

# Amendment 1: "the market-data source for P0 or VIX was unreachable or
# returned nothing at the pre-open run, after one retry" -> one retry
# means two attempts total.
DATA_FETCH_ATTEMPTS = 2

# ── Read-only DB session guard ────────────────────────────────────────────

DB_STATEMENT_TIMEOUT = "20s"
