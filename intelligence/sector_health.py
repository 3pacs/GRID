"""Sector health composite score.

Pure-function, deterministic computation of a 0-100 health score for
each sector in ``analysis/sector_map.SECTOR_MAP``. No LLM calls, no
web fetches — every input is a parameterized SQL query against
``griddb``.

Weights (sum to 1.0):

    0.25  margin_trajectory  — average gross_margin_trend across tickers
    0.20  chokepoint_exposure — inverse of avg chokepoint_score on sector edges
    0.20  capital_allocation  — reward high fcf_conversion + shareholder_yield,
                                penalize high debt_issuance_intensity
    0.15  insider_sentiment   — net insider buys - sells, normalized
    0.10  congress_sentiment  — net congressional buys - sells, normalized
    0.10  dark_pool           — accumulation = +1, distribution = -1, neutral = 0

Each sub-score is a float in [0, 1]. Sentiment scores that are natively
on [-1, 1] (insider/congress/dark_pool) are remapped via ``(x+1)/2``.
A sub-score with no underlying data returns ``None``; the composite
fills it with 0.5 (neutral) so a sector never scores 0 because one
puller is stale, and reports exactly which components were filled in
``data_coverage``. If *no* component has data, or the computation
fails, the payload is an explicit unavailable state (``score: None``,
``status: "unavailable"``, ``reason``) rather than a neutral 50 that
is indistinguishable from a measured score.

The 30d trend is derived from the nearest ``sector_health_snapshots``
row >= 25 days old. If no prior snapshot exists the trend is
``"stable"``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Public weight map. Kept at module level so tests can assert on it.
WEIGHTS: dict[str, float] = {
    "margin": 0.25,
    "chokepoints": 0.20,
    "capital_allocation": 0.20,
    "insider": 0.15,
    "congress": 0.10,
    "dark_pool": 0.10,
}

NEUTRAL: float = 0.5
TREND_WINDOW_DAYS: int = 30
TREND_EPS: float = 2.0  # points on the 0-100 scale


# ── Helpers ────────────────────────────────────────────────────────

def _clamp01(x: float) -> float:
    if x != x:  # NaN
        return NEUTRAL
    return max(0.0, min(1.0, float(x)))


def _remap_signed(x: float) -> float:
    """Map a value in [-1, 1] to [0, 1]."""
    return _clamp01((float(x) + 1.0) / 2.0)


def _safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None:
        return None
    try:
        d = float(den)
        return None if d == 0.0 else float(num) / d
    except (TypeError, ValueError):
        return None


def _table_exists(conn: Any, name: str) -> bool:
    try:
        row = conn.execute(text("SELECT to_regclass(:n)").bindparams(n=name)).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _sector_tickers(sector_name: str) -> list[str]:
    """Return the list of equity tickers tracked under a sector."""
    from analysis.sector_map import SECTOR_MAP
    sector = SECTOR_MAP.get(sector_name) or {}
    tickers: list[str] = []
    for sub in sector.get("subsectors", {}).values():
        for a in sub.get("actors", []) or []:
            t = a.get("ticker")
            if t:
                tickers.append(t)
    return tickers


def _id_variants(tickers: list[str]) -> list[str]:
    """Return a superset of ticker IDs that matches the canonical forms
    stored in ``capital_flows.actor_id`` (lowercase) and
    ``supply_chain_edges.upstream_id/downstream_id`` (slug / ticker).

    Keeping both the upper- and lower-case forms means the module works
    whether a table stores ``NVDA`` or ``nvda`` without a migration.
    """
    out: set[str] = set()
    for t in tickers:
        if not t:
            continue
        out.add(t)
        out.add(t.upper())
        out.add(t.lower())
    return sorted(out)


# ── Sub-score computations ────────────────────────────────────────

def _margin_score(conn: Any, tickers: list[str]) -> float | None:
    """Average 3y gross-margin trend across tickers.

    Uses ``capital_flows`` directly and mirrors the logic from
    ``api/routers/capital_flow.py::_compute_ratios`` — gross_margin =
    (revenue - cogs) / revenue, computed on the most recent 4 annual
    periods. Trend is measured as latest minus 4y-ago: >+0.5pp is
    "expanding" (1.0), <-0.5pp is "contracting" (0.0), otherwise
    0.5.
    """
    if not tickers or not _table_exists(conn, "public.capital_flows"):
        return None

    _id_variants(tickers)
    scores: list[float] = []
    for t in tickers:
        # Try both upper and lower variants of this ticker.
        variants = list({t, t.upper(), t.lower()})
        rows = conn.execute(
            text(
                """
                SELECT fiscal_period, flow_type, SUM(amount_usd) AS amt
                FROM capital_flows
                WHERE actor_id = ANY(:ids)
                  AND period_type = 'annual'
                  AND flow_type IN ('revenue', 'cogs')
                GROUP BY fiscal_period, flow_type
                ORDER BY fiscal_period DESC
                """
            ).bindparams(ids=variants)
        ).fetchall()
        if not rows:
            continue

        # Fold (period, flow_type) → amount into {period: {revenue, cogs}}
        by_period: dict[Any, dict[str, float]] = {}
        for fp, ft, amt in rows:
            by_period.setdefault(fp, {})[ft] = float(amt or 0.0)

        periods = sorted(by_period.keys(), reverse=True)
        margins: list[float] = []
        for p in periods[:4]:
            rev = by_period[p].get("revenue")
            cogs = by_period[p].get("cogs")
            gm = _safe_div((rev or 0.0) - (cogs or 0.0), rev)
            if gm is not None:
                margins.append(gm)

        if len(margins) < 2:
            continue

        delta = margins[0] - margins[-1]
        if delta > 0.005:
            scores.append(1.0)
        elif delta < -0.005:
            scores.append(0.0)
        else:
            scores.append(0.5)

    if not scores:
        return None
    return sum(scores) / len(scores)


def _chokepoint_score(conn: Any, tickers: list[str]) -> float | None:
    """Inverse of the average chokepoint_score on edges touching the sector.

    ``chokepoint_score`` in ``supply_chain_edges`` is 0..1 where 1 is a
    severe bottleneck (cocoa, lithium, TSMC N3, etc). We want the health
    score to go DOWN as chokepoint exposure goes UP, so we return
    ``1 - avg_chokepoint``.
    """
    if not tickers or not _table_exists(conn, "public.supply_chain_edges"):
        return None

    id_set = _id_variants(tickers)
    row = conn.execute(
        text(
            """
            SELECT AVG(chokepoint_score)::float
            FROM supply_chain_edges
            WHERE chokepoint_score IS NOT NULL
              AND (
                upstream_id = ANY(:ids)
                OR downstream_id = ANY(:ids)
              )
            """
        ).bindparams(ids=id_set)
    ).fetchone()

    if row is None or row[0] is None:
        return None

    avg_cp = _clamp01(float(row[0]))
    return 1.0 - avg_cp


def _capital_allocation_score(conn: Any, tickers: list[str]) -> float | None:
    """Reward high fcf_conversion + shareholder_yield, penalize high
    net-debt-issuance intensity. All ratios are derived from the latest
    annual ``capital_flows`` row per ticker.
    """
    if not tickers or not _table_exists(conn, "public.capital_flows"):
        return None

    sub_scores: list[float] = []
    for t in tickers:
        variants = list({t, t.upper(), t.lower()})
        rows = conn.execute(
            text(
                """
                SELECT fiscal_period, flow_type, SUM(amount_usd) AS amt
                FROM capital_flows
                WHERE actor_id = ANY(:ids)
                  AND period_type = 'annual'
                  AND flow_type IN (
                    'revenue', 'cogs', 'opex', 'capex',
                    'dividends', 'buybacks', 'debt_issuance'
                  )
                GROUP BY fiscal_period, flow_type
                ORDER BY fiscal_period DESC
                """
            ).bindparams(ids=variants)
        ).fetchall()
        if not rows:
            continue

        by_period: dict[Any, dict[str, float]] = {}
        for fp, ft, amt in rows:
            by_period.setdefault(fp, {})[ft] = float(amt or 0.0)
        if not by_period:
            continue

        latest_period = max(by_period.keys())
        amts = by_period[latest_period]
        rev = amts.get("revenue")
        if rev is None or rev <= 0:
            continue
        cogs = amts.get("cogs", 0.0) or 0.0
        opex = amts.get("opex", 0.0) or 0.0
        capex = amts.get("capex", 0.0) or 0.0
        div = amts.get("dividends", 0.0) or 0.0
        buy = amts.get("buybacks", 0.0) or 0.0
        debt = amts.get("debt_issuance", 0.0) or 0.0

        # FCF conv ≈ (revenue - cogs - opex - capex) / revenue  (-inf .. 1)
        fcf_conv = _safe_div(rev - cogs - opex - capex, rev) or 0.0
        # Shareholder yield ≈ (div + buy) / revenue  (0 .. ~0.1)
        sy = _safe_div(div + buy, rev) or 0.0
        # Debt issuance intensity ≈ debt / revenue (0 .. ~0.3+)
        debt_int = _safe_div(debt, rev) or 0.0

        # Normalize each component to [0, 1] with reasonable company-level caps.
        fcf_norm = _clamp01((fcf_conv + 0.2) / 0.5)         # -20% → 30% → 0..1
        sy_norm = _clamp01(sy / 0.08)                       # 0..8% shareholder yield
        debt_norm = 1.0 - _clamp01(debt_int / 0.15)         # 0..15% issuance ⇒ full..zero

        sub_scores.append((fcf_norm + sy_norm + debt_norm) / 3.0)

    if not sub_scores:
        return None
    return sum(sub_scores) / len(sub_scores)


def _insider_sentiment_score(conn: Any, tickers: list[str]) -> float | None:
    """Net insider buys - sells over the last 90d, normalized via the
    classic sentiment ratio (buys - sells) / (buys + sells) in [-1,1]
    then remapped to [0,1]. UNUSUAL_* variants count toward their side.
    """
    if not tickers or not _table_exists(conn, "public.insider_trades"):
        return None

    row = conn.execute(
        text(
            """
            SELECT
              SUM(CASE WHEN UPPER(trade_type) LIKE '%BUY%' THEN 1 ELSE 0 END) AS buys,
              SUM(CASE WHEN UPPER(trade_type) LIKE '%SELL%' THEN 1 ELSE 0 END) AS sells
            FROM insider_trades
            WHERE ticker = ANY(:tickers)
              AND trade_date >= (CURRENT_DATE - INTERVAL '90 days')
            """
        ).bindparams(tickers=_id_variants(tickers))
    ).fetchone()

    buys = float(row[0] or 0) if row else 0.0
    sells = float(row[1] or 0) if row else 0.0
    total = buys + sells
    if total <= 0:
        return None
    ratio = (buys - sells) / total  # [-1, 1]
    return _remap_signed(ratio)


def _congress_sentiment_score(conn: Any, tickers: list[str]) -> float | None:
    """Net congressional buys - sells over the last 180d, normalized
    via (buys - sells) / (buys + sells) and remapped to [0,1].
    """
    if not tickers or not _table_exists(conn, "public.congressional_trades"):
        return None

    row = conn.execute(
        text(
            """
            SELECT
              SUM(CASE WHEN UPPER(transaction_type) LIKE '%BUY%'
                         OR UPPER(transaction_type) LIKE 'PURCHASE%'
                    THEN 1 ELSE 0 END) AS buys,
              SUM(CASE WHEN UPPER(transaction_type) LIKE '%SELL%'
                    THEN 1 ELSE 0 END) AS sells
            FROM congressional_trades
            WHERE ticker = ANY(:tickers)
              AND disclosure_date >= (CURRENT_DATE - INTERVAL '180 days')
            """
        ).bindparams(tickers=_id_variants(tickers))
    ).fetchone()

    buys = float(row[0] or 0) if row else 0.0
    sells = float(row[1] or 0) if row else 0.0
    total = buys + sells
    if total <= 0:
        return None
    ratio = (buys - sells) / total
    return _remap_signed(ratio)


def _dark_pool_score(conn: Any, tickers: list[str]) -> float | None:
    """Average dark-pool positioning across sector tickers.

    dark_pool_weekly.short_pct > 0.55 → distribution (-1)
    dark_pool_weekly.short_pct < 0.45 → accumulation (+1)
    otherwise neutral (0).

    Return the mean across tickers with a recent reading, remapped to
    [0, 1].
    """
    if not tickers or not _table_exists(conn, "public.dark_pool_weekly"):
        return None

    rows = conn.execute(
        text(
            """
            WITH ranked AS (
                SELECT ticker, short_pct,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) rn
                FROM dark_pool_weekly
                WHERE ticker = ANY(:tickers)
                  AND short_pct IS NOT NULL
            )
            SELECT short_pct FROM ranked WHERE rn = 1
            """
        ).bindparams(tickers=_id_variants(tickers))
    ).fetchall()

    if not rows:
        return None

    raw: list[float] = []
    for r in rows:
        sp = float(r[0])
        if sp > 0.55:
            raw.append(-1.0)
        elif sp < 0.45:
            raw.append(1.0)
        else:
            raw.append(0.0)

    mean_signed = sum(raw) / len(raw)
    return _remap_signed(mean_signed)


# ── Trend + narrative ──────────────────────────────────────────────

def _trend_from_snapshots(conn: Any, sector_name: str, latest_score: float) -> str:
    """Return "improving" | "stable" | "deteriorating" based on the
    nearest snapshot row ~30 days old. Defaults to "stable" when no
    prior row exists.
    """
    if not _table_exists(conn, "public.sector_health_snapshots"):
        return "stable"

    row = conn.execute(
        text(
            """
            SELECT score
            FROM sector_health_snapshots
            WHERE sector_name = :s
              AND snapshot_date <= (CURRENT_DATE - INTERVAL '25 days')
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        ).bindparams(s=sector_name)
    ).fetchone()

    if row is None or row[0] is None:
        return "stable"

    prior = float(row[0])
    delta = latest_score - prior
    if delta > TREND_EPS:
        return "improving"
    if delta < -TREND_EPS:
        return "deteriorating"
    return "stable"


def _build_narrative(sector_name: str, score: float, components: dict[str, float],
                     trend: str) -> str:
    """Short deterministic template — no LLM."""
    strongest = max(components.items(), key=lambda kv: kv[1])
    weakest = min(components.items(), key=lambda kv: kv[1])
    pretty = {
        "margin": "margin trajectory",
        "chokepoints": "chokepoint resilience",
        "capital_allocation": "capital allocation",
        "insider": "insider sentiment",
        "congress": "congressional sentiment",
        "dark_pool": "dark-pool positioning",
    }
    trend_phrase = {
        "improving": "trending up",
        "stable": "stable",
        "deteriorating": "trending down",
    }.get(trend, "stable")
    return (
        f"{sector_name} health {score:.0f}/100 — {trend_phrase}. "
        f"Strongest lever: {pretty[strongest[0]]} ({strongest[1]:.2f}); "
        f"weakest: {pretty[weakest[0]]} ({weakest[1]:.2f})."
    )


# ── Public API ─────────────────────────────────────────────────────

def _unavailable(sector_name: str, reason: str) -> dict[str, Any]:
    """Honest payload for a sector whose health cannot be computed."""
    return {
        "sector": sector_name,
        "score": None,
        "trend_30d": None,
        "components": {},
        "narrative": f"{sector_name} health unavailable: {reason}.",
        "as_of": None,
        "status": "unavailable",
        "reason": reason,
        "data_coverage": {
            "with_data": [],
            "missing_neutral_filled": list(WEIGHTS.keys()),
        },
    }


_SCORERS = {
    "margin": _margin_score,
    "chokepoints": _chokepoint_score,
    "capital_allocation": _capital_allocation_score,
    "insider": _insider_sentiment_score,
    "congress": _congress_sentiment_score,
    "dark_pool": _dark_pool_score,
}


def compute_sector_health(engine: Engine, sector_name: str) -> dict[str, Any]:
    """Return the sector health dict described in the module docstring.

    Never raises for data errors, only for obviously wrong inputs
    (unknown sector). A sub-score with no data is neutral-filled and
    listed in ``data_coverage.missing_neutral_filled``. When no
    component has data, or the query batch fails, the result is the
    explicit unavailable payload from :func:`_unavailable` instead of
    a fabricated neutral 50.
    """
    from analysis.sector_map import SECTOR_MAP

    if sector_name not in SECTOR_MAP:
        raise ValueError(f"Unknown sector: {sector_name!r}")

    tickers = _sector_tickers(sector_name)

    components: dict[str, float] = {}
    missing: list[str] = []
    try:
        with engine.connect() as conn:
            for key, scorer in _SCORERS.items():
                raw = scorer(conn, tickers)
                if raw is None:
                    components[key] = NEUTRAL
                    missing.append(key)
                else:
                    components[key] = _clamp01(raw)

            if len(missing) == len(WEIGHTS):
                return _unavailable(
                    sector_name, "no underlying data for any component",
                )

            score_01 = sum(WEIGHTS[k] * components[k] for k in WEIGHTS)
            score = round(100.0 * score_01, 2)
            trend = _trend_from_snapshots(conn, sector_name, score)
    except Exception as exc:
        log.warning(
            "sector_health: computation failed for {s}: {e}",
            s=sector_name,
            e=str(exc),
        )
        return _unavailable(sector_name, f"computation failed: {exc}")

    narrative = _build_narrative(sector_name, score, components, trend)
    if missing:
        narrative += f" Neutral-filled (no data): {', '.join(missing)}."

    return {
        "sector": sector_name,
        "score": score,
        "trend_30d": trend,
        "components": {k: round(v, 4) for k, v in components.items()},
        "narrative": narrative,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "status": "ok",
        "data_coverage": {
            "with_data": [k for k in WEIGHTS if k not in missing],
            "missing_neutral_filled": missing,
        },
    }


def snapshot_all_sectors(
    engine: Engine,
    snapshot_date: date | None = None,
    *,
    computed_at: datetime | None = None,
    should_continue: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Compute health for every sector in ``SECTOR_MAP`` and upsert one
    row per (sector, ``snapshot_date``) into ``sector_health_snapshots``.

    Used by the Hermes daily scheduler (3:00 UTC). Returns a dict of
    ``{sector_name: {score, trend_30d}}`` plus an aggregate count.

    Args:
        snapshot_date: The date to stamp every row written this call
            with. Defaults to ``datetime.now(timezone.utc).date()`` —
            deliberately NOT ``date.today()`` (the local calendar date),
            because the caller (``hermes_operator._maybe_run_sector_health_snapshot``)
            schedules this task against a UTC due-period boundary
            (``_period_boundary``, 03:00 UTC). Callers that retry within
            the same due period (e.g. a 23:30 UTC attempt that fails and
            a 00:30 UTC retry that succeeds) MUST pass the same
            ``snapshot_date`` — the due-period date, not "today" at the
            moment of the call — so every attempt in one due period
            upserts the same (sector_name, snapshot_date) row instead of
            splitting across two calendar dates. The
            ``ON CONFLICT (sector_name, snapshot_date) DO UPDATE`` below
            is what makes retries within a due period idempotent, but
            only if the date matches.
        computed_at: Timestamp written explicitly as ``as_of`` for every
            row this call writes. Defaults to ``datetime.now(timezone.utc)``
            at call start. Passed explicitly (rather than relying on the
            database's ``NOW()``) so the upsert guard below can compare
            "when did THIS attempt compute its numbers" against whatever
            ``as_of`` is already stored, instead of "which upsert statement
            happened to reach Postgres last" — the two differ whenever an
            abandoned worker's write reaches the database after a newer
            attempt's write (see ``should_continue`` and the WHERE guard
            below).

            Tie rule: when two attempts compute the SAME ``as_of`` (e.g. one
            due-period retry started from a shared ``now``), the WHERE guard
            below uses strict ``<`` — whichever attempt's write reaches
            Postgres FIRST wins, and the second (equal ``as_of``) attempt's
            write is rejected (``rowcount == 0``, counted as
            ``snapshots_stale_skipped``) rather than overwriting the row
            that already landed. "First committed wins on equal as_of."
            This was previously ``<=``, which let a strictly OLDER/abandoned
            attempt whose ``computed_at`` happened to equal the stored row's
            ``as_of`` clobber it; ``<`` closes that.
        should_continue: Optional callable, checked before each sector's
            compute AND again immediately before each sector's upsert.
            When it returns False, the loop stops where it is (an ordinary
            return, not an exception) and the result carries
            ``"aborted_stale": True`` plus the counters accumulated so far.
            Intended for a caller (``hermes_operator._maybe_run_sector_health_snapshot``)
            to cheaply abandon an in-process attempt once it knows a newer
            attempt has superseded it — e.g. after
            ``_run_with_timeout`` orphans a worker on timeout and a later
            cycle starts a fresh attempt before the orphan finishes. This
            is a same-process guard only (checked in Python between DB
            calls); it does not by itself prevent an in-flight upsert from
            landing — that is what the ``as_of`` WHERE guard below is for.
    """
    import json

    from analysis.sector_map import SECTOR_MAP

    today = snapshot_date if snapshot_date is not None else datetime.now(timezone.utc).date()
    computed_at = computed_at if computed_at is not None else datetime.now(timezone.utc)
    written = 0
    skipped = 0
    upsert_failed = 0
    stale_skipped = 0
    aborted_stale = False
    out: dict[str, Any] = {"date": today.isoformat(), "sectors": {}}

    for sector_name in SECTOR_MAP.keys():
        if should_continue is not None and not should_continue():
            aborted_stale = True
            log.info(
                "snapshot_all_sectors: aborting before {s} — should_continue() "
                "returned False (a newer attempt has superseded this one)",
                s=sector_name,
            )
            break

        try:
            result = compute_sector_health(engine, sector_name)
        except Exception as exc:
            log.warning("snapshot_all_sectors: {s} failed: {e}",
                        s=sector_name, e=str(exc))
            continue

        out["sectors"][sector_name] = {
            "score": result["score"],
            "trend_30d": result["trend_30d"],
        }

        if result.get("score") is None:
            # An unavailable sector must not be written as a 50.0 row:
            # the 30d trend and the oracle's sector routing both read
            # this table as measured history.
            out["sectors"][sector_name]["status"] = "unavailable"
            skipped += 1
            log.info("snapshot_all_sectors: {s} unavailable, not written ({r})",
                     s=sector_name, r=result.get("reason"))
            continue

        if should_continue is not None and not should_continue():
            aborted_stale = True
            log.info(
                "snapshot_all_sectors: aborting immediately before {s}'s upsert "
                "— should_continue() returned False (a newer attempt has "
                "superseded this one)",
                s=sector_name,
            )
            break

        try:
            with engine.begin() as conn:
                upsert_result = conn.execute(
                    text(
                        """
                        INSERT INTO sector_health_snapshots
                            (sector_name, score, components, snapshot_date, as_of)
                        VALUES (:s, :sc, CAST(:c AS JSONB), :d, :as_of)
                        ON CONFLICT (sector_name, snapshot_date) DO UPDATE
                        SET score = EXCLUDED.score,
                            components = EXCLUDED.components,
                            as_of = EXCLUDED.as_of
                        WHERE sector_health_snapshots.as_of IS NULL
                           OR sector_health_snapshots.as_of < EXCLUDED.as_of
                        """
                    ).bindparams(
                        s=sector_name,
                        sc=float(result["score"]),
                        c=json.dumps(result["components"]),
                        d=today,
                        as_of=computed_at,
                    )
                )
                # rowcount is 1 when the INSERT landed or the DO UPDATE's
                # WHERE guard matched (this attempt is STRICTLY newer than
                # whatever as_of was already stored — strict `<`, not `<=`);
                # 0 means the guard rejected the write because an
                # already-stored row has an as-new-or-newer as_of — i.e. a
                # LATER (or, on an exact as_of tie, an earlier-COMMITTED)
                # attempt already wrote this sector, and this attempt's
                # write must not overwrite it. First committed wins on
                # equal as_of. That is not a failure: it is the
                # cross-attempt protection working as intended, so it is
                # counted separately from upsert_failed.
                if upsert_result.rowcount == 1:
                    written += 1
                else:
                    stale_skipped += 1
                    log.info(
                        "snapshot_all_sectors: {s} upsert skipped — an "
                        "as-new-or-newer row already exists for this "
                        "(sector, snapshot_date)",
                        s=sector_name,
                    )
        except Exception as exc:
            upsert_failed += 1
            log.warning(
                "snapshot_all_sectors: upsert failed for {s}: {e}",
                s=sector_name, e=str(exc),
            )

    out["snapshots_written"] = written
    out["snapshots_skipped_unavailable"] = skipped
    out["upsert_failed"] = upsert_failed
    out["snapshots_stale_skipped"] = stale_skipped
    out["aborted_stale"] = aborted_stale
    log.info(
        "sector_health: wrote {n} daily snapshots ({k} unavailable, skipped, "
        "{f} upsert failed, {st} stale-skipped{ab})",
        n=written, k=skipped, f=upsert_failed, st=stale_skipped,
        ab=", aborted stale" if aborted_stale else "",
    )
    return out
