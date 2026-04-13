"""Holder / deal overlap detector — "pre-positioning" cross-reference.

Cross-references ``institutional_holdings`` (13F filings) against
``capital_flows`` M&A announcement rows to find cases where the same
filer held material positions in BOTH the acquirer and the target
BEFORE the acquisition was announced.

Signal logic (see ``migrations/0034_holder_deal_overlap.sql``):

    1. For each ``capital_flows`` row where
           period_type = 'announcement'
           flow_type   = 'acquisitions'
           counterparty_id IS NOT NULL
       take ``fiscal_period`` as the announcement date.
    2. Find the latest ``institutional_holdings.report_date`` BEFORE
       the announcement date (T-45 days is the lookback edge).
    3. For that report_date, find all filers that held BOTH
       ``actor_id`` (acquirer) and ``counterparty_id`` (target).
    4. For each such filer, compute the two position values and flag
       ``pre_position_flag`` whenever either leg clears the
       ``MIN_POSITION_USD`` material-position floor.
    5. Flag ``quick_exit_flag`` when the filer's NEXT 13F report
       after the deal announcement has no row for the target (the
       position was liquidated in the quarter following the deal).

Idempotent: upserts on
    (deal_announcement_date, acquirer_ticker, target_ticker, filer_name)
so the daily Hermes job can refresh in place without dup rows.

All SQL uses SQLAlchemy ``text()`` + ``.bindparams()`` — no string
formatting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Tunables ─────────────────────────────────────────────────────────

# Lookback window from announcement → latest eligible 13F snapshot.
# 13F filings are quarterly with a 45-day filing delay, so the latest
# "pre-announcement" snapshot can be anywhere from 1 day to ~135 days
# old. We cap the lookback at 180 days to cover one missed quarter.
LOOKBACK_DAYS: int = 180

# The "material position" floor used for the pre_position_flag. A 13F
# position worth < $500k on either leg is noise (fund-admin dust).
# The mission spec mentions ">=1% of either" but absolute 13F
# percentages require float/shares-outstanding joins that don't exist
# in ``institutional_holdings`` — instead we flag any overlap where the
# weaker leg is still above this dollar floor. The threshold is a
# module-level constant so callers / tests can override it.
MIN_POSITION_USD: float = 500_000.0


# ── Data class ───────────────────────────────────────────────────────


@dataclass
class OverlapRow:
    """One (deal, filer) overlap candidate ready to upsert."""

    deal_announcement_date: date
    acquirer_ticker: str
    target_ticker: str
    filer_name: str
    acquirer_position_value_usd: float | None
    target_position_value_usd: float | None
    holding_report_date: date | None
    days_before_announcement: int | None
    pre_position_flag: bool = False
    quick_exit_flag: bool = False
    narrative: str = ""


# ── Core detection SQL ───────────────────────────────────────────────


# 1. Find every announcement-style acquisition row with a non-null
#    counterparty. We collapse duplicates from corporate_actions_parser
#    re-runs by picking the earliest ``fiscal_period`` per
#    (acquirer, target) pair so the detector anchors on the real
#    announcement, not a later 8-K amendment.
_DEALS_SQL = text(
    """
    SELECT
        MIN(fiscal_period)         AS announcement_date,
        actor_id                   AS acquirer_ticker,
        counterparty_id            AS target_ticker,
        MAX(amount_usd)            AS deal_size_usd
    FROM capital_flows
    WHERE period_type = 'announcement'
      AND flow_type   = 'acquisitions'
      AND counterparty_id IS NOT NULL
      AND counterparty_id <> ''
      AND actor_id      IS NOT NULL
      AND actor_id      <> ''
    GROUP BY actor_id, counterparty_id
    """
)


# 2. For a single (acquirer, target, announcement_date) triple, find
#    the latest 13F report_date before the announcement on BOTH legs
#    and emit one row per filer that held both legs on that date.
#
#    Implementation note: we first find the latest eligible
#    ``report_date`` for each leg independently (acquirer and target)
#    inside the lookback window, then INNER JOIN on
#    ``holder_name`` where both snapshots are non-null. Some filers
#    have CIK variants; we dedupe on the normalized ``holder_name``.
_LEG_SQL = text(
    """
    WITH target_cutoff AS (
        SELECT MAX(report_date) AS d
        FROM institutional_holdings
        WHERE ticker      = :target
          AND report_date <  :announcement_date
          AND report_date >= :min_date
    ),
    acquirer_cutoff AS (
        SELECT MAX(report_date) AS d
        FROM institutional_holdings
        WHERE ticker      = :acquirer
          AND report_date <  :announcement_date
          AND report_date >= :min_date
    ),
    acquirer_leg AS (
        SELECT holder_name,
               SUM(value_usd)   AS value_usd,
               SUM(shares_held) AS shares,
               MAX(report_date) AS report_date
        FROM institutional_holdings
        WHERE ticker = :acquirer
          AND report_date = (SELECT d FROM acquirer_cutoff)
          AND holder_name IS NOT NULL
        GROUP BY holder_name
    ),
    target_leg AS (
        SELECT holder_name,
               SUM(value_usd)   AS value_usd,
               SUM(shares_held) AS shares,
               MAX(report_date) AS report_date
        FROM institutional_holdings
        WHERE ticker = :target
          AND report_date = (SELECT d FROM target_cutoff)
          AND holder_name IS NOT NULL
        GROUP BY holder_name
    )
    SELECT
        a.holder_name                                   AS filer_name,
        a.value_usd                                     AS acq_value_usd,
        t.value_usd                                     AS tgt_value_usd,
        a.report_date                                   AS acq_report_date,
        t.report_date                                   AS tgt_report_date
    FROM acquirer_leg a
    JOIN target_leg   t USING (holder_name)
    """
)


# 3. Quick-exit detection: has the filer's NEXT 13F after the
#    announcement_date got a zero (or missing) position on the target?
_QUICK_EXIT_SQL = text(
    """
    WITH next_reports AS (
        SELECT DISTINCT report_date
        FROM institutional_holdings
        WHERE ticker      = :target
          AND report_date > :announcement_date
        ORDER BY report_date
        LIMIT 1
    )
    SELECT
        (SELECT report_date FROM next_reports LIMIT 1) AS next_date,
        (
            SELECT COALESCE(SUM(value_usd), 0)
            FROM institutional_holdings
            WHERE ticker      = :target
              AND holder_name = :filer
              AND report_date = (SELECT report_date FROM next_reports LIMIT 1)
        )                                               AS next_value_usd
    """
)


_UPSERT_SQL = text(
    """
    INSERT INTO holder_deal_overlap (
        deal_announcement_date, acquirer_ticker, target_ticker,
        filer_name, acquirer_position_value_usd,
        target_position_value_usd, holding_report_date,
        days_before_announcement, pre_position_flag,
        quick_exit_flag, narrative, as_of
    ) VALUES (
        :deal_announcement_date, :acquirer_ticker, :target_ticker,
        :filer_name, :acquirer_position_value_usd,
        :target_position_value_usd, :holding_report_date,
        :days_before_announcement, :pre_position_flag,
        :quick_exit_flag, :narrative, NOW()
    )
    ON CONFLICT (
        deal_announcement_date, acquirer_ticker, target_ticker,
        filer_name
    ) DO UPDATE SET
        acquirer_position_value_usd = EXCLUDED.acquirer_position_value_usd,
        target_position_value_usd   = EXCLUDED.target_position_value_usd,
        holding_report_date         = EXCLUDED.holding_report_date,
        days_before_announcement    = EXCLUDED.days_before_announcement,
        pre_position_flag           = EXCLUDED.pre_position_flag,
        quick_exit_flag             = EXCLUDED.quick_exit_flag,
        narrative                   = EXCLUDED.narrative,
        as_of                       = NOW()
    """
)


# ── Public API ───────────────────────────────────────────────────────


def find_deals(engine: Engine) -> list[dict[str, Any]]:
    """Return every acquisition announcement with a non-null target."""
    with engine.connect() as conn:
        rows = conn.execute(_DEALS_SQL).fetchall()
    return [
        {
            "announcement_date": r[0],
            "acquirer_ticker": r[1],
            "target_ticker": r[2],
            "deal_size_usd": float(r[3]) if r[3] is not None else None,
        }
        for r in rows
        if r[0] is not None
    ]


def _normalize_ticker(t: str) -> str:
    """capital_flows stores actor_id as lowercase slugs; institutional_holdings
    stores ticker as uppercase. Strip and uppercase so joins line up.
    Non-ticker slugs (e.g. ``wiz_private``) pass through unchanged so
    the lookup cleanly misses instead of matching something wrong.
    """
    if not t:
        return t
    s = t.strip()
    # Leave private-company slugs alone: they contain underscores or
    # mixed-case tokens that wouldn't round-trip through an exchange
    # ticker. Exchange tickers are alphanumeric, <= 6 chars, no
    # underscores.
    if "_" in s or len(s) > 6:
        return s
    return s.upper()


def detect_overlap_for_deal(
    engine: Engine,
    *,
    announcement_date: date,
    acquirer_ticker: str,
    target_ticker: str,
    min_position_usd: float = MIN_POSITION_USD,
) -> list[OverlapRow]:
    """Find filers that held BOTH legs before one specific deal.

    Returns a list of ``OverlapRow`` records, one per overlapping
    filer. ``pre_position_flag`` is True when the weaker leg is above
    ``min_position_usd``. ``quick_exit_flag`` is resolved only for
    pre-positioned filers.
    """
    min_date = date.fromordinal(
        max(1, announcement_date.toordinal() - LOOKBACK_DAYS)
    )
    acquirer_norm = _normalize_ticker(acquirer_ticker)
    target_norm = _normalize_ticker(target_ticker)

    out: list[OverlapRow] = []
    with engine.connect() as conn:
        legs = conn.execute(
            _LEG_SQL,
            {
                "acquirer": acquirer_norm,
                "target": target_norm,
                "announcement_date": announcement_date,
                "min_date": min_date,
            },
        ).fetchall()

        for leg in legs:
            filer = leg[0]
            acq_val = float(leg[1]) if leg[1] is not None else None
            tgt_val = float(leg[2]) if leg[2] is not None else None
            acq_rd = leg[3]
            tgt_rd = leg[4]

            # Anchor the holding report date on the later of the two
            # per-leg snapshots (more recent = more relevant).
            holding_rd = max(
                [d for d in (acq_rd, tgt_rd) if d is not None],
                default=None,
            )
            days_before = (
                (announcement_date - holding_rd).days
                if holding_rd is not None
                else None
            )

            weaker = min(
                v for v in (acq_val, tgt_val) if v is not None
            ) if (acq_val is not None and tgt_val is not None) else 0.0
            pre_flag = weaker >= min_position_usd

            quick_exit = False
            if pre_flag:
                try:
                    row = conn.execute(
                        _QUICK_EXIT_SQL,
                        {
                            "target": target_norm,
                            "announcement_date": announcement_date,
                            "filer": filer,
                        },
                    ).fetchone()
                    next_date = row[0] if row else None
                    next_val = float(row[1]) if row and row[1] is not None else 0.0
                    quick_exit = bool(next_date is not None and next_val == 0.0)
                except Exception as exc:
                    log.debug(
                        "quick_exit lookup failed for {f}/{t}: {e}",
                        f=filer, t=target_ticker, e=str(exc),
                    )

            narrative = _build_narrative(
                filer=filer,
                acquirer=acquirer_norm,
                target=target_norm,
                announcement_date=announcement_date,
                acq_val=acq_val,
                tgt_val=tgt_val,
                days_before=days_before,
                pre_flag=pre_flag,
                quick_exit=quick_exit,
            )

            out.append(
                OverlapRow(
                    deal_announcement_date=announcement_date,
                    acquirer_ticker=acquirer_norm,
                    target_ticker=target_norm,
                    filer_name=filer,
                    acquirer_position_value_usd=acq_val,
                    target_position_value_usd=tgt_val,
                    holding_report_date=holding_rd,
                    days_before_announcement=days_before,
                    pre_position_flag=pre_flag,
                    quick_exit_flag=quick_exit,
                    narrative=narrative,
                )
            )
    return out


def _build_narrative(
    *,
    filer: str,
    acquirer: str,
    target: str,
    announcement_date: date,
    acq_val: float | None,
    tgt_val: float | None,
    days_before: int | None,
    pre_flag: bool,
    quick_exit: bool,
) -> str:
    """Human-readable summary for the dashboard drawer."""
    acq_s = f"${acq_val / 1e6:.1f}M" if acq_val else "unknown"
    tgt_s = f"${tgt_val / 1e6:.1f}M" if tgt_val else "unknown"
    days_s = f"{days_before}d" if days_before is not None else "unknown"
    verdict = (
        "PRE-POSITIONED"
        if pre_flag
        else "overlap-below-threshold"
    )
    exit_s = " + QUICK EXIT" if quick_exit else ""
    return (
        f"{filer} held {acquirer} ({acq_s}) and {target} ({tgt_s}) "
        f"{days_s} before the {announcement_date.isoformat()} "
        f"acquisition announcement — {verdict}{exit_s}."
    )


def upsert_rows(engine: Engine, rows: list[OverlapRow]) -> int:
    """Upsert a batch of overlap rows. Returns count written.

    SYNTH-24: each pre-positioned overlap row also fires a non-fatal
    ``SignalFired`` contract so the oracle's signal_sources projection
    picks it up on the next prediction cycle. The emit call is
    completely decoupled from the DB write — an emit failure never
    aborts the upsert.
    """
    if not rows:
        return 0
    written = 0
    with engine.begin() as conn:
        for r in rows:
            conn.execute(
                _UPSERT_SQL,
                {
                    "deal_announcement_date": r.deal_announcement_date,
                    "acquirer_ticker": r.acquirer_ticker,
                    "target_ticker": r.target_ticker,
                    "filer_name": r.filer_name,
                    "acquirer_position_value_usd": r.acquirer_position_value_usd,
                    "target_position_value_usd": r.target_position_value_usd,
                    "holding_report_date": r.holding_report_date,
                    "days_before_announcement": r.days_before_announcement,
                    "pre_position_flag": r.pre_position_flag,
                    "quick_exit_flag": r.quick_exit_flag,
                    "narrative": r.narrative,
                },
            )
            written += 1

    # Non-fatal SignalFired fanout (SYNTH-24). Only pre-positioned
    # overlaps are promoted — raw scans without a material position are
    # too noisy to feed oracle directly.
    for r in rows:
        if not r.pre_position_flag:
            continue
        try:
            _emit_holder_overlap_signal(r)
        except Exception as exc:  # pragma: no cover - defensive only
            log.debug(
                "holder_deal_overlap emit skipped for {a}/{t}: {e}",
                a=r.acquirer_ticker, t=r.target_ticker, e=str(exc),
            )

    return written


def _emit_holder_overlap_signal(r: OverlapRow) -> None:
    """Emit a ``SignalFired`` contract for one pre-positioned overlap row.

    Strength is +1 when the filer also quick-exited post-announcement
    (strongest confirmation that the overlap was intentional), +0.5
    otherwise. We intentionally do NOT try to resolve a ``raw_row_ids``
    list — the overlap is derived from a JOIN across two tables, so we
    record the ticker pair in the source string instead.
    """
    from uuid import uuid4

    from contracts.correlation import get_current_correlation_id, new_correlation_id
    from contracts.emit import emit as _emit
    from contracts.schemas import SignalFired

    strength = 1.0 if r.quick_exit_flag else 0.5
    corr_id = get_current_correlation_id() or new_correlation_id()
    # Emit one signal per leg — both tickers inherit the pre-position
    # confirmation. The oracle handler normalises to upper case.
    for ticker in (r.acquirer_ticker, r.target_ticker):
        if not ticker:
            continue
        _emit(
            SignalFired(
                producer_module="intelligence.holder_deal_overlap",
                correlation_id=corr_id,
                signal_id=uuid4(),
                source=f"holder_deal_overlap:{r.filer_name}",
                signal_type="holder_overlap",
                strength=strength,
                ticker=ticker,
                actor_hint=r.filer_name,
                raw_row_ids=[],
            )
        )


# ── Orchestrator ─────────────────────────────────────────────────────


@dataclass
class RunStats:
    """Telemetry for one full pass of the detector."""

    deals_scanned: int = 0
    overlaps_written: int = 0
    pre_positioned: int = 0
    quick_exits: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "deals_scanned": self.deals_scanned,
            "overlaps_written": self.overlaps_written,
            "pre_positioned": self.pre_positioned,
            "quick_exits": self.quick_exits,
            "errors": list(self.errors),
        }


def run(
    engine: Engine,
    *,
    min_position_usd: float = MIN_POSITION_USD,
) -> dict[str, Any]:
    """Full detection pass: scan deals, detect overlaps, upsert rows."""
    stats = RunStats()
    deals = find_deals(engine)
    stats.deals_scanned = len(deals)

    for deal in deals:
        try:
            rows = detect_overlap_for_deal(
                engine,
                announcement_date=deal["announcement_date"],
                acquirer_ticker=deal["acquirer_ticker"],
                target_ticker=deal["target_ticker"],
                min_position_usd=min_position_usd,
            )
            if not rows:
                continue
            written = upsert_rows(engine, rows)
            stats.overlaps_written += written
            stats.pre_positioned += sum(1 for r in rows if r.pre_position_flag)
            stats.quick_exits += sum(1 for r in rows if r.quick_exit_flag)
        except Exception as exc:
            msg = (
                f"overlap scan failed for "
                f"{deal['acquirer_ticker']}/{deal['target_ticker']}: {exc}"
            )
            log.warning(msg)
            stats.errors.append(msg)

    log.info(
        "holder_deal_overlap: deals={d} overlaps={o} pre={p} exits={q}",
        d=stats.deals_scanned,
        o=stats.overlaps_written,
        p=stats.pre_positioned,
        q=stats.quick_exits,
    )
    return stats.to_dict()


# ── Drawer helper: read rows for one actor ──────────────────────────


def fetch_overlaps_for_actor(
    engine: Engine,
    actor_id: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return pre-positioned overlap rows touching one ticker.

    Used by ``api/routers/actor_detail.py`` to expose the
    ``pre_positioned_by_filers`` payload on the drawer.
    """
    sql = text(
        """
        SELECT deal_announcement_date, acquirer_ticker, target_ticker,
               filer_name, acquirer_position_value_usd,
               target_position_value_usd, holding_report_date,
               days_before_announcement, pre_position_flag,
               quick_exit_flag, narrative
        FROM holder_deal_overlap
        WHERE pre_position_flag = true
          AND (acquirer_ticker = :a OR target_ticker = :a)
        ORDER BY deal_announcement_date DESC
        LIMIT :lim
        """
    )
    out: list[dict[str, Any]] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql, {"a": _normalize_ticker(actor_id), "lim": limit}
            ).fetchall()
            for r in rows:
                out.append(
                    {
                        "deal_announcement_date": str(r[0]) if r[0] else None,
                        "acquirer_ticker": r[1],
                        "target_ticker": r[2],
                        "filer_name": r[3],
                        "acquirer_position_value_usd": (
                            float(r[4]) if r[4] is not None else None
                        ),
                        "target_position_value_usd": (
                            float(r[5]) if r[5] is not None else None
                        ),
                        "holding_report_date": str(r[6]) if r[6] else None,
                        "days_before_announcement": (
                            int(r[7]) if r[7] is not None else None
                        ),
                        "pre_position_flag": bool(r[8]),
                        "quick_exit_flag": bool(r[9]),
                        "narrative": r[10],
                    }
                )
    except Exception as exc:
        log.debug(
            "fetch_overlaps_for_actor failed for {a}: {e}",
            a=actor_id, e=str(exc),
        )
    return out
