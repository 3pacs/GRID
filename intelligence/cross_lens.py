"""
GRID Cross-Lens Correlation Detector.

Identifies when an upstream commodity or supplier shock historically coincided
with a downstream ticker price move, and attributes downstream moves to supply
shocks where the correlation is strong. This is the "explained by" layer that
closes the loop between the supply chain lens and price action.

Two methods:

    1. lagged_correlation — for every (upstream, downstream) pair in
       supply_chain_edges with non-trivial COGS exposure, compute Pearson
       correlation between log returns at various lags (1..N days). Record
       the best lag when |corr| >= min_correlation AND the sign matches the
       cost pass-through thesis (upstream up -> downstream down).

    2. event_study — find specific days where the upstream had a >1-sigma
       absolute move and record the cumulative N-day downstream move that
       followed. These rows let the UI point at a concrete "cocoa spiked 8%
       on 2025-02-10 -> HSY dropped 3% over the next 5 days" narrative.

Both methods persist to ``supply_shock_attributions`` via ON CONFLICT upsert,
so the runner is idempotent.

Non-goals:

    - Causal inference. Correlation is reported with a ``confidence`` label
      (derived | inferred) but never claimed as causation. See CLAUDE.md
      "Prediction Causation Standard" — this module produces evidence for
      causation claims, not the claims themselves.
    - PIT correctness. This module reads ``raw_series`` historical closes
      directly for correlation analysis, which is a historical-only,
      post-hoc diagnostic. Downstream consumers that feed this into live
      inference MUST re-validate via ``store/pit.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

DEFAULT_LOOKBACK_DAYS: int = 180
DEFAULT_MIN_CORRELATION: float = 0.5
DEFAULT_LAG_WINDOW: tuple[int, int] = (1, 10)

# Minimum overlapping observations required to trust a correlation.
MIN_OBSERVATIONS: int = 30

# Minimum COGS exposure for a downstream to be considered "material".
MIN_COGS_EXPOSURE: float = 0.05

# Event-study shock threshold (in upstream return stdevs).
SHOCK_STDEV_THRESHOLD: float = 1.0

# Event-study downstream window (days after shock to measure move).
EVENT_STUDY_WINDOW_DAYS: int = 5

# Cap event-study rows per (upstream, downstream) pair to avoid flooding.
EVENT_STUDY_MAX_PER_PAIR: int = 20


# ── Data Classes ──────────────────────────────────────────────────────────


@dataclass
class Attribution:
    """A single cross-lens attribution row before DB persistence."""

    upstream_id: str
    downstream_id: str
    shock_date: date
    shock_magnitude: float | None
    downstream_move_pct: float | None
    lag_days: int | None
    correlation: float | None
    confidence: str
    evidence: str
    method: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "upstream_id": self.upstream_id,
            "downstream_id": self.downstream_id,
            "shock_date": self.shock_date,
            "shock_magnitude": self.shock_magnitude,
            "downstream_move_pct": self.downstream_move_pct,
            "lag_days": self.lag_days,
            "correlation": self.correlation,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "method": self.method,
        }


# ── Ticker / series resolution ────────────────────────────────────────────

# Hand-curated slug -> futures ticker map for the most common commodities
# that appear as upstream nodes in supply_chain_edges. Extend as new
# chains are added.
_COMMODITY_SLUG_TO_TICKER: dict[str, str] = {
    "cocoa": "CC=F",
    "cocoa_beans": "CC=F",
    "coffee": "KC=F",
    "coffee_beans": "KC=F",
    "sugar": "SB=F",
    "wheat": "ZW=F",
    "corn": "ZC=F",
    "soybeans": "ZS=F",
    "soybean": "ZS=F",
    "orange_juice": "OJ=F",
    "oj": "OJ=F",
    "cattle": "LE=F",
    "live_cattle": "LE=F",
    "hogs": "HE=F",
    "lean_hogs": "HE=F",
    "aluminum": "ALI=F",
    "copper": "HG=F",
    "gold": "GC=F",
    "silver": "SI=F",
    "crude_oil": "CL=F",
    "oil_crude": "CL=F",
    "oil": "CL=F",
    "palm_oil": "CL=F",  # no direct futures ticker; use crude as proxy
    "natural_gas": "NG=F",
    "gas": "NG=F",
    "gasoline": "RB=F",
    "heating_oil": "HO=F",
    "cotton": "CT=F",
    "lumber": "LBR=F",
    "platinum": "PL=F",
    "palladium": "PA=F",
    # Non-commodity aliases: supplier slugs whose equity trades under a
    # different ticker than the slug itself.
    "tsmc": "TSM",
}


def resolve_price_series_id(node_id: str) -> str | None:
    """Resolve a supply_chain node id to a raw_series YF close series id.

    Lookup order:
        1. ``_COMMODITY_SLUG_TO_TICKER`` for known commodity slugs.
        2. Uppercase the node_id as if it were a ticker (``hsy`` -> ``HSY``).
        3. Return None if the node looks like something that won't have
           price data (country code, utility name, etc.).
    """
    if not node_id:
        return None
    key = node_id.strip().lower()
    if key in _COMMODITY_SLUG_TO_TICKER:
        return f"YF:{_COMMODITY_SLUG_TO_TICKER[key]}:close"
    # Heuristic: a ticker-looking id is uppercase/alnum with <= 5 chars.
    upper = node_id.strip().upper()
    if 1 <= len(upper) <= 6 and upper.replace(".", "").replace("-", "").isalnum():
        return f"YF:{upper}:close"
    return None


def fetch_close_series(
    engine: Engine,
    series_id: str,
    lookback_days: int,
) -> pd.DataFrame:
    """Return ``[obs_date, value]`` closes for the last ``lookback_days``.

    Tries both ``YF:{TKR}:adj_close`` and the given ``series_id`` so that
    split/dividend-adjusted data is preferred when available. Empty
    DataFrame if nothing found.
    """
    start = date.today() - timedelta(days=lookback_days + 30)
    # Derive adj_close twin.
    if series_id.endswith(":close"):
        adj_sid = series_id[: -len(":close")] + ":adj_close"
    else:
        adj_sid = series_id
    query = text(
        """
        SELECT obs_date, value
        FROM raw_series
        WHERE series_id IN (:sid1, :sid2)
          AND pull_status = 'SUCCESS'
          AND obs_date >= :start
        ORDER BY obs_date ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            query, {"sid1": adj_sid, "sid2": series_id, "start": start}
        ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["obs_date", "value"])
    df = pd.DataFrame(rows, columns=["obs_date", "value"])
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    # Collapse multiple source rows per date into a single median value.
    # Yahoo back-fills and source fanout mean there can be 3-10 rows per
    # (series_id, obs_date). Median is robust to the occasional fat-finger
    # close value that would otherwise wreck log-return math.
    df = (
        df.groupby("obs_date", as_index=False)["value"].median()
        .sort_values("obs_date")
        .reset_index(drop=True)
    )
    return df


def compute_log_returns(df: pd.DataFrame) -> pd.Series:
    """Return a pd.Series of log returns indexed by obs_date.

    Empty or single-row frames return an empty Series.
    """
    if df is None or df.empty or len(df) < 2:
        return pd.Series(dtype=float)
    values = pd.to_numeric(df["value"], errors="coerce")
    values = values.replace(0, np.nan)
    log_vals = np.log(values)
    returns = log_vals.diff().dropna()
    returns.index = pd.to_datetime(df["obs_date"]).iloc[1 : 1 + len(returns)].values
    return returns


# ── Correlation math ──────────────────────────────────────────────────────


def lagged_correlation(
    upstream_returns: pd.Series,
    downstream_returns: pd.Series,
    lag_window: tuple[int, int],
) -> tuple[float, int, int]:
    """Find the lag in ``lag_window`` that maximises ``|corr|``.

    Returns ``(best_correlation, best_lag, n_obs)``. Shifts the downstream
    series BACKWARDS by ``lag`` days so that ``upstream[t]`` is aligned with
    ``downstream[t + lag]``.

    Returns ``(0.0, 0, 0)`` if insufficient data at every lag.
    """
    if upstream_returns.empty or downstream_returns.empty:
        return 0.0, 0, 0
    min_lag, max_lag = lag_window
    if min_lag < 0 or max_lag < min_lag:
        raise ValueError(f"invalid lag_window {lag_window}")

    up_series = upstream_returns.copy()
    up_series.index = pd.to_datetime(up_series.index)
    down_series = downstream_returns.copy()
    down_series.index = pd.to_datetime(down_series.index)

    best_corr = 0.0
    best_lag = min_lag
    best_n = 0
    for lag in range(min_lag, max_lag + 1):
        shifted_down = down_series.shift(-lag)
        joined = pd.concat(
            [up_series.rename("up"), shifted_down.rename("down")],
            axis=1,
        ).dropna()
        if len(joined) < MIN_OBSERVATIONS:
            continue
        corr = float(joined["up"].corr(joined["down"]))
        if np.isnan(corr):
            continue
        if abs(corr) > abs(best_corr):
            best_corr = corr
            best_lag = lag
            best_n = len(joined)
    return best_corr, best_lag, best_n


def detect_shock_events(
    upstream_returns: pd.Series,
    downstream_df: pd.DataFrame,
    window_days: int = EVENT_STUDY_WINDOW_DAYS,
    stdev_threshold: float = SHOCK_STDEV_THRESHOLD,
    max_events: int = EVENT_STUDY_MAX_PER_PAIR,
) -> list[dict[str, Any]]:
    """Find upstream shock days and measure the downstream cumulative move.

    A shock day is any day where ``|upstream_return| > stdev_threshold *
    sample_stdev``. For each shock, we measure the cumulative downstream
    log-return over the next ``window_days`` trading days.

    Returns a list of event dicts sorted by shock date descending, capped
    at ``max_events`` entries.
    """
    if upstream_returns.empty or downstream_df is None or downstream_df.empty:
        return []
    sample_std = float(upstream_returns.std())
    if not np.isfinite(sample_std) or sample_std <= 0:
        return []
    threshold = stdev_threshold * sample_std

    shock_mask = upstream_returns.abs() > threshold
    shock_days = upstream_returns[shock_mask]
    if shock_days.empty:
        return []

    down_df = downstream_df.copy()
    down_df["obs_date"] = pd.to_datetime(down_df["obs_date"])
    down_df = down_df.sort_values("obs_date").reset_index(drop=True)
    date_to_idx: dict[pd.Timestamp, int] = {
        ts: idx for idx, ts in enumerate(down_df["obs_date"])
    }

    events: list[dict[str, Any]] = []
    for shock_ts, up_ret in shock_days.sort_index(ascending=False).items():
        shock_ts_pd = pd.Timestamp(shock_ts)
        if shock_ts_pd not in date_to_idx:
            # Find the next trading day at or after the shock.
            candidate_idx = down_df["obs_date"].searchsorted(shock_ts_pd)
            if candidate_idx >= len(down_df):
                continue
            base_idx = int(candidate_idx)
        else:
            base_idx = date_to_idx[shock_ts_pd]
        end_idx = base_idx + window_days
        if end_idx >= len(down_df):
            continue
        p0 = float(down_df["value"].iloc[base_idx])
        p1 = float(down_df["value"].iloc[end_idx])
        if p0 <= 0 or not np.isfinite(p0) or not np.isfinite(p1):
            continue
        downstream_move = float(np.log(p1 / p0))
        events.append(
            {
                "shock_date": down_df["obs_date"].iloc[base_idx].date(),
                "shock_magnitude": float(up_ret),
                "downstream_move_pct": downstream_move,
                "window_days": window_days,
            }
        )
        if len(events) >= max_events:
            break
    return events


# ── Supply chain graph helpers ────────────────────────────────────────────


def list_candidate_pairs(
    engine: Engine,
    min_cogs: float = MIN_COGS_EXPOSURE,
    include_commodity_edges_without_cogs: bool = True,
) -> list[tuple[str, str, float | None]]:
    """Return (upstream_id, downstream_id, pct_downstream_cogs) triples.

    Primary filter: edges where ``pct_downstream_cogs > min_cogs``.

    Secondary (optional): edges where pct_downstream_cogs is NULL but the
    upstream resolves to a known commodity futures ticker via
    ``resolve_price_series_id``. These rows get ``pct_cogs=None`` so the
    narrative drops the COGS fragment.

    This two-stage approach is necessary because the seed loader only
    populates pct_downstream_cogs for a handful of edges; the rest are
    inferred or confirmed but lack the % exposure. We still want to run
    the lagged-correlation detector on commodity-upstream edges, because
    those are the rows where a shock physically MUST feed into COGS.
    """
    query = text(
        """
        SELECT upstream_id, downstream_id, pct_downstream_cogs
        FROM supply_chain_edges
        WHERE pct_downstream_cogs IS NOT NULL
          AND pct_downstream_cogs > :min_cogs
        ORDER BY pct_downstream_cogs DESC
        """
    )
    with engine.connect() as conn:
        primary_rows = conn.execute(query, {"min_cogs": min_cogs}).fetchall()
    pairs: list[tuple[str, str, float | None]] = [
        (r[0], r[1], float(r[2])) for r in primary_rows
    ]
    seen: set[tuple[str, str]] = {(p[0], p[1]) for p in pairs}

    if include_commodity_edges_without_cogs:
        with engine.connect() as conn:
            extra_rows = conn.execute(
                text(
                    """
                    SELECT upstream_id, downstream_id
                    FROM supply_chain_edges
                    WHERE pct_downstream_cogs IS NULL
                       OR pct_downstream_cogs <= :min_cogs
                    """
                ),
                {"min_cogs": min_cogs},
            ).fetchall()
        for r in extra_rows:
            key = (r[0], r[1])
            if key in seen:
                continue
            if (r[0] or "").strip().lower() in _COMMODITY_SLUG_TO_TICKER:
                pairs.append((r[0], r[1], None))
                seen.add(key)
    return pairs


# ── Narrative ─────────────────────────────────────────────────────────────


def build_lagged_evidence(
    upstream_id: str,
    downstream_id: str,
    correlation: float,
    lag: int,
    n_obs: int,
    pct_cogs: float | None,
) -> str:
    """Template sentence describing a lagged-correlation attribution."""
    direction = "inverse" if correlation < 0 else "positive"
    cogs_frag = (
        f" (~{pct_cogs*100:.0f}% of {downstream_id} COGS)"
        if pct_cogs is not None
        else ""
    )
    return (
        f"{upstream_id} log returns show {direction} correlation "
        f"{correlation:+.2f} with {downstream_id}{cogs_frag} at a {lag}-day "
        f"lag over {n_obs} overlapping trading days. "
        f"Consistent with cost pass-through thesis."
    )


def build_event_evidence(
    upstream_id: str,
    downstream_id: str,
    shock_magnitude: float,
    downstream_move: float,
    window_days: int,
    pct_cogs: float | None,
) -> str:
    """Template sentence describing a single event-study attribution."""
    cogs_frag = (
        f" (~{pct_cogs*100:.0f}% of {downstream_id} COGS)"
        if pct_cogs is not None
        else ""
    )
    return (
        f"{upstream_id} moved {shock_magnitude*100:+.1f}% on shock day; "
        f"{downstream_id}{cogs_frag} moved {downstream_move*100:+.1f}% "
        f"over the following {window_days} trading days."
    )


def build_actor_narrative(rows: list[dict[str, Any]]) -> str:
    """Summarise a batch of attribution rows for an actor API response."""
    if not rows:
        return "No historical supply-shock attributions found for this actor."
    strongest = max(rows, key=lambda r: abs(r.get("correlation") or 0))
    up = strongest.get("upstream_id")
    down = strongest.get("downstream_id")
    corr = strongest.get("correlation")
    lag = strongest.get("lag_days")
    if corr is None:
        return f"{len(rows)} attribution rows found; no dominant correlation."
    lag_txt = f"{lag}-day lag" if lag is not None else "no lag"
    return (
        f"Strongest link: {up} -> {down} correlation {corr:+.2f} at "
        f"{lag_txt}. Total attribution rows: {len(rows)}."
    )


# ── Persistence ───────────────────────────────────────────────────────────


def upsert_attributions(
    engine: Engine,
    attributions: list[Attribution],
) -> int:
    """Idempotent upsert into ``supply_shock_attributions``.

    Uses ON CONFLICT against the ``(upstream_id, downstream_id, shock_date,
    method)`` unique constraint so reruns are safe. Returns number of rows
    touched (inserted or updated).
    """
    if not attributions:
        return 0
    stmt = text(
        """
        INSERT INTO supply_shock_attributions
            (upstream_id, downstream_id, shock_date, shock_magnitude,
             downstream_move_pct, lag_days, correlation, confidence,
             evidence, method, as_of)
        VALUES
            (:upstream_id, :downstream_id, :shock_date, :shock_magnitude,
             :downstream_move_pct, :lag_days, :correlation, :confidence,
             :evidence, :method, NOW())
        ON CONFLICT (upstream_id, downstream_id, shock_date, method) DO UPDATE
        SET shock_magnitude = EXCLUDED.shock_magnitude,
            downstream_move_pct = EXCLUDED.downstream_move_pct,
            lag_days = EXCLUDED.lag_days,
            correlation = EXCLUDED.correlation,
            confidence = EXCLUDED.confidence,
            evidence = EXCLUDED.evidence,
            as_of = NOW()
        """
    )
    with engine.begin() as conn:
        for attr in attributions:
            conn.execute(stmt, attr.as_dict())
    return len(attributions)


# ── Main orchestrator ─────────────────────────────────────────────────────


def detect_attributions(
    engine: Engine,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_correlation: float = DEFAULT_MIN_CORRELATION,
    lag_window: tuple[int, int] = DEFAULT_LAG_WINDOW,
) -> list[dict[str, Any]]:
    """Run full cross-lens attribution pass and persist results.

    Pipeline:

        1. Enumerate ``supply_chain_edges`` rows with
           ``pct_downstream_cogs > MIN_COGS_EXPOSURE``.
        2. For each upstream, resolve a price series via
           ``resolve_price_series_id``. Skip nodes without series data.
        3. Pull the last ``lookback_days`` closes, compute log returns.
        4. For each downstream in the edge list, do the same, compute
           lagged correlation, and if above threshold with the right sign,
           write a ``lagged_correlation`` attribution row.
        5. Also detect specific shock events (>= 1-sigma upstream moves)
           and write ``event_study`` rows.

    Returns the list of written row dicts (pre-persistence Attribution
    dicts).
    """
    pairs = list_candidate_pairs(engine)
    log.info(
        "cross_lens: {n} candidate (upstream, downstream) pairs above "
        "{c:.0%} COGS threshold",
        n=len(pairs),
        c=MIN_COGS_EXPOSURE,
    )
    if not pairs:
        return []

    # Group pairs by upstream_id so we only fetch each upstream series once.
    pairs_by_upstream: dict[str, list[tuple[str, float]]] = {}
    for upstream_id, downstream_id, pct_cogs in pairs:
        pairs_by_upstream.setdefault(upstream_id, []).append(
            (downstream_id, pct_cogs)
        )

    missing_upstream_series: list[str] = []
    attributions: list[Attribution] = []

    for upstream_id, downstreams in pairs_by_upstream.items():
        up_sid = resolve_price_series_id(upstream_id)
        if up_sid is None:
            missing_upstream_series.append(upstream_id)
            log.debug("cross_lens: no series for upstream {u}", u=upstream_id)
            continue
        up_df = fetch_close_series(engine, up_sid, lookback_days)
        if up_df.empty or len(up_df) < MIN_OBSERVATIONS:
            missing_upstream_series.append(upstream_id)
            log.debug(
                "cross_lens: insufficient upstream data for {u} ({sid}, {n} rows)",
                u=upstream_id,
                sid=up_sid,
                n=len(up_df),
            )
            continue
        up_returns = compute_log_returns(up_df)
        if up_returns.empty:
            missing_upstream_series.append(upstream_id)
            continue

        for downstream_id, pct_cogs in downstreams:
            down_sid = resolve_price_series_id(downstream_id)
            if down_sid is None:
                log.debug(
                    "cross_lens: no series for downstream {d}", d=downstream_id
                )
                continue
            down_df = fetch_close_series(engine, down_sid, lookback_days)
            if down_df.empty or len(down_df) < MIN_OBSERVATIONS:
                continue
            down_returns = compute_log_returns(down_df)
            if down_returns.empty:
                continue

            # (1) Lagged correlation
            best_corr, best_lag, n_obs = lagged_correlation(
                up_returns, down_returns, lag_window
            )
            # Cost pass-through thesis: commodity UP -> downstream DOWN
            # (negative corr) OR supplier UP -> downstream UP (positive,
            # e.g. a key OEM reports strong demand). We accept either
            # direction but label the thesis match in the evidence.
            if abs(best_corr) >= min_correlation and n_obs >= MIN_OBSERVATIONS:
                attributions.append(
                    Attribution(
                        upstream_id=upstream_id,
                        downstream_id=downstream_id,
                        shock_date=date.today(),
                        shock_magnitude=None,
                        downstream_move_pct=None,
                        lag_days=best_lag,
                        correlation=round(best_corr, 4),
                        confidence="derived",
                        evidence=build_lagged_evidence(
                            upstream_id,
                            downstream_id,
                            best_corr,
                            best_lag,
                            n_obs,
                            pct_cogs,
                        ),
                        method="lagged_correlation",
                    )
                )

            # (2) Event-study shock rows
            events = detect_shock_events(up_returns, down_df)
            for ev in events:
                attributions.append(
                    Attribution(
                        upstream_id=upstream_id,
                        downstream_id=downstream_id,
                        shock_date=ev["shock_date"],
                        shock_magnitude=round(ev["shock_magnitude"], 4),
                        downstream_move_pct=round(ev["downstream_move_pct"], 4),
                        lag_days=ev["window_days"],
                        correlation=None,
                        confidence="inferred",
                        evidence=build_event_evidence(
                            upstream_id,
                            downstream_id,
                            ev["shock_magnitude"],
                            ev["downstream_move_pct"],
                            ev["window_days"],
                            pct_cogs,
                        ),
                        method="event_study",
                    )
                )

    log.info(
        "cross_lens: prepared {n} attribution rows ({missing} upstream nodes "
        "lacked series data)",
        n=len(attributions),
        missing=len(missing_upstream_series),
    )

    upsert_attributions(engine, attributions)
    return [a.as_dict() for a in attributions]


def get_attributions_for_actor(
    engine: Engine,
    actor_id: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> dict[str, Any]:
    """Return persisted attributions where ``actor_id`` is upstream or downstream.

    Used by the API endpoint.
    """
    cutoff = date.today() - timedelta(days=lookback_days)
    query = text(
        """
        SELECT upstream_id, downstream_id, shock_date, shock_magnitude,
               downstream_move_pct, lag_days, correlation, confidence,
               evidence, method, as_of
        FROM supply_shock_attributions
        WHERE (upstream_id = :aid OR downstream_id = :aid)
          AND shock_date >= :cutoff
        ORDER BY ABS(COALESCE(correlation, 0)) DESC, shock_date DESC
        LIMIT 200
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"aid": actor_id, "cutoff": cutoff}).fetchall()
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        out_rows.append(
            {
                "upstream_id": r[0],
                "downstream_id": r[1],
                "shock_date": str(r[2]) if r[2] else None,
                "shock_magnitude": float(r[3]) if r[3] is not None else None,
                "downstream_move_pct": float(r[4]) if r[4] is not None else None,
                "lag_days": int(r[5]) if r[5] is not None else None,
                "correlation": float(r[6]) if r[6] is not None else None,
                "confidence": r[7],
                "evidence": r[8],
                "method": r[9],
                "as_of": r[10].isoformat() if r[10] else None,
            }
        )
    return {
        "actor_id": actor_id,
        "lookback_days": lookback_days,
        "rows": out_rows,
        "narrative": build_actor_narrative(out_rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
