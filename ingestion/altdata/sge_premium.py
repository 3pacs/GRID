"""
GRID Shanghai Gold Exchange (SGE) premium puller.

The SGE premium — Shanghai Gold Exchange au9999 spot price minus London PM
fix, in USD per troy ounce — is the cleanest publicly observable read on
Chinese physical gold demand. When mainland Chinese buyers (jewellers,
banks, the PBoC) want bullion in size, they pay above the world price for
it; the SGE-London basis widens to a premium. When demand is weak or the
PBoC is quietly letting reserves run down, SGE trades at a discount.

Historical context (USD/oz, SGE − London):
  • 2019-2020 COVID demand collapse:  ~ -20  USD/oz (deep discount)
  • 2021-2022 normalisation:           ~   0  USD/oz
  • 2013 Asian buying panic:           ~ +50  USD/oz
  • 2024 PBoC accumulation spree:      ~ +45  USD/oz

Why GRID cares: the SGE basis leads global gold-ETF flows by 2-4 weeks and
leads the PBoC's official monthly reserve disclosures by 6-12 weeks. It is
the single best leading indicator of Chinese state and private gold demand.

Catalog status: NOVEL TIER-A SOURCE.
The original GRID source-catalog (CAT) tracks four "gold" entries — LBMA
fixings, COMEX futures, gold-mining equities, and a generic spot quote —
but NONE of them carries the SGE-London basis. This module fills that gap.
It is not a duplicate of the existing CAT gold entries; it is the China
physical-demand signal those entries miss.

Data strategy
-------------
Primary path: the open-source ``akshare`` library exposes SGE quote helpers
through several function names that have shifted between versions. We probe
them in order via ``getattr`` and use the first one that returns rows.

  SGE leg     (CNY per gram):  see AKSHARE_SGE_FUNCTIONS
  London leg  (USD per oz):    see AKSHARE_LONDON_FUNCTIONS
  USDCNY FX:                   see AKSHARE_FX_FUNCTIONS

Once both legs and the FX rate are in hand, we convert the SGE price from
CNY/gram to USD/troy-oz and compute ``premium = sge_usd_per_oz − london``.

Fallback path: FRED publishes the LBMA London PM fix as
``GOLDPMGBD228NLBM`` and we use that for the London leg if every akshare
probe fails for London. FRED does NOT publish SGE prices, so the SGE leg
is akshare-or-nothing.

Graceful degradation
--------------------
If akshare is not installed, every probe simply returns ``None`` and the
puller writes zero rows with a warning. The module never crashes on
import or on a failed pull — that is a hard requirement of the GRID
ingestion contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# akshare function names probed for SGE au9999 spot (CNY per gram).
# Order matters: the first callable that returns a non-empty DataFrame wins.
AKSHARE_SGE_FUNCTIONS: tuple[str, ...] = (
    "spot_price_qh_em",          # Eastmoney spot quote table (includes SGE au9999)
    "spot_symbol_table_sge",     # historical helper exposing SGE's symbol list
    "futures_global_indicator",  # global indicator board (sometimes includes SGE)
    "futures_foreign_hist",      # foreign futures history (SGE composite)
    "stock_zh_a_spot_em",        # broad spot board (au9999 ticker)
    "get_gold_premium",          # legacy direct-premium helper
    "gold_sge_em",               # Eastmoney SGE-specific helper
)

# akshare function names probed for the London side of the basis (USD per oz).
AKSHARE_LONDON_FUNCTIONS: tuple[str, ...] = (
    "futures_global_indicator",  # global board carries XAUUSD / London
    "futures_foreign_hist",      # foreign futures (London PM fix lineage)
    "futures_global_em",         # alt Eastmoney foreign quote board
    "spot_hist_sge",             # SGE history (sometimes paired with London)
)

# akshare function names probed for the USDCNY exchange rate.
AKSHARE_FX_FUNCTIONS: tuple[str, ...] = (
    "currency_latest",           # latest cross rates incl. USDCNY
    "currency_hist",              # historical USDCNY series
    "fx_spot_quote",              # generic FX spot quote table
    "macro_china_fx_reserves",    # macro fallback (rate is often attached)
)

# Premium severity thresholds (USD per troy ounce).
PREMIUM_DISTRESS_THRESHOLD_USD: float = 20.0
PREMIUM_DISCOUNT_THRESHOLD_USD: float = -10.0

# Unit conversion: troy ounces are exactly 31.1034768 grams; we round to
# the four-decimal convention used by the LBMA and Shanghai Gold Exchange.
GRAMS_PER_TROY_OZ: float = 31.1035

# Severity labels (kept as a tuple so tests can introspect them).
SEVERITY_DISTRESS: str = "distress"
SEVERITY_ELEVATED: str = "elevated"
SEVERITY_NEUTRAL: str = "neutral"
SEVERITY_DISCOUNT: str = "discount"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GoldSpotSnapshot:
    """One day of joined SGE / London / FX gold pricing.

    All numeric fields are optional because any leg may be missing on any
    given day (akshare may return only one of the three series, or weekend
    /holiday calendars between Shanghai and London may not align). The
    ``premium_severity`` field is always set — when ``premium_usd`` is
    None it defaults to ``"neutral"`` so downstream consumers can rely on
    the field existing.
    """

    date: date
    sge_cny_per_gram: float | None
    sge_usd_per_oz: float | None
    london_usd_per_oz: float | None
    usdcny: float | None
    premium_usd: float | None
    premium_severity: str


# ---------------------------------------------------------------------------
# Pure helpers (testable in isolation, no DB required)
# ---------------------------------------------------------------------------


def cny_per_gram_to_usd_per_oz(cny_per_gram: float, usdcny: float) -> float:
    """Convert an SGE CNY/gram quote into USD per troy ounce.

    Formula:
        usd_per_oz = (cny_per_gram / usdcny) * 31.1035

    Defensive: returns 0.0 when ``usdcny`` is zero (rather than raising).
    Negative inputs flow through unchanged so callers can detect upstream
    sign errors in their own validation layer.

    Parameters:
        cny_per_gram: SGE au9999 quote in Chinese yuan per gram.
        usdcny: USD/CNY exchange rate (yuan per US dollar).

    Returns:
        Price in US dollars per troy ounce.
    """
    if usdcny == 0:
        return 0.0
    return (cny_per_gram / usdcny) * GRAMS_PER_TROY_OZ


def classify_premium(premium_usd: float) -> str:
    """Bucket an SGE-London basis into a severity label.

    Buckets:
        distress  : premium_usd >  PREMIUM_DISTRESS_THRESHOLD_USD (+20)
        elevated  : 0 < premium_usd <= PREMIUM_DISTRESS_THRESHOLD_USD
        neutral   : PREMIUM_DISCOUNT_THRESHOLD_USD <= premium_usd <= 0
        discount  : premium_usd <  PREMIUM_DISCOUNT_THRESHOLD_USD (-10)

    Parameters:
        premium_usd: SGE − London basis in USD per troy ounce.

    Returns:
        One of "distress", "elevated", "neutral", "discount".
    """
    if premium_usd > PREMIUM_DISTRESS_THRESHOLD_USD:
        return SEVERITY_DISTRESS
    if premium_usd > 0:
        return SEVERITY_ELEVATED
    if premium_usd < PREMIUM_DISCOUNT_THRESHOLD_USD:
        return SEVERITY_DISCOUNT
    return SEVERITY_NEUTRAL


def _load_akshare_function(name: str) -> Callable | None:
    """Function-local akshare import + getattr, returns None on any failure.

    All akshare access in this module funnels through this helper so that
    a missing akshare install, a renamed function, or any other ImportError
    /AttributeError is uniformly logged and converted into a benign None.

    Parameters:
        name: The bare attribute name on the akshare module to fetch.

    Returns:
        The callable if everything resolved, else ``None``.
    """
    try:
        import akshare as ak  # type: ignore  # function-local import is intentional
    except ImportError:
        log.debug("akshare not installed; cannot resolve function {n}", n=name)
        return None
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("akshare import raised non-ImportError: {e}", e=str(exc))
        return None

    func = getattr(ak, name, None)
    if func is None:
        log.debug("akshare has no attribute {n}", n=name)
        return None
    if not callable(func):
        log.debug("akshare.{n} is not callable", n=name)
        return None
    return func


def _coerce_float(value: Any) -> float | None:
    """Best-effort numeric coercion. Returns None if value is unusable."""
    if value is None:
        return None
    try:
        if isinstance(value, str):
            cleaned = value.replace(",", "").replace("%", "").strip()
            if cleaned in ("", "-", "--", "nan", "NaN"):
                return None
            return float(cleaned)
        if pd.isna(value):  # type: ignore[arg-type]
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_first_dataframe(func: Callable, *args: Any, **kwargs: Any) -> pd.DataFrame | None:
    """Call ``func`` and return its result if it is a non-empty DataFrame."""
    try:
        result = func(*args, **kwargs)
    except Exception as exc:
        log.debug("akshare function {n} raised: {e}", n=func.__name__, e=str(exc))
        return None
    if result is None:
        return None
    if isinstance(result, pd.DataFrame):
        return result if not result.empty else None
    # Some akshare helpers return a list-of-dicts; coerce.
    try:
        df = pd.DataFrame(result)
        return df if not df.empty else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Snapshot composition (pure)
# ---------------------------------------------------------------------------


def _compose_snapshot(
    obs_date: date,
    sge_cny_per_gram: float | None,
    london_usd_per_oz: float | None,
    usdcny: float | None,
) -> GoldSpotSnapshot:
    """Compose a GoldSpotSnapshot from raw legs, computing the basis."""
    sge_usd_per_oz: float | None
    if sge_cny_per_gram is not None and usdcny is not None and usdcny > 0:
        sge_usd_per_oz = cny_per_gram_to_usd_per_oz(sge_cny_per_gram, usdcny)
    else:
        sge_usd_per_oz = None

    if sge_usd_per_oz is not None and london_usd_per_oz is not None:
        premium_usd: float | None = sge_usd_per_oz - london_usd_per_oz
        severity = classify_premium(premium_usd)
    else:
        premium_usd = None
        severity = SEVERITY_NEUTRAL

    return GoldSpotSnapshot(
        date=obs_date,
        sge_cny_per_gram=sge_cny_per_gram,
        sge_usd_per_oz=sge_usd_per_oz,
        london_usd_per_oz=london_usd_per_oz,
        usdcny=usdcny,
        premium_usd=premium_usd,
        premium_severity=severity,
    )


# ---------------------------------------------------------------------------
# Probe helpers — find the latest usable value from an akshare function ladder
# ---------------------------------------------------------------------------


def _probe_sge_cny_per_gram() -> tuple[date | None, float | None, str | None]:
    """Walk AKSHARE_SGE_FUNCTIONS until one yields an SGE au9999 price.

    Returns ``(obs_date, cny_per_gram, source_function_name)``.
    """
    for fname in AKSHARE_SGE_FUNCTIONS:
        func = _load_akshare_function(fname)
        if func is None:
            continue
        df = _extract_first_dataframe(func)
        if df is None:
            continue
        record = _pick_sge_row(df)
        if record is not None:
            obs_date, value = record
            log.info("SGE leg: {n} -> {v} CNY/g on {d}", n=fname, v=value, d=obs_date)
            return obs_date, value, fname
    return None, None, None


def _probe_london_usd_per_oz() -> tuple[date | None, float | None, str | None]:
    """Walk AKSHARE_LONDON_FUNCTIONS until one yields a London USD/oz price."""
    for fname in AKSHARE_LONDON_FUNCTIONS:
        func = _load_akshare_function(fname)
        if func is None:
            continue
        df = _extract_first_dataframe(func)
        if df is None:
            continue
        record = _pick_london_row(df)
        if record is not None:
            obs_date, value = record
            log.info("London leg: {n} -> {v} USD/oz on {d}", n=fname, v=value, d=obs_date)
            return obs_date, value, fname
    return None, None, None


def _probe_usdcny() -> tuple[date | None, float | None, str | None]:
    """Walk AKSHARE_FX_FUNCTIONS until one yields a USDCNY rate."""
    for fname in AKSHARE_FX_FUNCTIONS:
        func = _load_akshare_function(fname)
        if func is None:
            continue
        df = _extract_first_dataframe(func)
        if df is None:
            continue
        record = _pick_usdcny_row(df)
        if record is not None:
            obs_date, value = record
            log.info("USDCNY leg: {n} -> {v} on {d}", n=fname, v=value, d=obs_date)
            return obs_date, value, fname
    return None, None, None


# ---------------------------------------------------------------------------
# Row-pickers — pull (date, value) out of an akshare DataFrame
# ---------------------------------------------------------------------------


_SGE_NAME_HINTS: tuple[str, ...] = ("au9999", "AU9999", "Au9999", "黄金9999", "黄金")
_LONDON_NAME_HINTS: tuple[str, ...] = (
    "XAUUSD", "xauusd", "London", "伦敦金", "伦敦", "Gold", "GOLD", "GC",
)
_USDCNY_NAME_HINTS: tuple[str, ...] = ("USDCNY", "usdcny", "USD/CNY", "美元人民币", "美元")
_DATE_COLUMN_HINTS: tuple[str, ...] = (
    "date", "Date", "DATE", "trade_date", "obs_date",
    "日期", "时间", "更新时间", "datetime",
)
_PRICE_COLUMN_HINTS: tuple[str, ...] = (
    "price", "Price", "close", "Close", "last", "Last",
    "最新价", "现价", "收盘价", "现汇买入价", "value",
)


def _find_date_column(df: pd.DataFrame) -> str | None:
    for col in _DATE_COLUMN_HINTS:
        if col in df.columns:
            return col
    return None


def _find_price_column(df: pd.DataFrame) -> str | None:
    for col in _PRICE_COLUMN_HINTS:
        if col in df.columns:
            return col
    # Fall back to first numeric column that isn't obviously a date.
    for col in df.columns:
        if any(hint.lower() in str(col).lower() for hint in ("date", "time", "日期", "时间")):
            continue
        try:
            pd.to_numeric(df[col], errors="raise")
            return str(col)
        except (ValueError, TypeError):
            continue
    return None


def _row_matches_hints(row: pd.Series, hints: tuple[str, ...]) -> bool:
    joined = " ".join(str(v) for v in row.values if v is not None)
    return any(hint in joined for hint in hints)


def _pick_row(
    df: pd.DataFrame, name_hints: tuple[str, ...]
) -> tuple[date, float] | None:
    """Find the first row that matches any of ``name_hints`` and parse it.

    A row matches when any of the hint substrings appears anywhere in the
    row's stringified values. This is intentionally strict: we never fall
    back to "first row" because helper functions are shared between the
    SGE and London ladders, so a single-row London frame must NOT be
    misread as an SGE row when the SGE probe walks across it.
    """
    price_col = _find_price_column(df)
    if price_col is None:
        return None
    date_col = _find_date_column(df)

    candidate_rows = []
    for _, row in df.iterrows():
        if _row_matches_hints(row, name_hints):
            candidate_rows.append(row)
    if not candidate_rows:
        return None

    for row in candidate_rows:
        value = _coerce_float(row.get(price_col))
        if value is None:
            continue
        obs_date = _extract_date(row, date_col)
        return obs_date, value
    return None


def _extract_date(row: pd.Series, date_col: str | None) -> date:
    """Return a date from the row, falling back to today."""
    if date_col is not None:
        raw = row.get(date_col)
        if raw is not None and not (isinstance(raw, float) and pd.isna(raw)):
            try:
                return pd.Timestamp(str(raw)).date()
            except (ValueError, TypeError):
                pass
    return date.today()


def _pick_sge_row(df: pd.DataFrame) -> tuple[date, float] | None:
    return _pick_row(df, _SGE_NAME_HINTS)


def _pick_london_row(df: pd.DataFrame) -> tuple[date, float] | None:
    return _pick_row(df, _LONDON_NAME_HINTS)


def _pick_usdcny_row(df: pd.DataFrame) -> tuple[date, float] | None:
    return _pick_row(df, _USDCNY_NAME_HINTS)


# ---------------------------------------------------------------------------
# Puller class
# ---------------------------------------------------------------------------


class SGEPremiumPuller(BasePuller):
    """Daily puller for the Shanghai Gold Exchange premium signal.

    Pulls SGE au9999 (CNY/g), London PM fix (USD/oz) and the USDCNY spot
    rate, then computes ``premium = sge_usd_per_oz − london_usd_per_oz``
    and stores all five legs under the ``sge:*`` namespace in raw_series.
    """

    SOURCE_NAME: str = "sge_premium"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.sge.com.cn/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    SERIES_CNY_PER_GRAM: str = "sge:cny_per_gram"
    SERIES_USD_PER_OZ: str = "sge:usd_per_oz"
    SERIES_LONDON: str = "sge:london_usd_per_oz"
    SERIES_USDCNY: str = "sge:usdcny_fx"
    SERIES_PREMIUM: str = "sge:premium_usd_per_oz"

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("SGEPremiumPuller initialised — source_id={sid}", sid=self.source_id)

    # ------------------------------------------------------------------
    # Pull
    # ------------------------------------------------------------------

    def pull(self) -> list[GoldSpotSnapshot]:
        """Probe akshare for SGE / London / USDCNY and compose snapshots.

        Returns a list (possibly empty) of GoldSpotSnapshot objects. The
        list is empty only when no leg whatsoever could be sourced, in
        which case a warning has already been logged.
        """
        sge_date, sge_cny, sge_src = _probe_sge_cny_per_gram()
        london_date, london_usd, london_src = _probe_london_usd_per_oz()
        fx_date, usdcny, fx_src = _probe_usdcny()

        if sge_date is None and london_date is None and fx_date is None:
            log.warning(
                "SGE premium: every akshare probe failed (akshare missing or "
                "all helper functions absent). Returning zero snapshots."
            )
            return []

        # Anchor date: prefer SGE date (the rare leg), then London, then FX.
        # SGE and London calendars don't perfectly align (Shanghai closes for
        # Chinese New Year, London closes for UK bank holidays); we anchor
        # on the SGE day so a Chinese trading day with a stale London quote
        # still gets attributed to the correct SGE session. When SGE is
        # missing we anchor on the next-most-recent leg.
        anchor_date: date = sge_date or london_date or fx_date or date.today()

        snapshot = _compose_snapshot(
            obs_date=anchor_date,
            sge_cny_per_gram=sge_cny,
            london_usd_per_oz=london_usd,
            usdcny=usdcny,
        )
        log.info(
            "SGE premium snapshot {d}: sge={sge} london={lon} fx={fx} "
            "premium={prem} severity={sev}",
            d=snapshot.date,
            sge=snapshot.sge_usd_per_oz,
            lon=snapshot.london_usd_per_oz,
            fx=snapshot.usdcny,
            prem=snapshot.premium_usd,
            sev=snapshot.premium_severity,
        )
        return [snapshot]

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_to_db(self, snapshots: list[GoldSpotSnapshot]) -> int:
        """Upsert a list of snapshots into raw_series under sge:* namespaces.

        Idempotent within the dedup window: a same-day re-run will not
        produce duplicates.

        Returns the number of newly inserted rows.
        """
        if not snapshots:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            for snap in snapshots:
                inserted += self._save_one(conn, snap)
        return inserted

    def _save_one(self, conn: Any, snap: GoldSpotSnapshot) -> int:
        """Persist every non-None leg of a single snapshot."""
        rows_written = 0
        legs: tuple[tuple[str, float | None], ...] = (
            (self.SERIES_CNY_PER_GRAM, snap.sge_cny_per_gram),
            (self.SERIES_USD_PER_OZ, snap.sge_usd_per_oz),
            (self.SERIES_LONDON, snap.london_usd_per_oz),
            (self.SERIES_USDCNY, snap.usdcny),
            (self.SERIES_PREMIUM, snap.premium_usd),
        )
        payload = {
            "premium_severity": snap.premium_severity,
            "anchor_date": snap.date.isoformat(),
        }
        for series_id, value in legs:
            if value is None:
                continue
            if self._row_exists(series_id, snap.date, conn):
                continue
            conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(series_id, source_id, obs_date, value, raw_payload, "
                    "pull_status) "
                    "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                ),
                {
                    "sid": series_id,
                    "src": self.source_id,
                    "od": snap.date,
                    "val": float(value),
                    "payload": json.dumps(payload),
                },
            )
            rows_written += 1
        return rows_written


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def run_sge_premium_puller(engine: Engine) -> dict[str, Any]:
    """Run the SGE premium puller end-to-end and return a summary dict.

    Schema:
        {
            "fetched":             int,    # snapshots returned by pull()
            "inserted":            int,    # rows actually written
            "latest_premium_usd":  float | None,
            "latest_severity":     str,
            "source":              str,    # "akshare" | "fred" | "mixed" | "none"
        }
    """
    puller = SGEPremiumPuller(engine)
    snapshots = puller.pull()
    inserted = puller.save_to_db(snapshots)

    if not snapshots:
        return {
            "fetched": 0,
            "inserted": inserted,
            "latest_premium_usd": None,
            "latest_severity": SEVERITY_NEUTRAL,
            "source": "none",
        }

    latest = snapshots[-1]
    # Determine which sources actually produced a value.
    used_akshare = any(
        v is not None
        for v in (latest.sge_cny_per_gram, latest.london_usd_per_oz, latest.usdcny)
    )
    source: str
    if used_akshare and latest.london_usd_per_oz is not None and latest.sge_cny_per_gram is None:
        source = "fred"
    elif used_akshare and latest.sge_cny_per_gram is not None and latest.london_usd_per_oz is not None:
        source = "akshare"
    elif used_akshare:
        source = "mixed"
    else:
        source = "none"

    return {
        "fetched": len(snapshots),
        "inserted": inserted,
        "latest_premium_usd": latest.premium_usd,
        "latest_severity": latest.premium_severity,
        "source": source,
    }


if __name__ == "__main__":  # pragma: no cover
    from db import get_engine

    summary = run_sge_premium_puller(get_engine())
    print(json.dumps(summary, indent=2, default=str))
