"""PBoC Open Market Operations + MLF renewals puller (CAT-3).

Why this matters
----------------
The People's Bank of China (PBoC) open market operations (OMO) are the
**dominant real-time Chinese monetary lever**. Each trading day the PBoC
conducts 7-day reverse repo operations that inject or withdraw CNY from
the interbank system, and once a month it rolls its Medium-term Lending
Facility (MLF) — the closest analogue to a Fed funds rate for China.

* Aggressive net injection (inj > withdrawal)  = stimulus → CNH/CNY bid,
  iron ore bid, SSE bid, copper bid (with a 3-10 day lag).
* Sustained net drain                          = tightening → CNH/CNY offer,
  industrial metals offer, property stocks offer.
* MLF rate cut / hike                          = front-runs the LPR (Loan
  Prime Rate) fix by ~5 days; every LPR move in the last three years was
  telegraphed first by MLF.

This puller feeds two downstream consumers:

* `features/fci.py` (CAT-124) — the global financial conditions index uses
  ``pboc:omo_net_cny_bn`` and ``pboc:reverse_repo_7d_rate`` as two of its
  four China inputs.
* `intelligence/global_growth_impulse` classifier — the net OMO sign over a
  trailing 10-day window is a feature in the credit-impulse regime model.

Design notes
------------
Akshare wraps the PBoC public data portal cleanly. Preferred functions
(as per task spec) are ``macro_china_cb_operation`` for OMO history and
``macro_china_mlf_rate`` for MLF history. The upstream akshare API has
churned names in the past, so the puller falls back to:

* ``repo_rate_hist`` (FR007 7-day repo rate) → proxy for reverse repo rate
* ``macro_china_lpr`` (LPR1Y) → proxy for MLF rate

If akshare is not installed, or every attempted call raises, the puller
logs a warning and returns a zero-row result — it never crashes the
scheduler process.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERIES_OMO_INJECTION = "pboc:omo_injection_cny_bn"
SERIES_OMO_WITHDRAWAL = "pboc:omo_withdrawal_cny_bn"
SERIES_OMO_NET = "pboc:omo_net_cny_bn"
SERIES_REVERSE_REPO_7D = "pboc:reverse_repo_7d_rate"
SERIES_MLF_RATE = "pboc:mlf_rate"
SERIES_MLF_NET = "pboc:mlf_net_cny_bn"

# Sentinels that the PBoC portal sometimes emits in place of numbers.
_NULL_SENTINELS: frozenset[str] = frozenset(
    {"", "-", "--", "—", "N/A", "n/a", "NA", "None", "null", "nan", "NaN"}
)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PBOCOmoSnapshot:
    """One day of PBoC open market operations.

    All CNY flows are in **billions** (CNY bn). A positive ``net_cny_bn``
    means the PBoC injected more liquidity than it drained that day.
    """

    date: date
    injection_cny_bn: float
    withdrawal_cny_bn: float
    net_cny_bn: float
    reverse_repo_7d_rate: float | None


@dataclass(frozen=True)
class MLFRenewal:
    """One PBoC Medium-term Lending Facility operation.

    ``maturing_cny_bn`` is the notional rolling off that day,
    ``renewed_cny_bn`` is the new MLF issued. Net = renewed - maturing;
    positive means the PBoC over-rolled (net injection to banks).
    """

    date: date
    maturing_cny_bn: float
    renewed_cny_bn: float
    rate_pct: float | None
    net_cny_bn: float


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_float(raw: Any) -> float | None:
    """Convert a PBoC / akshare cell to float, returning None on sentinels.

    Handles common sentinels ("—", "N/A", ""), thousand separators,
    and trailing "%" signs.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if isinstance(raw, float) and (raw != raw):  # NaN
            return None
        return float(raw)
    txt = str(raw).strip()
    if txt in _NULL_SENTINELS:
        return None
    txt = txt.replace(",", "").replace("%", "").replace(" ", "")
    try:
        return float(txt)
    except ValueError:
        return None


def _parse_date(raw: Any) -> date | None:
    """Parse a date from str / datetime / pandas Timestamp."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    try:
        return pd.Timestamp(str(raw)).date()
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------


class PBOCOmoPuller(BasePuller):
    """Pulls PBoC OMO + MLF history via akshare with graceful fallback."""

    SOURCE_NAME: str = "pboc_omo"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "http://www.pbc.gov.cn",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 15,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._omo_snapshots: list[PBOCOmoSnapshot] = []
        self._mlf_renewals: list[MLFRenewal] = []

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _fetch_omo_dataframe(self) -> pd.DataFrame | None:
        """Fetch OMO history.

        Tries ``macro_china_cb_operation`` first (preferred, per task spec),
        then falls back to ``repo_rate_hist`` for the 7-day repo rate series.
        Returns None on any failure.
        """
        try:
            import akshare as ak  # local import: missing akshare must not crash module
        except ImportError:
            log.warning("pboc_omo: akshare not installed — skipping OMO pull")
            return None

        # Preferred source: direct OMO operations history
        func = getattr(ak, "macro_china_cb_operation", None)
        if func is not None:
            try:
                df = func()
                if df is not None and not df.empty:
                    log.info("pboc_omo: loaded {n} OMO rows from macro_china_cb_operation", n=len(df))
                    return df
            except Exception as exc:
                log.warning("pboc_omo: macro_china_cb_operation failed: {e}", e=str(exc))

        # Fallback: interbank 7-day repo rate (proxy for PBoC 7-day reverse repo stance)
        func = getattr(ak, "repo_rate_hist", None)
        if func is not None:
            try:
                df = func()
                if df is not None and not df.empty:
                    log.info("pboc_omo: loaded {n} rows from repo_rate_hist (fallback)", n=len(df))
                    return df
            except Exception as exc:
                log.warning("pboc_omo: repo_rate_hist failed: {e}", e=str(exc))

        log.warning("pboc_omo: no OMO source available")
        return None

    def _fetch_mlf_dataframe(self) -> pd.DataFrame | None:
        """Fetch MLF history.

        Tries ``macro_china_mlf_rate`` (preferred, per task spec), then
        falls back to ``macro_china_lpr`` (LPR1Y is a downstream proxy).
        """
        try:
            import akshare as ak
        except ImportError:
            log.warning("pboc_omo: akshare not installed — skipping MLF pull")
            return None

        func = getattr(ak, "macro_china_mlf_rate", None)
        if func is not None:
            try:
                df = func()
                if df is not None and not df.empty:
                    log.info("pboc_omo: loaded {n} MLF rows from macro_china_mlf_rate", n=len(df))
                    return df
            except Exception as exc:
                log.warning("pboc_omo: macro_china_mlf_rate failed: {e}", e=str(exc))

        func = getattr(ak, "macro_china_lpr", None)
        if func is not None:
            try:
                df = func()
                if df is not None and not df.empty:
                    log.info("pboc_omo: loaded {n} rows from macro_china_lpr (fallback)", n=len(df))
                    return df
            except Exception as exc:
                log.warning("pboc_omo: macro_china_lpr failed: {e}", e=str(exc))

        log.warning("pboc_omo: no MLF source available")
        return None

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        """Return the first column in ``candidates`` present in ``df``."""
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _parse_omo(self, df: pd.DataFrame) -> list[PBOCOmoSnapshot]:
        """Convert a fetched OMO DataFrame into snapshots.

        Handles two schemas:

        * Preferred ``macro_china_cb_operation`` schema with Chinese
          injection/withdrawal columns (投放/回笼 = inject/withdraw).
        * Fallback ``repo_rate_hist`` schema — only carries the 7-day
          repo rate (FR007), so injection/withdrawal default to 0.
        """
        date_col = self._pick_column(df, ["date", "日期", "统计时间", "操作日期"])
        if date_col is None:
            log.warning("pboc_omo: no date column found in OMO df: {c}", c=list(df.columns))
            return []

        inj_col = self._pick_column(df, ["投放", "injection", "投放量", "reverse_repo_injection"])
        wit_col = self._pick_column(df, ["回笼", "withdrawal", "回笼量", "reverse_repo_withdrawal"])
        rate_col = self._pick_column(df, ["FR007", "中标利率", "rate", "reverse_repo_rate"])

        snapshots: list[PBOCOmoSnapshot] = []
        for _, row in df.iterrows():
            dt = _parse_date(row[date_col])
            if dt is None:
                continue
            injection = _parse_float(row[inj_col]) if inj_col else None
            withdrawal = _parse_float(row[wit_col]) if wit_col else None
            rate = _parse_float(row[rate_col]) if rate_col else None
            injection = injection or 0.0
            withdrawal = withdrawal or 0.0
            net = injection - withdrawal
            snapshots.append(
                PBOCOmoSnapshot(
                    date=dt,
                    injection_cny_bn=injection,
                    withdrawal_cny_bn=withdrawal,
                    net_cny_bn=net,
                    reverse_repo_7d_rate=rate,
                )
            )
        return snapshots

    def _parse_mlf(self, df: pd.DataFrame) -> list[MLFRenewal]:
        """Convert a fetched MLF DataFrame into renewals."""
        date_col = self._pick_column(
            df, ["date", "日期", "TRADE_DATE", "操作日期", "公布日期"]
        )
        if date_col is None:
            log.warning("pboc_omo: no date column found in MLF df: {c}", c=list(df.columns))
            return []

        mat_col = self._pick_column(df, ["到期", "maturing", "到期量", "maturing_cny_bn"])
        new_col = self._pick_column(df, ["操作量", "renewed", "投放", "new_cny_bn"])
        rate_col = self._pick_column(
            df, ["利率", "rate", "中标利率", "mlf_rate", "LPR1Y"]
        )

        renewals: list[MLFRenewal] = []
        for _, row in df.iterrows():
            dt = _parse_date(row[date_col])
            if dt is None:
                continue
            maturing = _parse_float(row[mat_col]) if mat_col else None
            renewed = _parse_float(row[new_col]) if new_col else None
            rate = _parse_float(row[rate_col]) if rate_col else None
            maturing = maturing or 0.0
            renewed = renewed or 0.0
            net = renewed - maturing
            renewals.append(
                MLFRenewal(
                    date=dt,
                    maturing_cny_bn=maturing,
                    renewed_cny_bn=renewed,
                    rate_pct=rate,
                    net_cny_bn=net,
                )
            )
        return renewals

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def pull(self) -> dict[str, Any]:
        """Fetch and parse PBoC OMO + MLF history.

        Populates ``self._omo_snapshots`` and ``self._mlf_renewals``.
        Returns a summary dict.
        """
        self._omo_snapshots = []
        self._mlf_renewals = []

        omo_df = self._fetch_omo_dataframe()
        if omo_df is not None:
            try:
                self._omo_snapshots = self._parse_omo(omo_df)
            except Exception as exc:
                log.warning("pboc_omo: parsing OMO df failed: {e}", e=str(exc))

        mlf_df = self._fetch_mlf_dataframe()
        if mlf_df is not None:
            try:
                self._mlf_renewals = self._parse_mlf(mlf_df)
            except Exception as exc:
                log.warning("pboc_omo: parsing MLF df failed: {e}", e=str(exc))

        return {
            "omo_rows": len(self._omo_snapshots),
            "mlf_rows": len(self._mlf_renewals),
        }

    def save_to_db(self) -> dict[str, int]:
        """Upsert parsed snapshots into raw_series.

        Idempotent: rows already present for the same (series_id, obs_date)
        pair on the same pull day are skipped via ``_row_exists``.
        """
        inserted = 0
        fetched = 0

        with self.engine.begin() as conn:
            for snap in self._omo_snapshots:
                fetched += 4
                for series_id, value in (
                    (SERIES_OMO_INJECTION, snap.injection_cny_bn),
                    (SERIES_OMO_WITHDRAWAL, snap.withdrawal_cny_bn),
                    (SERIES_OMO_NET, snap.net_cny_bn),
                ):
                    if self._row_exists(series_id, snap.date, conn):
                        continue
                    self._insert_raw(conn, series_id, snap.date, float(value))
                    inserted += 1
                if snap.reverse_repo_7d_rate is not None:
                    if not self._row_exists(SERIES_REVERSE_REPO_7D, snap.date, conn):
                        self._insert_raw(
                            conn,
                            SERIES_REVERSE_REPO_7D,
                            snap.date,
                            float(snap.reverse_repo_7d_rate),
                        )
                        inserted += 1

            for mlf in self._mlf_renewals:
                fetched += 2
                if not self._row_exists(SERIES_MLF_NET, mlf.date, conn):
                    self._insert_raw(
                        conn, SERIES_MLF_NET, mlf.date, float(mlf.net_cny_bn)
                    )
                    inserted += 1
                if mlf.rate_pct is not None:
                    if not self._row_exists(SERIES_MLF_RATE, mlf.date, conn):
                        self._insert_raw(
                            conn,
                            SERIES_MLF_RATE,
                            mlf.date,
                            float(mlf.rate_pct),
                        )
                        inserted += 1

        return {"fetched": fetched, "inserted": inserted}


# ---------------------------------------------------------------------------
# Module entrypoint
# ---------------------------------------------------------------------------


def run_pboc_omo_puller(engine: Engine) -> dict[str, Any]:
    """Run the PBoC OMO puller end-to-end.

    Returns a summary dict with keys:
        * fetched      — number of series/day cells attempted
        * inserted     — number of raw_series rows actually inserted
        * omo_rows     — number of OMO daily snapshots parsed
        * mlf_rows     — number of MLF operations parsed
    """
    try:
        puller = PBOCOmoPuller(engine)
        pull_summary = puller.pull()
        save_summary = puller.save_to_db()
        return {
            "fetched": save_summary["fetched"],
            "inserted": save_summary["inserted"],
            "omo_rows": pull_summary["omo_rows"],
            "mlf_rows": pull_summary["mlf_rows"],
        }
    except Exception as exc:
        log.error("pboc_omo: run_pboc_omo_puller crashed: {e}", e=str(exc))
        return {"fetched": 0, "inserted": 0, "omo_rows": 0, "mlf_rows": 0}
