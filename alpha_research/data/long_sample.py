"""Long-sample data for regime-conditioned research: Kenneth French portfolios, long FRED
series and the Caldara–Iacoviello geopolitical risk index.

One-off, documented downloads (registration: ``docs/paper_log/high-sharpe-ensemble-v6-
preregistration.md`` §3). Every file is fetched over HTTPS from the publisher, cached under a
directory the caller names, and recorded in ``manifest.json`` with its URL, size, sha256 and
download time. Nothing here writes to the GRID database; the scheduler puller for these series
is a separate, reviewed change. All frames are monthly, indexed by the first day of the
observation month (FRED convention) and tz-naive.

Point-in-time use: these are latest-vintage values. The caller applies the declared
publication lags (:data:`PUBLICATION_LAG_DAYS`) so a value dated month ``m`` is treated as
known only from ``m + lag``; the registration states this limitation (INDPRO and CPI are
revised; the French files are rebuilt monthly from CRSP).
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from loguru import logger as log

FRENCH_BASE = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
FRENCH_FILES: dict[str, str] = {
    "factors": "F-F_Research_Data_Factors_CSV.zip",     # Mkt-RF, SMB, HML, RF; monthly from 1926-07
    "momentum": "F-F_Momentum_Factor_CSV.zip",          # Mom; monthly from 1927-01
    "industries10": "10_Industry_Portfolios_CSV.zip",   # value-weighted 10 industries; monthly from 1926-07
}
GPR_URL = "https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls"
FRED_GRAPH_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"  # keyless CSV, as scripts/bulk_data_downloader.py uses

#: Long FRED series the regime labeller and the analog-cell report use, with their first year.
FRED_LONG_SERIES: dict[str, str] = {
    "CPIAUCNS": "CPI-U all items, NSA, monthly from 1913",
    "INDPRO": "industrial production index, monthly from 1919",
    "BAA": "Moody's Baa corporate yield, monthly from 1919",
    "AAA": "Moody's Aaa corporate yield, monthly from 1919",
    "TB3MS": "3-month Treasury bill, monthly from 1934",
    "GS10": "10-year Treasury constant maturity, monthly from 1953",
    "LTGOVTBD": "long-term US government bond yield, monthly 1925-2000",
    "WTISPLC": "WTI spot crude, monthly from 1946",
    "FEDFUNDS": "effective federal funds rate, monthly from 1954",
    "TWEXBMTH": "trade-weighted dollar, major currencies, monthly 1973-2019",
    "DTWEXBGS": "trade-weighted dollar, broad goods and services, daily from 2006",
    "PPIACO": "PPI all commodities, monthly from 1913",
    "UNRATE": "unemployment rate, monthly from 1948",
}

#: Calendar days after the first day of the observation month before a value is public.
#: CPI and INDPRO are released around the middle of the following month (~45 days after the
#: first of the observation month); monthly averages of daily market series are known at month
#: end (~35 days); the French files are rebuilt monthly and appear during the following month (~45).
PUBLICATION_LAG_DAYS: dict[str, int] = {
    "CPIAUCNS": 45, "INDPRO": 50, "UNRATE": 42, "PPIACO": 45,
    "BAA": 35, "AAA": 35, "TB3MS": 35, "GS10": 35, "LTGOVTBD": 35, "WTISPLC": 35,
    "FEDFUNDS": 35, "TWEXBMTH": 35, "DTWEXBGS": 35,
    "GPR": 45, "GPRH": 45, "french": 45,
}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class DownloadCache:
    """Fetch-once cache with a JSON manifest (url, bytes, sha256, fetched_at) per file."""

    def __init__(self, directory: str | Path) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.dir / "manifest.json"
        self.manifest: dict[str, Any] = (
            json.loads(self.manifest_path.read_text(encoding="utf-8")) if self.manifest_path.exists() else {}
        )

    def fetch(self, name: str, url: str, *, params: dict[str, str] | None = None, timeout: int = 120) -> bytes:
        path = self.dir / name
        if path.exists() and name in self.manifest:
            return path.read_bytes()
        log.info("long_sample: downloading {n} from {u}", n=name, u=url.split("?")[0])
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.content
        path.write_bytes(data)
        self.manifest[name] = {
            "url": url, "bytes": len(data), "sha256": _sha256(data),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        self.manifest_path.write_text(json.dumps(self.manifest, indent=1, sort_keys=True), encoding="utf-8")
        return data

    def receipt(self) -> dict[str, Any]:
        return {k: {kk: vv for kk, vv in v.items() if kk != "url" or "api_key" not in vv} for k, v in self.manifest.items()}


# ── Kenneth French ──────────────────────────────────────────────────────────


def _french_monthly_block(text: str, header_startswith: str = ",") -> pd.DataFrame:
    """The first monthly block of a French CSV: rows ``YYYYMM,...`` after the first header line."""
    lines = text.splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith(header_startswith) and len(ln) > 1)
    header = [h.strip() for h in lines[start].split(",")]
    rows = []
    for ln in lines[start + 1:]:
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) != len(header) or not parts[0].isdigit() or len(parts[0]) != 6:
            if rows:
                break
            continue
        rows.append(parts)
    frame = pd.DataFrame(rows, columns=header)
    idx = pd.to_datetime(frame.iloc[:, 0], format="%Y%m")
    frame = frame.iloc[:, 1:].astype(float)
    frame.index = idx
    frame.index.name = "month"
    frame = frame.replace({-99.99: float("nan"), -999.0: float("nan")})
    return frame / 100.0  # percent → decimal returns


def load_french(cache: DownloadCache) -> dict[str, pd.DataFrame]:
    """``factors`` (Mkt-RF, SMB, HML, RF), ``momentum`` (Mom), ``industries10`` (value-weighted), monthly decimals."""
    out: dict[str, pd.DataFrame] = {}
    for key, fname in FRENCH_FILES.items():
        data = cache.fetch(fname, FRENCH_BASE + fname)
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            text = zf.read(csv_name).decode("latin-1")
        out[key] = _french_monthly_block(text)
    return out


def french_price_panel(french: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Total-return price indices (base 1.0) for the market, bills, factor long-short legs and industries.

    ``MKT`` = Mkt-RF + RF (the market's total return), ``RF`` = bills, ``MOM``/``HML``/``SMB`` =
    the long-short factor return added to bills (an investable-looking index of a non-tradable
    factor; the registration states the proxy caveat), industries by their French names.
    """
    f = french["factors"]
    m = french["momentum"]
    ind = french["industries10"]
    rets = pd.DataFrame({
        "MKT": f["Mkt-RF"] + f["RF"],
        "RF": f["RF"],
        "MOM": m["Mom"] + f["RF"],
        "HML": f["HML"] + f["RF"],
        "SMB": f["SMB"] + f["RF"],
    })
    rets = rets.join(ind, how="left")
    rets = rets.dropna(subset=["MKT"])
    prices = (1.0 + rets.fillna(0.0)).cumprod()
    prices[rets.isna()] = float("nan")
    # month-end stamps so a weekly/monthly rebalance grid lands on real observations
    prices.index = prices.index + pd.offsets.MonthEnd(0)
    return prices


# ── FRED ────────────────────────────────────────────────────────────────────


def load_fred_long(cache: DownloadCache, series: dict[str, str] | None = None) -> pd.DataFrame:
    """Monthly frame of the long FRED series (daily series averaged to months), latest vintage.

    Uses FRED's keyless ``fredgraph.csv`` export (full history per series), the same endpoint
    ``scripts/bulk_data_downloader.py`` uses; no API key leaves the machine.
    """
    series = series or FRED_LONG_SERIES
    cols: dict[str, pd.Series] = {}
    for sid in series:
        try:
            data = cache.fetch(f"fred_{sid}.csv", FRED_GRAPH_URL, params={"id": sid})
        except requests.HTTPError as exc:
            log.warning("long_sample: FRED {s} unavailable ({e}); skipped", s=sid, e=str(exc)[:80])
            continue
        frame = pd.read_csv(io.BytesIO(data))
        date_col = frame.columns[0]
        s = pd.to_numeric(frame[sid], errors="coerce")
        s.index = pd.to_datetime(frame[date_col])
        s = s.dropna().sort_index()
        if s.empty:
            log.warning("long_sample: FRED {s} returned no observations", s=sid)
            continue
        if sid in ("DTWEXBGS",):
            s = s.resample("MS").mean()
        cols[sid] = s
    frame = pd.DataFrame(cols).sort_index()
    frame.index.name = "month"
    return frame


# ── Geopolitical risk ──────────────────────────────────────────────────────


def load_gpr(cache: DownloadCache) -> pd.DataFrame:
    """Caldara–Iacoviello GPR: ``GPRH`` (historical, from 1900) and ``GPR`` (recent, from 1985), monthly."""
    data = cache.fetch("data_gpr_export.xls", GPR_URL)
    frame = pd.read_excel(io.BytesIO(data))
    frame["month"] = pd.to_datetime(frame["month"])
    out = frame.set_index("month")[["GPR", "GPRH", "GPRT", "GPRA"]].sort_index()
    out.index.name = "month"
    return out


# ── Known-at helper ────────────────────────────────────────────────────────


def known_at(series: pd.Series, lag_days: int) -> pd.Series:
    """Re-index a monthly series by the date each value became public (month start + lag)."""
    s = series.dropna()
    idx = pd.DatetimeIndex([pd.Timestamp(d) + pd.Timedelta(days=lag_days) for d in s.index])
    out = pd.Series(s.to_numpy(), index=idx)
    out.index.name = "known_at"
    return out


def value_known_on(series_known: pd.Series, as_of: date | pd.Timestamp) -> float | None:
    """Latest value whose known-at date is on or before ``as_of`` (``None`` when nothing is)."""
    s = series_known.loc[: pd.Timestamp(as_of)]
    return float(s.iloc[-1]) if len(s) else None


__all__ = [
    "DownloadCache", "FRED_GRAPH_URL", "FRED_LONG_SERIES", "FRENCH_FILES", "GPR_URL", "PUBLICATION_LAG_DAYS",
    "french_price_panel", "known_at", "load_french", "load_fred_long", "load_gpr", "value_known_on",
]
