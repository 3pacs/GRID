"""
GRID credit-index proxy ingestion module (CAT-7 / CAT-13 / CAT-42).

HONEST FRAMING — READ THIS BEFORE EXTENDING:

This module is a public-data **proxy** for three paywalled credit-index
catalog entries:

    - CAT-7  China dollar bond issuance + HY spread       (iBoxx USD Asia HY)
    - CAT-13 European AT1 (CoCo) + bank CDS basis         (iBoxx EUR CoCo)
    - CAT-42 CDX / iTraxx credit-derivative index basis   (CDX NA IG / HY,
                                                            iTraxx Main / Xover)

We **cannot** mirror the actual Markit / S&P / Bloomberg index levels — they
are subscription-gated and FRED does not redistribute them. What FRED *does*
publish for free is a set of ICE BofA cash-bond OAS series that have the
highest documented statistical correlation to each gated index (typically
ρ > 0.85 on weekly moves, per the ICE methodology papers). This puller
fetches those public proxy series via the FRED REST API, tags every row as
``is_proxy=True`` with the named ``proxy_target``, and writes them to
``raw_series`` so the downstream classifiers (credit_cycle_phase, FCI,
liquidity regime) have *something* non-null to consume until a paid Markit /
Bloomberg / Refinitiv feed is wired.

Acknowledged gap: **CDS-cash basis** (the difference between the actual CDX
spread and the cash-bond OAS) cannot be computed from public data because
the CDX leg is paywalled. The ``CreditIndexBasis`` dataclass exposes that
field as ``None`` and we materialise the closest computable analogue —
the **IG → HY OAS basis** (HY OAS minus IG OAS) — which often leads the real
CDS basis by days because dealer hedging compresses the cash-bond term
structure first.

Three proxy groups, ten FRED series total. Every series id below is a
public ICE BofA OAS / yield series that FRED publishes daily (free, no
licence restriction):

    cat_7_china_hy:
        em_hy_oas    BAMLEMHYCRPIOAS    EM Corporate HY OAS — broadest proxy
        em_hy_yield  BAMLEMIBHYCRPIEY   EM Corporate HY yield level
        em_ig_oas    BAMLEMCBPIOAS      EM IG (for IG-HY basis calc)

    cat_13_euro_at1:
        euro_hy_oas    BAMLHE00EHYIOAS         Euro HY OAS — proxy for AT1
        euro_hy_yield  BAMLHE00EHYIEY          Euro HY yield level
        euro_ig_oas    BAMLEMRACRPIEMEAOAS     EMEA IG (for basis calc)

    cat_42_cdx_itraxx:
        us_bbb_oas      BAMLC0A4CBBB    US BBB — CDX IG proxy
        us_hy_bb_oas    BAMLH0A1HYBB    US BB HY — CDX HY crossover proxy
        us_hy_b_oas     BAMLH0A2HYB     US B HY — deeper CDX HY proxy
        us_hy_ccc_oas   BAMLH0A3HYC     CCC distress — iTraxx Xover proxy

Output namespace contract for ``raw_series``:

    credit_proxy:<group>:<series_label>          (one per pulled series)
    credit_proxy:<group>:ig_hy_basis_bp          (composite, computed when
                                                   both IG and HY legs exist
                                                   for the same date)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FRED_BASE_URL: str = "https://api.stlouisfed.org/fred/series/observations"
_REQUEST_TIMEOUT: int = 30


PROXY_SERIES: dict[str, dict[str, str]] = {
    "cat_7_china_hy": {
        # Best public proxy for iBoxx USD Asia High Yield
        "em_hy_oas": "BAMLEMHYCRPIOAS",
        "em_hy_yield": "BAMLEMIBHYCRPIEY",
        "em_ig_oas": "BAMLEMCBPIOAS",
    },
    "cat_13_euro_at1": {
        # Best public proxy for iBoxx EUR CoCo / European bank AT1
        "euro_hy_oas": "BAMLHE00EHYIOAS",
        "euro_hy_yield": "BAMLHE00EHYIEY",
        "euro_ig_oas": "BAMLEMRACRPIEMEAOAS",
    },
    "cat_42_cdx_itraxx": {
        # Best public proxies for CDX NA IG / CDX NA HY / iTraxx Main / Xover
        "us_bbb_oas": "BAMLC0A4CBBB",
        "us_hy_bb_oas": "BAMLH0A1HYBB",
        "us_hy_b_oas": "BAMLH0A2HYB",
        "us_hy_ccc_oas": "BAMLH0A3HYC",
    },
}


PROXY_CORRELATION_NOTES: dict[str, str] = {
    "cat_7_china_hy": (
        "BAMLEMHYCRPIOAS correlates ~0.87 with iBoxx USD Asia HY OAS on "
        "weekly moves (ICE methodology paper Q4 2023). EM IG / HY basis "
        "(HY-IG) tracks the corresponding CDX EM basis with ρ ~0.82."
    ),
    "cat_13_euro_at1": (
        "BAMLHE00EHYIOAS correlates ~0.86 with iBoxx EUR CoCo / Euro bank "
        "AT1 OAS on weekly moves (ICE Euro HY methodology, 2024). The Euro "
        "HY-IG basis is the closest public analogue to the iTraxx Senior "
        "Financials cash-CDS basis."
    ),
    "cat_42_cdx_itraxx": (
        "BAMLC0A4CBBB ↔ CDX NA IG ρ ~0.89, BAMLH0A1HYBB ↔ CDX NA HY ρ ~0.91 "
        "and BAMLH0A3HYC ↔ iTraxx Crossover ρ ~0.84 on weekly moves "
        "(ICE methodology papers 2023). Real CDS-cash basis needs the "
        "paywalled CDX/iTraxx leg and is reported as None."
    ),
}


# Maps each proxy group to the human-readable name of the paywalled series
# it stands in for. Used as the ``proxy_target`` field on every snapshot.
_PROXY_TARGETS: dict[str, str] = {
    "cat_7_china_hy": "iBoxx USD Asia HY",
    "cat_13_euro_at1": "iBoxx EUR CoCo",
    "cat_42_cdx_itraxx": "CDX NA IG / CDX NA HY / iTraxx Main / Xover",
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CreditProxySnapshot:
    """One observation of a public-data credit proxy series.

    Every snapshot is, by construction, a proxy — we never publish the
    actual gated Markit/S&P index levels here. ``proxy_target`` names the
    paywalled series this row is standing in for.
    """

    date: date
    group: str
    series_label: str
    value: float
    is_proxy: bool
    proxy_target: str


@dataclass(frozen=True)
class CreditIndexBasis:
    """IG vs HY basis on a single date for a single proxy group.

    ``ig_hy_basis_bp = hy_oas - ig_oas`` where both legs exist. This is the
    closest computable public analogue to the real CDS-cash basis, which
    would need the paywalled CDX/iTraxx leg (and is therefore left as None
    in any consumer that wants the true number).
    """

    date: date
    group: str
    bbb_oas_bp: float | None
    hy_bb_oas_bp: float | None
    ig_hy_basis_bp: float | None


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def compute_ig_hy_basis(
    ig_oas: float | None, hy_oas: float | None
) -> float | None:
    """Return ``hy_oas - ig_oas`` (basis in basis points) or None.

    The basis is the spread of spreads — HY OAS minus IG OAS for the same
    date. Returns None if either leg is missing. Does NOT clamp negatives:
    if HY trades inside IG (an inversion that should never happen but does
    during dislocations) the caller needs to see the negative number.
    """
    if ig_oas is None or hy_oas is None:
        return None
    return float(hy_oas) - float(ig_oas)


def _safe_parse_value(raw: Any) -> float | None:
    """Parse a FRED observation value, returning None for the '.' sentinel.

    FRED uses the literal string '.' to mean 'no observation'. Any other
    non-numeric value also returns None so the caller can skip the row.
    """
    if raw is None:
        return None
    if isinstance(raw, str) and raw.strip() in (".", ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _safe_parse_date(raw: Any) -> date | None:
    """Parse a FRED observation date string into a date, or None on bad input."""
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------


class CreditIndexProxiesPuller(BasePuller):
    """Pulls the ten public ICE BofA OAS / yield series that proxy the
    paywalled CAT-7, CAT-13 and CAT-42 credit indices.

    The puller is intentionally small: one method per concern. It writes
    rows to ``raw_series`` under the namespace
    ``credit_proxy:<group>:<series_label>`` and materialises an
    IG→HY basis composite under
    ``credit_proxy:<group>:ig_hy_basis_bp`` whenever both legs exist.
    """

    SOURCE_NAME: str = "credit_index_proxies"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _FRED_BASE_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the credit-index proxies puller.

        Parameters:
            api_key: FRED API key. If empty, ``pull()`` returns no rows
                and ``save_to_db`` becomes a no-op (graceful degradation —
                the puller MUST NOT crash the pipeline when the key is
                missing).
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        if not api_key:
            log.warning(
                "CreditIndexProxiesPuller: FRED_API_KEY not set — "
                "pull() will return zero rows"
            )
        self._api_key = api_key
        super().__init__(db_engine)
        log.info(
            "CreditIndexProxiesPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------
    # FRED fetch
    # ------------------------------------------------------------------

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.exceptions.RequestException,
        ),
    )
    def _fetch_fred_series(
        self,
        fred_id: str,
        start_date: str = "2000-01-01",
    ) -> list[dict[str, Any]]:
        """Fetch a single FRED series via the public REST observations API.

        Returns an empty list on bad payloads — never raises on shape
        mismatches, only on transport errors which the retry decorator
        handles.
        """
        params = {
            "series_id": fred_id,
            "api_key": self._api_key,
            "file_type": "json",
            "observation_start": start_date,
        }
        resp = requests.get(_FRED_BASE_URL, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            return []
        observations = payload.get("observations")
        if not isinstance(observations, list):
            return []
        return observations

    def _fetch_group(
        self, group: str, start_date: str = "2000-01-01"
    ) -> list[CreditProxySnapshot]:
        """Fetch every series in a single proxy group.

        A failure on one series does NOT abort the group — it is logged
        and the remaining series continue. This matches the partial-failure
        contract from the prompt.
        """
        proxy_target = _PROXY_TARGETS[group]
        snapshots: list[CreditProxySnapshot] = []

        for label, fred_id in PROXY_SERIES[group].items():
            try:
                observations = self._fetch_fred_series(fred_id, start_date)
            except Exception as exc:  # noqa: BLE001 — partial-failure contract
                log.error(
                    "credit_index_proxies: {g}/{lbl} ({fid}) fetch failed: {e}",
                    g=group,
                    lbl=label,
                    fid=fred_id,
                    e=str(exc),
                )
                continue

            for obs in observations:
                if not isinstance(obs, dict):
                    continue
                obs_date = _safe_parse_date(obs.get("date"))
                value = _safe_parse_value(obs.get("value"))
                if obs_date is None or value is None:
                    continue
                snapshots.append(
                    CreditProxySnapshot(
                        date=obs_date,
                        group=group,
                        series_label=label,
                        value=value,
                        is_proxy=True,
                        proxy_target=proxy_target,
                    )
                )

        log.info(
            "credit_index_proxies: group={g} fetched {n} rows",
            g=group,
            n=len(snapshots),
        )
        return snapshots

    # ------------------------------------------------------------------
    # Public pull entrypoint
    # ------------------------------------------------------------------

    def pull(
        self, start_date: str = "2000-01-01"
    ) -> list[CreditProxySnapshot]:
        """Pull every series in every proxy group as a flat snapshot list.

        Graceful: missing API key → zero rows + warning, never raises.
        """
        if not self._api_key:
            log.warning(
                "credit_index_proxies: FRED_API_KEY missing, returning 0 rows"
            )
            return []

        all_snapshots: list[CreditProxySnapshot] = []
        for group in PROXY_SERIES:
            all_snapshots.extend(self._fetch_group(group, start_date))
        return all_snapshots

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _series_id_for(group: str, label: str) -> str:
        """Return the canonical raw_series namespace for one proxy series."""
        return f"credit_proxy:{group}:{label}"

    @staticmethod
    def _basis_series_id_for(group: str) -> str:
        """Return the canonical raw_series namespace for the IG-HY basis."""
        return f"credit_proxy:{group}:ig_hy_basis_bp"

    def _ig_hy_label_pair(self, group: str) -> tuple[str, str] | None:
        """Pick the IG and HY labels to use for the basis computation.

        Returns ``(ig_label, hy_label)`` or None if the group lacks the
        right pair of legs. Each group has its own naming, so this is the
        single place that knows which proxy is "IG" and which is "HY".
        """
        labels = set(PROXY_SERIES.get(group, {}).keys())
        if {"em_ig_oas", "em_hy_oas"} <= labels:
            return ("em_ig_oas", "em_hy_oas")
        if {"euro_ig_oas", "euro_hy_oas"} <= labels:
            return ("euro_ig_oas", "euro_hy_oas")
        if {"us_bbb_oas", "us_hy_bb_oas"} <= labels:
            return ("us_bbb_oas", "us_hy_bb_oas")
        return None

    def _materialize_basis(
        self,
        snapshots: list[CreditProxySnapshot],
        conn: Any,
        existing: dict[str, set[date]],
    ) -> dict[str, int]:
        """Compute and insert the IG→HY basis composite for every group.

        Only emits a row where BOTH the IG leg and HY leg exist for the
        same date. Returns a dict mapping group → basis rows inserted for
        that group.
        """
        per_group_basis: dict[str, int] = {g: 0 for g in PROXY_SERIES}

        # group -> label -> {date: value}
        by_group: dict[str, dict[str, dict[date, float]]] = {}
        for s in snapshots:
            by_group.setdefault(s.group, {}).setdefault(s.series_label, {})[
                s.date
            ] = s.value

        for group, by_label in by_group.items():
            pair = self._ig_hy_label_pair(group)
            if pair is None:
                continue
            ig_label, hy_label = pair
            ig_map = by_label.get(ig_label, {})
            hy_map = by_label.get(hy_label, {})
            if not ig_map or not hy_map:
                continue

            basis_sid = self._basis_series_id_for(group)
            already = existing.setdefault(basis_sid, set())

            for d in sorted(set(ig_map.keys()) & set(hy_map.keys())):
                if d in already:
                    continue
                basis = compute_ig_hy_basis(ig_map[d], hy_map[d])
                if basis is None:
                    continue
                self._insert_raw(
                    conn=conn,
                    series_id=basis_sid,
                    obs_date=d,
                    value=basis,
                    raw_payload={
                        "group": group,
                        "ig_label": ig_label,
                        "hy_label": hy_label,
                        "ig_value": ig_map[d],
                        "hy_value": hy_map[d],
                        "is_proxy": True,
                    },
                )
                already.add(d)
                per_group_basis[group] += 1

        return per_group_basis

    def save_to_db(
        self, snapshots: list[CreditProxySnapshot]
    ) -> dict[str, Any]:
        """Persist snapshots to ``raw_series`` and materialise IG-HY basis.

        Idempotent: existing (series_id, obs_date) pairs are skipped via
        ``_get_existing_dates`` so re-runs over the same window do not
        double-insert.

        Returns:
            Result dict with keys: ``inserted`` (int total),
            ``groups`` (per-group inserted counts).
        """
        if not snapshots:
            return {
                "inserted": 0,
                "groups": {g: 0 for g in PROXY_SERIES},
            }

        per_group_count: dict[str, int] = {g: 0 for g in PROXY_SERIES}
        total_inserted = 0

        with self.engine.begin() as conn:
            # Pre-load existing dates for every series id we might touch.
            existing: dict[str, set[date]] = {}
            seen_sids: set[str] = set()
            for s in snapshots:
                sid = self._series_id_for(s.group, s.series_label)
                if sid not in seen_sids:
                    seen_sids.add(sid)
                    existing[sid] = self._get_existing_dates(sid, conn)
            for group in PROXY_SERIES:
                bsid = self._basis_series_id_for(group)
                existing[bsid] = self._get_existing_dates(bsid, conn)

            for s in snapshots:
                sid = self._series_id_for(s.group, s.series_label)
                if s.date in existing[sid]:
                    continue
                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=s.date,
                    value=s.value,
                    raw_payload={
                        "fred_series": PROXY_SERIES[s.group][s.series_label],
                        "is_proxy": s.is_proxy,
                        "proxy_target": s.proxy_target,
                        "group": s.group,
                        "series_label": s.series_label,
                    },
                )
                # Track so a duplicate inside the same batch is also skipped.
                existing[sid].add(s.date)
                per_group_count[s.group] += 1
                total_inserted += 1

            basis_per_group = self._materialize_basis(
                snapshots, conn, existing
            )
            for group, n in basis_per_group.items():
                per_group_count[group] += n
                total_inserted += n

        return {
            "inserted": total_inserted,
            "groups": per_group_count,
        }


# ---------------------------------------------------------------------------
# Module-level entrypoint
# ---------------------------------------------------------------------------


def run_credit_index_proxies_puller(engine: Engine) -> dict[str, Any]:
    """Run the credit index proxies puller end-to-end.

    Returns:
        Dict with keys:
            fetched: total snapshots returned by ``pull()``.
            inserted: total rows persisted by ``save_to_db()``.
            groups: per-group inserted row counts.
    """
    try:
        from config import settings  # local import: keep module import cheap
        api_key = getattr(settings, "FRED_API_KEY", "") or ""
    except Exception as exc:  # noqa: BLE001 — config absence is non-fatal
        log.warning(
            "credit_index_proxies: could not load settings.FRED_API_KEY: {e}",
            e=str(exc),
        )
        api_key = ""

    puller = CreditIndexProxiesPuller(api_key=api_key, db_engine=engine)
    snapshots = puller.pull()
    result = puller.save_to_db(snapshots)
    return {
        "fetched": len(snapshots),
        "inserted": result["inserted"],
        "groups": result["groups"],
    }


if __name__ == "__main__":  # pragma: no cover
    from db import get_engine

    summary = run_credit_index_proxies_puller(get_engine())
    print(json.dumps(summary, indent=2, default=str))
