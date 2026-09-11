#!/usr/bin/env python3
"""probe_fred_series.py — find FRED series ids GRID still references that
FRED no longer serves (or has quietly stopped updating).

FRED answers ``400 Bad Request`` — not 404 — for a series id it does not
carry, so a retired id surfaces as a generic client error rather than an
obvious not-found. That is why dead ids in this repo have sat dead for
months at a time (see ``WDTOTAL -> GFDEBTN`` and ``TEDRATE`` in
``ingestion/fred.py``, and the five repointed in PR #425). This script
makes the sweep a single command instead of an archaeology session.

It reports three failure modes, not one:

* ``dead``   — FRED returns 400: the id is not in the catalog. Repoint it.
* ``stale``  — the id resolves, but its newest observation is older than
  ``--stale-days``. A series that exists but stopped updating in 2019 is
  not a working input, and it is invisible to a resolves/does-not-resolve
  check.
* ``live``   — resolves and has a recent observation.

Anything else (transport failure, 429, 5xx, missing key) is reported as
``unverified`` and never as a pass. The point of the tool is to be
trustworthy about what it did *not* establish.

Usage::

    export FRED_API_KEY=...
    python -m scripts.probe_fred_series                  # sweep the repo
    python -m scripts.probe_fred_series --stale-days 400 # looser staleness
    python -m scripts.probe_fred_series --ids DBDI,BSI   # probe specific ids
    python -m scripts.probe_fred_series --json out.json  # machine-readable

Exit status is ``1`` when any id came back ``dead`` or ``stale`` so this can
gate a scheduled job; ``0`` otherwise, including the no-API-key case (a
missing key is a degraded run, not a failure — see the repo's graceful
degradation rule).
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import requests
from loguru import logger as log

_REPO_ROOT = Path(__file__).resolve().parent.parent

#: Shape of a FRED series id. Deliberately loose — the probe is the
#: authority on whether an id is real, so this only has to be tight enough
#: to keep prose and snake_case identifiers out.
#:
#: The floor is three characters, not four: FRED carries plenty of
#: three-character ids and this repo references several (``DFF``, ``TCU``,
#: ``M2V``, and ``BSI`` in ``baltic_dry.py``). A four-character floor drops
#: them silently, which is precisely the failure this tool exists to catch.
FRED_ID_RE: re.Pattern[str] = re.compile(r"^[A-Z][A-Z0-9]{2,20}$")

#: Files under ``ingestion/`` are scanned only if they mention FRED at all.
_FRED_FILE_MARKER: re.Pattern[str] = re.compile(
    r"stlouisfed|fedfred|fredapi|FRED|fred_id"
)

#: ``(module path, module-level constant)`` pairs that match the id shape
#: but hold something other than FRED series ids. Excluding by container —
#: rather than by a blocklist of individual strings — means a newly added
#: FRED container is picked up automatically. The bias is deliberately
#: fail-open: probing a non-FRED string costs one API call and one line of
#: output, whereas missing a real id leaves a dead series hidden.
_NON_FRED_CONTAINERS: frozenset[tuple[str, str]] = frozenset(
    {
        ("ingestion/base.py", "_VALID_REVISION_BEHAVIORS"),
        ("ingestion/base.py", "_REVISION_BEHAVIOR_ALIASES"),
        ("ingestion/crucix_bridge.py", "_EXTRACTORS"),
        ("ingestion/crucix_bridge.py", "_SKIP_SOURCES"),
        ("ingestion/scheduler.py", "_SOURCE_NAME_ALIASES"),
        ("ingestion/web_scraper.py", "TRUST_LABELS"),
        # Freightos route codes (FBX01…), not FRED ids.
        ("ingestion/altdata/supply_chain.py", "FBX_ROUTES"),
        # ISO 4217 currency codes, not FRED ids. (The real FRED ids in this
        # module live in ``_FRED_FX_SERIES`` as the DEX* dict keys.)
        ("ingestion/altdata/sec_xbrl_financials.py", "_PREFERRED_CURRENCIES"),
        ("ingestion/altdata/sec_xbrl_financials.py", "_FX_FALLBACK_USD_PER_CCY"),
        # Column/name matching hints for the SGE premium scraper.
        ("ingestion/altdata/sge_premium.py", "_DATE_COLUMN_HINTS"),
        ("ingestion/altdata/sge_premium.py", "_LONDON_NAME_HINTS"),
        ("ingestion/altdata/sge_premium.py", "_SGE_NAME_HINTS"),
        ("ingestion/altdata/sge_premium.py", "_USDCNY_NAME_HINTS"),
    }
)

_SERIES_URL = "https://api.stlouisfed.org/fred/series"
_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"

#: Newest observation older than this many days marks a series ``stale``.
DEFAULT_STALE_DAYS: int = 200

#: Minimum delay between FRED API calls (seconds). FRED allows 120 req/min.
_RATE_LIMIT_DELAY: float = 0.6

_HTTP_TIMEOUT_SECONDS: float = 20.0


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------


def _harvest(node: ast.AST, out: list[tuple[str, int]]) -> None:
    """Recursively collect id-shaped string literals from an AST node.

    One rule beyond a plain walk: **in a mapping, an id-shaped key is the
    series id and its value is metadata** — so when a key matches, the key
    is collected and its value is not descended into.

    Both container layouts in this repo depend on it::

        H8_SERIES = {"H8B1023NCBCMG": "ci_loans"}          # id is the key
        YC_SERIES = {"yc_1y": {"fred_id": "DGS1", ...}}    # id is in the value

    Without the rule, ``_FRED_FX_SERIES = {"DEXUSEU": ("EUR", ...)}`` would
    also yield ``EUR``, ``GBP``, ``JPY`` and eighteen other currency codes as
    phantom series — noise an operator would have to re-triage on every run.
    """
    if isinstance(node, ast.Dict):
        for key, value in zip(node.keys, node.values):
            if (
                isinstance(key, ast.Constant)
                and isinstance(key.value, str)
                and FRED_ID_RE.match(key.value)
            ):
                out.append((key.value, key.lineno))
                continue  # value is metadata about this id, not another id
            if key is not None:
                _harvest(key, out)
            _harvest(value, out)
        return

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        if FRED_ID_RE.match(node.value):
            out.append((node.value, node.lineno))
        return

    for child in ast.iter_child_nodes(node):
        _harvest(child, out)


def collect_fred_series_ids(root: Path | None = None) -> dict[str, list[str]]:
    """Scan ``ingestion/`` for FRED series ids referenced in the code.

    Only *module-level* assignments are considered. Every FRED id in this
    repo lives in a module-level constant (``FRED_SERIES_LIST``,
    ``BUYBACK_SERIES``, ``H8_SERIES``, …); restricting to module scope drops
    the enum-ish literals inside function bodies (``"SUCCESS"``, ``"RARE"``)
    that share the id shape.

    Parameters:
        root: Repository root. Defaults to the repo this script lives in.

    Returns:
        Mapping of FRED series id -> sorted list of ``"path:line"`` sites
        that reference it.
    """
    base = (root or _REPO_ROOT).resolve()
    found: dict[str, set[str]] = {}

    for path in sorted(base.glob("ingestion/**/*.py")):
        try:
            source = path.read_text(errors="ignore")
        except OSError as exc:  # pragma: no cover - unreadable file
            log.warning("probe_fred_series: cannot read {p}: {e}", p=path, e=str(exc))
            continue
        if not _FRED_FILE_MARKER.search(source):
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            log.warning(
                "probe_fred_series: skipping unparseable {p}: {e}",
                p=path,
                e=str(exc),
            )
            continue

        rel = path.relative_to(base).as_posix()
        for statement in tree.body:
            if isinstance(statement, ast.Assign):
                targets: list[ast.expr] = list(statement.targets)
            elif isinstance(statement, ast.AnnAssign):
                targets = [statement.target]
            else:
                continue
            names = [t.id for t in targets if isinstance(t, ast.Name)]
            if not names:
                continue
            if (rel, names[0]) in _NON_FRED_CONTAINERS:
                continue
            harvested: list[tuple[str, int]] = []
            _harvest(statement, harvested)
            for series_id, lineno in harvested:
                found.setdefault(series_id, set()).add(f"{rel}:{lineno}")

    return {series_id: sorted(sites) for series_id, sites in sorted(found.items())}


# ---------------------------------------------------------------------------
# Probing
# ---------------------------------------------------------------------------


@dataclass
class ProbeResult:
    """Outcome of probing a single FRED series id.

    Attributes:
        series_id: The id that was probed.
        status: One of ``live``, ``stale``, ``dead``, ``unverified``.
        http_status: HTTP status from the ``/series`` call, when one arrived.
        title: FRED's title for the series, when it resolved.
        last_observation: ISO date of the newest real observation, when known.
        detail: Human-readable note — the reason for ``dead``/``unverified``.
        references: ``path:line`` sites in this repo that use the id.
    """

    series_id: str
    status: str
    http_status: int | None = None
    title: str | None = None
    last_observation: str | None = None
    detail: str = ""
    references: list[str] = field(default_factory=list)


def _newest_observation(payload: dict[str, Any]) -> date | None:
    """Return the newest non-missing observation date in a FRED payload.

    FRED encodes a missing value as ``"."``; those rows are skipped so a
    series padded with empty recent periods is not mistaken for fresh.
    """
    newest: date | None = None
    for row in payload.get("observations") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("value", ".")).strip() in {"", "."}:
            continue
        raw = row.get("date")
        if not isinstance(raw, str):
            continue
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            log.warning(
                "probe_fred_series: unparseable observation date {d!r}", d=raw
            )
            continue
        if newest is None or parsed > newest:
            newest = parsed
    return newest


def probe_series(
    series_id: str,
    api_key: str,
    *,
    session: requests.Session | None = None,
    stale_after_days: int = DEFAULT_STALE_DAYS,
    timeout: float = _HTTP_TIMEOUT_SECONDS,
    today: date | None = None,
) -> ProbeResult:
    """Probe one FRED series id and classify it.

    A series is only reported ``live`` when FRED both resolves the id *and*
    returns an observation newer than ``stale_after_days``. Resolution alone
    is not enough: a series that exists but stopped publishing years ago is
    a silent gap in whatever model consumes it.

    Parameters:
        series_id: FRED series identifier, e.g. ``GFDEBTN``.
        api_key: FRED API key.
        session: Optional ``requests.Session`` for connection reuse.
        stale_after_days: Age at which a resolving series counts as stale.
        timeout: Per-request timeout in seconds.
        today: Reference date for the staleness test. Defaults to UTC today.

    Returns:
        A :class:`ProbeResult`. Never raises for network or HTTP problems —
        those come back as ``unverified``.
    """
    http = session or requests.Session()
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json"}

    try:
        response = http.get(_SERIES_URL, params=params, timeout=timeout)
    except requests.RequestException as exc:
        return ProbeResult(
            series_id=series_id,
            status="unverified",
            detail=f"transport error contacting FRED: {exc}",
        )

    status_code = response.status_code
    if status_code == 400:
        # FRED's not-found. Anything else 4xx/5xx is a different problem.
        return ProbeResult(
            series_id=series_id,
            status="dead",
            http_status=400,
            detail="FRED returned HTTP 400 — series id not in the catalog",
        )
    if status_code != 200:
        return ProbeResult(
            series_id=series_id,
            status="unverified",
            http_status=status_code,
            detail=f"unexpected HTTP {status_code} from FRED /series",
        )

    try:
        meta = response.json()
    except ValueError as exc:
        return ProbeResult(
            series_id=series_id,
            status="unverified",
            http_status=status_code,
            detail=f"unparseable JSON from FRED /series: {exc}",
        )

    entries = meta.get("seriess") or []
    title = None
    if entries and isinstance(entries[0], dict):
        title = entries[0].get("title")

    # Resolution established. Now decide live vs stale from observations.
    reference_day = today or datetime.now(timezone.utc).date()
    cutoff = reference_day - timedelta(days=stale_after_days)
    obs_params = dict(params)
    obs_params.update(
        {
            "sort_order": "desc",
            "limit": "12",
            "observation_start": (reference_day - timedelta(days=3650)).isoformat(),
        }
    )

    try:
        obs_response = http.get(_OBSERVATIONS_URL, params=obs_params, timeout=timeout)
    except requests.RequestException as exc:
        return ProbeResult(
            series_id=series_id,
            status="unverified",
            http_status=status_code,
            title=title,
            detail=f"resolved, but observations call failed: {exc}",
        )

    if obs_response.status_code != 200:
        return ProbeResult(
            series_id=series_id,
            status="unverified",
            http_status=status_code,
            title=title,
            detail=(
                "resolved, but observations returned HTTP "
                f"{obs_response.status_code}"
            ),
        )

    try:
        newest = _newest_observation(obs_response.json())
    except ValueError as exc:
        return ProbeResult(
            series_id=series_id,
            status="unverified",
            http_status=status_code,
            title=title,
            detail=f"resolved, but observations JSON unparseable: {exc}",
        )

    if newest is None:
        return ProbeResult(
            series_id=series_id,
            status="stale",
            http_status=status_code,
            title=title,
            detail="resolved but returned no non-missing observations",
        )

    if newest < cutoff:
        return ProbeResult(
            series_id=series_id,
            status="stale",
            http_status=status_code,
            title=title,
            last_observation=newest.isoformat(),
            detail=(
                f"newest observation {newest.isoformat()} is older than "
                f"{stale_after_days} days"
            ),
        )

    return ProbeResult(
        series_id=series_id,
        status="live",
        http_status=status_code,
        title=title,
        last_observation=newest.isoformat(),
    )


def probe_all(
    series_ids: Iterable[str],
    api_key: str,
    *,
    references: dict[str, list[str]] | None = None,
    session: requests.Session | None = None,
    stale_after_days: int = DEFAULT_STALE_DAYS,
    delay: float = _RATE_LIMIT_DELAY,
) -> list[ProbeResult]:
    """Probe many ids in sequence, rate-limited, attaching repo references."""
    http = session or requests.Session()
    sites = references or {}
    ordered = list(series_ids)
    results: list[ProbeResult] = []
    for index, series_id in enumerate(ordered):
        result = probe_series(
            series_id,
            api_key,
            session=http,
            stale_after_days=stale_after_days,
        )
        result.references = sites.get(series_id, [])
        results.append(result)
        # Pause between calls, but not after the final one.
        if delay and index + 1 < len(ordered):
            time.sleep(delay)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _resolve_api_key() -> str:
    """Return the FRED API key, or an empty string if none is configured.

    Reads the environment first so the script is usable without importing
    the whole settings stack, then falls back to ``config.settings``.
    """
    key = os.environ.get("FRED_API_KEY", "").strip()
    if key:
        return key
    try:
        from config import settings

        return str(getattr(settings, "FRED_API_KEY", "") or "").strip()
    except Exception as exc:  # noqa: BLE001 - config is optional here
        log.warning("probe_fred_series: could not load config settings: {e}", e=str(exc))
        return ""


def _render(results: Sequence[ProbeResult]) -> str:
    """Render a compact operator-readable report."""
    buckets: dict[str, list[ProbeResult]] = {
        "dead": [],
        "stale": [],
        "unverified": [],
        "live": [],
    }
    for result in results:
        buckets.setdefault(result.status, []).append(result)

    lines: list[str] = []
    lines.append(
        f"FRED sweep — {len(results)} ids: "
        f"{len(buckets['dead'])} dead, {len(buckets['stale'])} stale, "
        f"{len(buckets['unverified'])} unverified, {len(buckets['live'])} live"
    )
    for status in ("dead", "stale", "unverified"):
        group = buckets[status]
        if not group:
            continue
        lines.append("")
        lines.append(f"── {status.upper()} ({len(group)}) ──")
        for result in sorted(group, key=lambda r: r.series_id):
            lines.append(f"  {result.series_id:24s} {result.detail}")
            for site in result.references:
                lines.append(f"      {site}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Returns a process exit code."""
    parser = argparse.ArgumentParser(
        description="Probe every FRED series id GRID references."
    )
    parser.add_argument(
        "--ids",
        default="",
        help="Comma-separated ids to probe instead of sweeping the repo.",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=DEFAULT_STALE_DAYS,
        help=f"Age at which a resolving series is stale (default {DEFAULT_STALE_DAYS}).",
    )
    parser.add_argument("--json", default="", help="Write full results to this path.")
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Print the collected ids and exit without contacting FRED.",
    )
    args = parser.parse_args(argv)

    if args.ids:
        ids = [part.strip() for part in args.ids.split(",") if part.strip()]
        references: dict[str, list[str]] = {}
    else:
        references = collect_fred_series_ids()
        ids = list(references)

    if args.list_only:
        for series_id in ids:
            print(f"{series_id:24s} {', '.join(references.get(series_id, []))}")
        print(f"\n{len(ids)} ids collected.")
        return 0

    api_key = _resolve_api_key()
    if not api_key:
        # Graceful degradation: a missing key is a degraded run, not a crash.
        log.warning(
            "probe_fred_series: FRED_API_KEY is not set — probed nothing. "
            "{n} ids collected but left UNVERIFIED.",
            n=len(ids),
        )
        print(f"{len(ids)} ids collected; FRED_API_KEY unset, nothing probed.")
        return 0

    results = probe_all(ids, api_key, references=references, stale_after_days=args.stale_days)
    print(_render(results))

    if args.json:
        Path(args.json).write_text(
            json.dumps([asdict(r) for r in results], indent=2, sort_keys=True)
        )
        print(f"\nWrote {args.json}")

    broken = [r for r in results if r.status in {"dead", "stale"}]
    return 1 if broken else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
