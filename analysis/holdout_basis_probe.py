"""Holdout-period basis-report probe for VS1 (§2.3, §8; research only, read-only).

Runs only after a study's ``discovery_frozen`` record is witnessed off-host and only
with an explicit holdout authorization (``allow_holdout=True``), pinned preregistration
hash, and a snapshot instant strictly later than ``discovery_frozen.run_at``
(``docs/paper_log/vs1-insider-density-v6-preregistration.md`` §8).

Checks per ticker from the frozen discovery manifest's ``admitted`` set over the
holdout read window ``2020-01-01 .. 2026-06-30`` (using a pinned XNYS session calendar
manifest):

* **Source filtering**: TIINGO (``source_catalog`` 524) ``YF:{T}:adj_close`` and
  ``YF:{T}:close`` only, bounded by ``snapshot_as_of_ts``. Other sources under the same
  series id are ignored, counted and reported, never disqualifying.
* **Basis checks 1-4** (identical to the discovery probe): (1) zero multi-valued dates
  among TIINGO SUCCESS vintages; (2) every step of ``adj_close / close`` a split ratio
  within :data:`SPLIT_RTOL` or a distribution of at most 25%; (3) no XNYS holdout
  session inside the ticker's span missing a close; (4) no QUARANTINED TIINGO row.
* **Pull-batch splice check** (v5/v6 §2.3, §8): ``v6.splice_check`` at tolerance
  ``1e-4`` (TwelveData-corroborated), including the ``2020-01-01`` boundary against the
  immediately preceding session (e.g. ``2019-12-31``).
* **TwelveData return cross-check**: ``v6.crosscheck_statistics`` with the identical
  ``v6.CROSSCHECK`` tolerances (X = 99%, Y = 10 bp, N = 250, at most 10% adjustment
  pairs excluded, factor relative tolerance 1e-4).
* **C1 / startDate bounds**: Tiingo metadata presence (``no_meta``), optional SEC
  entity match (``entity_mismatch``), ``startDate`` shift against the frozen manifest's
  ``listed_from`` (``start_date_shifted``), post-``startDate`` close availability, and
  optional C1 ticker-interval coverage from ``submissions`` (no Form 4 transactions).
* **XLK benchmark check**: evaluated under the unchanged basis + splice + cross-check
  rule and reported as ``xlk_admitted_for_magnitudes`` (magnitudes only; never gates
  issuer tickers).

What it never does: accept Form 4 transaction/purchase inputs, align prices to events,
compute a label or IC, read outside ``2020-01-01 .. 2026-06-30`` (beyond the single
immediately preceding session anchor for the ``2020-01-01`` splice boundary), overwrite
existing outputs, or write to the database. The discovery probe
(``analysis/price_admission_probe.py``) retains its refusal of reads >= 2020-01-01
untouched.
"""

from __future__ import annotations

import dataclasses
import hashlib
import importlib
import importlib.util
import json
import math
import re
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import bindparam, text

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4
from analysis.offline_research_proof import digest, stamp
from analysis.research_forward_log import ForwardLog, _lines

PROBE_NAME = "vs1-holdout-basis-probe"
PROBE_VERSION = 1

PRICE_SOURCE = v6.PRICE_SOURCE
PRICE_SOURCE_ID = v6.PRICE_SOURCE_ID
SERIES_TEMPLATE = v6.SERIES_TEMPLATE
BASIS = v6.BASIS
BENCHMARK = v6.BENCHMARK
CROSSCHECK = v6.CROSSCHECK
SPLICE_TOL = v6.SPLICE_TOL

#: Holdout read window (v6 §3, §8): 2020-01-01 .. 2026-06-30 inclusive.
HOLDOUT_START = date(2020, 1, 1)
HOLDOUT_END = date(2026, 6, 30)
DEFAULT_WINDOW = (HOLDOUT_START, HOLDOUT_END)
#: Maximum calendar days before HOLDOUT_START permitted for the single 2020-01-01 splice boundary anchor.
MAX_BOUNDARY_LOOKBACK_DAYS = 10

CALENDAR_SCHEMA = "grid.xnys-session-manifest.v1"
CALENDAR_VERSION_RE = re.compile(
    r"^exchange_calendars:4\.11\.2:pandas:2\.3\.3:numpy:[0-9.]+:tzdata:[0-9.]+:XNYS:\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2}$"
)
SESSION_KEY_RE = re.compile(r"^XNYS:(\d{4}-\d{2}-\d{2}):RTH$")

FORBIDDEN_FORM4_ARGS = frozenset({
    "form4", "form4_path", "form4_events", "events", "purchases",
    "owners", "transactions", "nonderiv_transactions",
})
FORBIDDEN_FORM4_FLAGS = (
    "--form4", "--form4-path", "--form4-events", "--owners",
    "--transactions", "--nonderiv-transactions",
)
FORBIDDEN_FORM4_COLUMNS = frozenset({
    "TRANS_CODE", "TRANSACTION_CODE", "TRANSACTION_DATE",
    "TRANS_SHARES", "TRANS_PRICEPERSHARE", "SHARES", "PRICE_PER_SHARE",
})

INTERPRETATION = (
    "Holdout-period basis check (v6 §2.3, §8): basis checks 1-4, the pull-batch splice check "
    "(including the 2020-01-01 boundary), the TwelveData return cross-check with identical tolerances "
    "(X = 99%, Y = 10 bp, N = 250, at most 10% adjustment pairs excluded, factor tolerance 1e-4), "
    "and C1 / Tiingo startDate bounds over 2020-01-01..2026-06-30.",
    "Per-ticker abstention (v6 §8): a ticker failing the holdout-period basis check abstains in the "
    "holdout (no holdout label); the rest of the frozen discovery manifest is unchanged.",
    "XLK benchmark check: evaluated under the unchanged basis + splice + cross-check rule and reported "
    "as xlk_admitted_for_magnitudes; magnitudes never gate issuer admission or abstention.",
    "Source filtering: every read is TIINGO SUCCESS rows with pull_timestamp <= snapshot_as_of_ts. "
    "Other sources under the same series id are counted and reported, never read as prices.",
    "No Form 4 purchase/transaction input is accepted and no label or IC is computed.",
)


class HoldoutProbeRefused(PermissionError):
    """The holdout basis probe was asked to violate a pre-registration or custody invariant."""


# --- guards ----------------------------------------------------------------------------------------


def _is_plain_date(value: Any) -> bool:
    return isinstance(value, date) and not isinstance(value, datetime)


def check_source(name: str) -> None:
    """TIINGO is the single admitted price source; everything else is refused."""
    try:
        gd4.check_source(name)
    except gd4.ProbeRefused as exc:
        raise HoldoutProbeRefused(str(exc)) from exc


def check_holdout_window(lo: date, hi: date) -> None:
    """Refuse any read window outside the frozen holdout span [2020-01-01, 2026-06-30]."""
    if not _is_plain_date(lo) or not _is_plain_date(hi):
        raise HoldoutProbeRefused("holdout window bounds must be plain dates")
    if lo < HOLDOUT_START:
        raise HoldoutProbeRefused(
            f"holdout basis probe refuses reads before {HOLDOUT_START.isoformat()} (got {lo.isoformat()})"
        )
    if hi > HOLDOUT_END:
        raise HoldoutProbeRefused(
            f"holdout basis probe refuses reads after {HOLDOUT_END.isoformat()} (reserved period; got {hi.isoformat()})"
        )
    if lo > hi:
        raise HoldoutProbeRefused("holdout window start is after end")


def check_write_once_out(out: Path) -> Path:
    """Refuse if the output directory already exists (write-once)."""
    path = Path(out)
    if path.exists():
        raise HoldoutProbeRefused(f"{path} already exists: holdout basis probe output is write-once")
    return path


def check_no_form4_input(**kwargs: Any) -> None:
    """Refuse any Form 4 transaction/event input."""
    for key, value in kwargs.items():
        low = str(key).lower()
        if (low in FORBIDDEN_FORM4_ARGS or "form4" in low) and value is not None:
            raise HoldoutProbeRefused(f"holdout basis probe accepts no Form 4 input ({key!r})")


def check_no_form4_cli_args(argv: Sequence[str]) -> None:
    """Refuse any CLI argument supplying Form 4 transactions or owners."""
    for arg in argv:
        for flag in FORBIDDEN_FORM4_FLAGS:
            if arg == flag or arg.startswith(f"{flag}="):
                raise HoldoutProbeRefused(f"holdout basis probe accepts no Form 4 input ({flag})")


# --- pinned XNYS calendar manifest -----------------------------------------------------------------


@dataclass(frozen=True)
class CalendarManifest:
    """Validated ``grid.xnys-session-manifest.v1`` session schedule."""

    path: Path
    sha256: str
    start: date
    end: date
    calendar_version: str
    sessions: tuple[date, ...]

    def window_sessions(self, lo: date, hi: date) -> list[date]:
        check_holdout_window(lo, hi)
        if self.start > lo or self.end < hi:
            raise HoldoutProbeRefused(
                f"calendar manifest ({self.start.isoformat()}..{self.end.isoformat()}) does not cover "
                f"holdout window {lo.isoformat()}..{hi.isoformat()}"
            )
        out = [d for d in self.sessions if lo <= d <= hi]
        if not out:
            raise HoldoutProbeRefused(f"calendar manifest has no sessions in {lo.isoformat()}..{hi.isoformat()}")
        return out

    @property
    def boundary_anchor(self) -> date | None:
        """The single XNYS session immediately before 2020-01-01 (e.g. 2019-12-31) for the splice boundary."""
        prior = [d for d in self.sessions if d < HOLDOUT_START]
        if not prior:
            return None
        anchor = prior[-1]
        if anchor < HOLDOUT_START - timedelta(days=MAX_BOUNDARY_LOOKBACK_DAYS):
            return None
        return anchor


def _parse_iso_date(value: Any, label: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise HoldoutProbeRefused(f"{label} must be an ISO date (YYYY-MM-DD)")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise HoldoutProbeRefused(f"invalid ISO date for {label}: {value!r}") from exc


def load_calendar_manifest(path: Path, *, expected_sha256: str | None = None) -> CalendarManifest:
    """Load and validate a pinned ``grid.xnys-session-manifest.v1`` calendar manifest."""
    p = Path(path)
    if p.is_symlink():
        raise HoldoutProbeRefused("calendar manifest symlink refused")
    if not p.is_file():
        raise HoldoutProbeRefused(f"calendar manifest not found: {p}")
    raw = p.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None:
        if not v1._is_hex64(expected_sha256) or sha != expected_sha256:
            raise HoldoutProbeRefused("calendar manifest sha256 does not match the pinned hash")

    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, val in pairs:
            if k in out:
                raise HoldoutProbeRefused(f"duplicate JSON key {k!r} in calendar manifest")
            out[k] = val
        return out

    try:
        doc = json.loads(raw, object_pairs_hook=unique_pairs)
    except ValueError as exc:
        raise HoldoutProbeRefused(f"calendar manifest is not valid JSON: {exc}") from exc

    required = {"schema", "exchange", "session_type", "timezone", "start", "end", "calendar_version", "sessions"}
    if (
        not isinstance(doc, dict)
        or set(doc) != required
        or (doc["schema"], doc["exchange"], doc["session_type"], doc["timezone"])
        != (CALENDAR_SCHEMA, "XNYS", "RTH", "America/New_York")
    ):
        raise HoldoutProbeRefused("calendar manifest schema or exchange identity mismatch")
    cal_ver = doc["calendar_version"]
    if not isinstance(cal_ver, str) or not CALENDAR_VERSION_RE.fullmatch(cal_ver):
        raise HoldoutProbeRefused("calendar manifest carries unreviewed calendar_version provenance")
    begin = _parse_iso_date(doc["start"], "calendar start")
    finish = _parse_iso_date(doc["end"], "calendar end")
    if begin > finish:
        raise HoldoutProbeRefused("calendar manifest start after end")
    dep_lo, dep_hi = (_parse_iso_date(x, "calendar_version bound") for x in cal_ver.split(":")[-2:])
    if begin < dep_lo or finish > dep_hi:
        raise HoldoutProbeRefused("calendar manifest exceeds its calendar_version bounds")

    rows = doc["sessions"]
    if not isinstance(rows, list) or not rows:
        raise HoldoutProbeRefused("calendar manifest sessions must be a non-empty list")
    sessions: list[date] = []
    last_day: date | None = None
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"key", "opens", "closes"} or not isinstance(row["key"], str):
            raise HoldoutProbeRefused("invalid session entry in calendar manifest")
        match = SESSION_KEY_RE.fullmatch(row["key"])
        if not match:
            raise HoldoutProbeRefused(f"invalid session key in calendar manifest: {row['key']!r}")
        day = _parse_iso_date(match.group(1), "session key date")
        if not begin <= day <= finish or (last_day is not None and day <= last_day):
            raise HoldoutProbeRefused("calendar manifest sessions are out of order or out of bounds")
        try:
            op = datetime.fromisoformat(str(row["opens"]))
            cl = datetime.fromisoformat(str(row["closes"]))
        except ValueError as exc:
            raise HoldoutProbeRefused("invalid session timestamps in calendar manifest") from exc
        if op.tzinfo is None or cl.tzinfo is None or cl <= op:
            raise HoldoutProbeRefused("calendar manifest session opens/closes must be ordered UTC timestamps")
        sessions.append(day)
        last_day = day
    return CalendarManifest(
        path=p, sha256=sha, start=begin, end=finish, calendar_version=cal_ver, sessions=tuple(sessions)
    )


# --- frozen discovery price manifest ---------------------------------------------------------------


@dataclass(frozen=True)
class PriceManifestInfo:
    """Validated frozen discovery ``price_manifest.json`` (never mutated by the holdout probe)."""

    path: Path
    file_sha256: str
    digest: str
    source: str
    series_template: str
    basis: str
    benchmark: str
    admitted: tuple[str, ...]
    listed_from: dict[str, str]
    raw: dict[str, Any]

    @property
    def admitted_issuers(self) -> list[str]:
        return sorted(t for t in self.admitted if t != self.benchmark)


def _manifest_digest(raw: Mapping[str, Any]) -> str:
    listed = raw.get("listed_from") or {}
    items = listed.items() if isinstance(listed, Mapping) else listed
    norm_listed = [[str(t), str(d)] for t, d in sorted((str(t), str(d)) for t, d in items)]
    admitted = sorted(str(t) for t in raw["admitted"])
    return digest({**raw, "admitted": admitted, "listed_from": norm_listed})


def load_price_manifest(path: Path) -> PriceManifestInfo:
    """Load and validate the frozen discovery price manifest."""
    p = Path(path)
    if not p.is_file():
        raise HoldoutProbeRefused(f"price manifest not found: {p}")
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise HoldoutProbeRefused(f"price manifest is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise HoldoutProbeRefused("price manifest must be a JSON object")
    source = str(raw.get("source") or "")
    check_source(source)
    if raw.get("series_template") != SERIES_TEMPLATE:
        raise HoldoutProbeRefused(f"price manifest must use series_template {SERIES_TEMPLATE!r}")
    if raw.get("basis") != BASIS:
        raise HoldoutProbeRefused(f"price manifest must use basis {BASIS!r}")
    benchmark = str(raw.get("benchmark") or BENCHMARK)
    if benchmark != BENCHMARK:
        raise HoldoutProbeRefused(f"price manifest benchmark must be {BENCHMARK!r}")
    admitted_raw = raw.get("admitted")
    if not isinstance(admitted_raw, list) or not admitted_raw or not all(isinstance(t, str) and t.strip() for t in admitted_raw):
        raise HoldoutProbeRefused("price manifest admitted must be a non-empty list of tickers")
    admitted = tuple(sorted({t.strip().upper() for t in admitted_raw}))
    if not v1._is_hex64(raw.get("probe_report_sha256")):
        raise HoldoutProbeRefused("price manifest lacks a valid probe_report_sha256")
    for opt_hash in ("crosscheck_report_sha256", "tiingo_meta_report_sha256"):
        if opt_hash in raw and raw[opt_hash] is not None and not v1._is_hex64(raw[opt_hash]):
            raise HoldoutProbeRefused(f"price manifest has invalid {opt_hash}")
    listed_raw = raw.get("listed_from") or {}
    items = listed_raw.items() if isinstance(listed_raw, Mapping) else listed_raw
    listed_from: dict[str, str] = {}
    admitted_set = set(admitted)
    for t, d in items:
        sym = str(t).strip().upper()
        if sym not in admitted_set:
            raise HoldoutProbeRefused(f"price manifest listed_from names non-admitted ticker {sym!r}")
        parsed = _parse_iso_date(str(d)[:10], f"listed_from[{sym}]")
        listed_from[sym] = parsed.isoformat()
    return PriceManifestInfo(
        path=p,
        file_sha256=gd4.file_sha256(p),
        digest=_manifest_digest(raw),
        source=source.strip().upper(),
        series_template=SERIES_TEMPLATE,
        basis=BASIS,
        benchmark=benchmark,
        admitted=admitted,
        listed_from=dict(sorted(listed_from.items())),
        raw=raw,
    )


# --- witnessed discovery_frozen custody gate -------------------------------------------------------


@dataclass(frozen=True)
class DiscoveryFrozenProof:
    """Verified off-host witnessed ``discovery_frozen`` record."""

    study: str
    prereg_sha256: str
    discovery_frozen_sha256: str
    discovery_sha256: str
    run_at: str
    run_at_dt: datetime
    position: int
    witnessed_records: int
    witness_tip: str
    witness_path: str
    inputs_frozen_sha256: str
    price_manifest_sha256: str | None


def _registry_id(study: str) -> str:
    return study[len("vs1-"):] if study.startswith("vs1-sectors-") else study


def _registry_filenames(study: str) -> tuple[str, str, str, str]:
    """(log_filename, anchor_filename, lock_filename, witness_path) for a VS1/sectors registry id."""
    rid = _registry_id(study)
    try:
        witness_path = v1.canonical_witness_path(rid)
    except ValueError as exc:
        raise HoldoutProbeRefused(str(exc)) from exc
    family, _, number = rid.rpartition("-v")
    stem = f"granular_panel_prereg_v{number}" if family == "vs1" else f"granular_panel_prereg_sectors_v{number}"
    return f"{stem}.jsonl", f"{stem}.anchors.jsonl", f".{stem}.lock", witness_path


def _load_study_module(study: str):
    """Import the pinned version module if present in ``analysis/``, else None."""
    rid = _registry_id(study)
    family, _, number = rid.rpartition("-v")
    mod_name = (
        f"analysis.panel_insider_density_v{number}" if family == "vs1"
        else f"analysis.panel_insider_density_sectors_v{number}" if family == "sectors"
        else None
    )
    if not mod_name or importlib.util.find_spec(mod_name) is None:
        return None
    return importlib.import_module(mod_name)


def _locate_registry_log(log_dir: Path) -> Path:
    d = Path(log_dir)
    if not d.is_dir():
        raise HoldoutProbeRefused(f"registry log_dir does not exist: {d}")
    candidates = sorted(
        p for p in d.glob("granular_panel_prereg*.jsonl")
        if not p.name.endswith(".anchors.jsonl")
    )
    if not candidates:
        raise HoldoutProbeRefused(f"no VS1 registry log found in {d}")
    if len(candidates) > 1:
        raise HoldoutProbeRefused(f"multiple registry logs in {d}: {[p.name for p in candidates]}")
    return candidates[0]


def verify_witnessed_discovery_frozen(
    log_dir: Path,
    vault_repo: Path,
    *,
    prereg_sha256: str,
    snapshot_as_of_ts: datetime,
    allow_holdout: bool,
    remote_url: str | None = None,
    expected_manifest_digest: str | None = None,
    expected_manifest_file_sha256: str | None = None,
) -> DiscoveryFrozenProof:
    """Verify explicit holdout authorization, prereg pin, witnessed ``discovery_frozen``, and snapshot time.

    Refuses unless:
    1. ``allow_holdout is True``;
    2. ``prereg_sha256`` is a 64-hex digest matching the registry's header, preregistration, and
       ``discovery_frozen`` records;
    3. ``snapshot_as_of_ts`` is timezone-aware and strictly later than ``discovery_frozen.run_at``;
    4. the local hash chain is intact, carries no terminal STOP or prior ``holdout_result``, and holds
       exactly one ``discovery_frozen`` record;
    5. the off-host witness on the pinned vault ``main`` is append-only, matches the local chain, and
       already covers the ``discovery_frozen`` record (plus every prior ``prices_read`` witness tip);
    6. when a discovery manifest digest/sha is supplied, it matches the discovery's ``inputs_frozen`` record.
    """
    if allow_holdout is not True:
        raise HoldoutProbeRefused("holdout basis probe requires explicit --allow-holdout (allow_holdout=True)")
    if not v1._is_hex64(prereg_sha256):
        raise HoldoutProbeRefused("prereg_sha256 must be a 64-character hex sha256 digest")
    if not isinstance(snapshot_as_of_ts, datetime) or snapshot_as_of_ts.tzinfo is None:
        raise HoldoutProbeRefused("snapshot_as_of_ts must be a timezone-aware datetime")

    log_path = _locate_registry_log(log_dir)
    first_lines = list(_lines(log_path))
    if not first_lines:
        raise HoldoutProbeRefused(f"registry log {log_path} is empty")
    try:
        header = json.loads(first_lines[0])
    except ValueError as exc:
        raise HoldoutProbeRefused("registry header is not valid JSON") from exc
    if not isinstance(header, dict) or header.get("kind") != "header":
        raise HoldoutProbeRefused("first registry record is not a header")
    if header.get("prereg_sha256") != prereg_sha256:
        raise HoldoutProbeRefused(
            f"prereg_sha256 mismatch: requested {prereg_sha256[:12]}, registry header carries "
            f"{str(header.get('prereg_sha256'))[:12]}"
        )
    study = str(header.get("version") or "")
    log_name, anchor_name, lock_name, witness_path = _registry_filenames(study)
    if log_path.name != log_name:
        raise HoldoutProbeRefused(f"registry filename {log_path.name!r} does not match study {study!r} ({log_name!r})")

    log = ForwardLog(
        Path(log_dir), log_filename=log_name, anchor_filename=anchor_name,
        lock_filename=lock_name, prereg_sha256=prereg_sha256,
    )
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise HoldoutProbeRefused(f"registry chain is broken: {check['detail']}")
        records = log.read_all()
        line_hashes = v1._line_sha256(log)
        local_anchors = list(_lines(log.anchor_path))

    if len(records) < 2 or records[1].get("kind") != "preregistration":
        raise HoldoutProbeRefused("registry lacks header + preregistration baseline")
    if records[1].get("prereg_sha256") != prereg_sha256:
        raise HoldoutProbeRefused("prereg_sha256 mismatch in preregistration record")
    if not local_anchors:
        raise HoldoutProbeRefused("registry anchor file is empty")

    # If the study version has a pinned code module, enforce its pinned prereg hash and registration.
    mod = _load_study_module(study)
    if mod is not None:
        pinned_body = getattr(mod, "PREREG_BODY_SHA256", None)
        if pinned_body is not None and prereg_sha256 != pinned_body:
            raise HoldoutProbeRefused(f"prereg_sha256 does not match the pinned {study} PREREG_BODY_SHA256")
        pinned_recs = getattr(mod, "REGISTERED_RECORD_SHA256", None)
        if pinned_recs is not None and tuple(line_hashes[:2]) != tuple(pinned_recs):
            raise HoldoutProbeRefused(f"registry does not match the pinned {study} registration")
        pinned_anchor = getattr(mod, "REGISTERED_ANCHOR_LINE", None)
        if pinned_anchor is not None and local_anchors[0] != pinned_anchor:
            raise HoldoutProbeRefused(f"registry anchor does not match the pinned {study} anchor line")

    if v1._kind(records, "status"):
        raise HoldoutProbeRefused(f"{study} registry carries a terminal STOP status record")
    if v1._kind(records, "holdout_result"):
        raise HoldoutProbeRefused(f"{study} holdout_result is already sealed in this registry")

    sealed = v1._kind(records, "discovery_frozen")
    if len(sealed) != 1:
        raise HoldoutProbeRefused(f"{study} registry holds {len(sealed)} discovery_frozen records; need exactly one")
    frozen_rec = sealed[0]
    if frozen_rec.get("prereg_sha256") != prereg_sha256:
        raise HoldoutProbeRefused("prereg_sha256 mismatch in discovery_frozen record")
    frozen_position = v1._position(records, "discovery_frozen")
    frozen_sha256 = v1._record_sha256(frozen_rec)
    frozen_run_at_raw = frozen_rec.get("run_at")
    if not isinstance(frozen_run_at_raw, str):
        raise HoldoutProbeRefused("discovery_frozen record lacks run_at")
    frozen_run_at = stamp(frozen_run_at_raw)

    if snapshot_as_of_ts.astimezone(timezone.utc) <= frozen_run_at:
        raise HoldoutProbeRefused(
            f"snapshot_as_of_ts ({snapshot_as_of_ts.astimezone(timezone.utc).isoformat()}) must be "
            f"strictly later than discovery_frozen.run_at ({frozen_run_at_raw})"
        )

    inputs_sha = str(frozen_rec.get("inputs_frozen_sha256") or "")
    matched_inputs = [r for r in v1._kind(records, "inputs_frozen") if v1._record_sha256(r) == inputs_sha]
    if len(matched_inputs) != 1:
        raise HoldoutProbeRefused("discovery_frozen does not name a single inputs_frozen record of this chain")
    frozen_manifest_sha = (matched_inputs[0].get("inputs") or {}).get("price_manifest_sha256")
    if frozen_manifest_sha and (expected_manifest_digest or expected_manifest_file_sha256):
        if frozen_manifest_sha not in {expected_manifest_digest, expected_manifest_file_sha256}:
            raise HoldoutProbeRefused(
                "supplied --manifest does not match the discovery's inputs_frozen price_manifest_sha256"
            )

    # Off-host witness verification (vault main must witness discovery_frozen, append-only)
    url = v1.WITNESS_REMOTE_URL if remote_url is None else remote_url
    repo = Path(vault_repo)
    witness_ref = f"refs/{study}-holdout-witness/main"
    first_anchor = local_anchors[0]
    try:
        v1._git(repo, "rev-parse", "--git-dir")
        v1._git(
            repo, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", url,
            f"+refs/heads/{v1.WITNESS_BRANCH}:{witness_ref}",
        )
        tip = v1._git(repo, "rev-parse", "--verify", f"{witness_ref}^{{commit}}").strip()
        _, previous = v2._walk_witness(repo, witness_ref, witness_path, first_anchor, url)
        at_tip = v1._git(repo, "show", f"{tip}:{witness_path}", binary=True).replace(b"\r\n", b"\n")
        if [line for line in at_tip.split(b"\n") if line] != previous:
            raise HoldoutProbeRefused("the witness file at the fetched tip is not its last walked version")
    except HoldoutProbeRefused:
        raise
    except PermissionError as exc:
        raise HoldoutProbeRefused(f"off-host witness check failed: {exc}") from exc

    witness_content = b"\n".join(previous) + b"\n"
    with tempfile.TemporaryDirectory() as scratch:
        ext_path = Path(scratch) / Path(witness_path).name
        ext_path.write_bytes(witness_content)
        with log.locked():
            ext_check = log.verify_chain(external_anchors=ext_path)
    if not ext_check["ok"]:
        raise HoldoutProbeRefused(f"off-host anchor log does not witness this registry: {ext_check['detail']}")
    covered = int(json.loads(previous[-1])["records"])
    if covered < frozen_position:
        raise HoldoutProbeRefused(
            f"off-host witness covers {covered} records; discovery_frozen at record {frozen_position} "
            f"must be witnessed on {v1.WITNESS_BRANCH}:{witness_path} first"
        )
    import subprocess
    for record in v1._kind(records, "prices_read"):
        wtip = record.get("witness_tip")
        if wtip:
            res = subprocess.run(
                ["git", "-C", str(repo), "merge-base", "--is-ancestor", str(wtip), tip],
                capture_output=True, check=False,
            )
            if res.returncode != 0:
                raise HoldoutProbeRefused(
                    f"off-host witness no longer contains earlier prices_read commit {str(wtip)[:12]}"
                )

    return DiscoveryFrozenProof(
        study=study,
        prereg_sha256=prereg_sha256,
        discovery_frozen_sha256=frozen_sha256,
        discovery_sha256=str(frozen_rec.get("discovery_sha256") or ""),
        run_at=frozen_run_at_raw,
        run_at_dt=frozen_run_at,
        position=frozen_position,
        witnessed_records=covered,
        witness_tip=tip,
        witness_path=witness_path,
        inputs_frozen_sha256=inputs_sha,
        price_manifest_sha256=frozen_manifest_sha,
    )


# --- C1 submissions loader (rejects Form 4 transaction tables) -------------------------------------


def _submissions_column_names(path: Path) -> set[str]:
    p = Path(path)
    if ".parquet" in [s.lower() for s in p.suffixes]:
        import pyarrow.parquet as pq
        return {c.strip().upper() for c in pq.ParquetFile(p).schema_arrow.names}
    with open(p, "r", encoding="utf-8") as stream:
        first_line = stream.readline()
    return {c.strip().strip('"').upper() for c in first_line.split(",") if c.strip()}


def check_submissions_not_form4(submissions: Path) -> None:
    """Refuse any file carrying Form 4 transaction columns."""
    cols = _submissions_column_names(Path(submissions))
    forbidden = sorted(cols & FORBIDDEN_FORM4_COLUMNS)
    if forbidden:
        raise HoldoutProbeRefused(
            f"Form 4 transaction columns detected ({forbidden}): holdout basis probe accepts no Form 4 input"
        )


def load_c1_interval(submissions: Path, issuers: Sequence[Mapping[str, Any]]) -> gd4.C1Interval:
    """Build the C1 ticker interval from SUBMISSION metadata only (refuses Form 4 transaction tables)."""
    check_submissions_not_form4(Path(submissions))
    return gd4.C1Interval.from_submissions(Path(submissions), issuers)


# --- read-only database queries (strictly inside holdout window + single boundary anchor) ----------

_SOURCE_SQL = "SELECT id, name FROM source_catalog WHERE LOWER(name) = LOWER(:name)"
_ROWS_SQL = (
    "SELECT obs_date, value, pull_timestamp FROM raw_series "
    "WHERE series_id = :sid AND source_id = :src AND pull_status = 'SUCCESS' "
    "AND obs_date >= :lo AND obs_date <= :hi AND pull_timestamp <= :ts ORDER BY obs_date, pull_timestamp"
)
_STATUS_SQL = (
    "SELECT pull_status, COUNT(*) FROM raw_series WHERE series_id = :sid AND source_id = :src "
    "AND obs_date >= :lo AND obs_date <= :hi AND pull_timestamp <= :ts GROUP BY pull_status"
)
_OTHER_SOURCES_SQL = "SELECT id, name FROM source_catalog WHERE id <> :src ORDER BY id"
_OTHER_ROWS_SQL = text(
    "SELECT sc.name, r.pull_status, COUNT(*) FROM raw_series r JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE r.series_id = :sid AND r.source_id IN :others AND r.obs_date >= :lo AND r.obs_date <= :hi "
    "AND r.pull_timestamp <= :ts GROUP BY sc.name, r.pull_status"
).bindparams(bindparam("others", expanding=True))


def resolve_source(conn, name: str) -> tuple[int, str]:
    check_source(name)
    found = conn.execute(text(_SOURCE_SQL), {"name": name}).fetchall()
    if len(found) != 1:
        raise HoldoutProbeRefused(f"source_catalog has {len(found)} rows named {name!r}; need exactly one")
    return int(found[0][0]), str(found[0][1])


def _utc_ts(ts: datetime) -> datetime:
    if not isinstance(ts, datetime) or ts.tzinfo is None:
        raise HoldoutProbeRefused("as_of_ts must be a timezone-aware datetime")
    return ts.astimezone(timezone.utc)


def read_rows(conn, sid: str, source_id: int, lo: date, hi: date, as_of_ts: datetime) -> list[gd4.Row]:
    """Every TIINGO SUCCESS vintage in [lo, hi] bounded by as_of_ts (basis check 1)."""
    check_holdout_window(lo, hi)
    ts = _utc_ts(as_of_ts)
    rows = conn.execute(
        text(_ROWS_SQL), {"sid": sid, "src": source_id, "lo": lo, "hi": hi, "ts": ts}
    ).fetchall()
    return [gd4.Row(gd4._d(r[0]), float(r[1]), gd4._ts(r[2])) for r in rows if r[1] is not None]


def read_selected(conn, sid: str, lo: date, hi: date, as_of_ts: datetime) -> list[gd4.Row]:
    """The frozen read's selected rows via ``store.observations.read_window`` (TIINGO, bounded by as_of_ts)."""
    from store import observations

    check_holdout_window(lo, hi)
    ts = _utc_ts(as_of_ts)
    obs = observations.read_window(conn, sid, source=PRICE_SOURCE, start=lo, as_of=hi, as_of_ts=ts)
    return [gd4.Row(o.obs_date, float(o.value), o.pull_timestamp) for o in obs]


def read_boundary_anchor(conn, sid: str, anchor_date: date, as_of_ts: datetime) -> gd4.Row | None:
    """Read the single selected TIINGO row on the session immediately before 2020-01-01 (splice boundary only)."""
    from store import observations

    if not _is_plain_date(anchor_date):
        raise HoldoutProbeRefused("boundary anchor must be a plain date")
    ts = _utc_ts(as_of_ts)
    earliest = HOLDOUT_START - timedelta(days=MAX_BOUNDARY_LOOKBACK_DAYS)
    if not (earliest <= anchor_date < HOLDOUT_START):
        raise HoldoutProbeRefused(
            f"boundary anchor {anchor_date.isoformat()} outside permitted splice lookback "
            f"[{earliest.isoformat()}, {HOLDOUT_START.isoformat()})"
        )
    obs = observations.read_window(
        conn, sid, source=PRICE_SOURCE, start=anchor_date, as_of=anchor_date, as_of_ts=ts
    )
    if not obs:
        return None
    o = obs[-1]
    return gd4.Row(o.obs_date, float(o.value), o.pull_timestamp)


def read_statuses(conn, sid: str, source_id: int, lo: date, hi: date, as_of_ts: datetime) -> dict[str, int]:
    check_holdout_window(lo, hi)
    ts = _utc_ts(as_of_ts)
    return {
        str(s): int(n)
        for s, n in conn.execute(
            text(_STATUS_SQL), {"sid": sid, "src": source_id, "lo": lo, "hi": hi, "ts": ts}
        ).fetchall()
    }


def read_other_sources(conn, source_id: int) -> tuple[tuple[int, str], ...]:
    return tuple((int(i), str(n)) for i, n in conn.execute(text(_OTHER_SOURCES_SQL), {"src": source_id}).fetchall())


def read_other_source_rows(
    conn, sid: str, source_id: int, lo: date, hi: date, as_of_ts: datetime,
    other_source_ids: Sequence[int] | None = None,
) -> dict[str, dict[str, int]]:
    check_holdout_window(lo, hi)
    ts = _utc_ts(as_of_ts)
    ids = [i for i, _ in read_other_sources(conn, source_id)] if other_source_ids is None else list(other_source_ids)
    if source_id in ids:
        raise HoldoutProbeRefused("other-source list contains the admitted source")
    out: dict[str, dict[str, int]] = {}
    if not ids:
        return out
    for name, status, n in conn.execute(
        _OTHER_ROWS_SQL, {"sid": sid, "others": ids, "lo": lo, "hi": hi, "ts": ts}
    ).fetchall():
        out.setdefault(str(name), {})[str(status)] = int(n)
    return out


def read_td_splits(conn, ticker: str, lo: date, hi: date, as_of_ts: datetime) -> list[tuple[date, float]]:
    check_holdout_window(lo, hi)
    found = conn.execute(text(_SOURCE_SQL), {"name": gd4.TD_SPLITS_SOURCE}).fetchall()
    if len(found) != 1:
        return []
    rows = read_rows(conn, f"{gd4.TD_SPLITS_SOURCE}:{ticker}:ratio", int(found[0][0]), lo, hi, as_of_ts)
    by_date, _ = gd4.collapse_vintages(rows)
    return sorted(by_date.items())


# --- holdout vendor files (TwelveData & Tiingo metadata) -------------------------------------------

_RECEIPT_KEYS = (
    "outcome", "fetched_at", "body_sha256", "file_sha256", "file", "values", "first", "last", "meta",
    "td_code", "params",
)


def _holdout_td_closes_from_body(
    out_dir: Path, entry: Mapping[str, Any], ticker: str, *, lo: date, hi: date,
    boundary_anchor: date | None,
) -> tuple[dict[str, float], dict[str, float], int, int]:
    """Return ``(holdout_closes, boundary_closes, pre_holdout_rows, post_holdout_rows)``."""
    doc = json.loads(fetch.read_gz(Path(out_dir) / entry["file"]))
    fetch._validate_td_symbol(doc, ticker)
    lo_iso, hi_iso = lo.isoformat(), hi.isoformat()
    anchor_iso = boundary_anchor.isoformat() if boundary_anchor is not None else None
    closes: dict[str, float] = {}
    boundary: dict[str, float] = {}
    pre_rows = post_rows = 0
    for v in doc.get("values") or []:
        if not isinstance(v, dict):
            continue
        d = str(v.get("datetime", ""))[:10]
        if not d:
            continue
        if d > hi_iso:
            post_rows += 1
            continue
        if d < lo_iso:
            if anchor_iso is not None and d == anchor_iso:
                try:
                    boundary[d] = float(v["close"])
                except (KeyError, TypeError, ValueError):
                    pass
            else:
                pre_rows += 1
            continue
        try:
            closes[d] = float(v["close"])
        except (KeyError, TypeError, ValueError):
            continue
    return closes, boundary, pre_rows, post_rows


def load_holdout_td_closes(
    out_dir: Path,
    ticker: str,
    *,
    lo: date = HOLDOUT_START,
    hi: date = HOLDOUT_END,
    boundary_anchor: date | None = None,
    discovery_frozen_run_at: datetime | None = None,
    done: Mapping[str, dict] | None = None,
) -> dict[str, Any]:
    """Load verified holdout-window TwelveData closes (both adjust modes) from cached files.

    Fails closed on:
    * cached body symbol mismatch (``fetch._validate_td_symbol``);
    * fetch receipt stamped at or before ``discovery_frozen_run_at``;
    * any row dated after ``hi`` (reserved future period) or before ``lo`` (other than the
      single ``boundary_anchor`` date kept separately for 2020-01-01 splice corroboration).
    """
    check_holdout_window(lo, hi)
    done = fetch.FetchLog(Path(out_dir) / "fetch_log.jsonl").final() if done is None else done
    out: dict[str, Any] = {
        "receipts": {}, "out_of_window_rows": 0, "pre_holdout_rows": 0,
        "problems": [], "boundary_all": {}, "boundary_none": {},
    }
    unavailable = 0
    for adjust in fetch.TD_ADJUST_MODES:
        entry = done.get(f"{ticker}|{adjust}")
        if entry is None:
            continue
        out["receipts"][adjust] = {k: entry.get(k) for k in _RECEIPT_KEYS}
        if entry["outcome"] != "ok":
            unavailable += 1
            continue
        if discovery_frozen_run_at is not None and entry.get("fetched_at"):
            if stamp(entry["fetched_at"]) <= discovery_frozen_run_at:
                out["problems"].append(f"{adjust}:fetched_before_discovery_frozen")
                continue
        closes, boundary, pre_rows, post_rows = _holdout_td_closes_from_body(
            out_dir, entry, ticker, lo=lo, hi=hi, boundary_anchor=boundary_anchor
        )
        if post_rows:
            out["out_of_window_rows"] += post_rows
            out["problems"].append(f"{adjust}:out_of_window_rows")
            continue
        if pre_rows:
            out["pre_holdout_rows"] += pre_rows
            out["problems"].append(f"{adjust}:pre_holdout_rows")
            continue
        out[adjust] = closes
        out[f"boundary_{adjust}"] = boundary
    if all(a in out for a in fetch.TD_ADJUST_MODES):
        out["state"] = "ok"
    elif out["out_of_window_rows"]:
        out["state"] = "out_of_window_rows"
    elif out["pre_holdout_rows"]:
        out["state"] = "pre_holdout_rows"
    elif out["problems"]:
        out["state"] = out["problems"][0].split(":", 1)[1]
    elif unavailable:
        out["state"] = "unavailable"
    else:
        out["state"] = "not_fetched"
    return out


@dataclass
class HoldoutVendorFiles:
    """Verified TwelveData (holdout window) and Tiingo-metadata file caches."""

    twelvedata_dir: Path
    tiingo_meta_dir: Path
    lo: date = HOLDOUT_START
    hi: date = HOLDOUT_END
    boundary_anchor: date | None = None
    discovery_frozen_run_at: datetime | None = None
    _td_done: dict | None = field(default=None, repr=False)
    _meta_done: dict | None = field(default=None, repr=False)

    def td(self, ticker: str) -> dict[str, Any]:
        if self._td_done is None:
            self._td_done = fetch.FetchLog(Path(self.twelvedata_dir) / "fetch_log.jsonl").final()
        return load_holdout_td_closes(
            self.twelvedata_dir, ticker, lo=self.lo, hi=self.hi,
            boundary_anchor=self.boundary_anchor,
            discovery_frozen_run_at=self.discovery_frozen_run_at, done=self._td_done,
        )

    def meta(self, ticker: str) -> dict | None:
        if self._meta_done is None:
            self._meta_done = fetch.FetchLog(Path(self.tiingo_meta_dir) / "fetch_log.jsonl").final()
        return fetch.load_tiingo_meta(self.tiingo_meta_dir, ticker, self._meta_done)


# --- pure per-ticker holdout assessment ------------------------------------------------------------


def assess_holdout_ticker(
    ticker: str,
    *,
    vintages_close: Sequence[gd4.Row],
    vintages_adj: Sequence[gd4.Row],
    selected_close: Sequence[gd4.Row],
    selected_adj: Sequence[gd4.Row],
    statuses: Mapping[str, Mapping[str, int]],
    other_source_rows: Mapping[str, Mapping[str, int]],
    calendar: Sequence[date],
    td: Mapping[str, Any] | None,
    meta: Mapping[str, Any] | None,
    manifest_listed_from: str | None = None,
    sec_name: str | None = None,
    require_entity_match: bool = False,
    is_benchmark: bool = False,
    td_splits: Sequence[tuple[date, float]] = (),
    inside_interval: Sequence[bool] | None = None,
    boundary_anchor: date | None = None,
    boundary_close: gd4.Row | None = None,
    boundary_adj: gd4.Row | None = None,
) -> dict[str, Any]:
    """One ticker's holdout-period basis assessment (v6 §2.3, §8). Contains no prices."""
    reasons: list[str] = []
    close = {r.obs_date: float(r.value) for r in selected_close}
    adj = {r.obs_date: float(r.value) for r in selected_adj}
    present = bool(close) and bool(adj)
    if not present:
        reasons.append(
            "no_source_series" if not close and not adj else
            ("no_adjusted_series" if not adj else "no_close_series")
        )

    # Basis 1: zero multi-valued dates among TIINGO SUCCESS vintages
    _, close_multi = gd4.collapse_vintages(vintages_close)
    _, adj_multi = gd4.collapse_vintages(vintages_adj)
    zero_multi = not close_multi and not adj_multi
    if present and not zero_multi:
        reasons.append("multi_valued_dates")

    # Basis 2: split/distribution consistency
    steps = gd4.factor_steps(close, adj) if present else None
    td_split = gd4.match_td_splits(steps["implied_splits"], td_splits) if steps else {
        "td_splits_in_window": len(td_splits), "td_splits_unmatched": []
    }
    split_ok = (
        bool(steps) and not steps["anomalous_steps"] and not steps["nonpositive_dates"]
        and not td_split["td_splits_unmatched"]
    )
    if present and not split_ok:
        reasons.append("split_inconsistent")

    # Basis 3: no XNYS holdout session inside the ticker's span missing a close
    gaps_adj = gd4.calendar_gaps(adj, calendar)
    gaps_close = gd4.calendar_gaps(close, calendar)
    no_gaps = present and bool(calendar) and gaps_adj["missing_sessions"] == 0 and gaps_close["missing_sessions"] == 0
    if present and not no_gaps:
        reasons.append("calendar_gaps")

    # Basis 4: no QUARANTINED TIINGO row
    quarantined = sum(int(s.get("QUARANTINED", 0)) for s in statuses.values())
    if present and quarantined:
        reasons.append("quarantined_rows")

    # TwelveData state
    td = td or {}
    td_all, td_none = td.get("all"), td.get("none")
    td_receipts = td.get("receipts") or {}
    td_state = td.get("state") or (
        "ok" if td_all and td_none else
        "not_fetched" if len(td_receipts) < len(fetch.TD_ADJUST_MODES) else "unavailable"
    )
    if td_state == "ok" and not (td_all and td_none):
        td_state = "not_fetched"

    sessions = [d.isoformat() for d in calendar]
    adj_iso, close_iso = gd4._iso(adj), gd4._iso(close)

    # Pull-batch splice check including the 2020-01-01 boundary (v6 §2.3, §8)
    has_boundary = boundary_anchor is not None and boundary_close is not None and boundary_adj is not None
    if has_boundary:
        splice_sessions = [boundary_anchor.isoformat(), *sessions]
        splice_sel_adj = [boundary_adj, *selected_adj]
        splice_sel_close = [boundary_close, *selected_close]
        splice_adj_iso = {boundary_anchor.isoformat(): float(boundary_adj.value), **adj_iso}
        splice_close_iso = {boundary_anchor.isoformat(): float(boundary_close.value), **close_iso}
        splice_td_all = ({**(td.get("boundary_all") or {}), **td_all} if td_all else None)
        splice_td_none = ({**(td.get("boundary_none") or {}), **td_none} if td_none else None)
    else:
        splice_sessions = sessions
        splice_sel_adj = list(selected_adj)
        splice_sel_close = list(selected_close)
        splice_adj_iso = adj_iso
        splice_close_iso = close_iso
        splice_td_all = td_all or None
        splice_td_none = td_none or None

    all_positive = present and all(
        v > 0 and math.isfinite(v) for v in (*splice_adj_iso.values(), *splice_close_iso.values())
    )
    splice = (
        v6.splice_check(
            splice_sessions, gd4.batch_labels(splice_sel_adj, splice_sel_close),
            splice_adj_iso, splice_close_iso, splice_td_all, splice_td_none, tol=SPLICE_TOL,
        )
        if all_positive else ({"boundaries": 0, "failed": [], "passed": False, "detail": []} if present else None)
    )
    splice_ok = bool(splice and splice["passed"])
    if present and not splice_ok:
        reasons.append("splice_failed")

    # TwelveData return cross-check (identical tolerances to discovery: v6.CROSSCHECK)
    cross = v6.crosscheck_statistics(sessions, adj_iso, close_iso, td_all or {}, td_none or {}, CROSSCHECK)
    if td_state != "ok":
        reasons.append(f"twelvedata_{td_state}")
    if not cross["passed"]:
        reasons.append(f"crosscheck_{cross['reason']}")

    # Tiingo metadata, entity check, and C1 / startDate bounds (issuers only)
    m = (meta or {}).get("meta") if meta else None
    meta_start_raw = str(m["startDate"])[:10] if m and m.get("startDate") else None
    meta_start: str | None = None
    if meta_start_raw:
        try:
            meta_start = date.fromisoformat(meta_start_raw).isoformat()
        except ValueError:
            meta_start = None

    entity: dict[str, Any] = {"sec_name": sec_name, "tiingo_name": m.get("name") if m else None}
    meta_ok = True
    bounds_ok = True
    effective_start: str | None = None

    if is_benchmark:
        entity["check"] = "not_applicable_benchmark"
    else:
        if not m or not meta_start:
            entity["check"] = "no_meta"
            meta_ok = False
            reasons.append("no_meta")
        elif require_entity_match:
            matched = bool(sec_name) and v6.name_match(sec_name, m.get("name") or "")
            entity["check"] = "match" if matched else "entity_mismatch"
            if not matched:
                meta_ok = False
                reasons.append("entity_mismatch")
        else:
            entity["check"] = "not_checked_no_sic_map"

        if manifest_listed_from and meta_start and meta_start > manifest_listed_from:
            bounds_ok = False
            reasons.append("start_date_shifted")

        starts = [d for d in (manifest_listed_from, meta_start) if d]
        effective_start = max(starts) if starts else None
        if present and effective_start:
            eff_d = date.fromisoformat(effective_start)
            closes_on_or_after = sum(1 for d in adj if d >= eff_d)
            if closes_on_or_after == 0:
                bounds_ok = False
                reasons.append("start_date_after_closes")

    c1_info = gd4.interval_counts(inside_interval, calendar, effective_start, adj) if not is_benchmark else None
    if not is_benchmark and present and c1_info is not None and c1_info["closes_used"] == 0:
        bounds_ok = False
        reasons.append("outside_c1_interval")

    start_bound_summary = None
    if not is_benchmark:
        eff_d = date.fromisoformat(effective_start) if effective_start else None
        before_cnt = sum(1 for d in adj if eff_d is not None and d < eff_d)
        start_bound_summary = {
            "manifest_listed_from": manifest_listed_from,
            "tiingo_start_date": meta_start,
            "effective_start_date": effective_start,
            "closes_before_start_date": before_cnt,
            "closes_on_or_after_start_date": len(adj) - before_cnt,
        }

    low_close = sum(1 for v in close.values() if v < gd4.LOW_PRICE_USD)
    low_adj = sum(1 for v in adj.values() if v < gd4.LOW_PRICE_USD)
    pulls = sorted({r.pull_timestamp for r in [*vintages_close, *vintages_adj] if r.pull_timestamp})
    dates = sorted(set(close) | set(adj))
    passed = bool(
        present and zero_multi and split_ok and no_gaps and not quarantined and splice_ok
        and cross["passed"] and td_state == "ok" and meta_ok and bounds_ok
    )
    return {
        "ticker": ticker,
        "benchmark": is_benchmark,
        "admitted": passed,
        "abstains": not passed,
        "reasons": reasons,
        "listed_from": None if is_benchmark else effective_start,
        "checks": {
            "has_close_and_adj_close": present,
            "zero_multi_valued_dates": zero_multi if present else None,
            "split_consistent": split_ok if present else None,
            "no_gaps": no_gaps if present else None,
            "no_quarantined_rows": (quarantined == 0) if present else None,
            "splice": splice_ok if present else None,
            "crosscheck": cross["passed"],
            "entity": entity["check"],
            "start_date_and_c1_bounds": bounds_ok if not is_benchmark else None,
        },
        "coverage": {
            "first_date": dates[0].isoformat() if dates else None,
            "last_date": dates[-1].isoformat() if dates else None,
            "close_dates": len(close), "adj_close_dates": len(adj),
            "close_without_adj": len(set(close) - set(adj)),
            "adj_without_close": len(set(adj) - set(close)),
            "gaps_adj_close": gaps_adj, "gaps_close": gaps_close,
        },
        "multi_valued": {
            "close_dates": close_multi[:20], "close_count": len(close_multi),
            "adj_close_dates": adj_multi[:20], "adj_close_count": len(adj_multi),
        },
        "factor_steps": steps,
        "twelvedata_splits": td_split,
        "splice": None if splice is None else {
            "boundaries": splice["boundaries"],
            "failed": splice["failed"],
            "passed": splice["passed"],
            "boundary_2020_01_01_checked": bool(
                has_boundary and calendar and calendar[0] in adj and calendar[0] in close
            ),
            "detail": [
                {"pair": b["pair"], "step": round(b["step"], 8), "passed": b["passed"]}
                for b in splice["detail"]
            ],
        },
        "crosscheck": cross,
        "twelvedata": {
            "state": td_state,
            "receipts": td_receipts,
            "dates": {a: len(td.get(a) or {}) for a in fetch.TD_ADJUST_MODES},
            "out_of_window_rows": int(td.get("out_of_window_rows", 0)),
            "pre_holdout_rows": int(td.get("pre_holdout_rows", 0)),
            "problems": td.get("problems") or [],
        },
        "tiingo_meta": {"receipt": (meta or {}).get("receipt"), "meta": m},
        "entity": entity,
        "start_date_bound": start_bound_summary,
        "c1": c1_info,
        "low_price": {"sessions_close_below_1usd": low_close, "sessions_adj_close_below_1usd": low_adj},
        "source_filtering": {
            "other_source_rows": {k: dict(sorted(v.items())) for k, v in sorted(other_source_rows.items())},
            "other_source_rows_total": int(sum(sum(v.values()) for v in other_source_rows.values())),
        },
        "pull_batches": {
            "count": len(pulls),
            "first": pulls[0].isoformat() if pulls else None,
            "last": pulls[-1].isoformat() if pulls else None,
            "selected_batches": sorted(set(gd4.batch_labels(splice_sel_adj, splice_sel_close).values())),
        },
        "row_status_counts": {k: dict(sorted(v.items())) for k, v in sorted(statuses.items())},
        "rows_examined": {
            "close": len(vintages_close), "adj_close": len(vintages_adj),
            "close_sha256": gd4.rows_sha256(vintages_close),
            "adj_close_sha256": gd4.rows_sha256(vintages_adj),
            "selected_close_sha256": gd4.rows_sha256(selected_close),
            "selected_adj_close_sha256": gd4.rows_sha256(selected_adj),
        },
    }


# --- database-backed holdout probe run -------------------------------------------------------------


def probe_holdout_ticker(
    conn,
    ticker: str,
    source_id: int,
    lo: date,
    hi: date,
    as_of_ts: datetime,
    *,
    calendar_sessions: Sequence[date],
    boundary_anchor: date | None,
    vendors: HoldoutVendorFiles,
    manifest_listed_from: str | None,
    sec_name: str | None,
    require_entity_match: bool,
    is_benchmark: bool,
    inside_interval: Sequence[bool] | None,
    other_source_ids: Sequence[int] | None = None,
) -> dict[str, Any]:
    check_holdout_window(lo, hi)
    close_sid = gd4.series_id(ticker, gd4.CLOSE_FIELD)
    adj_sid = gd4.series_id(ticker, gd4.ADJ_FIELD)
    others: dict[str, dict[str, int]] = {}
    for sid in (close_sid, adj_sid):
        for name, by_status in read_other_source_rows(
            conn, sid, source_id, lo, hi, as_of_ts, other_source_ids
        ).items():
            for status, n in by_status.items():
                others.setdefault(name, {})[status] = others.get(name, {}).get(status, 0) + n
    b_close = (
        read_boundary_anchor(conn, close_sid, boundary_anchor, as_of_ts)
        if boundary_anchor is not None and lo == HOLDOUT_START else None
    )
    b_adj = (
        read_boundary_anchor(conn, adj_sid, boundary_anchor, as_of_ts)
        if boundary_anchor is not None and lo == HOLDOUT_START else None
    )
    return assess_holdout_ticker(
        ticker,
        vintages_close=read_rows(conn, close_sid, source_id, lo, hi, as_of_ts),
        vintages_adj=read_rows(conn, adj_sid, source_id, lo, hi, as_of_ts),
        selected_close=read_selected(conn, close_sid, lo, hi, as_of_ts),
        selected_adj=read_selected(conn, adj_sid, lo, hi, as_of_ts),
        statuses={
            close_sid: read_statuses(conn, close_sid, source_id, lo, hi, as_of_ts),
            adj_sid: read_statuses(conn, adj_sid, source_id, lo, hi, as_of_ts),
        },
        other_source_rows=others,
        calendar=calendar_sessions,
        td=vendors.td(ticker),
        meta=vendors.meta(ticker),
        manifest_listed_from=manifest_listed_from,
        sec_name=sec_name,
        require_entity_match=require_entity_match,
        is_benchmark=is_benchmark,
        td_splits=read_td_splits(conn, ticker, lo, hi, as_of_ts),
        inside_interval=inside_interval,
        boundary_anchor=boundary_anchor if lo == HOLDOUT_START else None,
        boundary_close=b_close,
        boundary_adj=b_adj,
    )


def run_holdout_probe(
    conn,
    *,
    manifest: PriceManifestInfo,
    calendar: CalendarManifest,
    lo: date = HOLDOUT_START,
    hi: date = HOLDOUT_END,
    as_of_ts: datetime,
    vendors: HoldoutVendorFiles,
    tickers: Sequence[str] | None = None,
    sec_names: Mapping[str, str] | None = None,
    interval: gd4.C1Interval | None = None,
    progress=None,
    **forbidden: Any,
) -> dict[str, Any]:
    """Run holdout basis checks for tickers in ``manifest.admitted`` plus the XLK magnitudes check."""
    check_no_form4_input(**forbidden)
    if forbidden:
        raise TypeError(f"unexpected keyword arguments: {sorted(forbidden)}")
    check_holdout_window(lo, hi)
    check_source(manifest.source)
    benchmark = manifest.benchmark
    if benchmark != BENCHMARK:
        raise HoldoutProbeRefused(f"holdout basis probe expects benchmark {BENCHMARK!r}")

    manifest_admitted = set(manifest.admitted)
    if tickers is not None:
        requested = {str(t).strip().upper() for t in tickers}
        extra = sorted(requested - manifest_admitted)
        if extra:
            raise HoldoutProbeRefused(
                f"requested tickers outside the frozen manifest's admitted set: {extra[:10]}"
            )
        wanted_issuers = sorted(requested - {benchmark})
    else:
        wanted_issuers = manifest.admitted_issuers

    source_id, source_name = resolve_source(conn, manifest.source)
    other_sources = read_other_sources(conn, source_id)
    other_ids = [i for i, _ in other_sources]
    sessions = calendar.window_sessions(lo, hi)
    boundary_anchor = calendar.boundary_anchor if lo == HOLDOUT_START else None
    mask = interval.mask(sessions) if interval is not None and sessions else None
    names = dict(sec_names or {})
    require_entity = bool(sec_names)

    benchmark_record = probe_holdout_ticker(
        conn, benchmark, source_id, lo, hi, as_of_ts,
        calendar_sessions=sessions, boundary_anchor=boundary_anchor,
        vendors=vendors, manifest_listed_from=None, sec_name=None,
        require_entity_match=False, is_benchmark=True, inside_interval=None,
        other_source_ids=other_ids,
    )

    records: dict[str, dict[str, Any]] = {}
    if benchmark in manifest_admitted and (tickers is None or benchmark in {str(t).strip().upper() for t in tickers}):
        records[benchmark] = benchmark_record

    for i, t in enumerate(wanted_issuers):
        inside = list(mask[t].to_numpy(dtype=bool)) if mask is not None and t in mask.columns else None
        try:
            records[t] = probe_holdout_ticker(
                conn, t, source_id, lo, hi, as_of_ts,
                calendar_sessions=sessions, boundary_anchor=boundary_anchor,
                vendors=vendors, manifest_listed_from=manifest.listed_from.get(t),
                sec_name=names.get(t), require_entity_match=require_entity,
                is_benchmark=False, inside_interval=inside, other_source_ids=other_ids,
            )
        except HoldoutProbeRefused:
            raise
        except Exception as exc:  # per-ticker runtime error abstains visibly
            records[t] = {
                "ticker": t, "benchmark": False, "admitted": False, "abstains": True,
                "reasons": ["probe_error"], "listed_from": None, "error": type(exc).__name__,
            }
        if progress:
            progress(i + 1, len(wanted_issuers), t)

    return {
        "source": {"name": source_name, "id": source_id},
        "calendar_sessions": len(sessions),
        "calendar_range": [sessions[0].isoformat(), sessions[-1].isoformat()],
        "boundary_anchor": boundary_anchor.isoformat() if boundary_anchor is not None else None,
        "interval_receipt_sha256": interval.receipt_sha256 if interval is not None else None,
        "other_sources": [{"id": i, "name": n} for i, n in other_sources],
        "benchmark": benchmark,
        "benchmark_record": benchmark_record,
        "records": records,
    }


# --- report builders and write-once output ---------------------------------------------------------


def _count(items: Iterable[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for k in items:
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def _common(
    probe: Mapping[str, Any], *, proof: DiscoveryFrozenProof, lo: date, hi: date,
    code_sha: str, snapshot_as_of_ts: datetime,
) -> dict[str, Any]:
    return {
        "prereg": {"study": proof.study, "body_sha256": proof.prereg_sha256, "rules": "v6 §2.3, §8"},
        "code_sha": code_sha,
        "snapshot_as_of_ts": snapshot_as_of_ts.astimezone(timezone.utc).isoformat(),
        "window": "holdout",
        "read_window": {
            "start": lo.isoformat(),
            "end": hi.isoformat(),
            "boundary_anchor": probe.get("boundary_anchor"),
        },
        "discovery_frozen": {
            "sha256": proof.discovery_frozen_sha256,
            "discovery_sha256": proof.discovery_sha256,
            "run_at": proof.run_at,
            "position": proof.position,
            "witnessed_records": proof.witnessed_records,
            "witness_tip": proof.witness_tip,
            "witness_path": proof.witness_path,
        },
        "source": dict(probe["source"]),
        "promotion_allowed": False,
    }


def build_holdout_crosscheck_report(
    probe: Mapping[str, Any], *, proof: DiscoveryFrozenProof, lo: date, hi: date,
    code_sha: str, snapshot_as_of_ts: datetime, twelvedata_fetch_log_sha256: str | None,
) -> dict[str, Any]:
    common = _common(probe, proof=proof, lo=lo, hi=hi, code_sha=code_sha, snapshot_as_of_ts=snapshot_as_of_ts)
    all_recs = dict(probe["records"])
    benchmark = probe["benchmark"]
    if benchmark not in all_recs and probe.get("benchmark_record"):
        all_recs[benchmark] = probe["benchmark_record"]
    tickers = {
        t: {**r["crosscheck"], "twelvedata": r["twelvedata"]}
        for t, r in sorted(all_recs.items()) if "crosscheck" in r
    }
    return {
        "report": f"{proof.study}-holdout-twelvedata-crosscheck",
        **common,
        "rule": dataclasses.asdict(CROSSCHECK),
        "fetch_log_sha256": twelvedata_fetch_log_sha256,
        "what": (
            "holdout-window agreement statistics only: common dates, consecutive-session pairs, "
            "within-Y share, excluded adjustment pairs, dropped pairs, first/last; "
            "no Form 4 input, event, label or IC"
        ),
        "summary": {
            "tickers": len(tickers),
            "passed": sum(1 for r in tickers.values() if r["passed"]),
            "by_reason": _count(r["reason"] for r in tickers.values()),
            "twelvedata_state": _count(r["twelvedata"]["state"] for r in tickers.values()),
            "twelvedata_unavailable": sorted(t for t, r in tickers.items() if r["twelvedata"]["state"] == "unavailable"),
            "twelvedata_not_fetched": sorted(t for t, r in tickers.items() if r["twelvedata"]["state"] == "not_fetched"),
            "below_n_pairs": sorted(t for t, r in tickers.items() if r["pairs"] < CROSSCHECK.min_pairs),
            "excluded_adjustment_pairs_total": sum(r["excluded_adjustment_pairs"] for r in tickers.values()),
            "dropped_nonconsecutive_total": sum(r["dropped_nonconsecutive"] for r in tickers.values()),
            "out_of_window_rows_total": sum(int(r["twelvedata"].get("out_of_window_rows", 0)) for r in tickers.values()),
            "pre_holdout_rows_total": sum(int(r["twelvedata"].get("pre_holdout_rows", 0)) for r in tickers.values()),
        },
        "tickers": tickers,
    }


def _slim_record(r: Mapping[str, Any]) -> dict[str, Any]:
    out = {k: v for k, v in r.items() if k not in ("crosscheck", "tiingo_meta")}
    if "crosscheck" in r:
        out["crosscheck"] = {k: r["crosscheck"][k] for k in ("passed", "reason", "pairs", "share_within")}
    return out


def _holdout_summary(
    records: Mapping[str, Mapping[str, Any]], benchmark_record: Mapping[str, Any], benchmark: str
) -> dict[str, Any]:
    issuers = {t: r for t, r in records.items() if t != benchmark}
    admitted_issuers = sorted(t for t, r in issuers.items() if not r["abstains"])
    abstaining_issuers = sorted(t for t, r in issuers.items() if r["abstains"])
    low = {t for t, r in issuers.items() if (r.get("low_price") or {}).get("sessions_close_below_1usd")}
    others = [r for r in issuers.values() if (r.get("source_filtering") or {}).get("other_source_rows_total")]
    by_source: dict[str, int] = {}
    for r in records.values():
        for name, by_status in ((r.get("source_filtering") or {}).get("other_source_rows") or {}).items():
            by_source[name] = by_source.get(name, 0) + sum(by_status.values())
    c1 = [r["c1"] for r in issuers.values() if r.get("c1")]
    return {
        "manifest_issuers": len(issuers),
        "admitted_issuers": len(admitted_issuers),
        "abstaining_issuers": len(abstaining_issuers),
        "abstaining_by_reason": _count(x for r in issuers.values() if r["abstains"] for x in r["reasons"]),
        "xlk_admitted_for_magnitudes": bool(benchmark_record.get("admitted")),
        "source_filtering": {
            "tickers_with_other_source_rows": len(others),
            "other_source_rows_by_source": dict(sorted(by_source.items())),
        },
        "splice": {
            "tickers_with_boundaries": sum(1 for r in records.values() if (r.get("splice") or {}).get("boundaries")),
            "boundaries_total": sum((r.get("splice") or {}).get("boundaries", 0) for r in records.values()),
            "tickers_failed": sorted(t for t, r in records.items() if "splice_failed" in r["reasons"]),
        },
        "low_price": {
            "threshold_usd": gd4.LOW_PRICE_USD,
            "tickers_with_close_below_threshold": len(low),
            "excluded_by_splice": sorted(t for t in low if "splice_failed" in issuers[t]["reasons"]),
            "excluded_by_crosscheck": sorted(
                t for t in low if any(x.startswith("crosscheck_") for x in issuers[t]["reasons"])
            ),
            "tickers": sorted(low),
        },
        "c1_interval": None if not c1 else {
            "tickers": len(c1),
            "sessions_outside_interval_total": sum(x["outside_interval"] for x in c1),
            "closes_blanked_total": sum(x["closes_blanked"] for x in c1),
            "tickers_with_closes_blanked": sum(1 for x in c1 if x["closes_blanked"]),
            "tickers_never_inside": sorted(
                t for t, r in issuers.items() if r.get("c1") and not r["c1"]["inside_interval"]
            ),
        },
        "promotion_allowed": False,
    }


def build_holdout_basis_report(
    probe: Mapping[str, Any],
    *,
    proof: DiscoveryFrozenProof,
    manifest: PriceManifestInfo,
    calendar: CalendarManifest,
    lo: date,
    hi: date,
    code_sha: str,
    snapshot_as_of_ts: datetime,
    inputs: Mapping[str, Any],
    crosscheck_report_sha256: str,
) -> dict[str, Any]:
    records = probe["records"]
    benchmark = probe["benchmark"]
    bench_rec = probe["benchmark_record"]
    admitted = sorted(t for t, r in records.items() if not r["abstains"])
    abstaining = sorted(t for t, r in records.items() if r["abstains"])
    common = _common(probe, proof=proof, lo=lo, hi=hi, code_sha=code_sha, snapshot_as_of_ts=snapshot_as_of_ts)
    return {
        "probe": PROBE_NAME,
        "probe_version": PROBE_VERSION,
        **common,
        "manifest": {
            "sha256": manifest.file_sha256,
            "digest": manifest.digest,
            "admitted_count": len(manifest.admitted),
        },
        "calendar": {
            "sha256": calendar.sha256,
            "calendar_version": calendar.calendar_version,
            "sessions": probe["calendar_sessions"],
            "range": probe["calendar_range"],
        },
        "series_template": SERIES_TEMPLATE,
        "basis": BASIS,
        "benchmark": benchmark,
        "xlk_admitted_for_magnitudes": bool(bench_rec.get("admitted")),
        "benchmark_check": _slim_record(bench_rec),
        "tolerances": {
            "same_value_rtol": gd4.SAME_VALUE_RTOL,
            "flat_rtol": gd4.FLAT_RTOL,
            "split_rtol": gd4.SPLIT_RTOL,
            "max_split_term": gd4.MAX_SPLIT_TERM,
            "max_whole_split": gd4.MAX_WHOLE_SPLIT,
            "min_split_move": gd4.MIN_SPLIT_MOVE,
            "max_distribution_drop": gd4.MAX_DISTRIBUTION_DROP,
            "td_match_days": gd4.TD_MATCH_DAYS,
            "splice_tol": SPLICE_TOL,
            "crosscheck": dataclasses.asdict(CROSSCHECK),
            "low_price_usd": gd4.LOW_PRICE_USD,
        },
        "interpretation": list(INTERPRETATION),
        "crosscheck_report_sha256": crosscheck_report_sha256,
        "c1_admission_receipt_sha256": probe.get("interval_receipt_sha256"),
        "inputs": dict(inputs),
        "summary": _holdout_summary(records, bench_rec, benchmark),
        "admitted": admitted,
        "abstaining": abstaining,
        "tickers": {t: _slim_record(records[t]) for t in sorted(records)},
    }


def write_holdout_outputs(
    out: Path,
    probe: Mapping[str, Any],
    *,
    proof: DiscoveryFrozenProof,
    manifest: PriceManifestInfo,
    calendar: CalendarManifest,
    lo: date,
    hi: date,
    code_sha: str,
    snapshot_as_of_ts: datetime,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Write ``crosscheck_report.json``, ``holdout_basis_report.json``, and ``sha256s.txt`` once."""
    out_dir = check_write_once_out(out)
    out_dir.mkdir(parents=True, exist_ok=False)
    hashes: dict[str, str] = {}
    cross = build_holdout_crosscheck_report(
        probe, proof=proof, lo=lo, hi=hi, code_sha=code_sha, snapshot_as_of_ts=snapshot_as_of_ts,
        twelvedata_fetch_log_sha256=inputs.get("twelvedata_fetch_log_sha256"),
    )
    hashes["crosscheck_report.json"] = gd4.write_json_once(out_dir / "crosscheck_report.json", cross)
    report = build_holdout_basis_report(
        probe, proof=proof, manifest=manifest, calendar=calendar, lo=lo, hi=hi,
        code_sha=code_sha, snapshot_as_of_ts=snapshot_as_of_ts, inputs=inputs,
        crosscheck_report_sha256=hashes["crosscheck_report.json"],
    )
    hashes["holdout_basis_report.json"] = gd4.write_json_once(out_dir / "holdout_basis_report.json", report)
    for name, h in hashes.items():
        assert gd4.file_sha256(out_dir / name) == h
    with open(out_dir / "sha256s.txt", "x", encoding="utf-8", newline="\n") as stream:
        stream.writelines(f"{h}  {name}\n" for name, h in sorted(hashes.items()))
    return {
        "out": str(out_dir),
        "window": report["window"],
        "summary": report["summary"],
        "xlk_admitted_for_magnitudes": report["xlk_admitted_for_magnitudes"],
        "admitted": report["admitted"],
        "abstaining": report["abstaining"],
        "sha256": hashes,
    }
