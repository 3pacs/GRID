"""VS1 v9: v8 rules with a pinned XNYS session calendar and no XLK benchmark admission gating.

Registration and every opening require the exact witnessed v8 terminal STOP
(``STOP_SUPERSEDED_BY_V9_UNOPENED``) and the exact witnessed ``sectors-v6`` terminal STOP
(``STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED``). Price admission binds the existing v8 probe
triple with no database rerun (owner ruling R9); the session calendar comes from the pinned
XNYS manifest ``docs/reference/calendar-manifests/vs1-xnys-2007-11-01-2026-06-30.json``
rather than XLK closes; labels are raw split-first issuer forward returns (no XLK
subtraction); XLK-relative magnitudes read ``"not available: benchmark not admitted"``
whenever XLK is not admitted for a window and never gate any decision. Discovery reads must
reproduce every admitted issuer's probe ``selected_adj_close_sha256`` before recording
``prices_read``. Stage-0, confirmatory selection (one-sided ``A90|fwd5`` at 0.05) and holdout
rules match v8 unchanged. Nothing here is a trading signal (``promotion_allowed=false``).
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v6 as s6
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis import price_admission_probe as gd4
from analysis.offline_research_proof import digest, stamp
from analysis.research_forward_log import canonical
from store import observations

VERSION = "vs1-v9"
REGISTRY_ID = VERSION
PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v9-preregistration.md")
#: SHA-256 of the UTF-8 LF body strictly between <!-- PREREG-BODY-START --> and <!-- PREREG-BODY-END -->.
#: Recompute and re-pin after filling operator placeholder tokens in the body.
PREREG_BODY_SHA256 = "befebfc952daa49698c24c9fe8f1686333027c73803d0beb90c898b285d441c9"

#: Pinned XNYS session calendar manifest (replaces XLK closes as the session calendar).
XNYS_MANIFEST_PATH = Path("docs/reference/calendar-manifests/vs1-xnys-2007-11-01-2026-06-30.json")
XNYS_MANIFEST_SHA256 = "9b8f0079f89eb6b5f744a6466373fb9af1af74abaafb01b68dc89e39984372ac"
XNYS_CALENDAR_SCHEMA = "grid.xnys-session-manifest.v1"
XNYS_CALENDAR_VERSION = (
    "exchange_calendars:4.11.2:pandas:2.3.3:numpy:2.4.6:tzdata:2026.6:XNYS:2007-11-01:2026-06-30"
)
XNYS_START = "2007-11-01"
XNYS_END = "2026-06-30"
_NY_TZ = ZoneInfo("America/New_York")

#: Bound v8 probe evidence (no discovery probe rerun per owner ruling R9).
#: Operator fills the six placeholder constants below from sha256sum on the real probe/vault files.
PROBE_DIR_NAME = v8.PROBE_DIR_NAME
PROBE_START = v8.PROBE_START
PROBE_END = "2019-12-31"
PROBE_CALENDAR_SESSIONS = 3061
PROBE_SNAPSHOT_AS_OF_TS = v8.PROBE_SNAPSHOT_AS_OF_TS
PROBE_ADMITTED_ISSUERS = v8.PROBE_ADMITTED_ISSUERS
PROBE_REPORT_SHA256: str = v8.PROBE_REPORT_SHA256
CROSSCHECK_REPORT_SHA256: str = v8.CROSSCHECK_REPORT_SHA256
TIINGO_META_REPORT_SHA256: str = v8.TIINGO_META_REPORT_SHA256
PROBE_CODE_SHA: str = v8.PROBE_CODE_SHA
OWNER_RULING_R9_VAULT_COMMIT: str = v8.OWNER_RULING_R9_VAULT_COMMIT
OWNER_RULING_R9_FILE_SHA256: str = v8.OWNER_RULING_R9_FILE_SHA256

#: Witnessed terminal STOP anchors bound by the operator after witnessing v8 and sectors-v6 STOPs.
V8_STOP_HEAD_SHA256: str | None = None
V8_STOP_ANCHOR_LINE: bytes | None = None
V8_STOP_RECORDS = v8.STOP_RECORDS
SECTORS_V6_STOP_HEAD_SHA256: str | None = None
SECTORS_V6_STOP_ANCHOR_LINE: bytes | None = None
SECTORS_V6_STOP_RECORDS = 3
SECTORS_V6_STOP_STATUS = "STOP_SUPERSEDED_BY_SECTORS_V7_UNOPENED"

#: Windows, confirmatory rule, and Stage-0 settings (unchanged from v8).
DISCOVERY_START = v8.DISCOVERY_START
CONFIRMATORY_ALPHA_ONE_SIDED = v8.CONFIRMATORY_ALPHA_ONE_SIDED
GAUSSIAN_SIMS = v8.GAUSSIAN_SIMS
E0_SIMS = v8.E0_SIMS
E0_GATED_SCENARIO = v8.E0_GATED_SCENARIO
E0_REPORTED_SCENARIOS = v8.E0_REPORTED_SCENARIOS
E0_MANIFEST_SHA256 = v8.E0_MANIFEST_SHA256
E0_TRIAL_ORDER = v8.E0_TRIAL_ORDER
DESIGN_TARGET_POWER = v8.DESIGN_TARGET_POWER
STAGE0_SETTINGS: Mapping[str, Any] = v8.STAGE0_SETTINGS
DESIGN: Mapping[str, Any] = {
    "selected": (
        "discovery start 2008-01-01; one-sided confirmatory A90|fwd5 at 0.05; two-model Stage-0; "
        "pinned XNYS session calendar without XLK benchmark admission gating; raw issuer forward returns"
    ),
    "primary_trial": v6.PRIMARY_TRIAL,
    "calendar_manifest": XNYS_MANIFEST_PATH.as_posix(),
    "calendar_manifest_sha256": XNYS_MANIFEST_SHA256,
    "probe_dir": PROBE_DIR_NAME,
    "probe_snapshot_as_of_ts": PROBE_SNAPSHOT_AS_OF_TS,
    "admitted_issuers": PROBE_ADMITTED_ISSUERS,
    "benchmark_admitted": False,
    "basis": (
        "non-outcome v8 probe basis and TwelveData agreement statistics only (owner ruling R9); "
        "no VS1 return, label, rank IC, discovery or holdout outcome read"
    ),
}

#: Registry and witness pins (populated only after the one witnessed v9 registration).
REGISTRY_LOG = "granular_panel_prereg_v9.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v9.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v9.lock"
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_REF = "refs/vs1-v9-witness/main"
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_ANCHOR_LINE: bytes | None = None
SUPERSEDED_BY: Mapping[str, Any] | None = None

#: Witness census baselines (v1-v5 & sectors-v2..v4 frozen at 2; v6, v7, v8 & sectors-v6 terminal at 3).
FROZEN_REGISTRIES = v8.FROZEN_REGISTRIES
TERMINAL_REGISTRIES = {
    v6.VERSION: v7.V6_STOP_RECORDS,
    v7.VERSION: v8.V7_STOP_RECORDS,
    v8.VERSION: V8_STOP_RECORDS,
    v8.SECTORS_V6: SECTORS_V6_STOP_RECORDS,
}
BASELINE_REGISTRIES = frozenset((*FROZEN_REGISTRIES, *TERMINAL_REGISTRIES))
#: Optional ten-sector generalization registration bound to v9; absent or exactly 2 records on openings.
SECTORS_V7 = "sectors-v7"
SECTORS_V7_RECORDS = 2
ALLOWED_REGISTRIES = frozenset((*BASELINE_REGISTRIES, REGISTRY_ID, SECTORS_V7))

#: Price contract constants (inherited from v6/v8; XLK no longer gates admission).
PRICE_SOURCE = v6.PRICE_SOURCE
PRICE_SOURCE_ID = v6.PRICE_SOURCE_ID
SERIES_TEMPLATE = v6.SERIES_TEMPLATE
BASIS = v6.BASIS
BENCHMARK = v6.BENCHMARK
PRIMARY_TRIAL = v6.PRIMARY_TRIAL
SECONDARY_TRIALS = v6.SECONDARY_TRIALS
POST_ADMISSION_POWER = True
BENCHMARK_MAGNITUDE_UNAVAILABLE = "not available: benchmark not admitted"

load_issuer_map = v6.load_issuer_map
load_sic_map = v6.load_sic_map
v2_universe = v6.v2_universe
load_inputs = v6.load_inputs
power_features = v6.power_features
admission_report = v6.admission_report
write_frozen = v6.write_frozen


# --- pin, placeholder, and pre-registration checks -------------------------------------------


def _is_hex40(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{40}", value))


def check_probe_pins() -> None:
    """Refuse while any v8 probe or owner-ruling R9 pin is a placeholder or malformed."""
    hex64_pins = {
        "XNYS_MANIFEST_SHA256": XNYS_MANIFEST_SHA256,
        "PROBE_REPORT_SHA256": PROBE_REPORT_SHA256,
        "CROSSCHECK_REPORT_SHA256": CROSSCHECK_REPORT_SHA256,
        "TIINGO_META_REPORT_SHA256": TIINGO_META_REPORT_SHA256,
        "OWNER_RULING_R9_FILE_SHA256": OWNER_RULING_R9_FILE_SHA256,
    }
    hex40_pins = {
        "PROBE_CODE_SHA": PROBE_CODE_SHA,
        "OWNER_RULING_R9_VAULT_COMMIT": OWNER_RULING_R9_VAULT_COMMIT,
    }
    bad = [
        name for name, val in hex64_pins.items()
        if not v1._is_hex64(val) or "PLACEHOLDER" in str(val)
    ] + [
        name for name, val in hex40_pins.items()
        if not _is_hex40(val) or "PLACEHOLDER" in str(val)
    ]
    if bad:
        raise PermissionError(f"v9 probe pins still contain placeholders or invalid hashes: {bad}")


def check_prereg(repo_root: Path = v2.REPO) -> str:
    """Verify the v9 pre-registration body matches its pinned SHA-256."""
    if not v1._is_hex64(PREREG_BODY_SHA256):
        raise PermissionError("v9 body hash is not bound")
    actual = v1.prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != PREREG_BODY_SHA256:
        raise PermissionError("v9 preregistration body differs from its pin")
    return actual


def _bound() -> tuple[str, str, str]:
    """Require probe pins, body hash, and witnessed v8 + sectors-v6 STOP pins to all be bound."""
    check_probe_pins()
    if not (
        v1._is_hex64(PREREG_BODY_SHA256)
        and v1._is_hex64(V8_STOP_HEAD_SHA256)
        and v1._is_hex64(SECTORS_V6_STOP_HEAD_SHA256)
        and V8_STOP_ANCHOR_LINE is not None
        and SECTORS_V6_STOP_ANCHOR_LINE is not None
    ):
        raise PermissionError("v9 body hash, witnessed v8 STOP and sectors-v6 STOP are not bound")
    return PREREG_BODY_SHA256, V8_STOP_HEAD_SHA256, SECTORS_V6_STOP_HEAD_SHA256


def check_ready_to_register(repo_root: Path = v2.REPO) -> str:
    """Refuse registration while any code pin or preregistration body placeholder remains."""
    body, v8_stop, s6_stop = _bound()
    check_prereg(repo_root)
    text = v1.prereg_body((Path(repo_root) / PREREG_PATH).read_text(encoding="utf-8"))
    if "@@" in text or "PLACEHOLDER" in text:
        raise PermissionError("v9 preregistration body still contains placeholder tokens")
    required_citations = (
        XNYS_MANIFEST_SHA256,
        PROBE_REPORT_SHA256,
        CROSSCHECK_REPORT_SHA256,
        TIINGO_META_REPORT_SHA256,
        PROBE_CODE_SHA,
        OWNER_RULING_R9_VAULT_COMMIT,
        OWNER_RULING_R9_FILE_SHA256,
        v8_stop,
        s6_stop,
    )
    missing = [token for token in required_citations if token not in text]
    if missing:
        raise PermissionError(f"v9 preregistration body omits bound hashes: {missing[:3]}")
    return body


# --- pinned XNYS session calendar manifest (stdlib-only strict validator) --------------------


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in pairs:
        if k in out:
            raise ValueError(f"duplicate JSON key in XNYS calendar manifest: {k!r}")
        out[k] = v
    return out


def _reject_nonfinite(token: str) -> Any:
    raise ValueError(f"non-finite JSON constant in XNYS calendar manifest: {token!r}")


def _parse_utc_minute(raw: Any, label: str) -> datetime:
    if not isinstance(raw, str):
        raise ValueError(f"XNYS calendar {label} must be an ISO timestamp string")
    ts = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    if ts.tzinfo is None or ts.utcoffset() != timedelta(0):
        raise ValueError(f"XNYS calendar {label} must be UTC")
    if ts.second != 0 or ts.microsecond != 0:
        raise ValueError(f"XNYS calendar {label} must be minute-aligned")
    return ts


def load_xnys_calendar(repo_root: Path = v2.REPO) -> tuple[date, ...]:
    """Load and strictly verify the pinned XNYS session manifest; return ordered session dates."""
    root = Path(repo_root)
    rel = XNYS_MANIFEST_PATH
    cur = root
    for part in rel.parts:
        cur = cur / part
        if cur.is_symlink():
            raise PermissionError(f"XNYS calendar manifest path traverses a symlink: {cur}")
    try:
        st = os.lstat(cur)
    except OSError as exc:
        raise PermissionError(f"XNYS calendar manifest missing: {cur}") from exc
    if not stat.S_ISREG(st.st_mode):
        raise PermissionError(f"XNYS calendar manifest is not a regular file: {cur}")
    raw = cur.read_bytes()
    actual_sha = hashlib.sha256(raw).hexdigest()
    if actual_sha != XNYS_MANIFEST_SHA256:
        raise PermissionError(
            f"XNYS calendar manifest sha256 mismatch: got {actual_sha}, pinned {XNYS_MANIFEST_SHA256}"
        )
    doc = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_no_duplicate_keys,
        parse_constant=_reject_nonfinite,
    )
    expected_keys = {"schema", "exchange", "session_type", "timezone", "start", "end", "calendar_version", "sessions"}
    if not isinstance(doc, dict) or set(doc) != expected_keys:
        raise ValueError("XNYS calendar manifest has missing or extra top-level fields")
    if (
        doc["schema"] != XNYS_CALENDAR_SCHEMA
        or doc["exchange"] != "XNYS"
        or doc["session_type"] != "RTH"
        or doc["timezone"] != "America/New_York"
        or doc["start"] != XNYS_START
        or doc["end"] != XNYS_END
        or doc["calendar_version"] != XNYS_CALENDAR_VERSION
    ):
        raise ValueError("XNYS calendar manifest header differs from pinned metadata")
    rows = doc["sessions"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("XNYS calendar manifest sessions must be a non-empty list")
    lo_bound, hi_bound = date.fromisoformat(XNYS_START), date.fromisoformat(XNYS_END)
    dates: list[date] = []
    prev: date | None = None
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"key", "opens", "closes"}:
            raise ValueError("XNYS calendar session entry has missing or extra fields")
        key = row["key"]
        if not isinstance(key, str) or not key.startswith("XNYS:") or not key.endswith(":RTH"):
            raise ValueError(f"invalid XNYS session key: {key!r}")
        d = date.fromisoformat(key.split(":")[1])
        if key != f"XNYS:{d.isoformat()}:RTH":
            raise ValueError(f"non-canonical XNYS session key: {key!r}")
        if d < lo_bound or d > hi_bound:
            raise ValueError(f"XNYS session date out of bounds: {d}")
        if prev is not None and d <= prev:
            raise ValueError(f"XNYS session dates are not strictly ascending at {d}")
        opens = _parse_utc_minute(row["opens"], "opens")
        closes = _parse_utc_minute(row["closes"], "closes")
        dur = closes - opens
        if dur <= timedelta(0) or dur > timedelta(minutes=390):
            raise ValueError(f"invalid XNYS session duration on {d}: {dur}")
        opens_ny = opens.astimezone(_NY_TZ)
        closes_ny = closes.astimezone(_NY_TZ)
        if opens_ny.date() != d or (opens_ny.hour, opens_ny.minute) != (9, 30) or closes_ny.date() != d:
            raise ValueError(f"XNYS session RTH local bounds invalid on {d}")
        dates.append(d)
        prev = d
    if dates[0] != lo_bound or dates[-1] != hi_bound:
        raise ValueError("XNYS calendar manifest endpoints do not cover 2007-11-01..2026-06-30")
    return tuple(dates)


# --- witness anchors, v8 terminal verification, and census -----------------------------------


def _load_sectors_v7():
    """The sectors-v7 module when present (lazy: it imports v9), else None."""
    import importlib
    import importlib.util

    if importlib.util.find_spec("analysis.panel_insider_density_sectors_v7") is None:
        return None
    return importlib.import_module("analysis.panel_insider_density_sectors_v7")


def _sectors_v7_anchor_at_tip(repo: Path, tip: str) -> None:
    """The sectors-v7 witness must be exactly one canonical two-record registration anchor."""
    content = v1._git(repo, "show", f"{tip}:{v1.canonical_witness_path(SECTORS_V7)}", binary=True)
    lines = content.split(b"\n")
    try:
        anchor = json.loads(lines[0]) if len(lines) == 2 and lines[1] == b"" else None
    except ValueError:
        anchor = None
    if (
        not isinstance(anchor, dict)
        or lines[0] != canonical(anchor)
        or anchor.get("records") != SECTORS_V7_RECORDS
        or anchor.get("prev_anchor_sha256") is not None
        or not v1._is_hex64(anchor.get("head_sha256"))
    ):
        raise PermissionError("sectors-v7 witness is not exactly one canonical two-record registration anchor")
    s7 = _load_sectors_v7()
    line = getattr(s7, "REGISTERED_ANCHOR_LINE", None) if s7 is not None else None
    if line is not None and content != line + b"\n":
        raise PermissionError("sectors-v7 witness is not exactly its pinned two-record registration anchor")


def _v8_anchor_at_tip(repo: Path, tip: str, expected_head: str) -> None:
    """Match the exact v8 STOP anchor on the same vault tip as the v9 witness."""
    if V8_STOP_ANCHOR_LINE is None:
        raise PermissionError("v8 STOP anchor line is not bound")
    content = v1._git(repo, "show", f"{tip}:{v8.WITNESS_PATH}", binary=True)
    if b"\r" in content or not content.endswith(b"\n"):
        raise PermissionError("v8 witness must have canonical LF line endings")
    lines = content.splitlines()
    if len(lines) != 2 or lines[0] != v8.REGISTERED_ANCHOR_LINE:
        raise PermissionError("v8 witness is not an exact two-anchor STOP witness")
    try:
        last = json.loads(lines[1])
    except ValueError as exc:
        raise PermissionError("v8 STOP anchor is not JSON") from exc
    if (
        lines[1] != V8_STOP_ANCHOR_LINE
        or lines[1] != canonical(last)
        or last.get("records") != V8_STOP_RECORDS
        or last.get("head_sha256") != expected_head
        or last.get("prev_anchor_sha256") != hashlib.sha256(lines[0]).hexdigest()
    ):
        raise PermissionError("v8 STOP anchor differs from the verified terminal head")


def _sectors_v6_stop_anchor_at_tip(repo: Path, tip: str, expected_head: str) -> None:
    """Match the exact sectors-v6 STOP anchor on the same vault tip as the v9 witness."""
    if SECTORS_V6_STOP_ANCHOR_LINE is None:
        raise PermissionError("sectors-v6 STOP anchor line is not bound")
    content = v1._git(repo, "show", f"{tip}:{s6.WITNESS_PATH}", binary=True)
    if b"\r" in content or not content.endswith(b"\n"):
        raise PermissionError("sectors-v6 witness must have canonical LF line endings")
    lines = content.splitlines()
    if len(lines) != 2 or lines[0] != s6.REGISTERED_ANCHOR_LINE:
        raise PermissionError("sectors-v6 witness is not an exact two-anchor STOP witness")
    try:
        last = json.loads(lines[1])
    except ValueError as exc:
        raise PermissionError("sectors-v6 STOP anchor is not JSON") from exc
    if (
        lines[1] != SECTORS_V6_STOP_ANCHOR_LINE
        or lines[1] != canonical(last)
        or last.get("records") != SECTORS_V6_STOP_RECORDS
        or last.get("head_sha256") != expected_head
        or last.get("prev_anchor_sha256") != hashlib.sha256(lines[0]).hexdigest()
    ):
        raise PermissionError("sectors-v6 STOP anchor differs from the verified terminal head")


def verify_v8_terminal(v8_log_dir: Path, vault_repo: Path, *, remote_url: str | None = None) -> dict:
    """Verify v8's 3-record terminal STOP directly against vault main without calling v8.check_offhost.

    Once ``sectors-v6`` has appended its 3-record STOP, ``v8.check_offhost`` refuses because v8's
    own census requires ``sectors-v6`` at 2 records. v9 therefore fetches vault ``main`` directly,
    walks v8's and sectors-v6's witness histories, verifies the local v8 chain against the fetched
    off-host anchor, and returns the verified terminal proof plus the fetched tip's census.
    """
    _, pinned_v8_stop, pinned_s6_stop = _bound()
    url = WITNESS_REMOTE_URL if remote_url is None else remote_url
    repo = Path(vault_repo)
    v1._git(repo, "rev-parse", "--git-dir")
    v1._git(
        repo, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", url,
        f"+refs/heads/{WITNESS_BRANCH}:{WITNESS_REF}",
    )
    tip = v1._git(repo, "rev-parse", "--verify", f"{WITNESS_REF}^{{commit}}").strip()
    versions, previous = v2._walk_witness(repo, WITNESS_REF, v8.WITNESS_PATH, v8.REGISTERED_ANCHOR_LINE, url)
    content = v1._git(repo, "show", f"{tip}:{v8.WITNESS_PATH}", binary=True)
    if [line for line in content.replace(b"\r\n", b"\n").split(b"\n") if line] != previous:
        raise PermissionError("v8 witness file at fetched tip is not its last walked version")
    _v8_anchor_at_tip(repo, tip, pinned_v8_stop)
    v2._walk_witness(repo, WITNESS_REF, s6.WITNESS_PATH, s6.REGISTERED_ANCHOR_LINE, url)
    _sectors_v6_stop_anchor_at_tip(repo, tip, pinned_s6_stop)

    log8 = v8.registry(Path(v8_log_dir))
    with tempfile.TemporaryDirectory() as scratch:
        ext = Path(scratch) / Path(v8.WITNESS_PATH).name
        ext.write_bytes(content)
        with log8.locked():
            records = v8.V8._chain(log8)
            check = log8.verify_chain(external_anchors=ext)
    if not check["ok"]:
        raise PermissionError(f"off-host anchor log does not witness the v8 registry: {check['detail']}")
    covered = versions[-1]["covered_records"]
    if len(records) != V8_STOP_RECORDS or covered != V8_STOP_RECORDS:
        raise PermissionError("v8 STOP witness must end at exactly three records")
    v8._check_stop_record(records[-1])
    if records[-1].get("status") != v8.STOP_SUPERSEDED_STATUS:
        raise PermissionError(f"v8 terminal STOP status must be {v8.STOP_SUPERSEDED_STATUS}")
    head = v1._record_sha256(records[-1])
    if head != pinned_v8_stop:
        raise PermissionError("v8 local STOP head differs from the pinned terminal head")
    census = v1.vs1_witness_census(repo, tip)
    if (census.get("records") or {}).get(v8.REGISTRY_ID) != V8_STOP_RECORDS:
        raise PermissionError("v8 census does not cover exactly the STOP record")
    return {
        "records": V8_STOP_RECORDS,
        "head_sha256": head,
        "status": records[-1]["status"],
        "witness_path": v8.WITNESS_PATH,
        "witness_tip": tip,
        "census": census,
        "repo": repo,
    }


def check_census(
    census: Mapping[str, Any],
    *,
    v8_stop_head_sha256: str | None = None,
    sectors_v6_stop_head_sha256: str | None = None,
    witness_repo: Path | None = None,
    opening: bool = False,
) -> None:
    """Accept only the witnessed v6/v7/v8/sectors-v6 STOPs, two-record older registries, and optional 2-record sectors-v7."""
    _, pinned_v8_stop, pinned_s6_stop = _bound()
    if (
        v8_stop_head_sha256 != pinned_v8_stop
        or sectors_v6_stop_head_sha256 != pinned_s6_stop
        or witness_repo is None
        or not census.get("tip")
    ):
        raise PermissionError("v9 needs the pinned, verified v8 and sectors-v6 STOP heads at the witness tip")
    if census.get("unknown"):
        raise PermissionError(f"unknown VS1 witnesses: {census['unknown']}")
    files, counts = census.get("files") or {}, census.get("records") or {}
    expected = BASELINE_REGISTRIES | ({REGISTRY_ID} if opening else set())
    if opening and (SECTORS_V7 in files or SECTORS_V7 in counts):
        expected = expected | {SECTORS_V7}  # optional; a sectors-v7 cannot precede v9's registration
    if set(files) != expected or set(counts) != expected:
        raise PermissionError("v9 witness census has missing or extra registries")
    for key, path in files.items():
        if path != v1.canonical_witness_path(key):
            raise PermissionError(f"{key} is not at its canonical witness path")
    if any(counts[key] != 2 for key in FROZEN_REGISTRIES):
        raise PermissionError("an older Technology or sectors registry grew past two records")
    for key, records in TERMINAL_REGISTRIES.items():
        if counts[key] != records:
            raise PermissionError(f"{key} witness must cover exactly its terminal STOP")
    if opening and (not isinstance(counts[REGISTRY_ID], int) or counts[REGISTRY_ID] < 2):
        raise PermissionError("v9 witness does not cover its registration")
    if SECTORS_V7 in counts:
        if counts[SECTORS_V7] != SECTORS_V7_RECORDS:
            raise PermissionError("sectors-v7 witness must cover exactly its two registration records")
        _sectors_v7_anchor_at_tip(Path(witness_repo), census["tip"])
    repo = Path(witness_repo)
    tip = census["tip"]
    v7._v6_anchor_at_tip(repo, tip, v7.V6_STOP_HEAD_SHA256)
    v8._v7_anchor_at_tip(repo, tip, v8.V7_STOP_HEAD_SHA256)
    _v8_anchor_at_tip(repo, tip, pinned_v8_stop)
    _sectors_v6_stop_anchor_at_tip(repo, tip, pinned_s6_stop)


def contamination(censuses: list[Mapping[str, Any] | None]) -> dict:
    """The known v6/v7/v8/sectors-v6 STOPs are clean; any other non-v9 growth, late sectors-v7, or unknown is not."""
    found: dict[str, Any] = {}
    unknown: set[str] = set()
    absent_earlier = present_earlier = False  # censuses are chronological: v9 openings, then the current one
    for census in censuses:
        if not census:
            continue
        unknown.update(census.get("unknown") or ())
        counts = census.get("records") or {}
        for key in BASELINE_REGISTRIES:
            if counts.get(key) != TERMINAL_REGISTRIES.get(key, 2):
                found[key] = counts.get(key)
        if SECTORS_V7 in counts and counts[SECTORS_V7] != SECTORS_V7_RECORDS:
            found[SECTORS_V7] = counts[SECTORS_V7]
        if SECTORS_V7 in counts and absent_earlier:
            found["sectors-v7 (appeared after a v9 opening)"] = counts[SECTORS_V7]
        if SECTORS_V7 not in counts and present_earlier:
            found["sectors-v7 (disappeared after a v9 opening)"] = None
        absent_earlier = absent_earlier or SECTORS_V7 not in counts
        present_earlier = present_earlier or SECTORS_V7 in counts
        for key in set(counts) - ALLOWED_REGISTRIES:
            found[key] = counts[key]
    return {
        "contaminated": bool(found or unknown),
        "detail": {
            "other_registries_outside_baseline": found,
            "unknown_witness_files": sorted(unknown),
        },
    }


def registration_records(now: datetime, code_sha: str, prereg_sha256: str | None = None) -> list[dict]:
    """New v9 chain, cross-linked to the verified terminal v8 and sectors-v6 STOPs."""
    body, v8_stop, s6_stop = _bound()
    if prereg_sha256 not in (None, body):
        raise PermissionError("registration body differs from the pinned v9 body")
    if now.tzinfo is None or not _is_hex40(code_sha):
        raise ValueError("registration needs a timezone and reviewed code SHA")
    check_ready_to_register()
    common = {
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": body,
        "promotion_allowed": False,
    }
    header = {"kind": "header", "version": VERSION, **common}
    prereg = {
        "kind": "preregistration",
        "version": VERSION,
        **common,
        "parent_registry_id": v8.VERSION,
        "parent_prereg_sha256": v8.PREREG_BODY_SHA256,
        "parent_registry_head_sha256": v8.REGISTERED_RECORD_SHA256[1],
        "parent_terminal_head_sha256": v8_stop,
        "parent_terminal_records": V8_STOP_RECORDS,
        "parent_terminal_status": v8.STOP_SUPERSEDED_STATUS,
        "parent_witness_path": v8.WITNESS_PATH,
        "sectors_v6_terminal": {
            "registry_id": s6.REGISTRY_ID,
            "prereg_sha256": s6.PREREG_BODY_SHA256,
            "registration_head_sha256": s6.REGISTERED_RECORD_SHA256[1],
            "terminal_head_sha256": s6_stop,
            "terminal_records": SECTORS_V6_STOP_RECORDS,
            "terminal_status": SECTORS_V6_STOP_STATUS,
            "witness_path": s6.WITNESS_PATH,
        },
        "prior_registrations": [
            {
                "version": e.version,
                "prereg_sha256": e.prereg_sha256,
                "registry_head_sha256": e.registry_head_sha256,
            }
            for e in (
                v2.V1_EARLIER,
                v2.V2_EARLIER,
                v6.V3_EARLIER,
                v6.V4_EARLIER,
                v6.V5_EARLIER,
                v7.V6_EARLIER,
                v8.V7_EARLIER,
            )
        ],
        "calendar_manifest": {
            "path": XNYS_MANIFEST_PATH.as_posix(),
            "sha256": XNYS_MANIFEST_SHA256,
            "calendar_version": XNYS_CALENDAR_VERSION,
            "probe_sessions": PROBE_CALENDAR_SESSIONS,
            "probe_span": [PROBE_START, PROBE_END],
        },
        "bound_v8_probe": {
            "dir": PROBE_DIR_NAME,
            "probe_report_sha256": PROBE_REPORT_SHA256,
            "crosscheck_report_sha256": CROSSCHECK_REPORT_SHA256,
            "tiingo_meta_report_sha256": TIINGO_META_REPORT_SHA256,
            "snapshot_as_of_ts": PROBE_SNAPSHOT_AS_OF_TS,
            "code_sha": PROBE_CODE_SHA,
            "benchmark_admitted": False,
            "admitted_issuers": PROBE_ADMITTED_ISSUERS,
        },
        "design": json.loads(json.dumps(DESIGN)),
        "primary_trial": PRIMARY_TRIAL,
        "confirmatory": {
            "trial": PRIMARY_TRIAL,
            "direction": v1.PRIMARY_DIRECTION,
            "alpha_one_sided": CONFIRMATORY_ALPHA_ONE_SIDED,
            "exploratory": [t for t in v1.trial_names() if t != PRIMARY_TRIAL],
        },
        "discovery": {"start": DISCOVERY_START, "end": v1.SPLIT},
        "holdout": {"start": v1.SPLIT, "end": v1.END},
        "probe_window": [PROBE_START, PROBE_END],
        "power_gate": json.loads(json.dumps(STAGE0_SETTINGS)),
    }
    return [header, prereg]


# --- price manifest, price panel, raw labels, and reported-only magnitudes -------------------


@dataclass(frozen=True)
class PriceManifest:
    """v9 admitted-price contract: TIINGO adj_close, pinned XNYS calendar, XLK not required to admit."""

    source: str
    series_template: str
    basis: str
    benchmark: str
    admitted: tuple[str, ...]
    probe_report_sha256: str
    crosscheck_report_sha256: str = ""
    tiingo_meta_report_sha256: str = ""
    listed_from: tuple[tuple[str, str], ...] = ()
    benchmark_admitted: bool = False
    calendar_manifest: str = ""
    calendar_manifest_sha256: str = ""
    probe_snapshot_as_of_ts: str = ""
    selected_adj_close_sha256: tuple[tuple[str, str], ...] = ()

    def validate(self) -> None:
        if not self.source or self.source.strip().lower() in v1.REFUSED_PRICE_SOURCES:
            raise ValueError(f"price source {self.source!r} is refused (unverified basis)")
        if self.source != PRICE_SOURCE:
            raise ValueError(f"v9 admits only the {PRICE_SOURCE} source, not {self.source!r}")
        if self.series_template != SERIES_TEMPLATE:
            raise ValueError(f"v9 reads only {SERIES_TEMPLATE}")
        if self.basis != BASIS or self.benchmark != BENCHMARK:
            raise ValueError(f"v9 declares basis {BASIS!r} and benchmark {BENCHMARK}")
        if not isinstance(self.benchmark_admitted, bool):
            raise ValueError("benchmark_admitted must be a boolean")
        if self.benchmark_admitted:
            if self.benchmark not in self.admitted:
                raise ValueError("benchmark_admitted=True requires the benchmark in admitted")
        else:
            if self.benchmark in self.admitted:
                raise ValueError("benchmark_admitted=False forbids the benchmark in admitted")
        if not self.admitted or len(set(self.admitted)) != len(self.admitted):
            raise ValueError("admitted must be a non-empty list of distinct tickers")
        for field_name, value in (
            ("probe_report_sha256", self.probe_report_sha256),
            ("crosscheck_report_sha256", self.crosscheck_report_sha256),
            ("tiingo_meta_report_sha256", self.tiingo_meta_report_sha256),
            ("calendar_manifest_sha256", self.calendar_manifest_sha256),
        ):
            if not v1._is_hex64(value):
                raise ValueError(f"v9 manifest {field_name} must be a sha256 hex digest")
        if self.calendar_manifest != XNYS_MANIFEST_PATH.as_posix():
            raise ValueError(f"v9 calendar_manifest must be {XNYS_MANIFEST_PATH.as_posix()!r}")
        if self.calendar_manifest_sha256 != XNYS_MANIFEST_SHA256:
            raise ValueError("v9 calendar_manifest_sha256 differs from the pinned XNYS calendar manifest")
        if not self.probe_snapshot_as_of_ts:
            raise ValueError("v9 manifest requires probe_snapshot_as_of_ts")
        stamp(self.probe_snapshot_as_of_ts)

        issuers = {t for t in self.admitted if t != self.benchmark}
        listed = dict(self.listed_from)
        if set(listed) != issuers:
            raise ValueError("v9 manifest listed_from must cover exactly the admitted issuer tickers")
        for ticker, start in self.listed_from:
            date.fromisoformat(start)

        rows_sha = dict(self.selected_adj_close_sha256)
        if set(rows_sha) != issuers:
            raise ValueError("v9 manifest selected_adj_close_sha256 must cover exactly the admitted issuer tickers")
        for ticker, digest_hex in self.selected_adj_close_sha256:
            if not v1._is_hex64(digest_hex):
                raise ValueError(f"{ticker}: selected_adj_close_sha256 must be a sha256 hex digest")

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "series_template": self.series_template,
            "basis": self.basis,
            "benchmark": self.benchmark,
            "benchmark_admitted": self.benchmark_admitted,
            "admitted": list(self.admitted),
            "probe_report_sha256": self.probe_report_sha256,
            "crosscheck_report_sha256": self.crosscheck_report_sha256,
            "tiingo_meta_report_sha256": self.tiingo_meta_report_sha256,
            "listed_from": dict(self.listed_from),
            "calendar_manifest": self.calendar_manifest,
            "calendar_manifest_sha256": self.calendar_manifest_sha256,
            "probe_snapshot_as_of_ts": self.probe_snapshot_as_of_ts,
            "selected_adj_close_sha256": dict(self.selected_adj_close_sha256),
        }

    @classmethod
    def from_file(cls, path: Path) -> "PriceManifest":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        listed = raw.get("listed_from") or {}
        listed_items = listed.items() if isinstance(listed, dict) else listed
        rows_sha = raw.get("selected_adj_close_sha256") or {}
        rows_items = rows_sha.items() if isinstance(rows_sha, dict) else rows_sha
        manifest = cls(
            **{
                **raw,
                "admitted": tuple(sorted(raw["admitted"])),
                "listed_from": tuple(sorted((str(t), str(d)) for t, d in listed_items)),
                "selected_adj_close_sha256": tuple(sorted((str(t), str(h)) for t, h in rows_items)),
            }
        )
        manifest.validate()
        return manifest

    def digest(self) -> str:
        return digest({
            **asdict(self),
            "admitted": list(self.admitted),
            "listed_from": [list(x) for x in self.listed_from],
            "selected_adj_close_sha256": [list(x) for x in self.selected_adj_close_sha256],
        })


_PRICE_LOADER = object()


class PricePanel:
    """v9 closes read through ``store.observations.read_window`` on the pinned XNYS calendar."""

    def __init__(
        self,
        *,
        token: object,
        manifest: PriceManifest,
        start: date,
        as_of: date,
        as_of_ts: datetime,
        window: str,
        sessions: tuple[date, ...],
        benchmark_admitted: bool,
        data: dict[str, tuple[observations.Observation, ...]],
        selected_rows_sha256: Mapping[str, str],
    ) -> None:
        if token is not _PRICE_LOADER:
            raise TypeError("PricePanel is built only by load_price_panel")
        self.manifest = manifest
        self.start, self.as_of, self.as_of_ts, self.window = start, as_of, as_of_ts, window
        self.sessions = tuple(sessions)
        self.benchmark_admitted = bool(benchmark_admitted)
        self._data = dict(data)
        self._selected_rows_sha256 = dict(selected_rows_sha256)
        self.receipt = self._receipt()
        self.receipt_sha = digest(self.receipt)

    def _receipt(self) -> dict[str, Any]:
        return {
            "reader": "store.observations.read_window",
            "manifest": self.manifest.to_dict(),
            "calendar_manifest": XNYS_MANIFEST_PATH.as_posix(),
            "calendar_manifest_sha256": XNYS_MANIFEST_SHA256,
            "benchmark_admitted": self.benchmark_admitted,
            "window": self.window,
            "start": self.start.isoformat(),
            "as_of": self.as_of.isoformat(),
            "as_of_ts": self.as_of_ts.isoformat(),
            "sessions": {
                "n": len(self.sessions),
                "first": self.sessions[0].isoformat() if self.sessions else None,
                "last": self.sessions[-1].isoformat() if self.sessions else None,
            },
            "series": {
                ticker: {
                    "n": len(obs),
                    "first": obs[0].obs_date.isoformat() if obs else None,
                    "last": obs[-1].obs_date.isoformat() if obs else None,
                    "sha256": digest([[o.obs_date.isoformat(), o.value] for o in obs]),
                    "rows_sha256": self._selected_rows_sha256.get(ticker),
                }
                for ticker, obs in sorted(self._data.items())
            },
        }

    def verify(self) -> None:
        if digest(self._receipt()) != self.receipt_sha:
            raise ValueError("price panel changed after its read")

    def closes(self) -> pd.DataFrame:
        """Session-date x ticker closes; sessions come strictly from the pinned XNYS manifest."""
        index = pd.DatetimeIndex([pd.Timestamp(d) for d in self.sessions])
        frame = pd.DataFrame(index=index)
        for ticker, obs in sorted(self._data.items()):
            series = pd.Series(
                [o.value for o in obs],
                index=pd.DatetimeIndex([pd.Timestamp(o.obs_date) for o in obs]),
                dtype=float,
            )
            frame[ticker] = series.reindex(index)
        return frame


def raw_labels(
    closes: pd.DataFrame, tickers: list[str], horizon: int, window: str
) -> tuple[list[int], np.ndarray, np.ndarray]:
    """Horizon-spaced decisions and split-first raw issuer forward returns (no XLK subtraction).

    Every close outside ``[lo, hi)`` is blanked before labelling (purge by construction).
    Decisions are the window's sessions every ``horizon`` sessions from its first session; a
    decision whose label end is not a session inside the window is dropped.
    """
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    lo, hi = v1.window_bounds(window)
    dates = closes.index
    day = pd.DatetimeIndex(dates)
    day = day.tz_localize("UTC") if day.tz is None else day.tz_convert("UTC")
    inside = np.asarray((day >= lo.normalize()) & (day < hi.normalize()))
    raw = closes[tickers].to_numpy(dtype=float)
    visible = raw.copy()
    visible[~inside, :] = np.nan
    positions = [i for i in np.flatnonzero(inside)[::horizon] if i + horizon < len(dates) and inside[i + horizon]]
    labels, momentum = [], []
    for i in positions:
        start, end = visible[i], visible[i + horizon]
        with np.errstate(divide="ignore", invalid="ignore"):
            ret = end / start - 1.0
        ret[~np.isfinite(ret)] = np.nan
        labels.append(ret)
        if i >= v1.MOMENTUM_SESSIONS:
            past, now = raw[i - v1.MOMENTUM_SESSIONS], raw[i]
            with np.errstate(divide="ignore", invalid="ignore"):
                mom = now / past - 1.0
            mom[~np.isfinite(mom)] = np.nan
        else:
            mom = np.full(len(tickers), np.nan)
        momentum.append(mom)
    shape = (len(positions), len(tickers))
    return (
        positions,
        np.array(labels).reshape(shape),
        np.array(momentum).reshape(shape),
    )


@dataclass
class TrialPanel(v2.TrialPanel):
    """v2 trial panel plus optional admitted-window benchmark return (for magnitudes only)."""

    benchmark_admitted: bool = False
    benchmark_return: np.ndarray | None = None


def buyer_excess(panel: v1.TrialPanel, rows: np.ndarray) -> dict[str, Any]:
    """Reported magnitudes (never gating): XLK-relative fields read 'not available' unless XLK is admitted."""
    admitted = bool(getattr(panel, "benchmark_admitted", False)) and getattr(panel, "benchmark_return", None) is not None
    bench = np.asarray(panel.benchmark_return, dtype=float) if admitted else None
    buyers_raw, others_raw = [], []
    buyers_rel, others_rel = [], []
    dates = 0
    for i in rows:
        m = np.isfinite(panel.feature[i]) & np.isfinite(panel.label[i])
        b = m & (panel.feature[i] > 0)
        o = m & (panel.feature[i] == 0)
        if b.any() and o.any():
            b_mean = float(panel.label[i, b].mean())
            o_mean = float(panel.label[i, o].mean())
            buyers_raw.append(b_mean)
            others_raw.append(o_mean)
            if admitted and np.isfinite(bench[i]):
                buyers_rel.append(b_mean - float(bench[i]))
                others_rel.append(o_mean - float(bench[i]))
            dates += 1
    if not dates:
        out: dict[str, Any] = {
            "dates": 0,
            "benchmark_admitted": admitted,
            "buyer_minus_benchmark": None if admitted else BENCHMARK_MAGNITUDE_UNAVAILABLE,
            "nonbuyer_minus_benchmark": None if admitted else BENCHMARK_MAGNITUDE_UNAVAILABLE,
            "buyer_minus_nonbuyer": None,
            "buyer_issuer_dates": 0,
        }
        if panel.largest is not None:
            for name in ("large_line", "small_line"):
                out[f"{name}_buyer_minus_benchmark"] = None if admitted else BENCHMARK_MAGNITUDE_UNAVAILABLE
                out[f"{name}_buyer_issuer_dates"] = 0
        return out
    b_arr, o_arr = np.array(buyers_raw), np.array(others_raw)
    count = int(
        sum(
            int((np.isfinite(panel.feature[i]) & (panel.feature[i] > 0) & np.isfinite(panel.label[i])).sum())
            for i in rows
        )
    )
    out = {
        "dates": dates,
        "benchmark_admitted": admitted,
        "buyer_minus_benchmark": (
            (float(np.mean(buyers_rel)) if buyers_rel else None)
            if admitted
            else BENCHMARK_MAGNITUDE_UNAVAILABLE
        ),
        "nonbuyer_minus_benchmark": (
            (float(np.mean(others_rel)) if others_rel else None)
            if admitted
            else BENCHMARK_MAGNITUDE_UNAVAILABLE
        ),
        "buyer_minus_nonbuyer": float((b_arr - o_arr).mean()),
        "buyer_issuer_dates": count,
    }
    if panel.largest is not None:
        for name, large in (("large_line", True), ("small_line", False)):
            flat_rel: list[float] = []
            stratum_count = 0
            for i in rows:
                lines = panel.largest[i]
                size = lines >= v1.LARGE_LINE_USD if large else (lines > 0) & (lines < v1.LARGE_LINE_USD)
                m = np.isfinite(panel.label[i]) & (np.nan_to_num(panel.feature[i]) > 0) & size
                n_here = int(m.sum())
                stratum_count += n_here
                if admitted and n_here and np.isfinite(bench[i]):
                    flat_rel.extend((panel.label[i, m] - float(bench[i])).tolist())
            out[f"{name}_buyer_minus_benchmark"] = (
                (float(np.mean(flat_rel)) if flat_rel else None)
                if admitted
                else BENCHMARK_MAGNITUDE_UNAVAILABLE
            )
            out[f"{name}_buyer_issuer_dates"] = stratum_count
    return out


def measure_trial(
    panel: v1.TrialPanel,
    *,
    block: int | None = None,
    perms: int = v1.PERMS,
    seed: int = v1.SEED,
    min_n: int = v1.MIN_N,
    sensitivity: bool = True,
) -> dict[str, Any]:
    """v2/v8 trial measurement with v9 benchmark-aware reported magnitudes (never gating)."""
    out = v2.measure_trial(panel, block=block, perms=perms, seed=seed, min_n=min_n, sensitivity=sensitivity)
    if out["status"] == "tested" and sensitivity:
        ic, _ = v1.rank_ic_series(panel.feature, panel.label)
        rows = np.flatnonzero(np.isfinite(ic))
        out["magnitude"] = buyer_excess(panel, rows)
    return out


def build_trial_panels(
    events: v1.Form4Events,
    admission: v2.Admission,
    universe: pd.DataFrame,
    prices: PricePanel,
    window: str,
) -> dict[str, TrialPanel]:
    """Build v9 trial panels using pinned XNYS sessions, C1 close masking, and raw issuer forward returns."""
    prices.verify()
    if prices.window != window:
        raise ValueError("price panel window differs")
    closes = prices.closes()
    benchmark = prices.manifest.benchmark
    bench_admitted = bool(getattr(prices, "benchmark_admitted", False)) and benchmark in closes.columns
    universe = universe[universe["ticker"].isin(closes.columns) & (universe["ticker"] != benchmark)]
    tickers = list(universe["ticker"])
    ciks = list(universe["cik"].astype(int))
    groups = list(universe["sic_group"])
    listed = dict(getattr(prices.manifest, "listed_from", ()) or ())
    closes = closes.copy()
    for ticker, start in listed.items():
        if ticker in closes.columns and ticker != benchmark:
            closes.loc[closes.index < pd.Timestamp(start), ticker] = np.nan
    keep = v6.interval_close_mask(admission)(closes, universe)
    for ticker in keep.columns:
        if ticker in closes.columns and ticker != benchmark:
            closes.loc[~keep[ticker].reindex(closes.index, fill_value=False).to_numpy(), ticker] = np.nan

    lo, hi = v1.window_bounds(window)
    day = pd.DatetimeIndex(closes.index)
    day = day.tz_localize("UTC") if day.tz is None else day.tz_convert("UTC")
    inside = np.asarray((day >= lo.normalize()) & (day < hi.normalize()))
    bench_visible = None
    if bench_admitted:
        bench_visible = closes[benchmark].to_numpy(dtype=float).copy()
        bench_visible[~inside] = np.nan

    panels: dict[str, TrialPanel] = {}
    for h in v1.HORIZONS:
        positions, labels, momentum = raw_labels(closes, tickers, h, window)
        sessions = [closes.index[i].date() for i in positions]
        decided = v1.decision_instants(sessions)
        ends = v1.decision_instants([closes.index[i + h].date() for i in positions])
        trading = (
            np.isfinite(closes[tickers].to_numpy(dtype=float)[positions])
            if positions
            else np.zeros((0, len(tickers)), bool)
        )
        after_listing = np.ones_like(trading)
        for j, ticker in enumerate(tickers):
            if ticker in listed:
                after_listing[:, j] = [s >= date.fromisoformat(listed[ticker]) for s in sessions]
        bench_ret = None
        if bench_visible is not None:
            with np.errstate(divide="ignore", invalid="ignore"):
                arr = np.array([bench_visible[i + h] / bench_visible[i] - 1.0 for i in positions], dtype=float)
            arr[~np.isfinite(arr)] = np.nan
            bench_ret = arr
        for name in v1.FEATURES:
            feature = v2.feature_panel(events, admission, ciks, decided, name).to_numpy(dtype=float)
            feature = np.where(trading & after_listing, feature, np.nan)
            largest = v1.largest_value(events.purchases, ciks, decided, v1.FEATURES[name][0])
            trial = f"{name}|fwd{h}"
            panels[trial] = TrialPanel(
                trial=trial,
                window=window,
                horizon=h,
                decision_at=[d.isoformat() for d in decided],
                label_end=[d.isoformat() for d in ends],
                entities=tickers,
                feature=feature,
                label=labels,
                momentum=momentum,
                largest=np.where(np.isfinite(feature), largest.to_numpy(dtype=float), np.nan),
                groups=groups,
                benchmark_admitted=bench_admitted,
                benchmark_return=bench_ret,
            )
    return panels


# --- Stage-0 & confirmatory selection (unchanged from v8) ------------------------------------

planted_power_one_sided = v8.planted_power_one_sided
e0_power = v8.e0_power
confirmatory_selection = v8.confirmatory_selection
_gate_passed = v8._gate_passed


# --- V9Harness -------------------------------------------------------------------------------


class V9Harness(v2.Harness):
    def check_prereg(self, repo_root: Path = v2.REPO) -> str:
        return check_prereg(repo_root)

    def register(
        self,
        log_dir: Path,
        now: datetime,
        code_sha: str,
        *,
        v8_log_dir: Path | None = None,
        vault_repo: Path | None = None,
        remote_url: str | None = None,
        **kwargs,
    ) -> list[dict]:
        if v8_log_dir is None or vault_repo is None:
            raise PermissionError("v9 registration requires the exact witnessed v8 STOP and vault repo")
        stop = verify_v8_terminal(v8_log_dir, vault_repo, remote_url=remote_url)
        check_census(
            stop["census"],
            v8_stop_head_sha256=stop["head_sha256"],
            sectors_v6_stop_head_sha256=SECTORS_V6_STOP_HEAD_SHA256,
            witness_repo=stop["repo"],
        )
        return super().register(log_dir, now, code_sha, **kwargs)

    def check_offhost(self, vault_repo: Path, *, remote_url: str | None = None):
        witness = super().check_offhost(vault_repo, remote_url=remote_url)
        check_census(
            witness.census,
            v8_stop_head_sha256=V8_STOP_HEAD_SHA256,
            sectors_v6_stop_head_sha256=SECTORS_V6_STOP_HEAD_SHA256,
            witness_repo=witness.repo,
            opening=True,
        )
        return witness

    def require_supersession(self, witness) -> dict:
        v1.refuse_superseded(9, self.superseded_by, witness)
        if not isinstance(witness, v2.OffhostWitness) or witness.path != WITNESS_PATH:
            raise PermissionError("v9 requires its pinned off-host witness")
        check_census(
            witness.census,
            v8_stop_head_sha256=V8_STOP_HEAD_SHA256,
            sectors_v6_stop_head_sha256=SECTORS_V6_STOP_HEAD_SHA256,
            witness_repo=witness.repo,
            opening=True,
        )
        return {
            "earlier_witnessed_records": dict(witness.earlier),
            "witness_tip": witness.tip,
            "vs1_witness_census": witness.census,
        }

    def run_contamination(self, key) -> dict:
        log = self.registry(key.log_dir)
        with log.locked():
            records = self._chain(log)
        censuses = [
            r.get("vs1_witness_census") or (r.get("supersession") or {}).get("vs1_witness_census")
            for r in records
        ]
        return contamination([*censuses, key.census])

    def _refuse_if_stopped(self, log_dir: Path) -> None:
        log = self.registry(log_dir)
        with log.locked():
            records = self._chain(log)
        if v1._kind(records, "status"):
            raise PermissionError("v9 carries a terminal STOP status record: no freeze or opening")

    def freeze_inputs(self, log_dir: Path, now: datetime, inputs: Mapping[str, Any]) -> dict:
        if inputs.get("accept_underpowered") is not False:
            raise PermissionError("v9 must STOP below either gated 0.50 Stage-0 model")
        raw_as_of = inputs.get("as_of_ts")
        try:
            as_of_val = stamp(raw_as_of) if raw_as_of else None
        except ValueError:
            as_of_val = None
        if as_of_val != stamp(PROBE_SNAPSHOT_AS_OF_TS):
            raise PermissionError(
                f"v9 freeze as_of_ts must equal the bound probe snapshot {PROBE_SNAPSHOT_AS_OF_TS}"
            )
        self._refuse_if_stopped(log_dir)
        v1.refuse_superseded(9, self.superseded_by)
        return super().freeze_inputs(log_dir, now, inputs)

    def open_discovery(self, log_dir: Path, now: datetime, observed: Mapping[str, Any], witness) -> dict:
        self._refuse_if_stopped(log_dir)
        return super().open_discovery(log_dir, now, observed, witness)

    def _holdout_probe(self, records: list[dict], holdout_probe: Mapping[str, Any] | None) -> dict | None:
        out = super()._holdout_probe(records, holdout_probe)
        if out is None:
            return None
        report = (holdout_probe or {}).get("report") or {}
        out["xlk_admitted_for_magnitudes"] = bool(report.get("benchmark_admitted") is True)
        out["abstains"] = sorted(report.get("abstains") or [])
        return out

    def open_holdout(self, frozen: dict, *, log_dir: Path, **kwargs) -> dict:
        """A sectors-v7 witness absent at discovery_opened may not appear before the holdout opens."""
        self._refuse_if_stopped(log_dir)
        witness = kwargs.get("witness")
        log = self.registry(log_dir)
        with log.locked():
            records = self._chain(log)
        opened = [r for r in records if r.get("kind") == "discovery_opened"]
        at_discovery = ((opened[-1].get("supersession") or {}).get("vs1_witness_census") or {}) if opened else {}
        now_present = SECTORS_V7 in ((getattr(witness, "census", None) or {}).get("records") or {})
        if now_present and SECTORS_V7 not in (at_discovery.get("records") or {}):
            raise PermissionError("sectors-v7 first appeared after v9 discovery_opened: invalid, holdout refused")
        return v2.Harness.open_holdout(self, frozen, log_dir=log_dir, **kwargs)

    # -- prices (pinned XNYS calendar, no XLK read when unadmitted, discovery rows_sha256 check) --
    def load_price_panel(
        self,
        conn,
        manifest: PriceManifest,
        tickers: Iterable[str],
        *,
        start: date,
        as_of: date,
        window: str,
        key: v2.DiscoveryKey | v2.HoldoutKey,
        repo_root: Path = v2.REPO,
    ) -> PricePanel:
        """Bounded, admitted, source-filtered read on the pinned XNYS calendar behind a v9 key."""
        manifest.validate()
        if window == "discovery":
            if not isinstance(key, v2.DiscoveryKey) or key.version != self.version:
                raise PermissionError(f"discovery prices need a {self.version} DiscoveryKey from resume_discovery")
            if as_of >= stamp(v1.SPLIT).date():
                raise PermissionError("discovery reads stop before the split date")
        elif window == "holdout":
            if not isinstance(key, v2.HoldoutKey) or key.version != self.version:
                raise PermissionError(f"holdout prices need a {self.version} HoldoutKey from resume_holdout")
            if as_of >= stamp(v1.END).date():
                raise PermissionError("holdout reads stop before the end of the frozen window")
        else:
            raise ValueError("window must be discovery or holdout")
        if manifest.digest() != key.inputs.get("price_manifest_sha256"):
            raise PermissionError("price manifest differs from the one in inputs_frozen")
        if manifest.probe_report_sha256 != key.inputs.get("probe_report_sha256"):
            raise PermissionError("price manifest's probe report differs from the one in inputs_frozen")

        all_sessions = load_xnys_calendar(repo_root)
        sessions = tuple(d for d in all_sessions if start <= d <= as_of)
        if not sessions:
            raise ValueError("no pinned XNYS sessions in the read window")

        include_benchmark = bool(manifest.benchmark_admitted)
        if window == "holdout" and include_benchmark:
            log = self.registry(key.log_dir)
            with log.locked():
                records = self._chain(log)
            h_opened = v1._kind(records, "holdout_opened")
            hp = (h_opened[-1].get("holdout_probe") or {}) if h_opened else {}
            if hp.get("xlk_admitted_for_magnitudes") is False:
                include_benchmark = False

        issuer_tickers = sorted(t for t in set(tickers) if t != manifest.benchmark)
        wanted = sorted(set(issuer_tickers) | ({manifest.benchmark} if include_benchmark else set()))
        refused = [t for t in wanted if t not in manifest.admitted]
        if refused:
            raise PermissionError(f"tickers not in the admitted-price manifest: {refused[:10]}")

        data = {
            ticker: tuple(
                observations.read_window(
                    conn,
                    manifest.series_template.format(ticker=ticker),
                    source=manifest.source,
                    start=start,
                    as_of=as_of,
                    as_of_ts=key.as_of_ts,
                )
            )
            for ticker in wanted
        }
        if include_benchmark and not data.get(manifest.benchmark):
            raise ValueError("no benchmark closes in the read window")

        rows_sha = {
            ticker: gd4.rows_sha256(
                gd4.Row(o.obs_date, float(o.value), o.pull_timestamp) for o in obs
            )
            for ticker, obs in data.items()
        }
        if window == "discovery":
            if stamp(key.as_of_ts.isoformat()) != stamp(manifest.probe_snapshot_as_of_ts) \
                    or stamp(key.as_of_ts.isoformat()) != stamp(PROBE_SNAPSHOT_AS_OF_TS):
                raise PermissionError("discovery read as_of_ts differs from the bound probe snapshot")
            expected_rows = dict(manifest.selected_adj_close_sha256)
            for ticker in issuer_tickers:
                expected = expected_rows.get(ticker)
                actual = rows_sha.get(ticker)
                if not expected or actual != expected:
                    raise PermissionError(
                        f"discovery price read for {ticker} failed selected_adj_close_sha256 "
                        f"reproduction (rows_sha256 mismatch: expected {expected}, got {actual})"
                    )

        panel = PricePanel(
            token=_PRICE_LOADER,
            manifest=manifest,
            start=start,
            as_of=as_of,
            as_of_ts=key.as_of_ts,
            window=window,
            sessions=sessions,
            benchmark_admitted=include_benchmark,
            data=data,
            selected_rows_sha256=rows_sha,
        )
        self.record_prices_read(key, panel.receipt_sha)
        return panel

    # -- Stage-0 (exact v8 settings, keeping v1.proxy_sessions for n_sessions) --
    def stage0_power(self, features: Mapping[str, np.ndarray]) -> dict:
        """v1 four-trial table (continuity only) plus the v9 two-model confirmatory gate."""
        out = v1.stage0_power(features)
        lo, hi = v1.window_bounds("discovery")
        n_sessions = len(v1.proxy_sessions(lo.date(), hi.date()))
        primary = np.asarray(features[PRIMARY_TRIAL], dtype=float)
        gate = {"gaussian_v1": planted_power_one_sided(primary, v1.POWER_GATE_IC)}
        for scenario in (E0_GATED_SCENARIO, *E0_REPORTED_SCENARIOS):
            gate[f"e0:{scenario}"] = e0_power(features, n_sessions, scenario)
        passed = _gate_passed(gate)
        return {
            **out,
            "version": self.version,
            "primary_trial": self.primary,
            "gate": (
                f"one-sided confirmatory {self.primary} at {CONFIRMATORY_ALPHA_ONE_SIDED}: "
                f"power(IC {v1.POWER_GATE_IC}) >= {v1.POWER_GATE} under gaussian_v1 AND "
                f"e0:{E0_GATED_SCENARIO}"
            ),
            "v9_stage0": {
                "settings": json.loads(json.dumps(STAGE0_SETTINGS)),
                "models": gate,
                "discovery_start": v1.discovery_start(),
                "n_sessions": n_sessions,
                "design_target_met": bool(gate[f"e0:{E0_GATED_SCENARIO}"]["power"] >= DESIGN_TARGET_POWER),
            },
            "gate_passed": passed,
        }

    def verify_power(self, power: Mapping[str, Any]) -> None:
        if power.get("version") != self.version or power.get("primary_trial") != self.primary:
            raise ValueError("power file was not computed by the vs1-v9 harness")
        if power.get("settings") != v1.power_settings():
            raise ValueError("power file was not computed at the pre-registered v1 table settings")
        table = power.get("table") or {}
        if set(table) != set(v1.trial_names()):
            raise ValueError("power file must cover exactly the declared trials")
        for trial, rows in table.items():
            if [r.get("target_ic") for r in rows] != list(v1.POWER_TARGET_ICS) or any(
                r.get("sims") not in (v1.POWER_SIMS, 0) for r in rows
            ):
                raise ValueError(f"{trial}: continuity table rows are not at the pre-registered settings")
        stage0 = power.get("v9_stage0") or {}
        if stage0.get("settings") != json.loads(json.dumps(STAGE0_SETTINGS)):
            raise ValueError("power file was not computed at the pre-registered v9 Stage-0 settings")
        start = DISCOVERY_START[:10]
        sessions = len(
            v1.proxy_sessions(
                datetime.fromisoformat(start).date(),
                datetime.fromisoformat(v1.SPLIT[:10]).date(),
            )
        )
        if stage0.get("discovery_start") != DISCOVERY_START or stage0.get("n_sessions") != sessions:
            raise ValueError("power file was not computed on the v9 discovery window")
        models = stage0.get("models") or {}
        expected = {"gaussian_v1", f"e0:{E0_GATED_SCENARIO}", *(f"e0:{s}" for s in E0_REPORTED_SCENARIOS)}
        if set(models) != expected:
            raise ValueError("power file must carry exactly the v9 Stage-0 models")
        g = models["gaussian_v1"]
        degenerate = g.get("sims") == 0 and g.get("power") == 0.0
        if (
            g.get("model") != "gaussian_v1"
            or (g.get("alpha_one_sided"), g.get("target_ic")) != (CONFIRMATORY_ALPHA_ONE_SIDED, v1.POWER_GATE_IC)
            or not degenerate
            and (g.get("sims"), g.get("perms"), g.get("seed")) != (GAUSSIAN_SIMS, v1.POWER_PERMS, v1.SEED)
        ):
            raise ValueError("gaussian_v1 Stage-0 row is not at the pre-registered settings")
        seeds = {"base": 20260930, "pilot": 20260931}
        for name, row in models.items():
            if name.startswith("e0:") and (
                row.get("model") != name
                or (
                    row.get("sims"),
                    row.get("perms"),
                    row.get("alpha_one_sided"),
                    row.get("target_ic"),
                    row.get("e0_manifest_sha256"),
                    row.get("e0_seeds"),
                )
                != (
                    E0_SIMS,
                    v1.POWER_PERMS,
                    CONFIRMATORY_ALPHA_ONE_SIDED,
                    v1.POWER_GATE_IC,
                    E0_MANIFEST_SHA256,
                    seeds,
                )
            ):
                raise ValueError(f"{name} Stage-0 row is not at the pre-registered settings")
        if power.get("gate_passed") is not _gate_passed(models):
            raise ValueError("power file's gate_passed disagrees with its gated models")

    # -- discovery selection & holdout magnitudes --
    def discover_panel(
        self,
        spec: v1.RunSpec,
        panels: Mapping[str, v1.TrialPanel],
        *,
        inputs: dict,
        repo_root: Path = v2.REPO,
        sensitivity: bool = True,
    ) -> dict:
        frozen = super().discover_panel(spec, panels, inputs=inputs, repo_root=repo_root, sensitivity=sensitivity)
        payload = frozen["payload"]
        if sensitivity:
            for row in payload["ledger"]:
                if row["status"] == "tested":
                    panel = panels[row["trial"]]
                    ic, _ = v1.rank_ic_series(panel.feature, panel.label)
                    row["magnitude"] = buyer_excess(panel, np.flatnonzero(np.isfinite(ic)))
        confirmatory_selection(payload["ledger"], self.primary)
        payload["selection"] = (
            f"one confirmatory trial {self.primary}: one-sided (positive) block sign-flip "
            f"p <= {CONFIRMATORY_ALPHA_ONE_SIDED} with positive mean IC; the other trials are "
            "exploratory (reported with Holm/BH-adjusted p, never selected)"
        )
        payload["calibration"] = self.calibration(payload["ledger"], spec.alpha)
        return {"payload": payload, "sha256": digest(payload)}

    def evaluate_panel_holdout(
        self,
        frozen: dict,
        panels: Mapping[str, v1.TrialPanel],
        key: v2.HoldoutKey,
        *,
        power: dict | None = None,
    ) -> dict:
        result = super().evaluate_panel_holdout(frozen, panels, key, power=power)
        for row in result["holdout_checks"]:
            if row["status"] == "tested":
                panel = panels[row["trial"]]
                ic, _ = v1.rank_ic_series(panel.feature, panel.label)
                row["magnitude"] = buyer_excess(panel, np.flatnonzero(np.isfinite(ic)))
        return result


V8_EARLIER = v2.EarlierVersion(
    version=v8.VERSION,
    number=8,
    prereg_sha256=v8.PREREG_BODY_SHA256,
    registry_head_sha256=v8.REGISTERED_RECORD_SHA256[1],
    witness_path=v8.WITNESS_PATH,
    anchor_line=v8.REGISTERED_ANCHOR_LINE,
)

V9 = V9Harness(
    v2.Pins(
        version=VERSION,
        number=9,
        prereg_path=PREREG_PATH,
        prereg_body_sha256=PREREG_BODY_SHA256,
        primary_trial=PRIMARY_TRIAL,
        registry_log=REGISTRY_LOG,
        registry_anchors=REGISTRY_ANCHORS,
        registry_lock=REGISTRY_LOCK,
        witness_path=WITNESS_PATH,
        witness_ref=WITNESS_REF,
        registration_records=registration_records,
        registered_at=None,
        registered_code_sha=None,
        registered_record_sha256=REGISTERED_RECORD_SHA256,
        registered_anchor_line=REGISTERED_ANCHOR_LINE,
        earlier=(
            v2.V1_EARLIER,
            v2.V2_EARLIER,
            v6.V3_EARLIER,
            v6.V4_EARLIER,
            v6.V5_EARLIER,
            v7.V6_EARLIER,
            v8.V7_EARLIER,
            V8_EARLIER,
        ),
        superseded_by=SUPERSEDED_BY,
        holdout_probe_required=True,
    ),
    module=sys.modules[__name__],
)

stage0_power = V9.stage0_power
verify_power = V9.verify_power
discover_panel = V9.discover_panel
check_holdout_request = V9.check_holdout_request
evaluate_panel_holdout = V9.evaluate_panel_holdout
load_price_panel = V9.load_price_panel
registry = V9.registry
register = V9.register
check_offhost = V9.check_offhost
require_supersession = V9.require_supersession
require_witness = V9.require_witness
export_anchors = V9.export_anchors
record_prices_read = V9.record_prices_read
freeze_inputs = V9.freeze_inputs
latest_frozen_inputs = V9.latest_frozen_inputs
open_discovery = V9.open_discovery
resume_discovery = V9.resume_discovery
seal_discovery = V9.seal_discovery
open_holdout = V9.open_holdout
resume_holdout = V9.resume_holdout
seal_holdout = V9.seal_holdout


# --- terminal STOP below either gated Stage-0 model (v9 body §3 step 8) ----------------------

STOP_STATUS = "STOP_FOR_OWNER_UNDERPOWERED_UNOPENED"
STOP_RECORDS = 3
POWER_INPUT_KEYS = v8.POWER_INPUT_KEYS


def _stop_required() -> dict[str, Any]:
    return {
        "kind": "status",
        "version": VERSION,
        "status": STOP_STATUS,
        "gate_passed": False,
        "inputs_frozen": False,
        "discovery_opened": False,
        "holdout_opened": False,
        "prereg_sha256": PREREG_BODY_SHA256,
        "primary_trial": PRIMARY_TRIAL,
        "power_gate": json.loads(json.dumps(STAGE0_SETTINGS)),
        "promotion_allowed": False,
    }


def _check_power_inputs(inputs: Any) -> None:
    if (
        not isinstance(inputs, Mapping)
        or set(inputs) != POWER_INPUT_KEYS
        or inputs.get("prereg_sha256") != PREREG_BODY_SHA256
        or not all(v1._is_hex64(inputs[k]) for k in POWER_INPUT_KEYS)
    ):
        raise PermissionError("power receipt is not a v9 post-admission Stage-0 on a price manifest")


def _gated_powers(power: Mapping[str, Any]) -> dict[str, float]:
    models = (power.get("v9_stage0") or {}).get("models") or {}
    return {name: float(models[name]["power"]) for name in ("gaussian_v1", f"e0:{E0_GATED_SCENARIO}")}


def _check_stop_record(record: Mapping[str, Any]) -> None:
    required = _stop_required()
    allowed = frozenset(required) | {
        "run_at",
        "stage0_power_ic_0_01",
        "power_receipt_sha256",
        "power_inputs",
        "decision_ref",
        "prev_sha256",
    }
    if any(record.get(key) != value for key, value in required.items()):
        raise PermissionError("v9 terminal record is not the exact STOP status")
    if set(record) != allowed:
        raise PermissionError("v9 terminal STOP record has missing or extra fields")
    powers = record.get("stage0_power_ic_0_01")
    if (
        not isinstance(powers, Mapping)
        or set(powers) != {"gaussian_v1", f"e0:{E0_GATED_SCENARIO}"}
        or not all(isinstance(p, float) and 0.0 <= p <= 1.0 for p in powers.values())
        or min(powers.values()) >= v1.POWER_GATE
    ):
        raise PermissionError("v9 STOP needs a gated Stage-0 model below the 0.50 gate")
    if not v1._is_hex64(record.get("power_receipt_sha256")) or not str(record.get("decision_ref") or "").strip():
        raise PermissionError("v9 STOP needs the sealed power receipt hash and a decision reference")
    _check_power_inputs(record.get("power_inputs"))


def append_stop_status(
    log_dir: Path,
    now: datetime,
    *,
    power_path: Path,
    decision_ref: str,
    expected_prev_sha256: str,
    witness,
    dry_run: bool = False,
) -> dict:
    """Append v9's one STOP after a verified, sealed, underpowered Stage-0 (exact 2-record chain)."""
    if REGISTERED_RECORD_SHA256 is None:
        raise PermissionError("v9 is not registered yet (no pinned registration in code)")
    if (
        now.tzinfo is None
        or now.utcoffset() != timedelta(0)
        or not str(decision_ref).strip()
        or expected_prev_sha256 != REGISTERED_RECORD_SHA256[1]
    ):
        raise ValueError("STOP needs a UTC time, a decision reference and the exact v9 registration head")
    body = Path(power_path).read_bytes()
    power = json.loads(body)
    verify_power(power)
    _check_power_inputs(power.get("inputs"))
    if power.get("gate_passed") is not False:
        raise PermissionError("v9 passed its Stage-0 gate: a STOP is refused (freeze instead)")
    V9.require_witness(log_dir, witness, 2)
    if (witness.census.get("records") or {}).get(REGISTRY_ID) != 2:
        raise PermissionError("v9's canonical witness is not at the two-record baseline")
    log = V9.registry(log_dir)
    with log.locked():
        records = V9._chain(log)
        if len(records) != 2:
            raise PermissionError("v9 registry differs from its two-record baseline; reconcile before STOP")
        if v1._record_sha256(records[-1]) != expected_prev_sha256:
            raise PermissionError("v9 prior head differs from the expected head")
        candidate = {
            **_stop_required(),
            "run_at": now.isoformat(),
            "stage0_power_ic_0_01": _gated_powers(power),
            "power_receipt_sha256": hashlib.sha256(body).hexdigest(),
            "power_inputs": dict(power["inputs"]),
            "decision_ref": str(decision_ref).strip(),
            "prev_sha256": v1._record_sha256(records[-1]),
        }
        _check_stop_record(candidate)
        if dry_run:
            return {
                "dry_run": True,
                "would_append": candidate,
                "would_be_head_sha256": v1._record_sha256(candidate),
            }
        return log.append_locked([candidate])[0]


def verify_terminal_stop(log_dir: Path, witness) -> dict:
    """Return the only acceptable v9 terminal head after exact off-host witnessing."""
    log = V9.registry(log_dir)
    proof = V9.require_witness(log_dir, witness, STOP_RECORDS)
    records = V9._chain(log)
    if len(records) != STOP_RECORDS or proof["witnessed_records"] != STOP_RECORDS:
        raise PermissionError("v9 STOP witness must end at exactly three records")
    _check_stop_record(records[-1])
    if (witness.census.get("records") or {}).get(REGISTRY_ID) != STOP_RECORDS:
        raise PermissionError("v9 census does not cover exactly the STOP record")
    return {
        "records": STOP_RECORDS,
        "head_sha256": v1._record_sha256(records[-1]),
        "status": records[-1]["status"],
        "witness_path": WITNESS_PATH,
        "witness_tip": witness.tip,
    }


def run_spec(run_id: str | None = None):
    return V9.run_spec(run_id)


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)
