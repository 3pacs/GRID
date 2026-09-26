"""Prospective forward log for frozen research-loop candidates (S10).

Rules: ``docs/paper_log/hypothesis-forward-v1-preregistration.md``. Its sha256
(LF bytes) is pinned below and written into the log's header record.

What it does
------------
* ``admit_scan`` checks one ``scripts/run_real_panel_scan.py`` output directory
  against the pre-registered eligibility rules and, all-or-nothing, appends one
  ``admission`` record per candidate with a frozen per-candidate plan (target,
  horizon, spacing, decision rule, minimum N, alpha, failure and stop rule).
* ``run_forward`` (the daily job) appends ``prediction`` records (the feature
  value GRID held at each decision instant, logged before its outcome can be
  known), ``outcome`` records (the target label, read after its publication
  time) and, once per candidate, a ``verdict`` record at the single
  pre-registered look.
* ``status_report`` counts activity only; it never computes a correlation or
  p-value before a candidate's verdict exists.

Storage: append-only JSONL. Every record carries ``prev_sha256``, the sha256
of the previous record's exact canonical JSON line (``sort_keys``, compact
separators, UTF-8, no trailing newline); the file separates lines with a
single ``\\n`` written in binary mode. The first record is a ``header`` with
``prereg_sha256``. ``verify_chain`` detects an edited, reordered, inserted or
deleted line. It cannot, on its own, detect truncation of trailing lines or a
recompute of the whole file: after every append ``(records, head_sha256)`` is
also appended to a chained anchor file, and the log is tamper-evident only
relative to a copy of that file held off-host. ``STATUS.md`` is regenerated
from the log and is not an anchor.

Admission is keyed on the scientific pair (target, label, horizon, feature
series and transform), not on the scan: a pair is forward-tested at most once.

Boundaries: DB reads go only through
``research_real_panel.load_latest_vintage_panel`` (``store.observations.read_window``:
SUCCESS rows, latest vintage per date, bounded by ``as_of``/``as_of_ts``). This
module builds no SQL, never names the hypothesis registry tables, writes no
weights and promotes nothing: every state keeps ``promotion_allowed: false``.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import re
import subprocess
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from scipy.stats import rankdata

from analysis.offline_research_proof import (
    FORWARD_PENDING,
    LATEST_VINTAGE_ORIGIN,
    MIN_BLOCKS,
    block_permutations,
    digest,
    stamp,
)
from analysis.research_real_panel import (
    PUBLICATIONS,
    REVISED_PREFIXES,
    REVISED_SERIES,
    Publication,
    SeriesSpec,
    TargetSpec,
    load_latest_vintage_panel,
    proxy_group,
    publication_times,
)

VERSION = "v1"
PREREG_PATH = Path("docs/paper_log/hypothesis-forward-v1-preregistration.md")
# sha256 of the pre-registration's LF bytes. Recompute and re-pin only before
# the first record is ever written; afterwards any change is a v2.
PREREG_SHA256 = "b0bfbd11e1047dde0c2d692f0ad0615b9fd52a6fcc36b010a42fbe387b33e814"

LOG_FILENAME = "hypothesis_forward_v1.jsonl"
ANCHOR_FILENAME = "hypothesis_forward_v1.anchors.jsonl"
STATUS_FILENAME = "STATUS.md"
LOCK_FILENAME = ".hypothesis_forward_v1.lock"
LOCK_TIMEOUT_S = 30.0
LOCK_STALE_AFTER_S = 900.0

# The scan's code must include PR #661 (S09b): this is its head commit
# (merged into main at a8b3a400bc5c74aeb4468ccba5b9bb5801ac9c0b).
MIN_SCAN_CODE_SHA = "4506ce4828f5a43a29d46f58569d82e42628b78d"
# The first real-panel scan (ef0d564b): 5 SELF_LAG + 3 RESCAN_REQUIRED.
DENIED_DISCOVERY_MANIFESTS = frozenset(
    {"b7515b2b4def28c21630d330a19c042b269eb1651ab457bd4dff6b41d6c63fbc"}
)
# Files whose sha256 the scan records in summary.json (scripts/run_real_panel_scan.py).
SCAN_FILES = (
    "analysis/offline_research_proof.py",
    "analysis/research_real_panel.py",
    "store/observations.py",
    "scripts/run_real_panel_scan.py",
)
RELABELLED_FILE = "frozen-candidates.relabelled.json"
REFUSED_STATES = frozenset({"SELF_LAG_NEVER_A_CANDIDATE", "RESCAN_REQUIRED"})

PERMS = 9999
SEED = 20260926
FAMILY_ALPHA = 0.05
MAX_DECISIONS_FACTOR = 2
GRACE = timedelta(days=5)
WARMUP = timedelta(days=200)  # >= 60 sessions for z60 plus carry-forward

SUPPORTED = "FORWARD_SUPPORTED_REVIEW_REQUIRED"
FAILED = "FORWARD_FAILED"
INCONCLUSIVE = "FORWARD_INCONCLUSIVE_STOPPED"
EXCL_FEATURE = "feature_abstained"
EXCL_LATE = "late_prediction"
EXCL_TARGET = "target_missing"
SESSION_HOLIDAYS = (
    USFederalHolidayCalendar()
    .holidays("1990-01-01", "2040-12-31")
    .to_numpy()
    .astype("datetime64[D]")
)
HEX40 = re.compile(r"[0-9a-f]{40}")
HEX64 = re.compile(r"[0-9a-f]{64}")


class Refused(ValueError):
    """A scan or candidate that the pre-registration does not admit."""


# --- hashing and chain -----------------------------------------------------------


def lf_sha256(data: bytes) -> str:
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def prereg_file_sha256(repo_root: Path) -> str:
    return lf_sha256((Path(repo_root) / PREREG_PATH).read_bytes())


def canonical(record: dict) -> bytes:
    return json.dumps(
        record, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _lines(path: Path) -> Iterator[bytes]:
    if not path.exists():
        return
    with open(path, "rb") as stream:
        for raw in stream:
            line = raw.rstrip(b"\n")
            if line:
                yield line


class ForwardLog:
    """One append-only, hash-chained JSONL file plus its lock, in ``log_dir``."""

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = Path(log_dir)
        self.path = self.log_dir / LOG_FILENAME
        self.anchor_path = self.log_dir / ANCHOR_FILENAME
        self.lock_path = self.log_dir / LOCK_FILENAME

    def read_all(self) -> list[dict]:
        return [json.loads(line) for line in _lines(self.path)]

    def verify_chain(self, external_anchors: Path | None = None) -> dict:
        """Walk the chain, then check it against the anchor file(s).

        The chain alone detects an edited, reordered, inserted or deleted line
        but not truncation of trailing lines or a full recompute. Anchors
        (``records``, ``head_sha256`` after every append) detect those for every
        anchored prefix -- but only as far as the anchor file itself is out of
        the tamperer's reach, i.e. a copy held off-host (``external_anchors``).
        """
        previous = None
        lines = list(_lines(self.path))
        heads = []
        for i, line in enumerate(lines):
            try:
                record = json.loads(line)
            except ValueError:
                return _broken(len(lines), i, "line is not JSON")
            if line != canonical(record):
                return _broken(len(lines), i, "line is not canonical JSON")
            if record.get("prev_sha256") != previous:
                return _broken(len(lines), i, "prev_sha256 does not match the previous line")
            if i == 0 and (
                record.get("kind") != "header"
                or record.get("prereg_sha256") != PREREG_SHA256
            ):
                return _broken(len(lines), 0, "first record is not the pinned header")
            if i > 0 and record.get("kind") == "header":
                return _broken(len(lines), i, "second header")
            previous = hashlib.sha256(line).hexdigest()
            heads.append(previous)
        if lines and not self.anchor_path.exists():
            return _broken(len(lines), len(lines), "anchor file missing")
        anchored = 0
        for source in (self.anchor_path, external_anchors):
            if source is None:
                continue
            problem, count = _check_anchors(Path(source), heads)
            if problem:
                return _broken(len(lines), count, f"{Path(source).name}: {problem}")
            if source == self.anchor_path:
                anchored = count
        return {
            "ok": True,
            "records": len(lines),
            "head_sha256": previous,
            "anchored_records": anchored,
            "detail": None,
        }

    def append(self, records: list[dict]) -> list[dict]:
        """Append records under the lock, chaining each to the previous line."""
        with self.locked():
            return self.append_locked(records)

    def append_locked(self, records: list[dict]) -> list[dict]:
        """Append while the caller holds :meth:`locked`; refuses a broken chain."""
        if not records:
            return []
        check = self.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"forward log chain is broken: {check['detail']}")
        previous = check["head_sha256"]
        out = []
        with open(self.path, "ab") as stream:
            for record in records:
                if record.get("kind") == "verdict":
                    # the log a consumer must verify this verdict against
                    record = {**record, "log_head_sha256": previous}
                line = canonical({**record, "prev_sha256": previous})
                stream.write(line + b"\n")
                previous = hashlib.sha256(line).hexdigest()
                out.append(json.loads(line))
            stream.flush()
            os.fsync(stream.fileno())
        anchors = list(_lines(self.anchor_path))
        anchor = {
            "records": check["records"] + len(records),
            "head_sha256": previous,
            "run_at": out[-1].get("run_at"),
            "prev_anchor_sha256": hashlib.sha256(anchors[-1]).hexdigest() if anchors else None,
        }
        with open(self.anchor_path, "ab") as stream:
            stream.write(canonical(anchor) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        return out

    @contextlib.contextmanager
    def locked(self):
        """Exclusive O_EXCL lock file; a lock older than 15 minutes is stale."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + LOCK_TIMEOUT_S
        while True:
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"pid={os.getpid()}".encode())
                os.close(fd)
                break
            except FileExistsError:
                try:
                    age = time.time() - self.lock_path.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age > LOCK_STALE_AFTER_S:
                    self.lock_path.unlink(missing_ok=True)
                    continue
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"could not acquire {self.lock_path}") from None
                time.sleep(0.2)
        try:
            yield
        finally:
            self.lock_path.unlink(missing_ok=True)


def _broken(n: int, index: int, detail: str) -> dict:
    return {
        "ok": False,
        "records": n,
        "head_sha256": None,
        "anchored_records": 0,
        "detail": f"record {index}: {detail}",
    }


def _check_anchors(path: Path, heads: list[str]) -> tuple[str | None, int]:
    """Every anchor must name a prefix the log still has, with the same head hash.

    Returns ``(problem, records)``: the first problem found (and the anchor's
    record count), or ``None`` and the last anchored count. Records written
    after the last anchor (a crash between the two appends) are allowed.
    """
    previous, count = None, 0
    for i, line in enumerate(_lines(path)):
        try:
            anchor = json.loads(line)
        except ValueError:
            return f"anchor {i} is not JSON", count
        if line != canonical(anchor) or anchor.get("prev_anchor_sha256") != previous:
            return f"anchor {i} breaks the anchor chain", count
        records = anchor.get("records")
        if not isinstance(records, int) or records < count or records < 1:
            return f"anchor {i} is not monotone", count
        if records > len(heads):
            return f"log truncated below anchor {i} ({records} records anchored)", records
        if heads[records - 1] != anchor.get("head_sha256"):
            return f"log rewritten below anchor {i}", records
        previous, count = hashlib.sha256(line).hexdigest(), records
    return None, count


def header_record(now: datetime, code_sha: str) -> dict:
    return {
        "kind": "header",
        "version": VERSION,
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": PREREG_SHA256,
        "min_scan_code_sha": MIN_SCAN_CODE_SHA,
        "rules": {
            "perms": PERMS,
            "seed": SEED,
            "family_alpha": FAMILY_ALPHA,
            "max_decisions_factor": MAX_DECISIONS_FACTOR,
            "grace_days": GRACE.days,
            "one_forward_test_per_pair": True,
            "sessions": "weekdays excluding US federal holidays",
        },
        "promotion_allowed": False,
    }


# --- git view of the scan's code --------------------------------------------------


class GitRepo:
    """Read-only ancestry and file lookups in a clone (``git`` subprocess)."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _git(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git", "-C", str(self.root), *args],
            capture_output=True,
            timeout=30,
            check=False,
        )

    def is_ancestor(self, ancestor: str, commit: str) -> bool:
        return self._git("merge-base", "--is-ancestor", ancestor, commit).returncode == 0

    def file_bytes(self, commit: str, path: str) -> bytes | None:
        result = self._git("show", f"{commit}:{path}")
        return result.stdout if result.returncode == 0 else None


# --- eligibility and admission -----------------------------------------------------


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise Refused(f"scan directory lacks {path.name}") from None


def _refuse_origin(origin: Any, where: str) -> None:
    if origin != LATEST_VINTAGE_ORIGIN:
        raise Refused(
            f"{where}: origin {origin!r} refused; only {LATEST_VINTAGE_ORIGIN} "
            "candidates may be forward-logged"
        )


def _check_code(summary: dict, repo: GitRepo, min_code_sha: str) -> str:
    code_sha = str(summary.get("code_sha", ""))
    if not HEX40.fullmatch(code_sha):
        raise Refused(f"scan code_sha {code_sha!r} is not a full commit sha")
    if not repo.is_ancestor(min_code_sha, code_sha):
        raise Refused(
            f"scan code {code_sha[:12]} does not include #661 ({min_code_sha[:12]}): "
            "rescan on current code"
        )
    recorded = summary.get("file_sha256") or {}
    if not set(SCAN_FILES) <= set(recorded):
        raise Refused("scan summary does not record the scan code's file sha256s")
    for path, sha in recorded.items():
        data = repo.file_bytes(code_sha, path)
        if data is None or lf_sha256(data) != sha:
            raise Refused(f"scan file {path} differs from {code_sha[:12]}:{path}")
    return code_sha


def _check_receipt(summary: dict, read_receipt: str) -> None:
    """Rebuild the panel receipt from summary.json; it must hash to the signed receipt.

    This authenticates the summary's feature specs, targets and publication
    schedules against the manifest. A receipt built under another revised-series
    denylist does not rebuild: rescan on current code.
    """
    try:
        rebuilt = {
            "reader": summary["reader"],
            "origin": LATEST_VINTAGE_ORIGIN,
            "vintage": summary["vintage"],
            "start": summary["read_start"],
            "as_of": summary["as_of"],
            "as_of_ts": datetime.fromisoformat(summary["as_of_ts"]).isoformat(),
            "features": summary["feature_specs"],
            "targets": summary["targets"],
            "publications": summary["publications"],
            "proxy_groups": summary["proxy_groups"],
            "revised_denylist_sha256": digest([sorted(REVISED_SERIES), list(REVISED_PREFIXES)]),
            "series": summary["series_read"],
        }
    except (KeyError, TypeError, ValueError):
        raise Refused("scan summary lacks the read receipt fields") from None
    if digest(rebuilt) != read_receipt:
        raise Refused(
            "scan summary does not rebuild the signed read receipt "
            "(edited summary, or a scan under another denylist: rescan on current code)"
        )


def _feature_spec(summary: dict, series_id: str) -> SeriesSpec:
    for spec in summary.get("feature_specs") or ():
        if spec.get("series_id") == series_id:
            return SeriesSpec(**spec)
    raise Refused(f"feature series {series_id} is not in the scan's declared universe")


def _target_spec(summary: dict, series_id: str) -> TargetSpec:
    for spec in summary.get("targets") or ():
        if spec.get("series_id") == series_id:
            return TargetSpec(**spec)
    raise Refused(f"target {series_id} is not a scan target")


def _publication(summary: dict, source: str) -> dict:
    recorded = (summary.get("publications") or {}).get(source)
    if source not in PUBLICATIONS or recorded != asdict(PUBLICATIONS[source]):
        raise Refused(f"publication schedule {source} differs from the scan's")
    return recorded


def shift_sessions(start: pd.Timestamp, n: int) -> pd.Timestamp:
    """00:00Z of the session ``n`` sessions after ``start`` (``n=0`` rolls forward).

    Sessions are weekdays that are not US federal holidays: the calendar
    ``research_real_panel.publication_times`` uses for business-day lags.
    """
    day = np.busday_offset(
        np.datetime64(start.date(), "D"), n, roll="forward", holidays=SESSION_HOLIDAYS
    )
    return pd.Timestamp(day).tz_localize("UTC")


def scientific_identity(family: str, feature: str) -> dict:
    """What a forward test is a test of, independent of the scan that froze it.

    Target series, label kind, horizon and the feature (series and transform
    suffix). Direction, manifest and scan are deliberately left out: a pair
    re-frozen by another scan, with either sign, is the same hypothesis.
    """
    target, label, fwd = family.rsplit("|", 2)
    series, suffix = feature.rsplit("|", 1)
    if not fwd.startswith("fwd") or not fwd[3:].isdigit():
        raise Refused(f"unknown family shape {family!r}")
    return {
        "target": target,
        "label": label,
        "horizon_sessions": int(fwd[3:]),
        "feature_series": series,
        "feature_suffix": suffix,
    }


def first_decision(frozen_at: datetime) -> pd.Timestamp:
    """The first session (00:00Z) strictly after ``frozen_at``."""
    day = pd.Timestamp(frozen_at.astimezone(timezone.utc).date(), tz="UTC") + pd.Timedelta(days=1)
    return shift_sessions(day, 0)  # rolls a weekend or holiday forward


def admit_scan(
    log: ForwardLog,
    scan_dir: Path,
    repo: GitRepo,
    now: datetime,
    code_sha: str,
    min_code_sha: str = MIN_SCAN_CODE_SHA,
) -> list[dict]:
    """Admit every candidate of one scan, or refuse the whole scan (``Refused``)."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    with log.locked():
        return _admit_locked(log, Path(scan_dir), repo, now, code_sha, min_code_sha)


def _admit_locked(
    log: ForwardLog,
    scan_dir: Path,
    repo: GitRepo,
    now: datetime,
    code_sha: str,
    min_code_sha: str,
) -> list[dict]:
    if (scan_dir / RELABELLED_FILE).exists():
        raise Refused(f"{RELABELLED_FILE} present: pre-S09b scan, candidates are not eligible")
    summary = _load_json(scan_dir / "summary.json")
    frozen = _load_json(scan_dir / "run" / "discovery-frozen.json")
    holdout = _load_json(scan_dir / "run" / "holdout-result.json")
    candidates = _load_json(scan_dir / "frozen-candidates.json")
    payload = frozen.get("payload") or {}
    manifest = frozen.get("sha256")
    if manifest in DENIED_DISCOVERY_MANIFESTS:
        raise Refused("discovery manifest of the ef0d564b scan: its candidates are never admitted")
    if digest(payload) != manifest:
        raise Refused("discovery manifest sha256 does not match its payload")
    protocol = payload.get("protocol") or {}
    _refuse_origin(protocol.get("origin"), "manifest protocol")
    _refuse_origin(summary.get("origin"), "scan summary")
    if (
        not HEX64.fullmatch(str(protocol.get("read_receipt", "")))
        or summary.get("read_receipt_sha256") != protocol["read_receipt"]
    ):
        raise Refused("scan read receipt missing or inconsistent")
    if "self_lag" not in protocol or "block_basis" not in payload:
        raise Refused("manifest predates S09b (no self_lag / block_basis)")
    if payload.get("candidate_eligible") is not True or protocol.get("sampling") != "horizon_spaced":
        raise Refused("manifest is not candidate-eligible (fixed-step runs are diagnostic only)")
    if payload.get("promotion_allowed") is not False:
        raise Refused("manifest does not carry promotion_allowed: false")
    if (
        holdout.get("discovery_manifest") != manifest
        or summary.get("discovery_manifest_sha256") != manifest
        or digest(holdout.get("candidates")) != digest(candidates)
    ):
        raise Refused("holdout result / frozen candidates are not the ones this manifest produced")
    _check_receipt(summary, protocol["read_receipt"])
    scan_code_sha = _check_code(summary, repo, min_code_sha)
    if not isinstance(candidates, list) or not candidates:
        raise Refused("scan froze no candidates: nothing to admit")

    check = log.verify_chain()
    if not check["ok"]:
        raise RuntimeError(f"forward log chain is broken: {check['detail']}")
    records = log.read_all()
    admitted = {r["candidate_id"] for r in records if r.get("kind") == "admission"}
    manifests = {r["scan"]["discovery_manifest"] for r in records if r.get("kind") == "admission"}
    if manifest in manifests:
        raise Refused("this scan was already admitted")
    # One forward test per scientific pair, ever: keyed on the pair, not the scan.
    decided = {r["candidate_id"] for r in records if r.get("kind") == "verdict"}
    tested = {
        r["identity_sha256"]: ("DECIDED" if r["candidate_id"] in decided else "OPEN")
        for r in records
        if r.get("kind") == "admission"
    }

    ledger = {(t["family"], t["feature"]): t for t in payload.get("ledger") or ()}
    checks = {c["trial_id"]: c for c in holdout.get("holdout_checks") or ()}
    self_lag = {tuple(pair) for pair in protocol.get("self_lag") or ()}
    family_size = len(candidates)
    alpha = FAMILY_ALPHA / family_size
    out = []
    for candidate in candidates:
        spec = candidate.get("specification") or {}
        cid = candidate.get("sha256")
        if digest(spec) != cid:
            raise Refused("candidate sha256 does not match its specification")
        if cid in admitted:
            raise Refused(f"candidate {cid[:12]} was already admitted")
        if candidate.get("state") in REFUSED_STATES or candidate.get("state") != FORWARD_PENDING:
            raise Refused(f"candidate {cid[:12]} state {candidate.get('state')!r} refused")
        if candidate.get("promotion_allowed") is not False:
            raise Refused(f"candidate {cid[:12]} does not carry promotion_allowed: false")
        _refuse_origin(spec.get("origin"), f"candidate {cid[:12]}")
        family, feature = spec.get("family", ""), spec.get("feature", "")
        if spec.get("discovery_manifest") != manifest:
            raise Refused(f"candidate {cid[:12]} names another discovery manifest")
        target_id, label, fwd = family.rsplit("|", 2)
        feature_id, suffix = feature.rsplit("|", 1)
        if (family, feature) in self_lag or feature_id in proxy_group(target_id):
            raise Refused(
                f"candidate {cid[:12]} is SELF_LAG: {feature_id} proxies target {target_id}"
            )
        identity = scientific_identity(family, feature)
        identity_sha = digest(identity)
        if identity_sha in tested:
            raise Refused(
                f"{family} <- {feature} already has a forward test ({tested[identity_sha]}) in "
                "this log: each scientific pair is tested forward at most once"
            )
        tested[identity_sha] = "ADMITTING"
        trial = ledger.get((family, feature))
        if (
            trial is None
            or trial.get("status") != "tested"
            or trial.get("selected") is not True
            or (1 if trial["r"] > 0 else -1) != spec.get("direction")
        ):
            raise Refused(f"candidate {cid[:12]} is not a selected discovery trial")
        check = checks.get(trial["trial_id"])
        if (
            check is None
            or check.get("retrospective_survivor") is not True
            or check["r"] * trial["r"] <= 0
        ):
            raise Refused(f"candidate {cid[:12]} is not a holdout retrospective survivor")
        forward_start = spec.get("forward_start_not_before")
        if forward_start != protocol.get("end") or now < stamp(forward_start):
            raise Refused(f"candidate {cid[:12]} cannot be frozen before {forward_start}")
        if not fwd.startswith("fwd") or not fwd[3:].isdigit() or suffix not in (
            "chg5",
            "chg20",
            "z60",
        ):
            raise Refused(f"candidate {cid[:12]} has an unknown family or feature shape")
        horizon = int(fwd[3:])
        if (payload.get("horizons") or {}).get(family) != spec.get("horizon") or not str(
            spec.get("horizon")
        ).startswith(f"{horizon} sessions"):
            raise Refused(f"candidate {cid[:12]} horizon differs from its family")
        feature_spec = _feature_spec(summary, feature_id)
        target_spec = _target_spec(summary, target_id)
        if target_spec.label != label:
            raise Refused(f"candidate {cid[:12]} label differs from the scan target's")
        min_n = int(protocol["min_n"])
        frozen_block = int(payload["blocks"][family])
        declared = int(protocol.get("block") or 0)
        block = declared or max(1, min(frozen_block, min_n // MIN_BLOCKS))
        start = first_decision(now)
        plan = {
            "target": {**asdict(target_spec), "publication": _publication(summary, target_spec.source)},
            "feature": {
                "name": feature,
                "suffix": suffix,
                **asdict(feature_spec),
                "publication": _publication(summary, feature_spec.source),
            },
            "family": family,
            "direction": int(spec["direction"]),
            "horizon_sessions": horizon,
            "spacing_sessions": max(int(protocol["step"]), horizon),
            "first_decision_at": start.isoformat(),
            "statistic": protocol["statistic"],
            "decision_rule": (
                "one look on the first min_n valid pairs in decision order: one-sided "
                "block-permutation test of the statistic in the candidate's direction; "
                "supported iff direction*rho > 0 and p <= alpha"
            ),
            "block": block,
            "frozen_discovery_block": frozen_block,
            "perms": PERMS,
            "seed": SEED,
            "min_n": min_n,
            "max_decisions": MAX_DECISIONS_FACTOR * min_n,
            "alpha": alpha,
            "family_size": family_size,
            "grace_days": GRACE.days,
            "failure": (
                f"{FAILED} if direction*rho <= 0, p > alpha or constant input at the look; "
                f"{INCONCLUSIVE} if max_decisions are resolved with fewer than min_n valid "
                "pairs; both mean not supported and close the candidate"
            ),
            "promotion_allowed": False,
        }
        out.append(
            {
                "kind": "admission",
                "run_at": now.isoformat(),
                "code_sha": code_sha,
                "candidate_id": cid,
                "identity": identity,
                "identity_sha256": identity_sha,
                "specification": spec,
                "scan": {
                    "dir": scan_dir.name,
                    "code_sha": scan_code_sha,
                    "discovery_manifest": manifest,
                    "read_receipt": protocol["read_receipt"],
                    "as_of": summary.get("as_of"),
                    "as_of_ts": summary.get("as_of_ts"),
                    "summary_sha256": lf_sha256((scan_dir / "summary.json").read_bytes()),
                },
                "plan": plan,
                "plan_sha256": digest(plan),
                "promotion_allowed": False,
            }
        )
    by_trial = {t["trial_id"]: (t["family"], t["feature"]) for t in payload.get("ledger") or ()}
    survivors = sorted(
        by_trial.get(c["trial_id"], ("?", "?"))
        for c in holdout.get("holdout_checks") or ()
        if c.get("retrospective_survivor") is True
    )
    frozen_pairs = sorted(
        ((c.get("specification") or {}).get("family"), (c.get("specification") or {}).get("feature"))
        for c in candidates
    )
    if survivors != frozen_pairs:
        raise Refused("frozen candidates are not exactly the holdout survivors (family size)")
    header = [] if records else [header_record(now, code_sha)]
    return log.append_locked(header + out)


# --- schedule ----------------------------------------------------------------------


def decision_at(plan: dict, k: int) -> pd.Timestamp:
    return shift_sessions(pd.Timestamp(plan["first_decision_at"]), k * plan["spacing_sessions"])


def label_end(plan: dict, k: int) -> pd.Timestamp:
    return shift_sessions(decision_at(plan, k), plan["horizon_sessions"])


def label_known_at(plan: dict, k: int) -> pd.Timestamp:
    publication = Publication(**plan["target"]["publication"])
    return publication_times([label_end(plan, k).date()], publication)[0]


# --- reads (latest-vintage adapter only) --------------------------------------------


def _specs(plan: dict) -> tuple[SeriesSpec, TargetSpec]:
    feature = {k: plan["feature"][k] for k in ("series_id", "transform", "source", "stale_sessions")}
    target = {k: plan["target"][k] for k in ("series_id", "label", "source")}
    for part in (plan["feature"], plan["target"]):
        if asdict(PUBLICATIONS[part["source"]]) != part["publication"]:
            raise RuntimeError(
                f"publication schedule {part['source']} changed since admission: "
                "the running code no longer matches the frozen plan"
            )
    return SeriesSpec(**feature), TargetSpec(**target)


def read_prediction(conn, plan: dict, decision: pd.Timestamp) -> tuple[float | None, str | None, str]:
    """The feature value GRID held at the decision instant, and when it was known."""
    feature, target = _specs(plan)
    instant = decision.to_pydatetime()
    panel = load_latest_vintage_panel(
        conn,
        (feature,),
        (target,),
        start=(instant - WARMUP).date(),
        as_of=instant.date(),
        as_of_ts=instant,
    )
    name = plan["feature"]["name"]
    value = panel.feature_frame()[name].get(decision)
    known = panel.feature_known_at()[name].get(decision)
    if value is None or not np.isfinite(value) or pd.isna(known):
        return None, None, panel.receipt_sha
    if known > decision:
        raise RuntimeError("adapter returned a feature known after its decision")
    return float(value), known.isoformat(), panel.receipt_sha


def read_outcome(
    conn, plan: dict, k: int, now: datetime
) -> tuple[float | None, float | None, str]:
    """Target levels at decision k and at its label end, as read now."""
    feature, target = _specs(plan)
    start, end = decision_at(plan, k), label_end(plan, k)
    panel = load_latest_vintage_panel(
        conn,
        (feature,),
        (target,),
        start=(start - pd.Timedelta(days=10)).date(),
        as_of=end.date(),
        as_of_ts=now,
    )
    levels = panel.target_level(target, pd.DatetimeIndex([start, end]))
    first, last = (float(v) if np.isfinite(v) else None for v in levels.to_numpy())
    return first, last, panel.receipt_sha


# --- state, evaluation and the stop rule --------------------------------------------


def candidates_state(records: list[dict]) -> dict[str, dict]:
    state: dict[str, dict] = {}
    for record in records:
        kind, cid = record.get("kind"), record.get("candidate_id")
        if kind == "admission":
            state[cid] = {"admission": record, "predictions": {}, "outcomes": {}, "verdict": None}
        elif kind in ("prediction", "outcome"):
            state[cid][f"{kind}s"][record["k"]] = record
        elif kind == "verdict":
            state[cid]["verdict"] = record
    return state


def valid_pair(admission: dict, prediction: dict | None, outcome: dict | None) -> bool:
    """Re-check every no-lookahead rule from the log itself."""
    if not prediction or not outcome or prediction.get("excluded") or outcome.get("excluded"):
        return False
    frozen_at = stamp(admission["run_at"])
    decided = stamp(prediction["decision_at"])
    known = stamp(prediction["label_known_at"])
    feature_known = prediction["feature"].get("known_at")
    return (
        decided > frozen_at
        and outcome["decision_at"] == prediction["decision_at"]
        and feature_known is not None
        and stamp(feature_known) <= decided
        and decided <= stamp(prediction["run_at"]) < known
        and stamp(outcome["run_at"]) >= known
        and prediction["feature"]["value"] is not None
        and outcome["label"] is not None
    )


def resolved(entry: dict, k: int) -> bool:
    prediction = entry["predictions"].get(k)
    return prediction is not None and (prediction.get("excluded") or k in entry["outcomes"])


def one_sided_p(x, y, direction: int, block: int, perms: int, seed: int) -> tuple[float, float]:
    xc = np.asarray(x, dtype=float) - np.mean(x)
    yc = np.asarray(y, dtype=float) - np.mean(y)
    scale = math.sqrt(float(xc @ xc) * float(yc @ yc))
    observed = float(xc @ yc) / scale
    null = (yc[block_permutations(len(yc), block, perms, seed)] @ xc) / scale
    extreme = int((direction * null >= direction * observed - 1e-12).sum())
    return observed, (1 + extreme) / (perms + 1)


def verdict(entry: dict) -> dict | None:
    """The single pre-registered look, or ``None`` while it is not due."""
    admission = entry["admission"]
    plan = admission["plan"]
    pairs, k = [], 0
    while k < plan["max_decisions"] and resolved(entry, k) and len(pairs) < plan["min_n"]:
        prediction, outcome = entry["predictions"][k], entry["outcomes"].get(k)
        if valid_pair(admission, prediction, outcome):
            pairs.append((k, prediction["feature"]["value"], outcome["label"]))
        k += 1
    base = {
        "kind": "verdict",
        "candidate_id": admission["candidate_id"],
        "identity": admission["identity"],
        "identity_sha256": admission["identity_sha256"],
        "family": plan["family"],
        "feature": plan["feature"]["name"],
        "direction": plan["direction"],
        "prereg_sha256": PREREG_SHA256,
        "plan_sha256": admission["plan_sha256"],
        "admitted_at": admission["run_at"],
        "windows": {
            "first_decision_at": plan["first_decision_at"],
            "pairs_first_decision_at": decision_at(plan, pairs[0][0]).isoformat() if pairs else None,
            "pairs_last_decision_at": decision_at(plan, pairs[-1][0]).isoformat() if pairs else None,
            "pairs_last_label_end": label_end(plan, pairs[-1][0]).isoformat() if pairs else None,
            "last_resolved_decision_at": decision_at(plan, k - 1).isoformat() if k else None,
        },
        "decisions_resolved": k,
        "n": len(pairs),
        "pairs_sha256": digest(pairs),
        "family_size": plan["family_size"],
        "alpha": plan["alpha"],
        "promotion_allowed": False,
    }
    if len(pairs) < plan["min_n"]:
        if k < plan["max_decisions"]:
            return None
        return {**base, "state": INCONCLUSIVE, "rho": None, "p_one_sided": None,
                "reason": "max_decisions resolved with fewer than min_n valid pairs"}
    x = np.array([p[1] for p in pairs], dtype=float)
    y = np.array([p[2] for p in pairs], dtype=float)
    if plan["statistic"] == "spearman":
        x, y = rankdata(x), rankdata(y)
    if np.ptp(x) == 0 or np.ptp(y) == 0:
        return {**base, "state": FAILED, "rho": None, "p_one_sided": None,
                "reason": "constant input"}
    rho, p = one_sided_p(x, y, plan["direction"], plan["block"], plan["perms"], plan["seed"])
    supported = plan["direction"] * rho > 0 and p <= plan["alpha"]
    return {
        **base,
        "state": SUPPORTED if supported else FAILED,
        "rho": rho,
        "p_one_sided": p,
        "block": plan["block"],
        "reason": None if supported else "direction*rho <= 0 or p > alpha",
    }


# --- the daily job -------------------------------------------------------------------


def run_forward(log: ForwardLog, conn, now: datetime, code_sha: str) -> list[dict]:
    """Log due predictions and known outcomes, then any verdict now due."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    with log.locked():
        return _run_locked(log, conn, now, code_sha)


def _run_locked(log: ForwardLog, conn, now: datetime, code_sha: str) -> list[dict]:
    check = log.verify_chain()
    if not check["ok"]:
        raise RuntimeError(f"forward log chain is broken: {check['detail']}")
    records = log.read_all()
    new: list[dict] = [] if records else [header_record(now, code_sha)]
    stamp_now = pd.Timestamp(now)
    for cid, entry in candidates_state(records).items():
        if entry["verdict"] is not None:
            continue
        plan = entry["admission"]["plan"]
        envelope = {"run_at": now.isoformat(), "code_sha": code_sha, "candidate_id": cid}
        for k in range(plan["max_decisions"]):
            decided = decision_at(plan, k)
            if decided > stamp_now:
                break
            known = label_known_at(plan, k)
            common = {
                **envelope,
                "k": k,
                "decision_at": decided.isoformat(),
                "label_end": label_end(plan, k).isoformat(),
                "label_known_at": known.isoformat(),
            }
            if k not in entry["predictions"]:
                if stamp_now >= known:
                    record = {**common, "kind": "prediction", "excluded": True,
                              "exclusion_reason": EXCL_LATE,
                              "feature": {"name": plan["feature"]["name"], "value": None,
                                          "known_at": None}, "receipt": None}
                else:
                    value, feature_known, receipt = read_prediction(conn, plan, decided)
                    record = {
                        **common,
                        "kind": "prediction",
                        "excluded": value is None,
                        "exclusion_reason": EXCL_FEATURE if value is None else None,
                        "feature": {"name": plan["feature"]["name"], "value": value,
                                    "known_at": feature_known},
                        "receipt": receipt,
                    }
                entry["predictions"][k] = record
                new.append(record)
            prediction = entry["predictions"][k]
            if prediction.get("excluded") or k in entry["outcomes"] or stamp_now < known:
                continue
            first, last, receipt = read_outcome(conn, plan, k, now)
            if first is None or last is None:
                if stamp_now < known + GRACE:
                    continue  # not pulled yet: wait inside the grace period
                label, excluded = None, EXCL_TARGET
            else:
                kind = plan["target"]["label"]
                label = last - first if kind == "change" else (last / first - 1 if first else None)
                excluded = None if label is not None else EXCL_TARGET
            record = {
                **common,
                "kind": "outcome",
                "target_start": first,
                "target_end": last,
                "label": label,
                "excluded": excluded is not None,
                "exclusion_reason": excluded,
                "receipt": receipt,
            }
            entry["outcomes"][k] = record
            new.append(record)
        closing = verdict(entry)
        if closing is not None:
            new.append({**closing, "run_at": now.isoformat(), "code_sha": code_sha})
    return log.append_locked(new)


# --- status (activity only until the look) ------------------------------------------


def status_report(
    log: ForwardLog, now: datetime | None = None, external_anchors: Path | None = None
) -> dict:
    check = log.verify_chain(external_anchors)
    report: dict[str, Any] = {"chain": check, "generated_at": (now or datetime.now(timezone.utc)).isoformat()}
    if not check["ok"]:
        report["candidates"] = []
        return report
    records = log.read_all()
    report["header"] = records[0] if records else None
    admissions = [r for r in records if r.get("kind") == "admission"]
    report["scans_admitted"] = len({r["scan"]["discovery_manifest"] for r in admissions})
    rows = []
    for cid, entry in candidates_state(records).items():
        plan = entry["admission"]["plan"]
        predictions = list(entry["predictions"].values())
        outcomes = list(entry["outcomes"].values())
        reasons: dict[str, int] = {}
        for record in predictions + outcomes:
            if record.get("excluded"):
                reasons[record["exclusion_reason"]] = reasons.get(record["exclusion_reason"], 0) + 1
        row = {
            "candidate_id": cid,
            "family": plan["family"],
            "feature": plan["feature"]["name"],
            "direction": plan["direction"],
            "admitted_at": entry["admission"]["run_at"],
            "first_decision_at": plan["first_decision_at"],
            "predictions": len(predictions),
            "outcomes": len(outcomes),
            "valid_pairs": sum(
                valid_pair(entry["admission"], entry["predictions"].get(k), entry["outcomes"].get(k))
                for k in entry["predictions"]
            ),
            "min_n": plan["min_n"],
            "max_decisions": plan["max_decisions"],
            "earliest_look_label_known_at": label_known_at(plan, plan["min_n"] - 1).isoformat(),
            "excluded_by_reason": reasons,
            "verdict": None,
        }
        if entry["verdict"] is not None:  # only after the single look
            v = entry["verdict"]
            row["verdict"] = {key: v.get(key) for key in ("state", "n", "rho", "p_one_sided", "alpha", "reason")}
        rows.append(row)
    report["candidates"] = rows
    return report


def format_status(report: dict) -> str:
    chain = report["chain"]
    lines = [
        "# Hypothesis forward log v1: status",
        "",
        f"Generated {report['generated_at']}. Activity and data quality only: no correlation "
        "or p-value is shown before a candidate's verdict. Promotion is never allowed here.",
        "",
        f"- Chain: {'OK' if chain['ok'] else 'BROKEN'} ({chain['records']} records)"
        + (f", head `{chain['head_sha256']}`" if chain["ok"] and chain["head_sha256"] else "")
        + (f": {chain['detail']}" if not chain["ok"] else ""),
        f"- Anchored records: {chain.get('anchored_records', 0)} (`{ANCHOR_FILENAME}`). The "
        "chain proves nothing against truncation or a full recompute except relative to "
        "an anchor copy held off-host; this STATUS.md is regenerated from the log and is "
        "not an anchor.",
        f"- Pre-registration sha256: `{PREREG_SHA256}` ({PREREG_PATH.as_posix()})",
        f"- Candidates: {len(report['candidates'])} from {report.get('scans_admitted', 0)} "
        f"scan(s). Each scan's candidates share alpha {FAMILY_ALPHA} (Bonferroni); each "
        "scientific pair is tested at most once; so P(any false 'supported') <= "
        f"{FAMILY_ALPHA} x scans = {FAMILY_ALPHA * report.get('scans_admitted', 0):.2f}.",
    ]
    for row in report["candidates"]:
        lines += [
            "",
            f"## {row['family']} <- {row['feature']} (direction {row['direction']:+d})",
            "",
            f"- Candidate `{row['candidate_id']}`, frozen {row['admitted_at']}, "
            f"first decision {row['first_decision_at']}",
            f"- Predictions {row['predictions']}, outcomes {row['outcomes']}, "
            f"valid pairs {row['valid_pairs']} / {row['min_n']} (stop after "
            f"{row['max_decisions']} decisions); earliest look after "
            f"{row['earliest_look_label_known_at']}",
            "- Excluded: "
            + (", ".join(f"{k} {v}" for k, v in sorted(row["excluded_by_reason"].items())) or "none"),
        ]
        if row["verdict"]:
            v = row["verdict"]
            lines.append(
                f"- Verdict: {v['state']} (n {v['n']}, rho {v['rho']}, one-sided p "
                f"{v['p_one_sided']}, alpha {v['alpha']}); promotion not allowed"
            )
    return "\n".join(lines) + "\n"


def write_status(log: ForwardLog, now: datetime | None = None) -> str:
    text = format_status(status_report(log, now))
    log.log_dir.mkdir(parents=True, exist_ok=True)
    temporary = log.log_dir / (STATUS_FILENAME + ".tmp")
    temporary.write_bytes(text.encode("utf-8"))
    os.replace(temporary, log.log_dir / STATUS_FILENAME)
    return text


def resolve_code_sha(repo_root: Path) -> str:
    """The running code's commit; there is no caller override.

    An installed archive carries a ``VERSION`` file (the installer's pinned
    commit). Otherwise the checkout must be a git work tree with no modified
    tracked files, and its ``HEAD`` is used.
    """
    version = Path(repo_root) / "VERSION"
    if version.exists():
        sha = version.read_text(encoding="utf-8").strip()
        if not HEX40.fullmatch(sha):
            raise RuntimeError(f"{version} does not hold a full commit sha")
        return sha

    def git(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    head = git("rev-parse", "HEAD")
    if head.returncode != 0 or not HEX40.fullmatch(head.stdout.strip()):
        raise RuntimeError(f"could not resolve code_sha under {repo_root}: no VERSION, no git")
    dirty = git("status", "--porcelain", "--untracked-files=no")
    if dirty.returncode != 0 or dirty.stdout.strip():
        raise RuntimeError(f"{repo_root} has modified tracked files: code_sha would not match")
    return head.stdout.strip()
