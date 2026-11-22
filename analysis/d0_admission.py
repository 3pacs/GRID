"""D0 admission verifier (gap G14; #1129, #1142).

The signed D0 contract (``docs/research/d0-research-contract-v0.md`` §8, §9 G14) lets a
hypothesis carry ``ADMITTED`` only when an authenticated verifier says so. This module is
that verifier. It is fail-closed in three independent ways:

1. :data:`VERIFIER_ACCEPTED` is ``False`` until the reviewer has accepted this code. While it
   is ``False``, every ``ADMITTED`` label is refused, whatever else holds.
2. Every admission needs SSH signatures (``ssh-keygen -Y verify``, namespace
   :data:`NAMESPACE`) from each principal in :data:`REQUIRED_PRINCIPALS`, checked against
   the committed ``allowed_signers`` file. That file ships with no keys. The reviewer and
   the operator each add their own key; this module never creates or registers one.
3. Every other prerequisite is checked against bytes on disk, not against strings that
   claim them.

What a hypothesis labelled ``ADMITTED`` must carry (review of bc52a40c):
- It must be one of the supported hypotheses in :data:`FROZEN_SPEC`. Its required inputs,
  sample floors, level, window, coverage scope and blocking gaps are read from that frozen
  spec, transcribed from the owner-signed contract body and pinned in
  :data:`FROZEN_SPEC_SHA256`, never from the mutable manifest. The manifest's own copies
  (``n_floor``, ``required_inputs``, ``blocking_gaps``, ``window``, ``one_sided_level``,
  ``track``) must agree with the frozen spec; any drift is refused.
- ``required_inputs``: exactly the frozen input ids. Every id must exist in the manifest
  (unknown dependencies are refused) and pass :func:`input_errors`.
- ``coverage``: ``{"measured_n": int >= the frozen floor, "receipt": <file receipt>}``. The
  receipt file is typed JSON (:func:`coverage_receipt_errors`): its hypothesis, scope,
  unit, inputs and count must match the frozen spec and ``measured_n``, and a judged
  historical D1 receipt must name the frozen G15 closure it was measured under.
- ``blocking_gaps``: a list of gap ids that includes every frozen blocking gap. Each must
  carry a typed closure whose evidence file is verified (:func:`gap_closure_errors`); a
  ``status`` string never closes a gap. G15 needs the typed price-only exception fields.
- ``admission``: ``{"signed_at": "YYYY-MM-DD"}``, a real calendar date, not before the
  owner's signature and not in the future.
- ``reviewer.signed`` must be exactly ``true`` (contract §8), with ``signed_at`` and
  ``manifest_sha256``.

The manifest as a whole must be owner-signed with the pinned contract body
(``analysis/d0_manifest.contract_pin_errors``). If ``reviewer.signed`` is true,
``reviewer.manifest_sha256`` must equal :func:`manifest_digest`.

Chronology (:func:`_owner_errors`, :func:`hypothesis_errors`, :func:`gap_closure_errors`,
:func:`coverage_receipt_errors`). Every date is a real ``YYYY-MM-DD`` no later than
today, and they run in this order::

    owner signed_at <= reviewer.signed_at <= admission.signed_at
    every blocking-gap closed_at <= admission.signed_at
    G15 closed_at <= coverage measured_at <= reviewer.signed_at      (judged historical D1)
    coverage measured_at <= reviewer.signed_at                       (every hypothesis)

The reviewer re-runs each coverage query before signing (contract §8), so the count is
measured before the reviewer's date, and a judged historical D1 count is measured under a
G15 exception that was already closed.

What each signature covers: :func:`admission_statement`. This is canonical JSON of the
contract id, the pinned body hash, the frozen spec hash, the hypothesis id, its admission
date, the owner's and the reviewer's signing dates, and the manifest digest. The digest is
canonical JSON of the whole manifest except ``reviewer.signed`` and
``reviewer.manifest_sha256``, so recording the result after signing does not change what
was signed (no self-referential field). ``reviewer.signed_at`` is inside the digest and
named in the statement, so relabelling it, like any other manifest edit after signing,
invalidates every signature.

A file receipt is ``{"path": <repo-relative path>, "sha256": <hex>}``. The file must
exist inside the repository and hash to that value; a string is never a receipt.

This module reads only repository files and runs ``ssh-keygen``. It never reads the
database, a provider or an outcome.
"""

from __future__ import annotations

import argparse
import copy
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any

from analysis import d0_manifest as M

NAMESPACE = "grid-d0-admission"
SIGNATURE_DIR = Path("docs/research/d0-admission")
ALLOWED_SIGNERS = SIGNATURE_DIR / "allowed_signers"
REQUIRED_PRINCIPALS: tuple[str, ...] = ("reviewer", "operator")
#: Only the reviewer's result fields. ``signed_at`` is NOT excluded: the reviewer's date is
#: committed by the digest and by every admission statement (no date relabelling).
DIGEST_EXCLUDED_REVIEWER_KEYS: tuple[str, ...] = ("signed", "manifest_sha256")
VISIBILITY_OK = {"qualified", "not_applicable"}
VISIBILITY_STATES = VISIBILITY_OK | {"applicable"}

#: Set to True only after the reviewer has accepted this verifier (contract §9 G14), as its
#: own reviewed commit on the operator's go. Until then nothing can be ADMITTED.
VERIFIER_ACCEPTED = False

#: Frozen admission spec per supported hypothesis, transcribed from the owner-signed contract
#: body (pinned in ``analysis/d0_manifest.D0_CONTRACT_BODY_SHA256``). The verifier reads the
#: required inputs, floors, level, window, coverage scope and blocking gaps from here, never
#: from the manifest. ``tests/test_d0_admission.py`` checks each number against the body text.
#: Input ids not yet in the manifest (``sec.edgar.8k_acceptance``, ``tiingo.eod_incl_delisted``,
#: the D2-H1 holdout fetches, ``ledger.fly_shadow_1.fly_days``) are refused as unknown
#: dependencies until they are registered with receipts. Any edit here is a reviewed code
#: change and needs a new :data:`FROZEN_SPEC_SHA256`.
FROZEN_SPEC: dict[str, Any] = {
    "schema": "d0-frozen-admission-spec/1",
    "hypotheses": {
        "D1-F1": {
            "basis": "contract §5.1 (sources, universe, prices), §5.2 holdout, §5.3 level and floor, §9 G2 G3 G15",
            "track": "D1",
            "window": {"dev": ["2012-01-01", "2018-12-31"], "holdout": ["2019-01-01", "2025-12-31"]},
            "scope": "holdout",
            "one_sided_level": 0.9875,
            "n_floor": 200,
            "coverage_unit": "top_quintile_events",
            "cells": ["+5", "+20"],
            "cell_floors": {"top_quintile_events": 200, "distinct_event_weeks": 60},
            "required_inputs": ["sec.edgar.8k_acceptance", "tiingo.eod_incl_delisted",
                                "universe.liquid_us_equity_pit"],
            "blocking_gaps": ["G15", "G2", "G3"],
            "judged_historical_d1": True,
        },
        "D1-F2": {
            "basis": "contract §5.1 (sources, universe, prices), §5.2 holdout, §5.3 level and floor, §9 G2 G3 G15",
            "track": "D1",
            "window": {"dev": ["2012-01-01", "2018-12-31"], "holdout": ["2019-01-01", "2025-12-31"]},
            "scope": "holdout",
            "one_sided_level": 0.9875,
            "n_floor": 200,
            "coverage_unit": "top_quintile_events",
            "cells": ["+5", "+20"],
            "cell_floors": {"top_quintile_events": 200, "distinct_event_weeks": 60},
            "required_inputs": ["sec.edgar.8k_acceptance", "tiingo.eod_incl_delisted",
                                "universe.liquid_us_equity_pit"],
            "blocking_gaps": ["G15", "G2", "G3"],
            "judged_historical_d1": True,
        },
        "D2-H1a": {
            "basis": "contract §6.2 (OPRA cbbo-1m plus SPY 1-minute bars, 2018-01-02 .. 2022-11-11), §6.4, §9 G4 G5",
            "track": "D2",
            "window": {"holdout": ["2018-01-02", "2022-11-11"]},
            "scope": "holdout",
            "one_sided_level": 0.975,
            "n_floor": 300,
            "coverage_unit": "sessions",
            "cells": [],
            "cell_floors": {},
            "required_inputs": ["research.opra.cbbo_1m.SPY_0dte_d2h1_holdout", "research.spy_1min.d2h1_holdout"],
            "blocking_gaps": ["G4", "G5"],
            "judged_historical_d1": False,
        },
        "D2-H1b": {
            "basis": "contract §6.2 (OPRA cbbo-1m plus SPY 1-minute bars, 2018-01-02 .. 2022-11-11), §6.4, §9 G4 G5",
            "track": "D2",
            "window": {"holdout": ["2018-01-02", "2022-11-11"]},
            "scope": "holdout",
            "one_sided_level": 0.975,
            "n_floor": 300,
            "coverage_unit": "sessions",
            "cells": [],
            "cell_floors": {},
            "required_inputs": ["research.opra.cbbo_1m.SPY_0dte_d2h1_holdout", "research.spy_1min.d2h1_holdout"],
            "blocking_gaps": ["G4", "G5"],
            "judged_historical_d1": False,
        },
        "D2-H2": {
            "basis": "contract §6.2 (live Schwab chain, ledger fly_days, forward from 2026-11-12), §6.4",
            "track": "D2",
            "window": {"forward_from": "2026-11-12"},
            "scope": "forward_from",
            "one_sided_level": 0.95,
            "n_floor": 100,
            "coverage_unit": "traded_sessions",
            "cells": [],
            "cell_floors": {},
            "required_inputs": ["ledger.fly_shadow_1.fly_days", "live.schwab.chains_quotes_bars"],
            "blocking_gaps": [],
            "judged_historical_d1": False,
        },
        "D2-H3": {
            "basis": "contract §6.2 (live dte0 regime plus bars, forward from 2026-11-12), §6.4",
            "track": "D2",
            "window": {"forward_from": "2026-11-12"},
            "scope": "forward_from",
            "one_sided_level": 0.95,
            "n_floor": 50,
            "coverage_unit": "qualifying_sessions",
            "cells": [],
            "cell_floors": {},
            "required_inputs": ["live.schwab.chains_quotes_bars"],
            "blocking_gaps": [],
            "judged_historical_d1": False,
        },
    },
    # contract §5.1 and §9 G15: a price-only exception tied to a named source, version and basis
    # and to retained evidence; it states the clocks it does not establish and the claims it
    # forbids, and is qualified before any judged look; no clock backfill and no outcome read.
    "g15_closure": {
        "exception": "price_only",
        "non_empty_strings": ["source", "source_version", "price_basis"],
        "non_empty_string_lists": ["clocks_not_established", "forbidden_claims"],
        "non_empty_receipt_lists": ["retained_evidence"],
        "exactly_true": ["qualified"],
        "exactly_false": ["clock_backfill", "outcome_read"],
        "qualified_by": "reviewer",
    },
}

#: sha256 of :func:`canonical` (:data:`FROZEN_SPEC`). Refused at runtime if it does not match.
FROZEN_SPEC_SHA256 = "dc09f57d01c886f60ff16fc1609ec56bd71558b4630c5d0f05f715b61ec857d0"

COVERAGE_RECEIPT_SCHEMA = "d0-coverage-receipt/1"
GAP_CLOSURE_SCHEMA = "d0-gap-closure/1"
CLOSED = "CLOSED"
#: Manifest hypothesis fields that must equal the frozen spec wherever the manifest carries them.
_FROZEN_SCALARS: tuple[str, ...] = ("track", "window", "one_sided_level", "n_floor")

_SHA256 = re.compile(r"[0-9a-f]{64}")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _today() -> date:
    return datetime.now(timezone.utc).date()


def parse_date(value: Any) -> date | None:
    """A real calendar date written exactly ``YYYY-MM-DD``; anything else is None."""
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def manifest_digest(manifest: dict[str, Any]) -> str:
    """sha256 of the manifest's canonical JSON, without the reviewer's result fields."""
    m = copy.deepcopy(manifest)
    reviewer = m.get("reviewer")
    if isinstance(reviewer, dict):
        for key in DIGEST_EXCLUDED_REVIEWER_KEYS:
            reviewer.pop(key, None)
    return hashlib.sha256(canonical(m)).hexdigest()


def frozen_spec_digest(spec: dict[str, Any] | None = None) -> str:
    return hashlib.sha256(canonical(FROZEN_SPEC if spec is None else spec)).hexdigest()


def frozen_spec_errors() -> list[str]:
    """Refuse everything if the frozen spec in this module no longer hashes to its pin."""
    actual = frozen_spec_digest()
    if actual != FROZEN_SPEC_SHA256:
        return [f"frozen spec digest {actual} != pinned {FROZEN_SPEC_SHA256} (a frozen-spec edit needs review)"]
    return []


def frozen_hypothesis(hypothesis_id: Any) -> dict[str, Any] | None:
    return (FROZEN_SPEC.get("hypotheses") or {}).get(hypothesis_id) if isinstance(hypothesis_id, str) else None


def frozen_drift_errors(manifest: dict[str, Any]) -> list[str]:
    """Where the manifest's copy of a supported hypothesis disagrees with the frozen spec.

    Checked for every supported hypothesis, admitted or not, so the manifest cannot drift
    silently. Fields the manifest omits are not drift here; admission requires them.
    """
    errs: list[str] = []
    hyps = {h.get("id"): h for h in manifest.get("hypotheses") or [] if isinstance(h, dict)}
    for hid, spec in (FROZEN_SPEC.get("hypotheses") or {}).items():
        hyp = hyps.get(hid)
        if hyp is None:
            errs.append(f"{hid}: supported hypothesis missing from the manifest")
        else:
            errs += hypothesis_drift_errors(hid, hyp, spec)
    return errs


def hypothesis_drift_errors(hid: str, hyp: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    """Where one manifest hypothesis disagrees with its frozen spec (omitted fields are not drift)."""
    errs: list[str] = []
    for key in _FROZEN_SCALARS:
        if key in hyp and hyp[key] != spec[key]:
            errs.append(f"{hid}: manifest {key} {hyp[key]!r} differs from the frozen spec {spec[key]!r}")
    req = hyp.get("required_inputs")
    if isinstance(req, list) and sorted(map(str, req)) != sorted(spec["required_inputs"]):
        errs.append(f"{hid}: manifest required_inputs {req!r} differ from the frozen required inputs "
                    f"{spec['required_inputs']!r}")
    gaps = hyp.get("blocking_gaps")
    if isinstance(gaps, list):
        for gid in spec["blocking_gaps"]:
            if gid not in gaps:
                errs.append(f"{hid}: manifest blocking_gaps omits frozen blocking gap {gid}")
    return errs


def admission_statement(manifest: dict[str, Any], hypothesis_id: str) -> bytes:
    """The exact bytes each principal signs to admit one hypothesis."""
    hyp = next((h for h in manifest.get("hypotheses") or [] if h.get("id") == hypothesis_id), None)
    if hyp is None:
        raise KeyError(f"no hypothesis {hypothesis_id!r}")
    admission = hyp.get("admission") if isinstance(hyp.get("admission"), dict) else {}
    reviewer = manifest.get("reviewer") if isinstance(manifest.get("reviewer"), dict) else {}
    return canonical({
        "contract": manifest.get("contract"),
        "contract_body_sha256": M.D0_CONTRACT_BODY_SHA256,
        "frozen_spec_sha256": FROZEN_SPEC_SHA256,
        "hypothesis": hypothesis_id,
        "admission_signed_at": admission.get("signed_at"),
        "owner_signed_at": manifest.get("signed_at"),
        "reviewer_signed_at": reviewer.get("signed_at"),
        "manifest_sha256": manifest_digest(manifest),
        "namespace": NAMESPACE,
    }) + b"\n"


def file_receipt_errors(rec: Any, repo_root: Path, label: str) -> list[str]:
    if not isinstance(rec, dict):
        return [f"{label}: not a file receipt (needs {{path, sha256}}; a string is not a receipt)"]
    path, want = rec.get("path"), rec.get("sha256")
    if not (isinstance(want, str) and _SHA256.fullmatch(want)):
        return [f"{label}: sha256 is not a lowercase hex digest"]
    if not isinstance(path, str) or not path or Path(path).is_absolute():
        return [f"{label}: path must be repository-relative"]
    root = Path(repo_root).resolve()
    full = (root / path).resolve()
    if root not in full.parents:
        return [f"{label}: path escapes the repository"]
    if not full.is_file():
        return [f"{label}: file {path} does not exist"]
    if hashlib.sha256(full.read_bytes()).hexdigest() != want:
        return [f"{label}: file {path} does not hash to its recorded sha256"]
    return []


def receipt_json(rec: Any, repo_root: Path, label: str) -> tuple[dict[str, Any] | None, list[str]]:
    """A verified file receipt's content as a JSON object; (None, reasons) when it is not one."""
    errs = file_receipt_errors(rec, repo_root, label)
    if errs:
        return None, errs
    try:
        body = json.loads((Path(repo_root).resolve() / rec["path"]).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, [f"{label}: file {rec['path']} is not JSON ({exc.__class__.__name__})"]
    if not isinstance(body, dict):
        return None, [f"{label}: file {rec['path']} is not a JSON object"]
    return body, []


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def g15_exception_errors(body: dict[str, Any], repo_root: Path, label: str) -> list[str]:
    """The typed price-only exception that closes G15 (frozen in ``FROZEN_SPEC['g15_closure']``)."""
    req = FROZEN_SPEC["g15_closure"]
    errs: list[str] = []
    if body.get("exception") != req["exception"]:
        errs.append(f"{label}: exception must be exactly {req['exception']!r}")
    for key in req["non_empty_strings"]:
        if not _non_empty_str(body.get(key)):
            errs.append(f"{label}: {key} must be a non-empty string")
    for key in req["non_empty_string_lists"]:
        val = body.get(key)
        if not (isinstance(val, list) and val and all(_non_empty_str(v) for v in val)):
            errs.append(f"{label}: {key} must be a non-empty list of non-empty strings")
    for key in req["non_empty_receipt_lists"]:
        val = body.get(key)
        if not (isinstance(val, list) and val):
            errs.append(f"{label}: {key} must be a non-empty list of file receipts")
        else:
            for n, rec in enumerate(val):
                errs += file_receipt_errors(rec, repo_root, f"{label}: {key}[{n}]")
    for key in req["exactly_true"]:
        if body.get(key) is not True:
            errs.append(f"{label}: {key} must be exactly true (a number or string is not a boolean)")
    for key in req["exactly_false"]:
        if body.get(key) is not False:
            errs.append(f"{label}: {key} must be exactly false")
    if body.get("qualified_by") != req["qualified_by"]:
        errs.append(f"{label}: qualified_by must be {req['qualified_by']!r}")
    return errs


def gap_closure_errors(gid: str, gap: dict[str, Any], repo_root: Path, admitted_on: date,
                       today: date) -> tuple[str | None, list[str]]:
    """Why gap ``gid`` is not closed by typed evidence; also the closure evidence sha256 when it is.

    A closure is ``gap.closure = {"state": "CLOSED", "closed_at": "YYYY-MM-DD", "evidence":
    <file receipt>}``. The evidence file is JSON ``{"schema": "d0-gap-closure/1", "gap": gid,
    "state": "CLOSED", "closed_at": <same date>, ...}``; for G15 it also carries the typed
    price-only exception. The ``status`` string is never read: no prefix closes a gap. The
    closure must predate (or equal) the admission date, so it is frozen before the look.
    """
    label = f"blocking gap {gid} is not closed"
    closure = gap.get("closure")
    if not isinstance(closure, dict):
        return None, [f"{label}: no typed closure (a status string never closes a gap)"]
    errs: list[str] = []
    if closure.get("state") != CLOSED:
        errs.append(f"{label}: closure.state must be exactly {CLOSED!r}")
    closed_at = parse_date(closure.get("closed_at"))
    if closed_at is None:
        errs.append(f"{label}: closure.closed_at is not a real YYYY-MM-DD date")
    elif closed_at > today:
        errs.append(f"{label}: closure.closed_at is in the future")
    elif closed_at > admitted_on:
        errs.append(f"{label}: closure.closed_at is after the admission date (not frozen before the look)")
    body, rerrs = receipt_json(closure.get("evidence"), repo_root, f"{label}: closure evidence")
    if rerrs:
        return None, errs + rerrs
    assert body is not None
    if body.get("schema") != GAP_CLOSURE_SCHEMA:
        errs.append(f"{label}: closure evidence schema must be {GAP_CLOSURE_SCHEMA!r}")
    if body.get("gap") != gid:
        errs.append(f"{label}: closure evidence is for gap {body.get('gap')!r}, not {gid}")
    if body.get("state") != CLOSED:
        errs.append(f"{label}: closure evidence state must be exactly {CLOSED!r}")
    if body.get("closed_at") != closure.get("closed_at"):
        errs.append(f"{label}: closure evidence closed_at differs from the manifest's")
    if gid == "G15":
        errs += g15_exception_errors(body, repo_root, f"{label}: G15 exception")
    return (None if errs else closure["evidence"]["sha256"]), errs


def coverage_receipt_errors(hid: str, spec: dict[str, Any], cov: dict[str, Any], repo_root: Path,
                            today: date, g15_sha256: str | None, *, admitted_on: date | None = None,
                            reviewer_on: date | None = None, g15_closed_on: date | None = None) -> list[str]:
    """The coverage receipt's content must be the frozen count and scope, not only authentic bytes.

    The receipt file is JSON ``{"schema": "d0-coverage-receipt/1", "hypothesis": hid,
    "scope": {"window": <frozen scope key>, "range": <frozen window value>}, "unit": <frozen
    unit>, "inputs": <frozen required inputs>, "measured_n": <= coverage.measured_n>,
    "measured_at": "YYYY-MM-DD"}``. D1 receipts add ``cells`` (one per frozen horizon, each
    with the frozen cell floors met, and ``measured_n`` = the smallest top-quintile count) and
    ``g15_closure_sha256``, the evidence hash of the typed G15 closure it was measured under.

    Chronology: ``measured_at`` is no later than today, the admission date and the reviewer's
    signing date (the reviewer re-runs the count before signing, contract §8). For a judged
    historical D1 it is no earlier than the G15 closure: the count is measured under an
    exception that was already closed, so naming the right closure hash is not enough.
    """
    label = f"{hid}: coverage receipt"
    body, errs = receipt_json(cov.get("receipt"), repo_root, label)
    if body is None:
        return errs
    if body.get("schema") != COVERAGE_RECEIPT_SCHEMA:
        errs.append(f"{label}: schema must be {COVERAGE_RECEIPT_SCHEMA!r}")
    if body.get("hypothesis") != hid:
        errs.append(f"{label}: measured for hypothesis {body.get('hypothesis')!r}, not {hid}")
    want_scope = {"window": spec["scope"], "range": spec["window"][spec["scope"]]}
    if body.get("scope") != want_scope:
        errs.append(f"{label}: scope {body.get('scope')!r} is not the frozen scope {want_scope!r}")
    if body.get("unit") != spec["coverage_unit"]:
        errs.append(f"{label}: unit {body.get('unit')!r} is not the frozen unit {spec['coverage_unit']!r}")
    inputs = body.get("inputs")
    if not (isinstance(inputs, list) and sorted(map(str, inputs)) == sorted(spec["required_inputs"])):
        errs.append(f"{label}: inputs {inputs!r} are not the frozen required inputs")
    n = body.get("measured_n")
    if not _is_int(n):
        errs.append(f"{label}: measured_n must be an integer")
    else:
        if n != cov.get("measured_n"):
            errs.append(f"{label}: receipt count {n} does not match coverage measured_n {cov.get('measured_n')!r}")
        if n < spec["n_floor"]:
            errs.append(f"{label}: receipt count {n} is below the frozen sample floor {spec['n_floor']}")
    measured_at = parse_date(body.get("measured_at"))
    if measured_at is None:
        errs.append(f"{label}: measured_at is not a real YYYY-MM-DD date")
    else:
        if measured_at > today:
            errs.append(f"{label}: measured_at is in the future")
        if admitted_on is not None and measured_at > admitted_on:
            errs.append(f"{label}: measured_at is after the admission date (not measured before the look)")
        if reviewer_on is not None and measured_at > reviewer_on:
            errs.append(f"{label}: measured_at is after the reviewer's signature (contract §8: the reviewer "
                        "re-runs the count before signing)")
        if spec["judged_historical_d1"] and g15_closed_on is not None and measured_at < g15_closed_on:
            errs.append(f"{label}: measured_at is before the G15 closure (a judged historical D1 count must "
                        "be measured under the closed price-only exception)")
    if spec["cells"]:
        cells = body.get("cells")
        if not (isinstance(cells, dict) and sorted(cells) == sorted(spec["cells"])):
            errs.append(f"{label}: cells must be exactly the frozen horizons {spec['cells']!r}")
        else:
            tops: list[int] = []
            for cell in spec["cells"]:
                rec = cells[cell] if isinstance(cells[cell], dict) else {}
                for key, floor in spec["cell_floors"].items():
                    val = rec.get(key)
                    if not _is_int(val):
                        errs.append(f"{label}: cell {cell} {key} must be an integer")
                    elif val < floor:
                        errs.append(f"{label}: cell {cell} {key} {val} is below the frozen floor {floor}")
                if _is_int(rec.get(spec["coverage_unit"])):
                    tops.append(rec[spec["coverage_unit"]])
            if _is_int(n) and len(tops) == len(spec["cells"]) and n != min(tops):
                errs.append(f"{label}: measured_n {n} is not the smallest cell count {min(tops)}")
    if spec["judged_historical_d1"]:
        if g15_sha256 is None:
            errs.append(f"{label}: a judged historical D1 result requires the frozen G15 closure")
        elif body.get("g15_closure_sha256") != g15_sha256:
            errs.append(f"{label}: g15_closure_sha256 does not name the frozen G15 closure evidence")
    return errs


def query_receipt_errors(rec: Any, repo_root: Path, label: str) -> list[str]:
    """The input's query receipt: snapshot bytes = receipt_sha256, query text = query_sha256."""
    if not isinstance(rec, dict):
        return [f"{label}: no query receipt"]
    snapshot = rec.get("snapshot")
    query = rec.get("query")
    if query is None and isinstance(snapshot, str) and snapshot.endswith(".result.json"):
        query = snapshot[: -len(".result.json")] + ".sql"
    return (file_receipt_errors({"path": snapshot, "sha256": rec.get("receipt_sha256")}, repo_root,
                                f"{label} snapshot")
            + file_receipt_errors({"path": query, "sha256": rec.get("query_sha256")}, repo_root,
                                  f"{label} query"))


def input_errors(inp: dict[str, Any], repo_root: Path) -> list[str]:
    iid = inp.get("id", "<no id>")
    errs: list[str] = []
    if inp.get("admission_status") != "ADMITTED":
        errs.append(f"{iid}: required input is not ADMITTED")
    miss = inp.get("missingness")
    if not (isinstance(miss, str) and miss.strip()) or re.search(r"\bun-?measured\b|\bnot measured\b", miss, re.I):
        errs.append(f"{iid}: missingness not measured")
    clocks = inp.get("clocks") if isinstance(inp.get("clocks"), dict) else {}
    errs += file_receipt_errors(clocks.get("first_availability_receipt"), repo_root,
                                f"{iid}: first-availability receipt")
    vis = clocks.get("durable_visibility")
    if not isinstance(vis, dict):
        errs.append(f"{iid}: durable visibility must be {{state, reason, receipt}}; null or text is never an exemption")
    else:
        state = vis.get("state")
        if state not in VISIBILITY_STATES:
            errs.append(f"{iid}: durable visibility state {state!r} not in {sorted(VISIBILITY_STATES)}")
        elif state not in VISIBILITY_OK:
            errs.append(f"{iid}: durable visibility applies but is not qualified")
        if not (isinstance(vis.get("reason"), str) and vis["reason"].strip()):
            errs.append(f"{iid}: durable visibility has no reason")
        errs += file_receipt_errors(vis.get("receipt"), repo_root, f"{iid}: durable-visibility receipt")
    errs += query_receipt_errors(inp.get("receipts"), repo_root, f"{iid}: query receipt")
    return errs


def reviewer_signed_on(manifest: dict[str, Any]) -> date | None:
    """The reviewer's signing date, only when ``reviewer.signed`` is exactly true and the date is real."""
    reviewer = manifest.get("reviewer") if isinstance(manifest.get("reviewer"), dict) else {}
    return parse_date(reviewer.get("signed_at")) if reviewer.get("signed") is True else None


def _owner_errors(manifest: dict[str, Any], repo_root: Path, today: date) -> list[str]:
    errs = list(M.contract_pin_errors(manifest, repo_root=repo_root))
    if (manifest.get("research_owner") or {}).get("signed") is not True:
        errs.append("research owner has not signed (research_owner.signed must be exactly true)")
    owner_on = parse_date(manifest.get("signed_at"))
    if owner_on is None:
        errs.append("manifest signed_at is not a real YYYY-MM-DD date")
    elif owner_on > today:
        errs.append("manifest signed_at is in the future")
    reviewer = manifest.get("reviewer") if isinstance(manifest.get("reviewer"), dict) else {}
    signed = reviewer.get("signed")
    # type check, not equality: 0 == False and 1 == True, so `in (True, False)` let 0 and 1 through
    if type(signed) is not bool:
        errs.append("reviewer.signed must be exactly true or false (a number or string is not a boolean)")
    elif signed is not True:
        errs.append("the reviewer has not signed the manifest (contract §8: reviewer.signed must be exactly true)")
    else:
        if reviewer.get("manifest_sha256") != manifest_digest(manifest):
            errs.append("reviewer.manifest_sha256 does not equal the manifest digest")
        reviewer_on = parse_date(reviewer.get("signed_at"))
        if reviewer_on is None:
            errs.append("reviewer.signed_at is not a real YYYY-MM-DD date")
        else:
            if reviewer_on > today:
                errs.append("reviewer.signed_at is in the future")
            if owner_on is not None and reviewer_on < owner_on:
                errs.append("reviewer.signed_at is before the owner's signature (contract signing order)")
    return errs


_SIGNER_OPTIONS = f'namespaces="{NAMESPACE}"'
_KEY_TYPES = {"ssh-ed25519", "ecdsa-sha2-nistp256", "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521", "ssh-rsa",
              "sk-ssh-ed25519@openssh.com", "sk-ecdsa-sha2-nistp256@openssh.com"}


def parse_signers(signers: Path) -> tuple[dict[str, str], list[str]]:
    """Strictly parse ``allowed_signers`` into {principal: its one line}, with every reason it is unusable.

    Two-party control: each line names exactly one known principal (no comma lists, no
    wildcards), carries exactly the option ``namespaces="grid-d0-admission"`` (so no
    ``cert-authority``, validity or other option), and a plain public key. Each principal has
    exactly one line, and no key may appear under two principals.
    """
    if not signers.is_file():
        return {}, ["allowed_signers file missing"]
    lines: dict[str, str] = {}
    keys: dict[str, str] = {}
    errs: list[str] = []
    for n, raw in enumerate(signers.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 4:
            errs.append(f"allowed_signers line {n}: expected <principal> {_SIGNER_OPTIONS} <key-type> <key>")
            continue
        principal, options, ktype, key = parts[0], parts[1], parts[2], parts[3]
        if principal not in REQUIRED_PRINCIPALS:
            errs.append(f"allowed_signers line {n}: principal {principal!r} is not one of {list(REQUIRED_PRINCIPALS)}"
                        " (no lists or wildcards)")
            continue
        if options != _SIGNER_OPTIONS:
            errs.append(f"allowed_signers line {n}: options must be exactly {_SIGNER_OPTIONS}")
            continue
        if ktype not in _KEY_TYPES:
            errs.append(f"allowed_signers line {n}: key type {ktype!r} is not a plain public key type")
            continue
        if principal in lines:
            errs.append(f"allowed_signers: principal {principal!r} has more than one key")
            continue
        if key in keys:
            errs.append(f"allowed_signers: the same key is listed for {keys[key]!r} and {principal!r}")
            continue
        lines[principal], keys[key] = " ".join(parts), principal
    return lines, errs


def verify_signature(statement: bytes, sig: Path, principal: str, signers: Path) -> str | None:
    """None when ``sig`` is a valid signature of ``statement`` by ``principal``'s one key; else why not."""
    exe = shutil.which("ssh-keygen")
    if exe is None:
        return "ssh-keygen not found; signatures cannot be verified"
    if not sig.is_file():
        return f"signature {sig.name} missing"
    lines, errs = parse_signers(signers)
    if errs:
        return "; ".join(errs)
    if principal not in lines:
        return f"principal {principal!r} has no key in allowed_signers"
    with tempfile.TemporaryDirectory() as tmp:  # verify against this principal's line only
        only = Path(tmp) / "allowed_signers"
        only.write_text(lines[principal] + "\n", encoding="utf-8")
        try:
            run = subprocess.run([exe, "-Y", "verify", "-f", str(only), "-I", principal, "-n", NAMESPACE,
                                  "-s", str(sig)], input=statement, capture_output=True, timeout=20, check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            return f"ssh-keygen verify failed to run: {exc}"
    return None if run.returncode == 0 else f"signature {sig.name} does not verify for {principal!r}"


def hypothesis_errors(hyp: dict[str, Any], manifest: dict[str, Any], repo_root: Path,
                      today: date | None = None) -> list[str]:
    """Why one ADMITTED hypothesis is not authenticated, judged against its frozen spec."""
    hid = hyp.get("id", "<no id>")
    today = today or _today()
    errs = [f"{hid}: {e}" for e in frozen_spec_errors() + _owner_errors(manifest, repo_root, today)]
    spec = frozen_hypothesis(hid)
    if spec is None:
        errs.append(f"{hid}: not a supported hypothesis (no frozen admission spec; contract §5.1, §6.2)")
        spec = {"track": None, "window": None, "one_sided_level": None, "n_floor": None, "scope": None,
                "coverage_unit": None, "cells": [], "cell_floors": {}, "required_inputs": [],
                "blocking_gaps": [], "judged_historical_d1": False}
    else:
        errs += hypothesis_drift_errors(hid, hyp, spec)
    floor = hyp.get("n_floor")
    if not (_is_int(floor) and floor > 0):
        errs.append(f"{hid}: n_floor must be a positive integer")
    adm = hyp.get("admission") if isinstance(hyp.get("admission"), dict) else {}
    when, owner_when = parse_date(adm.get("signed_at")), parse_date(manifest.get("signed_at"))
    if when is None:
        errs.append(f"{hid}: admission.signed_at is not a real YYYY-MM-DD date")
    elif when > today:
        errs.append(f"{hid}: admission.signed_at is in the future")
    elif owner_when is not None and when < owner_when:
        errs.append(f"{hid}: admission.signed_at is before the owner's signature")
    reviewer_on = reviewer_signed_on(manifest)
    if reviewer_on is not None and reviewer_on > today:
        reviewer_on = None  # refused in _owner_errors; no later date is bounded by a future one
    if when is not None and reviewer_on is not None and when < reviewer_on:
        errs.append(f"{hid}: admission.signed_at is before the reviewer's signature (contract §8: the reviewer "
                    "signs the manifest before admission)")
    admitted_on = when if when is not None and when <= today else today

    # dependencies: the frozen required inputs, never the manifest's list
    inputs = {i.get("id"): i for i in manifest.get("inputs") or []}
    req = hyp.get("required_inputs")
    if not (isinstance(req, list) and req and all(isinstance(r, str) for r in req) and len(set(req)) == len(req)):
        errs.append(f"{hid}: required_inputs must be a non-empty list of unique input ids")
    else:
        errs += [f"{hid}: unknown dependency {rid!r}" for rid in req
                 if rid not in inputs and rid not in spec["required_inputs"]]
    for rid in spec["required_inputs"]:
        if rid not in inputs:
            errs.append(f"{hid}: unknown dependency {rid!r} (frozen required input not in the manifest)")
        else:
            errs += [f"{hid}: {e}" for e in input_errors(inputs[rid], repo_root)]

    # gaps: every frozen blocking gap plus any the manifest adds, each closed by typed evidence
    gaps = {g.get("id"): g for g in manifest.get("gaps") or [] if isinstance(g, dict)}
    blocking = hyp.get("blocking_gaps")
    listed: list[str] = []
    if not (isinstance(blocking, list) and all(isinstance(g, str) for g in blocking)):
        errs.append(f"{hid}: blocking_gaps must be a list of gap ids (empty if none)")
    else:
        listed = list(blocking)
    g15_sha256: str | None = None
    g15_closed_on: date | None = None
    for gid in sorted(set(spec["blocking_gaps"]) | set(listed)):
        if gid not in gaps:
            errs.append(f"{hid}: unknown blocking gap {gid!r}")
            continue
        sha, gerrs = gap_closure_errors(gid, gaps[gid], repo_root, admitted_on, today)
        errs += [f"{hid}: {e}" for e in gerrs]
        if gid == "G15":
            g15_sha256 = sha
            if sha is not None:
                g15_closed_on = parse_date(gaps[gid]["closure"].get("closed_at"))

    # coverage: the measured count against the frozen floor, and the receipt's typed content
    cov = hyp.get("coverage") if isinstance(hyp.get("coverage"), dict) else None
    if cov is None:
        errs.append(f"{hid}: coverage not measured")
    else:
        n = cov.get("measured_n")
        if not _is_int(n):
            errs.append(f"{hid}: coverage measured_n must be an integer")
        elif _is_int(spec["n_floor"]) and n < spec["n_floor"]:
            errs.append(f"{hid}: coverage {n} is below the sample floor {spec['n_floor']} (frozen)")
        if spec["scope"] is not None:
            errs += coverage_receipt_errors(
                hid, spec, cov, repo_root, today, g15_sha256,
                admitted_on=when if when is not None and when <= today else None,
                reviewer_on=reviewer_on, g15_closed_on=g15_closed_on)
        else:
            errs += file_receipt_errors(cov.get("receipt"), repo_root, f"{hid}: coverage receipt")

    statement = admission_statement(manifest, hid)
    root = Path(repo_root)
    for principal in REQUIRED_PRINCIPALS:
        why = verify_signature(statement, root / SIGNATURE_DIR / f"{hid}.{principal}.sig", principal,
                               root / ALLOWED_SIGNERS)
        if why:
            errs.append(f"{hid}: {why}")
    return errs


def admission_errors(manifest: dict[str, Any], repo_root: Path = M.REPO, today: date | None = None,
                     accepted: bool | None = None) -> list[str]:
    """Why any ADMITTED label in the manifest is not authenticated (empty = none, or all verified)."""
    accepted = VERIFIER_ACCEPTED if accepted is None else accepted
    errs: list[str] = []
    hyps = [h for h in manifest.get("hypotheses") or [] if h.get("status") == "ADMITTED"]
    covered: set[str] = set()
    for h in hyps:
        hid = h.get("id")
        if accepted is not True:
            errs.append(f"{hid}: ADMITTED refused: the G14 verifier is not yet accepted by the reviewer")
        errs += hypothesis_errors(h, manifest, repo_root, today)
        spec = frozen_hypothesis(hid)  # frozen dependencies only; the manifest's list covers nothing
        if spec is not None:
            covered.update(spec["required_inputs"])
    for inp in manifest.get("inputs") or []:
        if inp.get("admission_status") == "ADMITTED":
            iid = inp.get("id")
            if accepted is not True:
                errs.append(f"{iid}: ADMITTED refused: the G14 verifier is not yet accepted by the reviewer")
            if iid not in covered:
                errs.append(f"{iid}: input ADMITTED without an ADMITTED hypothesis that requires it")
                errs += input_errors(inp, repo_root)
    return errs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", type=Path, default=M.REPO / M.MANIFEST_PATH)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("digest", help="print the manifest digest that signatures bind")
    st = sub.add_parser("statement", help="print the exact bytes to sign for one hypothesis")
    st.add_argument("hypothesis")
    sub.add_parser("verify", help="verify every ADMITTED label (same check as analysis.d0_manifest)")
    args = ap.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if args.cmd == "digest":
        print(manifest_digest(manifest))
        return 0
    if args.cmd == "statement":
        sys.stdout.buffer.write(admission_statement(manifest, args.hypothesis))
        return 0
    errs = admission_errors(manifest)
    for e in errs:
        print(e)
    print("OK" if not errs else f"{len(errs)} error(s)")
    return 1 if errs else 0


if __name__ == "__main__":
    sys.exit(main())
