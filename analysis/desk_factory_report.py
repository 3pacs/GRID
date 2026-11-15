"""Pure factory-report adapter. No data reads, execution, promotion or writes.

The caller supplies report bytes and an independently obtained expected file
hash. Matching bytes is integrity, not verification of the frozen discovery or
ledger. Receipt clocks describe the report observation/publication, never the
underlying market data freshness. Unknown report clocks remain withheld.
"""
import hashlib
import json
import math
import re
from datetime import datetime, timedelta

from analysis.desk_evidence import DeskEvidence, EvidenceKind, evidence_card

MAX_REPORT_BYTES = 2_000_000


def _identity(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError("bounded nonempty identity required")
    return value


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("lowercase sha256 required")
    return value


def _count(value):
    if type(value) is not int or value < 0:
        raise ValueError("nonnegative integer required")
    return value


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("non-finite JSON number")


def factory_report_cards(
    raw: bytes, *, expected_sha256: str, receipt_id: str,
    observed_at: datetime | None, available_at: datetime | None,
    as_of: datetime, max_age: timedelta,
) -> dict:
    """Adapt one immutable native report; missing/malformed evidence fails closed.

    One summary is emitted even when there are zero survivors. Ranking is only
    the producer's displayed subset; selected IDs absent from it remain explicit.
    Trial counts and historical fires are not forward samples. All labels remain
    development-only. The caller must not obtain expected_sha256 by merely
    hashing the same untrusted payload.
    """
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_REPORT_BYTES:
        raise ValueError("bounded nonempty report bytes required")
    actual = hashlib.sha256(raw).hexdigest()
    if actual != _sha(expected_sha256):
        raise ValueError("report hash mismatch")
    receipt_id = _identity(receipt_id)
    try:
        rep = json.loads(raw, object_pairs_hook=_object, parse_constant=_constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError, OverflowError) as exc:
        raise ValueError("invalid report JSON") from exc
    if not isinstance(rep, dict):
        raise ValueError("report object required")
    if rep.get("state") != "INTRADAY_DEV_RANKING_ONLY" or rep.get("promotion_allowed") is not False:
        raise ValueError("development-only state required")
    run_id = _identity(rep.get("run_id"))
    frozen_sha = _sha(rep.get("frozen_sha256"))
    declared = _count(rep.get("trial_count"))
    tested = _count(rep.get("tested"))
    untestable = _count(rep.get("untestable"))
    if tested + untestable != declared:
        raise ValueError("inconsistent trial counts")
    selected = rep.get("selected")
    ranking = rep.get("ranking")
    if not isinstance(selected, list) or not isinstance(ranking, list):
        raise ValueError("selection and ranking lists required")
    selected_ids = [_identity(v) for v in selected]
    selected_set = set(selected_ids)
    if len(selected_set) != len(selected_ids) or len(selected_ids) > tested or len(ranking) > tested:
        raise ValueError("inconsistent selection or ranking counts")

    def card(identity, samples):
        return evidence_card(
            DeskEvidence(identity, EvidenceKind.DEVELOPMENT, receipt_id,
                         observed_at, available_at, samples),
            as_of=as_of, max_age=max_age,
        )

    summary = card(run_id, 0)
    rows = []
    seen = set()
    for row in ranking:
        if not isinstance(row, dict):
            raise ValueError("ranking object required")
        trial_id = _identity(row.get("trial_id"))
        if trial_id in seen:
            raise ValueError("duplicate ranked trial")
        seen.add(trial_id)
        fires = _count(row.get("n"))
        if type(row.get("selected")) is not bool or row["selected"] != (trial_id in selected_set):
            raise ValueError("ranking selection mismatch")
        metrics = {}
        for key in ("r", "net_dollars", "t", "p", "adjusted_p"):
            value = row.get(key)
            try:
                finite = type(value) in (int, float) and math.isfinite(value)
            except (OverflowError, RecursionError) as exc:
                raise ValueError("finite numeric ranking metric required") from exc
            if not finite:
                raise ValueError("finite numeric ranking metric required")
            if key in ("p", "adjusted_p") and not 0 <= value <= 1:
                raise ValueError("invalid p-value")
            metrics[key] = value
        rows.append({
            "trial_id": trial_id,
            "family": _identity(row.get("family")),
            "feature": _identity(row.get("feature")),
            "development_fires": fires,
            "forward_samples": 0,
            "selected_for_development_followup": row["selected"],
            "reported_metrics": metrics,
            "evidence": card(trial_id, 0),
        })
    if len(ranking) == tested and seen & selected_set != selected_set:
        raise ValueError("complete ranking missing selected trial")
    return {
        "schema": "desk-factory-report-v1",
        "run_id": run_id,
        "report_sha256": actual,
        "frozen_sha256": frozen_sha,
        "integrity": "report_bytes_match_expected_hash",
        "frozen_and_ledger_verified": False,
        "source_market_data_freshness": "not_assessed",
        "clock_scope": "report_observation_and_availability",
        "target_unit": _identity(rep.get("target_unit")),
        "promotion_allowed": False,
        "summary": summary,
        "declared_trials": declared,
        "tested_trials": tested,
        "untestable_trials": untestable,
        "selected_count": len(selected_ids),
        "outcome": ("no_testable_trials" if tested == 0 else
                    "development_candidates_only" if selected_ids else
                    "no_selected_candidates"),
        "selected_trial_ids": selected_ids,
        "selected_ids_verified_against_discovery": False,
        "selected_not_in_ranking_status": "producer_claim_only",
        "ranking_complete": len(ranking) == tested,
        "selected_not_in_ranking": [v for v in selected_ids if v not in seen],
        "ranking": rows,
    }
