"""Offline evidence-quality checks, never a truth or investment-rating engine.

Callers must read/verify stored document bytes before supplying manifests. This
module verifies supplied text hashes, not remote authenticity or raw-file bytes.
"""
from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, localcontext

SCHEMA = "company-research-quality-v1"


def _time(value):
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO-8601 string with timezone")  # noqa: TRY004
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("invalid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return parsed.astimezone(UTC)


def evaluate_company_research(packet, *, as_of, max_age_days):
    """Evaluate a caller-defined coverage plan, with an explicit research cutoff.

    Unknown publication dates block point-in-time eligibility. Retrieval dates
    never substitute for publication dates. Freshness is publication age, not
    proof that the newest report has been collected. Returns JSON-safe output.
    Invalid container shapes raise ValueError; evidence defects become issues.
    """
    cutoff = _time(as_of)
    if not isinstance(packet, dict) or not isinstance(max_age_days, dict):
        raise ValueError("packet and max_age_days must be objects")  # noqa: TRY004
    company = packet.get("company_id")
    if not isinstance(company, str) or not company.strip():
        raise ValueError("company_id is required")
    required = packet.get("required_source_kinds")
    if (not isinstance(required, list) or not required
            or any(not isinstance(k, str) or not k for k in required)
            or len(set(required)) != len(required)):
        raise ValueError("required_source_kinds must be a nonempty unique string list")
    for kind, days in max_age_days.items():
        if not isinstance(kind, str) or type(days) is not int or days < 0:
            raise ValueError("freshness policy requires nonnegative integer days")
    documents, claims = packet.get("documents", []), packet.get("claims", [])
    if any(not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows)
           for rows in (documents, claims)):
        raise ValueError("documents and claims must be lists of objects")
    issues = []

    def issue(code, target, detail):
        issues.append({"code": code, "target": str(target), "detail": detail})

    indexed = defaultdict(list)
    for i, doc in enumerate(documents):
        indexed[str(doc.get("snapshot_id", ""))].append((i, doc))
    eligible = {}
    by_kind = defaultdict(list)
    for sid, entries in indexed.items():
        for i, doc in entries:
            target = f"documents[{i}]"
            before = len(issues)
            if not sid or len(entries) != 1:
                issue("snapshot_identity_invalid", target, "Missing or duplicate snapshot ID")
            if doc.get("company_id") != company:
                issue("company_mismatch", target, "Evidence belongs to a different or unknown entity")
            kind = doc.get("source_kind")
            if not isinstance(kind, str) or not kind:
                issue("source_kind_missing", target, "No source category")
                kind = "unknown"
            for field in ("source_url", "parser_version"):
                if not isinstance(doc.get(field), str) or not doc[field].strip():
                    issue("provenance_missing", target, field)
            for field in ("content_sha256", "text_sha256"):
                if not isinstance(doc.get(field), str) or not re.fullmatch(r"[0-9a-f]{64}", doc[field]):
                    issue("hash_missing_or_invalid", target, field)
            text = doc.get("text")
            if not isinstance(text, str) or not text.strip():
                issue("text_unavailable", target, "No digestible text")
            elif hashlib.sha256(text.encode("utf-8")).hexdigest() != doc.get("text_sha256"):
                issue("text_hash_mismatch", target, "Text does not match provenance")
            if doc.get("parse_status") != "parsed":
                issue("parse_incomplete", target, "Only fully parsed documents qualify")
            dates = {}
            for field in ("published_at", "retrieved_at"):
                try:
                    dates[field] = _time(doc.get(field))
                    if dates[field] > cutoff:
                        issue("after_cutoff", target, field)
                except ValueError:
                    issue("date_unknown_or_invalid", target, field)
            if len(dates) == 2 and dates["published_at"] > dates["retrieved_at"]:
                issue("date_order_invalid", target, "Publication is after retrieval")
            if kind not in max_age_days:
                issue("freshness_policy_missing", target, kind)
            elif "published_at" in dates and (cutoff - dates["published_at"]).total_seconds() > max_age_days[kind] * 86400:
                issue("stale", target, "Publication age exceeds explicit policy")
            valid = before == len(issues)
            by_kind[kind].append({"snapshot_id": sid, "eligible": valid})
            if valid:
                eligible[sid] = doc
    coverage = {}
    for kind in required:
        rows = by_kind[kind]
        status = "available" if any(r["eligible"] for r in rows) else "unusable" if rows else "missing"
        coverage[kind] = {"status": status, "snapshots": rows}
        if status != "available":
            issue("coverage_" + status, kind, "Required evidence is not eligible")
    if not claims:
        issue("claims_missing", "claims", "No assertions have been assessed")
    seen_ids = set()
    metric_groups = defaultdict(list)
    commitment_groups = defaultdict(list)
    for i, claim in enumerate(claims):
        target = f"claims[{i}]"
        cid = claim.get("id")
        if not isinstance(cid, str) or not cid or cid in seen_ids:
            issue("claim_identity_invalid", target, "Missing or duplicate claim ID")
        else:
            seen_ids.add(cid)
        if claim.get("kind") not in ("fact", "inference"):
            issue("claim_kind_invalid", target, "Explicit fact/inference label required")
        if not isinstance(claim.get("text"), str) or not claim["text"].strip():
            issue("claim_text_missing", target, "No assertion text")
        if claim.get("kind") == "inference" and not claim.get("reasoning"):
            issue("inference_reasoning_missing", target, "Explain reasoning separately from evidence")
        refs = claim.get("evidence")
        if not isinstance(refs, list) or not refs:
            issue("evidence_missing", target, "At least one source quote is required")
            refs = []
        for ref in refs:
            if not isinstance(ref, dict):
                issue("evidence_invalid", target, "Reference must be an object")
                continue
            doc = eligible.get(str(ref.get("snapshot_id", "")))
            quote = ref.get("quote")
            if doc is None:
                issue("evidence_ineligible", target, "Referenced snapshot unavailable or fails checks")
            elif not isinstance(quote, str) or not quote.strip() or quote not in doc["text"]:
                issue("quote_not_found", target, "Verbatim evidence quote is absent")
        metric = claim.get("metric")
        if metric is not None:
            fields = ("unit", "scale", "period_end", "fiscal_year", "fiscal_period", "scope", "basis")
            if any(claim.get(k) is None or claim.get(k) == "" for k in fields):
                issue("metric_context_missing", target, "Units, scale, fiscal period, scope and accounting basis required")
            try:
                if any(len(str(claim.get(k))) > 128 for k in ("value", "scale")):
                    raise InvalidOperation
                value = Decimal(str(claim.get("value")))
                scale = Decimal(str(claim.get("scale")))
                if (not value.is_finite() or not scale.is_finite() or scale <= 0
                        or abs(value.adjusted()) > 1000 or abs(scale.adjusted()) > 1000):
                    raise InvalidOperation
                if type(claim.get("value")) is bool or type(claim.get("scale")) is bool:
                    raise InvalidOperation
            except (InvalidOperation, ValueError):
                issue("metric_number_invalid", target, "Finite value and positive explicit scale required")
                continue
            try:
                end = datetime.strptime(claim.get("period_end", ""), "%Y-%m-%d").date()
                if end > cutoff.date():
                    issue("period_after_cutoff", target, "Reported period ends after research cutoff")
                if claim.get("period_type") == "duration":
                    start = datetime.strptime(claim.get("period_start", ""), "%Y-%m-%d").date()
                    if start > end:
                        raise ValueError
                elif claim.get("period_type") != "instant":
                    raise ValueError
            except (ValueError, TypeError):
                issue("period_invalid", target, "Explicit instant/duration and valid ordered dates required")
            key = tuple(str(claim.get(k)) for k in ("metric", "unit", "period_type", "period_start", "period_end", "scope", "basis"))
            with localcontext() as context:
                context.prec = 260  # Exact product for two bounded 128-character inputs.
                metric_groups[key].append((target, value * scale))
        if claim.get("commitment_id") is not None:
            commitment_groups[str(claim["commitment_id"])].append(target)
    for group in metric_groups.values():
        if len({value for _, value in group}) > 1:
            for target, _ in group:
                issue("metric_conflict", target, "Comparable claims disagree; no automatic winner")
    for group in commitment_groups.values():
        if len(group) > 1:
            for target in group:
                issue("commitment_repeated", target, "Same commitment repeated; do not sum as separate obligations")
    return {"schema": SCHEMA, "company_id": company, "as_of": as_of,
            "eligible_for_synthesis": not issues, "coverage": coverage,
            "issues": issues, "truth_assessed": False,
            "latest_source_confirmed": False}


def evaluate_archived_company_research(packet, archive_root, *, as_of, max_age_days):
    """Read verified intake snapshots before evaluation; no network or DB effects.

    Packet uses snapshot_ids instead of documents. Invalid archives raise
    ValueError/OSError rather than manufacturing empty or apparently good data.
    """
    from ingestion.company_documents import read_document

    if not isinstance(packet, dict) or "documents" in packet:
        raise ValueError("archive packet must use snapshot_ids, not documents")
    ids = packet.get("snapshot_ids")
    if not isinstance(ids, list) or len(ids) > 1000:
        raise ValueError("snapshot_ids must be a list of at most 1000 IDs")
    verified = dict(packet)
    verified.pop("snapshot_ids")
    verified["documents"] = [read_document(archive_root, sid) for sid in ids]
    return evaluate_company_research(verified, as_of=as_of, max_age_days=max_age_days)
