"""Immutable SEC Company Facts captures and deterministic normalized snapshots.

No network, database or filesystem work happens in this module. Current facts
are a projection; original source bytes remain available for later review.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

NORMALIZER_VERSION = "sec-financial-periods-v2"
MAX_SOURCE_BYTES = 32 * 1024 * 1024


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False,
                      default=lambda x: x.isoformat() if isinstance(x, (date, datetime)) else _unsupported(x))


def _unsupported(value: Any):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Nonfinite reconciliation amount")
        return str(value)
    raise TypeError(f"Unsupported snapshot value: {type(value).__name__}")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Capture timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate source JSON key: {key}")
        result[key] = value
    return result


@dataclass(frozen=True)
class FinancialSnapshot:
    snapshot_id: str
    actor_id: str
    cik: str
    request_started_at: datetime
    received_at: datetime
    source_url: str
    source_sha256: str
    normalized_sha256: str
    normalizer_version: str
    source_body: bytes
    normalized_json: str
    fx_json: str

    @property
    def rows(self) -> list[dict[str, Any]]:
        return json.loads(self.normalized_json)


def prepare_snapshot(*, source_body: bytes, actor_id: str, cik: str,
                     request_started_at: datetime, received_at: datetime,
                     fx_rates: dict[str, float]) -> FinancialSnapshot:
    """Admit a complete successful Company Facts response, not a row subset.

    This does not infer an issuer withdrawal from an omitted row. It records
    only that the metric is absent from this qualified current projection.
    """
    from ingestion.altdata.sec_xbrl_financials import (
        _collect_all_flows, _FLOW_MAPPINGS, _IFRS_TAG_MAP,
        _DEBT_PROCEEDS_TAG, _DEBT_REPAY_TAG, _IFRS_DEBT_PROCEEDS_TAG, _IFRS_DEBT_REPAY_TAG,
    )

    if not isinstance(source_body, bytes) or not 0 < len(source_body) <= MAX_SOURCE_BYTES:
        raise ValueError("Company Facts body must be nonempty bytes within the size bound")
    actor_id = actor_id.strip().lower()
    if not actor_id or len(actor_id) > 32 or not all(c.isalnum() or c in '.-' for c in actor_id):
        raise ValueError("Invalid actor identifier")
    if not str(cik).isdigit() or not 0 < int(cik) < 10**10:
        raise ValueError("Invalid SEC CIK")
    cik = str(int(cik)).zfill(10)
    started, received = _utc(request_started_at), _utc(received_at)
    if started > received:
        raise ValueError("Request start must not follow receipt")
    payload = json.loads(source_body, object_pairs_hook=_unique_object,
                         parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Nonfinite source JSON")))
    if not isinstance(payload, dict) or str(payload.get('cik', '')).lstrip('0') != str(int(cik)):
        raise ValueError("Source CIK does not match requested issuer")
    if not isinstance(payload.get('entityName'), str) or not payload['entityName'].strip():
        raise ValueError("Missing Company Facts entity name")
    facts = payload.get('facts')
    if not isinstance(facts, dict) or not any(isinstance(facts.get(t), dict) and facts[t]
                                             for t in ('us-gaap', 'ifrs-full')):
        raise ValueError("Missing recognized Company Facts taxonomy")
    mapped_tags = {
        'us-gaap': {tag for spec in _FLOW_MAPPINGS for tag in spec['tags']}
                   | {_DEBT_PROCEEDS_TAG, _DEBT_REPAY_TAG},
        'ifrs-full': {tag for tags in _IFRS_TAG_MAP.values() for tag in tags}
                     | {_IFRS_DEBT_PROCEEDS_TAG, _IFRS_DEBT_REPAY_TAG},
    }
    for taxonomy in ('us-gaap', 'ifrs-full'):
        namespace = facts.get(taxonomy, {})
        if not isinstance(namespace, dict):
            raise ValueError("Malformed Company Facts taxonomy")
        for tag_name, tag in namespace.items():
            if not isinstance(tag, dict) or not isinstance(tag.get('units'), dict) or not tag['units']:
                raise ValueError("Malformed Company Facts tag")
            for entries in tag['units'].values():
                if not isinstance(entries, list) or not entries:
                    raise ValueError("Malformed Company Facts units")
                for entry in entries:
                    if not isinstance(entry, dict):
                        raise ValueError("Malformed Company Facts entry")
                    value = entry.get('val')
                    if isinstance(value, bool) or not isinstance(value, (int, float)):
                        raise ValueError("Invalid Company Facts numeric value")
                    try:
                        finite = math.isfinite(value)
                        end = date.fromisoformat(entry['end'])
                        filed = date.fromisoformat(entry['filed'])
                        start = date.fromisoformat(entry['start']) if 'start' in entry else None
                    except (KeyError, TypeError, ValueError, OverflowError) as exc:
                        raise ValueError("Invalid Company Facts dates/value") from exc
                    if (not finite or (start is not None and start > end)
                            or not isinstance(entry.get('form'), str) or not entry['form']
                            or not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}', str(entry.get('accn', '')))):
                        raise ValueError("Invalid Company Facts filing context")
                    # Whole-company responses also contain forward-looking and
                    # instant non-flow concepts. Only our mapped duration flows
                    # require a start and a reporting end not after filing.
                    if tag_name in mapped_tags[taxonomy] and (start is None or filed < end):
                        raise ValueError("Invalid mapped financial-flow duration")
    fx_json = _json(fx_rates)
    rows = _collect_all_flows(payload, json.loads(fx_json))
    normalized_json = _json(sorted(rows, key=lambda r: (r['fiscal_period'], r['period_type'], r['flow_type'])))
    source_hash, normalized_hash = _sha(source_body), _sha(normalized_json.encode('utf-8'))
    identity = _json(dict(actor_id=actor_id, cik=cik, source_sha256=source_hash,
                         normalized_sha256=normalized_hash, fx_inputs=json.loads(fx_json),
                         normalizer_version=NORMALIZER_VERSION))
    return FinancialSnapshot(
        snapshot_id=_sha(identity.encode('utf-8')), actor_id=actor_id, cik=cik,
        request_started_at=started, received_at=received,
        source_url=f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
        source_sha256=source_hash, normalized_sha256=normalized_hash,
        normalizer_version=NORMALIZER_VERSION, source_body=source_body,
        normalized_json=normalized_json, fx_json=fx_json,
    )


def validate_snapshot(snapshot: FinancialSnapshot) -> None:
    """Recompute admission at the write boundary, detecting mutated artifacts."""
    expected = prepare_snapshot(source_body=snapshot.source_body, actor_id=snapshot.actor_id,
                                cik=snapshot.cik, request_started_at=snapshot.request_started_at,
                                received_at=snapshot.received_at, fx_rates=json.loads(snapshot.fx_json))
    if expected != snapshot:
        raise ValueError("Snapshot content or normalization identity mismatch")


def build_reconciliation_manifest(snapshot: FinancialSnapshot,
                                  legacy_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Read-only proposal; no SQL, connection, inferred old accession or writes."""
    validate_snapshot(snapshot)
    candidates = {(r['fiscal_period'], r['period_type'], r['flow_type']): r for r in snapshot.rows}
    entries = []
    seen = set()
    for old in legacy_rows:
        if (old.get('actor_id') != snapshot.actor_id or type(old.get('id')) is not int
                or old['id'] <= 0):
            raise ValueError("Every legacy row needs its exact actor and database row id")
        if old['id'] in seen:
            raise ValueError("Duplicate legacy row identity")
        seen.add(old['id'])
        key = (str(old.get('fiscal_period')), old.get('period_type'), old.get('flow_type'))
        candidate = candidates.get(key)
        if old.get('counterparty_id') not in (None, ''):
            disposition = 'outside_scope_counterparty'
        elif old.get('period_type') not in ('annual', 'quarter'):
            disposition = 'outside_scope_period_type'
        elif '[SEC-XBRL-PERIOD-V1]' in str(old.get('source_filing', '')):
            disposition = 'already_versioned_review_separately'
        elif not str(old.get('source_filing', '')).startswith(('10-K', '10-Q', '20-F', '40-F', '6-K')):
            disposition = 'outside_scope_source'
        elif candidate is None:
            disposition = 'review_unavailable'
        elif Decimal(str(old['amount_usd'])) != Decimal(str(candidate['amount_usd'])):
            disposition = 'review_value_difference'
        else:
            disposition = 'review_provenance_only'
        entries.append(dict(row_id=old['id'], expected_row_sha256=_sha(_json(old).encode('utf-8')),
                            before=old, disposition=disposition,
                            proposed_current=candidate if disposition.startswith('review_') else None))
    return dict(schema_version=1, mode='proposal_only_no_execution', snapshot_id=snapshot.snapshot_id,
                source_sha256=snapshot.source_sha256, source_url=snapshot.source_url,
                actor_id=snapshot.actor_id, entries=entries,
                required_followup=['approved compare-and-swap repair', 'preserve before-image evidence',
                                   'invalidate/recompute dependent TTM and reports', 'verify unavailable fields'],
                caveat='Legacy source labels cannot establish their original accession or duration.')
