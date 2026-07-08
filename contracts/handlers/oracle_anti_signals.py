"""Oracle anti-signal handler (SYNTH-28 / SYNTH-32 follow-up).

Closes the §7.3 ``CrossReferenceAnomaly`` route. When the cross_reference
engine emits an anomaly (government statistic vs. physical reality
disagreement OR a regulatory-events severity flag projected as the same
contract via ``statistic`` discriminator), this handler persists the row
into ``oracle_anti_signals`` so the next oracle scoring cycle can pick it
up via ``oracle.engine._find_anti_signals``.

Heavy logic — scoring, weighting, family routing — stays in
``oracle/engine.py``. This handler owns only the persistence write.

The handler MUST be non-fatal: any DB failure is logged at ``warning``
and swallowed so the contract bus keeps flowing.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from loguru import logger as log
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import CrossReferenceAnomaly


def _decimal_bind(value) -> str:
    """Serialize a Decimal-shaped value as a string for NUMERIC binding.

    NUMERIC columns preserve arbitrary precision; going through ``float()``
    would silently truncate cross-reference deltas past ~15 significant
    digits (PUNCH-LIST-2026-05-13 contracts/ [P2] line 94). Passing the
    string form + ``CAST(... AS NUMERIC)`` sidesteps DBAPI float coercion.
    """
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        return str(value)
    return str(Decimal(str(value)))


#: Severity floor — LOW anomalies are routed but not persisted so we don't
#: drown the anti-signal table in noise.
_MIN_PERSIST_SEVERITY: tuple[str, ...] = ("MEDIUM", "HIGH", "CRITICAL")


def on_cross_reference_anomaly(
    evt: "CrossReferenceAnomaly", *, engine: "Engine"
) -> None:
    """Persist a cross-reference anomaly for oracle anti-signal consumption.

    Schema assumption (best-effort, table is auto-created if missing):

        CREATE TABLE oracle_anti_signals (
            id BIGSERIAL PRIMARY KEY,
            event_id UUID NOT NULL UNIQUE,
            statistic TEXT NOT NULL,
            severity TEXT NOT NULL,
            official_value NUMERIC,
            reality_proxy_value NUMERIC,
            confidence_delta DOUBLE PRECISION,
            evidence_links JSONB,
            producer_module TEXT,
            correlation_id UUID,
            created_at TIMESTAMPTZ DEFAULT now()
        );

    Idempotency: insert is keyed on ``event_id``; replayed events are
    no-ops via ON CONFLICT DO NOTHING.
    """
    severity = (getattr(evt, "severity", "") or "").upper()
    if severity not in _MIN_PERSIST_SEVERITY and severity != "LOW":
        log.debug(
            "oracle_anti_signals.on_cross_reference_anomaly: unknown "
            "severity={s}, skipping",
            s=severity,
        )
        return

    if severity not in _MIN_PERSIST_SEVERITY:
        # LOW severity — count the dispatch (observability), no DB write.
        log.debug(
            "oracle_anti_signals.on_cross_reference_anomaly: LOW severity "
            "skip statistic={st}", st=getattr(evt, "statistic", "?"),
        )
        return

    statistic = getattr(evt, "statistic", "") or ""
    event_id = str(getattr(evt, "event_id", ""))
    producer = getattr(evt, "producer_module", "") or ""
    correlation_id = str(getattr(evt, "correlation_id", "") or "") or None
    evidence_links = list(getattr(evt, "evidence_links", None) or [])

    try:
        with engine.begin() as conn:
            # Auto-create table — handler must never block on a missing
            # schema in dev/test environments.
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS oracle_anti_signals (
                        id BIGSERIAL PRIMARY KEY,
                        event_id UUID NOT NULL UNIQUE,
                        statistic TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        official_value NUMERIC,
                        reality_proxy_value NUMERIC,
                        confidence_delta DOUBLE PRECISION,
                        evidence_links JSONB,
                        producer_module TEXT,
                        correlation_id UUID,
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )

            conn.execute(
                text(
                    """
                    INSERT INTO oracle_anti_signals (
                        event_id, statistic, severity,
                        official_value, reality_proxy_value,
                        confidence_delta, evidence_links,
                        producer_module, correlation_id
                    ) VALUES (
                        CAST(:eid AS UUID), :stat, :sev,
                        CAST(:ov AS NUMERIC), CAST(:rv AS NUMERIC),
                        :cd, CAST(:el AS JSONB),
                        :prod, CAST(:cid AS UUID)
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    """
                ),
                {
                    "eid": event_id,
                    "stat": statistic,
                    "sev": severity,
                    "ov": _decimal_bind(getattr(evt, "official_value", None)),
                    "rv": _decimal_bind(getattr(evt, "reality_proxy_value", None)),
                    "cd": float(getattr(evt, "confidence_delta", 0.0) or 0.0),
                    "el": json.dumps(evidence_links),
                    "prod": producer,
                    "cid": correlation_id,
                },
            )
    except Exception as exc:
        log.warning(
            "oracle_anti_signals.on_cross_reference_anomaly({s}): {e}",
            s=statistic, e=str(exc),
        )
        return

    log.info(
        "oracle_anti_signals.on_cross_reference_anomaly: stat={s} sev={sv} "
        "evt={eid}",
        s=statistic, sv=severity, eid=event_id,
    )
