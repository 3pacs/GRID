"""Operator input adapter — retrieves manually injected connections from DB.

Queries the actor_connections table for connections sourced from
the operator (injected via the spider inject endpoint).
Confidence tier: 4 (rumor — operator-supplied, unverified).
"""

from __future__ import annotations

import json
import sys
from typing import Any

from loguru import logger as log

from intelligence.spider.models import DiscoveredConnection


class OperatorInputAdapter:
    """Retrieve manually injected operator connections from PostgreSQL."""

    name = "operator_input"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        try:
            sys.path.insert(0, ".")
            from sqlalchemy import text as sql_text

            from db import get_engine

            engine = get_engine()

            query = sql_text("""
                SELECT
                    ac.actor_a,
                    COALESCE(actor_a.name, ac.actor_a) AS actor_a_name,
                    ac.actor_b,
                    COALESCE(actor_b.name, ac.actor_b) AS actor_b_name,
                    ac.relationship,
                    ac.evidence,
                    ac.strength
                FROM actor_connections ac
                LEFT JOIN actors actor_a ON actor_a.id = ac.actor_a
                LEFT JOIN actors actor_b ON actor_b.id = ac.actor_b
                WHERE (
                    actor_a.name ILIKE :name_pattern
                    OR actor_b.name ILIKE :name_pattern
                    OR ac.actor_a ILIKE :name_pattern
                    OR ac.actor_b ILIKE :name_pattern
                )
                  AND (
                    ac.evidence::text ILIKE '%operator%'
                    OR ac.relationship ILIKE 'operator_%'
                  )
                LIMIT 50
            """).bindparams(
                name_pattern=f"%{actor_name}%",
            )

            connections: list[DiscoveredConnection] = []
            with engine.connect() as conn:
                rows = conn.execute(query).fetchall()

            needle = actor_name.strip().lower()
            for row in rows:
                actor_a_id, actor_a_name, actor_b_id, actor_b_name, relationship, evidence_raw, strength_raw = row
                a_name = actor_a_name or actor_a_id or ""
                b_name = actor_b_name or actor_b_id or ""
                if needle in a_name.lower() or needle in str(actor_a_id).lower():
                    target = b_name
                elif needle in b_name.lower() or needle in str(actor_b_id).lower():
                    target = a_name
                else:
                    target = b_name

                relationship = relationship or "related_to"
                evidence_text = _safe_evidence_text(evidence_raw)
                strength = float(strength_raw) if strength_raw is not None else 0.5

                if not target or target.lower() == actor_name.lower():
                    continue

                connections.append(DiscoveredConnection(
                    target_name=target,
                    relationship=relationship,
                    strength=strength,
                    confidence_tier=4,
                    target_hint={"source": "operator"},
                    evidence=[{
                        "source": "operator_input",
                        "url": "",
                        "excerpt": evidence_text or f"Operator-injected: {actor_name} -> {relationship} -> {target}",
                        "raw_evidence": evidence_text,
                    }],
                ))

            log.debug("Operator input: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("Operator input adapter error for {a}: {e}", a=actor_name, e=str(exc))
            return []


def _safe_evidence_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except TypeError:
        return str(value)
