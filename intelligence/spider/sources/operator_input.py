"""Operator input adapter — retrieves manually injected connections from DB.

Queries the actor_connections table for connections sourced from
the operator (injected via the spider inject endpoint).
Confidence tier: 4 (rumor — operator-supplied, unverified).
"""

from __future__ import annotations

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
                    target_name,
                    relationship,
                    evidence,
                    strength,
                    notes
                FROM actor_connections
                WHERE source = :source
                  AND (
                      source_name ILIKE :name_pattern
                      OR target_name ILIKE :name_pattern
                  )
                LIMIT 50
            """).bindparams(
                source="operator",
                name_pattern=f"%{actor_name}%",
            )

            connections: list[DiscoveredConnection] = []
            with engine.connect() as conn:
                rows = conn.execute(query).fetchall()

            for row in rows:
                target = row[0] if row[0] else ""
                relationship = row[1] if row[1] else "related_to"
                evidence_raw = row[2] if row[2] else ""
                strength = float(row[3]) if row[3] else 0.5
                notes = row[4] if row[4] else ""

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
                        "excerpt": notes or f"Operator-injected: {actor_name} → {relationship} → {target}",
                        "raw_evidence": evidence_raw,
                    }],
                ))

            log.debug("Operator input: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("Operator input adapter error for {a}: {e}", a=actor_name, e=str(exc))
            return []
