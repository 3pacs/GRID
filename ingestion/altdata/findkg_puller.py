"""
FinDKG puller — Financial Dynamic Knowledge Graph.

Academic dataset mapping company relationships: supply chains,
competitive dynamics, partnerships, and corporate events.

Source: https://github.com/xiaohui-victor-li/FinDKG
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "findkg"

# Relationship types in FinDKG
REL_TYPES = {
    "supplier_of", "customer_of", "competitor_of", "partner_of",
    "subsidiary_of", "acquires", "invested_in", "spin_off",
}


class FinDKGPuller(BasePuller):
    """Pull financial knowledge graph relationships from FinDKG dataset."""

    SOURCE_NAME = "findkg"
    SOURCE_CONFIG = {
        "base_url": "https://github.com/xiaohui-victor-li/FinDKG",
        "cost_tier": "FREE",
        "latency_class": "BATCH",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 42,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def pull(self, data_path: str | None = None) -> dict[str, Any]:
        """Load FinDKG data from local files.

        The FinDKG dataset needs to be downloaded separately from GitHub.
        Place JSON/CSV files in data/findkg/.

        Args:
            data_path: Override path to FinDKG data files.

        Returns:
            Summary with relationship counts by type.
        """
        source_dir = Path(data_path) if data_path else DATA_DIR
        counts: dict[str, int] = {}

        # Look for JSON or JSONL files
        for json_file in sorted(source_dir.glob("*.json*")):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    if json_file.suffix == ".jsonl":
                        records = [json.loads(line) for line in f if line.strip()]
                    else:
                        records = json.load(f)
                        if isinstance(records, dict):
                            records = records.get("triples", records.get("data", [records]))

                for rec in records:
                    head = rec.get("head", rec.get("subject", ""))
                    tail = rec.get("tail", rec.get("object", ""))
                    rel = rec.get("relation", rec.get("predicate", ""))
                    ts = rec.get("timestamp", rec.get("date", ""))

                    if not head or not tail or not rel:
                        continue

                    rel_normalized = rel.lower().replace(" ", "_")
                    counts[rel_normalized] = counts.get(rel_normalized, 0) + 1

                    # Auto-discover actors from FinDKG triples
                    try:
                        from intelligence.actor_ingest import ingest_actor
                        ingest_actor(self.engine, head, "company", source="findkg")
                        ingest_actor(self.engine, tail, "company", source="findkg")
                    except Exception:
                        pass

                    with self.engine.begin() as conn:
                        self._insert_raw(
                            conn,
                            series_id=f"findkg:{head}:{rel_normalized}:{tail}",
                            obs_date=date.today(),
                            value=1.0,
                            raw_payload={
                                "head": head,
                                "tail": tail,
                                "relation": rel,
                                "timestamp": ts,
                                "source_file": json_file.name,
                            },
                        )

            except Exception as exc:
                log.warning("FinDKG file {f} failed: {e}", f=json_file.name, e=str(exc))

        total = sum(counts.values())
        log.info("FinDKG: {t} relationships loaded ({n} types)", t=total, n=len(counts))
        return {"total_relationships": total, "by_type": counts}
