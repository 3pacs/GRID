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
from sqlalchemy import text
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
        "latency_class": "MONTHLY",  # was BATCH (not in source_catalog_latency_class_check); monthly re-download
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 42,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def _triple_stored(self, conn: Any, series_id: str) -> bool:
        """True when this triple already has a SUCCESS raw_series row.

        FinDKG is a static dataset: a triple loaded once is not a new
        observation when the same file is re-read next month, and a triple
        repeated inside one file would collide on ``uq_raw_series_composite``
        (same series_id, obs_date and transaction pull_timestamp). Probes
        ``idx_raw_series_series_obs`` (series_id leads), not a table scan.
        """
        row = conn.execute(
            text(
                "SELECT 1 FROM raw_series WHERE series_id = :sid AND source_id = :src "
                "AND pull_status = 'SUCCESS' LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id},
        ).fetchone()
        return row is not None

    @staticmethod
    def _read_records(json_file: Path) -> list[Any]:
        with open(json_file, "r", encoding="utf-8") as f:
            if json_file.suffix == ".jsonl":
                return [json.loads(line) for line in f if line.strip()]
            records = json.load(f)
        if isinstance(records, dict):
            records = records.get("triples", records.get("data", [records]))
        if not isinstance(records, list):
            raise TypeError(f"expected a list of triples, got {type(records).__name__}")
        return records

    def pull(self, data_path: str | None = None) -> dict[str, Any]:
        """Load FinDKG data from local files.

        The FinDKG dataset needs to be downloaded separately from GitHub.
        Place JSON/CSV files in data/findkg/.

        Args:
            data_path: Override path to FinDKG data files.

        Returns:
            The scheduler write contract (``smart_scheduler._classify_outcome``):
            ``status`` and ``rows_inserted`` (raw_series rows counted after
            each file's transaction committed; triples already stored are
            not re-inserted), plus ``total_relationships`` (valid triples
            read) and ``by_type``. No data files is SKIPPED with the reason;
            every file failing is FAILED; some failing is PARTIAL.
        """
        source_dir = Path(data_path) if data_path else DATA_DIR
        counts: dict[str, int] = {}
        files = sorted(source_dir.glob("*.json*"))
        if not files:
            reason = f"no FinDKG data files in {source_dir} (dataset is downloaded separately)"
            log.warning("FinDKG skipped: {r}", r=reason)
            return {
                "status": "SKIPPED",
                "rows_inserted": 0,
                "skipped_reason": reason,
                "total_relationships": 0,
                "by_type": counts,
            }

        rows_inserted = 0
        already_stored = 0
        malformed = 0
        file_errors: list[str] = []
        obs_date = date.today()

        for json_file in files:
            try:
                records = self._read_records(json_file)
                file_counts: dict[str, int] = {}
                file_inserted = 0
                file_stored = 0
                file_malformed = 0
                with self.engine.begin() as conn:
                    for rec in records:
                        if not isinstance(rec, dict):
                            file_malformed += 1
                            continue
                        head = rec.get("head", rec.get("subject", ""))
                        tail = rec.get("tail", rec.get("object", ""))
                        rel = rec.get("relation", rec.get("predicate", ""))
                        ts = rec.get("timestamp", rec.get("date", ""))

                        if not head or not tail or not rel:
                            file_malformed += 1
                            continue

                        rel_normalized = rel.lower().replace(" ", "_")
                        file_counts[rel_normalized] = file_counts.get(rel_normalized, 0) + 1

                        # Auto-discover actors from FinDKG triples
                        try:
                            from intelligence.actor_ingest import ingest_actor
                            ingest_actor(self.engine, head, "company", source="findkg")
                            ingest_actor(self.engine, tail, "company", source="findkg")
                        except Exception:
                            pass

                        series_id = f"findkg:{head}:{rel_normalized}:{tail}"
                        if self._triple_stored(conn, series_id):
                            file_stored += 1
                            continue
                        result = self._insert_raw(
                            conn,
                            series_id=series_id,
                            obs_date=obs_date,
                            value=1.0,
                            raw_payload={
                                "head": head,
                                "tail": tail,
                                "relation": rel,
                                "timestamp": ts,
                                "source_file": json_file.name,
                            },
                            return_result=True,
                        )
                        rowcount = getattr(result, "rowcount", None)
                        file_inserted += rowcount if isinstance(rowcount, int) and rowcount > 0 else 0
                # Counted only once the file's transaction has committed.
                rows_inserted += file_inserted
                already_stored += file_stored
                malformed += file_malformed
                for rel_type, n in file_counts.items():
                    counts[rel_type] = counts.get(rel_type, 0) + n
                if records and not file_counts:
                    file_errors.append(f"{json_file.name}: no valid triples in {len(records)} records")
            except Exception as exc:
                log.warning("FinDKG file {f} failed: {e}", f=json_file.name, e=str(exc))
                file_errors.append(f"{json_file.name}: {exc}")

        total = sum(counts.values())
        if len(file_errors) >= len(files):
            status = "FAILED"
        elif file_errors:
            status = "PARTIAL"
        else:
            status = "SUCCESS"
        log.info(
            "FinDKG {s}: {t} relationships read ({n} types), {i} rows committed, "
            "{a} already stored, {m} malformed, {e}/{f} files failed",
            s=status, t=total, n=len(counts), i=rows_inserted, a=already_stored,
            m=malformed, e=len(file_errors), f=len(files),
        )
        result: dict[str, Any] = {
            "status": status,
            "rows_inserted": rows_inserted,
            "already_stored": already_stored,
            "malformed_records": malformed,
            "total_relationships": total,
            "by_type": counts,
        }
        if file_errors:
            result["error"] = (f"{len(file_errors)} of {len(files)} files failed: "
                               + "; ".join(file_errors[:5]))
        return result
