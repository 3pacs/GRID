"""PROPOSED ONLY: outside Alembic version_locations, never auto-applied.

Owner migration GO, current-head rebase and isolated PostgreSQL validation are
required before promotion into migrations/versions. No production execution.
"""
import json
import os
from pathlib import Path

from alembic import op
from sqlalchemy import text

revision = "gamma_watch_p1_20260928"
down_revision = "security_master_20260927"
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE gamma_watch_source_contracts (
 source_id INTEGER PRIMARY KEY REFERENCES source_catalog(id),
 contract JSONB NOT NULL, contract_sha256 TEXT NOT NULL CHECK (contract_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE TABLE gamma_watch_databases (
 database_uuid UUID NOT NULL, epoch_uuid UUID NOT NULL,
 identity_sha256 TEXT NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
 registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 identity_manifest JSONB NOT NULL,
 PRIMARY KEY(database_uuid, epoch_uuid)
);
CREATE TABLE gamma_watch_captures (
 database_uuid UUID NOT NULL, epoch_uuid UUID NOT NULL,
 stream TEXT NOT NULL, sequence BIGINT NOT NULL CHECK (sequence > 0),
 source_id INTEGER NOT NULL REFERENCES gamma_watch_source_contracts(source_id),
 schema_version INTEGER NOT NULL CHECK (schema_version > 0),
 payload_bytes BYTEA NOT NULL,
 payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
 previous_sha256 TEXT CHECK (previous_sha256 ~ '^[0-9a-f]{64}$'),
 source_time_raw TEXT, source_event_at TIMESTAMPTZ,
 collector_received_at TIMESTAMPTZ,
 journal_received_at TIMESTAMPTZ NOT NULL,
 available_at TIMESTAMPTZ NOT NULL,
 canonical_ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 availability_basis TEXT NOT NULL CHECK (availability_basis IN ('trusted_collector_receipt','journal_receipt','legacy_admission_receipt')),
 initial_status TEXT NOT NULL CHECK (initial_status IN ('SUCCESS','PARTIAL','FAILED','QUARANTINED')),
 reason_codes JSONB NOT NULL,
 lineage JSONB NOT NULL,
 PRIMARY KEY(database_uuid, epoch_uuid, stream, sequence),
 FOREIGN KEY(database_uuid, epoch_uuid) REFERENCES gamma_watch_databases(database_uuid, epoch_uuid),
 CHECK (available_at <= canonical_ingested_at),
 CHECK (journal_received_at <= canonical_ingested_at),
 CHECK ((availability_basis = 'trusted_collector_receipt' AND collector_received_at IS NOT NULL AND available_at = collector_received_at)
     OR (availability_basis = 'journal_receipt' AND available_at = journal_received_at)
     OR (availability_basis = 'legacy_admission_receipt' AND available_at >= journal_received_at))
);
CREATE INDEX ix_gamma_watch_captures_source_available ON gamma_watch_captures(source_id, available_at);
CREATE TABLE gamma_watch_model_inputs (
 database_uuid UUID NOT NULL, epoch_uuid UUID NOT NULL,
 model_stream TEXT NOT NULL, model_sequence BIGINT NOT NULL,
 input_stream TEXT NOT NULL, input_sequence BIGINT NOT NULL,
 PRIMARY KEY(database_uuid, epoch_uuid, model_stream, model_sequence, input_stream, input_sequence),
 FOREIGN KEY(database_uuid, epoch_uuid, model_stream, model_sequence)
 REFERENCES gamma_watch_captures(database_uuid, epoch_uuid, stream, sequence),
 FOREIGN KEY(database_uuid, epoch_uuid, input_stream, input_sequence)
 REFERENCES gamma_watch_captures(database_uuid, epoch_uuid, stream, sequence),
 CHECK (model_stream <> input_stream OR model_sequence <> input_sequence)
);
CREATE TABLE gamma_watch_cursors (
 database_uuid UUID NOT NULL, epoch_uuid UUID NOT NULL, stream TEXT NOT NULL,
 last_sequence BIGINT NOT NULL CHECK (last_sequence > 0),
 last_sha256 TEXT NOT NULL CHECK (last_sha256 ~ '^[0-9a-f]{64}$'),
 adapter_version TEXT NOT NULL,
 PRIMARY KEY(database_uuid, epoch_uuid, stream),
 FOREIGN KEY(database_uuid, epoch_uuid, stream, last_sequence)
 REFERENCES gamma_watch_captures(database_uuid, epoch_uuid, stream, sequence)
);
CREATE TABLE gamma_watch_admissions (
 database_uuid UUID NOT NULL, epoch_uuid UUID NOT NULL, stream TEXT NOT NULL, sequence BIGINT NOT NULL,
 field_key TEXT NOT NULL, series_id TEXT NOT NULL,
 raw_series_id BIGINT NOT NULL UNIQUE,
 policy_version TEXT NOT NULL,
 PRIMARY KEY(database_uuid, epoch_uuid, stream, sequence, field_key),
 FOREIGN KEY(database_uuid, epoch_uuid, stream, sequence)
 REFERENCES gamma_watch_captures(database_uuid, epoch_uuid, stream, sequence)
);
CREATE FUNCTION gamma_watch_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Gamma Watch receipt evidence is append-only'; END;
$$;
"""


def upgrade():
    if os.environ.get("GRID_APPROVE_GAMMA_WATCH_SCHEMA") != "approved":
        raise RuntimeError("Proposed migration: explicit owner approval and promotion required")
    # A collision fails, rather than silently redefining a pre-existing source.
    import hashlib
    manifest = json.loads((Path(__file__).resolve().parents[2] / "config/gamma_watch_sources.json").read_text())
    op.execute(DDL)
    for source in manifest["sources"]:
        source_id = op.get_bind().execute(text("""
          INSERT INTO source_catalog(name,base_url,cost_tier,latency_class,pit_available,revision_behavior,trust_score,priority_rank,active)
          VALUES (:name,:base_url,'FREE',:catalog_latency,false,'FREQUENT','LOW',1000,false) RETURNING id
        """), source).scalar_one()
        body = json.dumps(source, sort_keys=True, separators=(",", ":"))
        op.get_bind().execute(text("INSERT INTO gamma_watch_source_contracts VALUES (:id,CAST(:body AS JSONB),:sha)"),
                              {"id": source_id, "body": body, "sha": hashlib.sha256(body.encode()).hexdigest()})
    for table in ("source_contracts", "databases", "captures", "model_inputs", "admissions"):
        op.execute(f"CREATE TRIGGER gamma_watch_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON gamma_watch_{table} "
                   "FOR EACH STATEMENT EXECUTE FUNCTION gamma_watch_reject_mutation()")


def downgrade():
    raise RuntimeError("No destructive automatic downgrade: preserve receipts; reviewed archival plan required")
