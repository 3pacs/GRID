"""oracle_predictions: entry_price and confidence are NULL when nothing measured them.

Revision ID: oracle_pred_nullable_0918
Revises: journal_unscored_conf_0918
Create Date: 2026-09-18

``oracle_predictions`` is created by ``oracle/engine.py::OracleEngine._ensure_tables``
(``CREATE TABLE IF NOT EXISTS``); it has no alembic revision and is not in
schema.sql. That bootstrap declared ``entry_price DOUBLE PRECISION NOT NULL``
and ``confidence DOUBLE PRECISION NOT NULL`` while both writers already bind
``None`` -- ``_store_predictions`` for a ticker with no observed spot, and
``oracle/publish.py`` for a confidence nothing scored (fake-data audit D-H11 /
D-M32; ``docs/reference/CONFIDENCE_POLICY.md``). A NULL is the honest value:
``entry_price`` was previously published as a literal ``0.0`` and rendered as
"$0.00"; ``confidence`` was a placeholder.

``ALTER TABLE IF EXISTS`` because on a database where the oracle has never run
the table is absent and the engine's own CREATE (updated in the same PR) already
carries the nullable shape. The "silently skipped" hazard that implies is
pinned by ``tests/test_oracle_predictions_schema_parity.py``.

Downgrade: NOT NULL is restored only where no NULL exists in the column; a
column that already holds NULLs (rows published honestly after this
revision) stays nullable and the migration logs why -- rewriting or deleting
those rows is not an option (historical repair is on hold, and a NULL here is
a fact, not a defect).

Locking: ALTER COLUMN ... DROP NOT NULL is a catalog-only change on PostgreSQL
14 but takes ACCESS EXCLUSIVE on ``oracle_predictions`` for its duration; it
runs once at deploy time.
"""

import logging
from collections.abc import Sequence

from alembic import op

# 25 characters; alembic_version.version_num is VARCHAR(32).
revision: str = "oracle_pred_nullable_0918"
down_revision: str | Sequence[str] | None = "journal_unscored_conf_0918"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

_COLUMNS = ("entry_price", "confidence")


def upgrade() -> None:
    for col in _COLUMNS:
        op.execute(
            f"ALTER TABLE IF EXISTS oracle_predictions ALTER COLUMN {col} DROP NOT NULL"
        )


def downgrade() -> None:
    conn = op.get_bind()
    exists = conn.exec_driver_sql(
        "SELECT to_regclass('oracle_predictions') IS NOT NULL"
    ).scalar()
    if not exists:
        return
    for col in _COLUMNS:
        nulls = conn.exec_driver_sql(
            f"SELECT count(*) FROM oracle_predictions WHERE {col} IS NULL"
        ).scalar()
        if nulls:
            log.warning(
                "oracle_pred_nullable_0918 downgrade left oracle_predictions.%s "
                "NULLABLE: %s row(s) hold NULL and would have to be deleted or "
                "given an invented value to restore NOT NULL.",
                col, nulls,
            )
            continue
        op.execute(
            f"ALTER TABLE oracle_predictions ALTER COLUMN {col} SET NOT NULL"
        )
