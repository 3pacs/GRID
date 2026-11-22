"""The append-only change log for ``feature_registry.model_eligible`` (#1301). It changes no flag.

Revision ID: eligibility_core_20261122
Revises: regime_history_revs_20261122
Create Date: 2026-11-22 00:00:00.000000

``feature_registry.model_eligible`` is a global switch read by the Oracle, inference, discovery, feature
selection and the auto_regime BUY gate, and every deploy runs ``alembic upgrade head``. So this migration
flips nothing: a deploy only creates the place where a change is recorded. Nothing in this release writes to
it. The tool that changes flags (``--apply``, its owner-approved evidence receipt and its rollback) is a
separate reviewed PR, held until the Oracle polarity fix and auto_regime's own bindings plus hysteresis
(#1307) are deployed. Until then the table stays empty.

What it creates:

* ``feature_eligibility_changes``: one row per flag a run changed (feature id and name, old and new flag),
  grouped by ``change_set`` (one uuid per run). ``action`` is ``apply`` or ``rollback``; a rollback row
  names the change set it restores in ``rolls_back``. ``recorded_by`` is the role that wrote it.
* Triggers that refuse UPDATE, DELETE and TRUNCATE on it (append-only).
* Privileges: nothing for PUBLIC; the application role ``grid`` gets SELECT only (no INSERT, no sequence
  use), so the application cannot write a pre-image that a rollback would later trust. The grant first
  revokes everything, so default privileges cannot widen it.

When the role running this migration is ``grid`` itself (the deploy hook sources the application's env),
``grid`` owns the table and no grant can stop it writing. The migration still succeeds and logs a warning
that lists the ownership statements; any future writer must refuse while ``grid`` owns the table or the
function its triggers run.

Downgrade drops the table only while it is empty. Once any change is recorded it refuses, because the
rows are the only record of what each flag was before.
"""

import logging
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = "eligibility_core_20261122"
down_revision: Union[str, Sequence[str], None] = "regime_history_revs_20261122"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

# The application role. Tests replace it with a role they create (the test server's ``grid`` is a superuser).
APP_ROLE = "grid"

CHANGES_TABLE = """
CREATE TABLE IF NOT EXISTS feature_eligibility_changes (
    id            BIGSERIAL PRIMARY KEY,
    change_set    UUID NOT NULL,
    action        TEXT NOT NULL CHECK (action IN ('apply', 'rollback')),
    rolls_back    UUID,
    feature_id    INTEGER NOT NULL,
    name          TEXT NOT NULL,
    old_eligible  BOOLEAN NOT NULL,
    new_eligible  BOOLEAN NOT NULL,
    plan          TEXT NOT NULL CHECK (btrim(plan) <> ''),
    recorded_by   TEXT NOT NULL DEFAULT current_user,
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT feature_eligibility_changes_is_a_change CHECK (old_eligible IS DISTINCT FROM new_eligible),
    CONSTRAINT feature_eligibility_changes_rollback_names_its_set
        CHECK ((action = 'rollback') = (rolls_back IS NOT NULL)),
    CONSTRAINT feature_eligibility_changes_one_row_per_feature UNIQUE (change_set, feature_id)
)
"""

APPEND_ONLY = """
CREATE OR REPLACE FUNCTION feature_eligibility_changes_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'feature_eligibility_changes is append-only: % refused', TG_OP;
END;
$$ LANGUAGE plpgsql
"""

TRIGGER_NAMES = ("feature_eligibility_changes_no_mutation", "feature_eligibility_changes_no_truncate")

TRIGGERS = (
    "DROP TRIGGER IF EXISTS feature_eligibility_changes_no_mutation ON feature_eligibility_changes",
    """CREATE TRIGGER feature_eligibility_changes_no_mutation
    BEFORE UPDATE OR DELETE ON feature_eligibility_changes
    FOR EACH ROW EXECUTE FUNCTION feature_eligibility_changes_append_only()""",
    "DROP TRIGGER IF EXISTS feature_eligibility_changes_no_truncate ON feature_eligibility_changes",
    """CREATE TRIGGER feature_eligibility_changes_no_truncate
    BEFORE TRUNCATE ON feature_eligibility_changes
    FOR EACH STATEMENT EXECUTE FUNCTION feature_eligibility_changes_append_only()""",
)

# The role name is bound (set_config) and quoted server-side with format('%I'); no SQL is built in Python.
BIND_APP_ROLE = text("SELECT set_config('grid.eligibility_app_role', :role, true)")

GRANTS = """
DO $$
DECLARE
    app   TEXT := current_setting('grid.eligibility_app_role');
    owner OID  := (SELECT relowner FROM pg_class WHERE oid = 'feature_eligibility_changes'::regclass);
BEGIN
    REVOKE ALL ON feature_eligibility_changes FROM PUBLIC;
    REVOKE ALL ON SEQUENCE feature_eligibility_changes_id_seq FROM PUBLIC;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = app) AND NOT pg_has_role(app, owner, 'MEMBER') THEN
        EXECUTE format('REVOKE ALL ON feature_eligibility_changes FROM %I', app);
        EXECUTE format('REVOKE ALL ON SEQUENCE feature_eligibility_changes_id_seq FROM %I', app);
        EXECUTE format('GRANT SELECT ON feature_eligibility_changes TO %I', app);
    END IF;
END
$$
"""

APP_OWNS_THE_LOG = text("""
SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :role)
   AND pg_has_role(:role, (SELECT relowner FROM pg_class WHERE oid = 'feature_eligibility_changes'::regclass),
                   'MEMBER')
""")

# What a superuser runs, once, when the application role owns the log (logged by upgrade()).
# grid_eligibility_owner is an example name; grid must not be a member of it.
OWNERSHIP_FIX_SQL = (
    "CREATE ROLE grid_eligibility_owner NOLOGIN;",
    "ALTER TABLE feature_eligibility_changes OWNER TO grid_eligibility_owner;",  # its id sequence moves with it
    "ALTER FUNCTION feature_eligibility_changes_append_only() OWNER TO grid_eligibility_owner;",
    "REVOKE ALL ON feature_eligibility_changes FROM grid;",
    "REVOKE ALL ON SEQUENCE feature_eligibility_changes_id_seq FROM grid;",
    "GRANT SELECT ON feature_eligibility_changes TO grid;",
)

REFUSE_DOWNGRADE_WITH_HISTORY = """
DO $$
BEGIN
    IF to_regclass('feature_eligibility_changes') IS NOT NULL THEN
        IF EXISTS (SELECT 1 FROM feature_eligibility_changes) THEN
            RAISE EXCEPTION 'downgrade refused: feature_eligibility_changes holds recorded flag changes, '
                            'and dropping it would destroy the only record of each flag''s previous value';
        END IF;
    END IF;
END
$$
"""


def upgrade() -> None:
    op.execute(CHANGES_TABLE)
    op.execute(APPEND_ONLY)
    for statement in TRIGGERS:
        op.execute(statement)
    op.execute(BIND_APP_ROLE.bindparams(role=APP_ROLE))
    op.execute(GRANTS)
    if op.get_context().as_sql:  # offline (--sql) mode has no connection to ask
        return
    if op.get_bind().execute(APP_OWNS_THE_LOG, {"role": APP_ROLE}).scalar():
        log.warning(
            "feature_eligibility_changes: the application role %s owns it (or is a member of its owner), so it "
            "could write pre-images. Nothing writes to it in this release; before any tool records a change, a "
            "superuser should run: %s", APP_ROLE, " ".join(OWNERSHIP_FIX_SQL))


def downgrade() -> None:
    op.execute(REFUSE_DOWNGRADE_WITH_HISTORY)
    op.execute("DROP TABLE IF EXISTS feature_eligibility_changes")
    op.execute("DROP FUNCTION IF EXISTS feature_eligibility_changes_append_only()")
