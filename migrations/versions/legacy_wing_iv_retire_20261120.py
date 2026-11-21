"""Retire the legacy <ticker>_iv_25d_put/call features from model use.

Revision ID: legacy_wing_iv_retire_20261120
Revises: merge_wing_gdelt_20261120

From wing_iv_proxy_20261116 on, the options producer publishes versioned
fixed-moneyness proxies (<ticker>_iv_m90_put_v1, <ticker>_iv_m110_call_v1)
and stops writing <ticker>_iv_25d_put/call. Those legacy features were
registered model_eligible with FORWARD_FILL, so feature pipelines that select
model_eligible features (clustering, orthogonality, live inference) would
keep forward-filling their last value indefinitely. Their method was never a
true 25-delta read either (see docs/reference/OPTIONS_WING_IV_METHODS.md).

Mark them deprecated and not model-eligible. History stays in
resolved_series untouched; only the two registry flags change. Already
deprecated rows are left as they are.
"""
from alembic import op

revision = "legacy_wing_iv_retire_20261120"
down_revision = "merge_wing_gdelt_20261120"
branch_labels = None
depends_on = None

RETIRE_SQL = (
    "UPDATE feature_registry "
    "SET model_eligible = FALSE, deprecated_at = CURRENT_TIMESTAMP "
    "WHERE deprecated_at IS NULL "
    "AND (name LIKE '%\\_iv\\_25d\\_put' ESCAPE '\\' "
    "OR name LIKE '%\\_iv\\_25d\\_call' ESCAPE '\\')"
)


def upgrade() -> None:
    op.execute(RETIRE_SQL)


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade of legacy_wing_iv_retire_20261120 is refused: the prior "
        "model_eligible flags are not recorded, and the producer no longer "
        "writes the legacy features."
    )
