"""Retire the GDELT DOC API features from model use.

Revision ID: gdelt_doc_retire_20261123
Revises: cboe_equity_pcr_20261123

The scheduled GDELT DOC puller (ingestion/altdata/gdelt.py, PULLER_REGISTRY
"gdelt") is held from 2026-11-23: the DOC API answered 429 on most runs since
October and refused TCP connections from three hosts that day. Its 24 features
have no writer any more: the actor tones and country tensions stopped on
2026-11-03/06, most theme features in October.

The 14 actor-tone and tension features were registered model_eligible with
FORWARD_FILL (migrations/0062), so feature pipelines that select
model_eligible features would keep forward-filling their last value. Mark all
24 deprecated and not model-eligible. History stays in raw_series and
resolved_series untouched; only the two registry flags change. Already
deprecated rows are left as they are.

The 12 daily event features written from GDELT's bulk files
(ingestion/altdata/gdelt_bulk.py: gdelt_avg_tone, gdelt_conflict_count, ...)
are not touched: that loader still runs.

Downgrade restores the flags as 0062 registered them, for bringing the DOC
puller back (remove the registry hold first).
"""
from alembic import op
from sqlalchemy import bindparam, text

revision = "gdelt_doc_retire_20261123"
down_revision = "cboe_equity_pcr_20261123"
branch_labels = None
depends_on = None

THEME_FEATURES: tuple[str, ...] = (
    "gdelt_recession_tone",
    "gdelt_fed_tone",
    "gdelt_trade_conflict_volume",
    "gdelt_sanctions_russia_tone",
    "gdelt_taiwan_strait_volume",
    "gdelt_opec_tone",
    "gdelt_chip_controls_volume",
    "gdelt_central_bank_tone",
    "gdelt_em_currency_tone",
    "gdelt_defense_spending_volume",
)
# Registered model_eligible = TRUE by migrations/0062.
ACTOR_AND_TENSION_FEATURES: tuple[str, ...] = (
    "gdelt_actor_powell_tone",
    "gdelt_actor_lagarde_tone",
    "gdelt_actor_xi_tone",
    "gdelt_actor_putin_tone",
    "gdelt_actor_mbs_tone",
    "gdelt_actor_yellen_tone",
    "gdelt_actor_ueda_tone",
    "gdelt_tension_us_china",
    "gdelt_tension_us_russia",
    "gdelt_tension_us_iran",
    "gdelt_tension_china_taiwan",
    "gdelt_tension_russia_ukraine",
    "gdelt_tension_israel_iran",
    "gdelt_tension_india_china",
)
DOC_FEATURES: tuple[str, ...] = THEME_FEATURES + ACTOR_AND_TENSION_FEATURES

_RETIRE = text(
    "UPDATE feature_registry "
    "SET model_eligible = FALSE, deprecated_at = CURRENT_TIMESTAMP "
    "WHERE deprecated_at IS NULL AND name IN :names"
).bindparams(bindparam("names", expanding=True))
_UNDEPRECATE = text(
    "UPDATE feature_registry SET deprecated_at = NULL WHERE name IN :names"
).bindparams(bindparam("names", expanding=True))
_RE_ENABLE = text(
    "UPDATE feature_registry SET model_eligible = TRUE WHERE name IN :names"
).bindparams(bindparam("names", expanding=True))


def upgrade() -> None:
    op.get_bind().execute(_RETIRE, {"names": list(DOC_FEATURES)})


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(_UNDEPRECATE, {"names": list(DOC_FEATURES)})
    bind.execute(_RE_ENABLE, {"names": list(ACTOR_AND_TENSION_FEATURES)})
