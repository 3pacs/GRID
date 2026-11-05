"""Join the ghost-feature registration and options-digest ledger heads.

Revision ID: merge_ghost_digest_20261104
Revises: ghost_features_20261104, options_digest_calls_20261104

Both parents branch from merge_yf_finra_20261023 and keep their own
operations. Upgrade performs no schema or data operations. Downgrade is
refused: ghost_features_20261104 already refuses it (registry rows may gain
dependent resolved observations), and options_digest_calls_20261104 holds
an insert-only ledger of emailed calls.
"""

revision = "merge_ghost_digest_20261104"
down_revision = (
    "ghost_features_20261104",
    "options_digest_calls_20261104",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through merge_ghost_digest_20261104 is refused: the feature "
        "registrations may have dependent observations and the digest call "
        "ledger is insert-only. Use a separately reviewed forward plan."
    )
