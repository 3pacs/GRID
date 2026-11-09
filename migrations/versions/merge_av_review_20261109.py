"""Join AV date provenance and publication-review migrations without DDL.

Both parents retain their original operations and shared predecessor. This
source-only join does not repair historical data or authorize a rollout.
"""

revision = "merge_av_review_20261109"
down_revision = ("av_event_dates_20261108", "pub_review_ingest_20261108")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through merge_av_review_20261109 is refused: actual event "
        "dates must not be fabricated and publication reviews are append-only. "
        "Use a separately reviewed forward recovery plan."
    )
