"""Join the AV event-date chain and the GDELT GKG migration heads.

Revision ID: merge_av_gdelt_20261120
Revises: merge_av_review_20261109, gdelt_gkg_20261116

#1051's chain (av_event_dates_20261108 -> merge_av_review_20261109) branched
from sleuth_intersections_20261107 / pub_review_ingest_20261108; main moved on
to gdelt_gkg_20261116 while the PR was open.

Pure merge point: no schema or data operations. Downgrade refuses, because
merge_av_review_20261109 refuses to restore fabricated earnings dates.
"""

revision = "merge_av_gdelt_20261120"
down_revision = (
    "merge_av_review_20261109",
    "gdelt_gkg_20261116",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through merge_av_gdelt_20261120 is refused: "
        "merge_av_review_20261109 does not restore fabricated earnings dates."
    )
