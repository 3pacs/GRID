"""Join the Yahoo vintage and FINRA ATS registration migration heads.

Revision ID: merge_yf_finra_20261023
Revises: yf_latest_daily_vintage_20261019, finra_ats_feature_20261020

Both parent migrations keep their original lineage and operations. Upgrade
performs no schema or data operations. Downgrade refuses traversal through
this merge because the historical FINRA downgrade does not establish row
ownership. This does not protect direct parent calls or partial histories.
"""

revision = "merge_yf_finra_20261023"
down_revision = (
    "yf_latest_daily_vintage_20261019",
    "finra_ats_feature_20261020",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through merge_yf_finra_20261023 is refused: "
        "the historical FINRA downgrade can delete a pre-existing registration. "
        "Retain registry rows and dependent observations; use a separately "
        "reviewed forward recovery plan."
    )
