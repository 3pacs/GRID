"""Join the SEC/wing merge with main's subsequent research migration.

No data/schema operations; preserve the requested SEC/wing join and the newer
main lineage without rewriting either migration history.
"""

revision = "merge_sec_main_20261121"
down_revision = ("merge_sec_av_wing_20261121", "research_findings_20261121")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
