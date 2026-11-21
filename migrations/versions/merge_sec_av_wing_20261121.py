"""Join SEC/publication and current main migration histories.

No data/schema operations; each existing lineage retains its original identity.
"""

revision = "merge_sec_av_wing_20261121"
down_revision = ("sec_pub_merge_20261109", "merge_av_wing_20261120")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
