"""Join SEC qualification and publication-review migration branches.

No data/schema operations; each existing lineage retains its original identity.
"""
revision = "sec_pub_merge_20261109"
down_revision = ("sec_financial_outputs_20261108", "pub_review_ingest_20261108")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
