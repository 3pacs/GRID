"""Source-only migration graph checks: no database configuration or access."""
from pathlib import Path
from alembic.script import ScriptDirectory


def test_sec_and_publication_review_have_one_additive_merge_head():
    script = ScriptDirectory(str(Path(__file__).resolve().parents[1] / "migrations"))
    assert script.get_heads() == ["sec_pub_merge_20261109"]
    merge = script.get_revision("sec_pub_merge_20261109")
    assert set(merge.down_revision) == {
        "sec_financial_outputs_20261108", "pub_review_ingest_20261108"
    }
    assert script.get_revision("sec_financial_snap_20261107").down_revision == "sleuth_intersections_20261107"
    assert script.get_revision("pub_review_ingest_20261108").down_revision == "sleuth_intersections_20261107"
    # Merge itself does not run SQL or alter either branch's data/schema.
    merge.module.upgrade()
    merge.module.downgrade()
