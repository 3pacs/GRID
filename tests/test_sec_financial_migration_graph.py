"""Source-only migration graph checks: no database configuration or access."""
from pathlib import Path
from alembic.script import ScriptDirectory


def test_sec_and_publication_review_have_one_additive_merge_head():
    script = ScriptDirectory(str(Path(__file__).resolve().parents[1] / "migrations"))
    heads = script.get_heads()
    assert len(heads) == 1
    # Future additive migrations may extend the head. Both independent
    # histories must remain reachable without pinning today's tip.
    ancestors = {revision.revision for revision in script.walk_revisions()}
    assert {"sec_pub_merge_20261109", "merge_av_wing_20261120"} <= ancestors
    merge = script.get_revision("sec_pub_merge_20261109")
    assert set(merge.down_revision) == {
        "sec_financial_outputs_20261108", "pub_review_ingest_20261108"
    }
    assert script.get_revision("sec_financial_snap_20261107").down_revision == "sleuth_intersections_20261107"
    assert script.get_revision("pub_review_ingest_20261108").down_revision == "sleuth_intersections_20261107"
    # Merge itself does not run SQL or alter either branch's data/schema.
    merge.module.upgrade()
    merge.module.downgrade()
    for revision, parents in {
        "merge_sec_av_wing_20261121": {"sec_pub_merge_20261109", "merge_av_wing_20261120"},
        "merge_sec_main_20261121": {"merge_sec_av_wing_20261121", "research_findings_20261121"},
    }.items():
        join = script.get_revision(revision)
        assert set(join.down_revision) == parents
        join.module.upgrade()
        join.module.downgrade()
