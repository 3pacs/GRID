from __future__ import annotations

from pathlib import Path

from scripts import storage_curator


def test_storage_curator_flags_gdelt_tables_missing(tmp_path: Path) -> None:
    gdelt = tmp_path / "gdelt"
    (gdelt / ".state").mkdir(parents=True)
    (gdelt / ".state" / "v1_events.done").write_text("20240101.export.CSV.zip\n", encoding="utf-8")
    (gdelt / "v1" / "events").mkdir(parents=True)
    (gdelt / "v1" / "events" / "20240101.export.CSV.zip").write_text("zip", encoding="utf-8")
    parser_root = tmp_path / "grid" / "bulk" / "gdelt"
    parser_root.mkdir(parents=True)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            class _Result:
                def scalar(self):
                    return False

            return _Result()

    class _Engine:
        def connect(self):
            return _Conn()

    assessment = storage_curator.assess_gdelt(
        engine=_Engine(),
        gdelt_base=gdelt,
        parser_root=parser_root,
    )

    assert assessment.ingest_status == "archives_present_db_tables_missing"
    assert assessment.state_counts["v1_events.done"] == 1
    assert assessment.directory_file_counts["v1_events"] == 1
    assert any("GDELT bulk parser" in rec for rec in assessment.recommendations)


def test_storage_curator_builds_cold_storage_cleanup_plan(tmp_path: Path) -> None:
    active = tmp_path / "data" / "datasets"
    active.mkdir(parents=True)
    archive = active / "large.zip"
    archive.write_bytes(b"x" * 1024)

    report = storage_curator.build_storage_maintenance_report(
        engine=None,
        target_id="test-node",
        active_roots=(active,),
        cold_root=tmp_path / "mirror" / "grid-storage",
        max_files_per_root=10,
        min_archive_bytes=1,
    )

    assert report.cleanup_plan
    assert report.cleanup_plan[0]["action"] == "create_cold_storage_root"
    assert any(item.get("source") == str(archive) for item in report.cleanup_plan)
    assert report.cleanup_plan[-1]["delete_source"] is False


def test_storage_curator_write_report_uses_output_dir(tmp_path: Path, monkeypatch) -> None:
    active = tmp_path / "data"
    active.mkdir()
    monkeypatch.chdir(tmp_path)

    result = storage_curator.run_storage_maintenance(
        engine=None,
        target_id="test-node",
        active_roots=(active,),
        cold_root=tmp_path / "mirror",
        max_files_per_root=5,
    )

    assert result["markdown_path"]
    assert Path(result["markdown_path"]).exists()
    assert Path("outputs/storage_maintenance/storage_maintenance_latest.md").exists()
