import json
import os

from scripts import hermes_agent_fleet_audit as audit


def test_audit_agent_marks_current_missing_and_stale(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    current = root / "current.md"
    stale = root / "stale.md"
    current.write_text("current", encoding="utf-8")
    stale.write_text("stale", encoding="utf-8")
    now = 1_700_000_000.0
    os.utime(current, (now - 2 * audit.SECONDS_PER_DAY, now - 2 * audit.SECONDS_PER_DAY))
    os.utime(stale, (now - 30 * audit.SECONDS_PER_DAY, now - 30 * audit.SECONDS_PER_DAY))

    current_result = audit.audit_agent(
        {"id": "current-agent", "max_age_days": 7, "watch": ["current.md"]},
        root=root,
        now=now,
    )
    stale_result = audit.audit_agent(
        {"id": "stale-agent", "max_age_days": 7, "watch": ["stale.md"]},
        root=root,
        now=now,
    )
    missing_result = audit.audit_agent(
        {"id": "missing-agent", "max_age_days": 7, "watch": ["missing.md"]},
        root=root,
        now=now,
    )

    assert current_result.status == "current"
    assert stale_result.status == "stale"
    assert missing_result.status == "missing"
    assert missing_result.missing_paths == ["missing.md"]


def test_build_payload_counts_statuses(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "a.md").write_text("a", encoding="utf-8")
    registry = {
        "schema_version": 1,
        "agents": [
            {"id": "a", "max_age_days": 7, "watch": ["a.md"]},
            {"id": "b", "max_age_days": 7, "watch": ["b.md"]},
        ],
    }

    payload = audit.build_audit_payload(registry, root=root, now=(root / "a.md").stat().st_mtime)

    assert payload["counts"]["current"] == 1
    assert payload["counts"]["missing"] == 1


def test_markdown_report_names_read_only_and_agents():
    payload = {
        "generated_at": "2026-05-18T00:00:00Z",
        "counts": {"current": 1, "missing": 0, "stale": 0},
        "agents": [
            {
                "agent_id": "fleet-audit",
                "status": "current",
                "latest_age_days": 0.0,
                "max_age_days": 14,
                "missing_paths": [],
                "recommended_action": "No action required.",
            }
        ],
    }

    markdown = audit.markdown_report(payload)

    assert "Hermes Agent Fleet Audit" in markdown
    assert "read-only" in markdown
    assert "fleet-audit - current" in markdown


def test_write_outputs_writes_json_and_markdown(tmp_path):
    payload = {"generated_at": "now", "counts": {"current": 0}, "agents": []}
    json_path = tmp_path / "audit.json"
    md_path = tmp_path / "audit.md"

    audit.write_outputs(payload, json_path, md_path)

    assert json.loads(json_path.read_text())["generated_at"] == "now"
    assert "Hermes Agent Fleet Audit" in md_path.read_text()
