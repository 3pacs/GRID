import json
from pathlib import Path

from scripts import hermes_finetune_dataset as dataset


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_redact_text_scrubs_common_secret_shapes():
    text = "OPENAI_API_KEY=sk-secretsecretsecret Bearer abcdefghijklmnopqrstuvwxyz hf_abcdefghijklmnopqrstuvwxyz"

    redacted = dataset.redact_text(text)

    assert "secretsecret" not in redacted
    assert "abcdefghijklmnopqrstuvwxyz" not in redacted
    assert "OPENAI_API_KEY=[REDACTED]" in redacted
    assert "Bearer [REDACTED]" in redacted
    assert "hf_[REDACTED]" in redacted


def test_queue_record_to_example_preserves_safe_fleet_intent():
    example = dataset.queue_record_to_example(
        {"id": "imsg-1", "command": "fleet", "args": "panda render lane", "approved": True}
    )

    assert example is not None
    assert example["source"] == "queue:imsg-1"
    assert example["tags"] == ["queue", "fleet", "approved"]
    assert "/fleet panda render lane" in example["messages"][1]["content"]
    assert "read-only fleet audit" in example["messages"][2]["content"]


def test_restart_without_approval_teaches_no_mutation():
    example = dataset.queue_record_to_example(
        {"id": "imsg-2", "command": "restart", "args": "grid-hermes", "approved": False}
    )

    assert example is not None
    assert "Do not restart yet" in example["messages"][2]["content"]
    assert "same-sender approval" in example["messages"][2]["content"]


def test_report_file_to_example_extracts_recovery_sections(tmp_path):
    report = tmp_path / "report.md"
    report.write_text(
        "# Hermes Run\n\n"
        "## What changed\n\nAdded fleet audit.\n\n"
        "## Verification\n\npytest passed.\n\n"
        "## Manual asks\n\nApprove systemd install.\n",
        encoding="utf-8",
    )

    example = dataset.report_file_to_example(report)

    assert example is not None
    assert example["source"] == "report:report.md"
    assert "Added fleet audit" in example["messages"][2]["content"]
    assert "pytest passed" in example["messages"][2]["content"]
    assert "Approve systemd install" in example["messages"][2]["content"]


def test_build_examples_and_write_jsonl(tmp_path):
    queue = tmp_path / "queue.jsonl"
    reports = tmp_path / "reports"
    reports.mkdir()
    queue.write_text(json.dumps({"id": "imsg-3", "command": "todo", "args": "add panda drive", "approved": True}) + "\n")
    (reports / "latest.md").write_text("# Latest\n\n## Summary\n\nDone.\n", encoding="utf-8")

    examples = dataset.build_examples(
        queue_path=queue,
        reports_dir=reports,
        max_report_files=5,
        include_seed_examples=True,
    )
    output = tmp_path / "out.jsonl"
    dataset.write_jsonl(output, examples)

    loaded = read_jsonl(output)
    assert len(loaded) == 5
    assert loaded[-2]["source"] == "queue:imsg-3"
    assert loaded[-1]["source"] == "report:latest.md"
