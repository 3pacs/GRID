"""No external services or actual company facts are used in these fixtures."""
import copy
import json
import socket
from concurrent.futures import ThreadPoolExecutor

import pytest

from ingestion.company_documents import (
    FIELDS,
    MAX_BYTES,
    availability,
    build_document,
    ingest_document,
    read_document,
)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("network forbidden")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


@pytest.fixture
def meta():
    return {"company_id": "synthetic:fixture", "source_kind": "user_supplied",
                "source_url": "https://example.com/fixture", "title": "Synthetic test only",
                "published_at": "2026-01-01T12:00:00Z", "retrieved_at": "2026-01-02T12:00:00Z",
                "media_type": "text/plain"}


def test_archive_roundtrip_and_repeat(tmp_path, meta):
    result = ingest_document(tmp_path, meta, b"reported zero: 0")
    assert read_document(tmp_path, result["snapshot_id"]) == result
    assert ingest_document(tmp_path, meta, b"reported zero: 0") == result
    assert len(list(tmp_path.iterdir())) == 1


def test_revisions_preserved(tmp_path, meta):
    first = ingest_document(tmp_path, meta, b"first")
    second = ingest_document(tmp_path, meta, b"corrected")
    assert first["snapshot_id"] != second["snapshot_id"]
    assert read_document(tmp_path, first["snapshot_id"])["text"] == "first"
    meta["title"] = "Corrected metadata"
    third = ingest_document(tmp_path, meta, b"corrected")
    assert second["content_sha256"] == third["content_sha256"]
    assert second["snapshot_id"] != third["snapshot_id"]


def test_concurrent_identical_capture(tmp_path, meta):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: ingest_document(tmp_path, meta, b"same"), range(12)))
    assert all(result == results[0] for result in results)
    assert len(list(tmp_path.iterdir())) == 1


@pytest.mark.parametrize("field,value", [
    ("title", "tampered"), ("text", "invented"), ("content_sha256", "0" * 64),
    ("byte_length", 999), ("published_at", None), ("parser_version", "other"),
    ("content_base64", "eA=="), ("extra", "unknown"),
])
def test_tampering_rejected(tmp_path, meta, field, value):
    result = ingest_document(tmp_path, meta, b"fact")
    path = tmp_path / (result["snapshot_id"] + ".json")
    changed = copy.deepcopy(result)
    changed[field] = value
    path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(ValueError):
        read_document(tmp_path, result["snapshot_id"])
    with pytest.raises(ValueError):
        ingest_document(tmp_path, meta, b"fact")


@pytest.mark.parametrize("field,value", [
    ("company_id", ""), ("title", "bad\x00"), ("retrieved_at", "2026-01-02"),
    ("published_at", "2026-01-01T00:00:00"), ("published_at", "2027-01-01T00:00:00Z"),
    ("source_url", "http://example.com"), ("source_url", "https://user:secret@example.com"),
    ("source_kind", "invented"), ("published_at", 5),
])
def test_bad_metadata(meta, field, value):
    meta[field] = value
    with pytest.raises(ValueError):
        build_document(meta, b"fact")


@pytest.mark.parametrize("content", [b"", "not bytes", b"x" * (MAX_BYTES + 1)])
def test_content_limits(meta, content):
    with pytest.raises(ValueError):
        build_document(meta, content)


def test_html_preserves_raw_and_separates_cells(meta):
    meta["media_type"] = "text/html"
    raw = b"<table><tr><td>Revenue</td><td>0</td></tr></table><script>invented</script>"
    result = build_document(meta, raw)
    assert "Revenue\n\n0" in result["text"]
    assert "invented" not in result["text"]
    assert result["byte_length"] == len(raw)


@pytest.mark.parametrize("mime,content,status", [
    ("application/pdf", b"%PDF-1.7", "unsupported_media_type"),
    ("text/plain", b"\xff", "unsupported_encoding"),
    ("text/plain", b"\x00", "invalid_text"),
    ("text/plain", b"  ", "empty_text"),
])
def test_explicit_parse_missingness(meta, mime, content, status):
    meta["media_type"] = mime
    doc = build_document(meta, content)
    assert doc["parse_status"] == status
    assert availability(doc, as_of="2026-01-03T12:00:00Z", max_age_days=30) == status


def test_pit_and_freshness(meta):
    doc = build_document(meta, b"fact")
    assert availability(doc, as_of="2026-01-01T20:00:00Z", max_age_days=1) == "not_yet_observed"
    assert availability(doc, as_of="2026-01-02T12:00:00Z", max_age_days=1) == "available"
    assert availability(doc, as_of="2026-01-02T12:00:01Z", max_age_days=1) == "stale"
    meta["published_at"] = None
    doc = build_document(meta, b"fact")
    assert availability(doc, as_of="2026-01-03T12:00:00Z", max_age_days=30) == "unknown_publication"


def test_sec_locator_and_metadata_contract(meta):
    meta["source_kind"] = "sec_filing"
    with pytest.raises(ValueError):
        build_document(meta, b"fact")
    meta["source_url"] = "https://www.sec.gov/Archives/edgar/data/fixture.htm"
    assert set(build_document(meta, b"fact")) >= FIELDS
    meta["fiscal_period"] = "Q4"
    with pytest.raises(ValueError):
        build_document(meta, b"fact")


@pytest.mark.parametrize("snapshot_id", ["../other", "0" * 63, "G" * 64, None])
def test_path_ids(tmp_path, snapshot_id):
    with pytest.raises(ValueError):
        read_document(tmp_path, snapshot_id)


def test_cli_roundtrip(tmp_path, meta, capsys):
    from scripts.ingest_company_document import main
    metadata = tmp_path / "metadata.json"
    document = tmp_path / "document.txt"
    archive = tmp_path / "archive"
    metadata.write_text(json.dumps(meta), encoding="utf-8")
    document.write_bytes(b"synthetic fixture")
    assert main(["--metadata", str(metadata), "--document", str(document),
                 "--archive", str(archive)]) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert read_document(archive, receipt["snapshot_id"])["text"] == "synthetic fixture"


def test_normalized_timezone_identity(meta):
    doc = build_document(meta, b"fact")
    meta["retrieved_at"] = "2026-01-02T07:00:00-05:00"
    assert build_document(meta, b"fact") == doc


@pytest.mark.parametrize("age", [-1, True, 1.5])
def test_bad_age(meta, age):
    with pytest.raises(ValueError):
        availability(build_document(meta, b"fact"), as_of="2026-01-03T00:00:00Z", max_age_days=age)


def test_verified_quality_integration(tmp_path, meta):
    from company_research_quality import evaluate_archived_company_research
    doc = ingest_document(tmp_path, meta, b"Synthetic fixture reported zero.")
    packet = {"company_id": meta["company_id"], "required_source_kinds": ["user_supplied"],
              "snapshot_ids": [doc["snapshot_id"]], "claims": [{
                  "id": "fixture", "kind": "fact", "text": "Synthetic fixture reported zero.",
                  "evidence": [{"snapshot_id": doc["snapshot_id"],
                                "quote": "Synthetic fixture reported zero."}]}]}
    result = evaluate_archived_company_research(packet, tmp_path,
        as_of="2026-01-03T00:00:00Z", max_age_days={"user_supplied": 30})
    assert result["eligible_for_synthesis"]
    assert not result["truth_assessed"]
    path = tmp_path / (doc["snapshot_id"] + ".json")
    doc["text"] = "unverified substitution"
    path.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(ValueError):
        evaluate_archived_company_research(packet, tmp_path,
            as_of="2026-01-03T00:00:00Z", max_age_days={"user_supplied": 30})


def test_metric_precision_and_bounds():
    from tests.test_company_research_quality import QualityTests
    fixture = QualityTests()
    fixture.setUp()
    first = fixture.packet["claims"][0]
    first["value"] = "1.00000000000000000000000000001"
    other = copy.deepcopy(first)
    other.update(id="other", value="1.00000000000000000000000000002")
    fixture.packet["claims"].append(other)
    assert "metric_conflict" in fixture.codes()
    first["value"] = "1e999999999"
    assert "metric_number_invalid" in fixture.codes()
