"""Offline tests through the exact-archive and injected-consumer boundary."""
from copy import deepcopy
from hashlib import sha256
from unittest.mock import Mock
from unittest import TestCase
from pathlib import Path
from tempfile import TemporaryDirectory

from company_research_quality import diagnose_sec_exhibit_evidence
from ingestion.company_documents import ingest_document, read_document

CIK = "0000000001"
ACC = "0000000001-99-000001"
SECURITY = "fixture-common-1"
URL = "https://www.sec.gov/Archives/edgar/data/1/000000000199000001/fixture-exhibit.htm"


def stamp(clock):
    return f"2099-01-12T{clock}:00-05:00"


def receipt(root, *, claim="fictional issuer reports earnings", public="12:47", fetched="12:48",
            visible="12:49", version=1, supersedes=None, url=URL, publication_unknown=False):
    raw = f"<p>{claim}</p>".encode()
    doc = ingest_document(root, {
        "company_id": CIK, "source_kind": "sec_filing", "source_url": url, "title": "fabricated exhibit",
        "published_at": None if publication_unknown else stamp(public),
        "retrieved_at": stamp(fetched), "media_type": "text/html",
    }, raw)
    result = {
        "cik": CIK, "accession": ACC, "security_id": SECURITY, "version": version, "supersedes": supersedes,
        "accepted_at": stamp("12:45"), "public_observed_at": stamp(public), "retrieved_at": stamp(fetched),
        "snapshot_id": doc["snapshot_id"], "document_sha256": doc["content_sha256"], "source_url": url,
        "span": [3, len(raw) - 4], "claim_sha256": sha256(claim.encode()).hexdigest(),
    }
    result["visibility"] = {k: deepcopy(result[k]) for k in (
        "cik", "accession", "security_id", "version", "supersedes", "snapshot_id", "document_sha256",
        "span", "claim_sha256", "source_url", "accepted_at", "public_observed_at", "retrieved_at",
    )} | {"kind": "primary_post_commit_readback", "observed_at": stamp(visible)}
    return result


def inspect(root, *receipts, consumer):
    return diagnose_sec_exhibit_evidence(root, cik=CIK, accession=ACC, security_id=SECURITY,
                                        as_of=stamp("20:00"), receipts=receipts, consumer=consumer)


class TestSecExhibitEvidence(TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.tmp_path = Path(directory.name)

    def test_disabled_does_not_read_anything(self):
        tmp_path = self.tmp_path
        def bomb():
            raise AssertionError("disabled should not iterate")
            yield
        assert diagnose_sec_exhibit_evidence(tmp_path, cik=None, accession=None, security_id=None,
                                             as_of=None, receipts=bomb())["status"] == "disabled"
        assert not list(tmp_path.iterdir())

    def test_exact_cutoff_duplicate_archive_envelopes_are_one_vote(self):
        tmp_path = self.tmp_path
        first = receipt(tmp_path, visible="20:00")
        retry = receipt(tmp_path, fetched="12:50", visible="20:00")
        assert first["snapshot_id"] != retry["snapshot_id"]
        consumer = Mock(side_effect=lambda evidence: evidence)
        evidence = inspect(tmp_path, retry, first, retry, consumer=consumer)
        consumer.assert_called_once()
        assert len(evidence) == 1 and evidence[0]["claim"] == "fictional issuer reports earnings"
        assert evidence[0]["document_sha256"] == first["document_sha256"]

    def test_invalid_clock_or_identity_never_reaches_consumer(self):
        tmp_path = self.tmp_path
        for changes in [
            {"public_observed_at": None}, {"retrieved_at": stamp("20:01")}, {"security_id": "different-security"},
        ]:
            consumer = Mock()
            r = receipt(tmp_path) | changes
            assert inspect(tmp_path, r, consumer=consumer)["status"] == "no_qualifying_evidence"
            consumer.assert_not_called()

    def test_birth_and_visibility_receipt_must_bind_exact_claim(self):
        tmp_path = self.tmp_path
        for key,value in [
            ("version", 2), ("version", True), ("accepted_at", stamp("12:44")), ("claim_sha256", "a" * 64), ("span", [1, 3]), ("observed_at", stamp("20:01")),
        ]:
            r = receipt(tmp_path)
            r["visibility"][key] = value
            consumer = Mock()
            assert inspect(tmp_path, r, consumer=consumer)["status"] == "no_qualifying_evidence"
            consumer.assert_not_called()

    def test_late_correction_has_no_effect_but_timely_correction_is_forwarded(self):
        tmp_path = self.tmp_path
        first = receipt(tmp_path)
        late = receipt(tmp_path, claim="late correction", public="19:55", fetched="19:58",
                       visible="20:01", version=2, supersedes=1)
        timely = deepcopy(late)
        timely["visibility"]["observed_at"] = stamp("19:59")
        consumer = Mock(side_effect=lambda evidence: evidence)
        assert [e["version"] for e in inspect(tmp_path, late, first, consumer=consumer)] == [1]
        assert [e["version"] for e in inspect(tmp_path, timely, first, consumer=consumer)] == [1, 2]

    def test_wrong_archive_path_rejects_valid_bytes_and_hash(self):
        tmp_path = self.tmp_path
        r = receipt(tmp_path, url=URL.replace("199000001", "199000002"))
        consumer = Mock()
        assert inspect(tmp_path, r, consumer=consumer)["status"] == "unusable_visible_version"
        consumer.assert_not_called()

    def test_conflicting_version_is_not_a_vote(self):
        tmp_path = self.tmp_path
        consumer = Mock()
        assert inspect(tmp_path, receipt(tmp_path), receipt(tmp_path, claim="different claim"),
                       consumer=consumer)["status"] == "conflicting_version"
        consumer.assert_not_called()

    def test_non_monotone_amendment_abstains(self):
        tmp_path = self.tmp_path
        first = receipt(tmp_path, public="12:56", fetched="12:57", visible="12:58")
        second = receipt(tmp_path, version=2, supersedes=1)
        consumer = Mock()
        assert inspect(tmp_path, second, first, consumer=consumer)["status"] == "non_monotone_supersession"
        consumer.assert_not_called()

    def test_visible_unreadable_correction_cannot_resurrect_old_claim(self):
        root = self.tmp_path
        first = receipt(root)
        second = receipt(root, claim="replacement", public="19:55", fetched="19:56", visible="19:57",
                         version=2, supersedes=1)
        (root / (second["snapshot_id"] + ".json")).unlink()
        consumer = Mock()
        assert inspect(root, first, second, consumer=consumer)["status"] == "unusable_visible_version"
        consumer.assert_not_called()

    def test_late_unreadable_correction_has_no_effect(self):
        root = self.tmp_path
        first = receipt(root)
        second = receipt(root, claim="replacement", public="19:55", fetched="19:56", visible="20:01",
                         version=2, supersedes=1)
        (root / (second["snapshot_id"] + ".json")).unlink()
        evidence = inspect(root, first, second, consumer=lambda entries: entries)
        assert tuple(e["version"] for e in evidence) == (1,)

    def test_archived_before_cutoff_without_matching_readback_does_not_dispatch(self):
        root = self.tmp_path
        r = receipt(root)
        assert read_document(root, r["snapshot_id"])["content_sha256"] == r["document_sha256"]
        missing_readback = {k: val for k, val in r.items() if k != "visibility"}
        mismatched_readback = deepcopy(r)
        mismatched_readback["visibility"]["snapshot_id"] = "a" * 64
        consumer = Mock()
        for candidate in (missing_readback, mismatched_readback):
            with self.subTest(candidate=candidate):
                assert inspect(root, candidate, consumer=consumer) == {
                    "status": "no_qualifying_evidence", "evidence": (),
                }
        consumer.assert_not_called()

    def test_same_snapshot_readback_retry_keeps_original_clocks_in_either_order(self):
        root = self.tmp_path
        first = receipt(root, fetched="12:48", visible="12:49")
        retry = deepcopy(first)
        retry["visibility"]["observed_at"] = stamp("19:59")
        consumer = Mock(side_effect=lambda evidence: evidence)
        forward = inspect(root, first, retry, consumer=consumer)
        reverse = inspect(root, retry, first, consumer=consumer)
        assert forward == reverse and len(forward) == 1
        item = forward[0]
        assert item["snapshot_id"] == first["snapshot_id"]
        assert item["retrieved_at"].isoformat() == "2099-01-12T17:48:00+00:00"
        assert item["visible_at"].isoformat() == "2099-01-12T17:49:00+00:00"
        assert consumer.call_count == 2

    def test_null_archive_publication_cannot_be_filled_from_acceptance(self):
        root = self.tmp_path
        r = receipt(root, public="12:45", publication_unknown=True)
        assert r["public_observed_at"] == r["accepted_at"]
        assert read_document(root, r["snapshot_id"])["published_at"] is None
        consumer = Mock()
        assert inspect(root, r, consumer=consumer) == {
            "status": "unusable_visible_version", "evidence": (),
        }
        consumer.assert_not_called()
