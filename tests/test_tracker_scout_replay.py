import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from analysis.research_trackers.replay import replay


class Replay(unittest.TestCase):
    def test_four_sources_are_research_only_end_to_end(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            payloads = {
                "ercot.txt": b"PUBLIC  Loads Approved to Energize - Observations\n"
                             b"Of the 100 MW that have received Approval to Energize, ERCOT has observed a "
                             b"non-simultaneous monthly peak consumption of 90 MW in September 2026.\n9\n",
                "barge.json": json.dumps([dict(date="2026-11-17T00:00:00.000", week="46", month="11",
                                               year="2026", location="St. Louis")]).encode(),
                "permits.csv": (b"moratorium_id,state_abbrev,jurisdiction,jurisdiction_type,date_enacted_iso,"
                                b"date_enacted_uncertainty,enacted_status,sectors\n"
                                b"xx-site-2026,CA,Example,city,2026-09,month_only,active,\"[\"\"data_center\"\"]\"\n"),
            }
            for kind, rate in (("SOFR", 3.9), ("TGCR", 3.88)):
                payloads[kind + ".json"] = json.dumps({"refRates": [dict(
                    type=kind, effectiveDate="2026-11-19", percentRate=rate, revisionIndicator=""
                )]}).encode()
            for path, data in payloads.items():
                (root / path).write_bytes(data)
            seen = "2026-11-22T00:00:00Z"
            configs = (
                dict(source="ercot_monthly", payload="ercot.txt", metadata=dict(
                    source_url="https://www.ercot.com/files/docs/2026/10/16/ERCOT-Monthly-Operational-Overview-September.pdf",
                    released_on="2026-10-16", first_seen_at=seen, report_month="2026-09", page=9)),
                dict(source="usda_barge", payload="barge.json", metadata=dict(first_seen_at=seen)),
                dict(source="moratorium_nation", payload="permits.csv", metadata=dict(
                    release_tag="v2026.11.11.post13", expected_sha256=hashlib.sha256(payloads["permits.csv"]).hexdigest(),
                    first_seen_at=seen)),
                dict(source="nyfed_repo_pair", sofr_payload="SOFR.json", tgcr_payload="TGCR.json", metadata=dict(
                    sofr_source_id="ny_fed_sofr", sofr_seen_at=seen, tgcr_seen_at=seen)),
            )
            manifest_path = root / "manifest.json"
            for config in configs:
                with self.subTest(source=config["source"]):
                    manifest_path.write_text(json.dumps(dict(config, schema="grid-tracker-replay/1")))
                    result = replay(manifest_path)
                    self.assertEqual(result["lane"], "research_only")
                    for flag in ("eligible_for_backtest", "eligible_for_scoring", "promotion_allowed"):
                        self.assertIs(result[flag], False)
                    if config["source"] == "moratorium_nation":
                        self.assertEqual(result["result"]["records"][0]["evidence_class"], "research_lead")
                    if config["source"] == "nyfed_repo_pair":
                        self.assertIs(result["result"]["records"][0]["first_release_verified"], False)

    def test_invalid_source_or_path_refused(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.json"
            for source, target in (("made_up", "x"), ("usda_barge", "../out.json"), ("usda_barge", "/out.json")):
                path.write_text(json.dumps(dict(schema="grid-tracker-replay/1", source=source, payload=target,
                                                metadata={"first_seen_at": "2026-11-22T00:00:00Z"})))
                with self.assertRaises(ValueError):
                    replay(path)
