# Tracker Scout offline replay

This is a GRID-native **research preview**, not part of the live collector,
market-state inputs, Desk receipts, signals, or alerts. Run from repo root:

```sh
python -B -m unittest discover -s tests -p 'test_tracker_scout_*.py' -v
python -B -m analysis.research_trackers.replay /path/to/manifest.json
```

Use a local manifest with `schema: "grid-tracker-replay/1"`, `source` and
`metadata`. Payload filenames must be relative to the manifest directory.
The preview returns JSON on stdout. Capture clocks are caller-attested; set
them from actual records, not from observation or publication dates. The
preview marks every result ineligible for scoring, promotion and backtests.

| source | payload keys | metadata | scope |
| --- | --- | --- | --- |
| `ercot_monthly` | `payload` (text, e.g. locally produced by `pdftotext -layout`) | `source_url`, `released_on`, `first_seen_at`, `report_month`, `page` | Official approved MW and sum of individual monthly peak MW, not simultaneous demand or queue conversion. |
| `usda_barge` | `payload` (JSON) | `first_seen_at` | Percent of the route tariff; keep missing values unavailable, no imputed publication time. |
| `moratorium_nation` | `payload` (CSV) | `release_tag`, `expected_sha256`, `first_seen_at` | Research leads only. Exact day, month/year precision and unknown dates stay distinct; no claim of applicability, restriction in force or delay. |
| `nyfed_repo_pair` | `sofr_payload`, `tgcr_payload` (JSON) | `sofr_source_id: "ny_fed_sofr"`, `sofr_seen_at`, `tgcr_seen_at` | Explicit existing SOFR capture export plus separately saved TGCR; exact-date descriptive join. No first print inferred. |

The legacy `scripts/sources_expanded.py` stores a reserialized SOFR response in
`raw_ingest`, source `ny_fed_sofr`. Its UTC-as-naive `fetched_at` can only be
converted to an offset-aware capture timestamp when that writer's provenance is
verified. Its fetch date is **not** the economic effective date or the official
publication date. A preserved payload is hashed as exported, not as original
HTTP bytes. This change adds no fetcher, export job, or database access.

Source references: [ERCOT September report](https://www.ercot.com/files/docs/2026/10/16/ERCOT-Monthly-Operational-Overview-September.pdf),
[USDA AMS downbound grain barge data](https://agtransport.usda.gov/Barge/Downbound-Grain-Barge-Rates/deqi-uken),
[Moratorium Nation](https://github.com/mjbommar/moratorium-data-2026) (attribute
CC BY 4.0 data), [NY Fed markets API](https://markets.newyorkfed.org/static/docs/markets-api.html)
and [terms of use](https://www.newyorkfed.org/privacy/termsofuse). NY Fed
reference-rate presentation/distribution has source attribution and disclaimer requirements.
The draft includes no upstream raw data.

Previously validated official samples: three ERCOT PDFs; 1,329 Moratorium
Nation CSV rows; 1,000 USDA JSON rows (51 unavailable, none rejected); three NY
Fed pairs (no authenticated first prints). These are parse/metadata checks,
not a market edge or publication approval.

Claude review: run the exact test command above at the reviewed head. Check
that the changed paths contain only these preview modules, their tests and
this note; test unsafe and unavailable inputs; verify no release, scheduler,
consumer registration, or new SOFR collector is introduced. Do not run the
legacy capture job as part of this review.
