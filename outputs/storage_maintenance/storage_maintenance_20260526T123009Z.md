# GRID Storage Maintenance — 2026-05-26T12:30:09.659529+00:00

- Status: `ingest_gap`
- Target: `grid-svr-data`
- Cold storage root: `/mirror`

## Filesystems

- `/data` exists=True used=8588.218GiB free=2498.598GiB use_pct=77.5
- `/mirror` exists=True used=8065.354GiB free=8630.813GiB use_pct=48.3

## GDELT

- Ingest status: `archives_present_db_tables_missing`
- State counts: `{"v1_events.done": 4848, "v1_gkg.done": 9470, "v2_english.done": 1149248, "v2_translation.done": 1133693}`
- Directory file counts: `{"parser_root": 755, "v1_events": 4848, "v1_gkg": 9468, "v2_english": 1149248, "v2_translation": 1133693}`
- DB tables: `{"gdelt_daily_summary": {"exists": false, "rows": null}, "gdelt_events": {"exists": false, "rows": null}}`
- Recommendation: Create/extend a GDELT bulk parser job before moving archives; live DB lacks gdelt_events/gdelt_daily_summary.
- Recommendation: scripts/parse_gdelt.py only reads /data/grid/bulk/gdelt; /data/gdelt/v1/events has more files and needs parser coverage or a curated ingest symlink.
- Recommendation: GDELT v2 English/translation archives are massive; keep them on /mirror after parser/index coverage is proven.

## Root Scans

- `/data/gdelt` exists=True files_seen=25000 archives_sampled=25 archive_seen=257.824GiB truncated=True
  - 1.01GiB `/data/gdelt/v1/events/GDELT.MASTERREDUCEDV2.1979-2013.zip`
  - 0.18GiB `/data/gdelt/v1/events/2003.zip`
  - 0.16GiB `/data/gdelt/v1/events/2001.zip`
  - 0.15GiB `/data/gdelt/v1/events/1999.zip`
  - 0.15GiB `/data/gdelt/v1/events/2004.zip`
- `/data/bulk_data` exists=True files_seen=2686 archives_sampled=25 archive_seen=4.145GiB truncated=False
  - 1.01GiB `/data/bulk_data/gdelt/master_1979_2013.zip`
  - 0.55GiB `/data/bulk_data/fnspid/full_history.zip`
  - 0.5GiB `/data/bulk_data/wikipedia/pageviews-20260101-automated.bz2`
  - 0.44GiB `/data/bulk_data/wikipedia/pageviews-20260301-automated.bz2`
  - 0.41GiB `/data/bulk_data/wikipedia/pageviews-20260201-automated.bz2`
- `/data/datasets` exists=True files_seen=7157 archives_sampled=25 archive_seen=24.821GiB truncated=False
  - 4.1GiB `/data/datasets/layline_insider/layline-insider-trading.zip`
  - 3.95GiB `/data/datasets/fec_contributions_2024.zip`
  - 3.81GiB `/data/datasets/patentsview/g_persistent_assignee.tsv.zip`
  - 2.64GiB `/data/datasets/clinicaltrials_full.zip`
  - 1.96GiB `/data/datasets/uk_psc/psc-snapshot-2026-03-30.zip`
- `/data/grid/bulk` exists=True files_seen=1027 archives_sampled=25 archive_seen=10.904GiB truncated=False
  - 0.22GiB `/data/grid/bulk/eia/electricity.zip`
  - 0.12GiB `/data/grid/bulk/edgar/financials_2025q3.zip`
  - 0.12GiB `/data/grid/bulk/edgar/financials_2025q1.zip`
  - 0.12GiB `/data/grid/bulk/edgar/financials_2024q1.zip`
  - 0.11GiB `/data/grid/bulk/edgar/financials_2024q4.zip`
- `/data/archive` exists=True files_seen=192 archives_sampled=0 archive_seen=0.0GiB truncated=False

## Ingest Plan

- `run_gdelt_parser_smoke`: python3 scripts/parse_gdelt.py

## Cleanup Plan

- `verify_existing_cold_copy_then_consider_active_removal`: `/data/gdelt/v1/events/GDELT.MASTERREDUCEDV2.1979-2013.zip` -> `/mirror/gdelt/v1/events/GDELT.MASTERREDUCEDV2.1979-2013.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/bulk_data/gdelt/master_1979_2013.zip` -> `/mirror/bulk_data/gdelt/master_1979_2013.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/bulk_data/fnspid/full_history.zip` -> `/mirror/bulk_data/fnspid/full_history.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/bulk_data/wikipedia/pageviews-20260101-automated.bz2` -> `/mirror/bulk_data/wikipedia/pageviews-20260101-automated.bz2` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/layline_insider/layline-insider-trading.zip` -> `/mirror/datasets/layline_insider/layline-insider-trading.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/fec_contributions_2024.zip` -> `/mirror/datasets/fec_contributions_2024.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/patentsview/g_persistent_assignee.tsv.zip` -> `/mirror/datasets/patentsview/g_persistent_assignee.tsv.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/clinicaltrials_full.zip` -> `/mirror/datasets/clinicaltrials_full.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/uk_psc/psc-snapshot-2026-03-30.zip` -> `/mirror/datasets/uk_psc/psc-snapshot-2026-03-30.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/sec_submissions.zip` -> `/mirror/datasets/sec_submissions.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/sec_submissions2.zip` -> `/mirror/datasets/sec_submissions2.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/sec_companyfacts.zip` -> `/mirror/datasets/sec_companyfacts.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/patentsview/g_persistent_inventor.tsv.zip` -> `/mirror/datasets/patentsview/g_persistent_inventor.tsv.zip` delete_source=False
- `copy_to_cold_storage_then_verify_manifest`: `/data/datasets/patentsview/g_inventor_disambiguated.tsv.zip` -> `/mirror/datasets/patentsview/g_inventor_disambiguated.tsv.zip` delete_source=False
