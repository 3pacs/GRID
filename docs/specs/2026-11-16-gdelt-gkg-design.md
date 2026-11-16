# GDELT GKG: tone, tension, breaking news and company mentions from the bulk files (design, for agreement)

Status: **agreed by Anik 2026-11-16, as recommended**, with three additions:
- move the DOC callers and breaking news onto the bulk/GKG path now, a small slice at a time;
- Bessent replaces Yellen;
- nothing feeds a model until a hand-read shows at least 90% precision.

PR 1 (ledger `kind`, GKG parser, `gdelt_gkg_daily`, `gkg_*` series, held Hermes task `gdelt_gkg_daily`) is built. See "Weekday measurement" below for the lists it uses.

This is slice 3 of moving GDELT onto its bulk files:
- slice 1 is the export loader, 3pacs/GRID#1232;
- slice 2 is the historical correction, 3pacs/GRID#1233;
- item 3 of 3pacs/GRID#1180 (company-level triggers) shares this design.

## Why

These GRID users of GDELT go through the DOC API:
- about 40 series in `ingestion/altdata/gdelt.py` (`gdelt_tension_*`, `gdelt_actor_*_tone`, `gdelt_fed_tone`, ...);
- the `intelligence/breaking_news.py` monitor (`signal_data`, `signal_type='breaking_news'`);
- `actor_news`.

The DOC API answers 429 from every IP tried. Results:
- the tone series last updated between 2026-10-20 and 11-12;
- the last `breaking_news` signal was 2026-11-14 (32,054 rows in all);
- `analysis/thesis_scorer.py`'s geopolitical part reports `no_data`.

GDELT's Global Knowledge Graph (GKG 2.1) is published every 15 minutes next to the export files (`<stamp>.gkg.csv.zip`, listed in `lastupdate.txt`). It carries per-document tone, themes, persons, organizations and locations.

**The new series measure something different from the old ones**, so they get new names (`gkg_*`) and start fresh:
- the DOC API's `timelinetone` is GDELT's own query-matched article tone, computed server-side;
- the replacement is the mean V2Tone of the documents that match coded fields.

## Measured (grid-svr, 2026-11-16; four files: 2026-11-15 12:00/12:15/18:00, 11-16 03:00 UTC)

- **Size.** About 480 documents per 15-minute file (a weekend and off-hours sample; weekdays will be higher), every row 27 columns.
  - 1.6–2.3 MB zipped, about 6 MB unzipped.
  - Parsing all four took 0.6 s (pure Python `csv`).
  - A day is about 96 × 2 MB ≈ 190 MB to download and about 15 s to parse.
- **Tension pairs** (documents whose V2Locations hold both FIPS country codes), 4 files:

  | pair | docs | mean tone |
  |---|---:|---:|
  | US–China | 62 | −0.80 |
  | US–Iran | 36 | −3.58 |
  | Russia–Ukraine | 39 | −2.01 |
  | US–Russia | 32 | −1.59 |
  | Israel–Iran | 27 | −4.94 |
  | India–China | 14 | −1.46 |
  | China–Taiwan | 10 | −2.40 |

  A daily series has enough documents per pair.
- **Persons** (V2Persons, lower-case): in 4 weekend files, "donald trump" had 63 documents, "vladimir putin" 9, "scott bessent" 6. Powell, Lagarde, Xi, MBS and Ueda had none. Weekday counts must be measured before these series are trusted.
- **Themes.** Some GKG theme codes stand in for the DOC keyword queries, but sparsely in 4 files: `ARMEDCONFLICT` 217, `ECON_INFLATION` 26, `SANCTIONS` 11, `ECON_INTEREST_RATES` 9, `ECON_TRADE_DISPUTE` 2. `ECON_RECESSION` and `TAX_FNCACT_CENTRAL_BANK` did not appear. **The theme set has to be chosen from a full weekday, not from this sample.**
- **Companies (#1180 item 3).** V2Organizations names were matched to `supply_chain_nodes` names after lower-casing and stripping legal suffixes (inc, corp, ltd, plc, ...).
  - 110 of 1,925 documents (5.7%) named a graph node: 21 distinct nodes.
  - Top: nvda 46, msft 24, ba 17, nflx 8, saudi_aramco 5, wmt 5.
  - Over a day that is about 2,600 documents. **The precision of this name match is not measured yet.**
- **History on disk.**
  - No v2 GKG files are stored (`/data/gdelt/v2/english` has export and mentions only).
  - `/data/gdelt/v1/gkg` holds 9,470 GKG 1.0 daily files. GKG 1.0 has themes, persons, organizations, locations and tone, but no V2 character offsets.
  - Whether a GKG 1.0 daily file equals its day's GKG 2.1 files must be measured the way slice 1 did for events, before it is used as history.

## Weekday measurement (Friday 2026-11-13, all 96 GKG files, grid-svr)

This answers decision 3. The theme sets are chosen from these counts.

- **Volume.** 104,883 documents (642 to 1,766 per file), 444.7 MB zipped, every row 27 columns, 9,090 distinct theme codes. That is about 160 GB a year to download. Nothing raw is kept.
- **Pairs** (docs, mean tone):

  | pair | docs | mean tone |
  |---|---:|---:|
  | US–China | 3,326 | −1.18 |
  | US–Iran | 2,741 | −2.58 |
  | Russia–Ukraine | 2,127 | −1.28 |
  | US–Russia | 2,028 | −1.40 |
  | Israel–Iran | 1,013 | −3.05 |
  | India–China | 695 | −0.15 |
  | China–Taiwan | 591 | −1.41 |

  Every pair is usable daily.
- **Persons** (exact lower-cased names in Persons; in AllNames for Xi):
  - Putin 1,285 and Bessent 341.
  - Xi: Persons had none, but AllNames found him (32 documents in 12 files, about 150 a day). He is matched on Persons or AllNames.
  - Powell 10 and Lagarde 7.
  - Bin Salman 0 and Ueda 1: **left out**, because a series of zeros says nothing.
  - Kevin Warsh had 42 and Kevin Hassett 16. They are not added: **your call**.
  - "kimberly powell" (434, at Nvidia) and "jonathan powell" (203) are why names match exactly, not by surname.
- **Theme sets** (a document matches if it carries any code in the set):
  - armed_conflict: `ARMEDCONFLICT` 11,273
  - inflation: `ECON_INFLATION` 2,307, `WB_442_INFLATION` 2,167
  - rates: `ECON_INTEREST_RATES` 1,473, `EPU_POLICY_INTEREST_RATE(S)` 900 / 840
  - fed: `EPU_POLICY_FEDERAL_RESERVE` 825
  - central_bank: `ECON_CENTRALBANK` 691, `WB_1235_CENTRAL_BANKS` 688, `EPU_POLICY_CENTRAL_BANK` 671
  - sanctions: `SANCTIONS` 1,132
  - oil: `ECON_OILPRICE` 1,222, `ENV_OIL` 2,978
  - currency: `ECON_CURRENCY_EXCHANGE_RATE` 643, `EPU_CATS_SOVEREIGN_DEBT_CURRENCY_CRISES` 245
  - trade_dispute: `ECON_TRADE_DISPUTE` 102
  - defense_spending: `SLFID_MILITARY_SPENDING` 142
  - The DOC topics recession, chip export controls and Taiwan strait have no theme code with 50 or more documents. They get no theme series; Taiwan is covered by the China–Taiwan pair, and chips by company mentions (item 4).
- **Precision gate.** Every `gkg_*` feature is registered `model_eligible = FALSE`. One may be switched on only after:
  - 30 days of the series exist;
  - a hand-read of at least 100 matched documents for its group shows at least 90% are about the group;
  - the read is recorded in this doc.

## Design

**1. One loader, two file kinds.** This adapts slice 1 rather than adding a second loader.
- `ingestion/altdata/gdelt_bulk.py` gains a `gkg` kind next to `export`. It shares the stamps, fetch, Last-Modified dating, ledger, advisory lock and budget.
- The ledger key becomes `(kind, file_stamp)`. #1232 is unmerged, so its migration can take the column now instead of a second migration later.
- Raw GKG rows are **not stored** (46k+ wide documents a day). Each file is reduced to per-file aggregates in its ledger row, plus the company-mention rows in item 4.

**2. Daily series, new names.**
- A day is the 96 GKG files stamped on that UTC day.
- `release_date = vintage_date` is the UTC date of the latest Last-Modified among them, the same rule as slice 1. That is usually D, published about 23:33 UTC. The same-day question in #1232 applies here too.
- Each series has a `_tone` (mean V2Tone) and a `_docs` (document count), written straight to `resolved_series` under source `GDELT_GKG`:

| series | match on | replaces (DOC) |
|---|---|---|
| `gkg_pair_{us_china,us_russia,us_iran,china_taiwan,russia_ukraine,israel_iran,india_china}_{tone,docs}` | both FIPS codes in V2Locations | `gdelt_tension_*` |
| `gkg_person_{powell,lagarde,xi,putin,mbs,bessent,ueda}_{tone,docs}` | exact V2Persons name | `gdelt_actor_*_tone` (Yellen → Bessent, the current Treasury Secretary: **your call**) |
| `gkg_theme_{...}_{tone,docs}` | a fixed theme-code set per topic, chosen on a full weekday | `gdelt_{recession,fed,trade_conflict,sanctions_russia,taiwan_strait,opec,chip_controls,central_bank,em_currency,defense_spending}_*` |
| `gkg_docs_total` | every document | the denominator for shares |

- About 40 to 50 series.
- **Registration.**
  - New `feature_registry` rows with `model_eligible = FALSE` until each has 30 days, so no model picks one up half-filled.
  - Through a migration, like `raw_sql_port_20260914` / `ghost_features_20261105`.
  - `normalization/entity_map.py` does not need the new names, because they are written to `resolved_series` directly.
- **Consumers.**
  - `analysis/thesis_scorer.py` (geopolitical part): moves from `raw_series` `gdelt_tension_%` to `gkg_pair_*_tone`.
  - `physics/momentum.py` and `intelligence/signal_extractor.py`: the same move.
  - Each move is its own small change with its own test.

**3. Breaking news from 15-minute files.**
- `grid-breaking-news` stops polling the DOC API (about 3,500 requests a day, all refused). Instead it reads each new GKG file when `lastupdate.txt` names it, through the slice-1 loader with kind `gkg`.
- **Spike rule.** It keeps the 12 topics in `intelligence/breaking_news.py`, each mapped to theme codes. A topic spikes when its document count in the newest file exceeds 3× its median over the previous 96 files (one day), with a minimum count.
- **Signals.** Each signal cites the top documents' URLs. It is written as `signal_type = 'breaking_news_gkg'`, a new type because the measure differs. Readers (`global_levers`, `news_intel`, `signal_backlinker`, canvas routers) are pointed at the new type in the same PR.
- **Latency.** At most 15 minutes plus GDELT's lag. The old monitor promised 60 s but has not had an answer since 2026-11-14.

**4. Company mentions (#1180 item 3).**
- New table `gdelt_gkg_org_mentions (file_stamp, node_id, document_url, tone, themes text[], published_at)`. One row per document naming a `supply_chain_nodes` company, matched by normalised exact name plus a small alias list (e.g. "alphabet", "google" → googl). About 2,600 rows a day.
- **Trigger.** A node whose daily mentions exceed 3× its 30-day median, with a minimum count, becomes a listener trigger citing the document URLs. That is the same citation rule as the 8-K edges.
- **Not before precision is measured.** Triggers stay off until a hand-read of 100 matched documents shows the name match is right (watch e.g. "apple" the fruit, "ba" as a word, "target"). That is measured on the first full week and reported before the triggers are switched on.

**5. Scheduling and holds.**
- **Daily series:** a Hermes daily-intel task `gdelt_gkg_daily`, held in `DAILY_INTEL_HOLD_REASONS` until you release it.
- **Breaking news:** the existing `grid-breaking-news` unit, with the new code behind a flag that defaults to off.
- No LLM anywhere. `GRID_ALLOW_PAID_LLM` is untouched.

**6. DOC callers.**
- Once each replacement has 30 days, stop the DOC calls: `breaking_news` DOC polling, `gdelt.py` `pull_recent` theme/actor/tension queries, and `actor_news_puller`'s GDELT batch.
- Leave the old series in place, frozen, with no deletion.
- This also clears the shared DOC gate's permanent cooldown.

**7. History (later, measured first).**
- If GKG 1.0 daily files match GKG 2.1 by day, `/data/gdelt/v1/gkg` gives 2013–2026 history for the daily series. They would be dated by each file's Last-Modified (about 07:00 UTC D+1).
- Otherwise the series start on the first day loaded and the backtests wait.

## Order of PRs (smallest first)

1. Ledger `kind` column (amend #1232 before it merges) and the `gkg` parser with daily aggregates. Tests on fixture GKG zips. Held Hermes task.
2. Series registration and the consumer moves (thesis scorer first).
3. Breaking-news daemon on GKG, behind a flag.
4. Company mentions table plus the precision hand-read, then triggers (#1180 item 3).
5. DOC callers off, after 30 days of overlap.

## Decisions for you

1. The series list and the `gkg_*` names above.
2. Yellen → Bessent.
3. Theme sets: OK to choose them from one full weekday's counts (reported in PR 1)?
4. The company-mention table and the precision gate before any trigger.
5. Moving `grid-breaking-news` off the DOC API, with the new signal type `breaking_news_gkg`.
6. Turning the DOC callers off after 30 days of overlap.

## Whether the data/thesis got stronger or weaker

- **Weaker today:** the DOC-based series are frozen, and the geopolitical thesis part reports `no_data`.
- **After this:** a free, rate-limit-free source with document-level citations replaces a source that refuses GRID.
- **Untested:** no thesis has been tested on the new measures, and they are not the old ones. Any result that used the old tone series doesn't carry over and has to be re-run on the new names.
