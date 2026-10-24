# M0 acceptance check (read-only, HELD)

`scripts/m0_acceptance_check.py` produces **one dated JSON receipt**. Its verdict, `accepted`, is decided **only** by a linked chain
for ONE market session / as-of — never by counts, row existence or folder presence:

1. **L1 input observation** — `raw_series` vintage (`series_id`, `obs_date`, `pull_timestamp`, `capture_state`) that the reasoning output used;
2. **L2 materialization row** — the `resolved_series` row it produced (`feature`, `obs_date`, `vintage_date`; `materialized_at` = `vintage_date`, date granularity), same value and source, `vintage_date` ≥ the pull date;
3. **L3 reasoning output** — a `market_briefings` row (its `snapshot_data.equities[<key>]` value/date and `created_at` knowledge timestamp) **or** a `market_diary` row (`market_moves.indices[<label>]` close, or the explicit `"no close for date"` reason) that is consistent with that input;
4. **L4 retained publication bytes** — the capture packet entry for that exact row whose sha256 matches the retained capture receipt (`--capture-dir`, `--capture-receipt`).

The four context hops H1–H4 (counts, existence, folder presence) are still printed under `context_hops` / `context_overall`, but they can never set `accepted=true`.
It is an *observation* tool, not a certification.

> **HOLD NOTICE — execution on grid-svr (or any host with access to griddb) requires Anik's explicit authorization.**
> The script refuses to run against a database unless `--i-am-authorized` is passed. Passing the flag is the operator's
> assertion that this authorization exists; no other session or script may add it on their behalf. `--dry-run` prints the SQL
> and touches nothing and needs no authorization. This PR/runbook does not authorize or schedule any execution.

## Usage

```bash
# print every statement, bound parameter and transaction wrapper; no database is contacted
python3 scripts/m0_acceptance_check.py --date 2026-10-19 --dry-run

# HELD: only with the operator's authorization (reads griddb through the repo engine / .env)
python3 scripts/m0_acceptance_check.py --date 2026-10-19 --i-am-authorized \
    --reasoning briefing \
    --capture-dir /path/to/grid-diary-...-handoff/capture/<export_sha256> \
    --capture-receipt /path/to/CLAUDE-...-CAPTURE-RECEIPT-....md \
    --out /path/to/m0-receipt-2026-10-19.json
```

Chain arguments: `--series` (default `YF:^GSPC:close`), `--feature` (`sp500_full`), `--snapshot-key` (`^GSPC`, key under `snapshot_data.equities`), `--diary-label` (`S&P 500`), `--reasoning {briefing,diary}`, `--reasoning-id` (pin a `market_briefings.id`; otherwise the earliest briefing in `[date, date+3d]` whose snapshot value for the series is dated `date` is used).
`--capture-dir` is the retained `capture/<export_sha256>/` directory (`capture.json`, `packet.json`); `--capture-receipt` is the retained executor receipt (`.md`, rows `| id | type | created_at | row sha256 |` plus the `capture.json` sha line) or a `.json` `{"export_sha256": ..., "rows": [{"channel": "market_briefings"|"market_diary", "id": N, "sha256": ...}]}`.

Options: `--tickers` (default `^GSPC,SPY`, used by the census and source-mix queries), `--lookback-days` (H2 window, default 60),
`--max-age-days` (H2 freshness tolerance vs the target date, default 5), `--strict` (exit 1 unless `accepted`), `--out`
(created exclusively; an existing file is never overwritten; default `m0-acceptance-receipt-<date>-<UTC stamp>.json` in the CWD).
Exit codes: `0` receipt written (or dry-run), `1` `--strict` and not `accepted`, `2` refused / bad arguments / receipt path exists.

## Safety properties (enforced by `tests/test_m0_acceptance_check.py`)

- Every query is its own `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` transaction with `SET LOCAL statement_timeout <= 20s`
  (and `lock_timeout 2s`), always ending in `ROLLBACK`. Only a single `SELECT`/`WITH` statement is accepted (`assert_read_only_sql`).
- `raw_series` statements (1.9e9 rows) carry the planner hints from the freshness request (`enable_hashjoin/mergejoin/seqscan/bitmapscan = off`, applied with `set_config(name, value, true)` bind parameters, never string-built SQL)
  and equality/range bounds on `(series_id, obs_date)` so they use `idx_raw_series_lookup`; the others use date windows
  (`>= :floor`), `LIMIT`s, or the date-unique `market_diary.date` / `market_briefings.briefing_date`.
- The only file written is the receipt. No provider/API call, no write to any table, no service interaction.
- Statements Q1 (source_catalog / clock), Q2 (capture-state census), Q7 (source mix) are the ones specified in
  `CLAUDE-FRESHNESS-EVIDENCE-REQUEST-20261019.md`, with psql variables replaced by bind parameters.
- If a query errors or times out, the receipt records the error and the dependent checks become `null` (UNKNOWN), never PASS.

## The linked chain — how each link is decided

| link | decided from | ok when | does **not** prove |
|---|---|---|---|
| **L1** | `H1b`: SUCCESS `raw_series` vintages of `--series` at `obs_date = --date`, source 2 | a vintage exists and carries `raw_payload->>'capture_state'`; it is the vintage the reasoning output used (else the newest, reported as such) | that the value equals the provider's final close |
| **L2** | `L2`: `resolved_series` rows of `--feature` at the date | a row has the input's value, `source_priority_used` = input source and `vintage_date` ≥ the pull date. Retraction state is **not** read | that the resolver ran *because of* this pull beyond value/source/date equality; time of day (only a date exists) |
| **L3 briefing** | `L3:briefings` | the briefing's `snapshot_data.equities[key]` is dated `--date` and its value equals an input vintage pulled **before** the briefing's `created_at` | that the narrative text is correct; absence in a briefing is **not** accepted (a briefing has no explicit unavailable reason) |
| **L3 diary** | `H3:diary` | priced entry: close/obs_date equal a `POLICY_ELIGIBLE` vintage pulled before `generated_at`; or the explicit `"no close for date"` while **no** `POLICY_ELIGIBLE` vintage existed before generation | grounding/narrative quality |
| **L4** | local files only | `capture.json` sha256 = directory name = receipt export sha; the row's canonical sha256 (sort_keys, compact separators, `ensure_ascii=False`) = the receipt's row sha; `packet.json` has the entry for exactly that row (text sha256, `source_record` equal to the capture record, export sha equal); the DB row's id/`created_at` (briefing) or `generated_at` and index entry (diary) equal the retained record | that anything was published externally. A markdown receipt has no diary row hashes, so a diary chain needs a JSON receipt |

A link that errored or lacks its inputs is `null` (UNKNOWN) and also blocks acceptance; the receipt lists `blocking_links`.

## Context hops (never set `accepted`) — what each proves and does not

| hop | queries | checks (receipt `hops.<H>.checks`) | proves | does **not** prove |
|---|---|---|---|---|
| **H1 ingestion** | Q1a clock/read-only proof, Q1b source_catalog, H1a `pull_log` `YFinance_Daily` rows for the date, H1b `raw_series YF:^GSPC:close` vintages for the date (`pull_timestamp`, `raw_payload->>'capture_state'`), Q2 census, Q7 source mix | `db_read_only`, `pull_log_has_run`, `rows_inserted_positive`, `input_vintage_present`, `input_vintages_have_capture_state` | rows were written for that date by the scheduler and vintages carry a capture state | that the stored close equals the provider's final close (needs a provider read, out of scope); that any reader shows it; that other tickers were written (only the census list is checked) |
| **H2 materialization** | max date / max `created_at` / row count in `[date − lookback, ∞)` for `insider_trades`, `congressional_trades`, `dark_pool_weekly`, `etf_flows`, `junction_point_readings`; `resolved_series` max `vintage_date` for `spy_full`, `xlk_full` | `<table>_fresh` (newest date within `--max-age-days` of the target), `<feature>_vintage_recent` | the tables/features have rows whose newest date is recent | that the writer (grid-hermes `flow_materializer`) ran today; per-ticker completeness; value correctness; retraction-awareness (the `resolved_series` read counts vintages and ignores `resolved_series_retractions`) |
| **H3 reasoning** | `market_diary` row for the date | `diary_row_present`, `narrative_model_present`, `price_basis_present`, `priced_indices_carry_basis_and_source`; evidence lists indices reporting `no close for date` | a diary row exists, names its price basis/source, and which indices were refused for lack of a qualified close | that the narrative is grounded or right; that `no close for date` is a defect (it is the designed refusal until a `POLICY_ELIGIBLE` capture exists); market_briefings grounding fields |
| **H4 publication retained** (context only) | `market_briefings` rows for the date; `--capture-dir` presence (local `os.path`, bounded `listdir` count) | `market_briefings_rows_for_date`, `capture_folder_present_and_nonempty` (UNKNOWN if no path is given) | a briefing row was retained and the capture folder exists and is non-empty | that anything was published externally, or that capture content matches the DB rows |

Three timestamps stay separate in the receipt (`queries[].timestamp_kind`): **provider observation** (`obs_date`, flow-table date columns),
**ingestion/materialization** (`pull_timestamp`, `pull_log.started_at`, `created_at`), and **reader** (`resolved_series.vintage_date`,
`market_diary.generated_at`, `market_briefings.created_at`).

## Receipt (`schema: m0-acceptance-receipt-v1`)

`schema`, `generated_at_utc`, `target_date`, `host`, `read_only: true`, `authorized_flag: true`, `arguments`, **`accepted`** (true only if L1–L4 are all true),
`chain{accepted, blocking_links, rule, links{L1_input_observation, L2_materialization_row, L3_reasoning_output, L4_retained_publication_bytes}}`,
`context_overall` (`PASS`/`FAIL`/`UNKNOWN` over the context hops), `context_hops{H1..H4}{name, checks, status, evidence, proves, does_not_prove}`,
`queries[]{qid, purpose, timestamp_kind, error, row_count}`, `notes`.

## Interpreting results

- Run it for a trading date after the 22:0xZ `YFinance_Daily` slot (H1), after the daily Hermes block (H2) and after the diary/briefing jobs (H3/H4);
  an earlier run is expected to leave links false/UNKNOWN, not to be a script error.
- A weekend or holiday target date legitimately has no `pull_log` rows or GSPC vintages (equity pullers skip).
- `H2` FAIL on the flow tables with grid-hermes OFF is the expected signature of the materialization gap (see `ingestion/flow_materializer.py`
  and `scripts/hermes_operator.py:1508`).
