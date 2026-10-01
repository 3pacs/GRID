# E2: the forward scoreboard (v1)

E2 is milestone M2 of the GRID evals plan. It is the **only** objective a future
self-improving engine may climb. In-sample and backtest numbers never enter it.

## What it scores

Every forward-logged prediction stream is read by a pinned adapter and normalized
into one record (`records.py`):

- `stream`, `family`, `sector`, `prediction_id` and `issued_at`;
- `log_receipt`: the source file, its line index, the line's sha256 and `prev_sha256`
  (its hash-chain position), the source head E2 saw, the writer's code sha, and the
  witness basis;
- `target`, `horizon`, `outcome_not_before`;
- `call`: direction, probability, rank score, conditional level call, rule trade or signal;
- `rule_id`: the pre-registered scoring rule in `rules.json`.

| Stream | Adapter | Predictions | Rule(s) | Disclosure |
|---|---|---|---|---|
| S10 hypothesis forward log v1 | `adapters/s10.py` | one per non-excluded decision | `s10.ts_ic.v1`: signed time-series IC per candidate, over the S10 verdict's own pairs, checked against the verdict | **sealed until the S10 verdict**, because S10 allows one look |
| GEX-levels v1 paper log | `adapters/gex_levels.py` | per valid session: each tested level (real and mirror placebo), plus the H3 rule trade (real and placebo) | `gex.level_hold.v1`: hit rate with a Wilson CI. `gex.h3_net_pnl.v1`: net-of-cost P&L | **interim** (labelled) until 60 valid sessions, the pre-registered look |
| future streams (VS1/v8 survivors, GEM-derived) | `adapters/e2_stream.py` (`e2-stream-v1` log format) | as logged | `e2.direction.v1` (hit + net P&L), `e2.probability.v1` (Brier + log-loss), `e2.rank_ic.v1` (Spearman rank IC per date) | open |

Notes on specific rules:

- **Placebo arms** are scored as `role: control`. A stream-level rollup never pools them with candidates.
- **GEX H1** (regime vs range) is one cross-session regression with no per-prediction
  score. E2 v1 does not score it; the stream's own 60-session look governs it.

## Point-in-time resolution

A record resolves only once its outcome was observable at the run instant.

- **Live streams.** These log their own outcomes. The adapters re-check every timing
  rule from the log itself:
  - **GEX:** the pre-open was written before 09:30 ET. The post-close follows it in the
    chain and was written by the run instant, and its bars and OHLC were fetched at or
    after the session close.
  - **S10:** each outcome was read at or after `label_known_at`, and its prediction was
    logged before that. The feature was known by the decision, and the decision
    follows the admission.
- **Generic price-resolved streams.** These resolve through `resolve.py`:
  - `KnownAtCloseSource` reads `raw_series` through `store.observations.read_window_known_at`,
    using pull evidence only and one named source.
  - `SpyCloseReceiptSource` reads `astrogrid.price_close_receipt`.
  - A close is observable at `max(pull_timestamp, 16:00 ET)`. A revision pulled after
    the run instant is never used.
  - Any source that offers a close "available" before its session closed, or after
    the run instant, raises `LookAheadError`. The refusal is contained to that one
    prediction: it is recorded as void (`lookahead_refused`, never scored), one
    `integrity_alert` is appended for it, and the rest of the stream carries on.
  - The entry close must come after the prediction was logged.
- **Resolution receipts** carry the price source, the series and the vintage
  (pull timestamp or fetch time) of every price used.

## Scoring, cost model and uncertainty

- **Rules** are in `rules.json`; the code is `scoring.py` and `board.py`:
  - Brier and log-loss (`p` clipped to `[1e-6, 1-1e-6]`);
  - hit rate with a Wilson 95% CI;
  - Spearman rank IC per date, with at least 5 names;
  - a signed time-series IC for S10;
  - net-of-cost P&L.
- **Cost model:** `cost_model.json`, in basis points per side, charged on both sides:
  half-spread, plus commission, plus slippage, set per instrument class.

  | Class | bp per side | bp round trip |
  |---|---|---|
  | `us_equity_etf_large` | 3 | 6 |
  | `us_equity_large_cap` | 8 | 16 |
  | `us_equity_small_cap` | 30.5 | 61 |
  | `crypto_major_spot` | 40 | 80 |
  | `not_tradable` | - | - |

  These costs are conservative by construction. For GEX H3, E2 recomputes the return
  from the logged raw entry and exit prices at 6 bp round trip, rather than the
  pre-registration's 2 bp.
- **Aggregates** are computed per (stream, role), per (stream, family) and per
  (stream, family, sector, horizon):
  - windows `all`, `last_20` and `last_60` (a rolling window appears only once full);
  - a percentile bootstrap CI of the mean: 2,000 draws, from a splitmix64 stream seeded
    by sha256 of the group, so the CI is deterministic across numpy versions.
- **Official scores.** `rules.json` `registered_at` is the instant the rules were fixed.
  A score is official only if its outcome became observable after that instant.
  Earlier outcomes go to a separate `pre_registration` bucket, which is never official.

## Integrity

- **The ledger.** `chain.py` maintains `e2_scoreboard_<version>.jsonl` and its anchors.
  - It is append-only JSONL. Every line carries the sha256 of the previous line.
  - The header pins the E2 manifest sha256: a ledger can only be extended by the exact
    code that started it.
  - Every append adds `(records, head)` to a chained anchor file.
  - There is no code path that rewrites, truncates or deletes a line.
- **Refusals.** `run` refuses:
  - a broken chain;
  - a truncated or rewritten ledger;
  - a header from other code;
  - a run instant earlier than the last record.
- **Ingestion is once per prediction id.** If a stream later shows different content
  under the same id, E2 appends an `integrity_alert` and keeps the original. If a
  stream log no longer holds the prefix E2 saw on the last run, E2 appends one alert
  for that stream. A stream that emits a record breaking the E2 contract is reported
  not-ok for that run with nothing recorded from it; an unexpected exception aborts the
  run. Aggregate rows carry `stream_ok_this_run`.
- **Off-host witness** (`witness.py`, the VS1 pattern):
  - `run --witness-worktree` appends anchor lines to
    `05-GRID/Paper-Log/e2/e2_scoreboard_<version>.anchors.jsonl` in a vault clone,
    which the vault sync then pushes.
  - `verify --vault-repo` fetches the pinned `main` of `github.com/3pacs/obsidian-vault`.
    It checks that the witness file's history is append-only and that it witnesses
    this ledger.
- **Frozen.** Every file here is pinned in `MANIFEST.sha256`.
  `tests/test_e2_manifest_guard.py` pins the manifest's own hash per released version
  (append-only). It also fails CI if any module outside `evals/e2` imports the ledger
  writer, or if the API reads anything but `report.py`.
- **Versioned.** Changing a rule, the cost model, an adapter or the scoring code is a
  new E2 version, and it needs owner approval:
  1. Bump `evals.e2.VERSION` and `rules.json`.
  2. Run `python -m evals.e2 manifest --write --version e2-vN`.
  3. Append the hash to `RELEASED_MANIFESTS`.

  The new version writes its own ledger file and may rescore history there. Old
  ledgers are never touched.
- **Proposers** (agents, the E3 harness) may read the scoreboard and run E2. They may
  not edit E2 or write its ledger.

## Commands

```bash
python -m evals.e2 verify [--board-dir DIR] [--vault-repo CLONE]
python -m evals.e2 run --board-dir DIR --s10-log-dir D --gex-log-dir D [--witness-worktree VAULT] [--now ISO]
python -m evals.e2 report --board-dir DIR [--format md|json]    # read-only
```

**Read-only surfaces:**

- `GET /api/v1/evals/e2/scoreboard` (`?window=all|last_20|last_60&bucket=official|pre_registration|any`)
- `GET /api/v1/evals/e2/scoreboard.md`

**Daily job:** `deploy/systemd/grid-e2-scoreboard.{service,timer}.template` runs at
15:00 and 23:15 UTC, outside the 03:30-10:30 UTC backup window. It is a template
only, and activating it is an owner step. The job opens no database connection for
the two live streams.

Nothing here is a trading signal, and nothing here places orders.
