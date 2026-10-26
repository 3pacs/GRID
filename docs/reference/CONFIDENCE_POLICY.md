# The `confidence` policy

**Status:** adopted 2026-09-17. Enforced by `tests/test_confidence_policy.py`.
Background: `docs/audits/fake-data-2026-09-17/` (cluster C4 —
"hand-tuned constants emitted under a measurement name").
Companion contract: `docs/reference/AVAILABILITY_CONTRACT.md` — same vocabulary
(null + basis; never a midpoint).

## The rule

> A field named **`confidence`** or **`probability`** is **`null`** unless it
> comes from a **scored track record**. Any surviving heuristic is **renamed**
> and **ships its inputs**.

A scored track record means outcomes were recorded and the number was computed
from them: a hit rate over resolved predictions, a Brier-scored calibration
curve, a verified-vs-flagged count on the artefact itself. A tuned formula, a
per-branch literal, a source-type weight or an affine function of a row count
is **not** a track record, no matter how plausible its value looks.

There are exactly three honest outcomes for such a field:

| situation | what ships |
|---|---|
| scored against outcomes | the number, under `confidence`, with what scored it |
| a useful heuristic rank | a **renamed** field + **its inputs** + a `*_basis` string |
| neither | `null` — and no substitute number under another measured name |

Accepted rename targets: `heuristic_confidence`, `heuristic_score`,
`coverage_score`, `answer_heuristic_score`, `prior_weight`, `rank_score`, and
— for a provenance *label* rather than a number — `source_class`.

## What this forbids

- `confidence = 0.75 if context_text else 0.5` — a constant dressed as a
  measurement of the answer.
- `"confidence": 0.7` on every row of a table that has no confidence column.
- `min(0.9, 0.3 + cnt * 0.05)` published as `confidence`.
- `float(x or 0.5)` / `float(x or 50) / 100` — a NULL is unknown, not the
  midpoint, and the falsy test additionally rewrites a genuine `0.0`.
- `"confidence": "confirmed"` written per code path. A provenance label is
  read from the row, or the field is `source_class` and names the branch that
  produced it (`icij_offshore_leaks`, `signal_data:darkpool`).
- `confidence_label = "confirmed" if confidence > 0.7 else "derived"` — a
  model confidence over a threshold is not source confirmation.
- Replacing a retired literal with **another** literal, or quietly serving a
  loosely related metric under the original label.

## What consumers must do

- **Render null honestly:** `--`, `unrated`, `unknown`, `confidence unknown`.
  Never `0%`, never a mid-range bar, never a green light.
- **Sort null last.** `ORDER BY confidence DESC NULLS LAST` in SQL; in Python
  and JS, an explicit "unknown" tier below every scored value. Never `|| 0`
  and never `?? 0.5` in a comparator.
- **Exclude null from averages and calibration.** Publish the sample size
  (`n`, `scored_n`) beside the average so a reader can see what it covered.
- **Never persist a null as an observation** — no snapshot row, no TTL-cache
  pin, no "stated confidence" written to `oracle_predictions`.

## Where this already applies

| surface | before | after |
|---|---|---|
| `POST /api/v1/chat/ask` | `confidence: 0.75/0.5/0.3/0.1` | no `confidence` key; `claim_count`, `flagged_count`, `verified_claim_ratio`, `firewall_decision` |
| `/canvas/graph` actor edges | `confidence: 0.7`, `strength: x or 0.5` | no `confidence`; `strength: null` when the row has none |
| `/canvas/graph` actors | `(x or 50) / 100` | the column's own 0-1 value, `null` when NULL; unscored sorts last under `limit` |
| `/canvas/dots` | seven affine `confidence` formulas | no score; structured `inputs` carrying the raw counts |
| `/canvas/*` signals | `conf_map.get(label, 0.5)` | `source_class` + `source_class_score`, `"unknown"` when unmapped |
| `GET /api/v1/knowledge*` | `confidence` (text-shape heuristic) | `answer_heuristic_score` + `answer_heuristic_basis` + `answer_heuristic_inputs` |
| `oracle/publish.py` | one value published as three metrics, `or 0.5` | three independent inputs, `NULL` when unsupplied |
| oracle calibration | scored placeholder rows | `AND confidence IS NOT NULL`; `total_predictions` counts stated ones |
| `/astrogrid/snapshot` `seer` | `0.72 / 0.69 / 0.6` + tuned bands | `confidence: null`, `confidence_band: null`, plus `bucket`, `pressure_score`, `release_score` |
| `/astrogrid/scorecard` items | `confidence` | `coverage_score` + `coverage_score_basis`, beside `history_points`, `latest_date`, `coverage.has_live_price` |
| astrogrid review | `0.45 + 0.15 + 0.15 + 0.05` | `confidence: null` + `review_evidence` (`scored_n`, `hit_count`, `miss_count`, `best_alpha`) |
| `/api/v1/intel/*` | 51 per-branch `confidence`/`confidence_label` literals | `source_class` naming the producing branch |

### A note on `/chat/ask`

`verified_claim_ratio` is `(claim_count - flagged_count) / claim_count` from
the publishing firewall. Both counts are measurements of the answer: claims
extracted from it, and claims the verifier judged contradicted or
critically failed against the database. It is `null` when the firewall did not
run or found no checkable claim. `firewall_decision` is a label, not a number;
note that the gate's publish/review/reject choice does itself average
`oracle/claim_verifier`'s per-pattern constants, which is why that label ships
as a label and is never converted into a score.

## Enforcement

`tests/test_confidence_policy.py` scans `api/`, `intelligence/`, `analysis/`,
`oracle/`, `knowledge/`, `store/`, `trading/` and `astrogrid_api/` for
`confidence = 0.<d>` / `"confidence": 0.<d>` and the same for `probability`,
and fails on anything not in its allowlist. The allowlist lives in that file,
one entry per file with an explicit reason. Adding an entry is a deliberate,
reviewable act; adding one without a reason fails the test's own self-check.
