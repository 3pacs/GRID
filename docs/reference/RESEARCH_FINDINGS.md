# Research Desk findings in GRID

Code: `analysis/research_findings.py`, `api/routers/research_findings.py`, migration
`research_findings_20261121`. Tests: `tests/test_research_findings.py`.

## What it is

The Research Desk and the GEX searches have tested many trading rules: overnight persistence, stress
reversal, index changes, ARK follow, 0DTE fades, charm and vanna, DIX and others. Their write-ups live in the
desk's private repository. Until now GRID could not see any of them. A search could re-test a closed idea
without counting the earlier look, and the chat answered market questions without the desk's results.

The desk keeps one record per tested question, schema `desk-findings/1`. Each record holds:

- the rule as tested;
- the window and sample;
- the effect with its 95 % interval and t;
- the verdict;
- the unread data the look used up;
- the written result it came from.

The desk's exporter refuses any record whose numbers are not in quotes taken word for word from that result.
GRID loads the exported catalogue as data. No desk code moves into GRID.

## Tables

| | |
|---|---|
| `research_findings_loads` | one row per catalogue version (`catalogue_sha256` = `records_sha256` of the export), with its record count and repo commit |
| `research_findings` | one row per record per version: the columns below plus the full `record` JSONB |
| `research_findings_current` | view: the rows of the latest load |

Columns: `finding_id`, `family`, `area`, `kind`, `verdict`, `status`, `question`, `rule`, `window_from`,
`window_to`, `sample_n`, `sample_unit`, `effect_value`, `effect_unit`, `ci_low`, `ci_high`, `t_stat`,
`data_spent`, `source_path`, `source_sha256`, `recorded_at`.

Both tables are insert-only: triggers refuse UPDATE and DELETE. A changed catalogue is a new load, and older
versions stay readable by their hash.

## Vocabulary

- `verdict`:
  - CONFIRMED / NOT_CONFIRMED / UNDERPOWERED: a registered one-look confirmation.
  - PASSED_BAR / FAILED_BAR: a development bar fixed in advance.
  - DESCRIPTION: description only.
  - PENDING: a forward record or registration not yet at its look.
- `status`: closed, forward_live (a forward paper record is running), open, retired.
- `area`: cross_section, index_0dte, index_daily, market_state, flows, events, execution, data_quality.

## Reading it

- `current_findings(engine, family=, area=, verdict=, q=, limit=, offset=)` returns rows and the filtered total.
- `GET /api/v1/research/findings` serves the same with `total`, `limit`, `offset` and `has_more`, and requires auth.
- `context_for_prompt(engine, topic)` gives the findings that share words with a question, one line each with the
  verdict and numbers. `POST /api/v1/ollama/ask` puts these into the system context beside the knowledge tree.
  It is empty on any error.

## Loading

```bash
cd /data/grid_v4/grid_release && /data/grid_v4/grid_venv-314/bin/python -m analysis.research_findings load --path /data/agent-home/anikdang/desk-findings/desk-findings.json
cd /data/grid_v4/grid_release && /data/grid_v4/grid_venv-314/bin/python -m analysis.research_findings list --verdict NOT_CONFIRMED
```

A load first checks the schema, the count, every record's required fields and vocabulary, unique ids, and the
recomputed `records_sha256`. Any failure refuses the whole file. Loading a version already loaded does
nothing.

## What it is not

A finding is a record of a past result. It is not a signal, and nothing here scores, promotes or trades.
A NOT_CONFIRMED rule closes its family under the desk's evidence bar (owner decision #1213). Testing it again
on the same years is not a new test.
