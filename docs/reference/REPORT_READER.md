# Report reader: every agent report read and ingested

Code: `intelligence/report_reader.py`, `api/routers/agent_report_reads.py`, migration
`agent_report_reads_20261122`. Tests: `tests/test_report_reader.py`.

## Why

Every agent files a session report into `agent_reports` through the agent hub (`agent_hub/`): Claude, Codex,
Hermes and the watchers send 150 to 260 a day, and there were 17,700 by 2026-11-22. Nothing read them, so a
result, a blocked item or a request for the owner could sit unread. On 2026-11-18 a registered look ran, and its
session stopped before reporting it; nobody noticed for three days.

## What a read is

Each report gets one row in `agent_report_reads`:

- `summary`;
- `kind`: result, change, incident, blocked, routine or other;
- `needs_anik`: the report asks the owner for a decision, an approval or an action;
- `open_items`: what is left or blocked;
- `results`: claims with numbers or verdicts;
- `refs`: PRs and commits.

`how` records how it was read:

| how | when | model |
|---|---|---|
| `structured` | the report has Changed / Verified / Blocked / Left sections; parsed as written | none |
| `routine` | a healthy Hermes cycle, GPU watch or backup line | none |
| `llm` | everything else; `llm.router` Tier.LOCAL (local only, paid providers off) | local |
| `unreadable` | three failed attempts; still counted and shown in the digest | — |

Failed attempts go to `agent_report_read_attempts`. Both tables are insert-only (triggers).

## Running it

```bash
cd /data/grid_v4/grid_release && python -m intelligence.report_reader run --limit 400 --llm-limit 40   # newest unread first
cd /data/grid_v4/grid_release && python -m intelligence.report_reader coverage --days 7                 # received / read / unread per day
cd /data/grid_v4/grid_release && python -m intelligence.report_reader digest --day 2026-11-21           # the morning text
```

Each run reads the newest unread reports first, so today never waits behind the backlog. Structured and
routine reports cost nothing. LLM reads are capped per run; measured on gridz4, each takes 4 to 9 s. At about
130 LLM reads a day, a run every hour with `--llm-limit 40` keeps up, and the 2026 backlog drains in about two
weeks.

## Ingested

- `GET /api/v1/agent-reports/reads?agent=&kind=&needs_anik=&days=` (auth; total, limit, offset, has_more).
- `GET /api/v1/agent-reports/coverage?days=` returns, per day, the reports received, read, unread and
  unreadable.
- `POST /api/v1/ollama/ask` adds the read reports of the last 30 days that match the question to the system
  context, beside the knowledge tree and the research findings.

## Asks to the owner, carried until settled

An ask is the line of a report that is addressed to the owner. The reader picks the first open item that names him
or an approval, then the first open item, then the title. The same ask written twice, even reworded, keys the
same: its words, sorted. Asks are carried across days until they are settled.

Each hourly run checks the open asks of the last 14 days, records each check as one insert-only row in
`agent_report_ask_checks`, and rechecks an ask only when new matching reports have arrived. A check settles an
ask in one of two ways:

- **On GitHub.** The ask asks only for a merge, and every GRID PR it names is merged or closed. This uses the
  public API, and 3pacs/GRID only, so no model is needed.
- **By the local model.** It reads the ask beside up to six later reports that share at least two words with it,
  plus the GitHub state of any PR named. It settles the ask only when a later report says the thing was done,
  approved, declined, superseded or no longer needed.

## Nothing falls through

The digest is the safety net. It is texted each morning and lists:

- the counts for the previous day;
- every ask from the last 14 days still waiting on the owner, once each and newest first;
- how many asks later reports have settled;
- any unreadable report.

A day whose `unread` is not zero after the runs is visible in `coverage`.
