# GRID W5 source status — EIA + LME (2026-09-18)

Scope: source-only findings from `fable/source-fixtures-20260918`. No DB, no ssh, no
EIA/authenticated network calls were made to produce this file.

## LME warehouse puller — scheduling status

- **Registered:** yes, but only under `intelligence/scheduler.py`'s `_lme_warehouse_daily`
  job (`_sched.every().day.at("09:00").do(_lme_warehouse_daily)`,
  `intelligence/scheduler.py:892`; job body `intelligence/scheduler.py:842-855`), gated by
  `if __name__ == "__main__": run_intelligence_loop()`
  (`intelligence/scheduler.py:1197-1198`). Pinned in code by
  `tests/test_source_lme.py::TestSchedulingTruth`.
- **Runs under:** `server_setup/grid-intelligence.service`
  (`ExecStart=/usr/bin/python3 -m intelligence.scheduler`) — per the operator's vault
  notes, this service runs the **stale** `grid_repo` tree, not necessarily the tree that
  is actively deployed and kept current the way `grid-api`/`grid-hermes` are (see
  `docs/SERVER-SERVICES.md`'s "Deploy Path" notes for the analogous, documented
  `grid-hermes` case — `grid-intelligence` is not covered there and was not independently
  re-verified in this worktree).
- **Not registered** in `ingestion/scheduler.py` (the authoritative ingestion scheduler
  per `CLAUDE.md` gotcha #39) — confirmed by a full-file, case-insensitive grep for
  `"lme"`: zero hits.
- **Effective runtime: unknown from source.** Whether `grid-intelligence.service` is
  presently enabled/running on any host, and whether it is running a deployed tree that
  actually includes this job, cannot be determined from source alone — that needs
  operator/server access (`systemctl status`, deployed commit hash), which is out of
  bounds for this workstream.
- **The "zero `lme:*` rows" TODO is therefore unexplained by source alone.** The wiring in
  the tree examined here is correct (job defined, registered daily, reachable via the
  service's `ExecStart`), so a genuine zero-row outcome in production would need one of:
  (a) the service not actually enabled/running, (b) it running a stale deployed tree that
  predates this wiring, or (c) a fetch/parse-level failure — e.g. the live
  warehouse-stocks-report page being JS-rendered, so `_parse_lme_html` finds no `<table>`
  and silently returns zero snapshots (`ingestion/altdata/lme_warehouse.py:592-600`).
  (a)/(b) need server access; (c) needs a live network capture. Neither is available to
  this workstream.

## EIA weekly Cushing, OK crude stocks — PENDING SOURCE-CONTRACT VERIFICATION

- **Series id confirmed** from EIA's own public documentation: `W_EPC0_SAX_YCUOK_MBBL`,
  "Weekly Cushing, OK Ending Stocks excluding SPR of Crude Oil", units Thousand Barrels,
  weekly frequency.
  - URL: https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=W_EPC0_SAX_YCUOK_MBBL&f=W
- **v2 API facet shape for the `petroleum/stoc/wstk` route is unconfirmed.**
  `eia_puller.py`'s existing route (`petroleum/pri/spt/data/`) takes a single opaque
  `facets[series][]=<code>` value (`RBRTE`/`RWTC`). Whether `petroleum/stoc/wstk` accepts
  that same single-`series` convention, or instead requires `duoarea`/`product`/`process`
  facets, could not be confirmed from any public documentation page reachable without
  calling the EIA API itself — which requires `api_key` on every call, including
  metadata/route-directory calls, and is out of bounds per this workstream's "no
  EIA / no authenticated endpoint" boundary.
- **No code added** for this series. Adding it to `_SERIES_MAP` without confirming the
  facet contract risks a puller that silently returns zero rows forever — structurally
  the same failure mode as the LME gap above, which is exactly what this hardening slice
  is meant to prevent, not introduce.
- **The one request that would confirm it** (to be run only by someone authorized to call
  the EIA API with a real key):

  ```
  GET https://api.eia.gov/v2/petroleum/stoc/wstk/data/
      ?api_key=<REAL_KEY>
      &frequency=weekly
      &data[0]=value
      &facets[duoarea][]=YCUOK
      &facets[product][]=EPC0
      &start=2026-08-01
      &end=2026-09-16
  ```

  A 200 response whose `response.data[]` entries carry `period`/`value` fields (matching
  the shape `eia_puller.py` already parses) confirms the `duoarea`+`product` facet
  convention. A 400 error naming an unrecognized facet falsifies it and should name the
  actual required facet key instead.

## Secret hygiene

Grepped `tests/fixtures/sources/**`, `tests/test_source_eia.py`, `tests/test_source_lme.py`,
and this file for `api_key=`, long hex/base64 runs, `Bearer`, `password`, `secret`, `token`.

- One test value in `tests/test_source_eia.py` previously used a real-looking placeholder
  (`SECRET-REAL-KEY-999`) to exercise the credential-redaction test. Replaced with the
  unambiguous placeholder `REDACTED-FIXTURE` everywhere in that file (env var value and
  the embedded fixture request URL alike). The redaction test now asserts the logged
  message contains neither `REDACTED-FIXTURE` nor any other key text — only the
  `api_key=***` marker.
- No long hex/base64 runs, `Bearer` tokens, `password`, or `secret`/`token` strings found
  anywhere in the fixtures or test files under this workstream.
