# MF-1 provisional signal evaluator manifest

Base: frozen combined commit `8c35ec8572c1e297c2cfae0a43f7c942cc169020`.
Source inspected: PR #562 head `5c4e26ba1fe1fc9a1edf72dc1b99f2c6ee78be5c`
in the local GRID object store. Only its evaluation package, manual CLI,
price-contract prose, and tests were selected; no migration was imported.
The selected implementation was corrected rather than cherry-picked intact.

Files: `evaluation/__init__.py`, `evaluation/prices.py`,
`evaluation/signal_outcomes.py`, `scripts/evaluate_signals.py`,
`docs/reference/PRICE_SERIES_CONTRACT.md`, this manifest, and the three
focused test files. There are no changes to app, store, AstroGrid, resolver,
ingestion, scorer, CI workflow, schema, or migrations.

The #562 source used `PITStore.get_pit` on a resolved feature inferred from a
feature name. That cannot prove raw-close basis because the entity map can
route `close` and `adj_close` into one feature and resolved rows omit series
identity. The accessor can read only the exact successful `YF:{ticker}:close`
raw-series row, joined to yfinance and bounded by observation and pull time.
But raw rows lack an adjustment flag or ingester version. Current source
passes `auto_adjust=False`; it cannot prove historical-row basis. The accessor
requires an externally verified ingestion cutover. This branch and CLI supply
none, so unverified history remains ineligible. Tests inject a hypothetical
cutover solely to exercise query logic over fake data. It does not
guess a successor quote or create next-session evidence.

The signal evaluator now leaves immature horizons unresolved before an exit
lookup; rejects an old entry, a stopped/same-date exit, adjusted basis, and
unverified congressional disclosure time. The dry-run rule version assumes
four calendar days of bar tolerance, calendar-day horizons, and entry on the
next calendar date after 16:00 America/New_York. These are exploratory
assumptions, not approved exchange-session or execution policy. `created_at`
is used only as an ingestion-time availability proxy; it does not prove public
publication, especially for congressional transaction dates.

The manual CLI requires explicit source/date bounds and a finite row limit.
It selects DATE and TIMESTAMPTZ columns with type-specific predicates,
normalizes typed Python values, and refuses naive timestamps. It has no
persist option or write function. The constructed PostgreSQL engine enforces
a read-only transaction default and statement timeout. No database, provider,
network, or production execution occurred during this work.

Verification: `python -m pytest -q tests/test_signal_outcomes.py
tests/test_evaluation_prices.py tests/test_evaluate_signals_cli.py
tests/test_alembic_single_head.py tests/test_yfinance_auto_adjust_explicit.py`
passed 47 tests locally. `git diff --cached --check` passed. Tests used
fake engines/accessors; the migration-head and ingestion-basis tests were
unchanged from the frozen base.
No PostgreSQL integration proof is claimed. A disposable PostgreSQL read-only
exercise is needed to verify actual column types, query behavior, source
catalog/series availability, basis cutover provenance, and row-level data
quality. Live deployment,
metric acceptance, cohort interpretation, and any persistence design remain
separate decisions. This work is independent of finalization v4 landing.
