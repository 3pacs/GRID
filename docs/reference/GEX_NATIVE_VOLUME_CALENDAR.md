# GEX native-volume expected-session coverage

Source-only correction to the existing PR #1103 producer. Base:
`aeca6056b21cd31cc54975f76c18558063e64457`. No release was applied, no
provider was called, and no live snapshot, schema, schedule, signal, alert or
credential was changed. The separate receipt-backed producer remains separate.

## Contract

`ingestion/gex_native_volume.py` now selects the exact preceding ten XNYS
regular sessions from an offline, source-pinned schedule before checking their
bars. A completely missing expected session cannot be replaced by an eleventh
or twelfth older observed date. Missing minutes still withhold the payload.
Coverage compares actual minute timestamps with the schedule's open, so bars
shifted by seconds cannot satisfy a minute slot.

The existing `ingestion/market_calendar.py` owns the dependency-free manifest
adapter, adapted from `ingestion/realtime/native_volume_calendar.py` at
`a3172b27`. Its older approximate scheduler helpers are unchanged. Neither the
research interval library nor another producer/entrypoint is imported.

Calendar artifact, copied byte-for-byte from that reviewed source candidate:

- File: `docs/reference/calendar-manifests/xnys-rth-20261001-20271231.json`
- SHA256: `efa8897e2fd94afd4f9f9b29951f142b97aece83812aca7403026df1ec4eb8ad`
- Coverage: 2026-10-01 through 2027-12-31 inclusive. Ten preceding calendar
  sessions must also be present; the first ten sessions cannot produce ready.
- Provenance: exchange_calendars 4.11.2, pandas 2.3.3, numpy 2.5.4,
  tzdata 2026.6, XNYS. Runtime needs only the Python standard library and its
  America/New_York timezone data, not those exporter dependencies.
- File must be regular, non-symlink and not group/world writable (0644 works).
  The digest is pinned in Python source, not accepted from a neighboring file.
  Each invocation rechecks the artifact; no previously validated cache survives
  a missing, corrupted or replaced file.

Known closed days return `closed/outside_regular_session`. Out-of-range dates,
insufficient calendar history, unreadable/untrusted artifacts or validation
failure return `unavailable/calendar_unavailable`. The runner checks the same
calendar before any provider request and performs no request or snapshot write
for those states. Existing reader freshness rules age any prior snapshot out.
There is no weekday or observed-data fallback.

On shortened sessions the true close is respected. An early-close session
remains one of the exact prior ten, including when only its first minutes are
needed. At elapsed minute 210 a 13:00-close session is still comparable; at
minute 211 it yields `unavailable/incomparable_baseline_session` and an explicit
`early_close_insufficient_elapsed_minutes` baseline error. No scaling,
extrapolation, skipping or substitution is authorized. A different post-close
comparison policy would require an explicit decision and a changed contract.

The reader field set, producer identity, VWAP/RVOL basis strings, reconciliation
band and false `live`, `signal_confirmation`, `cross_source_reconciled`,
`exact_trade_vwap` and `historical_request` flags are unchanged. Only the
calendar provenance string changes to the pinned manifest identity. The frozen
burn-in protocol is untouched.

## Verified offline

138 tests plus 9 parser subtests passed under Python 3.11.15. They cover absent
whole sessions despite 11 older observed dates, partial and shifted minutes,
Thanksgiving/Christmas/weekends, fall and spring DST, early closes, exact
post-holiday selection, outside/insufficient calendar coverage, file integrity,
no provider calls on failed preflight, the existing mocked Schwab client, and
private-file round trips through the current sibling GEX reader.

Reader checked from GEX commit `b87678b620ec5210cbd1a8a1f6048b40dd8994f1`, file
`src/gex-watch/grid_native_volume.py`, SHA256
`40f65dbf2741eeae0178b58538af1da0f6a45448644f86df418b4b9e3e815347`.
Ready synthetic output remains ready; missing-session and incomparable-baseline
output remains unavailable. No provider, SSH or production file is needed.

Local exact check commands (the combined release kit also pins candidate HEAD):

```bash
cd /home/anik/dev/grid-gex-native-calendar-20261112
printf '%s  %s\n' \
  efa8897e2fd94afd4f9f9b29951f142b97aece83812aca7403026df1ec4eb8ad \
  docs/reference/calendar-manifests/xnys-rth-20261001-20271231.json | sha256sum -c -
DB_PASSWORD=offline-test-only PYTHONDONTWRITEBYTECODE=1 \
GEX_NATIVE_READER=/home/anik/dev/gex-main-deploy-20261111/src/gex-watch/grid_native_volume.py \
/home/anik/dev/grid-r4-vix-fallback/.venv/bin/python -m pytest -p no:cacheprovider \
  tests/test_gex_native_volume.py tests/test_market_calendar_manifest.py tests/test_schwab_market.py -q
ruff check --no-cache . --target-version py311 \
  --select F821,F822,F823,F402,F501,F502,F632 --exclude notebooks
ruff check --no-cache --target-version py311 --select E,W,F --ignore E501 \
  ingestion/gex_native_volume.py ingestion/market_calendar.py \
  scripts/gex_native_volume_snapshot.py tests/test_gex_native_volume.py \
  tests/test_market_calendar_manifest.py
git diff --check
```

The dummy DB_PASSWORD satisfies config construction only; the selected client
tests inject fake HTTP, fake credential files and synthetic data. The three
sibling-reader tests skip if GEX_NATIVE_READER is omitted, so do not omit it for
combined acceptance. Full repository/PostgreSQL/remote CI and live forward
acceptance were not run. The bare local Ruff command without a Python target
reports two pre-existing BaseExceptionGroup references; the repository's
supported Python 3.11 target passes the whole-repository blocking selection.

## User-only integration

Claude verifies the source candidate and the combined kit's exact command.
Anik alone integrates and applies the release. Carry the producer, existing
calendar module, runner and manifest together. Confirm the installed manifest
has the exact hash and allowed mode; do not copy the separate receipt-backed
worker, activate a new writer, change the frozen protocol or alter signals.
This source commit is a candidate, not a deployment or live acceptance claim.

The schedule is a bounded packaged snapshot, not a watcher for emergency
exchange announcements or unscheduled halts. Such changes need a newly reviewed
artifact and explicit pin update. Beyond the manifest boundary the producer
withholds rather than guesses. Existing post-close snapshot aging and observed
trailing-bar freshness rules are otherwise unchanged.
