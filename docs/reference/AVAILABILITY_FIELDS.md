# Per-field availability record (extends AVAILABILITY_CONTRACT.md)

**Status:** adopted 2026-09-18 for new and remediated per-field/per-source data (e.g. a
health/freshness row per data source, one series inside a multi-series response). Enforced
by `store/availability_fields.py`. This document extends, and does not replace,
[`AVAILABILITY_CONTRACT.md`](AVAILABILITY_CONTRACT.md) — read that one first. Everything
there (the whole-result `available`/`partial`/`unavailable` shapes, `Provenance`,
`Freshness`, `measured_or_none`, `cacheable`) still applies unchanged; this file adds a
richer record for describing one *field* within a result.

Background: `docs/reference/DATASET_CONSUMER_DAG.md` ("Contract extension proposal") audited
`store/availability.py` (#536) against twelve requested per-field dimensions and found gaps in
unit, observation period, published/ingested timestamps, calculation version, coverage, and
stale reason. `FieldRecord` in `store/availability_fields.py` fills those gaps for the
field-level case without touching #536's result-level contract.

## The rule this adds

**Dataset history and page freshness cannot disagree silently: every freshness figure a page
shows must come from one of these records.** If a page renders "3/5 sources healthy" or "last
updated 2 hours ago", that number must be read off `FieldRecord.availability` /
`stale_reason` / `ingested_at` — never recomputed ad hoc in the handler from a different
column than the one the record captured.

## Two independent axes

A `FieldRecord` never has one flag that means two things:

- **`availability`** — is there a usable value at all? `available` / `unavailable` / `invalid`.
- **`provenance`** — *how* an available value was obtained: `measured` (direct observation),
  `derived` (arithmetic/aggregation over measured values), `modeled` (a model or assumption).
  `provenance` is always `None` when `availability != "available"` — there is no provenance
  for a value that does not exist or was rejected.

These are orthogonal: a field can be `available` + `modeled` (a Black-Scholes estimate is a
real, usable value — it is just not measured), but can never be `unavailable` + `measured`.

There is deliberately **no boolean confidence field**. `bool` confidence is exactly the
pattern `AVAILABILITY_CONTRACT.md` calls out as a defect (`"confidence": "confirmed"` as a
literal). A caller reads `availability` + `stale_reason` instead.

## Fields

See the docstring table in `store/availability_fields.py::FieldRecord` for the authoritative,
versioned list. Summary:

| field | never becomes 0/"" when unknown | notes |
|---|---|---|
| `availability` / `provenance` | n/a (see above) | the two axes |
| `value` + `unit` | `value` stays `None` unless available | unit is a plain string (`"USD"`, `"pct"`) |
| `obs_date` / `obs_start` / `obs_end` | yes | a point-in-time obs uses `obs_date`; a period uses start/end |
| `published_at` | yes | when the source released the data |
| `available_at` | yes | when this system first could have acquired it |
| `ingested_at` | yes | when this system actually pulled/wrote it |
| `revision` | yes | vintage/revision id, e.g. a `pull_timestamp` |
| `source_catalog` / `series_id` | yes | `source_ref` split into the two ids callers already have |
| `calculation_version` | yes | version id of the code/formula for a derived/modeled value |
| `coverage_fraction` / `coverage_count` / `coverage_expected` | yes | use `coverage_or_none()`, never `count/expected` when `expected` is 0 or unknown |
| `stale_reason` | n/a — `None` means "no reason to report", not "unknown value" | one of the enum below; can co-occur with `availability="available"` (a fresh pull is not "stale", but an old successful pull is `available` + `stale_reason="stale"`) |

**Unknown must never coerce to 0.** `coverage_fraction=0.0` is a real measurement ("we
measured zero coverage"); `coverage_fraction=None` is "we don't know the coverage" — these
are never conflated. Use `coverage_or_none(count, expected)`, not `count / expected` inline.

## `stale_reason` enum

`never_configured` | `fetch_failed` | `stale` | `rate_limited` | `not_published_yet` |
`parser_error` | `empty_source` | `partial_history` | `materializer_failed` |
`consumer_query_mismatch` | `unknown`

A row with `rows == 0` from a **successful** pull is not automatically `empty_source` — check
whether the pull itself reported success before labelling it that way (a successful pull that
returned zero rows is `empty_source`; a pull that never completed is `fetch_failed` or
`materializer_failed` depending on where it broke; a source with no pull attempt on record at
all is `never_configured`).

## Constructors

`measured_field(value, **meta)`, `derived_field(value, **meta)`, `modeled_field(value, **meta)`
build `available` records; `unavailable_field(stale_reason, **meta)` and
`invalid_field(stale_reason, **meta)` build the other two. All validate their axes via
`FieldRecord.__post_init__` (raises `ValueError` on an inconsistent combination, e.g.
`provenance` set while `availability="unavailable"`).

## Checklist for an adopter

1. Every place that today infers "healthy"/"stale"/"missing" from raw timestamps or an
   in-handler `if/else` builds a `FieldRecord` instead, and reads `.to_dict()` into the
   response.
2. A caught exception that used to return an empty/zero-filled 200 instead returns a payload
   whose top-level state is explicit (`FieldRecord`-shaped: `availability="unavailable"` +
   `stale_reason`), never bare empty lists that a client would read as "0 sources".
3. `ingested_at` comes from an actual pull timestamp column when one exists; a field is not
   invented a timestamp it doesn't have — leave it `None` and set `stale_reason` instead.
4. UI: an `availability="unavailable"` top-level state renders honest text (never backend
   error text verbatim), and `stale_reason` is shown per row, not swallowed.
