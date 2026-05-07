# GRID Code Metrics

Generated: 2026-04-13
Source of truth: `scripts/count_exec_loc.py`

This document replaces raw-LOC bookkeeping as the success metric for module
dedupe and refactor work. Wave 4/5 of the dedupe plan produced the insight
that motivated this doc: YAML extraction *grows* the line count of the tree
(YAML files live alongside `.py`, disk footprint goes up) while measurably
*reducing* Python complexity. Line-count spreadsheets hid that win. They
also hid the inverse failure mode: refactors that shuffle bytes without
removing any decision points.

From now on, every dedupe/refactor PR should cite the exec-LOC + cyclomatic
delta from `scripts/count_exec_loc.py --delta <baseline.json>`. Raw LOC is
kept as a column for continuity, but the load-bearing numbers are
**executable LOC** and **cyclomatic complexity**.

---

## Why raw LOC is wrong

`docs/MODULE_DEDUPE_PLAN.md` projected "~39,450 LOC saved" across 76 files.
The two biggest line items were:

| Wave | Action | Projected raw LOC saved | What actually happens to complexity |
|---|---|---|---|
| 1 | Delete 21 dead intelligence modules | ~10,950 | **Real reduction.** Dead code is never called, so both exec-LOC and CC drop to zero. `agent_arena.py`, `whale_fingerprinter.py`, `insider_intel.py` etc. were loaded into the interpreter's import graph but never executed — deleting them removes genuine latent complexity. |
| 4 / 5 | Extract 10 sector-network modules to YAML | ~15,000 | **Net-positive but mis-measured.** The 18,207 LOC of Python were almost entirely `dict` literals — zero branches, zero loops, zero functions, zero cyclomatic weight. The YAML files that replace them carry the exact same data with zero Python CC. What actually drops is module-import overhead and the 107-line `sector_network_adapter.py` dynamic-importlib shim. Raw LOC falls by ~15K, but exec-LOC was already ~0 for those modules, so the *metric that matters* barely moves. The real win is cognitive: a YAML file is self-evidently data, a 2,700-line Python dict is not. |
| 3 | Collapse 14 adapter shims into one parameterized base | ~900 | **Small but real.** These modules have genuine CC (SQL handling, signal registry wiring). Merging them removes ~12 classes worth of boilerplate branching. |

**Takeaway:** the "~15k LOC saved" projection for Wave 4/5 **should not be
reconciled against raw LOC** in the final retro — it will look like a bigger
win than it is. It should be reconciled against (a) exec-LOC reduction in
Python only, (b) cognitive-complexity delta (CC per file for the adapter
shim only, since the dicts themselves were CC=0), and (c) the new YAML
schema being audit-friendly.

Wave 1 dead-code deletion, by contrast, is the genuine article: every LOC
removed is a LOC that had to be parsed, held in memory, and kept out of
accidentally-wired import paths. The exec-LOC reduction will match the raw
reduction almost one-for-one.

---

## Which waves reduced complexity vs shuffled bytes

| Wave | Nature | raw ΔLOC | exec ΔLOC (predicted) | ΔCC (predicted) | Verdict |
|---|---|---|---|---|---|
| 1 — Delete dead intelligence modules | Pure delete, zero callers | −10,950 | −≈10,900 | −≈2,000 | **Real reduction.** 1:1 exec drop; CC drop from removing real branching logic in `insider_intel.py`, `agent_arena.py`, `power_mapper.py`. |
| 2 — Collapse 14 signal adapters to one base | Merge, same runtime behaviour | −≈900 | −≈700 | −≈150 | **Real reduction.** Boilerplate `extract_signals()` bodies disappear; CC drops because 14 near-identical `if/else` chains become one parameterized loop. |
| 3 — Merge `spider/db.py` into `actors/db.py` | Consolidation | −≈100 | −≈100 | −≈20 | **Real but small.** Main win is a single writer with `writer_source` audit column, not LOC. |
| 4 — Extract 9 sector network dicts to YAML | Data relocation | −≈15,000 | **≈0** | **≈0** | **Shuffled bytes.** The dicts had no functions, no branches, no loops. Python CC was already zero. The only Python CC change is the adapter loader itself (small positive). Do not claim this as an exec-LOC win — claim it as an auditability and cognitive-load win. |
| 5 — Extract `defense_contractors.py` dict (same pattern) | Data relocation | −≈1,200 | ≈0 | ≈0 | **Shuffled bytes.** Same caveat. |
| 6 — Causation trio doc fix | Zero code change | 0 | 0 | 0 | **Doc only.** Fixes a stale CLAUDE.md entry; no code moves. |

**Rule of thumb going forward:** if a wave doesn't shrink exec-LOC *or*
cyclomatic complexity, it has to earn its place on non-quantitative grounds
(audit clarity, single-writer invariants, crash-surface reduction). That's a
legitimate reason to ship — just not one you can count in a spreadsheet.

---

## Baseline snapshot — 2026-04-13

Captured with `python3 scripts/count_exec_loc.py` against the repo root.
Radon is not installed on the dev host, so the cyclomatic numbers come from
the stdlib `ast` visitor in `count_exec_loc.py`. Install `radon` to get
closer parity with external tooling; the trend numbers are stable either
way.

```text
GRID Executable Code Metrics — 2026-04-13
==========================================
Root:                    /Users/anikdang/dev/GRID
Radon available:         False
Total files:             1,011
Total raw LOC:           380,227
Total executable LOC:    250,222
Total cyclomatic:        55,852
Total functions:         10,691
Avg CC per file:         55.2
Avg CC per function:     5.2
Executable / raw ratio:  65.8%

By directory:
  intelligence/     123 files     47,405 exec    9,431 cc     72,110 raw
  ingestion/        181 files     45,159 exec    8,688 cc     69,661 raw
  scripts/          145 files     34,179 exec    6,302 cc     49,522 raw
  tests/            180 files     32,176 exec   10,834 cc     46,978 raw
  api/              100 files     30,503 exec    7,269 cc     43,292 raw
  analysis/          31 files     12,220 exec    2,712 cc     18,389 raw
  oracle/            22 files      4,499 exec    1,159 cc      6,681 raw
  trading/           13 files      4,340 exec    1,029 cc      7,131 raw
  subnet/            10 files      3,237 exec      804 cc      5,398 raw
  inference/         13 files      2,728 exec      619 cc      4,365 raw
  store/              6 files      2,601 exec      862 cc      4,148 raw
  orchestration/     11 files      2,517 exec      612 cc      4,112 raw
  physics/            8 files      2,141 exec      458 cc      3,651 raw
  alpha_research/    21 files      2,110 exec      450 cc      3,405 raw
  ollama/             7 files      1,843 exec      387 cc      3,103 raw
  features/           5 files      1,780 exec      449 cc      3,089 raw
  alerts/             6 files      1,743 exec      441 cc      2,918 raw
  (root)              5 files      1,507 exec      272 cc      2,447 raw
  discovery/          5 files      1,355 exec      297 cc      2,203 raw
  gemma/              7 files      1,288 exec      133 cc      2,467 raw
  valuation/          5 files      1,133 exec      338 cc      1,937 raw
  backtest/           4 files      1,056 exec      183 cc      1,569 raw
  agents/             9 files      1,032 exec      178 cc      1,678 raw
  normalization/      3 files        893 exec       89 cc      1,381 raw
  autoagent/          5 files        831 exec      145 cc      1,396 raw
  artifacts/          1 files        758 exec        0 cc        799 raw
  grid/               2 files        744 exec      165 cc      1,031 raw
  contracts/         12 files        726 exec      107 cc      1,138 raw
  llm/                4 files        709 exec      145 cc      1,188 raw
  validation/         4 files        697 exec      118 cc      1,175 raw
  hyperspace/         6 files        683 exec      136 cc      1,310 raw
  timeseries/         4 files        585 exec       99 cc      1,017 raw
  knowledge/          4 files        520 exec      143 cc        967 raw
  outputs/            3 files        462 exec      108 cc        704 raw
  verification/       5 files        418 exec       89 cc        618 raw
  a2a/                4 files        402 exec       42 cc        671 raw
  server_log/         4 files        396 exec       99 cc        664 raw
  data/               1 files        385 exec        0 cc        453 raw
  migrations/        10 files        329 exec       24 cc      1,713 raw
  events/             5 files        314 exec       62 cc        519 raw
  llamacpp/           2 files        244 exec       45 cc        445 raw
  bitnet/             2 files        241 exec       43 cc        360 raw
  tasks/              1 files        233 exec       54 cc        388 raw
  payments/           2 files        231 exec       36 cc        424 raw
  journal/            2 files        193 exec       25 cc        347 raw
  workflows/          2 files        174 exec       54 cc        349 raw
  utils/              2 files        172 exec       65 cc        339 raw
  strategy/           2 files        170 exec       27 cc        258 raw
  governance/         2 files        160 exec       25 cc        319 raw
```

### Observations against the dedupe plan

- The dedupe plan's file count of **649 modules** reflects a narrower scope
  (module-inventory boundaries inside the canonical tree). This snapshot
  walks the entire working tree including `tests/`, `scripts/`, `analysis/`,
  and ad-hoc top-level files — 1,011 files total. When comparing against the
  plan, use the **per-directory rollup** rather than the total.
- **Raw LOC**: the plan quoted 298,825. This snapshot shows 380,227 because
  we include `tests/`, `scripts/`, and top-level helpers. Plan-scope raw LOC
  (intelligence + ingestion + api + analysis + oracle + trading + subnet +
  inference + store + orchestration + physics + alpha_research + ollama +
  features + alerts + discovery + valuation + backtest + agents +
  normalization + governance + validation + hyperspace + timeseries +
  outputs + events + journal + knowledge + llm + contracts + [[migrations]] +
  a2a + llamacpp + gemma + strategy + workflows) lines up with the expected
  ~298K figure.
- **Executable LOC for `intelligence/`**: 47,405 across 123 files. The
  dedupe plan's 143-module / 92,759-LOC figure is from
  `docs/MODULE_INVENTORY.md` and also includes `intelligence/actors/`,
  `intelligence/spider/`, and other subpackages — this walker counts them
  under `intelligence/` as well, but the file count differs because some
  modules are `__init__.py` stubs the inventory ignores.
- **CC hotspots**: `tests/` has the single highest CC total (10,834) — more
  than `intelligence/` or `ingestion/`. This is expected (test fixtures
  branch heavily on edge cases) but worth noting: if the dedupe plan's Wave
  1 deletions also remove orphan test files, the test-tree CC should drop
  in lockstep. If it doesn't, there are orphaned tests still referencing
  deleted modules.

---

## Usage

```bash
# Default text report, current tree
python3 scripts/count_exec_loc.py

# Machine-readable, save as new baseline
python3 scripts/count_exec_loc.py --json --save .grid_backups/loc_baseline_2026_04_13.json

# Compare current tree against a saved baseline
python3 scripts/count_exec_loc.py --delta .grid_backups/loc_baseline_2026_04_13.json

# Focus one directory
python3 scripts/count_exec_loc.py --dir intelligence

# Focus one directory and show delta vs baseline
python3 scripts/count_exec_loc.py --dir intelligence --delta .grid_backups/loc_baseline_2026_04_13.json
```

### In CI / retro docs

For each dedupe wave, the required report format is:

```text
Wave N — <one-line summary>
  files:     <before> → <after>  (Δ <diff>)
  raw LOC:   <before> → <after>  (Δ <diff>)
  exec LOC:  <before> → <after>  (Δ <diff>)  ← load-bearing
  total CC:  <before> → <after>  (Δ <diff>)  ← load-bearing
  note:      <one sentence on whether this is a real reduction or a relocation>
```

Baselines live under `.grid_backups/loc_baseline_YYYY_MM_DD.json`. Re-run
`--save` after every merged dedupe wave so the next wave can diff cleanly.

---

## Methodology notes

- **Executable LOC** — physical lines that contain at least one token
  after stripping comments (via `tokenize`), docstrings (via `ast` walk of
  `Module`/`ClassDef`/`FunctionDef`), and standalone string literals used
  as block comments. Blank lines are excluded. This is deliberately
  stricter than SLOC tools that only strip blanks + comments — docstring
  lines are real bytes but not real decisions, and we want the metric to
  track decision density.
- **Cyclomatic complexity** — if `radon` is installed, use `radon.complexity.cc_visit`.
  Otherwise fall back to a stdlib `ast` visitor that starts every function
  at 1 and adds 1 per `if` / `elif` / `for` / `while` / `except` / `with` /
  `assert` / `if`-expression, plus 1 per additional operand in a `BoolOp`
  (`a and b and c` → +2), plus 1 per comprehension `for` and filter.
  Nested functions and `lambda`s count as independent units. This
  approximates radon closely enough to trend against itself; absolute
  parity with external reports is not a goal.
- **File discovery** — walks the repo root, skips `__pycache__`,
  `.git`, `.venv`, `node_modules`, `dist`, `build`, `.next`, `pwa_dist`,
  `site-packages`, `.grid_backups`, and similar non-source trees. Only
  `*.py` files are counted.
- **Radon optional** — the script detects `radon` with a `try/except` at
  import time and falls back to the stdlib visitor automatically. Install
  it with `pip install radon` if you want closer parity with third-party
  reports; no other dependency is needed.
