# GRID W4d — Hermes automated Obsidian backlink step no longer rewrites tracked docs

Release-lead follow-up (now Fable-owned). Files touched:
`scripts/obsidian_backlinks.py` (new functions, `--apply` CLI path unchanged),
`scripts/hermes_operator.py` (`_run_obsidian_cycle`'s step 5 rewired to use
them), `tests/test_obsidian_backlinks_no_repo_writes.py` (new), this doc.

## The bug

`scripts/hermes_operator.py::_run_obsidian_cycle`'s step 5 (previously ~line
1438-1465) called `scripts.obsidian_backlinks.add_wikilinks()` on every file
`collect_markdown_files()` returned, and — whenever `add_wikilinks` found any
text matching an entry in `CONCEPT_LINKS` or the doc-stem registry — wrote the
annotated result straight back onto **that same tracked file**:

```python
for f in files:
    content = f.read_text(encoding="utf-8", errors="replace")
    new_content, changes = add_wikilinks(content, f, all_entities)
    if changes:
        f.write_text(new_content, encoding="utf-8")   # <-- rewrote the source file
        backlinks_added += len(changes)
```

This step ran gated only by `stubs_created > 0` from step 4 (concept stub
refresh), which is true on nearly every cycle a real docs corpus has — so
`hermes_operator.py`, running as an unattended background daemon (every ~5
minutes per `_run_obsidian_cycle`'s cadence comment), was silently mutating
tracked source files in the release tree: `git diff` on the checkout would
show wikilink insertions nobody asked for, appearing at arbitrary times,
attributable to no commit or review.

## What it used to rewrite (`collect_markdown_files()`, `SCAN_DIRS` = `docs/`
recursive + repo-root `*.md`, skipping `.claude/`, `node_modules/`, `.git/`,
`pwa/`, `__pycache__/`, `.obsidian/`, and `SKIP_FILES =
{.coordination.md, LICENSE.md}`)

As measured against this worktree (184 files total):

**Repo-root files (8):**
- `AGENTS.md`
- `ATTENTION.md`
- `CLAUDE.md`
- `DATA_SOURCES_CATALOG.md`
- `DEV-NOTES-DATA-INTEGRITY.md`
- `FIRST_DAY_REPORT.md`
- `HOSTING.md`
- `README.md`

**`docs/` by subdirectory (14 groups, file counts as measured):**
- `docs/*.md` (top level, 56 files) — e.g. `docs/architecture.md`,
  `docs/api-reference.md`, `docs/AGENT_PROMPT_TEMPLATE.md`,
  `docs/MODULE_INVENTORY.md`, `docs/SERVER-SERVICES.md`, and every
  `docs/astrogrid-*.md` page
- `docs/planning/` (38 files)
- `docs/superpowers/` (20 files)
- `docs/audits/` (16 files) — e.g. `ARCHITECTURE_REVIEW.md`,
  `CODE_REVIEW.md`, `DATABASE_REVIEW.md`
- `docs/decisions/` (15 files)
- `docs/handoffs/` (13 files) — including this doc's own directory
- `docs/agent_preamble/` (6 files)
- `docs/reference/` (4 files)
- `docs/dead-ends/` (3 files)
- `docs/agent-reports/` (1 file)
- `docs/playbooks/` (1 file)
- `docs/research/` (1 file)
- `docs/scoring/` (1 file)
- `docs/synthesis/` (1 file)

22 patterns/groups, 184 tracked files total — comparable to the "~30 file
patterns" this task named (the exact grouping granularity is a judgment
call; every one of these was a real, tracked, in-place-rewrite target).

## What it does now

`scripts/obsidian_backlinks.py` gained two functions
(`resolve_backlinks_output_dir()`, `write_annotated_copy()`); the manual CLI
(`python scripts/obsidian_backlinks.py --apply`) is **unchanged** — a human
runs it, reviews the diff, and commits through the normal PR flow, which is a
deliberate, reviewed action, not an unattended daemon's side effect. Only
`hermes_operator.py`'s automated path was rewired:

1. `resolve_backlinks_output_dir()` picks a directory to write annotated
   copies into, **never** inside the repository checkout (`GRID_ROOT`):
   - `GRID_OBSIDIAN_BACKLINKS_DIR` env var, if set to a non-empty value
     (set it to an empty string to explicitly disable the step).
   - Otherwise, `<config.settings.OBSIDIAN_VAULT_PATH>/grid-backlinks` — a
     subdirectory of the same Obsidian vault path
     `ingestion/altdata/obsidian_sync.py` already reads/writes
     (`~/Documents/Obsidian Vault` by default), so no new "where does this
     go" answer is needed for operators who already have that vault
     configured.
   - Returns `None` (and the caller logs why, then skips that cycle
     entirely) when: the env var is explicitly empty; the resolved
     directory would land inside `GRID_ROOT` (defensive — refused even
     though neither resolution path above can produce this today, so a
     future config change can't silently reintroduce the bug); or the
     directory cannot be created (permissions, unmounted vault, etc.).
2. `write_annotated_copy(output_dir, source_file, new_content)` mirrors
   `source_file`'s path relative to `GRID_ROOT` under `output_dir` and
   writes there — `source_file` itself is only ever read.
3. `hermes_operator.py::_run_obsidian_cycle` step 5 now calls
   `resolve_backlinks_output_dir()` once per cycle; if it returns `None`,
   the step is skipped with a `log.debug` reason and nothing under
   `GRID_ROOT` is touched at all (not even a read past
   `collect_markdown_files()`... actually it still reads the source files
   to compute `changes`? No — the skip happens BEFORE `collect_markdown_files()`
   is even called, so a fully-disabled step does zero I/O beyond the env/
   config check). Otherwise, exactly the same link-detection logic runs,
   but every write goes through `write_annotated_copy()` into the resolved
   output directory instead of `f.write_text(...)` on the source.

## Tests

`tests/test_obsidian_backlinks_no_repo_writes.py` (7, no real Postgres, no
real vault — a fake repo root under `tmp_path` with its own tracked-looking
`README.md`/`docs/ARCHITECTURE.md`, `GRID_ROOT`/`SCAN_DIRS` monkeypatched to
point at it):
- source docs are byte-identical after the step runs, and something is
  written, just not onto them;
- writes land only under the configured output directory, mirroring
  relative paths, and the mirrored copy actually contains the new
  `[[wikilink]]`;
- explicit env var wins over the vault-path default;
- an empty env var disables the step (`None`, no directory created);
- the vault-path default is used when the env var is unset;
- a resolved directory that would land inside the fake repo checkout is
  refused (`None`, directory never even created) — the defensive check;
- a directory that cannot be created (blocked by a same-named file) returns
  `None` rather than raising.

Pre-existing `tests/test_obsidian_backlinks.py` (4) still passes unmodified.

## What could not be verified without the vault/DB

- **Real vault behavior**: this fix was verified entirely against a fake
  `tmp_path` repo root and a fake vault path (both monkeypatched) — no run
  against the actual `~/Documents/Obsidian Vault` or any real Hermes
  process. Whether Obsidian itself correctly indexes the mirrored copies
  under `<vault>/grid-backlinks/` (graph view, backlink panel, etc.) was not
  checked — that requires an actual running Obsidian instance pointed at
  that vault, which this task's boundary (no DB/vault access) excludes.
- **Production `_run_obsidian_cycle` end-to-end**: `run_sync`,
  `regenerate_dashboard`, and `run_agent_cycle` (steps 1-3 of the same
  function) all need a live database and were not exercised here — only
  step 5's write-target logic was changed and tested. A full cycle run
  against grid-svr's live Postgres + vault was not performed.
- **Whether `GRID_OBSIDIAN_BACKLINKS_DIR` needs to be set on any live host**:
  not checked against grid-svr's actual environment — if
  `settings.OBSIDIAN_VAULT_PATH` there resolves to something unexpected (a
  path that doesn't exist, isn't writable by the `grid` user, etc.), the
  step will skip with a logged reason rather than error, but that has not
  been confirmed against the real host's filesystem/permissions.
