<!--
Merging to main deploys to production (.github/workflows/deploy.yml).
Fill in every section. Write "n/a" and a reason when a section does not apply.
-->

## What and why

<!-- What this changes, and the problem or goal behind it. -->

**Base / stack:** <!-- "main", or "stacked on #NNN". After a stacked PR is retargeted to
main, run `gh pr update-branch <this PR>` so CI runs against main before you merge. -->

## Verified

<!-- The commands you ran and their results, for example
`python -m pytest tests/test_x.py -v` → 42 passed. Name any failure that also fails on a
clean origin/main. -->

## Rules checklist (CLAUDE.md, .claude/rules/)

- [ ] **PIT:** analytical and inference reads go through `store/pit.py` or
      `store/observations.py` with an `as_of`; nothing reads data that is newer than the
      decision timestamp.
- [ ] **SQL:** only parameterized SQLAlchemy `text()` with bound params; no f-strings,
      `.format()` or concatenation in SQL.
- [ ] **Journal:** no update or delete of decision-journal rows; confidence and
      probability are validated to [0, 1] and are finite.
- [ ] **Data sources:** a new source has an ingestion module, an `entity_map.py` entry and
      PIT timestamps, and is registered in the scheduler.
- [ ] **Tests:** every new module has a test file in `tests/`, and a bug fix includes the
      test that would have caught it.
- [ ] **Config and secrets:** new settings come from `config.py`, `.env.example` is
      updated, and no secrets, `.env` files or credentials are committed.
- [ ] **Migrations:** alembic still has a single head.
- [ ] **Logging:** `log.error` is used only for real application bugs.

## Deploy and activation

<!-- What changes in production on merge: services restarted, migrations, new timers or
units, backfills. List any manual step after merge, in order, or write "none". -->

## Not done

<!-- What is deliberately left out, still unverified, or blocked. -->

## Provenance

<!-- Agent or person, host, and session or branch, for example
"Claude Code on precision5520, session <id>". -->
