# Migrations

GRID SQL migrations live under `migrations/*.sql` and are applied as the
`postgres` superuser:

```bash
sudo -u postgres psql griddb -f migrations/00XX_your_change.sql
```

Numbered migrations (`0020_*.sql`, `0021_*.sql`, ...) are the canonical,
ordered set. The `add_*.sql` files are older one-offs that pre-date the
numbering scheme; new work should always use the next number in sequence.

## The GRANT footer rule

Migrations run as `postgres`, but the API and every ingestor connect as
the unprivileged `grid` role. New tables created by `postgres` are owned
by `postgres` and produce `permission denied for table X` the first time
the API tries to read them — unless the migration ends with explicit
grants to `grid`.

**Every migration that creates a new table MUST include a GRANT footer.**
The footer template lives in [`migrations/_TEMPLATE.sql`](../migrations/_TEMPLATE.sql);
copy it into every new migration.

For each new table:

```sql
GRANT ALL ON <table> TO grid;
```

For each `SERIAL` / `BIGSERIAL` primary key (auto-creates a sequence):

```sql
GRANT USAGE, SELECT ON SEQUENCE <table>_<col>_seq TO grid;
```

For tables in a non-public schema:

```sql
GRANT USAGE ON SCHEMA <schema> TO grid;
GRANT ALL ON <schema>.<table> TO grid;
```

## Linting

The rule is enforced by [`scripts/lint_migrations.py`](../scripts/lint_migrations.py),
which scans every `migrations/*.sql` file and fails on any `CREATE TABLE`
that lacks the matching `GRANT ALL ON <table> TO grid` statement. Run it
before committing:

```bash
python3 scripts/lint_migrations.py
```

Wire it into pre-commit / CI to keep new migrations from regressing.

## Why this exists

The footer was forgotten on `0020`–`0026`, which caused repeated
"permission denied" failures in production every time a new table was
introduced. Each fix required a manual `psql` round trip on grid-svr to
issue the missing grants. The template + lint exists so that doesn't
happen again.
