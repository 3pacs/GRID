# grid-orient

Rebuild the GRID codebase index for fast session orientation. Run this when the codebase structure has changed significantly (new modules, schema changes, new services).

## When to Use

- After adding new intelligence modules or significant refactors
- After schema migrations (new tables, columns)
- After deploying new services
- When the auto-loaded context feels stale or incomplete

## What It Does

1. Scans `intelligence/` for public function signatures
2. Queries DB schema (tables, columns, row counts)
3. Checks server service status
4. Maps integration points (what's wired, what's not)
5. Writes compact index to `.claude/CODEBASE_INDEX.md`
6. Index auto-loads via SessionStart hook every new conversation

## Execution

Run this scan to rebuild the index:

```bash
cd ~/dev/GRID

echo "## Scanning intelligence modules..."
for f in intelligence/*.py; do
    mod=$(basename "$f" .py)
    echo "### $mod"
    grep -n "^def \|^    def \|^class " "$f" 2>/dev/null | head -20
done

echo ""
echo "## Scanning ingestion modules..."
ls ingestion/*.py ingestion/altdata/*.py ingestion/international/*.py 2>/dev/null | wc -l
echo " total puller files"

echo ""
echo "## DB Schema..."
ssh grid-svr "PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost -t -c \"
    SELECT table_name, array_to_string(array_agg(column_name ORDER BY ordinal_position), ', ')
    FROM information_schema.columns
    WHERE table_schema = 'public'
    GROUP BY table_name
    ORDER BY table_name;
\""

echo ""
echo "## DB Row Counts..."
ssh grid-svr "PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost -t -c \"
    SELECT schemaname || '.' || relname AS table, n_live_tup AS rows
    FROM pg_stat_user_tables
    ORDER BY n_live_tup DESC
    LIMIT 20;
\""

echo ""
echo "## Server Services..."
ssh grid-svr "systemctl status grid-api grid-realtime grid-scheduler grid-hermes --no-pager 2>&1 | grep -E '(●|Active:)'"
```

After reviewing the output, update `.claude/CODEBASE_INDEX.md` with:
- New module functions (add to Function Index table)
- New DB tables/columns (add to Schema Quick Reference)
- Changed integration points (update Integration Map)
- New services (add to Server Operations)

The SessionStart hook reads this file automatically — no further action needed.

## Index Location

`.claude/CODEBASE_INDEX.md` — loaded by SessionStart hook into every new conversation.

## Related Files

- `scripts/session_context.sh` — live state generator (called by SessionStart hook)
- `.claude/settings.json` — hook configuration
- `CLAUDE.md` — main project guidelines (architecture rules, patterns, gotchas)
