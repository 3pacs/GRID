# Runbook — Deploy pipeline fixes (2026-04-29)

For an agent operating directly on **grid-svr**. Two PRs are now on `main`:

| SHA | PR | What it fixes |
|---|---|---|
| `c987f5e` | #63 | Conflict Resolution timeout · Orthogonality EEXIST · Options scan `numpy.bool` |
| `fe0c20b` | #64 | Orthogonality hardening — broken-symlink + racy EEXIST recovery |

Production cron alerts kept firing through 02:39 UTC because `grid-svr` is still on the pre-fix tip. Goal of this runbook: get `main` onto both server trees, restart services, smoke-test the three failure paths, and confirm the next pipeline run is clean.

## Server topology (read this first)

GRID runs from **two** live trees on grid-svr (per `scripts/deploy.py` and `deploy_to_grid_svr.sh`). Both must end up on `fe0c20b` or services will silently run stale code.

| Tree | Path | WorkingDirectory for |
|---|---|---|
| `grid_repo` | `/home/grid/grid_v4/grid_repo` (alias: `/data/grid_v4/grid_repo`) | `grid-hermes`, `grid-extractor`, `grid-intelligence`, `grid-realtime`, `grid-spider`, `grid-backlinker`, `grid-breaking-news` |
| `astrogrid_dedup` | `/data/grid_v4/astrogrid_dedup` | `grid-api` |

The pipeline cron (`scripts/run_full_pipeline.py`) lives under the first tree; **all three failures we're fixing are reachable from there**. The second tree only matters if `grid-api` imports the changed modules — `discovery/options_scanner.py` is imported from `api/routers/intel.py`, so yes, it does.

## Pre-flight (read-only)

```bash
# 1. Confirm both trees exist and are on the same SHA before you start
cd /home/grid/grid_v4/grid_repo && git log -1 --oneline
cd /data/grid_v4/astrogrid_dedup  && git log -1 --oneline   # if it's a git checkout

# 2. Inspect outputs/orthogonality on the primary tree — the EEXIST bug
#    means there's likely a stray file or broken symlink at this path.
#    Use ls -la (NOT just ls) so symlinks are visible.
ls -la /home/grid/grid_v4/grid_repo/grid/outputs/ | grep orthogonality
# Expected one of:
#   drwx...  outputs/orthogonality                  → directory, fine
#   -rw-...  outputs/orthogonality                  → stray file, fix code (#63) handles it
#   lrwx... outputs/orthogonality -> /missing/path  → broken symlink, fix code (#64) handles it

# 3. Confirm no pipeline run is currently in flight (avoid racing the cron)
ps -ef | grep -E 'run_full_pipeline|resolver|orthogonality|options_scanner' | grep -v grep
# If something's running, wait for it to finish or pause hermes (step 4).
```

## Deploy

```bash
# 4. Pause the scheduler so a cron-triggered pipeline run can't start
#    mid-deploy. (grid-hermes is the systemd-managed scheduler.)
sudo systemctl stop grid-hermes

# 5. Update the primary tree
cd /home/grid/grid_v4/grid_repo
git fetch origin main
git log --oneline HEAD..origin/main           # sanity-check what's coming
git pull origin main                          # should fast-forward to fe0c20b

# 6. Update the secondary tree.
#    Pick ONE of these depending on whether astrogrid_dedup is a git checkout:
#
#    a) If it IS a git checkout:
cd /data/grid_v4/astrogrid_dedup
git fetch origin main && git pull origin main
#
#    b) If it is NOT a git checkout (rsync-mirrored from primary):
rsync -av --delete \
  /home/grid/grid_v4/grid_repo/ \
  /data/grid_v4/astrogrid_dedup/
#
#    c) Or use the canonical helper from the primary tree (preferred —
#       it does atomic dual-write with SHA256 verification):
cd /home/grid/grid_v4/grid_repo
python3 scripts/deploy.py --staged   # if you have local-only changes
# (this runbook itself doesn't need deploy.py; both fixes are already on main.)

# 7. Install Python deps (cheap no-op if requirements.txt unchanged)
cd /home/grid/grid_v4/grid_repo
pip install -r requirements.txt --quiet

# 8. Frontend: this PR set is backend-only, no rebuild needed. Skip unless
#    pwa/dist is stale for unrelated reasons.

# 9. Migrations: this PR set is code-only, no schema changes. Confirm:
cd /home/grid/grid_v4/grid_repo
python -m alembic current
python -m alembic heads          # should match `current` — no pending migrations expected
# If alembic shows pending revisions UNRELATED to this fix, do NOT run them
# from this runbook — that's a separate decision.

# 10. Restart services
sudo systemctl restart grid-api grid-llamacpp grid-crucix grid-hermes
sudo systemctl is-active grid-api grid-llamacpp grid-crucix grid-hermes
#    Each line should print `active`.
```

## Verify (smoke tests)

Run these as the `grid` user on the server. Each one exercises exactly one of the three failure paths.

```bash
# 11. Health check
curl -sf http://localhost:8000/api/v1/system/health | head -20

# 12. Conflict Resolution — was timing out at the SELECT DISTINCT scan.
#     Should now complete (10-min statement_timeout, was 120s default).
cd /home/grid/grid_v4/grid_repo/grid
python -c "
from db import get_engine
from normalization.resolver import Resolver
r = Resolver(get_engine())
print(r.resolve_pending(lookback_days=30, workers=8))
"
# Expect: dict like {'resolved': N, 'conflicts_found': M, 'errors': 0}
# Failure mode to look for: psycopg2.errors.QueryCanceled — would mean
# the SET LOCAL didn't apply (transaction not opened) or 10 min still
# isn't enough (then the real problem is missing indexes on raw_series).

# 13. Orthogonality Audit — was failing with [Errno 17] File exists.
#     Should self-heal any stray file/symlink at outputs/orthogonality.
cd /home/grid/grid_v4/grid_repo/grid
python -c "
from db import get_engine
from store.pit import PITStore
from discovery.orthogonality import OrthogonalityAudit
engine = get_engine()
audit = OrthogonalityAudit(db_engine=engine, pit_store=PITStore(engine))
print(audit.run_full_audit())
"
# Expect: dict with n_features_analyzed, true_dimensionality, etc.
# Look in outputs/ for a freshly-created outputs/orthogonality/ directory.
# If a stray was found, you'll see one of these in the log:
#   'Output path … exists but is not a directory; moving to ….bak.<ts>'
#   'mkdir raced on …; moving to ….bak.<ts>.eexist and retrying'

# 14. Options Mispricing Scan — was failing with can't adapt 'numpy.bool'.
cd /home/grid/grid_v4/grid_repo/grid
python -c "
from db import get_engine
from discovery.options_scanner import OptionsScanner
s = OptionsScanner(get_engine())
opps = s.scan_all(min_score=5.0)
n = s.persist_scan(opps)
print(f'persisted {n} opps')
"
# Expect: 'persisted N opps' with no psycopg2.ProgrammingError.

# 15. End-to-end: trigger one full pipeline pass (long — 5-15 min)
cd /home/grid/grid_v4/grid_repo/grid
python scripts/run_full_pipeline.py 2>&1 | tee /tmp/pipeline_post_fix.log
# Expect: every step logs 'OK' at the bottom summary; no FAILED lines for
# 'Conflict Resolution', 'Orthogonality Audit', or 'Options Mispricing Scan'.
```

## Confirm in the wild

```bash
# 16. Watch the next scheduled pipeline run (cron normally fires hourly
#     or per hermes schedule). Tail the alerts inbox and pipeline log:
sudo journalctl -u grid-hermes -f --since "5 minutes ago"
# In another shell:
tail -f /home/grid/grid_v4/grid_repo/grid/server_log/*.log 2>/dev/null
```

If the next scheduled run completes without any of the three known errors, the deploy is good. The pre-fix alerts already in the inbox (01:01, 01:47, 02:39 UTC) are historical and won't be retroactively dismissed — ignore them.

## Rollback (if anything goes sideways)

```bash
# Both trees back to the previous tip (replace SHA with whatever step 1
# showed before you ran step 5).
cd /home/grid/grid_v4/grid_repo && git reset --hard <PREVIOUS_SHA>
cd /data/grid_v4/astrogrid_dedup && git reset --hard <PREVIOUS_SHA>   # or rsync from primary
sudo systemctl restart grid-api grid-llamacpp grid-crucix grid-hermes
```

The two fixes are localized to three files (`normalization/resolver.py`, `discovery/orthogonality.py`, `discovery/options_scanner.py`), so a partial rollback (cherry-pick revert of just one of c987f5e/fe0c20b) is also viable if only one fix misbehaves.

## What this runbook does NOT do

- **Doesn't run alembic [[migrations]]**: there are none for this PR set, and the project has unrelated pending revisions on the schedule audit branch — running `upgrade head` blindly would apply those too.
- **Doesn't rebuild the frontend**: backend-only change.
- **Doesn't fix the failing CI Backend Tests check**: PR #63 and #64 both merged with that check red, on the user's explicit override. The failure pre-dates this PR set; investigate separately.
- **Doesn't touch `grid-coordinator` or `grid-worker`**: per `CLAUDE.md` the canonical "restart all" set is `grid-api grid-llamacpp grid-crucix grid-hermes`. If your cron actually runs the pipeline via `grid-worker`, restart that too.
