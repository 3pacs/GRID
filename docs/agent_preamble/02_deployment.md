### 2. Deployment — use `scripts/deploy.py` ONLY

The two server trees are now a **symlink pair**: `/data/grid_v4/grid_repo` → `/data/grid_v4/astrogrid_dedup`. Physical drift is impossible.

But you still must use the deploy helper — it hash-verifies every write, snapshots pre-images for bisectable rollback, and logs to `.grid_backups/deploy_log.jsonl`:

```bash
# One file
python3 scripts/deploy.py path/to/file.py

# Multiple files + snapshot + restart + smoke test (the safe full sequence)
python3 scripts/deploy.py --snapshot --restart --smoke path/to/file.py path/to/other.py

# Deploy all staged-in-git files
python3 scripts/deploy.py --staged --restart --smoke
```

**Forbidden:** raw `scp`, `rsync`, `ssh cp`. They bypass hash verification + audit logging. Any agent report that claims a successful deploy without running through `deploy.py` is rejected.
