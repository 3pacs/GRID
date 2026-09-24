# paper_log gex_levels_v1 — deploy kit

Prepares the SPY GEX structural-levels forward paper log
(`docs/paper_log/gex-levels-v1-preregistration.md`, `paper_log/gex_levels/`)
for cron on grid-svr. This kit **prepares** the install; it does not run,
activate, or schedule anything by itself — every step below that touches
grid-svr's crontab is manual and reviewed by the operator first.

Read the pre-registration before installing anything: nothing about the
schedule, thresholds, or hypotheses may change after the first logged
session without a new pre-registration (v2).

## What gets installed where

| Path (on grid-svr) | What |
|---|---|
| `/data/grid/paper_log/code/<sha>/` | One immutable, `git archive`-extracted copy of the code, per installed commit |
| `/data/grid/paper_log/code/<sha>/VERSION` | That commit's SHA — read by `storage.resolve_code_sha()`, becomes every record's `code_sha` |
| `/data/grid/paper_log/gex_levels_v1/gex_levels_v1.jsonl` | The append-only research record. Permanent. Never deleted by this kit. |
| `/data/grid/paper_log/gex_levels_v1/job.log` | Combined stdout/stderr from every cron run |
| grid-svr crontab (manual) | Two lines, `CRON_TZ=America/New_York`, 08:45 and 16:30 ET Mon–Fri |

## Install

From a checkout of the target commit (this worktree, or `origin/main` /
the PR branch once merged):

```bash
SHA=<commit-sha>
git archive --format=tar.gz -o /tmp/gex-levels-${SHA}.tar.gz ${SHA}
scp /tmp/gex-levels-${SHA}.tar.gz grid-svr:/tmp/
scp deploy/paper_log/install_gex_levels.sh grid-svr:/tmp/
ssh grid-svr "bash /tmp/install_gex_levels.sh ${SHA}"
```

The script extracts the code, writes `VERSION`, creates the log
directory, and **prints** the crontab block — it does not touch crontab.
Read the printed block, then install it yourself:

```bash
ssh grid-svr
crontab -l > /tmp/crontab.bak.$(date +%s)   # keep a copy first
crontab -e                                   # paste the block at the END
```

grid-svr's system TZ is UTC and its crontab already has several other
`CRON_TZ=America/Los_Angeles` blocks earlier in the file — this block's
own `CRON_TZ=America/New_York` line only affects entries that follow it,
so it must go at the very end, not be spliced in earlier.

## Smoke-testing before trusting it on a real schedule

Never run the installed cron copy by hand against the real log directory
to "just see if it works" — that would write a real record into the
permanent research log outside the pre-registered schedule. Instead, run
a throwaway copy against a throwaway `--log-dir`:

```bash
ssh grid-svr
TMPDIR=/tmp/claude-paperlog-smoke-$(date +%s)
mkdir -p "$TMPDIR"
git archive --format=tar.gz HEAD | tar -x -C "$TMPDIR"   # from the branch checkout, piped over ssh, or scp'd like the real archive
cd "$TMPDIR" && echo <sha> > VERSION
set -a && source /home/grid/grid_v4/grid_repo/.env && set +a
PYTHONPATH="$TMPDIR" /data/grid_v4/venv/bin/python -m paper_log.gex_levels preopen --log-dir "$TMPDIR/smoke_log"
PYTHONPATH="$TMPDIR" /data/grid_v4/venv/bin/python -m paper_log.gex_levels status --log-dir "$TMPDIR/smoke_log"
cat "$TMPDIR/smoke_log/gex_levels_v1.jsonl"
rm -rf "$TMPDIR"
```

This is read-only against the database (preopen only; the other three
commands never open one) and writes no orders — but it is still a live
read against production data, so treat the printed record as a **smoke
test**, not a real pre-registered session, and delete the temp directory
afterward.

## Uninstall

1. `crontab -e` and delete the block between the
   `---8<--- paper_log gex_levels_v1 ... ---8<---` markers (including its
   `CRON_TZ=America/New_York` line).
2. Confirm nothing is mid-run: `ls /tmp/paper-log-gex-levels-*.lock`
   should be absent (or clearly stale).
3. `rm -rf /data/grid/paper_log/code/<sha>` is safe any time — it's a
   disposable `git archive` extraction, reproducible from git.
4. **Do not delete `/data/grid/paper_log/gex_levels_v1/gex_levels_v1.jsonl`**
   unless you are deliberately abandoning v1 — the pre-registration
   commits to keeping the log as the permanent record ("the v1 log is
   kept as-is"). If it truly needs to go, archive it (e.g. into the vault
   mirror the pre-registration's Integrity section describes) before
   removing it here.

## Upgrading the code (bugfix, not a v2)

The pre-registration's rules are frozen; the code that implements them
can still get a non-substantive bugfix. Re-run the install steps above
with the new SHA — this creates a second `/data/grid/paper_log/code/<new-sha>/`
directory alongside the old one — then edit the two cron lines in place to
point at the new `<new-sha>` path (`cd`, `PYTHONPATH`) rather than
re-appending a duplicate block. The JSONL log is untouched either way; its
per-record `code_sha` will simply show the new commit from that point on.
