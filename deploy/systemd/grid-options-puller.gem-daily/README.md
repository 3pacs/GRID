# GEM daily options capture: pin, timer, activation

This runs the eight GEM tickers (OPCH, SPGI, FLUT, GEHC, GLND, SPY, QQQ, IWM) once per NYSE weekday session, started by the scheduler's own 13:30 UTC options pull completing (event-triggered, see below). Every GEM capture is its own immutable batch (`options_append_only_20260930`, `capture_source='gem'`). A later same-day scheduler capture never removes it; it stays replayable by `capture_batch_id`.

It reuses the existing `grid-options-puller.service` / `.timer` units. Their legacy hourly config stays on disk; these drop-ins override it.

| File in this directory | Installed as |
|---|---|
| `50-grid652-immutable-pin.conf.template` (rendered) | `/etc/systemd/system/grid-options-puller.service.d/50-grid652-immutable-pin.conf` |
| `99-grid652-quarantine.conf` | `/etc/systemd/system/grid-options-puller.service.d/99-grid652-quarantine.conf` |
| `timer-50-gem-daily.conf` | `/etc/systemd/system/grid-options-puller.timer.d/50-gem-daily.conf` |

## Gates, VALIDATE and CONTAIN

The runner is `scripts/gem_daily_capture.py`. Every gate fails closed.

1. **Activation identity.** `/data/grid_v4/gem_daily/ACTIVATED` exists. The code runs from a clean clone at `/data/grid_v4/grid-options-puller-pins/<pin>` whose HEAD equals `GEM_DAILY_PIN_SHA` and descends from #653.
2. **One attempt per UTC day.** A claim file lives at `/data/grid_v4/gem_daily/attempts/<day>`.
3. **Session.** The day is on or after 2026-10-01 and is an NYSE session. The time is 09:30–16:00 New York and before the absolute 10:20 New York deadline, which is also enforced by SIGALRM.
4. **Scheduler.** Since 13:29 UTC, the grid-scheduler journal shows exactly one `Starting daily pulls … market_open=True` and exactly one positive `Options daily pull complete`. Both lines come from the **same** systemd invocation ID and PID, and there is no options failure or skip line.
5. **Schema.** The append-only store is live: `options_snapshots` is a view, and the append-only triggers and constraints are present.
6. **VALIDATE.** For each ticker, `GEM_VALIDATE <T> PASS|FAIL batch=… ordinal=…` confirms:
   - the batch is registered with source `gem`;
   - stored rows = registered rows = reported rows;
   - calls and puts are both present, with 1–6 expiries;
   - the provider quote time is on the session day;
   - the capture falls inside the New York session.

   For each ticker that fails, a `GEM_REASON <T> attempt=N status=… reason=…` line follows. It gives the puller's own counts (failed fetches, undated quotes, empty expiries, contracts, timed out). Any other error text is reduced to `reason=error=other`, so no driver or connection text reaches the log or the receipt.
7. **RETRY.** The tickers that failed VALIDATE get exactly one more capture at **10:00 New York**. If the first pass ends later than that, the retry runs at once; it never starts at or after the 10:05 latest start. On the receipt:
   - `GEM_RETRY START tickers=[…]` opens the retry;
   - each retried ticker gets its own `GEM_VALIDATE <T> PASS|FAIL … attempt=2` line;
   - `GEM_RETRY SKIP latest start 10:05 New York reached failed=[…]` means no retry ran.
   - `GEM_RETRY FAIL <reason>` means the retry itself errored (database, provider, or the 10:20 alarm while it waited or ran). The first-pass verdicts stand, and the run still ends in `GEM_RESULT`.

   The retry's verdict replaces the first one in `GEM_RESULT`.
   Why: a thin name whose first regular-session trade comes after the early capture still carries the previous session's quote time. The capture is then correctly refused as undated. This is the likely cause for GLND on 2026-11-17/18, after Cboe became the primary provider. The retry captures it again once it has traded.
8. **RESULT.** One `GEM_RESULT PASS|PARTIAL|FAIL pass=N fail=M failed=[T,…]` line closes every run that reached VALIDATE, in the log and the day's claim receipt. The runner exits 0 for `PASS` and `PARTIAL`, and 1 only for `FAIL` (no ticker passed), an incomplete result set or a run error (`GEM_FAILED …`). A ticker that fails VALIDATE keeps its own `GEM_VALIDATE <T> FAIL` line; it no longer turns the unit red on its own, so read `GEM_RESULT` (or grep `PARTIAL`) to see it.
9. **CONTAIN.**
   - `Type=oneshot` with `TimeoutStartSec=55min` (09:31 arm to the 10:20 deadline is 49 min), so the timeout bounds the whole run, plus `KillMode=control-group`. The runner's own SIGALRM stops at 10:20 New York.
   - `ExecStopPost=-…/gem_daily_contain.py` appends `CONTAINED result=… status=…` to the day's claim receipt, whatever the outcome.
   - The timer uses `Persistent=false`, so there are no catch-up runs.
   - The quarantine drop-in keeps `RefuseManualStart=yes`.

**Event-triggered start (GEM-EV).** The timer only arms the runner at 09:31 New York. The runner then follows `journalctl -u grid-scheduler -f` and starts the capture within about two seconds of the scheduler's `Options daily pull complete` line. That line must come from the same invocation and PID as the `Starting daily pulls` line before it. Before the capture starts, a strict whole-window check repeats the original gate. A `GEM_TRIGGER scheduler_options_complete=… lag_s=…` line goes to the log and to the day's claim receipt.

- If the line has not appeared by **10:05 New York** (the latest start), the day is recorded as `GEM_SKIP scheduler options not complete by latest start`.
- A failure or skip line, a restart between the start and completion lines, or a second pull records `GEM_SKIP scheduler options journal gate`.
- In EDT the capture now starts about 13:57Z instead of 14:05Z. In EST the scheduler finishes before the open, so the capture starts at 09:31 New York: captures must lie inside the session.

**Quarantine marker change.** Installing `99-grid652-quarantine.conf` deliberately replaces the GRID-652 forensics `activation-held` condition with the GEM daily marker. That marker is created **root-owned** (below). Its directory belongs to `grid`, so the service account could still delete the marker, which is fail-safe because it deactivates. Activation also needs the timer enabled, which only root can do.

## Pin (after the append-only code is merged and deployed)

```bash
PIN=<40-hex main SHA containing this directory>
P=/data/grid_v4/grid-options-puller-pins/$PIN
sudo -u grid git clone --quiet /data/grid_v4/grid_release.releases/$PIN "$P"
sudo -u grid git -C "$P" checkout --quiet --detach $PIN
test "$(sudo -u grid git -C "$P" rev-parse HEAD)" = "$PIN"
test -z "$(sudo -u grid git -C "$P" status --porcelain --untracked-files=all)"
sudo -u grid git -C "$P" merge-base --is-ancestor 01b19194 $PIN
```

## Render and install (no activation yet)

```bash
OUT=/data/grid_v4/grid-options-puller-pins/gem-daily-units-$PIN   # sibling of the pin, never inside it
sudo -u grid /data/grid_v4/venv/bin/python3 "$P/scripts/render_gem_daily_units.py" $PIN $OUT
systemctl is-enabled grid-options-puller.timer; systemctl is-active grid-options-puller.timer   # expect disabled / inactive
BK=/home/grid/backups/gem_daily_units_$(date -u +%Y%m%dT%H%M%SZ); sudo install -d -m 0700 "$BK"
sudo cp -a /etc/systemd/system/grid-options-puller.service.d /etc/systemd/system/grid-options-puller.service /etc/systemd/system/grid-options-puller.timer "$BK"/
sudo sh -c "cd $BK && find . -type f -exec sha256sum {} + > SHA256SUMS"
sudo install -m 0644 -o root -g root $OUT/grid-options-puller.service.d/*.conf /etc/systemd/system/grid-options-puller.service.d/
sudo install -d -m 0755 /etc/systemd/system/grid-options-puller.timer.d
sudo install -m 0644 -o root -g root $OUT/grid-options-puller.timer.d/50-gem-daily.conf /etc/systemd/system/grid-options-puller.timer.d/
sudo systemctl daemon-reload
systemctl cat grid-options-puller.service grid-options-puller.timer
systemd-analyze calendar --iterations=3 'Mon..Fri *-*-* 09:31:00 America/New_York'
```

## Activation

Activation is operator-authorized and needs a verified scheduler restart onto the append-only code. The first run can be no earlier than Thu 2026-10-01. If the scheduler restart is not verified by 13:20 UTC, skip that day.

```bash
sudo -u grid install -d -m 0750 /data/grid_v4/gem_daily /data/grid_v4/gem_daily/attempts
printf 'pin=%s utc=%s\n' "$PIN" "$(date -u +%FT%TZ)" | sudo install -m 0644 -o root -g root /dev/stdin /data/grid_v4/gem_daily/ACTIVATED
sudo systemctl enable --now grid-options-puller.timer
systemctl list-timers grid-options-puller.timer
```

## First-run check

- Check `/var/log/grid-options-puller.log` for eight `GEM_VALIDATE … PASS` lines, one `GEM_RESULT PASS pass=8 fail=0 failed=[]` and one `GEM_CONTAIN result=success`. `GEM_RESULT PARTIAL` also ends in `result=success`; its `failed=[…]` list names the tickers to look at.
- Check `/data/grid_v4/gem_daily/attempts/<day>`. It should hold `STARTED`, then `GEM_TRIGGER … lag_s=…` (or a `GEM_SKIP` with its reason), the `GEM_VALIDATE` lines (with a `GEM_REASON` line for each failure, and `GEM_RETRY` plus `attempt=2` lines when a retry ran), `GEM_RESULT`, and finally `CONTAINED`.
- Call `GET /api/v1/derivatives/options-batches/SPY?snap_date=<day>`. It should list the scheduler batch and the `gem` batch.

## Re-pin (move a live, activated unit onto newer code)

The timer and the `ACTIVATED` marker stay as they are; only the pin clone and the rendered `50-grid652-immutable-pin.conf` change. The runner checks that `ACTIVATED` exists, not which pin it names. `NEW` must be a merged main SHA that is deployed, so `/data/grid_v4/grid_release.releases/$NEW` exists. Do it outside 09:31–10:20 New York on a session day.

```bash
NEW=<40-hex deployed main SHA>
P=/data/grid_v4/grid-options-puller-pins/$NEW
sudo -u grid git clone --quiet /data/grid_v4/grid_release.releases/$NEW "$P"
sudo -u grid git -C "$P" checkout --quiet --detach $NEW
test "$(sudo -u grid git -C "$P" rev-parse HEAD)" = "$NEW"
test -z "$(sudo -u grid git -C "$P" status --porcelain --untracked-files=all)"
sudo -u grid git -C "$P" merge-base --is-ancestor 01b19194 $NEW
OUT=/data/grid_v4/grid-options-puller-pins/gem-daily-units-$NEW
sudo -u grid /data/grid_v4/venv/bin/python3 "$P/scripts/render_gem_daily_units.py" $NEW $OUT
sudo diff -u /etc/systemd/system/grid-options-puller.service.d/99-grid652-quarantine.conf $OUT/grid-options-puller.service.d/99-grid652-quarantine.conf   # expect no output
BK=/home/grid/backups/gem_daily_units_$(date -u +%Y%m%dT%H%M%SZ); sudo install -d -m 0700 "$BK"
sudo cp -a /etc/systemd/system/grid-options-puller.service.d "$BK"/
sudo install -m 0644 -o root -g root $OUT/grid-options-puller.service.d/50-grid652-immutable-pin.conf /etc/systemd/system/grid-options-puller.service.d/
sudo systemctl daemon-reload
systemctl cat grid-options-puller.service | grep -c "$NEW"   # expect 6 (the rendered drop-in names the pin six times)
systemctl is-enabled grid-options-puller.timer; systemctl list-timers grid-options-puller.timer
```

Rollback: `sudo install -m 0644 -o root -g root $BK/grid-options-puller.service.d/50-grid652-immutable-pin.conf /etc/systemd/system/grid-options-puller.service.d/ && sudo systemctl daemon-reload`. The old pin clone stays on disk until the new one has passed a session.

## Kill switch

The primary kill switch is `sudo systemctl disable --now grid-options-puller.timer`, since only root can re-enable it. A secondary one is `sudo rm /data/grid_v4/gem_daily/ACTIVATED`, after which the unit does not start. Note that `grid` owns that directory and could re-create the marker. Both base units live in `/etc/systemd/system` (checked 2026-09-30). After the backup step, confirm that `SHA256SUMS` lists every copied file.
