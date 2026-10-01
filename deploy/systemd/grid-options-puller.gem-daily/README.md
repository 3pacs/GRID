# GEM daily options capture: pin, timer, activation

This runs the eight GEM tickers (OPCH, SPGI, FLUT, GEHC, GLND, SPY, QQQ, IWM) once per NYSE weekday session at 10:05 New York, after the scheduler's own 13:30 UTC options pull. Every GEM capture is its own immutable batch (`options_append_only_20260930`, `capture_source='gem'`). A later same-day scheduler capture never removes it; it stays replayable by `capture_batch_id`.

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
7. **CONTAIN.**
   - `Type=oneshot` with `TimeoutStartSec=15min`, so the timeout bounds the whole run, plus `KillMode=control-group`. The runner's own SIGALRM stops at 10:20 New York.
   - `ExecStopPost=-…/gem_daily_contain.py` appends `CONTAINED result=… status=…` to the day's claim receipt, whatever the outcome.
   - The timer uses `Persistent=false`, so there are no catch-up runs.
   - The quarantine drop-in keeps `RefuseManualStart=yes`.

**Timing margin.** In EDT, 10:05 New York is 14:05Z. On 2026-09-30 the scheduler's 13:30Z options pull completed at 13:57Z, so the margin is about 8 minutes. If the scheduler pull is still running at 14:05Z, the gate skips. The day's single claim is then used up and that day has no GEM batch. This fails closed. In EST the timer fires at 15:05Z, which leaves an hour.

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
systemd-analyze calendar --iterations=3 'Mon..Fri *-*-* 10:05:00 America/New_York'
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

- Check `/var/log/grid-options-puller.log` for eight `GEM_VALIDATE … PASS` lines and one `GEM_CONTAIN result=success`.
- Check `/data/grid_v4/gem_daily/attempts/<day>`: it should hold one `STARTED` line and one `CONTAINED` line.
- Call `GET /api/v1/derivatives/options-batches/SPY?snap_date=<day>`. It should list the scheduler batch and the `gem` batch.

## Kill switch

The primary kill switch is `sudo systemctl disable --now grid-options-puller.timer`, since only root can re-enable it. A secondary one is `sudo rm /data/grid_v4/gem_daily/ACTIVATED`, after which the unit does not start. Note that `grid` owns that directory and could re-create the marker. Both base units live in `/etc/systemd/system` (checked 2026-09-30). After the backup step, confirm that `SHA256SUMS` lists every copied file.
