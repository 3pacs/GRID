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
   - `TimeoutStartSec=15min` and `KillMode=control-group`.
   - `ExecStopPost=-…/gem_daily_contain.py` appends `CONTAINED result=… status=…` to the day's claim receipt, whatever the outcome.
   - The timer uses `Persistent=false`, so there are no catch-up runs.
   - The quarantine drop-in keeps `RefuseManualStart=yes`.

## Pin (after the append-only code is merged and deployed)

```bash
PIN=<40-hex main SHA containing this directory>
sudo -u grid git clone --quiet /data/grid_v4/grid_release.releases/$PIN /data/grid_v4/grid-options-puller-pins/$PIN
sudo -u grid git -C /data/grid_v4/grid-options-puller-pins/$PIN checkout --quiet --detach $PIN
test "$(git -C /data/grid_v4/grid-options-puller-pins/$PIN rev-parse HEAD)" = "$PIN"
test -z "$(git -C /data/grid_v4/grid-options-puller-pins/$PIN status --porcelain --untracked-files=all)"
git -C /data/grid_v4/grid-options-puller-pins/$PIN merge-base --is-ancestor 01b19194 $PIN
```

## Render and install (no activation yet)

```bash
cd /data/grid_v4/grid-options-puller-pins/$PIN
OUT=/data/grid_v4/grid-options-puller-pins/gem-daily-units-$PIN
/data/grid_v4/venv/bin/python3 scripts/render_gem_daily_units.py $PIN $OUT
# back up the current drop-ins first, then:
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
sudo -u grid sh -c 'printf "pin=%s utc=%s\n" "$1" "$(date -u +%FT%TZ)" > /data/grid_v4/gem_daily/ACTIVATED' _ "$PIN"
sudo systemctl enable --now grid-options-puller.timer
systemctl list-timers grid-options-puller.timer
```

## First-run check

- Check `/var/log/grid-options-puller.log` for eight `GEM_VALIDATE … PASS` lines and one `GEM_CONTAIN result=success`.
- Check `/data/grid_v4/gem_daily/attempts/<day>`: it should hold one `STARTED` line and one `CONTAINED` line.
- Call `GET /api/v1/derivatives/options-batches/SPY?snap_date=<day>`. It should list the scheduler batch and the `gem` batch.

## Kill switch

Use `sudo systemctl disable --now grid-options-puller.timer`, or remove `/data/grid_v4/gem_daily/ACTIVATED`. The unit will then not start.
