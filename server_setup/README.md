# server_setup

Systemd units, Caddy config, and deploy scripts for `grid-svr`.

## Layout

```
server_setup/
├── Caddyfile                       # public reverse-proxy config
├── deploy.sh                       # bootstrap / update script
├── grid.slice                      # parent cgroup for all grid-* services
├── grid-*.service                  # one unit per long-running grid component
├── grid-*.timer                    # timer companions (spider, walk-forward)
├── grid-*.service.d/cgroup.conf    # per-service cgroup drop-ins (memory caps, OOM bias)
└── ssh.service.d/oom-protect.conf  # protects sshd from the OOM killer
```

## cgroup / OOM model

Every grid-* service is pinned to `grid.slice` via a `Slice=grid.slice` drop-in.
The slice is the blast-radius cap — even if one service leaks, the whole GRID
workload cannot exceed the slice's `MemoryMax` and starve sshd, postgres, MinIO,
or Caddy. Per-service caps inside the slice protect peer services from a single
runaway tenant.

### Tiers

| Tier      | MemoryHigh | MemoryMax | TasksMax | Members                                                                                                                                                                                                                              |
|-----------|------------|-----------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| llm-large | 40 G       | 48 G      | 4096     | grid-llamacpp (Nemotron-49B Q5_K_M)                                                                                                                                                                                                  |
| llm-large | 24 G       | 32 G      | 4096     | grid-llamacpp-oracle (Qwen3-32B Q4_K_M)                                                                                                                                                                                              |
| llm-micro | 6 G        | 8 G       | 2048     | grid-micro-classifier, grid-micro-extractor, grid-micro-mapper, grid-micro-narrator                                                                                                                                                  |
| infra     | 16 G       | 24 G      | 8192     | grid-api, grid-intelligence                                                                                                                                                                                                          |
| infra     | 8 G        | 12 G      | 4096     | grid-crucix                                                                                                                                                                                                                          |
| worker    | 4 G        | 6 G       | 4096     | grid-worker, grid-coordinator, grid-scheduler, grid-realtime, grid-assimilator, grid-backlinker, grid-tao-miner, grid-breaking-news, grid-extractor, grid-hermes, grid-spider, grid-walk-forward-daily |
| worker+   | 6 G        | 8 G       | 4096     | grid-walk-forward-weekly (365d sweep needs more headroom)                                                                                                                                                                            |

`grid.slice` parent: `MemoryHigh=400G`, `MemoryMax=440G`, on a 503 GiB host (~60 GiB headroom for kernel + sshd + postgres + ad-hoc admin work).

`OOMScoreAdjust` ladder (higher = killed first):
- `ssh.service`: **-900** (last resort)
- `grid-api`: 100 (critical — public surface)
- `grid-crucix`, `grid-intelligence`: 150
- everything else under `grid.slice`: 200

### Install

```bash
# from a checkout of the repo on grid-svr
sudo cp server_setup/grid.slice /etc/systemd/system/
sudo cp server_setup/grid-*.service /etc/systemd/system/
sudo cp server_setup/grid-*.timer /etc/systemd/system/

# per-service drop-ins
for d in server_setup/grid-*.service.d; do
    name=$(basename "$d")
    sudo mkdir -p "/etc/systemd/system/$name"
    sudo cp "$d"/*.conf "/etc/systemd/system/$name/"
done

# sshd OOM protection
sudo mkdir -p /etc/systemd/system/ssh.service.d
sudo cp server_setup/ssh.service.d/oom-protect.conf /etc/systemd/system/ssh.service.d/

sudo systemctl daemon-reload
sudo systemctl restart ssh.service        # picks up new OOMScoreAdjust
# grid-* services pick up the slice + caps on their next restart cycle
```

> Note: on Debian/Ubuntu the sshd unit is `ssh.service`; on RHEL/Fedora it is
> `sshd.service`. Adjust the `ssh.service.d` directory name to match the
> distro before installing.

### Verify

```bash
# slice exists and is accounting
systemctl status grid.slice
systemctl show grid.slice | grep -E '(MemoryHigh|MemoryMax|MemoryAccounting)'

# every grid-* service is in the slice
systemctl show grid-llamacpp.service | grep -E '(Slice|MemoryHigh|MemoryMax|OOMScoreAdjust)'

# live cgroup memory pressure
systemd-cgtop /grid.slice

# sshd is protected
systemctl show ssh.service | grep -E '(OOMScoreAdjust|MemoryMin)'
cat /proc/$(pgrep -f 'sshd: /usr/sbin')/oom_score_adj   # should be -900
```

### Rollback

```bash
sudo rm -rf /etc/systemd/system/grid-*.service.d
sudo rm -f /etc/systemd/system/grid.slice
sudo rm -rf /etc/systemd/system/ssh.service.d
sudo systemctl daemon-reload
sudo systemctl restart ssh.service
# grid-* services revert to un-bounded behavior on their next restart
```

## 2026-05-02 incident — context

External symptoms: TCP :22 accepted but reset at banner-write (sshd child
killed before `write(2)` of the banner returned), Caddy :443 / Redis :6379 /
Redpanda :9092 / micro-models :8082-8085 all refused. Postgres :5432 and the
primary uvicorn :8000 stayed up. Pattern (fork-dependent and
recently-restarted processes die first, long-running survivors keep running)
is the OOM-killer cascade signature: out of physical memory, kernel walks the
process list killing the highest `oom_score` tasks until it recovers — and on
a default-config host, sshd children rank near the top because they're young
and small. The drop-ins in this directory:

1. cap the GRID workload as a whole (`grid.slice`),
2. cap each tenant inside the slice so one runaway can't starve peers, and
3. push sshd to the bottom of the kill-list with `OOMScoreAdjust=-900` plus a
   tiny `MemoryMin` floor so the box stays reachable during the spike.

## Next steps (not in this PR)

- Enable `systemd-oomd` so user-slice pressure triggers a graceful kill before
  the kernel's blunt OOM killer fires.
- Set `kernel.core_pattern` (or `coredumpctl`) to capture core dumps from
  OOM-killed services so future incidents leave evidence.
- Add `MemoryPressureWatch=` alerting via journald → Hermes operator inbox.
