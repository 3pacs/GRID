# panda worker.py sync — DEFERRED 2026-05-08

panda is currently off-tailscale per user. When it returns:
  scp /home/grid/grid_v4/grid_repo/scripts/worker.py panda:/tmp/worker.py.new
  ssh panda 'cp ~/grid_v4/astrogrid_dedup/scripts/worker.py /tmp/worker.py.bak && cp /tmp/worker.py.new ~/grid_v4/astrogrid_dedup/scripts/worker.py'
  ssh panda 'systemctl --user restart grid-worker.service'

Expected after re-register: panda 2x Tesla P100 -> ~32 GB total (was reporting 16 GB).
