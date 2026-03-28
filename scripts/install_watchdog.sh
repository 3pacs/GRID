#!/bin/bash
# Install watchdog as a systemd timer
cat > /tmp/grid-watchdog.service << 'EOF'
[Unit]
Description=GRID Server Watchdog
After=network.target

[Service]
Type=oneshot
ExecStart=/bin/bash /home/grid/grid_v4/grid_repo/scripts/watchdog.sh
User=root
EOF

cat > /tmp/grid-watchdog.timer << 'EOF'
[Unit]
Description=GRID Watchdog Timer

[Timer]
OnBootSec=60
OnUnitActiveSec=60
AccuracySec=10

[Install]
WantedBy=timers.target
EOF

sudo cp /tmp/grid-watchdog.service /etc/systemd/system/
sudo cp /tmp/grid-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable grid-watchdog.timer
sudo systemctl start grid-watchdog.timer
echo "Watchdog installed — runs every 60 seconds"
