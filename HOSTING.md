# GRID Hosting Architecture

## Recommended: DigitalOcean Droplet

Why: Full control, persistent processes, affordable, SSH access,
compatible with PostgreSQL + TimescaleDB + Hyperspace GPU support.

### Minimum Spec (no GPU)

Plan:    Basic Regular — 2 vCPU / 4GB RAM / 80GB SSD
Cost:    ~$24/month
OS:      Ubuntu 24.04 LTS
Region:  Closest to your physical location

### Recommended Spec (with Hyperspace inference)

Plan:    CPU-Optimized — 4 vCPU / 8GB RAM / 160GB SSD
Cost:    ~$48/month
OS:      Ubuntu 24.04 LTS

### GPU Option (for Hyperspace Research capability)

Plan:    GPU Droplet — 1x NVIDIA A100 or H100 as available
Cost:    $500-2000/month (only if Hyperspace research earnings justify it)
Note:    Start without GPU. Add later if Research rewards compound.

## Setup Sequence (run once on fresh Droplet)

```bash
# 1. Update system
sudo apt update && sudo apt upgrade -y

# 2. Install Docker (for PostgreSQL + TimescaleDB)
curl -fsSL https://get.docker.com | bash
sudo usermod -aG docker $USER

# 3. Install Python 3.11
sudo apt install python3.11 python3.11-venv python3-pip -y

# 4. Install Caddy (HTTPS reverse proxy — handles SSL automatically)
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install caddy -y

# 5. Install Hyperspace
curl -fsSL https://agents.hyper.space/cli | bash

# 6. Point your domain to the Droplet IP
# Add an A record: grid.yourdomain.com -> YOUR_DROPLET_IP
# The PWA requires HTTPS which requires a real domain.
# Freenom offers free domains. Cloudflare offers free DNS management.

# 7. Clone/upload GRID project to /opt/grid
sudo mkdir -p /opt/grid
sudo chown $USER:$USER /opt/grid
# scp -r ./grid user@YOUR_DROPLET_IP:/opt/grid

# 8. Run docker-compose for database
cd /opt/grid && docker-compose up -d

# 9. Apply schema
cd /opt/grid && python3 db.py

# 10. Start GRID API (see systemd service below)
# 11. Start Hyperspace node
# 12. Configure Caddy for HTTPS
```

## Caddy Configuration

Create /etc/caddy/Caddyfile:

```
grid.yourdomain.com {
    reverse_proxy localhost:8000
    encode gzip
    header {
        Strict-Transport-Security "max-age=31536000; includeSubDomains"
        X-Content-Type-Options nosniff
        X-Frame-Options DENY
    }
}
```

Then: `sudo systemctl reload caddy`

Your PWA will be served at https://grid.yourdomain.com with automatic
free SSL from Let's Encrypt. No certificate management needed.

## Process Management (systemd)

All three GRID processes run as systemd services:

- grid-api      : FastAPI backend
- grid-db       : Docker compose (PostgreSQL)
- hyperspace    : Hyperspace node

See server_setup/ directory for all service files.
