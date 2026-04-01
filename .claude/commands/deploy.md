# Deploy

Commit changes, restart services, verify health.

## Instructions

1. **Stage and commit:**
   ```bash
   cd /data/grid_v4/grid_repo
   git add -A
   git status
   git diff --cached --stat
   git commit -m "type: description"
   ```

2. **Restart services:**
   ```bash
   sudo systemctl restart grid-api grid-hermes
   ```
   Note: grid-llamacpp and cloudflared rarely need restart.

3. **Wait for startup** (API takes ~30-60s to load):
   ```bash
   sleep 30
   curl -s http://localhost:8000/api/v1/system/health | python3 -m json.tool | head -15
   ```

4. **Verify all services:**
   ```bash
   sudo systemctl status grid-api grid-hermes grid-llamacpp
   ```

5. **Check health response includes:**
   - `database: true`
   - `features_registered: true`
   - `recent_data: true`
   - `pool_healthy: true`
   - `llm_available: true`

## Running services
| Service | Port | Process |
|---------|------|---------|
| grid-api | 8000 | uvicorn api.main:app |
| grid-hermes | — | scripts/hermes_operator.py |
| grid-llamacpp | 8080 | llama-server (Qwen 32B) |
| cloudflared | — | Cloudflare tunnel → grid.stepdad.finance |

## Gotchas
- API takes 30-60s to start (lots of imports + intelligence loop init)
- Don't restart grid-llamacpp unless model needs reloading (takes 2+ min)
- Hermes auto-recovers from crashes (systemd restart=always)
