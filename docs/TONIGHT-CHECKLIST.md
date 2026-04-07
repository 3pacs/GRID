# Tonight's Checklist — Get GRID Fully Operational

## 1. Run the Colab Notebook (~15 min)
- Open `notebooks/gemma4_training.ipynb` in [Google Colab](https://colab.research.google.com)
- **Runtime → Change runtime type → GPU** (T4 for free, A100 for Pro)
- Cell 6: set `TASK` to each of these and run all cells 4 times:
  1. `"signal_classifier"`
  2. `"anomaly_narrator"` 
  3. `"edgar_extractor"`
  4. `"knowledge_mapper"`
- Download the 4 GGUF files when each finishes
- They auto-push to HuggingFace (`stepdadfinance/grid-gemma4-*`)

## 2. Deploy Micro Models via systemd (after Colab)
```bash
# Copy GGUFs to /data/models/micro/
mkdir -p /data/models/micro
# scp or download from HF:
# huggingface-cli download stepdadfinance/grid-gemma4-signal_classifier --local-dir /data/models/micro/

# Install systemd units (survive reboot)
sudo cp server_setup/grid-micro-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now grid-micro-classifier grid-micro-narrator grid-micro-extractor grid-micro-mapper

# Kill any old nohup processes
pkill -f "llama-server.*8082" 2>/dev/null
pkill -f "llama-server.*8083" 2>/dev/null
pkill -f "llama-server.*8084" 2>/dev/null
pkill -f "llama-server.*8085" 2>/dev/null
```

## 3. Backfill New FRED Series (~2 min)
```bash
cd /data/grid_v4/grid_repo
PYTHONPATH=. python3 -c "
from ingestion.fred import FREDPuller
from db import get_engine
p = FREDPuller(get_engine())
p.pull(['MMMFFAQ027S', 'WRMFNS', 'RRPONTSYD'])
"
```

## 4. Verify Freshness Guardian (just check)
```bash
# After ~2 hours, stale sources should be clearing:
PGPASSWORD=\$(grep DB_PASSWORD .env | cut -d= -f2) psql -h localhost -U grid -d griddb -c "
SELECT name, last_pull_at, 
  ROUND(EXTRACT(EPOCH FROM (NOW() - last_pull_at))/3600, 1) AS hours_ago
FROM source_catalog 
WHERE active AND (last_pull_at IS NULL OR last_pull_at < NOW() - INTERVAL '24 hours')
ORDER BY last_pull_at ASC NULLS FIRST
LIMIT 10
"
```
Should show fewer than 67 stale sources (was 67, targeting 0).

## 5. Restart API (pick up all code changes)
```bash
sudo systemctl restart grid-api
# Verify:
curl -s http://localhost:8000/api/v1/system/health | python3 -m json.tool | head -5
```

## 6. Clean Old Models (optional, saves ~150 GB)
```bash
# These are archived and no longer used:
ls -lh /data/models/archive/
# Safe to delete if space needed:
# rm /data/models/archive/nvidia_Llama-3_3-Nemotron-Super-49B-v1_5-Q5_K_M.gguf
# rm /data/models/archive/gemma-3-27b-it-Q5_K_M.gguf
```

## What's Already Done (no action needed)
- Gemma 4 31B running on :8080 ✓
- Hermes restarted with freshness guardian ✓  
- All code committed and pushed to GitHub ✓
- 0 test failures ✓
- Reference hallucination guard active ✓
- All data placeholders filled ✓
