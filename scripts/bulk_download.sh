#!/bin/bash
set -u

DATA_ROOT="${GRID_DATA_ROOT:-/data/grid}"
BULK_ROOT="${GRID_BULK_ROOT:-$DATA_ROOT/bulk}"
LOG_ROOT="${GRID_DOWNLOAD_LOG_ROOT:-$DATA_ROOT/logs/downloads}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
MANIFEST="$LOG_ROOT/bulk_download_${RUN_ID}.jsonl"

mkdir -p "$BULK_ROOT"/{eia,fred,edgar,gdelt,options}
mkdir -p "$LOG_ROOT"
cd "$DATA_ROOT"

log_manifest() {
  python3 - "$MANIFEST" "$@" <<'PY'
import json
import sys
from datetime import datetime, timezone

manifest_path = sys.argv[1]
payload = {"logged_at": datetime.now(timezone.utc).isoformat()}
for item in sys.argv[2:]:
    key, value = item.split("=", 1)
    payload[key] = value
with open(manifest_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
PY
}

file_size() {
  if [[ -f "$1" ]]; then
    stat -c%s "$1" 2>/dev/null || stat -f%z "$1" 2>/dev/null || echo "0"
  else
    echo "0"
  fi
}

run_download() {
  local dataset="$1"
  local url="$2"
  local target="$3"
  shift 3

  log_manifest \
    type=bulk_download \
    run_id="$RUN_ID" \
    dataset="$dataset" \
    state=queued \
    url="$url" \
    target="$target"

  (
    if wget -q "$url" -O "$target" "$@"; then
      log_manifest \
        type=bulk_download \
        run_id="$RUN_ID" \
        dataset="$dataset" \
        state=done \
        url="$url" \
        target="$target" \
        bytes="$(file_size "$target")"
    else
      log_manifest \
        type=bulk_download \
        run_id="$RUN_ID" \
        dataset="$dataset" \
        state=failed \
        url="$url" \
        target="$target"
    fi
  ) &
}

log_manifest type=bulk_download_run run_id="$RUN_ID" state=started bulk_root="$BULK_ROOT"

echo "=== 1. EIA BULK (several GB) ==="
cd "$BULK_ROOT/eia"
run_download "eia_petroleum" "https://api.eia.gov/bulk/PET.zip" "petroleum.zip"
run_download "eia_natural_gas" "https://api.eia.gov/bulk/NG.zip" "natural_gas.zip"
run_download "eia_electricity" "https://api.eia.gov/bulk/ELEC.zip" "electricity.zip"
run_download "eia_coal" "https://api.eia.gov/bulk/COAL.zip" "coal.zip"
run_download "eia_international" "https://api.eia.gov/bulk/INTL.zip" "international.zip"
echo "EIA downloads started in background"

echo "=== 2. FRED BULK ==="
cd "$BULK_ROOT/fred"
# All FRED series metadata
run_download "fred_all_series" "https://api.stlouisfed.org/fred/tags/series?api_key=bc8b4507787daf394e42f07b97d6c0fc&file_type=json&limit=100000" "fred_all_series.json"
echo "FRED metadata downloading"

echo "=== 3. SEC EDGAR FULL ==="
cd "$BULK_ROOT/edgar"
# Full company filing index
run_download "edgar_company_2024q1" "https://www.sec.gov/Archives/edgar/full-index/2024/QTR1/company.idx" "2024q1.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2024q2" "https://www.sec.gov/Archives/edgar/full-index/2024/QTR2/company.idx" "2024q2.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2024q3" "https://www.sec.gov/Archives/edgar/full-index/2024/QTR3/company.idx" "2024q3.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2024q4" "https://www.sec.gov/Archives/edgar/full-index/2024/QTR4/company.idx" "2024q4.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2025q1" "https://www.sec.gov/Archives/edgar/full-index/2025/QTR1/company.idx" "2025q1.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2025q2" "https://www.sec.gov/Archives/edgar/full-index/2025/QTR2/company.idx" "2025q2.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2025q3" "https://www.sec.gov/Archives/edgar/full-index/2025/QTR3/company.idx" "2025q3.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2025q4" "https://www.sec.gov/Archives/edgar/full-index/2025/QTR4/company.idx" "2025q4.idx" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_company_2026q1" "https://www.sec.gov/Archives/edgar/full-index/2026/QTR1/company.idx" "2026q1.idx" --header="User-Agent: GRID grid@ocmri.com"
# XBRL financial statements bulk
run_download "edgar_financials_2024q4" "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2024q4.zip" "financials_2024q4.zip" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_financials_2025q1" "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q1.zip" "financials_2025q1.zip" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_financials_2025q2" "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q2.zip" "financials_2025q2.zip" --header="User-Agent: GRID grid@ocmri.com"
run_download "edgar_financials_2025q3" "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q3.zip" "financials_2025q3.zip" --header="User-Agent: GRID grid@ocmri.com"
echo "SEC EDGAR downloading"

echo "=== 4. GDELT EVENTS ==="
cd "$BULK_ROOT/gdelt"
# Last 2 years of GDELT events (each ~100MB compressed)
for m in $(seq -w 01 12); do
    run_download "gdelt_2024${m}" "http://data.gdeltproject.org/events/202401${m}.export.CSV.zip" "2024${m}.zip"
done
for m in $(seq -w 01 12); do
    run_download "gdelt_2025${m}" "http://data.gdeltproject.org/events/202501${m}.export.CSV.zip" "2025${m}.zip"
done
echo "GDELT events downloading"

wait
log_manifest type=bulk_download_run run_id="$RUN_ID" state=completed bulk_root="$BULK_ROOT"
echo ""
echo "=== DOWNLOAD STATUS ==="
du -sh "$BULK_ROOT"/*/
du -sh "$BULK_ROOT"/
echo "Manifest: $MANIFEST"
