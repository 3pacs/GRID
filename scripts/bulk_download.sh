#!/bin/bash
cd /data/grid
mkdir -p bulk/{eia,fred,edgar,gdelt,options}

echo "=== 1. EIA BULK (several GB) ==="
cd /data/grid/bulk/eia
wget -q "https://api.eia.gov/bulk/PET.zip" -O petroleum.zip &
wget -q "https://api.eia.gov/bulk/NG.zip" -O natural_gas.zip &
wget -q "https://api.eia.gov/bulk/ELEC.zip" -O electricity.zip &
wget -q "https://api.eia.gov/bulk/COAL.zip" -O coal.zip &
wget -q "https://api.eia.gov/bulk/INTL.zip" -O international.zip &
echo "EIA downloads started in background"

echo "=== 2. FRED BULK ==="
cd /data/grid/bulk/fred
# All FRED series metadata
wget -q "https://api.stlouisfed.org/fred/tags/series?api_key=bc8b4507787daf394e42f07b97d6c0fc&file_type=json&limit=100000" -O fred_all_series.json &
echo "FRED metadata downloading"

echo "=== 3. SEC EDGAR FULL ==="
cd /data/grid/bulk/edgar
# Full company filing index
wget -q "https://www.sec.gov/Archives/edgar/full-index/2024/QTR1/company.idx" -O 2024q1.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2024/QTR2/company.idx" -O 2024q2.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2024/QTR3/company.idx" -O 2024q3.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2024/QTR4/company.idx" -O 2024q4.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2025/QTR1/company.idx" -O 2025q1.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2025/QTR2/company.idx" -O 2025q2.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2025/QTR3/company.idx" -O 2025q3.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2025/QTR4/company.idx" -O 2025q4.idx --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/Archives/edgar/full-index/2026/QTR1/company.idx" -O 2026q1.idx --header="User-Agent: GRID grid@ocmri.com" &
# XBRL financial statements bulk
wget -q "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2024q4.zip" -O financials_2024q4.zip --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q1.zip" -O financials_2025q1.zip --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q2.zip" -O financials_2025q2.zip --header="User-Agent: GRID grid@ocmri.com" &
wget -q "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q3.zip" -O financials_2025q3.zip --header="User-Agent: GRID grid@ocmri.com" &
echo "SEC EDGAR downloading"

echo "=== 4. GDELT EVENTS ==="
cd /data/grid/bulk/gdelt
# Last 2 years of GDELT events (each ~100MB compressed)
for m in $(seq -w 01 12); do
    wget -q "http://data.gdeltproject.org/events/202401${m}.export.CSV.zip" -O "2024${m}.zip" 2>/dev/null &
done
for m in $(seq -w 01 12); do
    wget -q "http://data.gdeltproject.org/events/202501${m}.export.CSV.zip" -O "2025${m}.zip" 2>/dev/null &
done
echo "GDELT events downloading"

wait
echo ""
echo "=== DOWNLOAD STATUS ==="
du -sh /data/grid/bulk/*/
du -sh /data/grid/bulk/
