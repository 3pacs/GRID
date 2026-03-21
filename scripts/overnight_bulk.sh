#!/bin/bash
echo "=== OVERNIGHT BULK DOWNLOAD ==="
echo "Started: $(date)"

# ═══════════════════════════════════════════
# 1. FULL EDGAR ARCHIVE
# ═══════════════════════════════════════════
echo ""
echo "=== EDGAR FULL ARCHIVE ==="
mkdir -p /data/grid/bulk/edgar/filings
cd /data/grid/bulk/edgar

# Full XBRL financial statements - every quarter since 2009
for year in $(seq 2009 2025); do
    for qtr in 1 2 3 4; do
        url="https://www.sec.gov/files/dera/data/financial-statement-data-sets/${year}q${qtr}.zip"
        out="financials_${year}q${qtr}.zip"
        if [ ! -f "$out" ]; then
            echo "  Downloading $out..."
            wget -q --header="User-Agent: GRID grid@ocmri.com" "$url" -O "$out" 2>/dev/null
            sleep 2
        fi
    done
done

# Full filing indices - every quarter since 2000
for year in $(seq 2000 2026); do
    for qtr in 1 2 3 4; do
        url="https://www.sec.gov/Archives/edgar/full-index/${year}/QTR${qtr}/company.idx"
        out="idx_${year}q${qtr}.idx"
        if [ ! -f "$out" ]; then
            wget -q --header="User-Agent: GRID grid@ocmri.com" "$url" -O "$out" 2>/dev/null
            sleep 1
        fi
    done
done

# Insider trading Form 4 bulk
for year in $(seq 2020 2026); do
    for qtr in 1 2 3 4; do
        url="https://www.sec.gov/Archives/edgar/full-index/${year}/QTR${qtr}/form.idx"
        out="form_idx_${year}q${qtr}.idx"
        if [ ! -f "$out" ]; then
            wget -q --header="User-Agent: GRID grid@ocmri.com" "$url" -O "$out" 2>/dev/null
            sleep 1
        fi
    done
done

# Mutual fund holdings
wget -q --header="User-Agent: GRID grid@ocmri.com" "https://www.sec.gov/files/dera/data/mutual-fund-prospectus-risk-return-summary-data-sets/2025q1.zip" -O mf_2025q1.zip 2>/dev/null

echo "EDGAR done: $(du -sh /data/grid/bulk/edgar/)"

# ═══════════════════════════════════════════
# 2. EIA FULL ENERGY DATA VIA API
# ═══════════════════════════════════════════
echo ""
echo "=== EIA FULL ENERGY DATA ==="
mkdir -p /data/grid/bulk/eia/series
cd /data/grid/bulk/eia

EIA_KEY="QAz3bg00oRnsiRgFrBJy3k8xI36lklWW6q7CdNEg"

# Petroleum
for series in WCRFPUS2 WCESTUS1 WCRIMUS2 WCRSTUS1 WPULEUS3 WTTSTUS1 WGTSTUS1 WKJSTUS1 WDISTUS1 WBCSTUS1 WRESTUS1 WDIRPUS2 WDIRIM2; do
    url="https://api.eia.gov/v2/petroleum/sum/sndw/data/?api_key=${EIA_KEY}&frequency=weekly&data[0]=value&facets[series][]=${series}&sort[0][column]=period&sort[0][direction]=desc&length=5000"
    out="series/pet_${series}.json"
    if [ ! -f "$out" ]; then
        echo "  EIA petroleum: $series"
        curl -s "$url" > "$out"
        sleep 0.5
    fi
done

# Natural Gas
for series in RNGWHHD RNGC1 RNGC2 RNGC3 RNGC4; do
    url="https://api.eia.gov/v2/natural-gas/pri/sum/data/?api_key=${EIA_KEY}&frequency=monthly&data[0]=value&facets[series][]=${series}&sort[0][column]=period&sort[0][direction]=desc&length=5000"
    out="series/ng_${series}.json"
    if [ ! -f "$out" ]; then
        echo "  EIA nat gas: $series"
        curl -s "$url" > "$out"
        sleep 0.5
    fi
done

# Electricity generation by source
for series in ELEC.GEN.ALL-US-99.M ELEC.GEN.SUN-US-99.M ELEC.GEN.WND-US-99.M ELEC.GEN.NG-US-99.M ELEC.GEN.COL-US-99.M ELEC.GEN.NUC-US-99.M; do
    encoded=$(echo $series | sed 's/\./%2E/g')
    url="https://api.eia.gov/v2/electricity/electric-power-operational-data/data/?api_key=${EIA_KEY}&frequency=monthly&data[0]=generation&sort[0][column]=period&sort[0][direction]=desc&length=5000"
    out="series/elec_$(echo $series | tr '.' '_').json"
    if [ ! -f "$out" ]; then
        echo "  EIA electricity: $series"
        curl -s "$url" > "$out"
        sleep 0.5
    fi
done

# Crude oil prices - full history
url="https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key=${EIA_KEY}&frequency=daily&data[0]=value&facets[series][]=RWTC&sort[0][column]=period&sort[0][direction]=desc&length=10000"
echo "  EIA crude price history..."
curl -s "$url" > "series/crude_price_full.json"

# Weekly petroleum status report
url="https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key=${EIA_KEY}&frequency=weekly&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&length=5000"
echo "  EIA weekly petroleum status..."
curl -s "$url" > "series/weekly_petroleum_status.json"

echo "EIA done: $(du -sh /data/grid/bulk/eia/)"

# ═══════════════════════════════════════════
# 3. GDELT FULL ARCHIVE (bonus)
# ═══════════════════════════════════════════
echo ""
echo "=== GDELT EVENTS ==="
mkdir -p /data/grid/bulk/gdelt
cd /data/grid/bulk/gdelt

# Daily event files for last 2 years
for year in 2024 2025 2026; do
    for month in $(seq -w 1 12); do
        for day in $(seq -w 1 28); do
            url="http://data.gdeltproject.org/events/${year}${month}${day}.export.CSV.zip"
            out="${year}${month}${day}.zip"
            if [ ! -f "$out" ]; then
                wget -q "$url" -O "$out" 2>/dev/null
                if [ ! -s "$out" ]; then rm -f "$out"; fi
            fi
        done
    done
    echo "  GDELT $year downloaded"
done

echo "GDELT done: $(du -sh /data/grid/bulk/gdelt/)"

# ═══════════════════════════════════════════
echo ""
echo "=== FINAL STATUS ==="
du -sh /data/grid/bulk/*/
du -sh /data/grid/bulk/
echo "Finished: $(date)"
