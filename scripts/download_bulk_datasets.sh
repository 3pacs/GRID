#!/usr/bin/env bash
# =============================================================================
# GRID Bulk Dataset Downloader
# Corrected URLs for failed downloads — 2026-03-29
# Run on server: bash /path/to/download_bulk_datasets.sh
# =============================================================================

set -euo pipefail

DATA_DIR="/data/datasets"
mkdir -p "$DATA_DIR"

echo "=============================================="
echo "GRID Bulk Dataset Downloader"
echo "Target: $DATA_DIR"
echo "Date: $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "=============================================="

# -----------------------------------------------------------------------------
# 1. GLEIF LEI Data (Level 1 + Level 2 Relationship Records)
#    ~2GB+ total. Golden Copy files via API v2.
#    Uses -L to follow redirects (GLEIF API redirects to storage).
# -----------------------------------------------------------------------------
echo ""
echo "[1/6] GLEIF LEI Golden Copy Data"
GLEIF_DIR="$DATA_DIR/gleif"
mkdir -p "$GLEIF_DIR"

# Level 1: All LEI records (~2.9M entities, ~435MB compressed)
echo "  Downloading Level 1 LEI records (CSV)..."
curl -L -o "$GLEIF_DIR/gleif-lei2-golden-copy-latest.csv.zip" \
  "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv"

# Level 2: Relationship Records (who-owns-whom, ~590K records, ~31MB)
echo "  Downloading Level 2 Relationship Records (CSV)..."
curl -L -o "$GLEIF_DIR/gleif-rr-golden-copy-latest.csv.zip" \
  "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/rr/latest.csv"

# Level 2: Reporting Exceptions
echo "  Downloading Level 2 Reporting Exceptions (CSV)..."
curl -L -o "$GLEIF_DIR/gleif-repex-golden-copy-latest.csv.zip" \
  "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/repex/latest.csv"

echo "  GLEIF files:"
ls -lh "$GLEIF_DIR/"

# -----------------------------------------------------------------------------
# 2. PatentsView Bulk TSV Tables
#    NOTE: As of March 20, 2026 PatentsView is migrating to data.uspto.gov
#    The S3 URLs below are the known working pattern. If they 404, check:
#      https://data.uspto.gov  (new Open Data Portal)
#      https://patentsview.org/download/data-download-tables
# -----------------------------------------------------------------------------
echo ""
echo "[2/6] PatentsView Bulk TSV Data"
PV_DIR="$DATA_DIR/patentsview"
mkdir -p "$PV_DIR"

PV_BASE="https://s3.amazonaws.com/data.patentsview.org/download"

# Core tables — adjust filenames if they've changed to g_ prefix
PV_FILES=(
  "g_patent.tsv.zip"
  "g_assignee_disambiguated.tsv.zip"
  "g_inventor_disambiguated.tsv.zip"
  "g_location_disambiguated.tsv.zip"
  "g_application.tsv.zip"
  "g_cpc_current.tsv.zip"
  "g_uspc.tsv.zip"
  "g_wipo.tsv.zip"
  "g_us_term_of_grant.tsv.zip"
  "g_persistent_assignee.tsv.zip"
  "g_persistent_inventor.tsv.zip"
)

for f in "${PV_FILES[@]}"; do
  echo "  Downloading $f ..."
  curl -L -f -o "$PV_DIR/$f" "$PV_BASE/$f" || {
    echo "  WARN: Failed $PV_BASE/$f"
    echo "  Trying legacy name: ${f/g_/}"
    curl -L -f -o "$PV_DIR/${f/g_/}" "$PV_BASE/${f/g_/}" || echo "  SKIP: Both URLs failed for $f — check data.uspto.gov"
  }
done

echo "  PatentsView files:"
ls -lh "$PV_DIR/"

# -----------------------------------------------------------------------------
# 3. Binance Historical Klines
#    Pattern: https://data.binance.vision/data/spot/monthly/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{YYYY}-{MM}.zip
# -----------------------------------------------------------------------------
echo ""
echo "[3/6] Binance Historical Klines"
BN_DIR="$DATA_DIR/binance_klines"
mkdir -p "$BN_DIR"

BN_BASE="https://data.binance.vision/data/spot/monthly/klines"

# Key pairs to download
BN_SYMBOLS=("BTCUSDT" "ETHUSDT" "SOLUSDT" "BNBUSDT" "XRPUSDT")
BN_INTERVAL="1d"

# Download last 24 months
for SYMBOL in "${BN_SYMBOLS[@]}"; do
  SYM_DIR="$BN_DIR/$SYMBOL"
  mkdir -p "$SYM_DIR"
  for YEAR in 2024 2025 2026; do
    for MONTH in $(seq -w 1 12); do
      # Skip future months
      if [[ "$YEAR" == "2026" && "$MONTH" -gt "02" ]]; then
        continue
      fi
      FILE="${SYMBOL}-${BN_INTERVAL}-${YEAR}-${MONTH}.zip"
      URL="${BN_BASE}/${SYMBOL}/${BN_INTERVAL}/${FILE}"
      if [ ! -f "$SYM_DIR/$FILE" ]; then
        echo "  Downloading $FILE ..."
        curl -sL -f -o "$SYM_DIR/$FILE" "$URL" 2>/dev/null || true
      fi
    done
  done
done

echo "  Binance klines files:"
du -sh "$BN_DIR/"

# -----------------------------------------------------------------------------
# 4. UK Companies House PSC (Persons with Significant Control)
#    Snapshot URL pattern: http://download.companieshouse.gov.uk/persons-with-significant-control-snapshot-YYYY-MM-DD.zip
#    Updated daily, ~2GB
# -----------------------------------------------------------------------------
echo ""
echo "[4/6] UK Companies House PSC Snapshot"
PSC_DIR="$DATA_DIR/uk_psc"
mkdir -p "$PSC_DIR"

# Try today's date, then work backwards up to 7 days
PSC_DOWNLOADED=false
for DAYS_AGO in 0 1 2 3 4 5 6 7; do
  if [ "$(uname)" = "Darwin" ]; then
    PSC_DATE=$(date -v-${DAYS_AGO}d '+%Y-%m-%d')
  else
    PSC_DATE=$(date -d "$DAYS_AGO days ago" '+%Y-%m-%d')
  fi
  PSC_URL="http://download.companieshouse.gov.uk/persons-with-significant-control-snapshot-${PSC_DATE}.zip"
  echo "  Trying $PSC_DATE ..."
  HTTP_CODE=$(curl -sL -o /dev/null -w "%{http_code}" "$PSC_URL")
  if [ "$HTTP_CODE" = "200" ]; then
    echo "  Found snapshot for $PSC_DATE — downloading (~2GB)..."
    curl -L -o "$PSC_DIR/psc-snapshot-${PSC_DATE}.zip" "$PSC_URL"
    PSC_DOWNLOADED=true
    break
  fi
done

if [ "$PSC_DOWNLOADED" = false ]; then
  echo "  WARN: Could not find a recent PSC snapshot. Check https://download.companieshouse.gov.uk/en_pscdata.html"
fi

echo "  PSC files:"
ls -lh "$PSC_DIR/" 2>/dev/null || echo "  (none)"

# -----------------------------------------------------------------------------
# 5. Layline Insider Trading Dataset (Harvard Dataverse)
#    DOI: 10.7910/DVN/VH6GVH
#    Uses Dataverse API to download all files as a zip bundle
#    Also available on Kaggle: https://www.kaggle.com/datasets/layline/insidertrading
# -----------------------------------------------------------------------------
echo ""
echo "[5/6] Layline Insider Trading Dataset"
LAYLINE_DIR="$DATA_DIR/layline_insider"
mkdir -p "$LAYLINE_DIR"

# Method 1: Harvard Dataverse API — download entire dataset as zip
echo "  Downloading from Harvard Dataverse (DOI: 10.7910/DVN/VH6GVH)..."
curl -L -o "$LAYLINE_DIR/layline-insider-trading.zip" \
  "https://dataverse.harvard.edu/api/access/dataset/:persistentId/?persistentId=doi:10.7910/DVN/VH6GVH"

FILE_SIZE=$(stat -f%z "$LAYLINE_DIR/layline-insider-trading.zip" 2>/dev/null || stat -c%s "$LAYLINE_DIR/layline-insider-trading.zip" 2>/dev/null || echo 0)
if [ "$FILE_SIZE" -lt 10000 ]; then
  echo "  WARN: Dataverse download may have failed (${FILE_SIZE} bytes)."
  echo "  Alternative: Download from Kaggle manually:"
  echo "    kaggle datasets download -d layline/insidertrading -p $LAYLINE_DIR/"
  echo "  Or use wget with recursive Dataverse dirindex:"
  echo "    wget -r -e robots=off -nH --cut-dirs=3 --content-disposition \\"
  echo "      'https://dataverse.harvard.edu/api/datasets/:persistentId/dirindex?persistentId=doi:10.7910/DVN/VH6GVH' \\"
  echo "      -P $LAYLINE_DIR/"
fi

echo "  Layline files:"
ls -lh "$LAYLINE_DIR/"

# -----------------------------------------------------------------------------
# 6. GDELT Event Files (recent months)
#    The masterfilelist.txt at http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
#    contains all V2 file URLs. Lines with ".export.CSV.zip" are event files.
#    Each daily file is ~5-10MB compressed.
# -----------------------------------------------------------------------------
echo ""
echo "[6/6] GDELT Event Files (Recent 3 Months)"
GDELT_DIR="$DATA_DIR/gdelt_bulk"
mkdir -p "$GDELT_DIR/events"

# Download/update the master file list (GDELT v2)
echo "  Downloading GDELT v2 master file list..."
curl -sL -o "$GDELT_DIR/masterfilelist-v2.txt" \
  "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

echo "  Master file list size: $(wc -c < "$GDELT_DIR/masterfilelist-v2.txt") bytes"

# Also get GDELT v1 master file list (simpler daily event files)
echo "  Downloading GDELT v1 master file list..."
curl -sL -o "$GDELT_DIR/masterfilelist-v1.txt" \
  "http://data.gdeltproject.org/events/masterfilelist.txt"

# Parse and download recent event files (last 90 days of GDELT v2 export files)
echo "  Downloading recent GDELT v2 event exports..."

# Generate date prefixes for last 90 days
GDELT_DATES=()
for DAYS_AGO in $(seq 0 90); do
  if [ "$(uname)" = "Darwin" ]; then
    GDELT_DATES+=("$(date -v-${DAYS_AGO}d '+%Y%m%d')")
  else
    GDELT_DATES+=("$(date -d "$DAYS_AGO days ago" '+%Y%m%d')")
  fi
done

# Filter masterfilelist for export files matching our date range, download them
DOWNLOAD_COUNT=0
for DATE_PREFIX in "${GDELT_DATES[@]}"; do
  # GDELT v2 event files have pattern: YYYYMMDDHHMMSS.export.CSV.zip
  # Get one file per day (the first 15-minute slice)
  MATCH=$(grep "${DATE_PREFIX}.*\.export\.CSV\.zip" "$GDELT_DIR/masterfilelist-v2.txt" | head -1 | awk '{print $3}' || true)
  if [ -n "$MATCH" ]; then
    FILENAME=$(basename "$MATCH")
    if [ ! -f "$GDELT_DIR/events/$FILENAME" ]; then
      echo "  Downloading $FILENAME ..."
      curl -sL -f -o "$GDELT_DIR/events/$FILENAME" "$MATCH" || true
      DOWNLOAD_COUNT=$((DOWNLOAD_COUNT + 1))
    fi
  fi
done

echo "  Downloaded $DOWNLOAD_COUNT new GDELT event files"
echo "  GDELT events directory:"
du -sh "$GDELT_DIR/events/"

# =============================================================================
echo ""
echo "=============================================="
echo "Download complete. Summary:"
echo "=============================================="
du -sh "$DATA_DIR/gleif/" 2>/dev/null || true
du -sh "$DATA_DIR/patentsview/" 2>/dev/null || true
du -sh "$DATA_DIR/binance_klines/" 2>/dev/null || true
du -sh "$DATA_DIR/uk_psc/" 2>/dev/null || true
du -sh "$DATA_DIR/layline_insider/" 2>/dev/null || true
du -sh "$DATA_DIR/gdelt_bulk/" 2>/dev/null || true
echo ""
echo "Verify no tiny error files:"
find "$DATA_DIR" -name "*.zip" -size -1k -exec echo "  SUSPECT: {} ($(stat -f%z {} 2>/dev/null || stat -c%s {} 2>/dev/null) bytes)" \;
echo "=============================================="
