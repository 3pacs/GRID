#!/usr/bin/env python3
"""Bridge Crucix DuckDB data into GRID PostgreSQL."""
import duckdb, psycopg2, json, os
from datetime import datetime
from config import settings

CRUCIX_DATA_DIR = os.environ.get("CRUCIX_DATA_DIR", os.path.expanduser("~/grid_v4/Crucix/data"))
CRUCIX_DB = os.path.join(CRUCIX_DATA_DIR, "crucix.db")
pg = psycopg2.connect(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    dbname=settings.DB_NAME,
    user=settings.DB_USER,
    password=settings.DB_PASSWORD,
)
pg.autocommit = True
cur = pg.cursor()

def get_fid(name, family, desc):
    cur.execute("INSERT INTO feature_registry (name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible) VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) ON CONFLICT (name) DO NOTHING RETURNING id", (name, family, desc))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("SELECT id FROM feature_registry WHERE name=%s", (name,))
    return cur.fetchone()[0]

def get_src(name):
    cur.execute("SELECT id FROM source_catalog WHERE name=%s", (name,))
    r = cur.fetchone()
    if r: return r[0]
    cur.execute("INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,revision_behavior,trust_score,priority_rank) VALUES (%s,'','FREE','REALTIME',FALSE,'FREQUENT','MED',5) RETURNING id", (name,))
    return cur.fetchone()[0]

def ins(fid, d, val, sid):
    cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (fid,d,d,d,val,sid))

total = 0

# Find Crucix DB
import os, glob
crucix_paths = glob.glob(os.path.join(CRUCIX_DATA_DIR, "*.db")) + glob.glob(os.path.join(os.path.dirname(CRUCIX_DATA_DIR), "*.db")) + glob.glob(os.path.join(CRUCIX_DATA_DIR, "*.duckdb"))
if not crucix_paths:
    # Check Crucix data structure
    if os.path.exists(CRUCIX_DATA_DIR):
        print(f"Crucix data dir exists: {os.listdir(CRUCIX_DATA_DIR)}")
    else:
        print("No Crucix data dir found, checking for JSON/memory files...")
        crucix_parent = os.path.dirname(CRUCIX_DATA_DIR)
        for f in glob.glob(os.path.join(crucix_parent, "**/*"), recursive=True):
            if f.endswith(('.json', '.db', '.sqlite', '.duckdb')):
                print(f"  Found: {f}")

# Try to read Crucix market data from its API
print("\n--- Crucix API Bridge ---")
sid = get_src('CRUCIX')
import requests
try:
    r = requests.get("http://localhost:3117/api/data", timeout=10)
    data = r.json()
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    # Markets
    markets = data.get('markets', {})
    for key, mkt in markets.items():
        if isinstance(mkt, dict):
            price = mkt.get('price') or mkt.get('last') or mkt.get('value')
            if price:
                name = f"crucix_{key.lower().replace(' ','_')}"
                fid = get_fid(name, 'sentiment', f'Crucix: {key}')
                try:
                    ins(fid, today, float(str(price).replace(',','')), sid)
                    total += 1
                except: pass
    print(f"  Markets: {len(markets)} captured")
    
    # Alerts
    alerts = data.get('alerts', [])
    if isinstance(alerts, list):
        fid = get_fid('crucix_alert_count', 'sentiment', 'Crucix Alert Count')
        ins(fid, today, len(alerts), sid)
        total += 1
        flash = sum(1 for a in alerts if isinstance(a, dict) and a.get('tier') == 'FLASH')
        fid2 = get_fid('crucix_flash_alerts', 'sentiment', 'Crucix FLASH Alert Count')
        ins(fid2, today, flash, sid)
        total += 1
        print(f"  Alerts: {len(alerts)} total, {flash} FLASH")
    
    # News count as sentiment
    news = data.get('news', [])
    if isinstance(news, list):
        fid = get_fid('crucix_news_count', 'sentiment', 'Crucix News Item Count')
        ins(fid, today, len(news), sid)
        total += 1
        print(f"  News: {len(news)} items")
    
    # Signals
    signals = data.get('signals', data.get('crossSourceSignals', []))
    if isinstance(signals, list):
        fid = get_fid('crucix_signal_count', 'sentiment', 'Crucix Cross-Source Signal Count')
        ins(fid, today, len(signals), sid)
        total += 1
        print(f"  Signals: {len(signals)}")

except Exception as e:
    print(f"  Crucix API: ERROR {e}")

# Add Crucix dashboard link to GRID
print(f"\nTotal bridged: {total}")
cur.execute("SELECT count(*) FROM resolved_series")
print(f"Total resolved: {cur.fetchone()[0]}")
pg.close()
