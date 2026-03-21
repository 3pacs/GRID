#!/usr/bin/env python3
"""Bridge Crucix DuckDB data into GRID PostgreSQL."""
import duckdb, psycopg2, json
from datetime import datetime

CRUCIX_DB = "/home/grid/grid_v4/Crucix/data/crucix.db"
pg = psycopg2.connect(dbname='griddb', user='grid', password='grid2026')
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
crucix_paths = glob.glob("/home/grid/grid_v4/Crucix/data/*.db") + glob.glob("/home/grid/grid_v4/Crucix/*.db") + glob.glob("/home/grid/grid_v4/Crucix/data/*.duckdb")
if not crucix_paths:
    # Check Crucix data structure
    crucix_data = "/home/grid/grid_v4/Crucix/data"
    if os.path.exists(crucix_data):
        print(f"Crucix data dir exists: {os.listdir(crucix_data)}")
    else:
        print("No Crucix data dir found, checking for JSON/memory files...")
        for f in glob.glob("/home/grid/grid_v4/Crucix/**/*", recursive=True):
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
