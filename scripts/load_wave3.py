import psycopg2, requests, json, time
from datetime import datetime

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
    cur.execute("INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,revision_behavior,trust_score,priority_rank) VALUES (%s,'','FREE','EOD',FALSE,'RARE','HIGH',8) RETURNING id", (name,))
    return cur.fetchone()[0]

def ins(fid, d, val, sid):
    cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (fid,d,d,d,val,sid))

total = 0
EIA_KEY = 'QAz3bg00oRnsiRgFrBJy3k8xI36lklWW6q7CdNEg'
NOAA_KEY = 'TAbZzkQbuOqhjvwZNsrNVLDYZyLiWCLH'

# ═══════════════════════════════════════════
# 1. EIA — Energy data
# ═══════════════════════════════════════════
print("--- EIA Energy ---")
sid = get_src('EIA')

eia_series = {
    'eia_crude_stocks': ('PET.WCESTUS1.W', 'commodity', 'US Crude Oil Stocks Weekly'),
    'eia_gas_stocks': ('NG.NW2_EPG0_SWO_R48_BCF.W', 'commodity', 'US Natural Gas Storage Weekly'),
    'eia_gas_price': ('PET.EMM_EPMR_PTE_NUS_DPG.W', 'commodity', 'US Regular Gas Price Weekly'),
    'eia_diesel_price': ('PET.EMD_EPD2D_PTE_NUS_DPG.W', 'commodity', 'US Diesel Price Weekly'),
    'eia_crude_production': ('PET.WCRFPUS2.W', 'commodity', 'US Crude Production Weekly'),
    'eia_crude_imports': ('PET.WCRIMUS2.W', 'commodity', 'US Crude Imports Weekly'),
    'eia_refinery_util': ('PET.WPULEUS3.W', 'commodity', 'US Refinery Utilization Weekly'),
    'eia_electricity_demand': ('ELEC.GEN.ALL-US-99.M', 'macro', 'US Electricity Generation Monthly'),
}

for feat_name, (series_id, family, desc) in eia_series.items():
    fid = get_fid(feat_name, family, desc)
    try:
        r = requests.get(f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={EIA_KEY}&frequency=weekly&start=2024-01-01&sort[0][column]=period&sort[0][direction]=desc&length=500", timeout=30)
        data = r.json()
        rows = data.get('response', {}).get('data', [])
        count = 0
        for row in rows:
            d = row.get('period', '')
            v = row.get('value')
            if d and v is not None:
                try:
                    ins(fid, d, float(v), sid)
                    count += 1
                    total += 1
                except: pass
        print(f"  {feat_name}: {count} rows")
    except Exception as e:
        print(f"  {feat_name}: ERROR {e}")
    time.sleep(0.5)

# ═══════════════════════════════════════════
# 2. NOAA — Heating/Cooling degree days
# ═══════════════════════════════════════════
print("\n--- NOAA Climate ---")
noaa_sid = get_src('NOAA')

# National HDD/CDD from NOAA
try:
    r = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=NORMAL_ANN&datatypeid=ANN-HTDD-NORMAL&locationid=FIPS:06&startdate=2024-01-01&enddate=2026-03-20&limit=1000",
        headers={"token": NOAA_KEY}, timeout=30)
    data = r.json().get('results', [])
    if data:
        fid = get_fid('noaa_hdd_ca', 'macro', 'CA Heating Degree Days Annual Normal')
        for row in data:
            d = row.get('date', '')[:10]
            v = row.get('value')
            if d and v: ins(fid, d, float(v), noaa_sid); total += 1
        print(f"  CA HDD normals: {len(data)} rows")
    else:
        print("  CA HDD: no data returned")
except Exception as e:
    print(f"  NOAA HDD: ERROR {e}")

# NOAA recent daily temps for major stations
stations = {
    'noaa_nyc_temp': ('GHCND:USW00094728', 'New York Central Park'),
    'noaa_chicago_temp': ('GHCND:USW00094846', 'Chicago OHare'),
    'noaa_houston_temp': ('GHCND:USW00012960', 'Houston Intercontinental'),
}
for feat_name, (station, desc) in stations.items():
    try:
        r = requests.get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=TAVG&stationid={station}&startdate=2025-01-01&enddate=2026-03-20&limit=1000&units=standard",
            headers={"token": NOAA_KEY}, timeout=30)
        data = r.json().get('results', [])
        fid = get_fid(feat_name, 'macro', f'{desc} Daily Temp')
        for row in data:
            d = row.get('date', '')[:10]
            v = row.get('value')
            if d and v: ins(fid, d, float(v), noaa_sid); total += 1
        print(f"  {desc}: {len(data)} rows")
    except Exception as e:
        print(f"  {desc}: ERROR {e}")
    time.sleep(0.5)

# ═══════════════════════════════════════════
# 3. OPEN-METEO FIX — Temperature only (HDD/CDD not in archive)
# ═══════════════════════════════════════════
print("\n--- Open-Meteo (fixed) ---")
om_sid = get_src('OPEN_METEO')
cities = {
    'weather_nyc': (40.71, -74.01, 'New York'),
    'weather_chicago': (41.88, -87.63, 'Chicago'),
    'weather_houston': (29.76, -95.37, 'Houston'),
    'weather_london': (51.51, -0.13, 'London'),
    'weather_tokyo': (35.68, 139.69, 'Tokyo'),
}
for feat_name, (lat, lon, city) in cities.items():
    try:
        r = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=2024-04-01&end_date=2026-03-20&daily=temperature_2m_mean", timeout=30)
        data = r.json().get('daily', {})
        dates = data.get('time', [])
        temps = data.get('temperature_2m_mean', [])
        fid = get_fid(f'{feat_name}_temp', 'macro', f'{city} Mean Temperature C')
        count = 0
        for i, d in enumerate(dates):
            if i < len(temps) and temps[i] is not None:
                ins(fid, d, temps[i], om_sid)
                count += 1
                total += 1
        print(f"  {city}: {count} days")
    except Exception as e:
        print(f"  {city}: ERROR {e}")

# ═══════════════════════════════════════════
# 4. DBnomics — BIS credit gap, IMF data
# ═══════════════════════════════════════════
print("\n--- DBnomics ---")
dbn_sid = get_src('DBNOMICS')
try:
    # BIS credit-to-GDP gap — US
    r = requests.get("https://api.db.nomics.world/v22/series/BIS/credit_gap/Q.US.P.A.M.XDC.A?observations=1", timeout=30)
    data = r.json()
    series = data.get('series', {}).get('docs', [{}])[0]
    periods = series.get('period', [])
    values = series.get('value', [])
    fid = get_fid('bis_credit_gap_us', 'credit', 'BIS US Credit-to-GDP Gap')
    count = 0
    for p, v in zip(periods, values):
        if v is not None and v != 'NA':
            ins(fid, p + '-01' if len(p) == 7 else p, float(v), dbn_sid)
            count += 1
            total += 1
    print(f"  BIS US credit gap: {count} quarters")
except Exception as e:
    print(f"  BIS credit gap: ERROR {e}")

try:
    # IMF World Economic Outlook — US GDP growth
    r = requests.get("https://api.db.nomics.world/v22/series/IMF/WEO:2024-10/USA.NGDP_RPCH?observations=1", timeout=30)
    data = r.json()
    series = data.get('series', {}).get('docs', [{}])[0]
    periods = series.get('period', [])
    values = series.get('value', [])
    fid = get_fid('imf_us_gdp_growth', 'macro', 'IMF WEO US GDP Growth Forecast')
    count = 0
    for p, v in zip(periods, values):
        if v is not None and v != 'NA':
            ins(fid, p + '-01-01' if len(p) == 4 else p, float(v), dbn_sid)
            count += 1
            total += 1
    print(f"  IMF US GDP growth: {count} years")
except Exception as e:
    print(f"  IMF GDP: ERROR {e}")

# ═══════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════
cur.execute("SELECT count(*) FROM resolved_series")
res_total = cur.fetchone()[0]
cur.execute("SELECT count(DISTINCT feature_id) FROM resolved_series")
feat_total = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM source_catalog")
src_total = cur.fetchone()[0]

print(f"\n{'='*50}")
print(f"Wave 3 insertions: {total}")
print(f"Total resolved series: {res_total}")
print(f"Total features with data: {feat_total}")
print(f"Total sources: {src_total}")
print(f"{'='*50}")

pg.close()
