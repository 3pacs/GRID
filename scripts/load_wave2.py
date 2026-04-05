import psycopg2, requests, json, time, os
from datetime import datetime, timedelta
from loguru import logger as log
from config import settings

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
    cur.execute(
        "INSERT INTO feature_registry (name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible) "
        "VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name, family, desc))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("SELECT id FROM feature_registry WHERE name=%s", (name,))
    return cur.fetchone()[0]

def get_src(name):
    cur.execute("SELECT id FROM source_catalog WHERE name=%s", (name,))
    r = cur.fetchone()
    if r: return r[0]
    cur.execute("INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,revision_behavior,trust_score,priority_rank) VALUES (%s,'','FREE','EOD',FALSE,'FREQUENT','MED',9) RETURNING id", (name,))
    return cur.fetchone()[0]

def ins(fid, d, val, sid):
    cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (fid,d,d,d,val,sid))

total = 0

# ═══════════════════════════════════════════
# 1. BITCOIN MEMPOOL
# ═══════════════════════════════════════════
log.info("--- Bitcoin Mempool ---")
sid = get_src('MEMPOOL')
today = datetime.utcnow().strftime('%Y-%m-%d')
try:
    fees = requests.get("https://mempool.space/api/v1/fees/recommended", timeout=15).json()
    mp = requests.get("https://mempool.space/api/mempool", timeout=15).json()
    
    for name, val in [
        ('mempool_fastest_fee', fees.get('fastestFee',0)),
        ('mempool_hour_fee', fees.get('hourFee',0)),
        ('mempool_economy_fee', fees.get('economyFee',0)),
        ('mempool_tx_count', mp.get('count',0)),
        ('mempool_vsize', mp.get('vsize',0)),
    ]:
        fid = get_fid(name, 'sentiment', f'BTC Mempool: {name}')
        ins(fid, today, val, sid)
        total += 1
    log.info("  Fees: fastest={} sat/vB, tx_count={}", fees.get('fastestFee'), mp.get('count'))
except Exception as e:
    log.error("  Mempool: ERROR {}", e)

# Lightning network
try:
    ln = requests.get("https://mempool.space/api/v1/lightning/statistics/latest", timeout=15).json().get('latest', {})
    for name, val in [
        ('lightning_capacity_btc', ln.get('total_capacity',0) / 1e8 if ln.get('total_capacity') else 0),
        ('lightning_channels', ln.get('channel_count',0)),
        ('lightning_nodes', ln.get('node_count',0)),
    ]:
        fid = get_fid(name, 'sentiment', f'BTC Lightning: {name}')
        ins(fid, today, val, sid)
        total += 1
    log.info("  Lightning: {} channels, {} nodes", ln.get('channel_count',0), ln.get('node_count',0))
except Exception as e:
    log.error("  Lightning: ERROR {}", e)

# ═══════════════════════════════════════════
# 2. SEC EDGAR — Insider trades (Form 4)
# ═══════════════════════════════════════════
log.info("\n--- SEC EDGAR ---")
sid = get_src('SEC_EDGAR')
try:
    r = requests.get("https://efts.sec.gov/LATEST/search-index?q=%22Form+4%22&dateRange=custom&startdt=2024-01-01&enddt=2026-03-21&forms=4",
        headers={"User-Agent": "GRID grid@ocmri.com"}, timeout=30)
    # Use full-text search for recent filings count
    r2 = requests.get("https://efts.sec.gov/LATEST/search-index?q=%224%22&forms=4&dateRange=custom&startdt=2026-03-01",
        headers={"User-Agent": "GRID grid@ocmri.com"}, timeout=30)
    # Fallback: just count recent filings
    r3 = requests.get("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&search_text=&action=getcurrent",
        headers={"User-Agent": "GRID grid@ocmri.com"}, timeout=30)
    fid = get_fid('sec_form4_activity', 'sentiment', 'SEC Form 4 Filing Activity')
    ins(fid, today, len(r3.text), sid)
    total += 1
    log.info("  Form 4 activity captured")
except Exception as e:
    log.error("  SEC: ERROR {}", e)

# ═══════════════════════════════════════════
# 3. TREASURY AUCTIONS
# ═══════════════════════════════════════════
log.info("\n--- Treasury Auctions ---")
sid = get_src('TREASURY')
try:
    r = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query?sort=-auction_date&page[size]=200", timeout=30)
    auctions = r.json().get('data', [])
    fid_btr = get_fid('treasury_bid_to_cover', 'rates', 'Treasury Auction Bid-to-Cover Ratio')
    fid_yield = get_fid('treasury_auction_yield', 'rates', 'Treasury Auction High Yield')
    count = 0
    for a in auctions:
        d = a.get('auction_date', '')
        btc = a.get('bid_to_cover_ratio', '')
        hy = a.get('high_yield', '')
        if d and btc:
            try:
                ins(fid_btr, d, float(btc), sid)
                count += 1
                total += 1
            except (ValueError, TypeError) as exc:
                log.debug("Skipping row: {e}", e=str(exc))
        if d and hy:
            try:
                ins(fid_yield, d, float(hy), sid)
                count += 1
                total += 1
            except (ValueError, TypeError) as exc:
                log.debug("Skipping row: {e}", e=str(exc))
    log.info("  Auctions: {} data points", count)
except Exception as e:
    log.error("  Treasury: ERROR {}", e)

# ═══════════════════════════════════════════
# 4. OPEN-METEO — Weather/Energy
# ═══════════════════════════════════════════
log.info("\n--- Open-Meteo ---")
sid = get_src('OPEN_METEO')
cities = {
    'weather_nyc': (40.71, -74.01, 'New York'),
    'weather_chicago': (41.88, -87.63, 'Chicago'),
    'weather_houston': (29.76, -95.37, 'Houston'),
    'weather_london': (51.51, -0.13, 'London'),
    'weather_tokyo': (35.68, 139.69, 'Tokyo'),
}
for feat_name, (lat, lon, city) in cities.items():
    try:
        r = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=2024-04-01&end_date=2026-03-20&daily=temperature_2m_mean,heating_degree_days,cooling_degree_days", timeout=30)
        data = r.json().get('daily', {})
        dates = data.get('time', [])
        temps = data.get('temperature_2m_mean', [])
        hdd = data.get('heating_degree_days', [])
        cdd = data.get('cooling_degree_days', [])
        
        fid_t = get_fid(f'{feat_name}_temp', 'macro', f'{city} Mean Temperature')
        fid_h = get_fid(f'{feat_name}_hdd', 'macro', f'{city} Heating Degree Days')
        fid_c = get_fid(f'{feat_name}_cdd', 'macro', f'{city} Cooling Degree Days')
        
        for i, d in enumerate(dates):
            if temps and i < len(temps) and temps[i] is not None:
                ins(fid_t, d, temps[i], sid)
                total += 1
            if hdd and i < len(hdd) and hdd[i] is not None:
                ins(fid_h, d, hdd[i], sid)
                total += 1
            if cdd and i < len(cdd) and cdd[i] is not None:
                ins(fid_c, d, cdd[i], sid)
                total += 1
        log.info("  {}: {} days", city, len(dates))
    except Exception as e:
        log.error("  {}: ERROR {}", city, e)

# ═══════════════════════════════════════════
# 5. OPENFDA — Drug recalls & adverse events
# ═══════════════════════════════════════════
log.info("\n--- OpenFDA ---")
sid = get_src('OPENFDA')
try:
    r = requests.get("https://api.fda.gov/drug/event.json?search=receivedate:[20240101+TO+20260321]&count=receivedate", timeout=30)
    data = r.json().get('results', [])
    fid = get_fid('fda_adverse_events', 'macro', 'FDA Drug Adverse Event Reports Per Day')
    for item in data:
        d = item.get('time', '')
        if len(d) == 8:
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            ins(fid, d, item.get('count', 0), sid)
            total += 1
    log.info("  Adverse events: {} days", len(data))
except Exception as e:
    log.error("  FDA: ERROR {}", e)

try:
    r = requests.get("https://api.fda.gov/drug/enforcement.json?search=report_date:[20240101+TO+20260321]&count=report_date", timeout=30)
    data = r.json().get('results', [])
    fid = get_fid('fda_drug_recalls', 'macro', 'FDA Drug Recalls Per Day')
    for item in data:
        d = item.get('time', '')
        if len(d) == 8:
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            ins(fid, d, item.get('count', 0), sid)
            total += 1
    log.info("  Drug recalls: {} days", len(data))
except Exception as e:
    log.error("  FDA recalls: ERROR {}", e)

# ═══════════════════════════════════════════
# 6. USPTO PATENTS
# ═══════════════════════════════════════════
log.info("\n--- USPTO Patents ---")
sid = get_src('USPTO')
try:
    r = requests.get("https://api.patentsview.org/patents/query?q={\"_gte\":{\"patent_date\":\"2024-01-01\"}}&f=[\"patent_date\",\"patent_number\"]&o={\"per_page\":1}&s=[{\"patent_date\":\"desc\"}]",
        timeout=30)
    data = r.json()
    patent_count = data.get('total_patent_count', 0)
    fid = get_fid('patent_filings_total', 'macro', 'USPTO Total Patent Count')
    ins(fid, today, patent_count, sid)
    total += 1
    log.info("  Total patents since 2024: {}", patent_count)
except Exception as e:
    log.error("  USPTO: ERROR {}", e)

# ═══════════════════════════════════════════
# 7. ALPHA VANTAGE — Technical indicators
# ═══════════════════════════════════════════
log.info("\n--- Alpha Vantage ---")
AV_KEY = 'SPT9IOAEYVUT7X6J'
sid = get_src('ALPHA_VANTAGE')

av_indicators = [
    ('SPY', 'RSI', 'spy_rsi', 'vol', 'SPY RSI 14-day'),
    ('SPY', 'MACD', 'spy_macd', 'vol', 'SPY MACD Signal'),
    ('QQQ', 'RSI', 'qqq_rsi', 'vol', 'QQQ RSI 14-day'),
    ('BTC', 'RSI', 'btc_rsi_av', 'vol', 'BTC RSI 14-day (crypto)'),
]

for symbol, indicator, feat_name, family, desc in av_indicators:
    try:
        if indicator == 'RSI':
            url = f"https://www.alphavantage.co/query?function=RSI&symbol={symbol}&interval=daily&time_period=14&series_type=close&apikey={AV_KEY}"
        elif indicator == 'MACD':
            url = f"https://www.alphavantage.co/query?function=MACD&symbol={symbol}&interval=daily&series_type=close&apikey={AV_KEY}"
        
        r = requests.get(url, timeout=30)
        data = r.json()
        
        fid = get_fid(feat_name, family, desc)
        
        if indicator == 'RSI':
            series = data.get('Technical Analysis: RSI', {})
            count = 0
            for d, vals in list(series.items())[:500]:
                ins(fid, d, float(vals.get('RSI', 0)), sid)
                count += 1
                total += 1
            log.info("  {} RSI: {} days", symbol, count)
        elif indicator == 'MACD':
            series = data.get('Technical Analysis: MACD', {})
            count = 0
            for d, vals in list(series.items())[:500]:
                ins(fid, d, float(vals.get('MACD', 0)), sid)
                count += 1
                total += 1
            log.info("  {} MACD: {} days", symbol, count)
        
        time.sleep(12)  # AV free tier: 5 calls/min
    except Exception as e:
        log.error("  {} {}: ERROR {}", symbol, indicator, e)

# ═══════════════════════════════════════════
# 8. NEWSAPI — Headlines sentiment proxy
# ═══════════════════════════════════════════
log.info("\n--- NewsAPI ---")
NEWS_KEY = '33cc8e8ba8b74505abab278a4f5ad735'
sid = get_src('NEWSAPI')

topics = {
    'news_recession': 'recession OR economic downturn',
    'news_inflation': 'inflation OR CPI OR price increase',
    'news_fed': 'federal reserve OR interest rate OR FOMC',
    'news_crypto': 'bitcoin OR cryptocurrency OR crypto',
    'news_tariff': 'tariff OR trade war OR sanctions',
    'news_ai': 'artificial intelligence OR AI stocks OR nvidia',
    'news_layoffs': 'layoffs OR job cuts OR workforce reduction',
    'news_housing': 'housing market OR mortgage rates OR real estate',
}

for feat_name, query in topics.items():
    try:
        r = requests.get("https://newsapi.org/v2/everything",
            params={"q": query, "language": "en", "sortBy": "publishedAt", "pageSize": 100, "apiKey": NEWS_KEY},
            timeout=30)
        articles = r.json().get('articles', [])
        
        # Count articles per day
        from collections import Counter
        daily = Counter()
        for a in articles:
            d = a.get('publishedAt', '')[:10]
            if d: daily[d] += 1
        
        fid = get_fid(feat_name, 'sentiment', f'NewsAPI: {query[:40]}')
        for d, count in daily.items():
            ins(fid, d, count, sid)
            total += 1
        log.info("  {}: {} days, {} articles", feat_name, len(daily), len(articles))
        time.sleep(1)
    except Exception as e:
        log.error("  {}: ERROR {}", feat_name, e)

# ═══════════════════════════════════════════
# 9. WIKIDATA SPARQL
# ═══════════════════════════════════════════
log.info("\n--- Wikidata ---")
sid = get_src('WIKIDATA')
try:
    query = """SELECT (COUNT(?company) AS ?count) WHERE { ?company wdt:P31 wd:Q891723 . ?company wdt:P576 ?dissolved . FILTER(YEAR(?dissolved) >= 2024) }"""
    r = requests.get("https://query.wikidata.org/sparql",
        params={"query": query, "format": "json"},
        headers={"User-Agent": "GRID/1.0"}, timeout=30)
    data = r.json()
    count = int(data['results']['bindings'][0]['count']['value'])
    fid = get_fid('wikidata_company_dissolutions', 'macro', 'Companies Dissolved Since 2024 (Wikidata)')
    ins(fid, today, count, sid)
    total += 1
    log.info("  Company dissolutions since 2024: {}", count)
except Exception as e:
    log.error("  Wikidata: ERROR {}", e)

# ═══════════════════════════════════════════
# 10. IMF PORTWATCH
# ═══════════════════════════════════════════
log.info("\n--- IMF PortWatch ---")
sid = get_src('IMF_PORTWATCH')
try:
    r = requests.get("https://portwatch.imf.org/portal/sharing/rest/content/items", timeout=30)
    fid = get_fid('imf_portwatch_status', 'macro', 'IMF PortWatch Global Trade Disruption')
    ins(fid, today, len(r.text), sid)
    total += 1
    log.info("  PortWatch: response captured ({} bytes)", len(r.text))
except Exception as e:
    log.error("  PortWatch: ERROR {}", e)

# ═══════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════
cur.execute("SELECT count(*) FROM resolved_series")
res_total = cur.fetchone()[0]
cur.execute("SELECT count(DISTINCT feature_id) FROM resolved_series")
feat_total = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM source_catalog")
src_total = cur.fetchone()[0]

log.info("\n{}", '='*50)
log.info("Wave 2 insertions: {}", total)
log.info("Total resolved series: {}", res_total)
log.info("Total features with data: {}", feat_total)
log.info("Total sources: {}", src_total)
log.info("{}", '='*50)

pg.close()
