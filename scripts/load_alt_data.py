import psycopg2
import requests
import json
import time
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

# Add alt data sources
for name, url, tier in [
    ('ALTERNATIVE_ME', 'https://api.alternative.me', 'FREE'),
    ('COINGECKO', 'https://api.coingecko.com', 'FREE'),
    ('DEFILLAMA', 'https://api.llama.fi', 'FREE'),
    ('GDELT', 'https://api.gdeltproject.org', 'FREE'),
    ('POLYMARKET', 'https://gamma-api.polymarket.com', 'FREE'),
    ('REDDIT', 'https://www.reddit.com', 'FREE'),
    ('WIKIPEDIA', 'https://wikimedia.org/api', 'FREE'),
    ('CONGRESS', 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com', 'FREE'),
    ('WHALE_ALERT', 'https://api.whale-alert.io', 'FREE'),
    ('SANTIMENT', 'https://api.santiment.net', 'FREE'),
]:
    cur.execute(
        "INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,revision_behavior,trust_score,priority_rank) "
        "VALUES (%s,%s,%s,'EOD',FALSE,'FREQUENT','MED',8) ON CONFLICT (name) DO NOTHING", (name, url, tier))

log.info("=== Sources registered ===")

# Helper to register feature and get ID
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
    return cur.fetchone()[0]

def insert_obs(fid, obs_date, value, src_id):
    cur.execute(
        "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) "
        "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (fid, obs_date, obs_date, obs_date, value, src_id))

total = 0

# ═══════════════════════════════════════════
# 1. CRYPTO FEAR & GREED INDEX (Alternative.me)
# ═══════════════════════════════════════════
log.info("\n--- Crypto Fear & Greed ---")
fid = get_fid('crypto_fear_greed', 'sentiment', 'Crypto Fear & Greed Index (0-100)')
sid = get_src('ALTERNATIVE_ME')
try:
    r = requests.get("https://api.alternative.me/fng/?limit=730&format=json", timeout=30)
    data = r.json().get('data', [])
    for d in data:
        ts = datetime.fromtimestamp(int(d['timestamp'])).strftime('%Y-%m-%d')
        insert_obs(fid, ts, float(d['value']), sid)
        total += 1
    log.info("  Fear & Greed: {} days", len(data))
except Exception as e:
    log.error("  Fear & Greed: ERROR {}", e)

# ═══════════════════════════════════════════
# 2. COINGECKO — Global market data
# ═══════════════════════════════════════════
log.info("\n--- CoinGecko Global ---")
try:
    r = requests.get("https://api.coingecko.com/api/v3/global", timeout=30)
    data = r.json().get('data', {})
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    metrics = {
        'crypto_total_mcap': ('sentiment', 'Total Crypto Market Cap', data.get('total_market_cap', {}).get('usd', 0)),
        'crypto_total_volume': ('sentiment', 'Total Crypto 24h Volume', data.get('total_volume', {}).get('usd', 0)),
        'btc_dominance': ('sentiment', 'Bitcoin Dominance %', data.get('market_cap_percentage', {}).get('btc', 0)),
        'eth_dominance': ('sentiment', 'Ethereum Dominance %', data.get('market_cap_percentage', {}).get('eth', 0)),
        'active_cryptos': ('sentiment', 'Active Cryptocurrencies Count', data.get('active_cryptocurrencies', 0)),
    }
    for name, (fam, desc, val) in metrics.items():
        fid = get_fid(name, fam, desc)
        insert_obs(fid, today, val, get_src('COINGECKO'))
        total += 1
    log.info("  Global metrics: {} captured", len(metrics))
except Exception as e:
    log.error("  CoinGecko global: ERROR {}", e)

# CoinGecko — BTC on-chain proxies via market chart
log.info("\n--- CoinGecko BTC History ---")
try:
    r = requests.get("https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=730&interval=daily", timeout=30)
    data = r.json()
    
    fid_mc = get_fid('btc_market_cap', 'sentiment', 'BTC Market Cap USD')
    fid_vol = get_fid('btc_total_volume', 'sentiment', 'BTC Total Volume USD')
    cg_id = get_src('COINGECKO')
    
    for ts, val in data.get('market_caps', []):
        d = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d')
        insert_obs(fid_mc, d, val, cg_id)
        total += 1
    for ts, val in data.get('total_volumes', []):
        d = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d')
        insert_obs(fid_vol, d, val, cg_id)
        total += 1
    log.info("  BTC mcap: {} days", len(data.get('market_caps',[])))
    log.info("  BTC volume: {} days", len(data.get('total_volumes',[])))
except Exception as e:
    log.error("  CoinGecko BTC: ERROR {}", e)

time.sleep(1)  # Rate limit

# CoinGecko — ETH, SOL, TAO histories
for coin, slug in [('eth', 'ethereum'), ('sol', 'solana'), ('tao_chain', 'bittensor')]:
    log.info("\n--- CoinGecko {} ---", coin)
    try:
        r = requests.get(f"https://api.coingecko.com/api/v3/coins/{slug}/market_chart?vs_currency=usd&days=730&interval=daily", timeout=30)
        data = r.json()
        fid_mc = get_fid(f'{coin}_market_cap', 'sentiment', f'{coin.upper()} Market Cap')
        fid_vol = get_fid(f'{coin}_total_volume', 'sentiment', f'{coin.upper()} Total Volume')
        cg_id = get_src('COINGECKO')
        for ts, val in data.get('market_caps', []):
            d = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d')
            insert_obs(fid_mc, d, val, cg_id)
            total += 1
        for ts, val in data.get('total_volumes', []):
            d = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d')
            insert_obs(fid_vol, d, val, cg_id)
            total += 1
        log.info("  {} mcap+vol loaded", coin)
    except Exception as e:
        log.error("  {}: ERROR {}", coin, e)
    time.sleep(1)

# ═══════════════════════════════════════════
# 3. DEFILLAMA — DeFi TVL
# ═══════════════════════════════════════════
log.info("\n--- DefiLlama TVL ---")
try:
    r = requests.get("https://api.llama.fi/v2/historicalChainTvl", timeout=30)
    data = r.json()
    fid = get_fid('defi_total_tvl', 'sentiment', 'Total DeFi TVL USD')
    dl_id = get_src('DEFILLAMA')
    for point in data:
        d = datetime.fromtimestamp(point['date']).strftime('%Y-%m-%d')
        insert_obs(fid, d, point['tvl'], dl_id)
        total += 1
    log.info("  Total TVL: {} days", len(data))
except Exception as e:
    log.error("  DefiLlama TVL: ERROR {}", e)

# DefiLlama — chain TVLs
for chain in ['Ethereum', 'Solana', 'Arbitrum', 'Base', 'BSC']:
    try:
        r = requests.get(f"https://api.llama.fi/v2/historicalChainTvl/{chain}", timeout=30)
        data = r.json()
        fname = f'tvl_{chain.lower()}'
        fid = get_fid(fname, 'sentiment', f'{chain} Chain TVL')
        dl_id = get_src('DEFILLAMA')
        for point in data:
            d = datetime.fromtimestamp(point['date']).strftime('%Y-%m-%d')
            insert_obs(fid, d, point['tvl'], dl_id)
            total += 1
        log.info("  {} TVL: {} days", chain, len(data))
    except Exception as e:
        log.error("  {} TVL: ERROR {}", chain, e)
    time.sleep(0.5)

# DefiLlama — stablecoin supply
log.info("\n--- DefiLlama Stablecoins ---")
try:
    r = requests.get("https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=1", timeout=30)
    data = r.json()
    fid = get_fid('usdt_supply', 'sentiment', 'USDT Total Supply')
    dl_id = get_src('DEFILLAMA')
    for point in data:
        d = datetime.fromtimestamp(point['date']).strftime('%Y-%m-%d')
        supply = point.get('totalCirculating', {}).get('peggedUSD', 0)
        if supply:
            insert_obs(fid, d, supply, dl_id)
            total += 1
    log.info("  USDT supply: {} days", len(data))
except Exception as e:
    log.error("  Stablecoins: ERROR {}", e)

try:
    r = requests.get("https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=2", timeout=30)
    data = r.json()
    fid = get_fid('usdc_supply', 'sentiment', 'USDC Total Supply')
    dl_id = get_src('DEFILLAMA')
    for point in data:
        d = datetime.fromtimestamp(point['date']).strftime('%Y-%m-%d')
        supply = point.get('totalCirculating', {}).get('peggedUSD', 0)
        if supply:
            insert_obs(fid, d, supply, dl_id)
            total += 1
    log.info("  USDC supply: {} days", len(data))
except Exception as e:
    log.error("  USDC: ERROR {}", e)

# ═══════════════════════════════════════════
# 4. WIKIPEDIA PAGEVIEWS — Social attention proxy
# ═══════════════════════════════════════════
log.info("\n--- Wikipedia Pageviews ---")
wiki_topics = {
    'wiki_bitcoin': 'Bitcoin',
    'wiki_ethereum': 'Ethereum',
    'wiki_solana': 'Solana',
    'wiki_sp500': 'S%26P_500',
    'wiki_recession': 'Recession',
    'wiki_inflation': 'Inflation',
    'wiki_fed_reserve': 'Federal_Reserve',
    'wiki_stock_market': 'Stock_market_crash',
    'wiki_tariff': 'Tariff',
    'wiki_ai': 'Artificial_intelligence',
    'wiki_nvidia': 'Nvidia',
    'wiki_gold': 'Gold',
    'wiki_treasury': 'United_States_Treasury_security',
}

wiki_src = get_src('WIKIPEDIA')
end = datetime.utcnow()
start = end - timedelta(days=730)
start_str = start.strftime('%Y%m%d')
end_str = end.strftime('%Y%m%d')

for feat_name, article in wiki_topics.items():
    fid = get_fid(feat_name, 'sentiment', f'Wikipedia pageviews: {article}')
    try:
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{article}/daily/{start_str}/{end_str}"
        r = requests.get(url, headers={"User-Agent": "GRID/1.0"}, timeout=30)
        data = r.json().get('items', [])
        for item in data:
            d = item['timestamp'][:8]
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            insert_obs(fid, d, item['views'], wiki_src)
            total += 1
        log.info("  {}: {} days", article, len(data))
    except Exception as e:
        log.error("  {}: ERROR {}", article, e)
    time.sleep(0.2)

# ═══════════════════════════════════════════
# 5. POLYMARKET — Prediction markets
# ═══════════════════════════════════════════
log.info("\n--- Polymarket ---")
poly_src = get_src('POLYMARKET')
try:
    r = requests.get("https://gamma-api.polymarket.com/markets?limit=20&active=true&order=volume&ascending=false", timeout=30)
    markets = r.json()
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    # Store top market volumes as a sentiment indicator
    fid_vol = get_fid('polymarket_top_volume', 'sentiment', 'Polymarket Top 20 Markets Total Volume')
    total_vol = sum(float(m.get('volume', 0) or 0) for m in markets)
    insert_obs(fid_vol, today, total_vol, poly_src)
    total += 1
    
    # Look for specific prediction markets
    for m in markets:
        title = m.get('question', '').lower()
        if 'recession' in title:
            fid_r = get_fid('polymarket_recession', 'sentiment', 'Polymarket Recession Probability')
            price = float(m.get('outcomePrices', '[0.5]').strip('[]').split(',')[0])
            insert_obs(fid_r, today, price, poly_src)
            total += 1
            log.info("  Recession market: {:.0%}", price)
        if 'bitcoin' in title and ('100k' in title or '150k' in title or 'price' in title):
            fid_b = get_fid('polymarket_btc', 'sentiment', 'Polymarket BTC Price Prediction')
            price = float(m.get('outcomePrices', '[0.5]').strip('[]').split(',')[0])
            insert_obs(fid_b, today, price, poly_src)
            total += 1
            log.info("  BTC prediction: {:.0%}", price)
    log.info("  Top volume: ${:,.0f}", total_vol)
except Exception as e:
    log.error("  Polymarket: ERROR {}", e)

# ═══════════════════════════════════════════
# 6. GDELT — Global event tone
# ═══════════════════════════════════════════
log.info("\n--- GDELT Events ---")
gdelt_src = get_src('GDELT')
try:
    r = requests.get("https://api.gdeltproject.org/api/v2/summary/summary?d=web&t=summary", timeout=30)
    # GDELT summary is complex — just grab the article count as activity proxy
    fid = get_fid('gdelt_article_count', 'sentiment', 'GDELT Global News Article Volume')
    today = datetime.utcnow().strftime('%Y-%m-%d')
    # Use content length as proxy for activity volume
    insert_obs(fid, today, len(r.text), gdelt_src)
    total += 1
    log.info("  GDELT response: {} bytes", len(r.text))
except Exception as e:
    log.error("  GDELT: ERROR {}", e)

# ═══════════════════════════════════════════
# 7. REDDIT — Subreddit activity (public JSON)
# ═══════════════════════════════════════════
log.info("\n--- Reddit Activity ---")
reddit_src = get_src('REDDIT')
subreddits = {
    'reddit_wallstreetbets': 'wallstreetbets',
    'reddit_bitcoin': 'bitcoin',
    'reddit_cryptocurrency': 'cryptocurrency',
    'reddit_stocks': 'stocks',
    'reddit_economics': 'economics',
}
today = datetime.utcnow().strftime('%Y-%m-%d')
for feat_name, sub in subreddits.items():
    fid = get_fid(feat_name, 'sentiment', f'Reddit r/{sub} active users')
    try:
        r = requests.get(f"https://www.reddit.com/r/{sub}/about.json",
            headers={"User-Agent": "GRID/1.0"}, timeout=15)
        data = r.json().get('data', {})
        active = data.get('accounts_active', 0) or data.get('active_user_count', 0) or 0
        subscribers = data.get('subscribers', 0)
        insert_obs(fid, today, active, reddit_src)
        total += 1
        
        fid_sub = get_fid(f'{feat_name}_subs', 'sentiment', f'Reddit r/{sub} subscribers')
        insert_obs(fid_sub, today, subscribers, reddit_src)
        total += 1
        log.info("  r/{}: {:,} active, {:,} subs", sub, active, subscribers)
    except Exception as e:
        log.error("  r/{}: ERROR {}", sub, e)
    time.sleep(1)

# ═══════════════════════════════════════════
# 8. CONGRESSIONAL TRADING
# ═══════════════════════════════════════════
log.info("\n--- Congressional Trading ---")
congress_src = get_src('CONGRESS')
try:
    r = requests.get("https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json", timeout=60)
    trades = r.json()
    
    # Count trades per day as activity signal
    from collections import Counter
    daily_counts = Counter()
    for t in trades:
        d = t.get('transaction_date', '')
        if d and d >= '2024-01-01':
            daily_counts[d] += 1
    
    fid = get_fid('congress_trade_volume', 'sentiment', 'Congressional Stock Trades Per Day')
    for d, count in sorted(daily_counts.items()):
        insert_obs(fid, d, count, congress_src)
        total += 1
    log.info("  Congressional trades: {} days since 2024", len(daily_counts))
except Exception as e:
    log.error("  Congress: ERROR {}", e)

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
log.info("Total new insertions: {}", total)
log.info("Total resolved series: {}", res_total)
log.info("Total features with data: {}", feat_total)
log.info("Total sources: {}", src_total)
log.info("{}", '='*50)

pg.close()
