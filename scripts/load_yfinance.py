import yfinance as yf
import psycopg2
from datetime import datetime

pg = psycopg2.connect(dbname='griddb', user='grid', password='grid2026')
pg.autocommit = True
cur = pg.cursor()

# Add yfinance source
cur.execute("INSERT INTO source_catalog (name,base_url,cost_tier,latency_class,pit_available,revision_behavior,trust_score,priority_rank) VALUES ('YFINANCE','https://finance.yahoo.com','FREE','EOD',TRUE,'NEVER','MED',7) ON CONFLICT (name) DO NOTHING")
cur.execute("SELECT id FROM source_catalog WHERE name='YFINANCE'")
src_id = cur.fetchone()[0]

# New features
NEW_FEATURES = [
    ('sp500', 'breadth', 'S&P 500 Index', 'RAW'),
    ('nasdaq', 'breadth', 'NASDAQ Composite', 'RAW'),
    ('russell2000', 'breadth', 'Russell 2000 Small Cap', 'RAW'),
    ('gold', 'commodity', 'Gold Spot Price', 'RAW'),
    ('crude_oil', 'commodity', 'WTI Crude Oil', 'RAW'),
    ('copper', 'commodity', 'Copper Futures', 'RAW'),
    ('btc', 'sentiment', 'Bitcoin USD', 'RAW'),
    ('eth', 'sentiment', 'Ethereum USD', 'RAW'),
    ('tlt', 'rates', 'iShares 20Y Treasury Bond ETF', 'RAW'),
    ('hyg', 'credit', 'iShares HY Corporate Bond ETF', 'RAW'),
    ('lqd', 'credit', 'iShares IG Corporate Bond ETF', 'RAW'),
    ('dxy_etf', 'fx', 'Invesco Dollar Bull ETF', 'RAW'),
    ('eem', 'breadth', 'iShares MSCI Emerging Markets', 'RAW'),
    ('xle', 'commodity', 'Energy Select SPDR', 'RAW'),
    ('xlf', 'breadth', 'Financial Select SPDR', 'RAW'),
    ('ita', 'breadth', 'iShares US Aerospace Defense ETF', 'RAW'),
]

TICKER_MAP = {
    'sp500': '^GSPC', 'nasdaq': '^IXIC', 'russell2000': '^RUT',
    'gold': 'GC=F', 'crude_oil': 'CL=F', 'copper': 'HG=F',
    'btc': 'BTC-USD', 'eth': 'ETH-USD',
    'tlt': 'TLT', 'hyg': 'HYG', 'lqd': 'LQD',
    'dxy_etf': 'UUP', 'eem': 'EEM',
    'xle': 'XLE', 'xlf': 'XLF', 'ita': 'ITA',
}

feat_ids = {}
for name, family, desc, transform in NEW_FEATURES:
    cur.execute(
        "INSERT INTO feature_registry (name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible) "
        "VALUES (%s,%s,%s,%s,1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name, family, desc, transform))
    row = cur.fetchone()
    if row:
        feat_ids[name] = row[0]
    else:
        cur.execute("SELECT id FROM feature_registry WHERE name=%s", (name,))
        feat_ids[name] = cur.fetchone()[0]

print(f"Features: {len(feat_ids)} registered")

# Download 2 years of daily data
total = 0
for name, ticker in TICKER_MAP.items():
    fid = feat_ids.get(name)
    if not fid:
        continue
    try:
        df = yf.download(ticker, period='2y', interval='1d', progress=False)
        if df.empty:
            print(f"  {name} ({ticker}): no data")
            continue
        for dt, row in df.iterrows():
            close = float(row['Close'].iloc[0]) if hasattr(row['Close'], 'iloc') else float(row['Close'])
            obs_date = dt.strftime('%Y-%m-%d')
            cur.execute(
                "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (fid, obs_date, obs_date, obs_date, close, src_id))
            total += 1
        print(f"  {name} ({ticker}): {len(df)} rows loaded")
    except Exception as e:
        print(f"  {name} ({ticker}): ERROR {e}")

print(f"\nTotal inserted: {total}")
cur.execute("SELECT count(*) FROM resolved_series")
print(f"Total resolved: {cur.fetchone()[0]}")
pg.close()
