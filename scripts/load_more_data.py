import yfinance as yf
import psycopg2
import requests
import json
import os
from datetime import datetime
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

cur.execute("SELECT id FROM source_catalog WHERE name='YFINANCE'")
yf_id = cur.fetchone()[0]
cur.execute("SELECT id FROM source_catalog WHERE name='FRED'")
fred_id = cur.fetchone()[0]

FEATURES = {
    # Global indices
    'nikkei': ('^N225', 'breadth', 'Nikkei 225'),
    'dax': ('^GDAXI', 'breadth', 'DAX Germany'),
    'ftse': ('^FTSE', 'breadth', 'FTSE 100 UK'),
    'hang_seng': ('^HSI', 'breadth', 'Hang Seng Index'),
    'shanghai': ('000001.SS', 'breadth', 'Shanghai Composite'),
    'kospi': ('^KS11', 'breadth', 'KOSPI South Korea'),
    'bovespa': ('^BVSP', 'breadth', 'Bovespa Brazil'),
    # More commodities
    'silver': ('SI=F', 'commodity', 'Silver Futures'),
    'platinum': ('PL=F', 'commodity', 'Platinum Futures'),
    'nat_gas': ('NG=F', 'commodity', 'Natural Gas Futures'),
    'wheat': ('ZW=F', 'commodity', 'Wheat Futures'),
    'uranium_etf': ('URA', 'commodity', 'Global X Uranium ETF'),
    # Hypothesis registry stocks
    'eog': ('EOG', 'earnings', 'EOG Resources'),
    'dvn': ('DVN', 'earnings', 'Devon Energy'),
    'cmcsa': ('CMCSA', 'earnings', 'Comcast'),
    'rtx': ('RTX', 'earnings', 'RTX Corp'),
    'gd': ('GD', 'earnings', 'General Dynamics'),
    'ci': ('CI', 'earnings', 'Cigna'),
    'pypl': ('PYPL', 'earnings', 'PayPal'),
    'siri': ('SIRI', 'earnings', 'Sirius XM'),
    'rxt': ('RXT', 'earnings', 'Rackspace'),
    'intc': ('INTC', 'earnings', 'Intel'),
    # More crypto
    'sol': ('SOL-USD', 'sentiment', 'Solana USD'),
    'tao': ('TAO22974-USD', 'sentiment', 'Bittensor TAO USD'),
    'ada': ('ADA-USD', 'sentiment', 'Cardano USD'),
    'link': ('LINK-USD', 'sentiment', 'Chainlink USD'),
    'avax': ('AVAX-USD', 'sentiment', 'Avalanche USD'),
    # Sector ETFs
    'xlk': ('XLK', 'breadth', 'Technology SPDR'),
    'xlv': ('XLV', 'breadth', 'Healthcare SPDR'),
    'xlu': ('XLU', 'breadth', 'Utilities SPDR'),
    'xlre': ('XLRE', 'breadth', 'Real Estate SPDR'),
    'xli': ('XLI', 'breadth', 'Industrial SPDR'),
    'xlp': ('XLP', 'breadth', 'Consumer Staples SPDR'),
    'xly': ('XLY', 'breadth', 'Consumer Discretionary SPDR'),
    'xlb': ('XLB', 'breadth', 'Materials SPDR'),
    'xlc': ('XLC', 'breadth', 'Communication Services SPDR'),
    # Volatility
    'vxn': ('^VXN', 'vol', 'NASDAQ Volatility'),
    'move_index': ('^MOVE', 'vol', 'MOVE Bond Volatility'),
    # Fixed income
    'shy': ('SHY', 'rates', 'iShares 1-3Y Treasury'),
    'ief': ('IEF', 'rates', 'iShares 7-10Y Treasury'),
    'tip': ('TIP', 'rates', 'iShares TIPS Bond ETF'),
    'bnd': ('BND', 'rates', 'Vanguard Total Bond'),
    # Real estate
    'vnq': ('VNQ', 'breadth', 'Vanguard Real Estate ETF'),
    # Dollar pairs
    'eurusd': ('EURUSD=X', 'fx', 'EUR/USD'),
    'usdjpy': ('JPY=X', 'fx', 'USD/JPY'),
    'gbpusd': ('GBPUSD=X', 'fx', 'GBP/USD'),
    'usdcnh': ('CNY=X', 'fx', 'USD/CNH'),
}

total = 0
for name, (ticker, family, desc) in FEATURES.items():
    cur.execute(
        "INSERT INTO feature_registry (name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible) "
        "VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name, family, desc))
    row = cur.fetchone()
    if row:
        fid = row[0]
    else:
        cur.execute("SELECT id FROM feature_registry WHERE name=%s", (name,))
        fid = cur.fetchone()[0]
    try:
        df = yf.download(ticker, period='2y', interval='1d', progress=False)
        if df.empty:
            print(f"  {name} ({ticker}): no data")
            continue
        count = 0
        for dt, r in df.iterrows():
            close = float(r['Close'].iloc[0]) if hasattr(r['Close'], 'iloc') else float(r['Close'])
            obs = dt.strftime('%Y-%m-%d')
            cur.execute(
                "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (fid, obs, obs, obs, close, yf_id))
            count += 1
        total += count
        print(f"  {name} ({ticker}): {count} rows")
    except Exception as e:
        print(f"  {name} ({ticker}): ERROR {e}")

# Additional FRED series
FRED_KEY = os.environ.get('FRED_API_KEY', 'bc8b4507787daf394e42f07b97d6c0fc')
FRED_NEW = {
    'ice_bofa_move': ('BAMLC0A4CBBB', 'vol', 'ICE BofA BBB Corporate Spread'),
    'ted_spread': ('TEDRATE', 'credit', 'TED Spread'),
    'consumer_sentiment': ('UMCSENT', 'sentiment', 'U Mich Consumer Sentiment'),
    'housing_starts': ('HOUST', 'macro', 'Housing Starts'),
    'building_permits': ('PERMIT', 'macro', 'Building Permits'),
    'initial_claims': ('ICSA', 'macro', 'Initial Jobless Claims'),
    'continued_claims': ('CCSA', 'macro', 'Continued Jobless Claims'),
    'pce_deflator': ('PCEPI', 'macro', 'PCE Price Index'),
    'core_pce': ('PCEPILFE', 'macro', 'Core PCE Price Index'),
    'real_gdp': ('GDPC1', 'macro', 'Real GDP'),
    'chicago_fed': ('NFCI', 'credit', 'Chicago Fed Financial Conditions'),
    'kansas_fed': ('FRBKCLMCIM', 'macro', 'KC Fed Labor Market Conditions'),
    'capacity_util': ('TCU', 'macro', 'Capacity Utilization'),
    'leading_index': ('USSLIND', 'macro', 'Leading Economic Index'),
    'loan_growth': ('TOTLL', 'credit', 'Total Loans and Leases'),
}

for name, (series, family, desc) in FRED_NEW.items():
    cur.execute(
        "INSERT INTO feature_registry (name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible) "
        "VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name, family, desc))
    row = cur.fetchone()
    if row:
        fid = row[0]
    else:
        cur.execute("SELECT id FROM feature_registry WHERE name=%s", (name,))
        fid = cur.fetchone()[0]
    try:
        data = requests.get("https://api.stlouisfed.org/fred/series/observations",
            params={"series_id": series, "api_key": FRED_KEY, "file_type": "json", "sort_order": "desc", "limit": "500"}, timeout=30).json()
        count = 0
        for o in data.get("observations", []):
            v = o.get("value", ".")
            if v == ".": continue
            cur.execute(
                "INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (fid, o["date"], o["date"], o["date"], float(v), fred_id))
            count += 1
        total += count
        print(f"  {name} ({series}): {count} rows")
    except Exception as e:
        print(f"  {name} ({series}): ERROR {e}")

cur.execute("SELECT count(*) FROM resolved_series")
print(f"\nTotal inserted: {total}")
print(f"Total resolved: {cur.fetchone()[0]}")
cur.execute("SELECT count(DISTINCT feature_id) FROM resolved_series")
print(f"Total features with data: {cur.fetchone()[0]}")
pg.close()
