import yfinance as yf
import psycopg2, json, time
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

def get_fid(name, family, desc):
    cur.execute("INSERT INTO feature_registry (name,family,description,transformation,transformation_version,lag_days,normalization,missing_data_policy,eligible_from_date,model_eligible) VALUES (%s,%s,%s,'RAW',1,0,'ZSCORE','FORWARD_FILL','2024-04-01',TRUE) ON CONFLICT (name) DO NOTHING RETURNING id", (name, family, desc))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("SELECT id FROM feature_registry WHERE name=%s", (name,))
    return cur.fetchone()[0]

def get_src():
    cur.execute("SELECT id FROM source_catalog WHERE name='YFINANCE'")
    return cur.fetchone()[0]

def ins(fid, d, val, sid):
    cur.execute("INSERT INTO resolved_series (feature_id,obs_date,release_date,vintage_date,value,source_priority_used) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (fid,d,d,d,val,sid))

sid = get_src()
total = 0
today = datetime.utcnow().strftime('%Y-%m-%d')

WATCHLIST = {
    'BTC-USD': 'btc', 'ETH-USD': 'eth', 'SOL-USD': 'sol',
    'EOG': 'eog', 'DVN': 'dvn', 'CMCSA': 'cmcsa',
    'RTX': 'rtx', 'GD': 'gd', 'CI': 'ci',
    'PYPL': 'pypl', 'INTC': 'intc',
    'SPY': 'spy', 'QQQ': 'qqq', 'IWM': 'iwm',
    'XLE': 'xle', 'XLF': 'xlf', 'ITA': 'ita',
    'TLT': 'tlt', 'GLD': 'gld', 'URA': 'ura',
}

for ticker, prefix in WATCHLIST.items():
    print(f"\n--- {ticker} ---")
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        # Fundamentals
        fundamentals = {
            'pe_ratio': ('earnings', f'{prefix} P/E Ratio', info.get('trailingPE')),
            'forward_pe': ('earnings', f'{prefix} Forward P/E', info.get('forwardPE')),
            'pb_ratio': ('earnings', f'{prefix} P/B Ratio', info.get('priceToBook')),
            'ev_ebitda': ('earnings', f'{prefix} EV/EBITDA', info.get('enterpriseToEbitda')),
            'market_cap': ('breadth', f'{prefix} Market Cap', info.get('marketCap')),
            'beta': ('vol', f'{prefix} Beta', info.get('beta')),
            'dividend_yield': ('earnings', f'{prefix} Dividend Yield', info.get('dividendYield')),
            'short_pct': ('sentiment', f'{prefix} Short % Float', info.get('shortPercentOfFloat')),
            'avg_volume': ('breadth', f'{prefix} Avg Volume 10d', info.get('averageDailyVolume10Day')),
            'fcf': ('earnings', f'{prefix} Free Cash Flow', info.get('freeCashflow')),
            'revenue_growth': ('earnings', f'{prefix} Revenue Growth', info.get('revenueGrowth')),
            'profit_margin': ('earnings', f'{prefix} Profit Margin', info.get('profitMargins')),
            'roe': ('earnings', f'{prefix} Return on Equity', info.get('returnOnEquity')),
            'debt_to_equity': ('credit', f'{prefix} Debt/Equity', info.get('debtToEquity')),
            'current_ratio': ('credit', f'{prefix} Current Ratio', info.get('currentRatio')),
            'fifty_day_avg': ('vol', f'{prefix} 50d Moving Avg', info.get('fiftyDayAverage')),
            'two_hundred_avg': ('vol', f'{prefix} 200d Moving Avg', info.get('twoHundredDayAverage')),
            'fifty_two_high': ('vol', f'{prefix} 52w High', info.get('fiftyTwoWeekHigh')),
            'fifty_two_low': ('vol', f'{prefix} 52w Low', info.get('fiftyTwoWeekLow')),
        }
        
        fcount = 0
        for suffix, (family, desc, val) in fundamentals.items():
            if val is not None:
                fid = get_fid(f'{prefix}_{suffix}', family, desc)
                ins(fid, today, float(val), sid)
                total += 1
                fcount += 1
        print(f"  Fundamentals: {fcount} metrics")

        # Insider transactions
        try:
            insiders = t.insider_transactions
            if insiders is not None and not insiders.empty:
                buys = len(insiders[insiders['Text'].str.contains('Purchase|Buy', case=False, na=False)])
                sells = len(insiders[insiders['Text'].str.contains('Sale|Sell', case=False, na=False)])
                fid_b = get_fid(f'{prefix}_insider_buys', 'sentiment', f'{prefix} Insider Buy Count')
                fid_s = get_fid(f'{prefix}_insider_sells', 'sentiment', f'{prefix} Insider Sell Count')
                ins(fid_b, today, buys, sid)
                ins(fid_s, today, sells, sid)
                total += 2
                print(f"  Insiders: {buys} buys, {sells} sells")
        except Exception as e:
            print(f"  Insiders: {e}")

        # Institutional holders
        try:
            inst = t.institutional_holders
            if inst is not None and not inst.empty:
                inst_pct = inst['pctHeld'].sum() if 'pctHeld' in inst.columns else 0
                fid = get_fid(f'{prefix}_inst_ownership', 'sentiment', f'{prefix} Institutional Ownership %')
                ins(fid, today, float(inst_pct), sid)
                total += 1
                print(f"  Institutional: {inst_pct:.1%}")
        except Exception as e:
            print(f"  Institutional: {e}")

        # Earnings history
        try:
            earnings = t.earnings_history
            if earnings is not None and not earnings.empty:
                for _, row in earnings.iterrows():
                    d = str(row.get('Earnings Date', ''))[:10] if 'Earnings Date' in earnings.columns else None
                    surprise = row.get('Surprise(%)', None) or row.get('surprisePercent', None)
                    if d and surprise is not None:
                        fid = get_fid(f'{prefix}_earnings_surprise', 'earnings', f'{prefix} Earnings Surprise %')
                        ins(fid, d, float(surprise), sid)
                        total += 1
                print(f"  Earnings history loaded")
        except Exception as e:
            print(f"  Earnings: {e}")

        # Recommendations
        try:
            recs = t.recommendations
            if recs is not None and not recs.empty:
                recent = recs.tail(1).iloc[0] if len(recs) > 0 else None
                if recent is not None:
                    buy = recent.get('strongBuy', 0) + recent.get('buy', 0)
                    sell = recent.get('strongSell', 0) + recent.get('sell', 0)
                    hold = recent.get('hold', 0)
                    fid_b = get_fid(f'{prefix}_analyst_buy', 'sentiment', f'{prefix} Analyst Buy Ratings')
                    fid_s = get_fid(f'{prefix}_analyst_sell', 'sentiment', f'{prefix} Analyst Sell Ratings')
                    fid_h = get_fid(f'{prefix}_analyst_hold', 'sentiment', f'{prefix} Analyst Hold Ratings')
                    ins(fid_b, today, buy, sid)
                    ins(fid_s, today, sell, sid)
                    ins(fid_h, today, hold, sid)
                    total += 3
                    print(f"  Analysts: {buy} buy, {hold} hold, {sell} sell")
        except Exception as e:
            print(f"  Recommendations: {e}")

        time.sleep(0.5)

    except Exception as e:
        print(f"  {ticker}: ERROR {e}")

cur.execute("SELECT count(*) FROM resolved_series")
print(f"\nTotal resolved: {cur.fetchone()[0]}")
cur.execute("SELECT count(DISTINCT feature_id) FROM resolved_series")
print(f"Total features: {cur.fetchone()[0]}")
pg.close()
print(f"Ticker deep: {total} inserted")
