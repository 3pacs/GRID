"""Read-only sector comparison from delayed Cboe chains, never an execution feed."""
import json, math, re, statistics, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

UNIVERSE = [('SPY','S&P 500','benchmark'),('QQQ','Nasdaq 100','benchmark'),('IWM','Small caps','benchmark'),
 ('XLK','Technology','sector'),('XLF','Financials','sector'),('XLE','Energy','sector'),
 ('XLV','Health care','sector'),('XLI','Industrials','sector'),('XLY','Consumer discretionary','sector'),
 ('XLP','Consumer staples','sector'),('XLU','Utilities','sector'),('XLB','Materials','sector'),
 ('XLRE','Real estate','sector'),('XLC','Communication services','sector'),('SMH','Semiconductors','industry')]
ET = ZoneInfo('America/New_York')

def number(v):
    return float(v) if isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) else None

def summarize(doc, symbol, now=None):
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(ET).date().isoformat()
    d = doc['data']
    if d.get('symbol') != symbol: raise ValueError('Symbol mismatch')
    spot = number(d.get('current_price'))
    rows = []
    for o in d.get('options',[]):
        m = re.fullmatch(re.escape(symbol)+r'(\d{6})([CP])(\d{8})',o.get('option',''))
        if not m: continue
        expiry = datetime.strptime(m[1],'%y%m%d').date().isoformat()
        if expiry < today: continue
        rows.append({**o,'expiry':expiry,'side':m[2],'strike':int(m[3])/1000})
    expiries = sorted({o['expiry'] for o in rows})
    expiry = expiries[0] if expiries else None
    selected = [o for o in rows if o['expiry']==expiry]
    same_day = expiry == today
    # No missing-volume-to-zero coercion; an incomplete total is withheld.
    def total(field, contracts):
        values = [number(o.get(field)) for o in contracts]
        return sum(values) if values and all(v is not None and v>=0 for v in values) else None
    strikes = sorted({o['strike'] for o in selected},key=lambda k:abs(k-spot))[:3] if spot and spot>0 else []
    atm = []
    for o in selected:
        if o['strike'] not in strikes: continue
        bid,ask = number(o.get('bid')),number(o.get('ask'))
        good = bid is not None and ask is not None and 0<bid<=ask
        atm.append({'contract':o['option'],'strike':o['strike'],'side':o['side'],
          'bid':bid,'ask':ask,'spread':ask-bid if good else None,
          'spread_pct':200*(ask-bid)/(ask+bid) if good else None,
          'volume':number(o.get('volume')),'oi':number(o.get('open_interest')),
          'bid_size':number(o.get('bid_size')),'ask_size':number(o.get('ask_size'))})
    spreads = [o['spread_pct'] for o in atm if o['spread_pct'] is not None]
    scenario_expiries=[e for e in expiries if e==today or datetime.fromisoformat(e).weekday()==4][:6]
    if expiry and expiry not in scenario_expiries:scenario_expiries.insert(0,expiry)
    scenario_contracts=[]
    for e in scenario_expiries:
        contracts=[o for o in rows if o['expiry']==e]
        ks=sorted({o['strike'] for o in contracts},key=lambda k:abs(k-spot))[:3] if spot else []
        for o in contracts:
            if o['strike'] in ks:
                scenario_contracts.append({k:o.get(k) for k in ('option','expiry','side','strike','bid','ask','bid_size','ask_size','volume','open_interest','delta','gamma','theta','iv')})
    prev = number(d.get('prev_day_close'))
    return {'symbol':symbol,'price':spot,'change_pct':100*(spot/prev-1) if spot and prev and prev>0 else None,
      'provider_timestamp_raw':doc.get('timestamp'),'underlying_trade_time_raw':d.get('last_trade_time'),
      'received_at':now.isoformat(),'session_date':today,'expiries':expiries,'nearest_expiry':expiry,
      'has_today_expiry':same_day if expiries else None,
      'today_volume':total('volume',selected) if same_day else None,
      'expiry_volume':total('volume',selected),'expiry_oi':total('open_interest',selected),
      'atm_spread_pct':statistics.median(spreads) if spreads else None,
      'atm_two_sided':len(spreads),'atm_contracts':len(atm),'contracts':atm,
      'source':'Cboe delayed options snapshot; not executable quotes',
      'timestamp_note':'Provider timestamps retained as supplied; timezone and quote age not independently verified.',
      'scenario_contracts':scenario_contracts,'error':None}

def fetch_one(item):
    symbol,name,kind = item
    try:
        req=urllib.request.Request('https://cdn.cboe.com/api/global/delayed_quotes/options/'+symbol+'.json',headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req,timeout=18) as response: doc=json.load(response)
        row=summarize(doc,symbol)
    except Exception as exc:
        row={'symbol':symbol,'error':type(exc).__name__,'received_at':datetime.now(timezone.utc).isoformat()}
    return {**row,'name':name,'kind':kind}

def snapshot():
    with ThreadPoolExecutor(max_workers=3) as pool: rows=list(pool.map(fetch_one,UNIVERSE))
    return {'rows':rows,'computed_at':datetime.now(timezone.utc).isoformat(),
      'method':'All 11 sector SPDRs plus SMH and SPY/QQQ/IWM benchmarks. Sort by observed same-day contract volume; this is not a liquidity guarantee. Spreads use the three strikes nearest the delayed underlying price, both calls and puts, nearest listed expiry. Zero bids and crossed quotes are excluded, with coverage shown. Missing data stays unavailable. No dealer positioning inferred.'}
