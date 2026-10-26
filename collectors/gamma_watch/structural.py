"""Structural context, not a trading signal. First receipt is the replay boundary."""
import json, math, urllib.request, subprocess, os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from journal import stamp

ET=ZoneInfo('America/New_York')
BASE='https://markets.newyorkfed.org/api/'
SOURCES={
 'fed_treasury':BASE+'tsy/all/operations/summary/latest.json',
 'repo':BASE+'rp/all/all/results/latest.json',
 'tga':'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance?sort=-record_date&page[size]=20',
}
SYMBOLS=['SPY','QQQ','IWM','RSP','TLT','IEF','HYG','LQD','VIX','/ES:XCME','/NQ:XCME','/CL:XNYM','$TICK','$ADVN','$DECN','$UVOL','$DVOL']

def iso():return datetime.now(timezone.utc).isoformat()
def number(v):
    if isinstance(v,bool):return None
    try:
        x=float(str(v).replace(',',''));return x if math.isfinite(x) else None
    except (ValueError,TypeError):return None
def fetch(url):
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=12) as r:return json.load(r)

def official_one(item):
    name,url=item
    try:
        raw=fetch(url);received=iso()
        if name=='fed_treasury':rows=raw['treasury']['auctions']
        elif name=='repo':rows=raw['repo']['operations']
        elif name=='tga':
            allrows=raw['data'];latest=max((r['record_date'] for r in allrows),default=None)
            rows=[r for r in allrows if r['record_date']==latest]
        else:rows=raw
        if not isinstance(rows,list):raise ValueError('Invalid rows')
        return name,{'status':'available' if rows else 'no_records_returned','received_at':received,'source_url':url,'rows':rows,
          'timing':'Published results/schedule, not streaming. Empty response is not zero flow. Source dates retained; first local receipt controls replay.'}
    except Exception as e:return name,{'status':'unavailable','received_at':iso(),'source_url':url,'reason':type(e).__name__,'rows':[]}

def official():
    today=datetime.now(ET).date()
    urls={**SOURCES,**{'auction_'+(today+timedelta(days=i)).isoformat():'https://www.treasurydirect.gov/TA_WS/securities/search?format=json&auctionDate='+(today+timedelta(days=i)).isoformat() for i in range(8)}}
    with ThreadPoolExecutor(max_workers=4) as pool:results=dict(pool.map(official_one,urls.items()))
    # Retain only the market fields needed; raw API bodies are not public account data.
    keys=['cusip','securityType','securityTerm','announcementDate','auctionDate','issueDate','offeringAmount','highYield','highDiscountRate','bidToCoverRatio','closingTimeCompetitive']
    for name,result in results.items():
        if name.startswith('auction_'):result['rows']=[{k:r.get(k) for k in keys} for r in result['rows']]
    return {'computed_at':iso(),'sources':results,'interpretation':'No aggregate net-liquidity or equity-direction claim. Repo rollover, Treasury maturities, taxes and spending must be reconciled separately.'}

def rebalance_amount(equity_return,bond_return,weight=.6):
    """Equity purchase dollars per $1bn INITIAL portfolio, fully restored target."""
    if any(number(x) is None for x in [equity_return,bond_return,weight]) or not 0<weight<1 or min(equity_return,bond_return)<=-1:raise ValueError('Invalid return/weight')
    return 1e9*weight*(1-weight)*(bond_return-equity_return)

def adjusted_series(doc,now):
    r=doc['chart']['result'][0];adj=r['indicators']['adjclose'][0]['adjclose'];result={}
    local=datetime.fromtimestamp(now,ET)
    for t,v in zip(r['timestamp'],adj):
        day=datetime.fromtimestamp(t,ET).date()
        # Daily bars may be partial: exclude today until 16:15 ET.
        if day>local.date() or (day==local.date() and (local.hour,local.minute)<(16,15)):continue
        v=number(v)
        if v is not None and v>0:result[day.isoformat()]=v
    return result

def rebalance():
    now=datetime.now(timezone.utc);local=now.astimezone(ET);series={}
    urls={s:'https://query1.finance.yahoo.com/v8/finance/chart/'+s+'?interval=1d&range=1y' for s in ['SPY','AGG']}
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:raw=dict(zip(urls,pool.map(fetch,urls.values())))
        series={s:adjusted_series(d,now.timestamp()) for s,d in raw.items()}
        common=sorted(set(series['SPY'])&set(series['AGG']))
        if not common:raise ValueError('No synchronized observations')
        latest=common[-1]
        if latest!=max(series['SPY']) or latest!=max(series['AGG']):raise ValueError('Latest dates disagree')
        if (local.date()-datetime.fromisoformat(latest).date()).days>4:raise ValueError('Daily data stale')
        month=local.date().replace(day=1);quarter=month.replace(month=((month.month-1)//3)*3+1);rows=[]
        for label,start in [('month',month),('quarter',quarter)]:
            prior=[d for d in common if d<start.isoformat()]
            if not prior:continue
            base=prior[-1]
            if (start-datetime.fromisoformat(base).date()).days>5:continue
            er=series['SPY'][latest]/series['SPY'][base]-1;br=series['AGG'][latest]/series['AGG'][base]-1
            rows.append({'period':label,'baseline':base,'as_of_date':latest,'equity_return':er,'bond_return':br,
              'captured_adjusted_closes':{s:{'baseline':series[s][base],'latest':series[s][latest]} for s in series},
              'scenarios':[{'equity_weight':w,'equity_purchase_per_initial_billion':rebalance_amount(er,br,w)} for w in [.4,.6,.8]]})
        return {'status':'modeled_daily_proxy' if rows else 'unavailable','computed_at':iso(),'rows':rows,'sources':urls,
         'assumptions':'SPY/AGG adjusted-close proxies; 40/60, 60/40 and 80/20 starting allocations. Full reset, no contributions/withdrawals or prior trades. Positive = hypothetical equity purchase. Not actual pension orders, aggregate AUM, or a timing signal. Yahoo history can revise; only this captured version is replayable.'}
    except Exception as e:return {'status':'unavailable','computed_at':iso(),'rows':[],'reason':type(e).__name__}

def normalize_rtd(raw,now):
    if raw.get('schema')!='structural-rtd-v1':raise ValueError('Unexpected schema')
    collected=stamp(raw.get('collected_at'));healthy=collected is not None and -5<=now-collected<=20 and raw.get('heartbeat',0)>0
    local=datetime.fromtimestamp(now,ET)
    session=local.weekday()<5 and (9,30)<=(local.hour,local.minute)<(16,0)
    records={r['symbol']:r for r in raw.get('records',[]) if r.get('field')=='LAST'};rows=[]
    for s in SYMBOLS:
        r=records.get(s,{});v=number(r.get('value'));t=stamp(r.get('received_at'))
        age=now-t if t is not None else None
        valid=v is not None and r.get('callback_seen') and (s=='$TICK' or v>=0)
        usable=bool(valid and r.get('callback_count',0)>=2 and healthy and age is not None and 0<=age<=20 and session)
        rows.append({'symbol':s,'value':v if valid else None,'callback_at':r.get('received_at'),'age_seconds':round(age,1) if age is not None else None,
          'status':'fresh_receipt' if usable else 'unavailable' if not valid else 'outside_regular_session' if not session else 'initial_cache_unverified' if r.get('callback_count',0)<2 else 'stale',
          'direction_usable':usable})
    values={r['symbol']:r['value'] for r in rows if r['direction_usable']}
    ad=(values['$ADVN']-values['$DECN']) if all(s in values for s in ['$ADVN','$DECN']) else None
    return {'computed_at':iso(),'collected_at':raw.get('collected_at'),'collector_healthy':healthy,'regular_session_clock':session,'rows':rows,'advance_minus_decline':ad,
      'note':'RTD LAST callback receipt, not exchange event time or execution tape. Initial cached callbacks do not verify entitlement. Regular-session clock only; no holiday calendar. Futures mapping and next-session live behavior require verification.'}

def live():
    p=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','anik','cat /c/Users/anikd/Documents/Codex/Structural-Feed/snapshot.json'],capture_output=True,timeout=12,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    if p.returncode:raise ValueError('Structural collector unavailable')
    return normalize_rtd(json.loads(p.stdout),datetime.now(timezone.utc).timestamp())
