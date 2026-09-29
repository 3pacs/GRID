"""Local read-only SPY monitor. Public polling + explicitly labeled frozen-chain research."""
import csv, json, math, threading, time, urllib.request, os
from datetime import datetime, timezone
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import numpy as np
import broker
import pressure
import sectors
import journal
import scenarios
import structural
from urllib.parse import urlparse, parse_qs

ROOT=Path(__file__).resolve().parent
PORT=int(os.environ.get('GEX_PORT','8769'))
ALLOWED_HOSTS={'127.0.0.1:'+str(PORT),'localhost:'+str(PORT)} | set(filter(None,os.environ.get('GEX_ALLOWED_HOSTS','').split(',')))
STATE={'quote':None,'gex':None,'quote_error':None,'gex_error':None,'model':None}
LOCK=threading.Lock()
TICKS=[]
JOURNAL=journal.Journal(ROOT/'observations.sqlite3')

def journal_loop():
    while True:
        try:
            JOURNAL.ingest(payload(),time.time())
            with LOCK:STATE['journal_error']=None
        except Exception:
            with LOCK:STATE['journal_error']='Persistent journal write failed'
        time.sleep(5)
def iso(): return datetime.now(timezone.utc).isoformat()
def fetch(url,payload=None):
    req=urllib.request.Request(url,data=json.dumps(payload).encode() if payload else None,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json, text/event-stream','Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=12) as r:return json.load(r)
def quote():
    d=fetch('https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1m&range=1d&includePrePost=true')['chart']['result'][0]
    q=d['indicators']['quote'][0]
    bars=[{'t':t,'c':q['close'][i],'h':q['high'][i],'l':q['low'][i]} for i,t in enumerate(d['timestamp']) if q['close'][i] is not None]
    if not bars:raise ValueError('No priced bars')
    # Keep only current ET calendar date. No overnight bars in the range calculation.
    from zoneinfo import ZoneInfo
    et=ZoneInfo('America/New_York'); today=datetime.now(et).date()
    bars=[b for b in bars if datetime.fromtimestamp(b['t'],et).date()==today]
    if not bars:raise ValueError('No current-session bars')
    pm=[b for b in bars if (4,0)<=(datetime.fromtimestamp(b['t'],et).hour,datetime.fromtimestamp(b['t'],et).minute)<(9,30)]
    return {'price':bars[-1]['c'],'as_of':datetime.fromtimestamp(bars[-1]['t'],timezone.utc).isoformat(),'received_at':iso(),'source':'Yahoo chart · public, latency not guaranteed','previous_close':d['meta'].get('chartPreviousClose'),'pm_high':max((b['h'] for b in pm if b['h'] is not None),default=None),'pm_low':min((b['l'] for b in pm if b['l'] is not None),default=None),'bars':bars}
def gex():
    d=fetch('https://zerogex.io/mcp',{'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':'get_gamma_levels','arguments':{'symbol':'SPY'}}})
    r=d.get('result',{})
    if r.get('isError') or 'structuredContent' not in r:raise ValueError('Provider did not return a level snapshot')
    s=r['structuredContent']
    if s.get('symbol')!='SPY' or not s.get('as_of'):raise ValueError('Invalid symbol or timestamp')
    return {**s,'received_at':iso(),'source':'ZeroGEX · delayed ~15 minutes'}

def model():
    rows=list(csv.DictReader((ROOT/'chain.csv').open(encoding='utf-8-sig')))
    now=datetime.now(timezone.utc); pairs={}
    for r in rows:pairs[(r['expiry'],r['strike'],r['opt_type'])]=r
    grid=np.arange(700,800.01,.5); variants=[]; excluded=0
    for mode in ['paired OTM IV','raw IV filtered']:
      chain=[]; excluded=0
      for r in rows:
        expiry=datetime.fromisoformat(r['expiry']+'T20:00:00+00:00'); T=(expiry-now).total_seconds()/(365*86400)
        if T<=0: excluded+=1;continue
        vol=float(r['implied_vol'] or 0)
        if mode=='paired OTM IV':
          # Use prior-snapshot reference spot only for choosing the OTM IV side.
          side='put' if float(r['strike'])<767.81 else 'call'
          other=pairs.get((r['expiry'],r['strike'],side))
          vol=float(other['implied_vol'] or 0) if other else vol
        if not .02<=vol<=3:excluded+=1;continue
        chain.append([float(r['strike']),T,vol,float(r['open_interest']),1 if r['opt_type']=='call' else -1])
      if not chain:raise ValueError('No usable option rows')
      a=np.array(chain); S=grid[:,None]; K,T,vol,oi,sign=a.T
      for shock in [0,.2,.4]:
        iv=vol*(1+shock); d1=(np.log(S/K)+(.04-.012+.5*iv**2)*T)/(iv*np.sqrt(T))
        gamma=np.exp(-.012*T)*np.exp(-.5*d1*d1)/np.sqrt(2*np.pi)/(S*iv*np.sqrt(T))
        net=(gamma*oi*100*S*S*.01*sign).sum(axis=1)/1e9
        roots=[]
        for i in range(len(grid)-1):
          if net[i]*net[i+1]<0:roots.append(round(float(grid[i]-net[i]*(grid[i+1]-grid[i])/(net[i+1]-net[i])),2))
        variants.append({'name':mode,'iv_shock':shock,'values':np.round(net,4).tolist(),'roots':roots,'used':len(chain),'excluded':excluded})
    return {'computed_at':iso(),'chain_as_of':rows[0]['created_at'],'snapshot_date':rows[0]['snap_date'],'expiries':sorted(set(r['expiry'] for r in rows)),'rows':len(rows),'grid':grid.tolist(),'variants':variants,'scope':'SPY only; 12 captured expiries, not the complete options market','assumptions':'Calls positive / puts negative; OI is not observed dealer inventory. Frozen IV and OI; 4% rate, 1.2% dividend yield; 16:00 ET expiry approximation. No SPX/ES offset or intraday opening/closing flow. Research only.'}

def loop(name,fn,interval):
    while True:
        try:
            result=fn()
            with LOCK: STATE[name]=result;STATE[name+'_error']=None
        except Exception as e:
            with LOCK: STATE[name+'_error']=type(e).__name__+': '+str(e)[:160]
        time.sleep(interval)

def broker_loop():
    while True:
        try:
            feed=broker.get_snapshot()
            try:curve=broker.curves(feed);model_error=None
            except Exception as e:curve=None;model_error=str(e)
            stamp=broker.dt(feed['quote_received_at']).timestamp()
            with LOCK:
                STATE['broker']={k:v for k,v in feed.items() if k!='rows'}
                STATE['broker_error']=None;STATE['broker_model']=curve;STATE['broker_model_error']=model_error
                if not TICKS or stamp>TICKS[-1]['t']:
                    TICKS.append({'t':stamp,'c':feed['price'],'h':feed['price'],'l':feed['price']})
                    del TICKS[:-1200]
                    try:
                        logdir=ROOT/'tape-recordings';logdir.mkdir(exist_ok=True)
                        with (logdir/(datetime.now(timezone.utc).strftime('%Y-%m-%d')+'.jsonl')).open('a') as tape:
                            tape.write(json.dumps({'received_at':iso(),'source':'RTD sampled quote, not execution tape',**TICKS[-1]})+'\n')
                        STATE['recording_error']=None
                    except OSError as e:STATE['recording_error']=str(e)
        except Exception as e:
            with LOCK:STATE['broker_error']=str(e)[:180]
        time.sleep(5)

def payload():
    with LOCK:
        data={**STATE,'served_at':iso()}
        b=data.get('broker');fresh=b and not data.get('broker_error') and (datetime.now(timezone.utc)-broker.dt(b['collected_at'])).total_seconds()<20
        data['broker_active']=bool(fresh)
        data['pressure']=pressure.summarize(list(TICKS),time.time())
        if not fresh:
            for window in data['pressure']['windows']:
                window.update(status='unavailable: brokerage disconnected',price_change=None)
        if fresh:
            public=data.get('quote') or {}
            data['quote_error']=None
            history=[p for p in public.get('bars',[]) if not TICKS or p['t']<TICKS[0]['t']]+list(TICKS)
            data['quote']={**public,'price':b['price'],'as_of':b['quote_received_at'],'received_at':b['received_at'],'source':'thinkorswim RTD · ANIK · collector receipt time','bid':b['bid'],'ask':b['ask'],'bars':history,'is_rtd':True}
            if data.get('broker_model') and not data.get('broker_model_error'):data['model']=data['broker_model']
        return public_labels(data)

def public_labels(value):
    # Keep internal host identity out of public JSON, including error messages.
    if isinstance(value, dict):return {k:public_labels(v) for k,v in value.items()}
    if isinstance(value, list):return [public_labels(v) for v in value]
    if isinstance(value, str):
        import re
        return re.sub(r'(?i)anik(?:dang|d|srobot)?', 'brokerage-feed', value)
    return value

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get('Host') not in ALLOWED_HOSTS:
            self.send_error(403);return
        route=urlparse(self.path)
        if route.path=='/api/contracts':
            args=parse_qs(route.query);symbol=args.get('symbol',['SPY'])[0];option=args.get('contract',[''])[0]
            try:shock=float(args.get('shock',['0'])[0])
            except ValueError:self.send_error(400);return
            if shock not in (-.2,0,.2):self.send_error(400);return
            with LOCK:row=next((r for r in (STATE.get('sectors') or {}).get('rows',[]) if r['symbol']==symbol),None)
            if row is None:self.send_error(404);return
            contract=next((c for c in row.get('scenario_contracts',[]) if c['option']==option),None)
            result=scenarios.grid(row,contract,shock) if contract else {'symbol':symbol,'contracts':row.get('scenario_contracts',[]),'source':row.get('source')}
            if contract:
                try:JOURNAL.record_scenario(result)
                except Exception:result['recording_error']='Scenario history write failed'
            body=json.dumps(result,allow_nan=False).encode();typ='application/json'
        elif route.path=='/api/journal':
            try:since=max(0,float(parse_qs(route.query).get('since',['0'])[0]))
            except ValueError:self.send_error(400);return
            if not math.isfinite(since):self.send_error(400);return
            body=json.dumps(JOURNAL.read(since),allow_nan=False).encode();typ='application/json'
        elif self.path=='/api/state':
            body=json.dumps(payload(),allow_nan=False).encode()
            typ='application/json'
        elif self.path in ('/','/index.html'):
            body=(ROOT/'index.html').read_bytes();typ='text/html; charset=utf-8'
        else:self.send_error(404);return
        self.send_response(200);self.send_header('Content-Type',typ);self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff');self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'self'");self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass

if __name__=='__main__':
    for name,fn,interval in [('quote',quote,20),('gex',gex,120),('model',model,300),('sectors',sectors.snapshot,300),('structural_official',structural.official,300),('structural_rebalance',structural.rebalance,1800),('structural_live',structural.live,5)]:threading.Thread(target=loop,args=(name,fn,interval),daemon=True).start()
    threading.Thread(target=broker_loop,daemon=True).start()
    threading.Thread(target=journal_loop,daemon=True).start()
    print(f'SPY monitor on http://127.0.0.1:{PORT}',flush=True)
    ThreadingHTTPServer(('127.0.0.1',PORT),Handler).serve_forever()
