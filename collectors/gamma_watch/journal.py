"""Durable, receipt-time journal. No historical backfill or inferred executions."""
import json, sqlite3, math, re
from contextlib import contextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def stamp(s):
    try: return datetime.fromisoformat(re.sub(r'(\.\d{6})\d+',r'\1',s.replace('Z','+00:00'))).timestamp()
    except (ValueError,TypeError,AttributeError): return None

def fresh(s,now,limit):
    t=stamp(s)
    return t is not None and -5<=now-t<=limit

class Journal:
    def __init__(self,path):
        self.path=str(path)
        with self.db() as db:
            db.executescript('CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, received REAL NOT NULL, source_time TEXT, kind TEXT, body TEXT); CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, body TEXT); CREATE TABLE IF NOT EXISTS samples(received REAL, source_time TEXT, price REAL, source TEXT);')
            db.execute('CREATE TABLE IF NOT EXISTS artifacts(kind TEXT, version TEXT, received REAL, body TEXT, PRIMARY KEY(kind,version))')
    @contextmanager
    def db(self):
        db=sqlite3.connect(self.path,timeout=10)
        try:
            db.execute('PRAGMA journal_mode=WAL')
            with db:yield db
        finally:db.close()
    def ingest(self,s,now):
        with self.db() as db:
            for name in ['structural_official','structural_rebalance','structural_live']:
                snapshot=s.get(name)
                if snapshot:
                    db.execute('INSERT OR IGNORE INTO artifacts VALUES(?,?,?,?)',(name,snapshot['computed_at'],now,json.dumps(snapshot)))
            old={k:json.loads(v) for k,v in db.execute('SELECT key,body FROM state')}
            def event(kind,body,source_time=None):
                if kind=='level_revised':body={**body,'site_received_at':(s.get('gex') or {}).get('received_at')}
                elif kind in ('feed','crossed','returned_through','sampled_acceptance'):body={**body,'site_received_at':(s.get('quote') or {}).get('received_at')}
                db.execute('INSERT INTO events(received,source_time,kind,body) VALUES(?,?,?,?)',(now,source_time,kind,json.dumps(body)))
            def save(key,value): db.execute('INSERT OR REPLACE INTO state VALUES(?,?)',(key,json.dumps(value)))
            q=s.get('quote') or {};g=s.get('gex') or {}
            qok=not s.get('quote_error') and fresh(q.get('as_of'),now,20 if q.get('is_rtd') else 120)
            gok=not s.get('gex_error') and fresh(g.get('as_of'),now,1800)
            health={'broker':bool(s.get('broker_active')),'price_usable':qok,'gex_context_usable':gok,'source':q.get('source')}
            if health!=old.get('health'):event('feed',{'before':old.get('health'),'after':health},q.get('as_of'));save('health',health)
            levels={k:g[k] for k in ('call_wall','put_wall','gamma_flip','max_pain') if isinstance(g.get(k),(int,float)) and math.isfinite(g[k])} if gok else {}
            if gok:
                for k,v in levels.items():
                    previous=old.get('levels',{}).get(k)
                    if previous is not None and previous!=v:event('level_revised',{'level':k,'before':previous,'after':v,'note':'First observed here; exact intervening change time unknown.'},g.get('as_of'))
                save('levels',levels)
            for r in (s.get('sectors') or {}).get('rows',[]):
                key='spread:'+r['symbol'];v=r.get('atm_spread_pct');prev=old.get(key)
                if isinstance(v,(int,float)):
                    if prev and prev['expiry']==r.get('nearest_expiry') and v-prev['value']>=5 and v>=prev['value']*1.5:
                        event('spread_widened',{'symbol':r['symbol'],'before':prev['value'],'after':v,'expiry':r.get('nearest_expiry'),'site_received_at':r.get('received_at'),'note':'Delayed three-strike median, not one contract.'},r.get('provider_timestamp_raw'))
                    save(key,{'value':v,'expiry':r.get('nearest_expiry')})
            sectors=s.get('sectors')
            if sectors:db.execute('INSERT OR IGNORE INTO artifacts VALUES(?,?,?,?)',('sector_snapshot',sectors['computed_at'],now,json.dumps(sectors)))
            p=q.get('price');last=old.get('last_quote');t=stamp(q.get('as_of'))
            if not qok or not isinstance(p,(int,float)):
                save('interactions',{});return
            if last and t<=last['t']:return
            db.execute('INSERT INTO samples VALUES(?,?,?,?)',(now,q.get('as_of'),p,q.get('source')))
            continuous=bool(last and 0<t-last['t']<=30 and q.get('source')==last['source'] and now-last['received']<=30)
            interactions=old.get('interactions',{}) if continuous and gok else {}
            new={}
            for name,value in levels.items():
                prev=interactions.get(name);buffer=max(.05,value*.0001);side=1 if p>value+buffer else -1 if p<value-buffer else 0
                if not prev or prev['value']!=value:
                    prev={'value':value,'tests':0,'side':side,'outside':side,'since':t,'band_since':t if side==0 else None,'tested':False,'cross_at':None,'held':False,'points':[]}
                else:
                    if side==0 and prev['side']!=0:prev['band_since']=t;prev['tested']=False
                    if side==0 and prev.get('band_since') is not None and t-prev['band_since']>=10 and not prev.get('tested'):
                        prev['tests']+=1;prev['tested']=True
                    if side and prev.get('outside') and side!=prev['outside']:
                        kind='returned_through' if prev['cross_at'] and t-prev['cross_at']<=120 else 'crossed'
                        event(kind,{'level':name,'fixed_value':value,'price':p,'buffer':buffer,'side':side,'sampled':True},q.get('as_of'))
                        prev['cross_at']=t;prev['held']=False
                    if side!=prev['side']:prev['since']=t
                    if side and prev['cross_at'] and not prev['held'] and t-prev['since']>=120:
                        event('sampled_acceptance',{'level':name,'fixed_value':value,'side':side,'seconds':t-prev['since'],'note':'Sampled persistence; not proof of a continuous hold.'},q.get('as_of'));prev['held']=True
                if side:prev['outside']=side
                prev.update(side=side,displacement=round(p-value,4),seconds_on_side=round(t-prev['since']),buffer=buffer,last_source_time=q.get('as_of'),received=now)
                prev['points']=(prev['points']+[round(p-value,4)])[-30:];new[name]=prev
            save('interactions',new);save('last_quote',{'t':t,'source':q.get('source'),'received':now})
            db.execute('INSERT OR IGNORE INTO artifacts VALUES(?,?,?,?)',('level_interactions',q['as_of'],now,json.dumps(new)))
    def record_scenario(self,result):
        with self.db() as db:db.execute('INSERT OR IGNORE INTO artifacts VALUES(?,?,?,?)',('scenario',result['computed_at'],datetime.now(timezone.utc).timestamp(),json.dumps(result)))
    def read(self,since=0):
        with self.db() as db:
            start=db.execute('SELECT MIN(received) FROM events').fetchone()[0]
            rows=db.execute('SELECT id,received,source_time,kind,body FROM events WHERE received>=? ORDER BY id DESC LIMIT 300',(since,)).fetchall()
            interactions=db.execute("SELECT body FROM state WHERE key='interactions'").fetchone()
            opening=datetime.now(ZoneInfo('America/New_York')).replace(hour=9,minute=30,second=0,microsecond=0).timestamp()
            return {'session_open':opening,'recording_started':start,'events':[dict(id=i,received_at=r,source_time=t,kind=k,details=json.loads(b)) for i,r,t,k,b in rows],'interactions':json.loads(interactions[0]) if interactions else {},'limit':300,'note':'Persisted since recording started; no inferred pre-start history. Receipt time controls replay availability.'}
