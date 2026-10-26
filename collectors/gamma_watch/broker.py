"""Market data only, pulled through existing authenticated SSH to ANIK."""
import json, math, os, subprocess, re
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
ROOT=Path(__file__).resolve().parent
CONTRACTS=json.loads((ROOT/'contracts.json').read_text())
def number(v):
    if isinstance(v,bool):return None
    try:
        x=float(str(v).replace(',','').replace('%',''))
        return x if math.isfinite(x) else None
    except (TypeError,ValueError):return None
def dt(s):return datetime.fromisoformat(re.sub(r'(\.\d{6})\d+', r'\1', s.replace('Z','+00:00')))
def get_snapshot():
    r=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','anik','cat /c/Users/anikd/Documents/Codex/SPY-GEX-Feed/snapshot.json'],capture_output=True,timeout=12,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    if r.returncode:raise ValueError('ANIK SSH market-data pull failed')
    d=json.loads(r.stdout);now=datetime.now(timezone.utc)
    if d.get('schema')!='spy-rtd-v1' or d.get('host','').upper()!='ANIK':raise ValueError('Unexpected feed identity')
    lag=(now-dt(d['collected_at'])).total_seconds()
    if lag< -30 or lag>20 or d.get('heartbeat',0)<=0:raise ValueError('ANIK feed heartbeat stale')
    fields={}
    for rec in d['records']:
        if not rec.get('callback_seen'):continue
        fields.setdefault(rec['symbol'],{})[rec['field']]=rec
    sq=fields.get('SPY',{})
    last=sq.get('LAST',{});p=number(last.get('value'));bid=number(sq.get('BID',{}).get('value'));ask=number(sq.get('ASK',{}).get('value'))
    if not p or not bid or not ask or bid>ask or ask-bid>2:raise ValueError('Invalid SPY quote')
    if (now-dt(last['received_at'])).total_seconds()>20:raise ValueError('SPY RTD callback is stale')
    return build_feed(fields, d, now, p, bid, ask, last)

def build_feed(fields, d, now, p, bid, ask, last):
    def iv_value(f):
        rec=f.get('IMPL_VOL',{});raw=rec.get('value');v=number(raw)
        if isinstance(raw,str) and '%' in raw and v is not None:v/=100
        if v is None or not .02<=v<=3 or not rec.get('received_at'):return None
        if not -30<=(now-dt(rec['received_at'])).total_seconds()<=120:return None
        return v
    lookup={(c['expiry'],c['strike'],c['opt_type']):c for c in CONTRACTS}
    rows=[];missing=0;zero=0;direct=0;recovered=0;near_total=0;near_valid=0
    for c in CONTRACTS:
        near=abs(c['strike']-p)<=5;near_total+=int(near)
        f=fields.get(c['symbol'],{});iv=iv_value(f);ivrec=f.get('IMPL_VOL',{})
        oi=number(f.get('OPEN_INT',{}).get('value'));b=number(f.get('BID',{}).get('value'));a=number(f.get('ASK',{}).get('value'))
        gam=number(f.get('GAMMA',{}).get('value'));gam=gam if gam is not None and gam>=0 else None
        if any(x is None for x in [oi,b,a]) or oi<0 or b<0 or a<=0 or a<b:missing+=1;continue
        origin='direct';donor=None
        if iv is None:
            # Recover only ITM contracts from their matching OTM opposite side.
            otm='put' if c['strike']<p else 'call'
            mate=lookup.get((c['expiry'],c['strike'],otm)) if c['opt_type']!=otm else None
            mf=fields.get(mate['symbol'],{}) if mate else {}
            mb=number(mf.get('BID',{}).get('value'));ma=number(mf.get('ASK',{}).get('value'))
            if mb is not None and ma is not None and 0<=mb<=ma and ma>0:
                iv=iv_value(mf)
                if iv is not None:origin='paired_otm_recovery';donor=mate['symbol'];ivrec=mf['IMPL_VOL']
        if iv is None:missing+=1;continue
        direct+=int(origin=='direct');recovered+=int(origin!='direct');near_valid+=int(near)
        if oi==0:zero+=1;continue
        rows.append({**c,'implied_vol':iv,'open_interest':oi,'bid':b,'ask':a,'provider_gamma':gam,'iv_received_at':ivrec['received_at'],'iv_origin':origin,'iv_donor':donor})
    total=len(CONTRACTS)
    return {'source':'thinkorswim RTD on ANIK','collected_at':d['collected_at'],'received_at':now.isoformat(),'quote_received_at':last['received_at'],'price':p,'bid':bid,'ask':ask,'heartbeat':d['heartbeat'],'updates':d['update_count'],'entitlement':'real-time per user confirmation; exchange timestamps unavailable','expected_contracts':total,'usable_contracts':len(rows),'missing_contracts':missing,'zero_oi_contracts':zero,'coverage':(direct+recovered)/total,'direct_coverage':direct/total,'recovered_contracts':recovered,'near_spot_coverage':near_valid/near_total if near_total else 0,'near_spot_expected':near_total,'rows':rows,'expiries':sorted(set(c['expiry'] for c in CONTRACTS))}

def curves(feed):
    now=datetime.now(timezone.utc);rows=feed['rows'];spot=feed['price']
    if feed['coverage']<.9:raise ValueError('Less than 90% of subscribed contracts have usable RTD fields')
    if feed.get('near_spot_coverage',0)<.9:raise ValueError('Less than 90% coverage within $5 of spot')
    if not 745<=spot<=780:raise ValueError('Spot near/outside subscribed strike window; model withheld')
    pairs={(r['expiry'],r['strike'],r['opt_type']):r for r in rows}
    grid=np.arange(735,790.01,.25);variants=[]
    for mode in ['paired OTM IV','raw IV filtered']:
        chain=[]
        for r in rows:
            if mode=='raw IV filtered' and r.get('iv_origin')=='paired_otm_recovery':continue
            T=(dt(r['expiry']+'T20:00:00+00:00')-now).total_seconds()/(365*86400)
            if T<=0:continue
            iv=r['implied_vol']
            if mode=='paired OTM IV':
                side='put' if r['strike']<spot else 'call';other=pairs.get((r['expiry'],r['strike'],side))
                if other:iv=other['implied_vol']
            chain.append([r['strike'],T,iv,r['open_interest'],1 if r['opt_type']=='call' else -1])
        if not chain:raise ValueError('No unexpired usable contracts')
        a=np.array(chain);S=grid[:,None];K,T,vol,oi,sign=a.T
        for shock in [0,.2,.4]:
            iv=vol*(1+shock);d1=(np.log(S/K)+(.04-.012+.5*iv**2)*T)/(iv*np.sqrt(T));gamma=np.exp(-.012*T)*np.exp(-.5*d1*d1)/np.sqrt(2*np.pi)/(S*iv*np.sqrt(T));net=(gamma*oi*100*S*S*.01*sign).sum(axis=1)/1e9
            roots=[]
            for i in range(len(grid)-1):
                if net[i]*net[i+1]<0:roots.append(round(float(grid[i]-net[i]*(grid[i+1]-grid[i])/(net[i+1]-net[i])),2))
            variants.append({'name':mode,'iv_shock':shock,'values':np.round(net,4).tolist(),'roots':roots,'used':len(chain),'excluded':len(rows)-len(chain)})
    # Provider GAMMA is rounded, so display as a separate diagnostic, not a precision reference.
    pgex=sum(r['provider_gamma']*r['open_interest']*100*spot*spot*.01*(1 if r['opt_type']=='call' else -1) for r in rows if r['provider_gamma'] is not None)/1e9 if all(r['provider_gamma'] is not None for r in rows) else None
    return {'computed_at':now.isoformat(),'chain_as_of':feed['collected_at'],'snapshot_date':now.date().isoformat(),'expiries':feed['expiries'],'rows':len(rows),'expected_rows':len(CONTRACTS),'grid':grid.tolist(),'variants':variants,'live_inputs':True,'spot':spot,'provider_gamma_gex_b':round(pgex,3) if pgex is not None else None,'scope':'PARTIAL chain: four selected expiries, strikes $735–790. Neither full SPY nor SPX/ES inventory. Missing wings can change signs and roots.','assumptions':'Paired curve may recover ITM IV from matching OTM contracts; raw curve excludes recovered rows. Streaming RTD IV and latest available OI; user confirms real-time entitlement. Times are collector receipt times, not exchange timestamps. OI date not supplied by RTD. Calls positive / puts negative is an assumption. 4% rate, 1.2% dividend yield, 16:00 ET expiry approximation. No trade-flow or actual dealer inventory data.'}
