"""Illustrative long-option scenarios; delayed quotes, no execution claims."""
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def price(s,k,t,iv,side,r=.04,q=.012):
    if t<=0:return max(0,s-k) if side=='C' else max(0,k-s)
    n=lambda x:(1+math.erf(x/math.sqrt(2)))/2
    d1=(math.log(s/k)+(r-q+iv*iv/2)*t)/(iv*math.sqrt(t));d2=d1-iv*math.sqrt(t)
    return s*math.exp(-q*t)*n(d1)-k*math.exp(-r*t)*n(d2) if side=='C' else k*math.exp(-r*t)*n(-d2)-s*math.exp(-q*t)*n(-d1)

def grid(row,contract,shock=0,now=None):
    now=now or datetime.now(timezone.utc)
    out={'symbol':row['symbol'],'contract':contract,'source':row.get('source'),'received_at':row.get('received_at'),
      'provider_timestamp_raw':row.get('provider_timestamp_raw'),'quote_age':'unverified; delayed source',
      'computed_at':now.isoformat(),'status':'unavailable'}
    vals=[row.get('price')]+[contract.get(k) for k in ('strike','iv','bid','ask')]
    if not all(isinstance(v,(int,float)) and math.isfinite(v) for v in vals):return {**out,'reason':'Missing or invalid spot, IV or bid/ask.'}
    s,k,iv,bid,ask=vals
    if not(s>0 and k>0 and .02<=iv<=3 and 0<bid<=ask):return {**out,'reason':'Invalid IV or two-sided quote.'}
    expiry=datetime.fromisoformat(contract['expiry']+'T16:00:00').replace(tzinfo=ZoneInfo('America/New_York'))
    t=(expiry-now).total_seconds()
    if t<=0:return {**out,'reason':'Contract expired under the 16:00 ET approximation.'}
    received=datetime.fromisoformat(row['received_at'])
    if not -5<=(now-received).total_seconds()<=600:return {**out,'reason':'Collection stale; scenarios withheld.'}
    spread=ask-bid;cells=[]
    for minutes in (0,5,15,30):
        for move in (-1,-.5,0,.5,1):
            value=price(s+move,k,max(0,t-minutes*60)/(365*86400),iv*(1+shock),contract['side'])
            exit_value=max(0,value-spread/2) if t>minutes*60 else value
            cells.append({'minutes':minutes,'move':move,'value':round(value,4),'pnl':round(100*(exit_value-ask),2),'at_expiry':t<=minutes*60})
    baseline=price(s,k,t/(365*86400),iv,contract['side'])
    return {**out,'status':'illustrative_delayed','spot':s,'entry_ask':ask,'spread_cost_dollars':round(spread*100,2),'model_minus_mid_dollars':round(100*(baseline-(bid+ask)/2),2),'iv_shift':shock,'cells':cells,
      'assumptions':'One long standard 100-share option. Entry at delayed ask; exit at European Black-Scholes value minus half the current dollar spread, floored at zero; intrinsic at expiry. IV shifted immediately and held fixed; rate 4%, dividend yield 1.2% assumed for every ETF. 16:00 ET expiry approximation. No commissions, slippage beyond assumed spread, American early exercise or discrete dividends. Quote age unverified: research only, not executable P&L.'}
