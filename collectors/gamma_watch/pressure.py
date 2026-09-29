"""Observed price movement, explicitly not signed volume or dealer flow."""
from datetime import datetime, timezone

def summarize(ticks, now):
    rows=[]
    ticks=sorted(ticks,key=lambda x:x['t'])
    for minutes in (1,5,15):
        start=now-minutes*60
        pts=[p for p in ticks if start-10<=p['t']<=now]
        before=[p for p in pts if p['t']<=start]
        if before:pts=[before[-1]]+[p for p in pts if p['t']>start]
        status='warming_up';change=None
        if pts and now-pts[-1]['t']>20:status='stale'
        elif len(pts)>1 and pts[0]['t']<=start+10:
            if max(b['t']-a['t'] for a,b in zip(pts,pts[1:]))>20:status='gap'
            else:status='available';change=round(pts[-1]['c']-pts[0]['c'],4)
        rows.append(dict(minutes=minutes,status=status,price_change=change,samples=len(pts)))
    return dict(as_of=datetime.fromtimestamp(now,timezone.utc).isoformat(),
                source='thinkorswim RTD sampled last prices; collector timestamps',
                signed_share_volume=None,options_delta_flow=None,flow_status='unavailable',
                reason='No verified execution-level feed with sizes and contemporaneous quotes connected.',
                windows=rows)
