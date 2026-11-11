import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
from ingestion.realtime.minute_retention import initialize
from ingestion.realtime.native_volume_cache import fetch_and_retain, retain_receipt

T=datetime(2026,11,10,15,tzinfo=timezone.utc)
class Client:
    _max_retries=0
    calls=0
    def price_history(self,symbol,**kw):
        self.calls+=1
        return {'symbol':symbol,'candles':[dict(datetime=int((T-timedelta(minutes=2)).timestamp()*1000),open=2,high=3,low=1,close=2,volume=10)]}
@pytest.fixture
def setup(tmp_path):
    tmp_path.chmod(0o700);db=tmp_path/'minutes.db';initialize(db)
    return dict(store=db,receipts_dir=tmp_path)
def run(c,setup,**kw):
    return fetch_and_retain(c,symbol='SPY',start=T-timedelta(minutes=5),end=T,clock=lambda:T,**setup,**kw)
def test_roundtrip(setup):
    c=Client();r=run(c,setup);assert c.calls==1 and r['inserted']==1
    raw=json.loads(Path(r['receipt_file']).read_text());assert retain_receipt(raw,**setup)['inserted']==0
    assert r['live'] is False

def test_invalid_source_no_append(setup):
    class Bad(Client):
        def price_history(self,*a,**k):return {'symbol':'WRONG','candles':[]}
    with pytest.raises(ValueError):run(Bad(),setup)
    assert len(list(Path(setup['receipts_dir']).glob('unvalidated-*.json'))) == 1
    assert not list(Path(setup['receipts_dir']).glob('[0-9a-f]' * 64 + '.json'))

def test_client_retry_refused(setup):
    c=Client();c._max_retries=4
    with pytest.raises(ValueError):run(c,setup)
    assert c.calls==0

def test_network_error_sanitized(setup):
    class Bad(Client):
        def price_history(self,*a,**k):raise RuntimeError('secret')
    with pytest.raises(RuntimeError,match='no automatic retry') as e:run(Bad(),setup)
    assert 'secret' not in str(e.value)

def test_future_refused(setup):
    c=Client()
    with pytest.raises(ValueError):fetch_and_retain(c,symbol='SPY',start=T,end=T+timedelta(minutes=1),clock=lambda:T,**setup)
    assert c.calls==0

def test_nonprivate_refused(setup):
    Path(setup['store']).chmod(0o644)
    with pytest.raises(ValueError):run(Client(),setup)

def test_outside_refused(setup):
    class Bad(Client):
        def price_history(self,*a,**k):
            p=super().price_history(*a,**k);p['candles'][0]['datetime']-=86400000;return p
    with pytest.raises(ValueError):run(Bad(),setup)

def test_forming_excluded(setup):
    class Forming(Client):
        def price_history(self,*a,**k):
            p=super().price_history(*a,**k);p['candles'][0]['datetime']=int(T.timestamp()*1000);return p
    r=run(Forming(),setup);assert r['excluded_forming']==1 and r['inserted']==0

@pytest.mark.parametrize("policy", [False, True, None, 0.0, "0"])
def test_unknown_retry_policy_refused(setup, policy):
    c=Client();c._max_retries=policy
    with pytest.raises(ValueError):run(c,setup)
    assert c.calls==0

def test_absent_retry_policy_refused(setup):
    class Unknown:
        calls=0
        def price_history(self,*a,**k):
            self.calls+=1
            raise AssertionError("must not call")
    c=Unknown()
    with pytest.raises(ValueError):run(c,setup)
    assert c.calls==0
