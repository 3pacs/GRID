from datetime import datetime,timedelta,timezone
import json
import pytest
from ingestion.realtime.research_intervals import Session
from ingestion.realtime.native_volume_worker import run_once,request_plan,owner_lock
T=datetime(2026,11,10,15,tzinfo=timezone.utc)
class Calendar:
    def __call__(self,key):return Session(key,T-timedelta(minutes=30),T+timedelta(hours=6),'fixture-v1')
    def prior_sessions(self,key,count):
        return [Session(str(i),T-timedelta(days=i,minutes=30),T-timedelta(days=i,minutes=29),'fixture-v1') for i in range(10,0,-1)]
class Client:
    _max_retries=0
    def __init__(self):self.calls=[]
    def price_history(self,symbol,**kw):
        self.calls.append(kw)
        times=[T-timedelta(days=i,minutes=30) for i in range(10,0,-1)]
        times += [T-timedelta(minutes=30)+timedelta(minutes=i) for i in range(40)]
        return {'symbol':symbol,'candles':[dict(datetime=int(t.timestamp()*1000),open=2,high=3,low=1,close=2,volume=10) for t in times if kw['startDate']<=int(t.timestamp()*1000)<=kw['endDate']]}
@pytest.fixture
def root(tmp_path):tmp_path.chmod(0o700);return tmp_path
def call(c,root,now=T+timedelta(seconds=5),**kw):
    return run_once(c,root=root,calendar=Calendar(),session_key='today',clock=lambda:now,**kw)
def test_bootstrap_and_incremental(root):
    c=Client();r=call(c,root,initialize_store=True);assert r['mode']=='bootstrap'
    assert c.calls[0]['startDate']==int((T-timedelta(days=10,minutes=30)).timestamp()*1000)
    r=call(c,root,T+timedelta(minutes=1,seconds=5));assert r['mode']=='incremental'
    assert c.calls[1]['startDate']==int((T-timedelta(minutes=2)).timestamp()*1000)
    assert r['live'] is False

def test_before_settlement_and_duplicate(root):
    c=Client();call(c,root,initialize_store=True)
    assert call(c,root,T+timedelta(minutes=1,seconds=4))['status']=='idle'
    assert len(c.calls)==1

def test_no_overlap(root):
    c=Client()
    with owner_lock(root):
        with pytest.raises(RuntimeError,match='already active'):call(c,root,initialize_store=True)
    assert not c.calls

def test_explicit_init(root):
    with pytest.raises(FileNotFoundError):call(Client(),root)
    assert not (root/'minutes.sqlite').exists()

def test_gap_refused(root):
    c=Client();call(c,root,initialize_store=True)
    with pytest.raises(ValueError,match='gap'):call(c,root,T+timedelta(minutes=4,seconds=5))
    assert len(c.calls)==1

def test_failure_not_retried(root):
    class Bad(Client):
        def price_history(self,*a,**k):self.calls.append(k);raise ValueError('secret')
    c=Bad()
    with pytest.raises(RuntimeError,match='inspect retained'):call(c,root,initialize_store=True)
    with pytest.raises(ValueError,match='reconciliation'):call(c,root)
    assert len(c.calls)==1

def test_initialization_privacy(root):
    (root/'receipts').mkdir(mode=0o755)
    with pytest.raises(ValueError,match='private'):call(Client(),root,initialize_store=True)

def test_lock_symlink(root):
    (root/'writer.lock').symlink_to(root/'other')
    with pytest.raises(OSError):call(Client(),root,initialize_store=True)

def test_bad_budget(root):
    c=Client()
    with pytest.raises(ValueError,match='budget'):call(c,root,disk_budget_bytes=1)
    assert not c.calls

def test_early_close_settled():
    class Early(Calendar):
        def __call__(self,key):return Session(key,T-timedelta(hours=1),T,'fixture-v1')
    p=request_plan(Early(),'today',T+timedelta(seconds=5))
    assert p['window_end']==T

def test_old_session_no_request():
    assert request_plan(Calendar(),'today',T+timedelta(days=1)) is None

def test_ten_sessions_required():
    class Bad(Calendar):
        def prior_sessions(self,*args):return []
    with pytest.raises(ValueError,match='ten'):request_plan(Bad(),'today',T)

def test_partial_bootstrap_not_promoted(root):
    class Empty(Client):
        def price_history(self,symbol,**kw):self.calls.append(kw);return {'symbol':symbol,'candles':[]}
    c=Empty()
    with pytest.raises(RuntimeError,match='coverage incomplete'):call(c,root,initialize_store=True)
    with pytest.raises(ValueError,match='reconciliation'):call(c,root)
    assert len(c.calls)==1
