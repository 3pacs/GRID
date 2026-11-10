from datetime import datetime,timedelta,timezone
import pytest
import scripts.native_volume_once as cli
from ingestion.realtime.research_intervals import Session
T=datetime(2026,11,10,15,tzinfo=timezone.utc)
class Calendar:
    def __call__(self,key):return Session(key,T-timedelta(minutes=30),T+timedelta(hours=6),'fixture')
    def prior_sessions(self,key,n):return [Session(str(i),T-timedelta(days=i,minutes=30),T-timedelta(days=i,minutes=29),'fixture') for i in range(10,0,-1)]
@pytest.fixture
def root(tmp_path):tmp_path.chmod(0o700);return tmp_path

def forbidden():raise AssertionError('client must not be constructed')
def test_fetch_required(root):
    assert cli.main(['--root',str(root)],client_factory=forbidden)==2

def test_closed_market(root):
    assert cli.main(['--fetch','--root',str(root)],calendar_factory=Calendar,client_factory=forbidden,clock=lambda:T+timedelta(days=1))==0

def test_holiday(root):
    class Closed(Calendar):
        def __call__(self,key):raise ValueError('not a session')
    assert cli.main(['--fetch','--root',str(root)],calendar_factory=Closed,client_factory=forbidden,clock=lambda:T)==1

def test_uninitialized_no_client(root):
    assert cli.main(['--fetch','--root',str(root)],calendar_factory=Calendar,client_factory=forbidden,clock=lambda:T)==1

def test_explicit_flow_composes(root,monkeypatch):
    calls=[]
    def worker(client,**kw):
        calls.append('worker');assert kw['initialize_store'];return {'requested':True,'receipt_sha256':'a'*64}
    monkeypatch.setattr(cli,'run_once',worker)
    monkeypatch.setattr(cli,'sqlite_loader',lambda p:'loader')
    monkeypatch.setattr(cli,'build_snapshot',lambda *a,**kw:{'payload':{'status':'ready'}})
    monkeypatch.setattr(cli,'write_snapshot',lambda *a,**kw:{'sha256':'b'*64})
    assert cli.main(['--fetch','--initialize','--root',str(root)],calendar_factory=Calendar,client_factory=forbidden,clock=lambda:T)==0
    assert calls==['worker']

def test_unsafe_root_no_client(root):
    root.chmod(0o755)
    assert cli.main(['--fetch','--root',str(root)],calendar_factory=Calendar,client_factory=forbidden,clock=lambda:T)==1

def test_lazy_policy():
    class Bad:_max_retries=False
    with pytest.raises(ValueError):cli.LazyClient(Bad).price_history('SPY')

def test_no_blind_recovery(root):
    with pytest.raises(SystemExit):cli.main(['--fetch','--root',str(root),'--recover'])

def test_concurrent_cli_refuses(root):
    with cli.invocation_lock(root):
        assert cli.main(['--fetch','--root',str(root)],calendar_factory=Calendar,client_factory=forbidden,clock=lambda:T)==1

def test_manifest_flags_required(root):
    assert cli.main(['--fetch','--root',str(root)],client_factory=forbidden,clock=lambda:T)==1

def test_manifest_flags_composed(root,monkeypatch):
    seen=[]
    def calendar(path,digest):seen.append((str(path),digest));return Calendar()
    monkeypatch.setattr(cli,'default_calendar',calendar)
    assert cli.main(['--fetch','--root',str(root),'--calendar-manifest','/reviewed/cal.json','--calendar-sha256','a'*64],client_factory=forbidden,clock=lambda:T+timedelta(days=1))==0
    assert seen==[('/reviewed/cal.json','a'*64)]
