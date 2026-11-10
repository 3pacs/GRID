"""Real source methods, isolated fake config, no credentials or network."""
import errno
import fcntl
import importlib.util
import os
from pathlib import Path
import sys
import time
from types import ModuleType, SimpleNamespace
import pytest

@pytest.fixture
def sm(monkeypatch):
    config = ModuleType('config')
    config.log = SimpleNamespace(warning=lambda *a,**k:None,info=lambda *a,**k:None)
    config.settings = SimpleNamespace(SCHWAB_TOKEN_FILE='/unused/token')
    name = '_schwab_bounded_test_module'
    spec = importlib.util.spec_from_file_location(name,Path(__file__).parents[1]/'ingestion/schwab_market.py')
    mod = importlib.util.module_from_spec(spec)
    with monkeypatch.context() as context:
        context.setitem(sys.modules,'config',config)
        context.setitem(sys.modules,name,mod)
        spec.loader.exec_module(mod)
    monkeypatch.setattr(mod,'load_credentials',lambda *a:('fake-key','fake-secret'))
    return mod

@pytest.mark.parametrize('value',[0,-1,float('inf'),float('nan'),True,'3'])
def test_invalid_timeout(sm,value):
    with pytest.raises(ValueError):sm.SchwabMarketClient(token_lock_timeout=value)
@pytest.mark.parametrize('value',[0,1,None,'false'])
def test_invalid_reauth(sm,value):
    with pytest.raises(ValueError):sm.SchwabMarketClient(reauth_on_401=value)
def test_defaults(sm):
    c=sm.SchwabMarketClient()
    assert c._token_lock_timeout is None and c._reauth_on_401 is True
    assert c._lock().timeout is None

def test_lock_timeout_closes_fd(sm,tmp_path,monkeypatch):
    tmp_path.chmod(0o700);token=tmp_path/'tokens.json'
    holder=sm._TokenFileLock(token);holder.__enter__()
    opened=[];real_open=os.open
    def tracked(*a,**kw):
        fd=real_open(*a,**kw);opened.append(fd);return fd
    monkeypatch.setattr(sm.os,'open',tracked)
    contender=sm._TokenFileLock(token,timeout=.03)
    began=time.monotonic()
    try:
        with pytest.raises(sm.SchwabTransientError,match='lock timed out'):contender.__enter__()
        assert time.monotonic()-began < .5
        assert contender._fd is None
        with pytest.raises(OSError):os.fstat(opened[-1])
    finally:holder.__exit__()
    with sm._TokenFileLock(token,timeout=.03):pass

def test_lock_noncontention_error_closes(sm,tmp_path,monkeypatch):
    tmp_path.chmod(0o700)
    def broken(*a):raise OSError(errno.EBADF,'fake')
    monkeypatch.setattr(sm.fcntl,'flock',broken)
    lock=sm._TokenFileLock(tmp_path/'t',timeout=.03)
    with pytest.raises(OSError):lock.__enter__()
    assert lock._fd is None

class Response:
    headers={}
    def __init__(self,status):self.status_code=status
    def json(self):return {'ok':True}
class Session:
    def __init__(self):self.calls=0
    def get(self,*a,**k):self.calls+=1;return Response(401 if self.calls==1 else 200)

def test_disabled_401_no_replay(sm,monkeypatch):
    session=Session();c=sm.SchwabMarketClient(session=session,max_retries=0,reauth_on_401=False,token_lock_timeout=3)
    access=[]
    monkeypatch.setattr(c,'_access_token',lambda stale_token=None:access.append(stale_token) or 'fake-token')
    with pytest.raises(sm.SchwabError,match='HTTP 401'):c.price_history('SPY')
    assert session.calls==1 and access==[None] and c._lock().timeout==3

def test_default_401_still_replays(sm,monkeypatch):
    session=Session();c=sm.SchwabMarketClient(session=session,max_retries=0)
    access=[]
    monkeypatch.setattr(c,'_access_token',lambda stale_token=None:access.append(stale_token) or 'fake-token')
    assert c.price_history('SPY')=={'ok':True}
    assert session.calls==2 and access==[None,'fake-token']

def test_no_late_acquisition_after_deadline(sm,tmp_path,monkeypatch):
    tmp_path.chmod(0o700);attempts=[];ticks=iter([0,.5,2])
    monkeypatch.setattr(sm.time,'monotonic',lambda:next(ticks))
    monkeypatch.setattr(sm.time,'sleep',lambda n:None)
    def flock(*args):
        attempts.append(args)
        if len(attempts)==1:raise BlockingIOError(errno.EAGAIN,'busy')
    monkeypatch.setattr(sm.fcntl,'flock',flock)
    with pytest.raises(sm.SchwabTransientError):sm._TokenFileLock(tmp_path/'t',timeout=1).__enter__()
    assert len(attempts)==1
