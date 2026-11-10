"""Explicit single-owner local native-volume invocation. No schedule or credentials.

The caller provisions a trusted owner-private root. Lifetime flock protects both
request and checkpoint. An interrupted/failed request requires explicit operator
reconciliation; this worker never retries an uncertain invocation automatically.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from ingestion.realtime.minute_retention import initialize, sqlite_loader
from ingestion.realtime.native_minutes import select_as_of
from ingestion.realtime.native_volume_cache import fetch_and_retain, _private, MAX_BYTES
from ingestion.realtime.research_intervals import _utc

SCHEMA = 'grid.native-volume-worker.v1'

@contextmanager
def owner_lock(root):
    """Trusted private ancestor required; lock is held throughout one invocation."""
    root = _private(root, directory=True)
    fd = os.open(root / 'writer.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        s = os.fstat(fd)
        if not stat.S_ISREG(s.st_mode) or s.st_uid != os.geteuid() or stat.S_IMODE(s.st_mode) & 0o077 or s.st_nlink != 1:
            raise ValueError('unsafe writer lock')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('native volume writer already active') from None
        yield root
    finally:
        os.close(fd)


def _read_checkpoint(path):
    try:
        _private(path)
    except FileNotFoundError:
        return None
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as f:
        raw = f.read(16385)
    if len(raw) > 16384:
        raise ValueError('oversized checkpoint')
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get('schema') != SCHEMA:
        raise ValueError('invalid checkpoint')
    return value


def _checkpoint(root, value):
    target = root / 'checkpoint.json'
    if target.exists() or target.is_symlink():
        _private(target)
    raw = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    if len(raw) > 16384:
        raise ValueError('oversized checkpoint')
    fd, temp = tempfile.mkstemp(prefix='.checkpoint-', dir=root)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(raw); f.flush(); os.fsync(f.fileno())
        os.replace(temp, target)
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally:
        if os.path.exists(temp): os.unlink(temp)


def request_plan(calendar, session_key, now, previous=None):
    now = _utc(now)
    session = calendar(session_key)
    opens, closes = _utc(session.opens), _utc(session.closes)
    settled = (now-timedelta(seconds=5)).replace(second=0, microsecond=0)
    boundary = min(settled, closes)
    if boundary <= opens:
        return None
    if now < opens or now > closes+timedelta(minutes=5):
        return None  # no historical invocation posing as a fresh producer
    if previous is not None:
        if previous.get('status') != 'complete':
            raise ValueError('prior invocation requires reconciliation; no automatic retry')
        if previous.get('session') == session_key:
            last = _utc(datetime.fromisoformat(previous['window_end']))
            if last > boundary:
                raise ValueError('clock or checkpoint moved backwards')
            if last == boundary: return None
            if boundary-last > timedelta(minutes=3):
                raise ValueError('incremental gap exceeds overlap; explicit recovery required')
            return {'mode':'incremental', 'start':max(opens,boundary-timedelta(minutes=3)), 'end':boundary-timedelta(milliseconds=1), 'window_end':boundary}
    prior = calendar.prior_sessions(session_key, 10)
    if len(prior) != 10 or any(_utc(s.closes) > opens for s in prior):
        raise ValueError('exact ten preceding sessions required')
    start = _utc(prior[0].opens)
    if not timedelta(0) < boundary-start <= timedelta(days=31):
        raise ValueError('bootstrap outside bounded range')
    return {'mode':'bootstrap', 'start':start, 'end':boundary-timedelta(milliseconds=1), 'window_end':boundary}


def run_once(client, *, root, calendar, session_key, symbol='SPY', initialize_store=False,
             clock=None, disk_budget_bytes=128_000_000):
    """One provider call maximum, explicit store initialization, no retries."""
    clock = clock or (lambda: datetime.now(timezone.utc))
    if not isinstance(symbol,str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,11}',symbol):
        raise ValueError('invalid symbol')
    if type(disk_budget_bytes) is not int or disk_budget_bytes < 2*MAX_BYTES:
        raise ValueError('insufficient explicit disk budget')
    with owner_lock(root) as root:
        prior = _read_checkpoint(root/'checkpoint.json')
        if prior and prior.get('symbol') != symbol:
            raise ValueError('checkpoint symbol mismatch')
        now = _utc(clock())
        plan = request_plan(calendar, session_key, now, prior)
        if plan is None: return {'status':'idle', 'requested':False}
        store, receipts = root/'minutes.sqlite', root/'receipts'
        if initialize_store:
            try: receipts.mkdir(mode=0o700)
            except FileExistsError: pass
            _private(receipts,directory=True)
            initialize(store)
        _private(store); _private(receipts,directory=True)
        files = list(receipts.iterdir())
        if len(files)>10000: raise ValueError('receipt inventory bound exceeded')
        used = store.stat().st_size
        for p in files:
            _private(p); used += p.stat().st_size
        # Reserve receipt plus conservative SQLite expansion for this request.
        if used+2*MAX_BYTES>disk_budget_bytes: raise ValueError('native cache disk budget exhausted')
        state = dict(schema=SCHEMA, symbol=symbol, session=session_key, status='in_progress',
                     mode=plan['mode'], window_end=plan['window_end'].isoformat(), started_at=now.isoformat())
        _checkpoint(root,state)
        try:
            result = fetch_and_retain(client,symbol=symbol,start=plan['start'],end=plan['end'],store=store,receipts_dir=receipts,clock=clock)
        except Exception:
            state['status']='failed'; _checkpoint(root,state)
            raise RuntimeError('native volume invocation failed; inspect retained receipts before recovery') from None
        # A successful HTTP response is not bootstrap completeness. Do not strand
        # missing baseline/session minutes behind an incremental checkpoint.
        try:
            cutoff = _utc(datetime.fromisoformat(result['received_at']))
            current = calendar(session_key)
            sessions = list(calendar.prior_sessions(session_key,10)) if plan['mode']=='bootstrap' else []
            sessions.append(current)
            loader = sqlite_loader(store)
            for session in sessions:
                begin = _utc(session.opens)
                finish = min(_utc(session.closes),plan['window_end'])
                if plan['mode']=='incremental':begin=max(begin,plan['start'])
                rows = loader(symbol,'schwab_pricehistory',session,cutoff)
                selected = select_as_of(rows,symbol=symbol,source='schwab_pricehistory',price_basis='provider_unspecified',as_of=cutoff,opens=begin,closes=finish)
                expected = {begin+timedelta(minutes=i) for i in range(int((finish-begin).total_seconds()//60))}
                if {b.ts for b in selected} != expected:
                    raise ValueError('incomplete retained coverage')
        except Exception:
            state['status']='coverage_failed'; _checkpoint(root,state)
            raise RuntimeError('native volume coverage incomplete; explicit recovery required') from None
        state.update(status='complete',receipt_sha256=result['receipt_sha256'],received_at=result['received_at'],closed_minutes=result['closed_minutes'])
        _checkpoint(root,state)
        return {**state,'requested':True,'inserted':result['inserted'],'live':False}
