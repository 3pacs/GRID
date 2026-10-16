"""Portable stdlib-only repository tests; all worker/HTTP/DB boundaries fake.

Historical53 artifact-layout methods remain exact in offline provenance; this
file needs only the declared product paths in a normal repository tree.
"""
from pathlib import Path
import ast,builtins,contextlib,copy,hashlib,io,json,runpy,sys,types,unittest
from datetime import date,datetime,timezone
from decimal import Decimal
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
SCHED=ROOT/'ingestion/smart_scheduler.py'
OPER=ROOT/'scripts/hermes_operator.py'
def front(text=None):
    raw=SCHED.read_text() if text is None else text
    ns={'__name__':'_test_selected_front'}
    exec(compile(raw[:raw.index('\nimport calendar\n')],str(SCHED),'exec'),ns)
    return types.SimpleNamespace(**ns)
def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()
def digest(raw):return hashlib.sha256(raw).hexdigest()
HEADER='Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market'
GOOD=(HEADER+'\n20261013|AAPL|10|1|100|B,Q,N\n20261013|MSFT|20|2|200|B,Q,N\n2\n').encode()

def primitives(driver):
    d=driver
    class Clock:
        def __init__(self):self.now=10.0
        def __call__(self):return self.now
        def utc(self):return datetime(2026,10,15,0,0,tzinfo=timezone.utc)
        def wait(self,n):self.now+=n
    class Cursor:
        def __init__(self,raw):self.raw=raw;self.connection=raw;self.data=[];self.rowcount=-1;self.closed=False
        def execute(self,sql,args):
            v=self.raw.driver;v.sql.append((sql,args));self.data=[]
            if v.execute_error:raise RuntimeError('SYNTHETIC_PRIVATE_DSN_MUST_NOT_EXPORT')
            if sql==d.ROLE_SQL:self.data=[v.role_override or (self.raw.role,'syntheticdb',False,False,False,False)]
            elif sql==d.SOURCE_SQL:self.data=[(7,'FINRA_SHORT_VOLUME')]
            elif sql==d.TABLE_SQL:self.data=[(123 if args[0]=='raw_series' else 124,'r',False,False)]
            elif sql in (d.TRIGGER_SQL,d.UNIQUE_SQL):self.data=[]
            elif sql==d.READ_SQL:
                if self.raw.read and v.fail_after and v.committed:raise RuntimeError('AFTER_READ_FAILED')
                self.data=list(v.stored)
            elif sql==d.LOCK_SQL:pass
            elif sql==d.INSERT_SQL:
                self.rowcount=v.count
                sid,source_id,obs,value,payload=args
                self.raw.pending.append((sid,obs,Decimal(str(value)),'SUCCESS',datetime(2026,10,15,tzinfo=timezone.utc),json.loads(payload),source_id))
            else:raise AssertionError('UNEXPECTED_SQL')
        def fetchmany(self,n):out=self.data[:n];self.data=self.data[n:];return out
        def close(self):
            self.closed=True
            if self.raw.driver.cursor_close_error:raise RuntimeError('CURSOR_CLOSE_FAILED')
    class Raw:
        def __init__(self,driver,read):self.driver=driver;self.read=read;self.role='read_role' if read else 'write_role';self.pending=[];self.closed=False;self.rolled=False
        def set_session(self,**kw):
            assert kw=={'readonly':self.read,'autocommit':False,'isolation_level':'REPEATABLE READ'}
            if self.driver.enter_error:raise RuntimeError('POST_CONNECT_INIT_FAILED')
        def cursor(self):
            self.cur=Cursor(self);return self.cur
        def commit(self):
            self.driver.stored.extend(self.pending);self.pending=[];self.driver.committed=True
            if self.driver.commit_error:raise RuntimeError('SYNTHETIC_PRIVATE_DSN_MUST_NOT_EXPORT')
        def rollback(self):self.pending=[];self.rolled=True
        def close(self):self.closed=True
    class Driver:
        def __init__(self):
            self.count=1;self.commit_error=False;self.execute_error=False;self.enter_error=False;self.cursor_close_error=False;self.fail_after=False
            self.stored=[];self.raws=[];self.sql=[];self.committed=False;self.role_override=None
        def connect(self,dsn,**kw):
            assert kw['connect_timeout']==5 and 'statement_timeout=5000' in kw['options']
            r=Raw(self,dsn=='SYNTHETIC_READ');self.raws.append(r);return r
    class Response:
        def __init__(self,data,status=200,clock=None,late=False):self.data=data;self.status=status;self.encoding=None;self.closed=False;self.clock=clock;self.late=late
        def read(self,n):
            if self.late:self.clock.now=1000
            out=self.data[:n];self.data=self.data[n:];return out
        def close(self):self.closed=True
    class Exchange:
        def __init__(self,data=GOOD,status=200,clock=None,late=False):self.data=data;self.status=status;self.calls=[];self.clock=clock;self.late=late
        def __call__(self,url,deadline,cap):
            self.calls.append((url,deadline,cap));self.response=Response(self.data,self.status,self.clock,self.late);return self.response
    class ParentHandle:
        def __init__(self,owner,receipt,code=0):self.owner=owner;self.identity=(self,1);self.output=d.canonical(receipt).encode();self.code=code;self.closed=False
        def poll(self):return self.code
        def reap(self,code):return code==self.code
        def signal(self,kind):self.code=-15;return True
        def capture_complete(self):
            if not hasattr(self,'streams'):self.streams=[io.BytesIO(self.output),io.BytesIO()];self.eofs=[False,False]
            for index,stream in enumerate(self.streams):
                if not self.eofs[index] and stream.read(65536)==b'':self.eofs[index]=True
            return all(self.eofs)
        def close(self):self.closed=True;return True
    return types.SimpleNamespace(**locals())

class WorkflowRuntime:
    """Only process/resource/transport/DB boundaries are fake."""
    injected=True
    def __init__(self,driver, *, faults=()):
        self.driver=driver;self.p=primitives(driver);self.c=self.p.Clock()
        self.clock=self.c;self.utc=self.c.utc;self.wait=self.c.wait
        self.faults=list(faults);self.events=[];self.saved=[];self.children=[]
        self.closed=[];self.spent=False;self.launches=0;self.db_drivers=[]
        self.hook=None
    def consume_workflow(self,packet,workflow_id):
        self.events.append(('consume',workflow_id))
        if self.spent:raise self.driver.Refused('ALREADY_CONSUMED')
        self.spent=True
        if self.hook:self.hook('consume',packet)
        return True
    def preflight(self,packet):
        self.events.append(('preflight',packet['call']['call_id']))
        if self.hook:self.hook('preflight',packet)
        return object()
    def launch(self,context,bound):
        self.launches+=1;packet=json.loads(bound);self.events.append(('launch',packet['call']['call_id']))
        fault=self.faults[self.launches-1] if self.launches<=len(self.faults) else 'ok'
        db=self.p.Driver();self.db_drivers.append(db)
        if fault=='commit_unknown':db.commit_error=True
        if fault=='db_unavailable':db.enter_error=True
        exchange=self.p.Exchange(status=503 if fault=='503' else 200,clock=self.c)
        if fault in ('timeout','connection'):
            def exchange(*a):
                if fault=='timeout':raise TimeoutError('SYNTHETIC_SECRET')
                raise ConnectionError('SYNTHETIC_SECRET')
        if fault=='malformed_body':exchange=self.p.Exchange(data=b'bad',clock=self.c)
        if fault=='empty':exchange=self.p.Exchange(data=(self.p.GOOD.split(b'\n')[0]+b'\n0\n'),clock=self.c)
        child=self.driver.execute_worker(packet,producer=self.driver.load_bound_producer(
            (ROOT/'ingestion/altdata/finra_short_volume.py').read_bytes()),backend=self.driver.DBAPIBackend(
                db,'SYNTHETIC_WRITE','SYNTHETIC_READ'),exchange=exchange,
            clock=self.clock,utc=self.utc)
        if fault=='ack_without_effects':
            child['effects'].update(db_started=False,write_bearing=False,commit_attempted=False)
        handle=self.p.ParentHandle(context,child)
        if fault=='malformed_receipt':handle.output=b'{bad'
        if fault=='missing_eof':handle.capture_complete=lambda:False
        if fault=='reap_unknown':handle.reap=lambda code:False
        if fault=='wrong_child':
            child['binding']['call_id']='00000000-0000-0000-0000-000000000000'
            handle.output=self.driver.canonical(child).encode()
        if fault=='close_failed':
            def fail_close():raise self.driver.Refused('CLOSE_FAILED')
            handle.close=fail_close
        if self.hook:self.hook('launch',packet)
        self.children.append(child);return handle
    def persist(self,context,call_id,receipt):
        self.events.append(('persist',call_id));self.saved.append(copy.deepcopy(receipt))
        if self.hook:self.hook('persist',receipt)
    def close(self,context):
        self.events.append(('close',));self.closed.append(context)
        if self.hook:self.hook('close',context)

def admission(driver,driver_path,producer_path,driver_sha):
    call=driver.asdict(driver.Call('12345678-1234-4234-8234-123456789abc',
        '2026-10-15','2026-10-15T23:59:59Z',10.,7))
    for key in ('call_id','day','cutoff_utc','t0'):call[key]=None
    import hashlib
    empty=hashlib.sha256(driver.canonical([]).encode()).hexdigest()
    return {'schema':'finra-selected-workflow-admission-v2','status':'ADMITTED',
      'workflow_id':'12345678-1234-4234-8234-123456789abc',
      'issued_utc':'2026-10-15T00:00:00Z','expires_utc':'2026-10-22T00:00:00Z',
      'call':call,'caps':driver.asdict(driver.Caps()),'workflow_caps':dict(driver.WORKFLOW_CAPS),
      'runtime':{'python':'/synthetic/python','python_sha256':'1'*64,
         'driver':str(driver_path),'driver_sha256':driver_sha,'producer':str(producer_path),
         'driver_package':{},'cgroup':{},'receipt_root':{}},
      'db_plan':{'database':'syntheticdb','read_role':'read_role','write_role':'write_role',
         'table_oid':123,'catalog_oid':124,'trigger_sha256':empty,'unique_sha256':empty}}


def finra_selection(module):
    return {'schema':'hermes-finra-selected-v1','base':module._FINRA_SELECTED_BASE,
            'source':'finra_short_volume','module':'ingestion.altdata.finra_short_volume',
            'class':'FINRAShortVolumePuller','method':'pull',
            'kwargs':{'trade_date':'2026-10-13',
                      'url':'https://cdn.finra.org/equity/regsho/daily/CNMSshvol20261013.txt',
                      'market_prefix':'CNMS','dry_run':False},
            'artifacts':{key:{'path':'/synthetic/'+key,'sha256':'0'*64}
                         for key in ('driver','producer')},
            'workflow_admission':{'path':'/synthetic/admission','sha256':'1'*64}}

class FinraEntryTests(unittest.TestCase):
    def test_exact_selection_and_scope_refusals_are_before_reads(self):
        m=front();s=finra_selection(m);raw=encode(s)
        self.assertEqual(m._finra_selection(raw,digest(raw)),s)
        for key,value in [('source','sec_xbrl_shares'),('method','pull_recent'),
                          ('extra',True),('base','other')]:
            bad=copy.deepcopy(s);bad[key]=value;raw=encode(bad);reads=[]
            with self.assertRaises(m.FinraSelectedRefused):m._finra_selection(raw,digest(raw))
            r=m.run_finra_selected_job(raw,digest(raw),entry_origin=1.,clock=lambda:1.,
                                      read_bytes=lambda *a:reads.append(a))
            self.assertEqual(r['status'],'REFUSED');self.assertEqual(reads,[])
            self.assertEqual(r['driver_invocations'],0)
        for key,value in [('dry_run',0),('trade_date','2026-10-14'),
                          ('url','https://OTHER/secret'),('extra',True)]:
            bad=copy.deepcopy(s);bad['kwargs'][key]=value;raw=encode(bad)
            with self.assertRaises(m.FinraSelectedRefused):m._finra_selection(raw,digest(raw))

    def test_clock_types_sticky_failure_and_original_bound(self):
        for origin in (None,True,1,float('nan'),float('inf')):
            m=front()
            with self.assertRaises(m.FinraSelectedRefused):m._finra_clock(origin,lambda:1.)
        for values in ([1.,True,2.],[1.,float('nan'),2.],[1.,.9,2.],[1.,451.,2.]):
            m=front();it=iter(values);observe=m._finra_clock(1.,lambda:next(it))
            with self.assertRaises(m.FinraSelectedRefused):observe()
            with self.assertRaises(m.FinraSelectedRefused):observe()
        m=front();it=iter([1.,450.999]);observe=m._finra_clock(1.,lambda:next(it))
        self.assertEqual(observe(),450.999)

    def test_actual_reader_rebinding_during_callback_is_refused(self):
        m=front();runtime=types.SimpleNamespace()
        def reader():runtime.clock=lambda:1.;return 1.
        runtime.clock=reader
        with self.assertRaises(m.FinraSelectedRefused):m._finra_clock(1.,reader,runtime)

    def test_malformed_selection_and_hostile_exception_never_render_raw_strings(self):
        m=front()
        for raw,pin in [(b'{"source":1,"source":2}',None),(b'{"x":NaN}',None),
                        (b'{}','0'*64),(b'['*65537,None)]:
            r=m.run_finra_selected_job(raw,digest(raw) if pin is None else pin,
                                      entry_origin=1.,clock=lambda:1.)
            self.assertEqual(r['status'],'REFUSED');self.assertEqual(r['driver_invocations'],0)
        class Hostile(m.FinraSelectedRefused):
            def __str__(self):raise AssertionError('raw secret must never be rendered')
        def clock():raise Hostile('postgres://SECRET')
        raw=encode(finra_selection(m));r=m.run_finra_selected_job(raw,digest(raw),entry_origin=1.,clock=clock)
        self.assertEqual(r['code'],'UNKNOWN_FAILURE');self.assertNotIn('SECRET',json.dumps(r))

    def test_operator_finra_early_dispatch_blocks_broad_startup(self):
        original=builtins.__import__;effects=[]
        def imp(name,*a,**k):
            if name.split('.')[0] in ('loguru','sqlalchemy','config','ingestion','orchestration'):
                effects.append(name);raise RuntimeError('forbidden broad import')
            return original(name,*a,**k)
        for flags in (['--finra-unknown'],['--finra-only','--sec-only']):
            with patch.object(sys,'argv',[str(OPER),*flags]),patch('os.chdir',side_effect=lambda *a:effects.append('chdir')),patch('os.getenv',side_effect=lambda *a:effects.append('getenv')),patch.object(builtins,'__import__',imp),contextlib.redirect_stdout(io.StringIO()):
                try:runpy.run_path(str(OPER),run_name='__main__')
                except BaseException as exc:code=exc.code if type(exc) is SystemExit else None
                else:code=None
            self.assertEqual(code,2);self.assertEqual(effects,[])

    def test_finra_cli_branch_never_loads_broad_scheduler(self):
        ns={'__name__':'__hermes_finra_only__','HERMES_FINRA_ONLY_ARGS':['--finra-only'],
            'HERMES_FINRA_ENTRY_ORIGIN':1.,'HERMES_FINRA_CLOCK':lambda:1.}
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(SystemExit) as caught:exec(compile(SCHED.read_text(),str(SCHED),'exec'),ns)
        self.assertEqual(caught.exception.code,2)
        self.assertEqual(json.loads(out.getvalue())['driver_invocations'],0)
        self.assertNotIn('SmartScheduler',ns)

class FinraCompositionHarness:
    def __init__(self, *, faults=()):
        fixtures=types.SimpleNamespace(WorkflowRuntime=WorkflowRuntime,admission=admission)
        self.m=front();self.ns=self.m.run_finra_selected_job.__globals__
        self.driver_path=str(ROOT/'scripts/finra_occurrence_driver.py')
        self.producer_path=str(ROOT/'ingestion/altdata/finra_short_volume.py')
        self.files={self.driver_path:Path(self.driver_path).read_bytes(),
                    self.producer_path:Path(self.producer_path).read_bytes()}
        self.driver=self.m._sec_load(self.driver_path,self.files[self.driver_path],
                                     self.m._FINRA_SELECTED_PINS['driver'])
        self.runtime=fixtures.WorkflowRuntime(self.driver,faults=faults)
        self.packet=fixtures.admission(self.driver,self.driver_path,self.producer_path,
                                      self.m._FINRA_SELECTED_PINS['driver'])
        self.selection=finra_selection(self.m)
        self.selection['artifacts']={role:{'path':path,'sha256':digest(self.files[path])}
                                    for role,path in [('driver',self.driver_path),('producer',self.producer_path)]}
        self.reads=[];self.constructors=0;self.result=None
    def prepare(self):
        raw=encode(self.packet);self.files['/synthetic/admission']=raw
        self.selection['workflow_admission']={'path':'/synthetic/admission','sha256':digest(raw)}
    def read(self,path,cap):
        self.reads.append(path);return self.files[path]
    def factory(self):self.constructors+=1;return self.runtime
    def run(self, *, injected=False):
        self.prepare();raw=encode(self.selection)
        self.result=self.m.run_finra_selected_job(raw,digest(raw),entry_origin=10.,
            clock=self.runtime.clock,read_bytes=self.read,
            **({'runtime':self.runtime} if injected else {'native_factory':self.factory}))
        return self.result

class FinraCompositionTests(unittest.TestCase):
    def test_aggregate_ack_cannot_promote_exhausted_or_unknown_attempt(self):
        for faults in (['timeout','connection','503'],['malformed_receipt'],['commit_unknown']):
            with self.subTest(faults=faults):
                h=FinraCompositionHarness(faults=faults);load=h.ns['_sec_load']
                def changed(*a):
                    module=load(*a);run=module.run_selected_workflow
                    def corrupt(*args,**kwargs):
                        result=run(*args,**kwargs)
                        result.update(transaction_ack='ACKNOWLEDGED',committed_rows=2,
                                      after={'row_count':2,'rows':[]})
                        return result
                    module.run_selected_workflow=corrupt;return module
                h.ns['_sec_load']=changed;r=h.run()
                self.assertEqual(r['status'],'UNKNOWN');self.assertIsNone(r['workflow'])
                self.assertEqual(r['commit'],'UNKNOWN');self.assertEqual(r['driver_invocations'],1)

    def test_ack_child_with_no_commit_effects_is_unknown_and_never_retried(self):
        h=FinraCompositionHarness(faults=['ack_without_effects','ok']);r=h.run()
        self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(h.runtime.launches,1)
        self.assertTrue(h.runtime.db_drivers[0].committed)
        self.assertEqual(r['commit'],'UNKNOWN');self.assertFalse(r['workload_success'])

    def test_actual_operator_cli_to_driver_constructor_with_only_boundary_projection(self):
        h=FinraCompositionHarness();h.prepare()
        raw=encode(h.selection);path='/synthetic/selection';h.files[path]=raw
        args=['--finra-only','--finra-selection',path,'--finra-selection-sha256',digest(raw)]
        original_load=h.ns['_sec_load'];h.ns['_sec_read']=h.read
        def load(*a):
            module=original_load(*a)
            module.NativeRuntime=h.factory  # only the native boundary is fake
            return module
        h.ns['_sec_load']=load
        def loader(path,run_name,init_globals):
            self.assertEqual(Path(path),SCHED);self.assertEqual(run_name,'__hermes_finra_only__')
            self.assertEqual(init_globals['HERMES_FINRA_ENTRY_ORIGIN'],10.)
            code=h.m._hermes_finra_cli(init_globals['HERMES_FINRA_ONLY_ARGS'],
                entry_origin=init_globals['HERMES_FINRA_ENTRY_ORIGIN'],
                clock=init_globals['HERMES_FINRA_CLOCK'])
            raise SystemExit(code)
        actual_run_path=runpy.run_path
        with patch.object(sys,'argv',[str(OPER),*args]),patch('time.monotonic',h.runtime.clock),patch.object(runpy,'run_path',loader),patch('os.chdir',side_effect=AssertionError('broad startup')),contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(SystemExit) as caught:actual_run_path(str(OPER),run_name='__main__')
        self.assertEqual(caught.exception.code,0);r=json.loads(out.getvalue())
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['driver_invocations'],1)
        self.assertEqual(h.constructors,1);self.assertEqual(h.runtime.launches,1)
        self.assertEqual(r['workflow']['frame']['origin'],10.)

    def test_actual_selected_constructor_driver_worker_producer_and_pending_final(self):
        h=FinraCompositionHarness();r=h.run()
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['driver_invocations'],1)
        self.assertEqual(h.constructors,1);self.assertEqual(h.runtime.launches,1)
        self.assertTrue(h.runtime.spent);self.assertEqual(len(h.runtime.db_drivers),1)
        self.assertTrue(h.runtime.db_drivers[0].committed)
        self.assertEqual(r['workflow']['frame']['origin'],10.)
        self.assertEqual(r['workflow']['frame']['end'],460.)
        self.assertEqual(r['workflow']['observed_transaction_ack'],'ACKNOWLEDGED')
        self.assertEqual(r['workflow']['observed_committed_rows'],2)
        self.assertEqual(r['workflow']['db_write_bearing_attempts'],1)
        self.assertEqual(h.runtime.saved[0]['parent_finalization']['state'],'PENDING_AT_PERSIST')
        self.assertFalse(r['native_accepted']);self.assertFalse(r['live_qualified'])
        self.assertFalse(r['workload_success']);self.assertEqual(r['commit'],'UNKNOWN')
        self.assertEqual(r['workflow']['actual_usage']['wire_gets'],'UNKNOWN')
        self.assertNotIn('finra:short_volume',json.dumps(r))

    def test_three_safe_pre_db_transients_are_driver_owned_and_exhausted(self):
        h=FinraCompositionHarness(faults=['timeout','connection','503']);r=h.run()
        self.assertEqual(r['status'],'EXHAUSTED');self.assertEqual(r['driver_invocations'],1)
        self.assertEqual(h.runtime.launches,3);self.assertEqual(h.constructors,1)
        w=r['workflow'];self.assertEqual(w['reservations'],{'gets':3,'body_bytes':25165824,'http_seconds':135})
        self.assertEqual(w['actual_usage']['exchange_calls'],3)
        self.assertEqual(w['db_write_bearing_attempts'],0)
        self.assertTrue(all(not d.raws for d in h.runtime.db_drivers))
        self.assertEqual(len({a['attempt_id'] for a in w['attempts']}),3)
        self.assertTrue(all(a['state']=='RETRYABLE' for a in w['attempts']))
        self.assertTrue(all(a['stdout_eof'] and a['stderr_eof'] and a['cleanup_closed'] for a in w['attempts']))
        self.assertTrue(all(a['stop_end']<=460. for a in w['attempts']))

    def test_pre_db_transient_then_success_stops_at_first_db_bearing_attempt(self):
        h=FinraCompositionHarness(faults=['503','ok','ok']);r=h.run(injected=True)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(h.runtime.launches,2)
        self.assertEqual(r['workflow']['db_write_bearing_attempts'],1)
        self.assertFalse(h.runtime.db_drivers[0].raws);self.assertTrue(h.runtime.db_drivers[1].committed)
        self.assertEqual(r['driver_invocations'],1);self.assertEqual(h.constructors,0)

    def test_unknown_commit_receipt_reap_eof_and_close_never_retry(self):
        for fault in ('commit_unknown','malformed_receipt','wrong_child','reap_unknown','missing_eof','close_failed'):
            with self.subTest(fault=fault):
                h=FinraCompositionHarness(faults=[fault,'ok']);r=h.run()
                self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(h.runtime.launches,1)
                self.assertEqual(r['driver_invocations'],1);self.assertTrue(h.runtime.spent)
                self.assertEqual(r['commit'],'UNKNOWN');self.assertFalse(r['workload_success'])
                self.assertNotIn('SYNTHETIC_SECRET',json.dumps(r))

    def test_terminal_parser_or_db_init_failure_never_spends_retry_allowance(self):
        for fault in ('malformed_body','db_unavailable'):
            with self.subTest(fault=fault):
                h=FinraCompositionHarness(faults=[fault,'ok']);r=h.run()
                self.assertEqual(r['status'],'TERMINAL');self.assertEqual(h.runtime.launches,1)
                self.assertEqual(r['workflow']['reservations']['gets'],1)
                self.assertFalse(any(d.committed for d in h.runtime.db_drivers))

    def test_expiry_static_identity_and_caps_refuse_before_constructor(self):
        changes=[('expires_utc','2026-10-15Z'),('expires_utc','2026-10-15T00:01:00Z'),
                 ('expires_utc','2026-10-23T00:00:00Z'),('issued_utc','2026-10-16T00:00:00Z')]
        for key,value in changes:
            h=FinraCompositionHarness();h.packet[key]=value;r=h.run()
            self.assertNotEqual(r['status'],'COMPLETE');self.assertEqual(h.runtime.launches,0)
            if value=='2026-10-15Z' or value=='2026-10-23T00:00:00Z':self.assertEqual(h.constructors,0)
            self.assertFalse(h.runtime.spent)
        for section,key,value in [('caps','body',True),('workflow_caps','attempts',4),
                                  ('runtime','producer','/other/producer'),('call','dry_run',0)]:
            h=FinraCompositionHarness();h.packet[section][key]=value;r=h.run()
            self.assertEqual(h.constructors,0);self.assertEqual(h.runtime.launches,0)

    def test_single_use_latch_and_no_budget_reset_after_slow_preparation(self):
        h=FinraCompositionHarness();first=h.run();self.assertEqual(first['status'],'COMPLETE')
        second=h.run();self.assertEqual(second['code'],'CALLER_ALREADY_CONSUMED')
        self.assertEqual(h.runtime.launches,1);self.assertEqual(h.constructors,1)
        h=FinraCompositionHarness();read=h.read
        def late(*a):h.runtime.c.now=461.;return read(*a)
        h.read=late;r=h.run();self.assertEqual(r['status'],'REFUSED')
        self.assertEqual(h.constructors,0);self.assertEqual(h.runtime.launches,0)

    def test_callback_clock_rebinding_and_expired_entry_never_launch(self):
        h=FinraCompositionHarness();read=h.read
        def rebound(*a):h.runtime.clock=lambda:10.;return read(*a)
        h.read=rebound;r=h.run(injected=True);self.assertEqual(r['status'],'REFUSED')
        self.assertEqual(h.constructors,0);self.assertEqual(h.runtime.launches,0)
        h=FinraCompositionHarness()
        h.runtime.hook=lambda stage,p: setattr(h.runtime,'clock',lambda:10.) if stage=='consume' else None
        r=h.run();self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(h.runtime.launches,0)
        self.assertTrue(h.runtime.spent);self.assertEqual(r['driver_invocations'],1)

    def test_utc_callback_cannot_admit_preflight_after_original_deadline(self):
        h=FinraCompositionHarness();utc=h.runtime.utc;calls=0
        def wall():
            nonlocal calls
            calls+=1
            if calls==6:h.runtime.c.now=461.
            return utc()
        h.runtime.utc=wall;r=h.run()
        self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(h.runtime.launches,0)
        self.assertEqual([e for e in h.runtime.events if e[0]=='preflight'],[])

    def test_source_admission_pins_and_path_identity_before_constructor(self):
        for role in ('driver','producer'):
            h=FinraCompositionHarness();path=h.selection['artifacts'][role]['path'];h.files[path]+=b'\n# changed after selection pin\n'
            r=h.run();self.assertEqual(r['code'],'SOURCE_PIN');self.assertEqual(h.constructors,0)
            self.assertEqual(h.runtime.launches,0)
        h=FinraCompositionHarness();h.packet['runtime']['driver_sha256']='0'*64
        r=h.run();self.assertEqual(r['code'],'ADMISSION_BINDING');self.assertEqual(h.constructors,0)

    def test_producer_content_drift_refuses_before_constructor(self):
        h=FinraCompositionHarness();path=h.selection['artifacts']['producer']['path']
        h.files[path]+=b'\n# byte drift, still syntactically valid\n'
        r=h.run();self.assertEqual(r['status'],'REFUSED');self.assertEqual(r['code'],'SOURCE_PIN')
        self.assertEqual(h.constructors,0);self.assertEqual(h.runtime.launches,0)

    def test_post_validator_result_mutation_is_unknown_without_raw_export(self):
        h=FinraCompositionHarness();load=h.ns['_sec_load']
        def changed(*a):
            module=load(*a);validate=module.validate_selected_result;calls=0
            def mutate(result,*args):
                nonlocal calls
                calls+=1;value=validate(result,*args)
                if calls==2:result['after']='SECRET_POST_VALIDATOR_CHANGE'
                return value
            module.validate_selected_result=mutate;return module
        h.ns['_sec_load']=changed;r=h.run()
        self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(r['code'],'RESULT_MUTATED')
        self.assertEqual(h.runtime.launches,1);self.assertEqual(r['commit'],'UNKNOWN')
        self.assertNotIn('SECRET',json.dumps(r))

    def test_typed_aggregate_and_original_frame_faults_cannot_be_promoted(self):
        for field,value in [('reservations',{'gets':True,'body_bytes':8388608,'http_seconds':45}),
                            ('actual_usage',{'exchange_calls':True,'body_bytes':0,'http_seconds':0.,'wire_gets':'UNKNOWN'}),
                            ('db_write_bearing_attempts',True),('native_accepted',True)]:
            h=FinraCompositionHarness();load=h.ns['_sec_load']
            def changed(*a):
                module=load(*a);run=module.run_selected_workflow
                def mutate(*args,**kwargs):
                    result=run(*args,**kwargs);result[field]=value;return result
                module.run_selected_workflow=mutate;return module
            h.ns['_sec_load']=changed;r=h.run()
            self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(h.runtime.launches,1)
            self.assertFalse(r['native_accepted']);self.assertIsNone(r['workflow'])


class CurrentSecDefaultTests(unittest.TestCase):
    def test_unselected_default_source_tails_are_current_base_exact(self):
        for path,marker,pin in [(OPER,'\nimport argparse\n','d1b169204f2ed57db284acee8993cf338281311204e32f1eef0871601d6c78a5'),(SCHED,'\nimport calendar\n','11bca7eb2a3cc0852bae9ab0dd4e5135b5cdaf1ebf28c906a76680ec3ce5ad21')]:
            self.assertEqual(digest(path.read_text().split(marker,1)[1].encode()),pin)
    def test_all_accepted_sec_function_bodies_are_preserved(self):
        nodes=[ast.dump(n,include_attributes=False) for n in ast.parse(SCHED.read_text()).body
            if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_sec_')
            or n.name in ('SecOnlyRefused','run_sec_only_job','_hermes_sec_cli'))]
        self.assertEqual(digest(json.dumps(nodes,separators=(',',':')).encode()),'c04a1c9332f8c794970dce0f18ef35157fd8be5a48b6263be4fc18235b73e66b')
    def test_sec_wrong_source_and_clock_refuse_before_reads(self):
        m=front();selection={'schema':'hermes-sec-only-v1','base':m._SEC_ONLY_BASE,
          'source':'other','module':'ingestion.altdata.sec_xbrl_shares','class':'SECXBRLSharesPuller',
          'method':'pull_all','kwargs':{'tickers':['AAPL'],'limit':1,'backfill_days':0},
          'artifacts':{},'root_binding':{},'driver_admission':{}}
        raw=encode(selection);reads=[]
        result=m.run_sec_only_job(raw,digest(raw),read_bytes=lambda *a:reads.append(a))
        self.assertEqual(result['code'],'SOURCE_ALLOWLIST');self.assertEqual(reads,[])
        for raw in (b'{"x":1,"x":2}',b'{"x":NaN}'):
            with self.assertRaises(m.SecOnlyRefused):m._sec_json(raw)
        with self.assertRaises(m.SecOnlyRefused):m._sec_outer_clock(True,lambda:1.)
    def test_sec_early_operator_still_refuses_without_broad_imports(self):
        original=builtins.__import__;effects=[]
        def imp(name,*a,**k):
            if name.split('.')[0] in ('loguru','sqlalchemy','config','ingestion','orchestration'):
                effects.append(name);raise RuntimeError('broad import forbidden')
            return original(name,*a,**k)
        with patch.object(sys,'argv',[str(OPER),'--sec-unknown']),patch('os.chdir',side_effect=lambda *a:effects.append('chdir')),patch.object(builtins,'__import__',imp),contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as caught:runpy.run_path(str(OPER),run_name='__main__')
        self.assertEqual(caught.exception.code,2);self.assertEqual(effects,[])
