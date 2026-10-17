"""Inert actual FINRA producer/HTTP/DBAPI/entrypoint/resource path checks."""
from __future__ import annotations
import copy
import selectors,subprocess,resource,argparse,ast
import io,http.client
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import signal
import stat
import sys
import types
import unittest
from datetime import date,datetime,timezone
from decimal import Decimal

ROOT=Path(__file__).resolve().parents[1]
SOURCE_TEXT=(ROOT/'scripts/finra_occurrence_driver.py').read_text()  # retained before the effect fence
_spec=importlib.util.spec_from_file_location("bounded_finra_driver",ROOT/"scripts/finra_occurrence_driver.py")
d=importlib.util.module_from_spec(_spec);sys.modules[_spec.name]=d;_spec.loader.exec_module(d)
PRODUCER=(ROOT/'ingestion/altdata/finra_short_volume.py').read_bytes()
HEADER='Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market'
GOOD=(HEADER+'\n20261013|AAPL|10|1|100|B,Q,N\n20261013|MSFT|20|2|200|B,Q,N\n2\n').encode()
EMPTY=(HEADER+'\n0\n').encode()


class Clock:
    def __init__(self):self.now=10.0
    def __call__(self):return self.now
    def utc(self):return datetime(2026,10,15,0,0,tzinfo=timezone.utc)
    def wait(self,n):self.now+=n


def packet():
    call=d.Call('12345678-1234-4234-8234-123456789abc','2026-10-15','2026-10-15T00:30:00Z',10.0,7)
    digest=hashlib.sha256(d.canonical([]).encode()).hexdigest()
    return {'schema':'finra-occurrence-admission-v1','status':'ADMITTED','call':d.asdict(call),'caps':d.asdict(d.Caps()),
      'runtime':{'python':'/synthetic/python','python_sha256':'1'*64,'driver':'/synthetic/driver.py','driver_sha256':'2'*64,'producer':'/synthetic/finra.py','driver_package':{},'cgroup':{},'receipt_root':{}},
      'db_plan':{'database':'syntheticdb','read_role':'read_role','write_role':'write_role','table_oid':123,'catalog_oid':124,'trigger_sha256':digest,'unique_sha256':digest}}


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


def run(data=GOOD,*,driver=None,clock=None,status=200,late=False,packet_value=None):
    driver=driver or Driver();clock=clock or Clock();exchange=Exchange(data,status,clock,late)
    p=packet_value or packet();producer=d.load_bound_producer(PRODUCER)
    receipt=d.execute_worker(p,producer=producer,backend=d.DBAPIBackend(driver,'SYNTHETIC_WRITE','SYNTHETIC_READ'),exchange=exchange,clock=clock,utc=clock.utc)
    return receipt,driver,exchange


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


class Runtime:
    injected=True
    def __init__(self):self.c=Clock();self.clock=self.c;self.utc=self.c.utc;self.wait=self.c.wait;self.closed=False;self.persisted=None;self.child_kind='good';self.late=None;self.launches=0
    def preflight(self,p):return object()
    def launch(self,context,bound):
        self.launches+=1;p=json.loads(bound);r,_,_=run(packet_value=p)
        if self.child_kind=='bool':r['binding']['source_id']=True
        h=ParentHandle(context,r,-9 if self.child_kind=='killed' else 0)
        if self.child_kind=='malformed':h.output=b'{bad'
        return h
    def persist(self,context,call_id,receipt):
        self.persisted=copy.deepcopy(receipt)
        if self.late=='persist':self.c.now=161.0
    def close(self,context):
        self.closed=True
        if self.late=='close':self.c.now=161.0


class Tests(unittest.TestCase):
    def test_actual_producer_success_and_typed_readback(self):
        r,driver,exchange=run()
        self.assertEqual(r['status'],'SUCCESS');self.assertEqual(r['committed_rows'],2);self.assertEqual(r['transaction_ack'],'ACKNOWLEDGED')
        self.assertEqual(r['write_attempts'],2);self.assertEqual(r['after']['row_count'],2);self.assertEqual(r['input']['parsed_rows'],2)
        self.assertTrue(all(x.closed for x in driver.raws));self.assertTrue(exchange.response.closed)
        self.assertEqual(d.exported(Decimal('1.230000')),{'type':'Decimal','exact':'1.230000'})
        self.assertFalse(r['live_qualified']);self.assertFalse(r['Card_PIT_accepted']);self.assertTrue(r['synthetic_only'])
        self.assertNotIn('DSN',d.canonical(r))

    def test_complete_readback_value_timestamp_types(self):
        valid=('finra:short_volume:AAPL',date(2026,10,13),Decimal('1.230000'),'SUCCESS',datetime(2026,10,15,tzinfo=timezone.utc),{},7)
        driver=Driver();driver.stored=[valid]
        receipt,_,_=run(driver=driver)
        self.assertEqual(receipt['status'],'SUCCESS')
        for index,bad in [(2,'1.23'),(2,True),(2,Decimal('NaN')),(2,Decimal('Infinity')),(2,-1),(4,'2026-10-15'),(4,datetime(2026,10,15)),(4,datetime(2026,10,15,1,tzinfo=timezone.utc))]:
            with self.subTest(index=index,bad=repr(bad)):
                driver=Driver();row=list(valid);row[index]=bad;driver.stored=[tuple(row)]
                receipt,_,exchange=run(driver=driver)
                self.assertEqual(receipt['status'],'FAILED');self.assertEqual(exchange.calls,[])
                self.assertEqual(receipt['write_attempts'],0);self.assertEqual(receipt['before'],'UNKNOWN')

    def test_raw_six_column_header_framing(self):
        for header in [HEADER.replace('ShortExemptVolume|TotalVolume','TotalVolume|ShortExemptVolume'),HEADER.replace('ShortVolume','UnrelatedMetric',1),'Date|Symbol',HEADER+'|Extra',HEADER.replace('Market','TotalVolume')]:
            with self.subTest(header=header):
                receipt,driver,_=run(GOOD.replace(HEADER.encode(),header.encode()))
                self.assertEqual(receipt['status'],'FAILED');self.assertEqual(receipt['write_attempts'],0);self.assertFalse(driver.committed)

    def test_raw_exact_ascii_trade_date(self):
        for value in ['20261013BAD','X20261013','2026101','202610130','２０２６１０１３','20261014','2026-10-13','20261013 00:00:00']:
            with self.subTest(value=value):
                receipt,driver,_=run(GOOD.replace(b'20261013|',value.encode()+b'|'))
                self.assertEqual(receipt['status'],'FAILED');self.assertEqual(receipt['write_attempts'],0);self.assertFalse(driver.committed)

    def test_paired_case_whitespace_fractional_and_empty_preserved(self):
        header=' date | symbol | shortvolume | shortexemptvolume | totalvolume | market '
        data=GOOD.replace(HEADER.encode(),header.encode()).replace(b'20261013|',b' 20261013 |').replace(b'|10|1|100|',b'|10.25|1.5|100.75|')
        receipt,driver,_=run(data)
        self.assertEqual(receipt['status'],'SUCCESS');self.assertEqual(receipt['committed_rows'],2)
        self.assertEqual(driver.stored[0][2],Decimal('10.25'));self.assertEqual(driver.stored[0][5]['short_exempt_volume'],1.5)
        self.assertEqual(driver.stored[0][5]['total_volume'],100.75);self.assertEqual(driver.stored[0][5]['market'],'B,Q,N')
        receipt,driver,_=run(EMPTY.replace(HEADER.encode(),header.encode()))
        self.assertEqual(receipt['status'],'NO_NEW_DATA');self.assertEqual(receipt['committed_rows'],0);self.assertFalse(driver.committed)

    def test_clean_empty_and_publication_skip_distinct(self):
        r,driver,_=run(EMPTY);self.assertEqual(r['status'],'NO_NEW_DATA');self.assertEqual(r['committed_rows'],0);self.assertFalse(driver.committed)
        for status in (403,404):
            r,driver,_=run(status=status);self.assertEqual(r['status'],'SKIPPED');self.assertEqual(r['committed_rows'],0);self.assertEqual(r['write_attempts'],0)

    def test_malformed_date_count_and_nonfinite_refuse_before_writes(self):
        for data in (b'',(HEADER+'\n').encode(),GOOD.replace(b'\n2\n',b'\n3\n'),GOOD.replace(b'20261013',b'20261014'),GOOD.replace(b'|10|',b'|nan|'),GOOD.replace(b'|10|',b'|inf|'),GOOD.replace(b'|10|',b'|-1|'),GOOD.replace(b'|AAPL|',b'||')):
            with self.subTest(data=data[:80]):
                r,driver,_=run(data);self.assertEqual(r['status'],'FAILED');self.assertEqual(r['write_attempts'],0);self.assertFalse(driver.committed)

    def test_overlimit_body_and_parsed_rows_refuse(self):
        for data in (b'x'*(d.Caps().body+1),b' '*(d.Caps().body+1)+EMPTY,(HEADER+'\n'+('20261013|AAPL|1|0|10|N\n'*20001)+'20001\n').encode()):
            r,driver,_=run(data);self.assertEqual(r['status'],'FAILED');self.assertEqual(r['write_attempts'],0);self.assertFalse(driver.committed)

    def test_unknown_count_and_unknown_commit_never_zero(self):
        for count in (None,-1,True,1.0,2):
            with self.subTest(count=count):
                driver=Driver();driver.count=count;r,driver,_=run(driver=driver)
                self.assertEqual(r['status'],'FAILED');self.assertIsNone(r['committed_rows']);self.assertEqual(r['transaction_ack'],'UNKNOWN');self.assertFalse(driver.committed)
        driver=Driver();driver.commit_error=True;r,driver,_=run(driver=driver)
        self.assertTrue(driver.committed);self.assertEqual(r['after']['row_count'],2)
        self.assertEqual(r['transaction_ack'],'UNKNOWN');self.assertIsNone(r['committed_rows']);self.assertNotIn('SYNTHETIC_PRIVATE_DSN',d.canonical(r))

    def test_preexisting_rows_and_same_file_key_dedupe(self):
        r,driver,_=run();r2,_,_=run(driver=driver)
        self.assertEqual(r2['status'],'NO_NEW_DATA');self.assertEqual(r2['write_attempts'],0);self.assertEqual(r2['committed_rows'],0)
        raw=(HEADER+'\n20261013|AAPL|1|0|10|B\n20261013|AAPL|2|0|10|N\n2\n').encode()
        r,driver,_=run(raw);self.assertEqual(r['input']['unique_keys'],1);self.assertEqual(r['committed_rows'],1);self.assertEqual(driver.stored[0][2],Decimal('1.0'))

    def test_after_failure_preserves_acknowledged_accounting(self):
        driver=Driver();driver.fail_after=True;r,_,_=run(driver=driver)
        self.assertEqual(r['status'],'PARTIAL');self.assertEqual(r['transaction_ack'],'ACKNOWLEDGED');self.assertEqual(r['committed_rows'],2);self.assertEqual(r['after'],'UNKNOWN')

    def test_late_transport_sticky_stop_before_write(self):
        c=Clock();r,driver,_=run(clock=c,late=True)
        self.assertEqual(r['status'],'FAILED');self.assertEqual(r['write_attempts'],0);self.assertEqual(r['after'],'UNKNOWN');self.assertFalse(driver.committed)

    def test_authority_caps_source_args_refuse(self):
        for key,value in [('status','NOT_ADMITTED'),('schema','sec-occurrence-admission-v1')]:
            p=packet();p[key]=value;data=d.canonical(p).encode()
            with self.assertRaises(d.Refused):d.bind_live(data,hashlib.sha256(data).hexdigest(),10.)
        for key,value in [('source_id',True),('url','https://evil.invalid'),('trade_date','2026-10-14'),('t0',True),('dry_run',0)]:
            p=packet();p['call'][key]=value;data=d.canonical(p).encode()
            with self.assertRaises((d.Refused,ValueError,TypeError)):d.bind_live(data,hashlib.sha256(data).hexdigest(),10.)
        p=packet();p['caps']['rows']=True;data=d.canonical(p).encode()
        with self.assertRaises(d.Refused):d.bind_live(data,hashlib.sha256(data).hexdigest(),10.)
        with self.assertRaises(d.Refused):d.load_bound_producer(PRODUCER+b'\n')

    def test_actual_cursor_bounded_no_fetchall_and_foreign_sql(self):
        p=packet();c=Clock();budget=d.Budget(d.Call(**p['call']),d.Caps(),c,c.utc);driver=Driver();backend=d.DBAPIBackend(driver,'SYNTHETIC_WRITE','SYNTHETIC_READ')
        with backend.session('read',budget) as ses:
            ses.execute(d.ROLE_SQL);self.assertEqual(len(ses.rows(1)),1)
            ses.cursor.data=[(1,),(2,)]
            with self.assertRaises(d.Refused):ses.rows(1)
            with self.assertRaises(d.Refused):ses.execute('DELETE FROM raw_series')

    def test_postconnect_and_cursor_close_originals_finalized(self):
        c=Clock();budget=d.Budget(d.Call(**packet()['call']),d.Caps(),c,c.utc)
        driver=Driver();driver.enter_error=True;backend=d.DBAPIBackend(driver,'SYNTHETIC_WRITE','SYNTHETIC_READ')
        with self.assertRaises(RuntimeError):
            with backend.session('read',budget):pass
        self.assertTrue(driver.raws[0].closed)
        driver=Driver();driver.cursor_close_error=True;backend=d.DBAPIBackend(driver,'SYNTHETIC_WRITE','SYNTHETIC_READ')
        with self.assertRaises(RuntimeError):
            with backend.session('read',budget):pass
        self.assertTrue(driver.raws[0].closed);self.assertTrue(driver.raws[0].cur.closed)

    def test_rebound_db_resource_never_closes_foreign(self):
        c=Clock();budget=d.Budget(d.Call(**packet()['call']),d.Caps(),c,c.utc);driver=Driver();backend=d.DBAPIBackend(driver,'SYNTHETIC_WRITE','SYNTHETIC_READ')
        ses=backend.session('read',budget);ses.__enter__();original=ses.raw;foreign=Raw(driver,True);ses.raw=foreign
        with self.assertRaises(d.Refused):ses.close()
        self.assertTrue(original.closed);self.assertFalse(foreign.closed)

    def test_actual_parent_missing_child_receipts_stay_unknown(self):
        for kind in ('killed','malformed','bool'):
            with self.subTest(kind=kind):
                rt=Runtime();rt.child_kind=kind;p=packet();raw=d.canonical(p).encode()
                r=d.run_live(raw,hashlib.sha256(raw).hexdigest(),runtime=rt)
                self.assertEqual(r['write_attempts'],'UNKNOWN');self.assertEqual(r['transaction_ack'],'UNKNOWN');self.assertIsNone(r['committed_rows']);self.assertTrue(rt.closed)

    def test_parent_final_clock_proof_and_pending_snapshot(self):
        for stage in (None,'persist','close'):
            with self.subTest(stage=stage):
                rt=Runtime();rt.late=stage;raw=d.canonical(packet()).encode();r=d.run_live(raw,hashlib.sha256(raw).hexdigest(),runtime=rt)
                self.assertEqual(r['transaction_ack'],'ACKNOWLEDGED');self.assertEqual(r['committed_rows'],2)
                self.assertEqual(rt.persisted['parent_finalization']['state'],'PENDING_AT_PERSIST')
                if stage:self.assertEqual(r.get('error_type'),'ParentFinalizationRefused')
                else:self.assertNotIn('error_type',r);self.assertEqual(r['parent_finalization']['state'],'CLOSED_WITHIN_BOUND')

    def test_cli_uses_actual_parent_and_worker_paths(self):
        raw=d.canonical(packet()).encode();out=[]
        code=d.cli(['--admission','synthetic','--admission-sha256',hashlib.sha256(raw).hexdigest()],read_bytes=lambda p:raw,runtime=Runtime(),emit=out.append)
        self.assertEqual(code,0);self.assertEqual(json.loads(out[0])['schema'],'finra-occurrence-parent-v1')
        called=[];self.assertEqual(d.cli(['--worker','--packet-sha256','a'*64],worker_entry=called.append),0);self.assertEqual(called,['a'*64])

    def test_concrete_https_host_redirect_length_and_stream(self):
        c=Clock();made=[]
        class Resp:
            status=200
            def getheader(self,k):return '2' if k=='Content-Length' else None
            def read(self,n):return b'x'[:n]
            def close(self):self.closed=True
        class Conn:
            def __init__(self,host,timeout):self.sock=self;self.closed=False;self.res=Resp();made.append(self)
            def settimeout(self,n):self.timeout=n
            def request(self,*a,**kw):self.req=(a,kw)
            def getresponse(self):self.res.fp=types.SimpleNamespace(raw=types.SimpleNamespace(_sock=self));return self.res
            def close(self):self.closed=True
        ex=d.HTTPSExchange(c,connection_factory=Conn);res=ex(d.URL,55.,1024);self.assertEqual(res.read(1),b'x');res.close();self.assertTrue(made[-1].closed)
        with self.assertRaises(d.Refused):ex('https://evil.invalid/',55.,1024)
        class Redir(Conn):
            def getresponse(self):self.res.status=302;return self.res
        with self.assertRaises(d.Refused):d.HTTPSExchange(c,connection_factory=Redir)(d.URL,55.,1024)
        self.assertTrue(made[-1].closed)
        with self.assertRaises(d.Refused):ex(d.URL,55.,1)

    def test_typed_catalog_and_triggers_refuse_before_http(self):
        for key,value in [('source_id',8)]:
            p=packet();p['call'][key]=value;r,driver,ex=run(packet_value=p)
            self.assertEqual(r['status'],'FAILED');self.assertEqual(ex.calls,[]);self.assertEqual(r['write_attempts'],0)
        p=packet();p['db_plan']['trigger_sha256']='0'*64;r,driver,ex=run(packet_value=p)
        self.assertEqual(r['status'],'FAILED');self.assertEqual(ex.calls,[])
        driver=Driver();driver.role_override=('read_role','syntheticdb',0,0,0,0);r,driver,ex=run(driver=driver)
        self.assertEqual(r['status'],'FAILED');self.assertEqual(ex.calls,[])


class CGOS:
    O_RDONLY=os.O_RDONLY;O_WRONLY=os.O_WRONLY;O_DIRECTORY=os.O_DIRECTORY;O_NOFOLLOW=os.O_NOFOLLOW;O_CREAT=os.O_CREAT;O_EXCL=os.O_EXCL
    path=types.SimpleNamespace(realpath=lambda p:p)
    def __init__(self):
        self.values={'memory.max':'268435456','memory.swap.max':'0','pids.max':'1','cpu.max':'100000 100000','cgroup.max.depth':'0','cgroup.max.descendants':'0','cgroup.procs':'','cgroup.threads':''};self.names={};self.closed=[];self.writes=[];self.fd=20
    def getuid(self):return 1000
    def getpid(self):return 123
    def open(self,name,flags,mode=0o777,**kw):
        self.fd+=1;self.names[self.fd]=name;return self.fd
    def fstat(self,fd):return types.SimpleNamespace(st_dev=1,st_ino=2,st_uid=1000,st_mode=stat.S_IFDIR|0o700)
    def read(self,fd,n):return self.values[self.names[fd]].encode()
    def write(self,fd,data):self.writes.append((self.names[fd],data));return len(data)
    def close(self,fd):self.closed.append(fd)


class NativeBoundaryTests(unittest.TestCase):
    def test_concrete_cgroup_controls_and_join_using_fake_fs(self):
        os_api=CGOS();binding={'path':'/sys/fs/cgroup/synthetic','device':1,'inode':2,'uid':1000}
        cg=d.DelegatedCgroup(binding,os_api=os_api);cg.child_join();self.assertEqual(os_api.writes,[('cgroup.procs',b'123')]);cg.close()
        os_api=CGOS();os_api.values['memory.max']='max'
        with self.assertRaises(d.Refused):d.DelegatedCgroup(binding,os_api=os_api)
        self.assertTrue(os_api.closed)
        os_api=CGOS();os_api.values['memory.max']='536870912'
        with self.assertRaises(d.Refused):d.DelegatedCgroup(binding,os_api=os_api)

    def test_receipt_fd_guard_before_any_marker(self):
        os_api=CGOS();rt=d.NativeRuntime(native_api=types.SimpleNamespace(os=os_api),clock=Clock(),wait=lambda n:None,utc=Clock().utc)
        ctx={'receipt_fd':456,'receipt_original_fd':123,'receipt_pin':(1,2,1000)}
        with self.assertRaises(d.Refused):rt.verify_receipt_fd(ctx)
        self.assertEqual(os_api.names,{})
        ctx['receipt_fd']=123;ctx['receipt_pin']=(1,999,1000)
        with self.assertRaises(d.Refused):rt.verify_receipt_fd(ctx)
        self.assertEqual(os_api.closed,[])


class Pipe:
    def __init__(self,fd,child):self.fd,self.child,self.closed=fd,child,False
    def fileno(self):return self.fd
    def write(self,data):self.child.synchronous.extend(data);return len(data)
    def flush(self):pass
    def close(self):
        self.closed=True
        if self.fd==103 and self.child.code is None:self.child.code=0


class Child:
    def __init__(self):self.pid=123;self.code=None;self.killed=False;self.synchronous=bytearray();self.stdout=Pipe(101,self);self.stderr=Pipe(102,self);self.stdin=Pipe(103,self)
    def poll(self):return self.code
    def wait(self,timeout=0):
        if self.code is None:raise RuntimeError('FAKE_CHILD_STILL_RUNNING')
        return self.code
    def kill(self):self.killed=True;self.code=-9


class Selector:
    def __init__(self):self.closed=False
    def register(self,s,*a):
        if not hasattr(self,"streams"):self.streams=[]
        self.streams.append(s)
    def unregister(self,s):self.streams.remove(s)
    def select(self,timeout=0):return [(types.SimpleNamespace(fileobj=s),1) for s in self.streams]
    def close(self):self.closed=True


class PipeOS(CGOS):
    environ={d.WRITE_ENV:'SYNTHETIC_WRITE',d.READ_ENV:'SYNTHETIC_READ'}
    def __init__(self,blocked=False):super().__init__();self.blocked=blocked;self.data=bytearray();self.blocking=[]
    def fstat(self,fd):
        if fd in (101,102,103):return types.SimpleNamespace(st_dev=1,st_ino=fd,st_mode=stat.S_IFIFO|0o600)
        if fd==700:return types.SimpleNamespace(st_dev=1,st_ino=700,st_mode=stat.S_IFREG|0o600)
        return super().fstat(fd)
    def read(self,fd,n):
        if fd in (101,102):return b""
        return super().read(fd,n)
    def pidfd_open(self,pid,flags):assert pid==123;return 700
    def set_blocking(self,fd,enabled):self.blocking.append((fd,enabled))
    def write(self,fd,data):
        if fd==103:
            if self.blocked:raise BlockingIOError()
            self.data.extend(data);return len(data)
        return super().write(fd,data)


def native_fixture(blocked=False,broken=False):
    c=Clock();os_api=PipeOS(blocked);child=Child();seen={};limits=[]
    ops=types.SimpleNamespace(os=os_api,child_type=Child,selector=Selector,send=lambda fd,kind:setattr(child,'code',-15 if kind==signal.SIGTERM else -9))
    class CG:
        fd=10
        def read(self,name):return str(child.pid)
        def child_join(self):seen['joined']=True
        def close(self):seen['cgroup_closed']=True
    def launch(argv,**kw):seen.update(argv=argv,kwargs=kw);kw['preexec_fn']();return child
    def handle(*a):
        if broken:raise RuntimeError('PARTIAL_NATIVE_HANDLE')
        return d.LiveHandle(*a,native_ops=ops)
    api=types.SimpleNamespace(os=os_api,Popen=launch,setrlimit=lambda *a:limits.append(a),RLIMIT_AS=1,RLIMIT_CPU=2,RLIMIT_CORE=3,handle=handle)
    rt=d.NativeRuntime(native_api=api,clock=c,wait=c.wait,utc=c.utc)
    context={'receipt_fd':500,'receipt_original_fd':500,'receipt_pin':(1,2,1000),'cgroup':CG()}
    return rt,context,child,os_api,c,seen,limits


class ConcreteWorkerTests(unittest.TestCase):
    def test_progressing_packet_native_launch_under_owned_supervision(self):
        rt,ctx,child,os_api,c,seen,limits=native_fixture();raw=d.canonical(packet()).encode()
        handle=rt.launch(ctx,raw)
        self.assertEqual(os_api.data,b'');self.assertEqual(child.synchronous,b'');self.assertEqual(handle.packet,raw+b'\n')
        self.assertTrue(seen['joined']);self.assertEqual(len(limits),3)
        self.assertEqual(seen['argv'][:4],('/synthetic/python','-I','-S','-B'))
        self.assertNotIn('SYNTHETIC_WRITE',' '.join(seen['argv']))
        stop=d.OwnedWorker(handle,ctx,c,c.wait,130.,160.).finish()
        self.assertEqual(stop['exit_code'],0);self.assertTrue(stop['reaped']);self.assertEqual(os_api.data,raw+b'\n')
        self.assertTrue(child.stdin.closed and child.stdout.closed and child.stderr.closed)
        self.assertIn(700,os_api.closed);rt.close(ctx);self.assertIn(500,os_api.closed)

    def test_blocked_packet_term_reap_before_same_deadline(self):
        rt,ctx,child,os_api,c,seen,limits=native_fixture(blocked=True);raw=d.canonical(packet()).encode();handle=rt.launch(ctx,raw)
        result=d.OwnedWorker(handle,ctx,c,c.wait,10.1,11.).finish()
        self.assertTrue(result['term_sent']);self.assertTrue(result['reaped']);self.assertLess(c.now,11.)
        self.assertEqual(os_api.data,b'');self.assertEqual(result['commit'],'UNKNOWN');rt.close(ctx)

    def test_native_partial_handle_failure_stops_original_child(self):
        rt,ctx,child,os_api,c,seen,limits=native_fixture(broken=True)
        with self.assertRaisesRegex(RuntimeError,'PARTIAL_NATIVE_HANDLE'):rt.launch(ctx,d.canonical(packet()).encode())
        self.assertTrue(child.killed);self.assertEqual(child.code,-9)
        self.assertTrue(child.stdin.closed and child.stdout.closed and child.stderr.closed)
        rt.close(ctx);self.assertTrue(seen['cgroup_closed'])

    def test_poll_identity_rebinding_cannot_signal_foreign(self):
        c=Clock();owner=object()
        class Handle:
            identity=('owned',1)
            def __init__(self):self.owner=owner;self.signals=[]
            def poll(self):self.identity=('foreign',2);c.now=130.;return None
            def signal(self,kind):self.signals.append(kind);return True
            def close(self):raise AssertionError('FOREIGN_CLOSE')
        h=Handle()
        with self.assertRaises(d.Refused):d.OwnedWorker(h,owner,c,c.wait,130.,160.).finish()
        self.assertEqual(h.signals,[])


    def test_launch_receipt_pin_precedes_marker_and_worker(self):
        rt,ctx,child,os_api,c,seen,limits=native_fixture();ctx['receipt_fd']=456
        with self.assertRaises(d.Refused):rt.launch(ctx,d.canonical(packet()).encode())
        self.assertEqual(os_api.names,{});self.assertEqual(seen,{})


class TransportMemoryRaw(io.RawIOBase):
    def __init__(self,sock):self._sock=sock
    def readable(self):return True
    def readinto(self,b):
        n=min(len(b),len(self._sock.data));b[:n]=self._sock.data[:n];self._sock.data=self._sock.data[n:];return n


class TransportMemorySocket:
    def __init__(self,data=b"{}",status=200,length=True):
        self.data=(f"HTTP/1.1 {status} fixture\r\nConnection: close\r\n"+(f"Content-Length: {len(data)}\r\n" if length else "")+"\r\n").encode()+data
        self.timeouts=[];self.closed=0
    def makefile(self,mode):assert mode=="rb";return io.BufferedReader(TransportMemoryRaw(self))
    def sendall(self,b):pass
    def settimeout(self,n):self.timeouts.append(n)
    def close(self):self.closed+=1


class ResponseOwnedTransportTests(unittest.TestCase):
    def exchange(self,*,data=b"{}",status=200,length=True):
        clock=Clock();owned=[];responses=[]
        def factory(host,timeout):
            con=http.client.HTTPConnection(host,timeout=timeout);sock=TransportMemorySocket(data,status,length);con.sock=sock
            self.assertIs(type(con),http.client.HTTPConnection);self.assertIs(con.getresponse.__func__,http.client.HTTPConnection.getresponse)
            def response(*a,**kw):
                obj=http.client.HTTPResponse(*a,**kw);self.assertIs(type(obj),http.client.HTTPResponse);responses.append(obj);return obj
            con.response_class=response;owned.append((con,sock));return con
        return clock,d.HTTPSExchange(clock,connection_factory=factory),owned,responses

    def test_real_stdlib_detaches_connection_body_timeout_and_eof(self):
        c,ex,owned,rs=self.exchange();r=ex(d.URL,55.,1024)
        self.assertIsNone(owned[0][0].sock);self.assertTrue(rs[0].will_close)
        first=None;failure=None
        try:first=r.read(1)
        except d.Refused as exc:failure=exc
        self.assertIsNone(failure);self.assertEqual(first,b"{");self.assertEqual(r.read(2),b"}");self.assertEqual(r.read(1),b"")
        self.assertTrue(owned[0][1].timeouts);self.assertTrue(all(0<n<=15 for n in owned[0][1].timeouts));r.close()

    def test_unknown_length_real_stdlib_eof(self):
        c,ex,owned,rs=self.exchange(length=False);r=ex(d.URL,55.,1024)
        self.assertEqual(r.read(9),b"{}");self.assertEqual(r.read(9),b"");r.close()

    def test_missing_or_rebound_response_socket_refuses_without_read(self):
        c,ex,owned,rs=self.exchange();r=ex(d.URL,55.,1024);reads=[];original=r.response.read
        r.response.read=lambda n:reads.append(n)
        r.response.fp.raw._sock=object()
        with self.assertRaises(d.Refused):r.read(1)
        self.assertEqual(reads,[]);r.response.read=original;r.close()
        con=types.SimpleNamespace(close=lambda:None);response=types.SimpleNamespace(status=200,getheader=lambda k:None,close=lambda:None)
        with self.assertRaises(d.Refused):d.HTTPResponse(con,response,55.,c)

    def test_timeout_callback_expiry_refuses_before_blocking_read(self):
        c,ex,owned,rs=self.exchange();r=ex(d.URL,55.,1024);reads=[];original=r.response.read
        r.response.read=lambda n:reads.append(n)
        def expire(n):c.now=55.
        owned[0][1].settimeout=expire
        with self.assertRaises(d.Refused):r.read(1)
        self.assertEqual(reads,[]);r.response.read=original;r.close()

    def test_read_late_and_invalid_count_or_body_refuses(self):
        for count in [True,0,65537]:
            c,ex,owned,rs=self.exchange();r=ex(d.URL,55.,1024)
            with self.subTest(count=count),self.assertRaises(d.Refused):r.read(count)
            r.close()
        for kind in ['late','type','overcount']:
            c,ex,owned,rs=self.exchange();r=ex(d.URL,55.,1024);original=r.response.read
            def read(n):
                if kind=='late':c.now=55.;return b'x'
                return 'x' if kind=='type' else b'xx'
            r.response.read=read
            with self.subTest(kind=kind),self.assertRaises(d.Refused):r.read(1)
            r.response.read=original;r.close()

    def test_redirect_and_header_primary_survives_both_cleanup_errors(self):
        for kind in ['redirect','length']:
            c=Clock();closed=[]
            class Resp:
                status=302 if kind=='redirect' else 200
                def __init__(self,sock):self.fp=types.SimpleNamespace(raw=types.SimpleNamespace(_sock=sock))
                def getheader(self,key):return 'not-a-length' if kind=='length' and key=='Content-Length' else None
                def close(self):closed.append('response');raise RuntimeError('SENSITIVE_DSN')
            class Con:
                def __init__(self,*a,**kw):self.sock=self
                def settimeout(self,n):pass
                def request(self,*a,**kw):pass
                def getresponse(self):return Resp(self)
                def close(self):closed.append('connection');raise RuntimeError('SENSITIVE_DSN')
            with self.subTest(kind=kind),self.assertRaises(d.Refused) as caught:d.HTTPSExchange(c,connection_factory=Con)(d.URL,55.,1024)
            self.assertEqual(caught.exception.args,('HTTP_REDIRECT_FORBIDDEN' if kind=='redirect' else 'HTTP_LENGTH',));self.assertEqual(closed,['response','connection'])

    def test_primary_body_error_survives_response_and_budget_cleanup(self):
        p=packet();producer=d.load_bound_producer(PRODUCER);clock=Clock();budget=d.Budget(d.Call(**p['call']),d.Caps(**p['caps']),clock,clock.utc)
        backend=d.DBAPIBackend(Driver(),'SYNTHETIC_WRITE','SYNTHETIC_READ');receipt={'http':None};primary=d.Refused('HTTP_BODY_TYPE');closed=[]
        class Resp:
            status=200;encoding=None
            def read(self,n):raise primary
            def close(self):closed.append(True);clock.now=1000.;raise RuntimeError('SENSITIVE_DSN')
        occurrence=d.OccurrenceIO(d.Call(**p['call']),d.Caps(**p['caps']),producer,backend,lambda *a:Resp(),budget,p['db_plan'],receipt)
        puller=types.SimpleNamespace(source_id=7)
        error=None
        try:occurrence.pull(puller,trade_date=d.TRADE_DATE,url=d.URL,market_prefix='CNMS',dry_run=False)
        except BaseException as exc:error=exc
        self.assertIs(error,primary);self.assertEqual(closed,[True]);self.assertEqual(receipt.get('write_attempts',0),0)

    def test_independent_close_and_unknown_error_object_never_formatted(self):
        calls=[]
        class Trap(BaseException):
            def __str__(self):raise AssertionError('UNTRUSTED_STR')
            def __repr__(self):raise AssertionError('UNTRUSTED_REPR')
        primary=Trap()
        def first():calls.append('first');raise Trap()
        def second():calls.append('second');raise Trap()
        d._http_finalize((first,second),primary=primary);self.assertEqual(calls,['first','second'])
        c,ex,owned,rs=self.exchange();r=ex(d.URL,55.,1024);calls.clear()
        r._finalizers=(first,second)
        with self.assertRaises(Trap):r.close()
        self.assertEqual(calls,['first','second'])

    def test_actual_worker_real_stdlib_body_full_parser_commit_and_readback(self):
        c,ex,owned,rs=self.exchange(data=GOOD);p=packet();driver=Driver();producer=d.load_bound_producer(PRODUCER)
        r=d.execute_worker(p,producer=producer,backend=d.DBAPIBackend(driver,'SYNTHETIC_WRITE','SYNTHETIC_READ'),exchange=ex,clock=c,utc=c.utc)
        self.assertEqual(r['status'],'SUCCESS');self.assertEqual(r['transaction_ack'],'ACKNOWLEDGED');self.assertEqual(r['committed_rows'],2);self.assertEqual(r['after']['row_count'],2);self.assertEqual(len(owned),1);self.assertIsNone(owned[0][0].sock);self.assertTrue(owned[0][1].timeouts)


def selected_admission():
    p=packet();call=dict(p['call']);call.update(call_id=None,day=None,cutoff_utc=None,t0=None)
    return dict(schema=d.SELECTED_SCHEMA,status='ADMITTED',workflow_id='87654321-1234-4234-8234-123456789abc',issued_utc='2026-10-14T00:00:00Z',expires_utc='2026-10-21T00:00:00Z',call=call,caps=p['caps'],workflow_caps=dict(d.WORKFLOW_CAPS),runtime=p['runtime'],db_plan=p['db_plan'])

class WorkflowRuntime(Runtime):
    def __init__(self,sequence=()):
        super().__init__();self.sequence=list(sequence);self.used=False;self.drivers=[];self.exchanges=[];self.inner=[];self.consume_calls=0;self.mutate=None;self.spent=set()
    def consume_workflow(self,p,wid):
        self.consume_calls+=1
        if wid in self.spent:raise FileExistsError()
        self.spent.add(wid)
        if self.mutate=='consume_clock':self.clock=lambda:10.
        if self.mutate=='consume_late':self.c.now=460.
        return True
    def launch(self,context,bound):
        self.launches+=1;p=json.loads(bound);self.inner.append(copy.deepcopy(p));kind=self.sequence.pop(0) if self.sequence else 'success';driver=Driver();c=self.c
        if kind=='unknown_commit':driver.commit_error=True
        if kind=='bad_role':driver.role_override=('wrong','syntheticdb',False,False,False,False)
        class EX(Exchange):
            def __call__(self,url,deadline,cap):
                self.calls.append((url,deadline,cap))
                if kind in ('timeout','connection'):
                    c.now+=1.;raise TimeoutError('SENSITIVE_MUST_NOT_EXPORT') if kind=='timeout' else ConnectionError('SECRET')
                if kind=='foreign':raise RuntimeError('SECRET')
                self.response=Response(GOOD.replace(b'|10|',b'|nan|') if kind=='malformed' else GOOD,503 if kind=='503' else 403 if kind=='403' else 200,c)
                if kind=='cleanup_fail':
                    def fail():raise RuntimeError('SECRET')
                    self.response.close=fail;self.response.status=503
                return self.response
        ex=EX();self.drivers.append(driver);self.exchanges.append(ex)
        child=d.execute_worker(p,producer=d.load_bound_producer(PRODUCER),backend=d.DBAPIBackend(driver,'SYNTHETIC_WRITE','SYNTHETIC_READ'),exchange=ex,clock=c,utc=c.utc)
        if kind=='forged_frame':child['selected_workflow']['frame']['admission_sha256']='f'*64
        if kind=='forged_effect':child['effects']['db_started']=True;child['retry_classification']={'phase':'PRE_DB_TRANSPORT','code':'TRANSPORT_TIMEOUT','eligible':True}
        h=ParentHandle(context,child)
        if kind=='missing':h.output=b'{bad'
        if kind=='late':c.now=460.
        if kind=='no_eof':h.capture_complete=lambda:False
        return h

def selected_run(sequence=(),*,rt=None,p=None,origin=10.):
    rt=rt or WorkflowRuntime(sequence);p=p or selected_admission();raw=d.canonical(p).encode();result=d.run_selected_workflow(raw,hashlib.sha256(raw).hexdigest(),entry_origin=origin,runtime=rt);return result,rt,raw

class SelectedWorkflowTests(unittest.TestCase):
    def test_transient_then_success_actual_producer_transaction(self):
        for transient in ('timeout','connection','503'):
            with self.subTest(transient=transient):
                r,rt,raw=selected_run((transient,'success'))
                self.assertEqual(r['state'],'COMPLETE');self.assertEqual(len(r['attempts']),2);self.assertEqual(rt.drivers[0].raws,[])
                self.assertEqual(r['attempts'][0]['state'],'RETRYABLE');self.assertEqual(r['transaction_ack'],'ACKNOWLEDGED');self.assertEqual(r['committed_rows'],2)
                self.assertEqual(r['db_write_bearing_attempts'],1);self.assertEqual(r['reservations'],{'gets':2,'body_bytes':16777216,'http_seconds':90})
                self.assertTrue(d.validate_selected_result(r,raw,hashlib.sha256(raw).hexdigest(),10.));self.assertNotIn('SENSITIVE',d.canonical(r))
                self.assertTrue(all(a['stdout_eof'] and a['stderr_eof'] and a['cleanup_closed'] for a in r['attempts']))
    def test_three_transients_exhaust_no_DB_or_refund(self):
        r,rt,raw=selected_run(('timeout','503','connection'))
        self.assertEqual(r['state'],'EXHAUSTED');self.assertEqual(rt.launches,3);self.assertTrue(all(not x.raws for x in rt.drivers))
        self.assertEqual(r['reservations'],{'gets':3,'body_bytes':25165824,'http_seconds':135});self.assertEqual(r['actual_usage']['exchange_calls'],3);self.assertEqual(r['db_write_bearing_attempts'],0)
        self.assertEqual(len(set(a['attempt_id'] for a in r['attempts'])),3)
    def test_nontransient_parse_auth_cleanup_and_foreign_stop(self):
        for kind in ('malformed','bad_role','cleanup_fail','foreign','403'):
            with self.subTest(kind=kind):
                r,rt,raw=selected_run((kind,'success'))
                self.assertEqual(rt.launches,1);self.assertNotEqual(r['attempts'][0]['state'],'RETRYABLE');self.assertFalse(rt.drivers[0].committed)
    def test_unknown_commit_never_retry_or_infer_zero(self):
        r,rt,raw=selected_run(('unknown_commit','success'))
        self.assertEqual(rt.launches,1);self.assertEqual(r['state'],'UNKNOWN');self.assertEqual(r['transaction_ack'],'UNKNOWN');self.assertIsNone(r['committed_rows']);self.assertTrue(rt.drivers[0].committed)
        self.assertEqual(r['db_write_bearing_attempts'],1);self.assertEqual(r['after']['row_count'],2)
    def test_missing_mismatch_eof_unknown_never_retry(self):
        for kind in ('missing','forged_frame','forged_effect','no_eof','late'):
            with self.subTest(kind=kind):
                r,rt,raw=selected_run((kind,'success'));self.assertEqual(rt.launches,1);self.assertEqual(r['state'],'UNKNOWN');self.assertIsNone(r['committed_rows']);self.assertIsNone(r['db_write_bearing_attempts']);self.assertTrue(all(r['actual_usage'][k] is None for k in ('exchange_calls','body_bytes','http_seconds')))
    def test_consumption_exact_once_and_callback_clock_late_fail_closed(self):
        rt=WorkflowRuntime();r,_,raw=selected_run(rt=rt);self.assertEqual(r['state'],'COMPLETE');n=rt.launches
        r,_,_=selected_run(rt=rt);self.assertEqual(rt.launches,n);self.assertEqual(r['state'],'UNKNOWN')
        for change in ('consume_clock','consume_late'):
            rt=WorkflowRuntime();rt.mutate=change;r,_,_=selected_run(rt=rt);self.assertEqual(rt.launches,0);self.assertEqual(r['state'],'UNKNOWN')
    def test_future_expiry_original_origin_no_static_five_second_gate(self):
        rt=WorkflowRuntime();rt.c.now=50.;r,_,raw=selected_run(rt=rt,origin=10.)
        self.assertEqual(r['state'],'COMPLETE');self.assertEqual(r['frame']['origin'],10.);self.assertEqual(r['frame']['end'],460.);self.assertEqual(r['attempts'][0]['start'],50.)
        self.assertEqual(rt.inner[0]['call']['day'],'2026-10-15');self.assertTrue(rt.inner[0]['call']['cutoff_utc'].startswith('2026-10-15T23:59:59'))
        self.assertEqual(rt.inner[0]['call']['source_sha256'],d.PRODUCER_SHA)
    def test_expired_caps_mutated_call_and_short_reserve_before_effects(self):
        values=[]
        for change in ('expired','too_long','caps','workflow_caps','url','static_t0'):
            p=selected_admission()
            if change=='expired':p['expires_utc']='2026-10-15T00:00:00Z'
            elif change=='too_long':p['expires_utc']='2026-10-22T00:00:00Z'
            elif change=='caps':p['caps']['work']=121
            elif change=='workflow_caps':p['workflow_caps']['attempts']=4
            elif change=='url':p['call']['url']='https://invalid.example'
            else:p['call']['t0']=10.
            rt=WorkflowRuntime()
            with self.subTest(change=change),self.assertRaises((d.Refused,TypeError,ValueError)):selected_run(rt=rt,p=p)
            self.assertEqual(rt.consume_calls,0);self.assertEqual(rt.launches,0)
        rt=WorkflowRuntime();rt.c.now=461.
        with self.assertRaises(d.Refused):selected_run(rt=rt,origin=10.)
        self.assertEqual(rt.consume_calls,0)
    def test_result_reducer_refuses_unknown_promotion_types_and_usage(self):
        r,rt,raw=selected_run(('missing',));sha=hashlib.sha256(raw).hexdigest()
        for change in ('promote','usage','db','frame','bool'):
            bad=copy.deepcopy(r)
            if change=='promote':bad['state']='COMPLETE'
            elif change=='usage':bad['actual_usage']['exchange_calls']=True
            elif change=='db':bad['db_write_bearing_attempts']=0
            elif change=='frame':bad['frame']['origin']=11.
            else:bad['attempts'][0]['stdout_eof']=1
            with self.subTest(change=change),self.assertRaises(d.Refused):d.validate_selected_result(bad,raw,sha,10.)
    def test_CLI_captures_origin_before_config_read(self):
        rt=WorkflowRuntime();p=selected_admission();raw=d.canonical(p).encode();out=[]
        def read(path):rt.c.now=30.;return raw
        code=d.cli(['--selected-workflow','--admission','/synthetic','--admission-sha256',hashlib.sha256(raw).hexdigest()],read_bytes=read,runtime=rt,emit=out.append)
        self.assertEqual(code,0);r=json.loads(out[0]);self.assertEqual(r['frame']['origin'],10.);self.assertEqual(r['frame']['end'],460.)

class LifecycleRepairTests(unittest.TestCase):
    def test_primary_and_initial_deadline_preserve_original_close(self):
        owner=object();c=Clock();primary=ValueError('primary');closed=[]
        class H:
            identity=('own',1)
            def __init__(self):self.owner=owner
            def poll(self):raise primary
            def close(self):closed.append(True);raise RuntimeError('secondary')
        caught=None
        try:d.OwnedWorker(H(),owner,c,c.wait,130.,160.).finish()
        except BaseException as exc:caught=exc
        self.assertIs(caught,primary);self.assertEqual(closed,[True])
        closed.clear();h=H();worker=d.OwnedWorker(h,owner,c,c.wait,130.,160.);worker.handle=H()
        with self.assertRaises(d.Refused):worker.finish()
        self.assertEqual(closed,[True])
    def test_actual_native_bothEOF_and_missingEOF(self):
        for complete in (False,True):
            native,ctx,child,os_api,c,seen,limits=native_fixture();r,_,_=run();queue=[(101,d.canonical(r).encode())]+([(101,b''),(102,b'')] if complete else []);trace=[]
            class Sel:
                def __init__(self):self.streams={}
                def register(self,s,*args):self.streams[s.fileno()]=s
                def unregister(self,s):self.streams.pop(s.fileno());trace.append(s.fileno())
                def select(self,timeout=0):return [(types.SimpleNamespace(fileobj=self.streams[queue[0][0]]),1)] if queue else []
                def close(self):pass
            old=os_api.read
            def read(fd,n):
                if fd not in (101,102):return old(fd,n)
                actual,data=queue.pop(0);assert fd==actual;return data
            os_api.read=read;os_api.fsync=lambda fd:None
            ops=types.SimpleNamespace(os=os_api,child_type=Child,selector=Sel,send=lambda *a:None);native.api.handle=lambda *a:d.LiveHandle(*a,native_ops=ops)
            class RT:
                injected=True;clock=c;utc=c.utc;wait=c.wait
                def preflight(self,p):return ctx
                def launch(self,c,p):return native.launch(c,p)
                def persist(self,c,cid,p):native.persist(c,cid,p)
                def close(self,c):native.close(c)
            raw=d.canonical(packet()).encode();result=d.run_live(raw,hashlib.sha256(raw).hexdigest(),runtime=RT())
            if complete:self.assertEqual(result['committed_rows'],2);self.assertEqual(trace,[101,102])
            else:self.assertEqual(result['transaction_ack'],'UNKNOWN');self.assertIsNone(result['committed_rows'])
            self.assertTrue(child.stdout.closed and child.stderr.closed and child.stdin.closed)


class AdditionalSelectedTests(unittest.TestCase):
    def test_UTC_callback_cannot_reach_preflight_or_launch_late(self):
        for at in (2,6,8):
            rt=WorkflowRuntime();calls=[];preflights=[];wall=rt.utc
            def utc():
                calls.append(True)
                if len(calls)==at:rt.c.now=461.
                return wall()
            def preflight(packet):preflights.append(True);return object()
            rt.utc=utc;rt.preflight=preflight;r,_,_=selected_run(rt=rt)
            with self.subTest(UTC_callback=at):
                self.assertEqual(rt.launches,0);self.assertEqual(r['state'],'UNKNOWN')
                if at in (2,6):self.assertEqual(preflights,[])
    def test_commit_attempt_cannot_launch_second_even_if_final_validation_refuses(self):
        rt=WorkflowRuntime(('unknown_commit','success'));caught=None
        try:r,_,_=selected_run(rt=rt)
        except d.Refused as exc:caught=exc
        self.assertEqual(rt.launches,1);self.assertIsNone(caught)
    def test_actual_native_exclusive_marker_and_original_context_close(self):
        rt,ctx,child,os_api,c,seen,limits=native_fixture();paths=set();original_open=os_api.open
        def exclusive(name,flags,*args,**kwargs):
            if name in paths:raise FileExistsError()
            assert flags & os.O_EXCL and flags & os.O_NOFOLLOW;paths.add(name);return original_open(name,flags,*args,**kwargs)
        os_api.open=exclusive;os_api.fsync=lambda fd:None;rt.preflight=lambda p:ctx
        outer=WorkflowRuntime();r,_,raw=selected_run(rt=outer);inner=outer.inner[0];wid=r['workflow_id']
        self.assertTrue(rt.consume_workflow(inner,wid));self.assertIn(wid+'.workflow.spent',paths)
        self.assertTrue(seen['cgroup_closed']);self.assertIn(500,os_api.closed)
        with self.assertRaises(FileExistsError):rt.consume_workflow(inner,wid)
        n=len(paths)
        with self.assertRaises(d.Refused):rt.consume_workflow(inner,'00000000-0000-0000-0000-000000000000')
        self.assertEqual(len(paths),n)
    def test_preflight_rebind_stops_before_attempt_launch(self):
        rt=WorkflowRuntime()
        def preflight(p):rt.clock=lambda:10.;return object()
        rt.preflight=preflight;r,_,_=selected_run(rt=rt)
        self.assertEqual(rt.launches,0);self.assertEqual(r['state'],'UNKNOWN')
    def test_strict_child_unknown_count_bool_and_terminal_receipt(self):
        rt=WorkflowRuntime();r,_,raw=selected_run(rt=rt);inner=rt.inner[0];digest=hashlib.sha256(d.canonical(inner).encode()).hexdigest()
        child=d.execute_worker(inner,producer=d.load_bound_producer(PRODUCER),backend=d.DBAPIBackend(Driver(),'SYNTHETIC_WRITE','SYNTHETIC_READ'),exchange=Exchange(),clock=rt.c,utc=rt.c.utc)
        for key,value in [('http_requests',True),('http_elapsed',float('nan')),('body_bytes',True),('commit_attempted',1)]:
            bad=copy.deepcopy(child);bad['effects'][key]=value
            with self.subTest(key=key),self.assertRaises(d.Refused):d.validate_selected_child(bad,inner,digest)
        bad=copy.deepcopy(child);bad['status']='UNRECOGNIZED'
        with self.assertRaises(d.Refused):d.validate_selected_child(bad,inner,digest)
        bad=copy.deepcopy(child);bad['retry_classification']={'phase':'PRE_DB_TRANSPORT','code':'TRANSPORT_TIMEOUT','eligible':True}
        with self.assertRaises(d.Refused):d.validate_selected_child(bad,inner,digest)


new=globals()
class Fresh(unittest.TestCase):
 def test_missing_receipt_unknown_cannot_gain_known_aggregate(self):
  good,rt,raw=new['selected_run'](('missing',));sha=hashlib.sha256(raw).hexdigest();self.assertEqual(good['state'],'UNKNOWN');self.assertTrue(d.validate_selected_result(good,raw,sha,10.))
  for field,value in (('transaction_ack','ACKNOWLEDGED'),('committed_rows',2),('after',{'row_count':2})):
   with self.subTest(field=field):
    bad=copy.deepcopy(good);bad[field]=value
    with self.assertRaises(d.Refused):d.validate_selected_result(bad,raw,sha,10.)
 def test_exhausted_preDB_cannot_gain_commit_or_rows(self):
  good,rt,raw=new['selected_run'](('timeout','503','connection'));sha=hashlib.sha256(raw).hexdigest();self.assertEqual(good['state'],'EXHAUSTED');self.assertEqual(good['db_write_bearing_attempts'],0)
  for field,value in (('transaction_ack','ACKNOWLEDGED'),('committed_rows',2)):
   with self.subTest(field=field):
    bad=copy.deepcopy(good);bad[field]=value
    with self.assertRaises(d.Refused):d.validate_selected_result(bad,raw,sha,10.)
 def test_actual_commit_child_requires_corresponding_effect_proof(self):
  result,rt,raw=new['selected_run']();inner=rt.inner[0];digest=hashlib.sha256(d.canonical(inner).encode()).hexdigest();child=d.execute_worker(inner,producer=d.load_bound_producer(new['PRODUCER']),backend=d.DBAPIBackend(new['Driver'](),'SYNTHETIC_WRITE','SYNTHETIC_READ'),exchange=new['Exchange'](),clock=rt.c,utc=rt.c.utc)
  self.assertEqual(child['transaction_ack'],'ACKNOWLEDGED');self.assertEqual(child['committed_rows'],2);self.assertTrue(child['effects']['commit_attempted'])
  bad=copy.deepcopy(child);bad['effects'].update(db_started=False,write_bearing=False,commit_attempted=False)
  with self.assertRaises(d.Refused):d.validate_selected_child(bad,inner,digest)


class ReceiptConsistencyPairs(unittest.TestCase):
    def assert_result_valid(self,result,raw):
        self.assertTrue(d.validate_selected_result(result,raw,hashlib.sha256(raw).hexdigest(),10.))
    def worker(self,driver=None,data=GOOD,status=200):
        result,rt,raw=selected_run();inner=rt.inner[0]
        child=d.execute_worker(inner,producer=d.load_bound_producer(PRODUCER),backend=d.DBAPIBackend(driver or Driver(),'SYNTHETIC_WRITE','SYNTHETIC_READ'),exchange=Exchange(data,status),clock=rt.c,utc=rt.c.utc)
        self.assertTrue(d.validate_selected_child(child,inner,hashlib.sha256(d.canonical(inner).encode()).hexdigest()))
        return child,inner
    def test_observed_unknown_commit_retains_after_and_DB_effect(self):
        r,rt,raw=selected_run(('unknown_commit',));self.assert_result_valid(r,raw)
        self.assertEqual(r['state'],'UNKNOWN');self.assertEqual(r['transaction_ack'],'UNKNOWN');self.assertIsNone(r['committed_rows']);self.assertEqual(r['db_write_bearing_attempts'],1);self.assertEqual(r['after']['row_count'],2)
        for key,value in [('transaction_ack','ACKNOWLEDGED'),('committed_rows',0),('after','UNKNOWN')]:
            bad=copy.deepcopy(r);bad[key]=value
            with self.subTest(key=key),self.assertRaises(d.Refused):d.validate_selected_result(bad,raw,hashlib.sha256(raw).hexdigest(),10.)
    def test_actual_late_outer_failure_retains_observed_ACK(self):
        rt=WorkflowRuntime();original=rt.utc;calls=[]
        def utc():
            calls.append(True)
            if len(calls)==12:rt.c.now=460.
            return original()
        rt.utc=utc;r,_,raw=selected_run(rt=rt)
        self.assertEqual(r['state'],'UNKNOWN');self.assertEqual(r['transaction_ack'],'ACKNOWLEDGED');self.assertEqual(r['committed_rows'],2);self.assertEqual(r['after']['row_count'],2);self.assert_result_valid(r,raw)
    def test_zero_write_ACK_still_requires_commit_and_DB(self):
        driver=Driver();first,inner=self.worker(driver);second,inner=self.worker(driver)
        self.assertEqual(second['write_attempts'],0);self.assertEqual(second['committed_rows'],0);self.assertEqual(second['transaction_ack'],'ACKNOWLEDGED');self.assertTrue(second['effects']['db_started'] and second['effects']['write_bearing'] and second['effects']['commit_attempted'])
        bad=copy.deepcopy(second);bad['effects']['commit_attempted']=False
        with self.assertRaises(d.Refused):d.validate_selected_child(bad,inner,hashlib.sha256(d.canonical(inner).encode()).hexdigest())
    def test_clean_empty_and_skipped_no_write_pairs(self):
        for data,status in ((EMPTY,200),(GOOD,403)):
            child,inner=self.worker(data=data,status=status)
            self.assertEqual(child['write_attempts'],0);self.assertEqual(child['transaction_ack'],'NOT_ATTEMPTED');self.assertEqual(child['committed_rows'],0);self.assertFalse(child['effects']['write_bearing']);self.assertFalse(child['effects']['commit_attempted'])
        child,inner=self.worker(data=GOOD,status=403);bad=copy.deepcopy(child);bad['after']={'row_count':0}
        with self.assertRaises(d.Refused):d.validate_selected_child(bad,inner,hashlib.sha256(d.canonical(inner).encode()).hexdigest())
    def test_unknown_SQL_count_without_commit_is_valid_unknown(self):
        driver=Driver();driver.count=None;child,inner=self.worker(driver)
        self.assertGreater(child['write_attempts'],0);self.assertEqual(child['transaction_ack'],'UNKNOWN');self.assertIsNone(child['committed_rows']);self.assertTrue(child['effects']['db_started'] and child['effects']['write_bearing']);self.assertFalse(child['effects']['commit_attempted'])
        bad=copy.deepcopy(child);bad['committed_rows']=1
        with self.assertRaises(d.Refused):d.validate_selected_child(bad,inner,hashlib.sha256(d.canonical(inner).encode()).hexdigest())
    def test_summary_cannot_launder_missing_receipt_or_write_flags(self):
        r,rt,raw=selected_run(('missing',));bad=copy.deepcopy(r);a=bad['attempts'][-1];a.update(transaction_ack='ACKNOWLEDGED',committed_rows=2);bad.update(transaction_ack='ACKNOWLEDGED',committed_rows=2)
        with self.assertRaises(d.Refused):d.validate_selected_result(bad,raw,hashlib.sha256(raw).hexdigest(),10.)
        r,rt,raw=selected_run();bad=copy.deepcopy(r);bad['attempts'][-1].update(db_started=False,write_bearing=False,commit_attempted=False);bad['db_write_bearing_attempts']=0
        with self.assertRaises(d.Refused):d.validate_selected_result(bad,raw,hashlib.sha256(raw).hexdigest(),10.)

class GuardIO:
    """Memory only: original parent's kernel event and private pipe model."""
    def __init__(self):self.dead=False;self.control=b'';self.pins={7:(1,7,1),8:(1,8,2)};self.callback=None
    def pin(self,fd):return self.pins[fd]
    def parent_pid(self):return 42
    def pidfd_pid(self,fd):return 42 if fd==7 else -1
    def is_pipe(self,fd):return fd==8
    def events(self,parent,control):
        if self.callback:self.callback()
        value=self.control;self.control=b'';return self.dead,value


class F8GuardianTests(unittest.TestCase):
    def arm(self,clock=None,ops=None):
        clock=clock or Clock();ops=ops or GuardIO()
        return d.ParentDeathArm(7,8,42,ops=ops,clock=clock,end=160.),clock,ops

    def worker(self,owner,c,*,exit_on=None,eof=True):
        class H(ParentHandle):
            def __init__(self):super().__init__(owner,{'binding':packet()['call']},code=None);self.signals=[];self.reaps=[]
            def signal(self,kind):
                self.signals.append((kind,c.now))
                if kind==exit_on:self.code=-15 if kind=='TERM' else -9
                return True
            def reap(self,code):self.reaps.append(code);return super().reap(code)
            def capture_complete(self):return eof
        return H()

    def test_native_export_absence_is_pre_marker_dependency_refusal(self):
        names=('pidfd_open','pipe2','set_blocking','scandir','read','write','fstat')
        exports=types.SimpleNamespace(**{name:(lambda *a:None) for name in names})
        libc=types.SimpleNamespace(prctl=lambda *a:0)
        self.assertTrue(d.require_guardian_exports(exports,lambda:None,libc))
        for name in names:
            bad=copy.copy(exports);setattr(bad,name,None)
            with self.subTest(name=name),self.assertRaises(d.Refused):d.require_guardian_exports(bad,lambda:None,libc)
        for loader in (None,types.SimpleNamespace(prctl=1)):
            with self.assertRaises(d.Refused):d.require_guardian_exports(exports,lambda:None,loader)

    def test_guard_marker_is_exclusive_even_when_previous_write_failed(self):
        names=set();written=[];closed=[]
        def opened(name,flags,mode,*,dir_fd):
            self.assertTrue(flags & 128);self.assertTrue(flags & 256)
            self.assertEqual((mode,dir_fd),(0o600,9))
            if name in names:raise FileExistsError()
            names.add(name);return 17
        ops=types.SimpleNamespace(O_WRONLY=1,O_CREAT=64,O_EXCL=128,O_NOFOLLOW=256,
              open=opened,write=lambda fd,data:(written.append(bytes(data)) or len(data)),
              fsync=lambda fd:None,close=closed.append)
        rt=d.NativeRuntime.__new__(d.NativeRuntime);rt.api=types.SimpleNamespace(os=ops);rt.verify_receipt_fd=lambda ctx:None
        name=packet()['call']['call_id']+'.guardian.spent';ctx={'receipt_fd':9}
        rt._exclusive_guard_marker(ctx,name,b'fixed-packet-pin')
        with self.assertRaises(FileExistsError):rt._exclusive_guard_marker(ctx,name,b'fixed-packet-pin')
        self.assertEqual(written,[b'fixed-packet-pin']);self.assertEqual(closed,[17])
        # Partial writes stay consumed, not a permission to use another guard.
        ops.write=lambda fd,data:0;other=packet()['call']['call_id']+'.spent'
        with self.assertRaises(d.Refused):rt._exclusive_guard_marker(ctx,other,b'pin')
        with self.assertRaises(FileExistsError):rt._exclusive_guard_marker(ctx,other,b'pin')

    def test_dead_parent_or_pipe_EOF_before_arm_refuses(self):
        for dead,control in ((True,b''),(False,b'EOF')):
            ops=GuardIO();ops.dead=dead;ops.control=control
            with self.subTest(dead=dead),self.assertRaises(d.Refused):self.arm(ops=ops)

    def test_original_parent_death_latches_without_parent_finally(self):
        arm,c,ops=self.arm();ops.dead=True
        self.assertEqual(arm.check(),'TERM');ops.dead=False
        self.assertEqual(arm.check(),'TERM')
        ops.control=b'K';self.assertEqual(arm.check(),'KILL')

    def test_caller_loss_requests_early_TERM_and_owns_reap_close(self):
        arm,c,ops=self.arm();owner=object();h=self.worker(owner,c,exit_on='TERM');ops.dead=True
        stop=d.OwnedWorker(h,owner,c,c.wait,130.,160.,stop_request=arm.check).finish()
        self.assertEqual(h.signals[0],('TERM',10.));self.assertEqual(len(h.reaps),1)
        self.assertTrue(h.closed);self.assertTrue(stop['stdout_eof'] and stop['stderr_eof'])
        self.assertEqual(stop['commit'],'UNKNOWN')

    def test_hard_stopped_caller_does_not_reset_original_timers(self):
        arm,c,ops=self.arm();owner=object();h=self.worker(owner,c,exit_on='KILL')
        stop=d.OwnedWorker(h,owner,c,c.wait,130.,160.,stop_request=arm.check).finish()
        self.assertEqual([v[0] for v in h.signals],['TERM','KILL'])
        self.assertGreaterEqual(h.signals[0][1],130.);self.assertLess(h.signals[0][1],130.1)
        self.assertGreaterEqual(h.signals[1][1],145.);self.assertLess(h.signals[1][1],145.1)
        self.assertLess(c.now,160.);self.assertTrue(stop['reaped']);self.assertTrue(h.closed)

    def test_reused_channel_rebound_clock_and_late_callback_refuse(self):
        for kind in ('fd','clock','late'):
            arm,c,ops=self.arm()
            def change():
                if kind=='fd':ops.pins[8]=(1,99,2)
                elif kind=='clock':arm.clock=lambda:10.
                else:c.now=160.
            ops.callback=change
            with self.subTest(kind=kind),self.assertRaises(d.Refused):arm.check()

    def test_native_readonly_parent_poll_method_memory_projection(self):
        io=d.NativeArmIO.__new__(d.NativeArmIO);calls=[]
        class Poll:
            def register(self,fd,mask):calls.append(('register',fd,mask))
            def poll(self,timeout):self_timeout=timeout;calls.append(('poll',timeout));return [(7,1),(8,16)]
        io.select=types.SimpleNamespace(poll=Poll,POLLIN=1,POLLHUP=16,POLLERR=8,POLLNVAL=32)
        io.os=types.SimpleNamespace(read=lambda fd,n:b'')
        self.assertEqual(io.events(7,8),(True,b'EOF'))
        self.assertEqual(calls[-1],('poll',0))

    def test_guardian_handle_never_signals_its_guardian_process(self):
        h=d.GuardianHandle.__new__(d.GuardianHandle);writes=[];h.fd=6;h.control_fd=8
        h.control_closed=False;h.control_pin=(1,8,2);h.requests=set();h.clock=lambda:10.;h.stop_end=160.
        h.check_fd=lambda:None
        h._os=types.SimpleNamespace(fstat=lambda fd:types.SimpleNamespace(st_dev=1,st_ino=fd,st_mode=2),
                                    write=lambda fd,b:(writes.append((fd,b)) or len(b)))
        self.assertTrue(h.signal('TERM'));self.assertTrue(h.signal('TERM'));self.assertTrue(h.signal('KILL'))
        self.assertEqual(writes,[(8,b'T'),(8,b'K')])
        h.control_pin=(1,99,2)
        with self.assertRaises(d.Refused):h.signal('TERM')

    def test_unarmed_guard_refuses_before_worker_creation(self):
        arm,c,ops=self.arm();arm.armed=False;spawn=[];closed=[];owner=object()
        def create(*args,**kwargs):spawn.append(True);return ParentHandle(owner,{'binding':packet()['call']})
        rt=types.SimpleNamespace(clock=c,wait=c.wait,_spawn_worker=create,close=lambda ctx:closed.append(ctx))
        with self.assertRaises(d.Refused):d.supervise_guarded_worker(rt,owner,d.canonical(packet()).encode(),arm)
        self.assertEqual(spawn,[]);self.assertEqual(len(closed),1)

    def test_actual_guardian_supervision_success_requires_both_EOF(self):
        for eof in (True,False):
            arm,c,ops=self.arm();owner=object();h=ParentHandle(owner,{'binding':packet()['call']});closed=[]
            if not eof:h.capture_complete=lambda:False
            rt=types.SimpleNamespace(clock=c,wait=c.wait,_spawn_worker=lambda *a,**k:h,close=lambda ctx:closed.append(ctx))
            if eof:
                frame=d.supervise_guarded_worker(rt,owner,d.canonical(packet()).encode(),arm)
                self.assertEqual(frame['schema'],'finra-independent-guardian-v1');self.assertTrue(frame['stop']['reaped'])
            else:
                with self.assertRaises(d.Refused):d.supervise_guarded_worker(rt,owner,d.canonical(packet()).encode(),arm)
            self.assertTrue(h.closed);self.assertEqual(closed,[owner])

    def test_guardian_context_close_late_never_exports_success(self):
        arm,c,ops=self.arm();owner=object();h=ParentHandle(owner,{'binding':packet()['call']})
        def close(ctx):c.now=160.
        rt=types.SimpleNamespace(clock=c,wait=c.wait,_spawn_worker=lambda *a,**k:h,close=close)
        with self.assertRaises(d.Refused):d.supervise_guarded_worker(rt,owner,d.canonical(packet()).encode(),arm)
        self.assertTrue(h.closed)

    def test_primary_error_survives_context_close_error(self):
        arm,c,ops=self.arm();primary=KeyboardInterrupt();seen=[]
        def spawn(*a,**k):raise primary
        def close(ctx):seen.append(ctx);raise RuntimeError('SENSITIVE_MUST_NOT_EXPORT')
        rt=types.SimpleNamespace(clock=c,wait=c.wait,_spawn_worker=spawn,close=close)
        caught=None
        try:d.supervise_guarded_worker(rt,object(),d.canonical(packet()).encode(),arm)
        except BaseException as exc:caught=exc
        self.assertIs(caught,primary);self.assertEqual(len(seen),1)

    def test_guardian_binds_outer_end_before_spawn(self):
        arm,c,ops=self.arm();p=packet();p['schema']=d.ATTEMPT_SCHEMA;p['selected_workflow']={'frame':{'end':159.}}
        spawn=[];rt=types.SimpleNamespace(clock=c,wait=c.wait,_spawn_worker=lambda *a,**k:spawn.append(True),close=lambda ctx:None)
        with self.assertRaises(d.Refused):d.supervise_guarded_worker(rt,object(),d.canonical(p).encode(),arm)
        self.assertEqual(spawn,[])


class F8RevisionTests(unittest.TestCase):
    arm=F8GuardianTests.arm

    def test_initial_freshness_and_later_guardian_continuation_are_distinct(self):
        raw=d.canonical(packet()).encode();digest=hashlib.sha256(raw).hexdigest()
        self.assertEqual(d.bind_live(raw,digest,14.999)[1].t0,10.)
        for now in (15.,16.,129.999):
            c=Clock();c.now=now;arm,_,_=self.arm(clock=c)
            with self.subTest(now=now):
                with self.assertRaises(d.Refused):d.bind_live(raw,digest,now)
                bound,call,caps=d.bind_guardian_packet(raw,digest,now,origin=10.,arm=arm)
                self.assertEqual(bound,raw);self.assertEqual(call.t0,10.)
                self.assertEqual((call.t0+caps.work,call.t0+caps.work+caps.stop),(130.,160.))

    def test_continuation_never_accepts_missing_arm_reset_or_work_expiry(self):
        raw=d.canonical(packet()).encode();digest=hashlib.sha256(raw).hexdigest()
        for mode in ('missing','unarmed','reset','expired','before_origin'):
            c=Clock();c.now=130. if mode=='expired' else 16.;arm,_,_=self.arm(clock=c)
            if mode=='unarmed':arm.armed=False
            with self.subTest(mode=mode),self.assertRaises(d.Refused):
                d.bind_guardian_packet(raw,digest,9. if mode=='before_origin' else c.now,
                                      origin=16. if mode=='reset' else 10.,arm=None if mode=='missing' else arm)

    def test_actual_guardian_main_uses_continuation_after_six_second_bootstrap(self):
        # Execute the exact guardian_main method AST under memory-only bindings.
        # No real os/signal/clock/NativeRuntime/NativeArmIO is invoked.
        c=Clock();c.now=16.;ops=GuardIO();raw=d.canonical(packet()).encode();writes=[];claims=[];forwarded=[]
        class Scan:
            def __enter__(self):return iter((object(),))
            def __exit__(self,*args):return False
        os_fake=types.SimpleNamespace(scandir=lambda p:Scan(),set_blocking=lambda *a:None,
            read=lambda fd,n:raw+b'\n',write=lambda fd,data:(writes.append(bytes(data)) or len(data)),close=lambda fd:None)
        signal_fake=types.SimpleNamespace(SIGINT=2,SIGTERM=15,SIGCHLD=17,SIG_DFL=0,signal=lambda *a:None)
        rt=types.SimpleNamespace(clock=c,wait=c.wait,native=lambda:types.SimpleNamespace(
            RLIMIT_AS=1,RLIMIT_CPU=2,RLIMIT_CORE=3,setrlimit=lambda *a:None),
            preflight=lambda p:object(),_claim_guardian=lambda *a:claims.append(True),close=lambda *a:None)
        imports={'os':os_fake,'sys':types.SimpleNamespace(),'time':types.SimpleNamespace(),'signal':signal_fake}
        base=__builtins__ if type(__builtins__) is dict else vars(__builtins__);safe_builtins=dict(base)
        def only_memory(name,*args,**kwargs):
            if name not in imports:raise AssertionError('UNEXPECTED_PROJECTED_IMPORT')
            return imports[name]
        safe_builtins['__import__']=only_memory
        ns=dict(d.__dict__);ns.update(__builtins__=safe_builtins,NativeRuntime=lambda:rt,NativeArmIO=lambda:ops)
        def supervised(runtime,context,data,arm):forwarded.append(json.loads(data)['call']['t0']);return {'synthetic':True}
        ns['supervise_guarded_worker']=supervised
        text=getattr(d,'__source_text__',SOURCE_TEXT)
        method=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='guardian_main')
        exec(compile(ast.Module(body=[method],type_ignores=[]),'<memory-guardian-main>','exec'),ns)
        result=ns['guardian_main'](hashlib.sha256(raw).hexdigest(),7,8,42,10.)
        self.assertEqual(result,0);self.assertEqual(claims,[True]);self.assertEqual(forwarded,[10.]);self.assertTrue(writes)

    def queued_handle(self,*,partial=False,close_error=False):
        c=Clock();owner=object();child=Child();os_api=PipeOS();signals=[]
        def send(fd,kind):signals.append(kind);child.code=-15 if kind==signal.SIGTERM else -9
        ops=types.SimpleNamespace(os=os_api,child_type=Child,selector=Selector,send=send)
        h=d.LiveHandle(child,owner,d.Caps().receipt,c,160.,native_ops=ops)
        h.queue_packet(b'{"synthetic_only":true,"call":"queued"}',130.)
        if partial:
            original=os_api.write
            def prefix(fd,data):return original(fd,data[:3]) if fd==103 else original(fd,data)
            os_api.write=prefix;h.deliver_packet();os_api.write=original
        if close_error:
            def refused_close():raise RuntimeError('SENSITIVE_CLOSE_MUST_NOT_EXPORT')
            child.stdin.close=refused_close
        return h,owner,c,os_api,signals

    def test_actual_queued_packet_normal_delivery_remains_supported(self):
        h,owner,c,os_api,signals=self.queued_handle()
        stop=d.OwnedWorker(h,owner,c,c.wait,130.,160.,stop_request=lambda:'NONE').finish()
        self.assertEqual(bytes(os_api.data),b'{"synthetic_only":true,"call":"queued"}\n')
        self.assertTrue(stop['reaped'] and stop['stdout_eof'] and stop['stderr_eof']);self.assertEqual(signals,[])

    def test_actual_queued_packet_is_suppressed_before_poll_for_TERM_and_KILL(self):
        for request in ('TERM','KILL'):
            h,owner,c,os_api,signals=self.queued_handle();arm,_,ops=self.arm(clock=c)
            # Healthy final arm precedes fake spawn; parent dies during spawn.
            self.assertEqual(arm.check(),'NONE');ops.dead=True
            if request=='KILL':ops.control=b'K'
            stop=d.OwnedWorker(h,owner,c,c.wait,130.,160.,stop_request=arm.check).finish()
            with self.subTest(request=request):
                self.assertEqual(bytes(os_api.data),b'');self.assertTrue(h.packet_cancelled)
                self.assertTrue(h.packet_stdin_closed);self.assertTrue(stop['reaped'] and stop['stdout_eof'] and stop['stderr_eof'])

    def test_partial_frame_never_sends_remaining_bytes_after_cancellation(self):
        h,owner,c,os_api,signals=self.queued_handle(partial=True);prefix=bytes(os_api.data)
        self.assertEqual(len(prefix),3)
        d.OwnedWorker(h,owner,c,c.wait,130.,160.,stop_request=lambda:'TERM').finish()
        self.assertEqual(bytes(os_api.data),prefix);self.assertEqual(h.packet_offset,3);self.assertTrue(h.packet_cancelled)

    def test_suppression_survives_close_error_and_later_poll_attempt(self):
        h,owner,c,os_api,signals=self.queued_handle(close_error=True)
        with self.assertRaises(RuntimeError):d.OwnedWorker(h,owner,c,c.wait,130.,160.,stop_request=lambda:'TERM').finish()
        self.assertTrue(h.packet_cancelled);self.assertEqual(bytes(os_api.data),b'')
        h.deliver_packet()  # must return before any descriptor/clock/write callback
        self.assertEqual(bytes(os_api.data),b'')
        with self.assertRaises(d.Refused):h.poll()  # closed PIDFD; cannot dispatch
        self.assertEqual(bytes(os_api.data),b'')


class GuardianPacketPredicateTests(unittest.TestCase):
    def guard_expression(self,reason):
        # Future synthetic test only: project the exact production predicate,
        # preserving the strict require contract and without native imports.
        text=getattr(d,'__source_text__',SOURCE_TEXT)
        method=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='guardian_main')
        calls=[n for n in ast.walk(method) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='require'
               and len(n.args)==2 and isinstance(n.args[1],ast.Constant) and n.args[1].value==reason]
        self.assertEqual(len(calls),1)
        return compile(ast.Expression(body=calls[0].args[0]),'<memory-actual-guardian-predicate>','eval')

    def test_actual_chunk_guard_returns_bool_and_accepts_nonempty_bytes_only(self):
        code=self.guard_expression('GUARDIAN_PACKET_EOF')
        class Untouched:
            def __bool__(self):raise AssertionError('FOREIGN_BOOL_CALLED')
            def __len__(self):raise AssertionError('FOREIGN_LEN_CALLED')
        for chunk,expected in ((b'x',True),(b'{"synthetic":true}\n',True),(b'',False),
                               ('not-bytes',False),(bytearray(b'x'),False),(None,False),(Untouched(),False)):
            value=eval(code,{'type':type,'bytes':bytes,'len':len,'chunk':chunk})
            with self.subTest(expected=expected,type=type(chunk).__name__):
                self.assertIs(type(value),bool);self.assertIs(value,expected)
                if expected:d.require(value,'SYNTHETIC_CHUNK_GUARD')
                else:
                    with self.assertRaises(d.Refused):d.require(value,'SYNTHETIC_CHUNK_GUARD')

    def test_actual_frame_guard_keeps_newline_and_packet_SHA_binding(self):
        code=self.guard_expression('GUARDIAN_PACKET_PIN');raw=d.canonical(packet()).encode()
        digest=hashlib.sha256(raw).hexdigest()
        for data,expected_sha,accepted in ((raw+b'\n',digest,True),(raw,digest,False),
              (raw+b'\n\n',digest,False),(raw+b'\ntrailing',digest,False),(raw+b'\n','f'*64,False)):
            result=eval(code,{'data':bytearray(data),'expected_sha':expected_sha,'hashlib':hashlib})
            self.assertIs(type(result),bool);self.assertIs(result,accepted)
            if accepted:d.require(result,'SYNTHETIC_FRAME_GUARD')
            else:
                with self.assertRaises(d.Refused):d.require(result,'SYNTHETIC_FRAME_GUARD')


if __name__=='__main__':
    mode=sys.argv[1] if len(sys.argv)>1 else 'selected'
    argparse.ArgumentParser();events=[]
    source_text=(ROOT/'scripts/finra_occurrence_driver.py').read_text()
    def fence(event,args):
        if event=='open' or event.startswith(('socket.','subprocess.','os.fork','os.exec','os.posix_spawn','ctypes.dlopen','os.system')):events.append(event);raise RuntimeError('PROHIBITED_EFFECT')
    sys.addaudithook(fence)
    class Quiet(unittest.TextTestResult):
        def _exc_info_to_string(self,err,test):return 'ASSERTION_FAILURE' if issubclass(err[0],AssertionError) else 'UNEXPECTED_ERROR '+err[0].__name__
    if mode=='F8-v8-focused':
        suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(case)
                                for case in (F8RevisionTests,GuardianPacketPredicateTests))
        result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(suite)
        print(json.dumps({'status':'SYNTHETIC_ONLY','methods':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'prohibited_events':events}))
        raise SystemExit(0 if result.wasSuccessful() and not events else 1)
    if mode=='F8-v8-mutant':
        original=d;before="require(type(chunk) is bytes and len(chunk)>0,'GUARDIAN_PACKET_EOF')"
        after="require(type(chunk) is bytes and chunk,'GUARDIAN_PACKET_EOF')"
        assert source_text.count(before)==1
        changed=source_text.replace(before,after,1);module=types.ModuleType('F8_v8_predicate_mutant');sys.modules[module.__name__]=module
        exec(compile(changed,'<memory-restored-byte-predicate>','exec'),module.__dict__);module.__source_text__=changed;d=module
        result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(unittest.TestSuite([
            GuardianPacketPredicateTests('test_actual_chunk_guard_returns_bool_and_accepts_nonempty_bytes_only')]))
        d=original;detected=bool(result.failures) and not result.errors
        print(json.dumps({'name':'restore_byte_valued_packet_guard','assertion_failures':len(result.failures),'errors':len(result.errors),'detected':detected,'prohibited_events':events}))
        raise SystemExit(0 if detected and not events else 1)
    if mode=='F8-v7-focused':
        result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(unittest.defaultTestLoader.loadTestsFromTestCase(F8RevisionTests))
        print(json.dumps({'status':'SYNTHETIC_ONLY','methods':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'prohibited_events':events}))
        raise SystemExit(0 if result.wasSuccessful() and not events else 1)
    if mode=='F8-v7-mutants':
        original=d;outcomes=[]
        variants=[('repeat_initial_freshness','bound,call,caps=bind_guardian_packet(raw,expected_sha,runtime.clock(),origin=origin,arm=arm)',
                   'bound,call,caps=bind_live(raw,expected_sha,runtime.clock())','test_actual_guardian_main_uses_continuation_after_six_second_bootstrap'),
                  ('omit_cancel_before_delivery','                if cancelled:\n','                if False and cancelled:\n','test_actual_queued_packet_is_suppressed_before_poll_for_TERM_and_KILL')]
        for index,(name,before,after,method) in enumerate(variants):
            assert source_text.count(before)==1,name
            changed=source_text.replace(before,after,1);module=types.ModuleType('F8_v7_mutant_'+str(index));sys.modules[module.__name__]=module
            exec(compile(changed,'<memory-V7-omission>','exec'),module.__dict__);module.__source_text__=changed;d=module
            result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(unittest.TestSuite([F8RevisionTests(method)]))
            outcomes.append({'name':name,'assertion_failures':len(result.failures),'errors':len(result.errors),'detected':bool(result.failures) and not result.errors})
        d=original;print(json.dumps({'mutants':outcomes,'prohibited_events':events}))
        raise SystemExit(0 if all(x['detected'] for x in outcomes) and not events else 1)
    if mode=='F8-focused':
        result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(unittest.defaultTestLoader.loadTestsFromTestCase(F8GuardianTests))
        print(json.dumps({'status':'SYNTHETIC_ONLY','methods':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'prohibited_events':events}))
        raise SystemExit(0 if result.wasSuccessful() and not events else 1)
    if mode=='F8-mutants':
        original=d;outcomes=[]
        for index,(name,before,after,method) in enumerate([
            ('omit_parent_death','if dead or control in (b"T",b"TK",b"EOF"):','if control in (b"T",b"TK",b"EOF"):','test_original_parent_death_latches_without_parent_finally'),
            ('omit_cancel_callback','request=self.stop_request() if self.stop_request is not None else "NONE"','request="NONE"','test_caller_loss_requests_early_TERM_and_owns_reap_close'),
            ('omit_arm_gate','require(arm.armed is True and arm.end==end and same_reader(arm.clock,original_clock),"GUARDIAN_ARM_REQUIRED")','require(True,"GUARDIAN_ARM_REQUIRED")','test_unarmed_guard_refuses_before_worker_creation'),
            ('omit_both_EOF','                if done:break','                if True:break','test_actual_guardian_supervision_success_requires_both_EOF')]):
            assert source_text.count(before)==1,name
            module=types.ModuleType('F8_mutant_'+str(index));sys.modules[module.__name__]=module
            exec(compile(source_text.replace(before,after,1),'<inert-F8-omission>','exec'),module.__dict__);d=module
            result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(unittest.TestSuite([F8GuardianTests(method)]))
            outcomes.append({'name':name,'assertion_failures':len(result.failures),'errors':len(result.errors),'detected':bool(result.failures) and not result.errors})
        d=original;print(json.dumps({'mutants':outcomes,'prohibited_events':events}))
        raise SystemExit(0 if all(x['detected'] for x in outcomes) and not events else 1)
    if mode=='FR-mutants':
        original=d;outcomes=[]
        for index,(name,start,end,case,method) in enumerate([
            ('omit_FR1_aggregate_derivation','    # FR1: derive aggregate facts','    # END FR1',Fresh,'test_missing_receipt_unknown_cannot_gain_known_aggregate'),
            ('omit_FR2_effect_accounting','    # FR2: accounting evidence','    # END FR2',Fresh,'test_actual_commit_child_requires_corresponding_effect_proof')]):
            begin=source_text.index(start);finish=source_text.index(end,begin)+len(end);changed=source_text[:begin]+source_text[finish:]
            module=types.ModuleType('FR_mutant_'+str(index));sys.modules[module.__name__]=module;exec(compile(changed,'<synthetic-FR-source-mutant>','exec'),module.__dict__);d=module
            result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(unittest.TestSuite([case(method)]));outcomes.append({'name':name,'assertion_failures':len(result.failures),'errors':len(result.errors),'detected':bool(result.failures) and not result.errors})
        d=original;print(json.dumps({'mutants':outcomes,'prohibited_events':events}));raise SystemExit(0 if all(x['detected'] for x in outcomes) and not events else 1)
    if mode=='FR-focused':
        classes=[Fresh,ReceiptConsistencyPairs,SelectedWorkflowTests,LifecycleRepairTests,AdditionalSelectedTests]
        suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in classes)
        for case,method in [(Tests,'test_unknown_count_and_unknown_commit_never_zero'),(ResponseOwnedTransportTests,'test_actual_worker_real_stdlib_body_full_parser_commit_and_readback'),(ResponseOwnedTransportTests,'test_primary_body_error_survives_response_and_budget_cleanup')]:suite.addTest(case(method))
        result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(suite);print(json.dumps({'methods':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'prohibited_events':events}));raise SystemExit(0 if result.wasSuccessful() and not events else 1)
    if mode=='mutants':
        original=d;variants=[
          ('primary_cleanup_mask','primary=primary)\n\n\nclass NativeOwnedHandle','primary=None)\n\n\nclass NativeOwnedHandle',LifecycleRepairTests,'test_primary_and_initial_deadline_preserve_original_close'),
          ('ignore_actual_EOF','                if done:break','                if True:break',LifecycleRepairTests,'test_actual_native_bothEOF_and_missingEOF'),
          ('retry_after_commit','                if retry["eligible"]:\n','                if retry["eligible"] or effects["commit_attempted"]:\n',AdditionalSelectedTests,'test_commit_attempt_cannot_launch_second_even_if_final_validation_refuses'),
          ('omit_clock_reader_pin','    require(same_reader(runtime.clock,clock) and same_reader(runtime.utc,utc),"WORKFLOW_CLOCK_REBOUND")','    pass',SelectedWorkflowTests,'test_consumption_exact_once_and_callback_clock_late_fail_closed'),
          ('omit_child_frame_binding','    require(type(child) is dict and child.get("schema")=="finra-occurrence-worker-v2" and canonical(child.get("selected_workflow"))==canonical(packet["selected_workflow"]) and child.get("attempt_packet_sha256")==packet_sha and canonical(child.get("binding"))==canonical(packet["call"]),"SELECTED_CHILD_IDENTITY")','    pass',SelectedWorkflowTests,'test_missing_mismatch_eof_unknown_never_retry')]
        variants.append(('omit_preflight_post_UTC_check','    budget.check("preflight_ready");require_readers(runtime,original_clock,original_utc)\n','',AdditionalSelectedTests,'test_UTC_callback_cannot_reach_preflight_or_launch_late'))
        outcomes=[]
        for index,(name,before,after,case,method) in enumerate(variants):
            assert source_text.count(before)==1,(name,source_text.count(before))
            module=types.ModuleType('finra_mutant_'+str(index));sys.modules[module.__name__]=module;exec(compile(source_text.replace(before,after,1),'<synthetic-source-mutant>','exec'),module.__dict__);d=module
            result=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(unittest.TestSuite([case(method)]));outcomes.append({'name':name,'assertion_failures':len(result.failures),'errors':len(result.errors),'detected':bool(result.failures) and not result.errors})
        d=original;print(json.dumps({'mutants':outcomes,'prohibited_events':events}));raise SystemExit(0 if all(x['detected'] for x in outcomes) and not events else 1)
    classes=[SelectedWorkflowTests,LifecycleRepairTests,AdditionalSelectedTests] if mode=='selected' else [Tests,NativeBoundaryTests,ConcreteWorkerTests,ResponseOwnedTransportTests]
    suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in classes)
    r=unittest.TextTestRunner(verbosity=2,resultclass=Quiet).run(suite);print(json.dumps({'methods':r.testsRun,'failures':len(r.failures),'errors':len(r.errors),'prohibited_events':events,'actual_python':sys.version.split()[0]}));raise SystemExit(0 if r.wasSuccessful() and not events else 1)
