"""One finite CNMS FINRA occurrence; default native route exists but is unrun.

Offline tests use actual producer parser/methods with inert adapters only.
"""
from __future__ import annotations
import ast
import hashlib
import json
import math
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from urllib.parse import urlsplit

RELEASE = "a94db4efaba6b8d1cfe0a242627540aa93012f31"
BASE_TREE = "86473f16b373a2bd4ff741468d20b7a0d2546182"
PRODUCER_SHA = "76579df966612c7f215a9f9c27251574ec13488a31c1e40221dbe09a651337f5"
TRADE_DATE = "2026-10-13"
URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20261013.txt"
WRITE_ENV = "GRID_FINRA_OCCURRENCE_WRITE_DSN"
READ_ENV = "GRID_FINRA_OCCURRENCE_READ_DSN"
LIFECYCLE_REFERENCE_SHA = "a3f2d119ed277bb320c38905a718640753c75dbb3be3fb1004f55ec5d51585e7"

class Refused(RuntimeError):
    """No broader route, retry or success inference follows this refusal."""


def require(ok, reason):
    if ok is not True:
        raise Refused(reason)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def finalize_owned(actions, primary=None):
    """Attempt independent verified finalizers; never replace the primary error."""
    failures = []
    for label, action in actions:
        try:
            action()
        except BaseException as exc:
            failures.append((label, exc))
    chosen = primary if primary is not None else failures[0][1] if failures else None
    if chosen is not None:
        for label, exc in failures:
            if exc is not chosen:
                try:
                    note=getattr(chosen,"add_note",None)
                    if callable(note):note("independent owned cleanup failed")
                except BaseException:pass
        if primary is None:
            raise chosen


class OwnedWorker:
    """Finite one-worker stop sequence, exercised on inert handles offline.

    The native handle below owns a Popen object and PIDFD. It targets that exact
    child, never a supplied/reused PID. NativeRuntime supplies the concrete
    delegated-cgroup route; absence yields a specific pre-launch error.
    """

    def __init__(self, handle, owner, clock, wait, work_end, stop_end):
        require(finite(work_end) and finite(stop_end) and work_end < stop_end, "WORKER_DEADLINES")
        require(handle.owner is owner, "WORKER_OWNER")
        self.handle, self.owner = handle, owner
        self._original_handle,self._original_owner,self._original_identity=handle,owner,handle.identity
        self.clock, self.wait = clock, wait
        self.work_end, self.stop_end = work_end, stop_end
        self.identity = handle.identity
        self.last = None
        self.stopped = False

    def check(self):
        require(self.handle is self._original_handle and self.owner is self._original_owner and self.identity==self._original_identity and self._original_handle.owner is self._original_owner and self._original_handle.identity==self._original_identity,
                "WORKER_IDENTITY_CHANGED")
        now = self.clock()
        require(finite(now) and (self.last is None or now >= self.last), "WORKER_CLOCK")
        self.last = now
        require(self.handle is self._original_handle and self.owner is self._original_owner and self._original_handle.owner is self._original_owner and self._original_handle.identity==self._original_identity,"WORKER_IDENTITY_CHANGED")
        return now

    def finish(self):
        """No restart. TERM at work end, KILL with half stop reserve, reap once."""
        original_handle=self._original_handle;original_owner=self._original_owner;original_identity=self._original_identity
        original_close=original_handle.close
        def close_original():
            require(original_handle.owner is original_owner and original_handle.identity==original_identity,"WORKER_CLEANUP_IDENTITY")
            current=getattr(original_handle,"close",None)
            require(getattr(current,"__func__",current) is getattr(original_close,"__func__",original_close) and getattr(current,"__self__",None) is getattr(original_close,"__self__",None),"WORKER_CLOSE_REBOUND")
            require(original_close() is True,"WORKER_HANDLE_CLOSE_UNVERIFIED")
        def close_clock():require(self.check()<self.stop_end,"WORKER_CLOSE_DEADLINE")
        kill_at = self.work_end + (self.stop_end - self.work_end) / 2
        term_sent = kill_sent = False
        code = None;primary=None
        try:
            now=self.check()
            while now < self.stop_end:
                code = self.handle.poll()
                now = self.check()
                require(now < self.stop_end, "WORKER_POLL_DEADLINE")
                if code is not None:
                    require(type(code) is int, "WORKER_EXIT_TYPE")
                    break
                if now >= self.work_end and not term_sent:
                    require(self.check() < self.stop_end, "WORKER_TERM_DEADLINE")
                    require(self.handle.signal("TERM") is True, "WORKER_TERM_UNVERIFIED")
                    now = self.check()
                    require(now < self.stop_end, "WORKER_TERM_CALLBACK_DEADLINE")
                    term_sent = True
                if now >= kill_at and not kill_sent:
                    require(self.check() < self.stop_end, "WORKER_KILL_DEADLINE")
                    require(self.handle.signal("KILL") is True, "WORKER_KILL_UNVERIFIED")
                    now = self.check()
                    require(now < self.stop_end, "WORKER_KILL_CALLBACK_DEADLINE")
                    kill_sent = True
                self.wait(min(0.05, self.stop_end - now))
                now = self.check()
            require(code is not None, "WORKER_STOP_DEADLINE")
            require(self.check() < self.stop_end, "WORKER_REAP_DEADLINE")
            require(self.handle.reap(code) is True, "WORKER_REAP_UNVERIFIED")
            self.stopped = True
            # Exit/reap alone cannot authenticate a truncated stdout frame.
            # The concrete handle derives completeness only from BOTH empty reads.
            complete=getattr(original_handle,"capture_complete",None)
            require(callable(complete),"WORKER_CAPTURE_INTERFACE")
            while True:
                require(self.check()<self.stop_end,"WORKER_CAPTURE_DEADLINE")
                done=complete();require(type(done) is bool,"WORKER_CAPTURE_TYPE")
                require(self.check()<self.stop_end,"WORKER_CAPTURE_DEADLINE")
                if done:break
                self.wait(min(0.05,self.stop_end-self.last))
            require(self.check() < self.stop_end, "WORKER_FINAL_DEADLINE")
            return {"exit_code": code, "reaped": True, "term_sent": term_sent,
                    "kill_sent": kill_sent, "commit": "UNKNOWN" if term_sent or code else "RECEIPT_REQUIRED",
                    "aggregate_RSS_enforced": False, "native_qualified": False,
                    "stdout_eof":True,"stderr_eof":True}
        except BaseException as exc:
            primary=exc;raise
        finally:
            # Independent steps preserve primary and attempt eligible own release
            # even if the separate clock check fails. Concrete close refuses late
            # signalling, but still releases verified original allocations.
            finalize_owned((("preclose clock",close_clock),("original owned handle",close_original),("postclose clock",close_clock)),primary=primary)


class NativeOwnedHandle:
    """Exact child PIDFD stop/reap operations; never called by offline tests.

    Construction is only for an already-owned Popen child; neither launch nor a
    caller PID is accepted. Creating the PIDFD itself requires future admission.
    The absence of qualified aggregate resource containment still blocks launch.
    """

    def __init__(self, child, owner, *, native_ops=None):
        import os
        import subprocess
        if native_ops is not None:os=native_ops.os
        self._os=os;self._native_ops=native_ops
        require(type(child) is (native_ops.child_type if native_ops is not None else subprocess.Popen)
                and child.poll() is None, "NOT_OWNED_RUNNING_CHILD")
        self.child, self.owner = child, owner
        self.fd = os.pidfd_open(child.pid, 0)
        try:st = os.fstat(self.fd)
        except BaseException:
            os.close(self.fd);raise
        self.fd_pin = (st.st_dev, st.st_ino, st.st_mode)
        self.identity = (child, self.fd)
        self.closed = False

    def check_fd(self):
        os=self._os
        require(not self.closed and self.child is self.identity[0], "WORKER_HANDLE_CLOSED_OR_REBOUND")
        st = os.fstat(self.fd)
        require((st.st_dev, st.st_ino, st.st_mode) == self.fd_pin, "WORKER_PIDFD_REUSED")

    def poll(self):
        self.check_fd()
        return self.child.poll()

    def signal(self, kind):
        import signal
        self.check_fd()
        require(not self.closed and kind in ("TERM", "KILL"), "WORKER_SIGNAL_SCOPE")
        send=self._native_ops.send if self._native_ops is not None else signal.pidfd_send_signal
        send(self.fd, signal.SIGTERM if kind == "TERM" else signal.SIGKILL)
        return True

    def reap(self, code):
        require(type(code) is int and self.child.poll() == code, "WORKER_REAP_STATE")
        return self.child.wait(timeout=0) == code

    def close(self):
        os=self._os
        if not self.closed:
            self.check_fd()
            os.close(self.fd)
            self.closed = True
        return True


class DelegatedCgroup:
    """Existing delegated Linux cgroup-v2 leaf; never creates or sets controls."""
    def __init__(self,binding,*,require_empty=True,os_api=None):
        import os,stat
        os=os_api or os
        self.os=os;self.fd=None
        require(type(binding) is dict and set(binding)=={"path","device","inode","uid"},"CGROUP_BINDING")
        require(all(type(binding[k]) is int for k in ("device","inode","uid")),"CGROUP_PIN_TYPES")
        path=binding["path"]
        require(type(path) is str and path.startswith("/sys/fs/cgroup/") and
                os.path.realpath(path)==path,"CGROUP_PATH")
        self.fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            st=os.fstat(self.fd)
            require(stat.S_ISDIR(st.st_mode) and (st.st_dev,st.st_ino,st.st_uid)==
                    (binding["device"],binding["inode"],binding["uid"]) and st.st_uid==os.getuid(),"CGROUP_PIN_OWNER")
            self.pin=(st.st_dev,st.st_ino,st.st_uid)
            require(self.read("memory.max").isdigit() and 0<int(self.read("memory.max"))<=268435456,"CGROUP_MEMORY_MAX")
            require(self.read("memory.swap.max")=="0" and self.read("pids.max")=="1","CGROUP_SWAP_PIDS")
            cpu=self.read("cpu.max").split()
            require(len(cpu)==2 and all(x.isdigit() for x in cpu) and 0<int(cpu[0])<=int(cpu[1]),"CGROUP_CPU_MAX")
            require(self.read("cgroup.max.depth")=="0" and self.read("cgroup.max.descendants")=="0","CGROUP_DESCENDANTS")
            if require_empty:
                require(not self.read("cgroup.procs") and not self.read("cgroup.threads"),"CGROUP_NOT_EMPTY")
            else:
                require(self.read("cgroup.procs")==str(os.getpid()) and
                        self.read("cgroup.threads")==str(os.getpid()),"WORKER_NOT_IN_EXACT_CGROUP")
        except BaseException:
            os.close(self.fd);self.fd=None;raise

    def check(self):
        st=self.os.fstat(self.fd)
        require((st.st_dev,st.st_ino,st.st_uid)==self.pin,"CGROUP_PIN_CHANGED")

    def read(self,name):
        self.check()
        fd=self.os.open(name,self.os.O_RDONLY|self.os.O_NOFOLLOW,dir_fd=self.fd)
        try:
            b=self.os.read(fd,4097);require(len(b)<=4096,"CGROUP_CONTROL_SIZE")
            return b.decode("ascii").strip()
        finally:self.os.close(fd)

    def child_join(self):
        self.check()
        fd=self.os.open("cgroup.procs",self.os.O_WRONLY|self.os.O_NOFOLLOW,dir_fd=self.fd)
        try:self.os.write(fd,str(self.os.getpid()).encode())
        finally:self.os.close(fd)

    def close(self):
        if self.fd is not None:
            self.check();self.os.close(self.fd);self.fd=None


class LiveHandle(NativeOwnedHandle):
    def __init__(self,child,owner,cap,clock,stop_end,*,native_ops=None):
        import os,selectors
        super().__init__(child,owner,native_ops=native_ops)
        os=self._os
        self.cap=cap;self.output=bytearray();self.stderr_bytes=0
        self.clock,self.stop_end=clock,stop_end
        self.selector=None;self.streams=(child.stdout,child.stderr)
        self._capture_streams=self.streams;self._capture_selector=None;self._capture_eof=[False,False]
        self._capture_pins=[];self._capture_closers=tuple(stream.close for stream in self._capture_streams)
        self.packet=None;self.packet_offset=0;self.packet_stdin=None;self.packet_stdin_closed=False
        try:
            self.selector=(native_ops.selector() if native_ops is not None else selectors.DefaultSelector())
            self._capture_selector=self.selector
            for stream in self.streams:
                fd=stream.fileno();st=os.fstat(fd);self._capture_pins.append((fd,st.st_dev,st.st_ino,st.st_mode))
                os.set_blocking(fd,False);self.selector.register(stream,selectors.EVENT_READ)
        except BaseException as exc:
            actions=[("partial PIDFD",super().close)]
            if self.selector is not None:actions.append(("partial selector",self.selector.close))
            actions.extend(("partial pipe",s.close) for s in self.streams)
            finalize_owned(actions,exc);raise

    def queue_packet(self,data,deadline):
        require(self.packet is None and type(data) is bytes and len(data)<=65536,
                "PACKET_QUEUE_SCOPE")
        require(finite(deadline) and self.clock()<deadline<=self.stop_end,"PACKET_QUEUE_DEADLINE")
        self.packet=data+b"\n";self.packet_deadline=deadline
        self.packet_stdin=self.child.stdin
        self.packet_fd=self.packet_stdin.fileno()
        st=self._os.fstat(self.packet_fd)
        self.packet_pin=(st.st_dev,st.st_ino,st.st_mode)
        self.packet_last=self.clock()
        self._os.set_blocking(self.packet_fd,False)

    def deliver_packet(self):
        if self.packet is None or self.packet_offset==len(self.packet):return
        now=self.clock()
        require(finite(now) and now>=self.packet_last and now<self.packet_deadline,"PACKET_DELIVERY_DEADLINE")
        self.packet_last=now
        require(self.child.stdin is self.packet_stdin,"PACKET_STDIN_REBOUND")
        st=self._os.fstat(self.packet_fd)
        require((st.st_dev,st.st_ino,st.st_mode)==self.packet_pin,"PACKET_STDIN_FD_REUSED")
        try:
            written=self._os.write(self.packet_fd,self.packet[self.packet_offset:self.packet_offset+4096])
        except BlockingIOError:
            return  # no bytes written; existing supervisor polls the same offset
        require(type(written) is int and 0<written<=min(4096,len(self.packet)-self.packet_offset),"PACKET_WRITE_PROGRESS")
        self.packet_offset+=written
        require(self.clock()<self.packet_deadline,"PACKET_WRITE_CALLBACK_DEADLINE")
        if self.packet_offset==len(self.packet):self.close_packet_stdin()

    def close_packet_stdin(self):
        if self.packet_stdin is None or self.packet_stdin_closed:return
        require(self.child.stdin is self.packet_stdin,"PACKET_STDIN_REBOUND")
        st=self._os.fstat(self.packet_fd)
        require((st.st_dev,st.st_ino,st.st_mode)==self.packet_pin,"PACKET_STDIN_FD_REUSED")
        self.packet_stdin.close();self.packet_stdin_closed=True

    def drain(self):
        self.check_fd();os=self._os
        require(self.streams is self._capture_streams and self.selector is self._capture_selector,"CAPTURE_RESOURCE_REBOUND")
        for _ in range(18):
            require(self.clock()<self.stop_end,"CAPTURE_DRAIN_DEADLINE")
            events=self._capture_selector.select(timeout=0)
            require(type(events) in (list,tuple) and len(events)<=2,"CAPTURE_EVENT_BOUND")
            if not events:break
            for key,_ in events:
                stream=key.fileobj
                require(stream is self._capture_streams[0] or stream is self._capture_streams[1],"CAPTURE_FOREIGN_STREAM")
                index=0 if stream is self._capture_streams[0] else 1
                require(not self._capture_eof[index],"CAPTURE_EVENT_AFTER_EOF")
                require(self.clock()<self.stop_end,"CAPTURE_READ_DEADLINE")
                fd=stream.fileno();st=os.fstat(fd)
                require((fd,st.st_dev,st.st_ino,st.st_mode)==self._capture_pins[index],"CAPTURE_FD_REUSED")
                count=min(65536,self.cap-len(self.output)-self.stderr_bytes+1)
                require(type(count) is int and 0<count<=65536,"CAPTURE_READ_BOUND")
                chunk=os.read(fd,count)
                require(type(chunk) is bytes and len(chunk)<=count,"CAPTURE_READ_TYPE")
                require(self.clock()<self.stop_end,"CAPTURE_READ_DEADLINE")
                if not chunk:self._capture_selector.unregister(stream);self._capture_eof[index]=True
                elif index==0:self.output.extend(chunk)
                else:self.stderr_bytes+=len(chunk)  # discarded, never exported
            require(len(self.output)+self.stderr_bytes<=self.cap,"WORKER_IPC_OUTPUT_LIMIT")

    def capture_complete(self):
        self.drain()
        return self._capture_eof==[True,True]

    def poll(self):
        self.deliver_packet()
        self.drain()
        return super().poll()

    def close(self):
        primary=None
        try:
            if self.child.poll() is None:
                require(self.clock()<self.stop_end,"LIVE_HANDLE_STOP_EXPIRED")
                super().signal("KILL")
                self.child.wait(timeout=max(0.001,self.stop_end-self.clock()))
            self.drain()
        except BaseException as exc:primary=exc
        def selector_close():
            require(self.selector is self._capture_selector,"CAPTURE_SELECTOR_REBOUND")
            self._capture_selector.close()
        def stream_close(index):
            stream=self._capture_streams[index]
            require((self.child.stdout if index==0 else self.child.stderr) is stream,"CAPTURE_STREAM_REBOUND")
            fd=stream.fileno();st=self._os.fstat(fd)
            require((fd,st.st_dev,st.st_ino,st.st_mode)==self._capture_pins[index],"CAPTURE_FD_REUSED")
            current=stream.close;original=self._capture_closers[index]
            require(getattr(current,"__func__",current) is getattr(original,"__func__",original) and getattr(current,"__self__",None) is getattr(original,"__self__",None),"CAPTURE_CLOSE_REBOUND")
            original()
        finalize_owned([("PIDFD",super().close),("selector",selector_close),
                        ("stdout",lambda:stream_close(0)),("stderr",lambda:stream_close(1))] +
                       ([("stdin",self.close_packet_stdin)] if self.packet_stdin is not None else []),primary)
        if primary is not None:raise primary
        return True


def verify_driver_package(package):
    from pathlib import Path
    require(type(package) is dict and set(package)=={"path","files"} and
            type(package["files"]) is dict and 1<=len(package["files"])<=256,"PSYCOPG2_PACKAGE_BINDING")
    root=Path(package["path"])
    require(root.is_absolute() and root.name=="psycopg2" and not root.is_symlink(),"PSYCOPG2_PACKAGE_PATH")
    files={str(p.relative_to(root)):p for p in root.rglob('*') if p.is_file() and '__pycache__' not in p.parts}
    require(set(files)==set(package["files"]),"PSYCOPG2_EXACT_PACKAGE_MEMBERSHIP")
    total=0
    for name,p in files.items():
        total+=p.stat().st_size
        require(total<=64*1024*1024 and p.stat().st_size<=32*1024*1024,"PSYCOPG2_PACKAGE_SIZE")
        require(not p.is_symlink() and hashlib.sha256(p.read_bytes()).hexdigest()==package["files"][name],"PSYCOPG2_PACKAGE_HASH")
    return root


@dataclass(frozen=True)
class Caps:
    work: int = 120
    stop: int = 30
    receipt: int = 1048576
    body: int = 8388608
    rows: int = 20000
    read_rows: int = 20001
    http_wall: int = 45
    connect: int = 5
    read_timeout: int = 15
    statement_ms: int = 5000
    lock_ms: int = 2000
    sql_calls: int = 20032
    def validate(self):
        require(all(type(v) is int for v in asdict(self).values()) and self==Caps(),"EXACT_CAPS")


@dataclass(frozen=True)
class Call:
    call_id: str
    day: str
    cutoff_utc: str
    t0: float
    source_id: int
    source_sha256: str = PRODUCER_SHA
    release: str = RELEASE
    trade_date: str = TRADE_DATE
    url: str = URL
    market_prefix: str = "CNMS"
    dry_run: bool = False
    def validate(self):
        require(type(self.call_id) is str and re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}",self.call_id) is not None,"CALL_ID")
        require(type(self.source_id) is int and self.source_id>0 and type(self.t0) is float and finite(self.t0),"CALL_TYPES")
        require(self.source_sha256==PRODUCER_SHA and self.release==RELEASE and self.trade_date==TRADE_DATE and self.url==URL and self.market_prefix=="CNMS" and self.dry_run is False,"EXACT_FINRA_ARGUMENTS")
        day=date.fromisoformat(self.day);cutoff=datetime.fromisoformat(self.cutoff_utc.replace("Z","+00:00"))
        require(day.isoformat()==self.day and cutoff.tzinfo==timezone.utc and cutoff.date()==day and date.fromisoformat(TRADE_DATE)<=day,"UTC_DAY_CUTOFF")


class Budget:
    def __init__(self,call,caps,clock,utc,*,synthetic=True,outer=None):
        call.validate();caps.validate();self.call,self.caps,self.clock,self.utc=call,caps,clock,utc
        self.outer=None if outer is None else json.loads(canonical(outer));self.outer_pin=canonical(self.outer)
        self.binding=canonical(asdict(call));self.last=call.t0;self.failed=False;self.samples=[];self.observed=0
        self.provenance="SYNTHETIC_INJECTED_CLOCK" if synthetic else "ACTUAL_WORKER_CLOCK"
    def check(self,stage="boundary",*,cleanup=False):
        require(not self.failed or cleanup,"STICKY_BUDGET_FAILURE")
        try:
            require(canonical(asdict(self.call))==self.binding,"CALL_MUTATED")
            now=self.clock();require(finite(now) and now>=self.last,"MONOTONIC_CLOCK")
            self.last=now
            end=self.call.t0+self.caps.work+(self.caps.stop if cleanup else 0)
            require(now<end,"TOTAL_DEADLINE" if cleanup else "WORK_DEADLINE")
            if self.outer is not None:
                require(canonical(self.outer)==self.outer_pin and self.outer["origin"]<=now<self.outer["end"],"SELECTED_OUTER_DEADLINE")
            wall=self.utc();require(type(wall) is datetime and wall.tzinfo==timezone.utc and wall.date().isoformat()==self.call.day and wall<datetime.fromisoformat(self.call.cutoff_utc.replace("Z","+00:00")),"WALL_DAY_CUTOFF")
            observed=self.clock();require(finite(observed) and now<=observed<end,"POST_UTC_ORIGINAL_DEADLINE")
            if self.outer is not None:require(observed<self.outer["end"],"POST_UTC_OUTER_DEADLINE")
            now=observed;self.last=now
            self.observed+=1
            self.samples.append({"stage":stage,"monotonic":now,"utc":wall.isoformat(),"provenance":self.provenance})
            if len(self.samples)>32:self.samples.pop(0)
            return now
        except BaseException:
            self.failed=True;raise


def exported(value):
    if type(value) is tuple:return [exported(x) for x in value]
    if type(value) is list:return [exported(x) for x in value]
    if type(value) is dict:return {str(k):exported(v) for k,v in value.items()}
    if type(value) is Decimal:
        require(value.is_finite(),"EXPORT_NONFINITE_DECIMAL")
        return {"type":"Decimal","exact":str(value)}
    if type(value) is datetime:return {"type":"datetime","iso":value.isoformat()}
    if type(value) is date:return {"type":"date","iso":value.isoformat()}
    require(value is None or type(value) in (bool,str,int,float),"EXPORT_TYPE")
    if type(value) is float:require(math.isfinite(value),"EXPORT_NONFINITE")
    return value


def load_bound_producer(data):
    require(type(data) is bytes and hashlib.sha256(data).hexdigest()==PRODUCER_SHA,"EXECUTED_PRODUCER_PIN")
    tree=ast.parse(data);body=[ast.ImportFrom(module="__future__",names=[ast.alias(name="annotations")],level=0)]
    for n in tree.body:
        if isinstance(n,(ast.Assign,ast.AnnAssign)):
            try:ast.literal_eval(n.value)
            except (TypeError,ValueError):continue
            body.append(n)
        elif isinstance(n,ast.FunctionDef) and n.name in ("parse_daily_short_volume_file","_bounded_error"):
            body.append(n)
        elif isinstance(n,ast.ClassDef) and n.name=="FINRAShortVolumePuller":
            for method in n.body:
                if isinstance(method,ast.FunctionDef):
                    method.decorator_list=[dec for dec in method.decorator_list if not isinstance(dec,ast.Call)]
            body.append(n)
    class NoCatalogBase:
        def __init__(self,*a,**kw):raise Refused("DEFAULT_CATALOG_FACTORY_FORBIDDEN")
    class NoLog:
        def __getattr__(self,n):return lambda *a,**kw:None
    import builtins
    def importer(name,*a,**kw):
        require(name=="__future__","PRODUCER_IMPORT_FORBIDDEN")
        return builtins.__import__(name,*a,**kw)
    ns={"__name__":"bound_finra_producer","__builtins__":dict(vars(builtins),__import__=importer),"date":date,"timedelta":__import__("datetime").timedelta,"BasePuller":NoCatalogBase,"log":NoLog(),"retry_on_failure":lambda **kw:lambda fn:fn}
    # Only definitions/constants execute; config, requests, loguru, Base and factories do not import.
    exec(compile(ast.fix_missing_locations(ast.Module(body=body,type_ignores=[])),"bound_finra_producer","exec"),ns)
    return ns


def _http_finalize(actions, primary=None):
    """Independent HTTP cleanup; raw messages/notes never format or mask primary."""
    first=None
    for action in actions:
        try:action()
        except BaseException as exc:
            if first is None:first=exc
    if primary is None and first is not None:raise first


class HTTPResponse:
    def __init__(self,connection,response,deadline,clock):
        self.connection,self.response,self.deadline,self.clock=connection,response,deadline,clock
        self._response,self._connection=response,connection
        self._finalizers=(response.close,connection.close)
        self.status=response.status;self.encoding=response.getheader("Content-Encoding")
        length=response.getheader("Content-Length")
        require(length is None or type(length) is str and length.isascii() and length.isdecimal(),"HTTP_LENGTH")
        self.length=int(length) if length is not None else None
        self.consumed=0
        self.read_socket=getattr(getattr(getattr(response,"fp",None),"raw",None),"_sock",None)
        require(self.read_socket is not None and callable(getattr(self.read_socket,"settimeout",None)),"HTTP_RESPONSE_SOCKET")
    def read(self,n):
        require(type(n) is int and 0<n<=65536,"HTTP_READ_BOUND")
        remaining=self.deadline-self.clock()
        require(finite(remaining) and remaining>0,"HTTP_READ_BOUND")
        require(self.response is self._response and self.connection is self._connection,"HTTP_RESPONSE_CHANGED")
        if self.length is not None and self.consumed==self.length:
            require(self.clock()<self.deadline,"HTTP_READ_LATE");return b""
        if self._response.fp is None and self._response.isclosed() is True:
            require(self.length is None or self.consumed==self.length,"HTTP_LENGTH_MISMATCH")
            require(self.clock()<self.deadline,"HTTP_READ_LATE");return b""
        current=getattr(getattr(getattr(self._response,"fp",None),"raw",None),"_sock",None)
        require(current is self.read_socket,"HTTP_RESPONSE_SOCKET_CHANGED")
        timeout=min(15,remaining)
        require(finite(timeout) and 0<timeout<=remaining,"HTTP_TIMEOUT_BOUND")
        self.read_socket.settimeout(timeout)
        require(self.clock()<self.deadline and getattr(getattr(getattr(self._response,"fp",None),"raw",None),"_sock",None) is self.read_socket,"HTTP_TIMEOUT_OR_SOCKET_CHANGED")
        data=self._response.read(n)
        require(type(data) is bytes and len(data)<=n,"HTTP_BODY_TYPE")
        self.consumed+=len(data)
        require(self.length is None or self.consumed<=self.length,"HTTP_LENGTH_MISMATCH")
        require(bool(data) or self.length is None or self.consumed==self.length,"HTTP_LENGTH_MISMATCH")
        require(self.clock()<self.deadline,"HTTP_READ_LATE")
        return data
    def close(self):
        _http_finalize(self._finalizers)


class HTTPSExchange:
    def __init__(self,clock,*,connection_factory=None):self.clock,self.factory=clock,connection_factory
    def __call__(self,url,deadline,cap):
        import http.client
        require(url==URL,"HTTP_EXACT_FILE")
        parsed=urlsplit(url);require(parsed.scheme=="https" and parsed.hostname=="cdn.finra.org" and parsed.port is None and not parsed.query and not parsed.fragment,"HTTP_HOST")
        remaining=deadline-self.clock();require(finite(remaining) and remaining>0,"HTTP_CONNECT_DEADLINE")
        con=(self.factory or http.client.HTTPSConnection)(parsed.hostname,timeout=min(5,remaining));response=None
        try:
            con.request("GET",parsed.path,headers={"User-Agent":"GRID bounded FINRA occurrence","Accept":"text/plain","Accept-Encoding":"identity","Connection":"close"})
            response=con.getresponse();require(self.clock()<deadline,"HTTP_HEADER_LATE")
            wrapped=HTTPResponse(con,response,deadline,self.clock)
            require(wrapped.status not in range(300,400),"HTTP_REDIRECT_FORBIDDEN")
            require(wrapped.encoding in (None,"identity") and (wrapped.length is None or wrapped.length<=cap),"HTTP_ENCODING_OR_BODY")
            return wrapped
        except BaseException as primary:
            actions=([response.close] if response is not None else [])+[con.close]
            _http_finalize(actions,primary=primary)
            raise


ROLE_SQL="SELECT current_user, current_database(), r.rolsuper, r.rolbypassrls, r.rolcreaterole, r.rolcreatedb FROM pg_roles r WHERE r.rolname=current_user"
SOURCE_SQL="SELECT id, name FROM public.source_catalog WHERE id=%s AND name=%s LIMIT 2"
TABLE_SQL="SELECT c.oid, c.relkind, c.relrowsecurity, c.relforcerowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=%s LIMIT 2"
TRIGGER_SQL="SELECT t.tgname, pg_get_triggerdef(t.oid), pg_get_functiondef(t.tgfoid) FROM pg_trigger t WHERE t.tgrelid=%s ORDER BY t.tgname LIMIT 65"
UNIQUE_SQL="SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s AND contype IN ('u','p','x') ORDER BY conname LIMIT 65"
READ_SQL="SELECT series_id, obs_date, value, pull_status, pull_timestamp, raw_payload, source_id FROM public.raw_series WHERE source_id=%s AND obs_date=%s AND series_id LIKE %s AND pull_status='SUCCESS' ORDER BY series_id, obs_date, pull_timestamp, id"
LOCK_SQL="SELECT pg_advisory_xact_lock(%s)"
INSERT_SQL="INSERT INTO public.raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) VALUES (%s,%s,%s,%s,%s,'SUCCESS')"
ALLOW_SQL={ROLE_SQL,SOURCE_SQL,TABLE_SQL,TRIGGER_SQL,UNIQUE_SQL,READ_SQL,LOCK_SQL,INSERT_SQL}


class DBSession:
    def __init__(self,backend,mode,budget):
        self.backend,self.mode,self.budget=backend,mode,budget
        self.raw=self.cursor=None;self.owned=None;self.commit_ack="UNKNOWN";self.commit_attempted=False;self.closed=False
    def identity(self):
        require(self.owned is not None and self.raw is self.owned[0] and self.cursor is self.owned[1] and not self.closed,"DB_RESOURCE_REBOUND_OR_CLOSED")
    def __enter__(self):
        self.budget.check("db_connect_before")
        try:
            self.raw=self.backend.connect(self.mode);original=self.raw
            self.owned=(original,None)
            original.set_session(readonly=self.mode=="read",autocommit=False,isolation_level="REPEATABLE READ")
            self.budget.check("db_connect_after")
            self.cursor=original.cursor();self.owned=(original,self.cursor)
            self.budget.check("db_cursor_after");return self
        except BaseException as exc:
            self.close(primary=exc);raise
    def execute(self,sql,args=()):
        self.identity();self.budget.check("db_execute_before")
        require(sql in ALLOW_SQL and not (self.mode=="read" and sql in (INSERT_SQL,LOCK_SQL)),"SQL_ALLOWLIST")
        self.backend.calls+=1;require(self.backend.calls<=self.budget.caps.sql_calls,"SQL_COUNT_CAP")
        self.cursor.execute(sql,args);self.identity();self.budget.check("db_execute_after")
        return self.cursor
    def rows(self,cap):
        self.identity();require(type(cap) is int and 0<cap<=self.budget.caps.read_rows,"FETCH_CAP")
        self.budget.check("fetch_before")
        rows=self.cursor.fetchmany(cap+1)
        self.identity();self.budget.check("fetch_after")
        require(type(rows) in (list,tuple) and len(rows)<=cap,"FETCH_OVERFLOW")
        # A driver returning a shorter batch need not be EOF. Check one more bounded row.
        tail=self.cursor.fetchmany(1);self.identity();self.budget.check("fetch_eof")
        require(type(tail) in (list,tuple) and len(tail)==0,"FETCH_INCOMPLETE_OR_OVERFLOW")
        return list(rows)
    def commit(self):
        self.identity();self.budget.check("commit_before");self.commit_attempted=True
        self.raw.commit();self.commit_ack="ACKNOWLEDGED"
        self.identity();self.budget.check("commit_after")
    def close(self,primary=None):
        if self.closed:return
        owned=self.owned
        if owned is None:return
        if self.raw is not owned[0] or (owned[1] is not None and self.cursor is not owned[1]):
            primary=primary or Refused("DB_CLOSE_FOREIGN_REFUSAL")
        # Finalizers always target the privately retained original resources.
        actions=[]
        if self.commit_ack!="ACKNOWLEDGED":actions.append(("own transaction rollback",owned[0].rollback))
        if owned[1] is not None:actions.append(("own DB cursor",owned[1].close))
        actions.append(("own DB connection",owned[0].close))
        try:finalize_owned(actions,primary)
        finally:self.closed=True
        self.budget.check("db_close",cleanup=True)
        if primary is not None:raise primary
    def __exit__(self,typ,exc,tb):self.close(primary=exc);return False


class DBAPIBackend:
    def __init__(self,driver,write_dsn,read_dsn):
        self.driver,self._dsns=driver,{"write":write_dsn,"read":read_dsn};self.calls=0
    def connect(self,mode):
        require(mode in self._dsns,"DB_MODE")
        return self.driver.connect(self._dsns[mode],connect_timeout=5,options="-c statement_timeout=5000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=60000")
    def session(self,mode,budget):return DBSession(self,mode,budget)


def validate_db_plan(plan):
    keys={"database","read_role","write_role","table_oid","catalog_oid","trigger_sha256","unique_sha256"}
    require(type(plan) is dict and set(plan)==keys,"DB_PLAN_FIELDS")
    require(all(type(plan[k]) is str and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}",plan[k]) is not None for k in ("database","read_role","write_role")),"DB_PLAN_IDENTIFIERS")
    require(plan["read_role"]!=plan["write_role"] and all(type(plan[k]) is int and plan[k]>0 for k in ("table_oid","catalog_oid")),"DB_ROLE_TABLE_BINDING")
    require(all(type(plan[k]) is str and re.fullmatch(r"[a-f0-9]{64}",plan[k]) is not None for k in ("trigger_sha256","unique_sha256")),"DB_PLAN_HASHES")


def verify_database(session,call,plan):
    session.execute(ROLE_SQL);role=session.rows(1)
    require(len(role)==1 and canonical(exported(role[0]))==canonical([plan[session.mode+"_role"],plan["database"],False,False,False,False]),"ACTUAL_DB_ROLE")
    for table_name,oid_key in (("raw_series","table_oid"),("source_catalog","catalog_oid")):
        session.execute(TABLE_SQL,(table_name,));table=session.rows(2)
        require(len(table)==1 and canonical(exported(table[0]))==canonical([plan[oid_key],"r",False,False]),"ACTUAL_TARGET_TABLE")
    session.execute(SOURCE_SQL,(call.source_id,"FINRA_SHORT_VOLUME"));catalog=session.rows(2)
    require(len(catalog)==1 and canonical(exported(catalog[0]))==canonical([call.source_id,"FINRA_SHORT_VOLUME"]),"EXISTING_CATALOG_ONLY")
    for sql,field in ((TRIGGER_SQL,"trigger_sha256"),(UNIQUE_SQL,"unique_sha256")):
        session.execute(sql,(plan["table_oid"],));rows=session.rows(64)
        encoded=canonical(exported(rows)).encode();require(len(encoded)<=131072 and hashlib.sha256(encoded).hexdigest()==plan[field],"TRIGGER_OR_UNIQUE_PLAN_CHANGED")


def read_partition(session,call):
    session.execute(READ_SQL,(call.source_id,date.fromisoformat(TRADE_DATE),"finra:short_volume:%"))
    rows=session.rows(Caps().rows);keys=set()
    read_observed_utc=datetime.fromisoformat(session.budget.samples[-1]["utc"])
    for row in rows:
        require(type(row) in (tuple,list) and len(row)==7,"READBACK_ROW_SHAPE")
        require(type(row[0]) is str and row[0].startswith("finra:short_volume:") and type(row[1]) is date and row[1].isoformat()==TRADE_DATE and row[3]=="SUCCESS" and type(row[6]) is int and row[6]==call.source_id,"READBACK_KEY_BOUND")
        require(type(row[2]) in (int,float,Decimal) and math.isfinite(row[2]) and row[2]>=0,"READBACK_FINITE_VALUE")
        require(type(row[3]) is str and type(row[4]) is datetime and row[4].tzinfo is not None and row[4].utcoffset() is not None and row[4]<=read_observed_utc,"READBACK_TYPED_TIMESTAMP_STATUS")
        require((row[0],row[1]) not in keys,"DUPLICATE_PERSISTED_KEY")
        keys.add((row[0],row[1]))
    data=canonical(exported(rows)).encode();require(len(data)<=8*1024*1024,"READBACK_BYTE_CAP")
    return keys,{"row_count":len(rows),"sha256":hashlib.sha256(data).hexdigest(),"typed_values":"exact Decimal/date/datetime export; raw rows not in receipt"}


class OccurrenceIO:
    def __init__(self,call,caps,producer,backend,exchange,budget,db_plan,receipt):
        self.call,self.caps,self.producer,self.backend,self.exchange,self.budget,self.plan,self.receipt=call,caps,producer,backend,exchange,budget,db_plan,receipt
        self.source_id=call.source_id;self.used=False
        self.selected=receipt.get("schema")=="finra-occurrence-worker-v2";self.transient_code="NONE";self.close_failed=False
        self.effects={"db_started":False,"write_bearing":False,"commit_attempted":False,"http_requests":0,"body_bytes":0,"http_elapsed":0.0,"transport_cleanup":False}

    def pull(self,puller,*,trade_date,url,market_prefix,dry_run):
        require(not self.used,"SINGLE_OCCURRENCE_ONLY");self.used=True
        require(type(trade_date) is str and trade_date==TRADE_DATE and url==URL and market_prefix=="CNMS" and dry_run is False and puller.source_id==self.source_id,"PRODUCER_DISPATCH_BINDING")
        raw=self.download() if self.selected else None
        if self.selected and raw is None:
            self.receipt["transaction_ack"]="NOT_ATTEMPTED";self.receipt["committed_rows"]=0
            return {"status":"SKIPPED","rows_inserted":0,"skipped_reason":"FILE_UNAVAILABLE_OR_NOT_PUBLISHED"}
        self.budget.check("read_init")
        if self.selected:self.effects["db_started"]=True
        with self.backend.session("read",self.budget) as conn:
            verify_database(conn,self.call,self.plan);_,self.receipt["before"]=read_partition(conn,self.call)
        if not self.selected:raw=self.download()
        if raw is None:
            self.receipt["transaction_ack"]="NOT_ATTEMPTED";self.receipt["committed_rows"]=0
            return {"status":"SKIPPED","rows_inserted":0,"skipped_reason":"FILE_UNAVAILABLE_OR_NOT_PUBLISHED"}
        text=raw.decode("utf-8","strict");lines=[line.strip() for line in text.splitlines() if line.strip()]
        require(2<=len(lines)<=self.caps.rows+2 and lines[-1].isascii() and lines[-1].isdecimal() and int(lines[-1])==len(lines)-2,"COMPLETE_FILE_TRAILER_COUNT")
        header=tuple(field.strip().upper() for field in lines[0].split("|"))
        require(header==("DATE","SYMBOL","SHORTVOLUME","SHORTEXEMPTVOLUME","TOTALVOLUME","MARKET"),"EXACT_ORDERED_FILE_HEADER")
        raw_dates=[line.split("|",1)[0].strip() for line in lines[1:-1]]
        require(all(len(value)==8 and value.isascii() and value.isdecimal() and value==TRADE_DATE.replace("-","") for value in raw_dates),"EXACT_RAW_FILE_DATE")
        parsed=self.producer["parse_daily_short_volume_file"](text)
        require(type(parsed) is dict and type(parsed.get("rows")) is list and type(parsed.get("skipped")) is int and parsed["skipped"]==0 and len(parsed["rows"])==len(lines)-2,"MALFORMED_OR_INCOMPLETE_FILE")
        rows=parsed["rows"];require(len(rows)<=self.caps.rows,"PARSED_ROW_CAP")
        targets={}
        for row in rows:
            require(type(row.get("date")) is date and row["date"].isoformat()==TRADE_DATE,"EXACT_FILE_DATE")
            require(type(row.get("symbol")) is str and 0<len(row["symbol"])<=64 and type(row.get("market")) is str and 0<len(row["market"])<=64,"FILE_SYMBOL_MARKET")
            require(all(type(row[k]) in (int,float) and math.isfinite(row[k]) and row[k]>=0 for k in ("short_volume","short_exempt_volume","total_volume")),"FILE_FINITE_VOLUME")
            sid=puller.series_id(row["symbol"]);targets.setdefault((sid,row["date"]),row)
        self.receipt["input"]={"bytes":len(raw),"sha256":hashlib.sha256(raw).hexdigest(),"parsed_rows":len(rows),"unique_keys":len(targets),"publication_vintage":"UNKNOWN"}
        if not targets:
            self.receipt["transaction_ack"]="NOT_ATTEMPTED";self.receipt["committed_rows"]=0
            return {"status":"NO_NEW_DATA","rows_inserted":0}
        conn=None;staged=0
        try:
            if self.selected:self.effects["write_bearing"]=True
            with self.backend.session("write",self.budget) as conn:
                verify_database(conn,self.call,self.plan)
                lock=int.from_bytes(hashlib.sha256(("FINRA_SHORT_VOLUME|pull|"+TRADE_DATE).encode()).digest()[:8],"big") & 0x7FFFFFFFFFFFFFFF
                conn.execute(LOCK_SQL,(lock,))
                existing,_=read_partition(conn,self.call)
                self.receipt["transaction_ack"]="UNKNOWN"
                for key,row in targets.items():
                    self.budget.check("row_write")
                    if key in existing:continue
                    payload={"short_exempt_volume":row["short_exempt_volume"],"total_volume":row["total_volume"],"symbol":row["symbol"],"market":row["market"],"source":"FINRA Reg SHO Daily Short Sale Volume File","docs_url":self.producer["_DOCS_URL"]}
                    self.receipt["write_attempts"]+=1
                    cursor=conn.execute(INSERT_SQL,(key[0],self.source_id,key[1],row["short_volume"],json.dumps(payload)))
                    count=getattr(cursor,"rowcount",None)
                    require(type(count) is int and 0<=count<=1,"AFFECTED_ROWCOUNT_UNKNOWN")
                    staged+=count
                if self.selected:self.effects["commit_attempted"]=True
                conn.commit()
            self.receipt["transaction_ack"]="ACKNOWLEDGED";self.receipt["committed_rows"]=staged
            return {"status":"SUCCESS" if staged else "NO_NEW_DATA","rows_inserted":staged}
        finally:
            if conn is not None and conn.commit_ack=="ACKNOWLEDGED":
                self.receipt["transaction_ack"]="ACKNOWLEDGED";self.receipt["committed_rows"]=staged
    def download(self):
        started=self.budget.check("download_start");self.close_failed=False
        deadline=min(self.call.t0+self.caps.work,self.budget.check("http_start")+self.caps.http_wall)
        response=None;body=bytearray();primary=None
        try:
            if self.selected:self.effects["http_requests"]+=1
            response=self.exchange(URL,deadline,self.caps.body)
            self.receipt["http"]={"requests":1,"status":response.status,"url":URL}
            require(type(response.status) is int,"HTTP_STATUS_TYPE")
            if response.status in (403,404):
                return None
            if self.selected and response.status in (429,502,503,504):raise PreDBTransient("HTTP_TRANSIENT")
            require(response.status==200 and response.encoding in (None,"identity"),"HTTP_RESPONSE_STATUS_ENCODING")
            while True:
                self.budget.check("http_read")
                require(self.budget.last<deadline,"HTTP_AGGREGATE_WALL")
                chunk=response.read(min(65536,self.caps.body-len(body)+1))
                require(type(chunk) is bytes,"HTTP_BODY_TYPE")
                body.extend(chunk)
                if self.selected:self.effects["body_bytes"]=len(body)
                require(len(body)<=self.caps.body and self.budget.check("http_chunk")<deadline,"HTTP_BODY_OR_WALL_LIMIT")
                if not chunk:break
        except BaseException as exc:
            primary=exc
            if self.selected and (type(exc) in (TimeoutError,ConnectionError) or type(exc) is PreDBTransient):
                self.transient_code="HTTP_TRANSIENT" if type(exc) is PreDBTransient else "TRANSPORT_TIMEOUT" if type(exc) is TimeoutError else "TRANSPORT_CONNECTION"
            raise
        finally:
            if response is not None:
                def close_response():
                    try:response.close()
                    except BaseException:
                        self.close_failed=True;raise
                _http_finalize((close_response,lambda:self.budget.check("http_close",cleanup=True)),primary=primary)
            if self.selected:
                self.effects["http_elapsed"]=max(0.0,self.budget.clock()-started)
                self.effects["transport_cleanup"]=not self.close_failed
        return bytes(body)
    def after(self):
        with self.backend.session("read",self.budget) as conn:
            verify_database(conn,self.call,self.plan);_,self.receipt["after"]=read_partition(conn,self.call)


def execute_worker(packet,*,producer,backend,exchange,clock,utc,synthetic=True):
    packet=json.loads(canonical(packet))  # private value authority before any callback
    call=Call(**packet["call"]);caps=Caps(**packet["caps"]);call.validate();caps.validate();validate_db_plan(packet["db_plan"])
    selected=packet.get("schema")==ATTEMPT_SCHEMA
    if selected:validate_attempt_packet(packet,clock(),utc())
    budget=Budget(call,caps,clock,utc,synthetic=synthetic,outer=packet["selected_workflow"]["frame"] if selected else None)
    receipt={"schema":"finra-occurrence-worker-v1","binding":asdict(call),"base_tree":BASE_TREE,"status":"FAILED","write_attempts":0,"committed_rows":None,"transaction_ack":"NOT_ATTEMPTED","before":"UNKNOWN","after":"UNKNOWN","native_accepted":False,"Card_PIT_accepted":False,"live_qualified":False,"synthetic_only":synthetic,"producer_projection":"exact pinned parser/class/constants; default factories/imports excluded"}
    if selected:
        receipt["schema"]="finra-occurrence-worker-v2"
        receipt["selected_workflow"]=packet["selected_workflow"]
        receipt["attempt_packet_sha256"]=hashlib.sha256(canonical(packet).encode()).hexdigest()
    io=OccurrenceIO(call,caps,producer,backend,exchange,budget,packet["db_plan"],receipt)
    try:
        budget.check("worker_start")
        puller=producer["FINRAShortVolumePuller"](None,occurrence_io=io)
        result=puller.pull(trade_date=TRADE_DATE,url=URL,market_prefix="CNMS",dry_run=False)
        receipt["result"]=result;receipt["status"]=result["status"]
    except BaseException as exc:
        receipt["error_type"]=("PRE_DB_TRANSPORT" if io.transient_code!="NONE" else "UNKNOWN") if selected else type(exc).__name__
        receipt["status"]="PARTIAL" if receipt["transaction_ack"]=="ACKNOWLEDGED" else "FAILED"
    try:
        if not selected or io.effects["db_started"]:io.after();budget.check("worker_complete")
    except BaseException as exc:
        receipt["after"]="UNKNOWN";receipt["after_error_type"]="UNKNOWN" if selected else type(exc).__name__
        receipt["status"]="PARTIAL" if receipt["transaction_ack"]=="ACKNOWLEDGED" else "FAILED"
    if selected:
        eligible=False
        if io.transient_code!="NONE" and not io.effects["db_started"] and io.effects["transport_cleanup"] and not budget.failed:
            try:budget.check("retry_classification");eligible=receipt["status"]=="FAILED"
            except BaseException:pass
        if eligible:receipt["committed_rows"]=0
        receipt["effects"]=io.effects
        receipt["retry_classification"]={"phase":"PRE_DB_TRANSPORT" if eligible else "TERMINAL","code":io.transient_code if eligible else "NONE","eligible":eligible}
    receipt["clock"]={"observed_boundaries":budget.observed,"last_samples":budget.samples,"provenance":budget.provenance}
    require(len(canonical(exported(receipt)).encode())<=caps.receipt,"WORKER_RECEIPT_CAP")
    return receipt


def bind_live(admission,expected_sha,now):
    require(type(admission) is bytes and len(admission)<=65536 and type(expected_sha) is str and hashlib.sha256(admission).hexdigest()==expected_sha,"ADMISSION_BYTES_PIN")
    packet=json.loads(admission)
    require(type(packet) is dict,"FINRA_ADMISSION_SCHEMA")
    selected=packet.get("schema")==ATTEMPT_SCHEMA
    keys={"schema","status","call","caps","runtime","db_plan"}|({"selected_workflow"} if selected else set())
    require(set(packet)==keys and packet["schema"] in ("finra-occurrence-admission-v1",ATTEMPT_SCHEMA) and packet["status"]=="ADMITTED","FINRA_ADMISSION_SCHEMA")
    if selected:validate_attempt_packet(packet,now,None)
    call=Call(**packet["call"]);caps=Caps(**packet["caps"]);call.validate();caps.validate();validate_db_plan(packet["db_plan"])
    require(finite(now) and call.t0<=now<call.t0+5,"FRESH_ACTUAL_T0")
    runtime=packet["runtime"]
    require(type(runtime) is dict and set(runtime)=={"python","python_sha256","driver","driver_sha256","producer","driver_package","cgroup","receipt_root"},"RUNTIME_BINDINGS_REQUIRED")
    require(all(type(runtime[k]) is str and runtime[k].startswith("/") for k in ("python","driver","producer")),"RUNTIME_PATHS")
    require(all(type(runtime[k]) is str and re.fullmatch(r"[0-9a-f]{64}",runtime[k]) is not None for k in ("python_sha256","driver_sha256")),"RUNTIME_HASHES")
    # Canonical immutable packet, typed dataclass binding checked again in child.
    bound=canonical(packet).encode();require(len(bound)<=65536,"PACKET_CAP")
    return bound,call,caps


class NativeRuntime:
    """Concrete default future route; none of these native methods run in tests."""
    def __init__(self,*,native_api=None,clock=None,wait=None,utc=None):
        import time
        self.clock=clock or time.monotonic;self.wait=wait or time.sleep
        self.utc=utc or (lambda:datetime.now(timezone.utc))
        self.api=native_api
        self.injected=native_api is not None or clock is not None or wait is not None or utc is not None

    def native(self):
        if self.api is None:
            import os,subprocess,resource,types
            self.api=types.SimpleNamespace(os=os,Popen=subprocess.Popen,setrlimit=resource.setrlimit,
                RLIMIT_AS=resource.RLIMIT_AS,RLIMIT_CPU=resource.RLIMIT_CPU,RLIMIT_CORE=resource.RLIMIT_CORE,
                handle=LiveHandle)
        return self.api

    def preflight(self,packet):
        import os,sys
        from pathlib import Path
        r=packet["runtime"]
        require(bool(os.environ.get(WRITE_ENV)) and bool(os.environ.get(READ_ENV)),"DEPENDENCY_NAMED_DSN_ENV_MISSING")
        for field,pin in (("python",r["python_sha256"]),("driver",r["driver_sha256"]),
                          ("producer",PRODUCER_SHA)):
            p=Path(r[field]);require(p.is_absolute() and not p.is_symlink(),"RUNTIME_ABSOLUTE_FILE")
            require(hashlib.sha256(p.read_bytes()).hexdigest()==pin,"RUNTIME_FILE_HASH")
        require(Path(r["python"]).resolve()==Path(sys.executable).resolve() and
                Path(r["driver"]).resolve()==Path(__file__).resolve(),"EXECUTING_RUNTIME_BINDING")
        require(callable(getattr(os,"pidfd_open",None)),"DEPENDENCY_OS_PIDFD_OPEN")
        import signal
        require(callable(getattr(signal,"pidfd_send_signal",None)),"DEPENDENCY_SIGNAL_PIDFD_SEND")
        verify_driver_package(r["driver_package"])
        cg=DelegatedCgroup(r["cgroup"])
        try:
            rb=r["receipt_root"]
            require(type(rb) is dict and set(rb)=={"path","device","inode","uid"},"RECEIPT_ROOT_BINDING")
            require(type(rb["path"]) is str and rb["path"].startswith("/") and
                    os.path.realpath(rb["path"])==rb["path"] and
                    all(type(rb[k]) is int for k in ("device","inode","uid")),"RECEIPT_ROOT_FIELDS")
            fd=os.open(rb["path"],os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            st=os.fstat(fd)
            if not ((st.st_dev,st.st_ino,st.st_uid)==(rb["device"],rb["inode"],rb["uid"]) and
                    st.st_uid==os.getuid() and st.st_mode & 0o777==0o700):
                os.close(fd);raise Refused("RECEIPT_ROOT_PIN_MODE")
            return {"cgroup":cg,"receipt_fd":fd,"receipt_original_fd":fd,"receipt_pin":(st.st_dev,st.st_ino,st.st_uid)}
        except BaseException:cg.close();raise

    def consume_workflow(self,packet,workflow_id):
        require(workflow_id==packet["selected_workflow"]["frame"]["workflow_id"],"WORKFLOW_MARKER_IDENTITY")
        context=self.preflight(packet);primary=None;fd=None
        try:
            os=self.native().os;self.verify_receipt_fd(context)
            fd=os.open(workflow_id+".workflow.spent",os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=context["receipt_fd"])
            data=canonical({"workflow_id":workflow_id,"admission_sha256":packet["selected_workflow"]["frame"]["admission_sha256"]}).encode()
            offset=0
            while offset<len(data):
                count=os.write(fd,data[offset:]);require(type(count) is int and 0<count<=len(data)-offset,"WORKFLOW_MARKER_WRITE");offset+=count
            os.fsync(fd)
        except BaseException as exc:primary=exc;raise
        finally:
            actions=[]
            if fd is not None:actions.append(("workflow marker",lambda:self.native().os.close(fd)))
            actions.append(("workflow context",lambda:self.close(context)))
            finalize_owned(actions,primary=primary)
        return True

    def launch(self,context,packet_bytes):
        api=self.native();os=api.os
        packet=json.loads(packet_bytes);r=packet["runtime"]
        self.verify_receipt_fd(context)
        # Consumption is exclusive and occurs once; never retry this call ID.
        marker=os.open(packet["call"]["call_id"]+".spent",os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,
                       0o600,dir_fd=context["receipt_fd"])
        os.close(marker)
        env={"LANG":"C.UTF-8","TZ":"UTC","GRID_GIT_SINK_DISABLED":"1","PYTHONDONTWRITEBYTECODE":"1"}
        for name in ("GRID_FINRA_OCCURRENCE_WRITE_DSN","GRID_FINRA_OCCURRENCE_READ_DSN"):
            require(bool(os.environ.get(name)),"DEPENDENCY_NAMED_DSN_ENV_MISSING")
            env[name]=os.environ[name]
        def limits_and_join():
            context["cgroup"].child_join()
            api.setrlimit(api.RLIMIT_AS,(512*1024*1024,)*2)
            api.setrlimit(api.RLIMIT_CPU,(30,30));api.setrlimit(api.RLIMIT_CORE,(0,0))
        argv=(r["python"],"-I","-S","-B",r["driver"],"--worker","--packet-sha256",hashlib.sha256(packet_bytes).hexdigest())
        child=api.Popen(argv,stdin=-1,stdout=-1,stderr=-1,
                               env=env,close_fds=True,pass_fds=(context["cgroup"].fd,),
                               preexec_fn=limits_and_join,start_new_session=True)
        handle=None
        try:
            handle=api.handle(child,context,Caps().receipt,self.clock,packet["call"]["t0"]+150)
            require(context["cgroup"].read("cgroup.procs")==str(child.pid),"WORKER_CGROUP_MEMBERSHIP")
            handle.queue_packet(packet_bytes,packet["call"]["t0"]+Caps().work)
            return handle
        except BaseException as exc:
            if handle is not None:
                finalize_owned([("unreturned owned worker",handle.close)],exc)
            else:
                # Popen's unreaped direct child retains its PID; never a supplied PID.
                try:
                    child.kill();child.wait(timeout=max(0.001,packet["call"]["t0"]+150-self.clock()))
                except BaseException:
                    try:
                        note=getattr(exc,"add_note",None)
                        if callable(note):note("owned child stop unverified")
                    except BaseException:pass
                actions=[("unreturned child pipe",s.close) for s in
                         (getattr(child,"stdin",None),getattr(child,"stdout",None),getattr(child,"stderr",None)) if s is not None]
                finalize_owned(actions,exc)
            if handle is not None:finalize_owned([("unreturned stdin",child.stdin.close)],exc)
            raise

    def verify_receipt_fd(self,context):
        import stat
        os=self.native().os
        require(type(context["receipt_fd"]) is int and type(context["receipt_original_fd"]) is int and
                context["receipt_fd"]==context["receipt_original_fd"],"RECEIPT_FD_REBOUND")
        st=os.fstat(context["receipt_fd"])
        require((st.st_dev,st.st_ino,st.st_uid)==context["receipt_pin"] and
                st.st_uid==os.getuid() and stat.S_ISDIR(getattr(st,"st_mode",0)) and
                st.st_mode & 0o777==0o700,"RECEIPT_ROOT_CHANGED")

    def persist(self,context,call_id,receipt):
        os=self.native().os
        self.verify_receipt_fd(context)
        data=canonical(receipt).encode();require(len(data)<=Caps().receipt,"FINAL_RECEIPT_LIMIT")
        fd=os.open(call_id+".json",os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=context["receipt_fd"])
        try:
            offset=0
            while offset<len(data):
                written=os.write(fd,data[offset:]);require(type(written) is int and written>0,"RECEIPT_WRITE_PROGRESS")
                offset+=written
            os.fsync(fd)
        finally:os.close(fd)

    def close(self,context):
        os=self.native().os
        def close_receipt():
            self.verify_receipt_fd(context)
            os.close(context["receipt_fd"])
        finalize_owned([("receipt directory",close_receipt),
                        ("delegated cgroup FD",context["cgroup"].close)])


def run_live(admission=None,expected_sha=None,*,runtime=None,**unsupported):
    require(not unsupported,"LIVE_BOUNDARY_UNQUALIFIED: UNSUPPORTED_ARGUMENTS")
    require(type(admission) is bytes and type(expected_sha) is str,
            "LIVE_BOUNDARY_UNQUALIFIED: ADMISSION_BYTES_AND_PIN_REQUIRED")
    runtime=runtime or NativeRuntime()
    original_clock,original_utc=runtime.clock,runtime.utc
    bound,call,caps=bind_live(admission,expected_sha,original_clock())
    require_readers(runtime,original_clock,original_utc)
    selected=json.loads(bound).get("schema")==ATTEMPT_SCHEMA
    budget=Budget(call,caps,runtime.clock,runtime.utc,synthetic=type(runtime) is not NativeRuntime or runtime.injected,outer=json.loads(bound)["selected_workflow"]["frame"] if selected else None)
    budget.check()
    require((datetime.fromisoformat(call.cutoff_utc.replace("Z","+00:00"))-runtime.utc()).total_seconds()
            >=caps.work+caps.stop,"INSUFFICIENT_UTC_WINDOW")
    packet=json.loads(bound)
    budget.check("preflight_ready");require_readers(runtime,original_clock,original_utc)
    context=runtime.preflight(packet)
    handle=None;stop=None;error=None;worker=None
    receipt={"schema":"finra-occurrence-parent-v1","binding":asdict(call),"base_tree":BASE_TREE,
             "child_receipt":None,"write_attempts":"UNKNOWN","transaction_ack":"UNKNOWN",
             "after":"UNKNOWN","committed_rows":None,"native_accepted":False,"Card_PIT_accepted":False,"live_qualified":False}
    if selected:receipt["selected_workflow"]=packet["selected_workflow"];receipt["attempt_packet_sha256"]=expected_sha
    receipt["synthetic_only"]=type(runtime) is not NativeRuntime or runtime.injected
    receipt["resource_semantics"]={"memory.max":"charged-memory cap; temporary overshoot possible",
          "aggregate_RSS":"NOT_EQUIVALENT_OR_CLAIMED","pids.max":1,
          "migration":"not an admission barrier against unrelated cgroup migrations"}
    try:
        require_readers(runtime,original_clock,original_utc)
        require(canonical(packet).encode()==bound,"PARENT_PACKET_MUTATED")
        budget.check()
        require((datetime.fromisoformat(call.cutoff_utc.replace("Z","+00:00"))-runtime.utc()).total_seconds()
                >=call.t0+caps.work+caps.stop-runtime.clock(),"UTC_WINDOW_EXPIRED_DURING_PREFLIGHT")
        budget.check("launch_ready");require_readers(runtime,original_clock,original_utc)
        handle=runtime.launch(context,bound)
        require_readers(runtime,original_clock,original_utc)
        worker=OwnedWorker(handle,context,runtime.clock,runtime.wait,call.t0+caps.work,call.t0+caps.work+caps.stop)
        stop=worker.finish()
        receipt["stop"]=stop
        require(stop.get("stdout_eof") is True and stop.get("stderr_eof") is True,"PARENT_BOTH_PIPE_EOF_REQUIRED")
        require(stop["exit_code"]==0,"WORKER_EXIT_NOT_ZERO")
        if stop["exit_code"]==0:
            child=json.loads(bytes(handle.output))
            require(type(child) is dict and child.get("schema")==("finra-occurrence-worker-v2" if selected else "finra-occurrence-worker-v1") and
                    type(child.get("binding")) is dict and child.get("base_tree")==BASE_TREE,
                    "CHILD_RECEIPT_BINDING")
            if selected:validate_selected_child(child,packet,expected_sha)
            child_call=Call(**child["binding"])
            child_call.validate()
            require(canonical(child["binding"])==canonical(asdict(call)),"CHILD_BINDING_EXACT_TYPES")
            require(type(child.get("write_attempts")) is int and 0<=child["write_attempts"]<=caps.rows and
                    child.get("transaction_ack") in ("NOT_ATTEMPTED","UNKNOWN","ACKNOWLEDGED","ROLLBACK_ACKNOWLEDGED") and
                    child.get("native_accepted") is False and child.get("Card_PIT_accepted") is False and
                    child.get("live_qualified") is False,"CHILD_ACCOUNTING_TYPES")
            require(child.get("committed_rows") is None or type(child["committed_rows"]) is int and 0<=child["committed_rows"]<=child["write_attempts"],"CHILD_COMMITTED_COUNT")
            require(child.get("transaction_ack")!="ACKNOWLEDGED" or type(child.get("committed_rows")) is int,"CHILD_ACK_COUNT")
            receipt["committed_rows"]=child.get("committed_rows")
            receipt["child_receipt"]=child
            receipt["write_attempts"]=child.get("write_attempts","UNKNOWN")
            receipt["transaction_ack"]=child.get("transaction_ack","UNKNOWN")
            receipt["after"]=child.get("after","UNKNOWN")
    except BaseException as exc:
        error=type(exc).__name__
        receipt["error_type"]=error
        # Bounded rescue on the ORIGINAL owned handle only; no new worker/DB recovery.
        if handle is not None and stop is None:
            try:
                OwnedWorker(handle,context,runtime.clock,runtime.wait,
                            min(runtime.clock(),call.t0+caps.work),call.t0+caps.work+caps.stop).finish()
            except BaseException:receipt["stop_unverified"]=True
    finally:
        stop_end=call.t0+caps.work+caps.stop
        last=max(budget.last,worker.last if worker is not None and finite(worker.last) else budget.last)
        observations=[];failures=[]
        receipt["parent_finalization"]={"state":"PENDING_AT_PERSIST","stop_end":stop_end}
        def final_sample(stage):
            nonlocal last
            try:
                now=runtime.clock()
                require(finite(now) and now>=last and now<stop_end,"PARENT_FINAL_DEADLINE_OR_CLOCK")
                last=now
                observations.append({"stage":stage,"monotonic":now})
                return True
            except BaseException as exc:
                failures.append({"stage":stage,"error_type":type(exc).__name__})
                return False
        if final_sample("before_persist"):
            try:
                require(canonical(packet).encode()==bound,"PARENT_PACKET_MUTATED")
                runtime.persist(context,call.call_id,receipt)
            except BaseException as exc:failures.append({"stage":"persist","error_type":type(exc).__name__})
        final_sample("after_persist")
        final_sample("before_close")
        try:runtime.close(context)
        except BaseException as exc:failures.append({"stage":"close","error_type":type(exc).__name__})
        final_sample("after_close")
        receipt["parent_finalization"]={"state":"FAILED" if failures else "CLOSED_WITHIN_BOUND",
             "stop_end":stop_end,"observations":observations,"failures":failures,
             "persisted_version":"PENDING_AT_PERSIST; returned/CLI frame carries final close proof"}
        if failures:
            if "error_type" in receipt:receipt["primary_error_type"]=receipt["error_type"]
            receipt["error_type"]="ParentFinalizationRefused"
    return receipt


def worker_main(expected_sha):
    import os,sys,time,importlib
    from pathlib import Path
    data=sys.stdin.buffer.readline(65538)
    require(data.endswith(b"\n") and len(data)<=65537 and hashlib.sha256(data[:-1]).hexdigest()==expected_sha,"WORKER_INPUT_PIN")
    packet=json.loads(data);call=Call(**packet["call"]);caps=Caps(**packet["caps"]);call.validate();caps.validate();validate_db_plan(packet["db_plan"])
    require(packet["schema"] in ("finra-occurrence-admission-v1",ATTEMPT_SCHEMA) and packet["status"]=="ADMITTED" and os.environ.get("GRID_GIT_SINK_DISABLED")=="1","WORKER_SOURCE_POLICY")
    if packet["schema"]==ATTEMPT_SCHEMA:validate_attempt_packet(packet,time.monotonic(),datetime.now(timezone.utc))
    r=packet["runtime"]
    require(Path(r["driver"]).resolve()==Path(__file__).resolve() and hashlib.sha256(Path(r["driver"]).read_bytes()).hexdigest()==r["driver_sha256"] and Path(r["python"]).resolve()==Path(sys.executable).resolve() and hashlib.sha256(Path(r["python"]).read_bytes()).hexdigest()==r["python_sha256"],"WORKER_EXECUTING_PINS")
    cg=DelegatedCgroup(r["cgroup"],require_empty=False);cg.close()
    producer=load_bound_producer(Path(r["producer"]).read_bytes())
    package=verify_driver_package(r["driver_package"]);sys.path.insert(0,str(package.parent))
    driver=importlib.import_module("psycopg2")
    require(Path(driver.__file__).resolve().parent==package.resolve(),"LOADED_DB_DRIVER_ORIGIN")
    backend=DBAPIBackend(driver,os.environ[WRITE_ENV],os.environ[READ_ENV])
    receipt=execute_worker(packet,producer=producer,backend=backend,exchange=HTTPSExchange(time.monotonic),clock=time.monotonic,utc=lambda:datetime.now(timezone.utc),synthetic=False)
    sys.stdout.write(canonical(exported(receipt))+"\n")


def cli(argv=None,*,read_bytes=None,runtime=None,emit=print,worker_entry=None):
    import time
    entry_origin=(runtime.clock() if runtime is not None else time.monotonic())
    import argparse
    from pathlib import Path
    parser=argparse.ArgumentParser(description="Exactly one admitted CNMS FINRA file occurrence")
    parser.add_argument("--selected-workflow",action="store_true")
    parser.add_argument("--admission");parser.add_argument("--admission-sha256")
    parser.add_argument("--worker",action="store_true");parser.add_argument("--packet-sha256")
    args=parser.parse_args(argv)
    if args.worker:
        require(not args.selected_workflow and args.admission is None and args.admission_sha256 is None and type(args.packet_sha256) is str and re.fullmatch(r"[a-f0-9]{64}",args.packet_sha256) is not None,"WORKER_ARGV")
        (worker_entry or worker_main)(args.packet_sha256);return 0
    require(args.packet_sha256 is None and type(args.admission) is str and type(args.admission_sha256) is str,"PARENT_ADMISSION_ARGV")
    data=(read_bytes or (lambda p:Path(p).read_bytes()))(args.admission)
    receipt=run_selected_workflow(data,args.admission_sha256,entry_origin=entry_origin,runtime=runtime) if args.selected_workflow else run_live(data,args.admission_sha256,runtime=runtime)
    emit(canonical(exported(receipt)))
    return int(receipt.get("state")!="COMPLETE") if args.selected_workflow else int("error_type" in receipt)


# New route: finite static consent, exactly-once workflow and bounded attempts.
SELECTED_SCHEMA="finra-selected-workflow-admission-v2"
ATTEMPT_SCHEMA="finra-selected-attempt-v2"
RESULT_SCHEMA="finra-selected-workflow-result-v2"
WORKFLOW_CAPS={"attempts":3,"gets":3,"body":25165824,"http_wall":135,"total":450}
FRAME_KEYS={"workflow_id","admission_sha256","origin","end","expires_utc","execution_day","clock_domain","clock_pair_monotonic","clock_pair_utc"}
EFFECT_KEYS={"db_started","write_bearing","commit_attempted","http_requests","body_bytes","http_elapsed","transport_cleanup"}
RETRY_CODES={"TRANSPORT_TIMEOUT","TRANSPORT_CONNECTION","HTTP_TRANSIENT"}
SUMMARY_KEYS={"outcome","index","attempt_id","start","stop_end","state","phase","code","db_started","write_bearing","commit_attempted","http_requests","body_bytes","http_elapsed","transaction_ack","committed_rows","after","stdout_eof","stderr_eof","cleanup_closed"}
RESULT_KEYS={"schema","workflow_id","admission_sha256","call","frame","attempts","reservations","actual_usage","db_write_bearing_attempts","state","terminal_reason","transaction_ack","committed_rows","after","new_rows","durability","native_accepted","live_qualified","synthetic_only"}

class PreDBTransient(Refused):
    pass

def utc_value(value):
    require(type(value) is str and len(value)<=40,"UTC_STRING")
    out=datetime.fromisoformat(value.replace("Z","+00:00"))
    require(out.tzinfo==timezone.utc,"UTC_REQUIRED")
    return out

def deterministic_attempt_id(workflow_id,index):
    require(type(workflow_id) is str and re.fullmatch(r"[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}",workflow_id) is not None and type(index) is int and 1<=index<=3,"ATTEMPT_ID_INPUT")
    h=hashlib.sha256(("FINRA_SELECTED_V2|"+workflow_id+"|"+str(index)).encode()).hexdigest()[:32]
    return h[:8]+"-"+h[8:12]+"-"+h[12:16]+"-"+h[16:20]+"-"+h[20:]

def validate_selected_admission(admission,expected_sha):
    require(type(admission) is bytes and len(admission)<=65536 and type(expected_sha) is str and re.fullmatch(r"[a-f0-9]{64}",expected_sha) is not None and hashlib.sha256(admission).hexdigest()==expected_sha,"SELECTED_ADMISSION_PIN")
    p=json.loads(admission)
    require(type(p) is dict and set(p)=={"schema","status","workflow_id","issued_utc","expires_utc","call","caps","workflow_caps","runtime","db_plan"} and p["schema"]==SELECTED_SCHEMA and p["status"]=="ADMITTED","SELECTED_ADMISSION_SCHEMA")
    deterministic_attempt_id(p["workflow_id"],1)
    issued,expiry=utc_value(p["issued_utc"]),utc_value(p["expires_utc"])
    require(0<(expiry-issued).total_seconds()<=7*86400,"FINITE_CONSENT_EXPIRY")
    Caps(**p["caps"]).validate();validate_db_plan(p["db_plan"])
    require(type(p["workflow_caps"]) is dict and canonical(p["workflow_caps"])==canonical(WORKFLOW_CAPS),"WORKFLOW_CAPS")
    call=p["call"];require(type(call) is dict and set(call)==set(Call.__dataclass_fields__),"STATIC_CALL_KEYS")
    require(all(call[k] is None for k in ("call_id","day","cutoff_utc","t0")),"STATIC_CALL_NOT_EXECUTED")
    probe=dict(call,call_id=p["workflow_id"],day=TRADE_DATE,cutoff_utc=TRADE_DATE+"T23:59:59Z",t0=0.0);Call(**probe).validate()
    rt=p["runtime"]
    require(type(rt) is dict and set(rt)=={"python","python_sha256","driver","driver_sha256","producer","driver_package","cgroup","receipt_root"},"SELECTED_RUNTIME_KEYS")
    require(all(type(rt[k]) is str and rt[k].startswith("/") for k in ("python","driver","producer")) and all(type(rt[k]) is str and re.fullmatch(r"[a-f0-9]{64}",rt[k]) is not None for k in ("python_sha256","driver_sha256")),"SELECTED_RUNTIME_PINS")
    return p

def validate_outer_frame(frame):
    require(type(frame) is dict and set(frame)==FRAME_KEYS,"OUTER_FRAME_KEYS")
    deterministic_attempt_id(frame["workflow_id"],1)
    require(type(frame["admission_sha256"]) is str and re.fullmatch(r"[a-f0-9]{64}",frame["admission_sha256"]) is not None,"OUTER_ADMISSION_PIN")
    require(all(type(frame[k]) is float and finite(frame[k]) for k in ("origin","end","clock_pair_monotonic")) and frame["end"]==frame["origin"]+450.0 and frame["origin"]<=frame["clock_pair_monotonic"]<frame["end"],"OUTER_ORIGINAL_DEADLINE")
    pair=utc_value(frame["clock_pair_utc"]);expiry=utc_value(frame["expires_utc"])
    require(frame["clock_domain"]=="CLOCK_MONOTONIC" and frame["execution_day"]==pair.date().isoformat() and expiry>pair and (expiry-pair).total_seconds()>=frame["end"]-frame["clock_pair_monotonic"],"OUTER_UTC_RESERVE")
    return frame

def validate_attempt_packet(p,now,wall):
    require(type(p) is dict and set(p)=={"schema","status","call","caps","runtime","db_plan","selected_workflow"} and p["schema"]==ATTEMPT_SCHEMA and p["status"]=="ADMITTED","ATTEMPT_PACKET_KEYS")
    selected=p["selected_workflow"];require(type(selected) is dict and set(selected)=={"frame","index","attempt_id"},"ATTEMPT_FRAME_KEYS")
    f=validate_outer_frame(selected["frame"]);c=Call(**p["call"]);c.validate();Caps(**p["caps"]).validate();validate_db_plan(p["db_plan"])
    require(c.call_id==selected["attempt_id"]==deterministic_attempt_id(f["workflow_id"],selected["index"]) and c.day==f["execution_day"] and type(c.t0) is float and f["origin"]<=c.t0 and c.t0+150<=f["end"],"ATTEMPT_ORIGINAL_BINDING")
    day_end=datetime.fromisoformat(c.day).replace(tzinfo=timezone.utc)+timedelta(days=1,microseconds=-1)
    require(utc_value(c.cutoff_utc)==min(utc_value(f["expires_utc"]),day_end),"ATTEMPT_CUTOFF_IMMUTABLE")
    require(finite(now) and c.t0<=now<f["end"],"ATTEMPT_OUTER_DEADLINE")
    if wall is not None:require(type(wall) is datetime and wall.tzinfo==timezone.utc and wall.date().isoformat()==c.day and wall<utc_value(c.cutoff_utc),"ATTEMPT_WALL")

def validate_selected_child(child,packet,packet_sha):
    require(type(child) is dict and child.get("schema")=="finra-occurrence-worker-v2" and canonical(child.get("selected_workflow"))==canonical(packet["selected_workflow"]) and child.get("attempt_packet_sha256")==packet_sha and canonical(child.get("binding"))==canonical(packet["call"]),"SELECTED_CHILD_IDENTITY")
    require(type(child.get("status")) is str and child["status"] in ("SUCCESS","NO_NEW_DATA","SKIPPED","FAILED","PARTIAL"),"SELECTED_CHILD_OUTCOME")
    e=child.get("effects");r=child.get("retry_classification")
    require(type(e) is dict and set(e)==EFFECT_KEYS and all(type(e[k]) is bool for k in ("db_started","write_bearing","commit_attempted","transport_cleanup")),"SELECTED_EFFECT_TYPES")
    require(type(e["http_requests"]) is int and 0<=e["http_requests"]<=1 and type(e["body_bytes"]) is int and 0<=e["body_bytes"]<=Caps().body and type(e["http_elapsed"]) in (float,int) and finite(e["http_elapsed"]) and 0<=e["http_elapsed"]<=Caps().http_wall,"SELECTED_HTTP_COUNTS")
    require((not e["write_bearing"] or e["db_started"]) and (not e["commit_attempted"] or e["write_bearing"]),"SELECTED_EFFECT_ORDER")
    # FR2: accounting evidence also constrains effect flags (not just vice versa).
    attempts=child.get("write_attempts");ack=child.get("transaction_ack");count=child.get("committed_rows")
    require(type(attempts) is int and 0<=attempts<=Caps().rows and ack in ("NOT_ATTEMPTED","UNKNOWN","ACKNOWLEDGED","ROLLBACK_ACKNOWLEDGED") and (count is None or type(count) is int and 0<=count<=attempts),"CHILD_EFFECT_ACCOUNTING_TYPES")
    if attempts>0 or ack=="ACKNOWLEDGED" or (count is not None and count>0):
        require(e["db_started"] and e["write_bearing"],"CHILD_ACCOUNTING_REQUIRES_WRITE_EFFECT")
    if ack=="ACKNOWLEDGED":require(e["commit_attempted"] and type(count) is int,"CHILD_ACK_REQUIRES_COMMIT")
    if count is not None and count>0:require(ack=="ACKNOWLEDGED" and e["commit_attempted"],"CHILD_ROWS_REQUIRE_ACK")
    if ack=="UNKNOWN":require(count is None,"CHILD_UNKNOWN_COMMIT_COUNT")
    if ack in ("NOT_ATTEMPTED","ROLLBACK_ACKNOWLEDGED"):require(count is None or count==0,"CHILD_NONCOMMIT_COUNT")
    if e["commit_attempted"]:require(ack in ("UNKNOWN","ACKNOWLEDGED"),"CHILD_COMMIT_ACK_STATE")
    if not e["write_bearing"]:require(attempts==0 and ack=="NOT_ATTEMPTED" and not e["commit_attempted"],"CHILD_NO_WRITE_EVIDENCE")
    if not e["db_started"]:require(child.get("before")=="UNKNOWN" and child.get("after")=="UNKNOWN","CHILD_NO_DB_READBACK")
    # END FR2
    require(type(r) is dict and set(r)=={"phase","code","eligible"} and type(r["eligible"]) is bool,"RETRY_CLASSIFICATION")
    if r["eligible"]:
        require(r["phase"]=="PRE_DB_TRANSPORT" and r["code"] in RETRY_CODES and not e["db_started"] and not e["write_bearing"] and not e["commit_attempted"] and e["http_requests"]==1 and e["transport_cleanup"] and child.get("status")=="FAILED" and child.get("error_type")=="PRE_DB_TRANSPORT" and type(child.get("write_attempts")) is int and child["write_attempts"]==0 and child.get("transaction_ack")=="NOT_ATTEMPTED" and type(child.get("committed_rows")) is int and child["committed_rows"]==0 and child.get("before")=="UNKNOWN" and child.get("after")=="UNKNOWN" and "after_error_type" not in child,"RETRY_PRE_DB_PROOF")
    else:require(r["phase"]=="TERMINAL" and r["code"]=="NONE","TERMINAL_CLASSIFICATION")
    return e,r

def validate_selected_result(result,admission,expected_sha,entry_origin):
    p=validate_selected_admission(admission,expected_sha)
    require(type(result) is dict and set(result)==RESULT_KEYS and result["schema"]==RESULT_SCHEMA and result["workflow_id"]==p["workflow_id"] and result["admission_sha256"]==expected_sha and canonical(result["call"])==canonical(p["call"]),"SELECTED_RESULT_KEYS_IDENTITY")
    f=validate_outer_frame(result["frame"])
    require(type(entry_origin) is float and f["origin"]==entry_origin and f["workflow_id"]==p["workflow_id"] and f["admission_sha256"]==expected_sha and f["expires_utc"]==p["expires_utc"],"SELECTED_RESULT_FRAME")
    attempts=result["attempts"];require(type(attempts) is list and len(attempts)<=3,"RESULT_ATTEMPTS")
    for index,a in enumerate(attempts,1):
        require(type(a) is dict and set(a)==SUMMARY_KEYS and type(a["index"]) is int and a["index"]==index and a["attempt_id"]==deterministic_attempt_id(p["workflow_id"],index) and type(a["start"]) is float and f["origin"]<=a["start"] and a["stop_end"]==a["start"]+150<=f["end"],"RESULT_ATTEMPT_IDENTITY")
        require(a["outcome"] in ("UNKNOWN","SUCCESS","NO_NEW_DATA","SKIPPED","FAILED","PARTIAL"),"RESULT_OUTCOME")
        require(a["state"] in ("UNKNOWN","RETRYABLE","TERMINAL") and a["phase"] in ("UNKNOWN","PRE_DB_TRANSPORT","TERMINAL") and a["code"] in RETRY_CODES|{"NONE","UNKNOWN"},"RESULT_ATTEMPT_STATE")
        for k in ("db_started","write_bearing","commit_attempted","stdout_eof","stderr_eof","cleanup_closed"):require(a[k] is None or type(a[k]) is bool,"RESULT_BOOLEAN")
        require(a["transaction_ack"] in ("UNKNOWN","NOT_ATTEMPTED","ACKNOWLEDGED","ROLLBACK_ACKNOWLEDGED") and (a["committed_rows"] is None or type(a["committed_rows"]) is int and 0<=a["committed_rows"]<=Caps().rows),"RESULT_ACK_TYPES")
        # UNKNOWN here means missing/untrusted attempt evidence, not unknown commit.
        if a["state"]=="UNKNOWN":
            require(all(a[k] is None for k in ("db_started","write_bearing","commit_attempted","http_requests","body_bytes","http_elapsed","stdout_eof","stderr_eof","cleanup_closed")) and a["outcome"]==a["phase"]==a["code"]=="UNKNOWN" and a["transaction_ack"]=="UNKNOWN" and a["committed_rows"] is None and a["after"]=="UNKNOWN","RESULT_MISSING_ATTEMPT_FACTS")
        else:
            require(all(type(a[k]) is bool for k in ("db_started","write_bearing","commit_attempted")) and a["stdout_eof"] is True and a["stderr_eof"] is True and a["cleanup_closed"] is True,"RESULT_KNOWN_ATTEMPT_PROOF")
            require((not a["write_bearing"] or a["db_started"]) and (not a["commit_attempted"] or a["write_bearing"]),"RESULT_EFFECT_ORDER")
            if a["transaction_ack"]=="ACKNOWLEDGED":require(a["db_started"] and a["write_bearing"] and a["commit_attempted"] and type(a["committed_rows"]) is int,"RESULT_ACK_EFFECT_EVIDENCE")
            if a["committed_rows"] is not None and a["committed_rows"]>0:require(a["transaction_ack"]=="ACKNOWLEDGED","RESULT_ROWS_REQUIRE_ACK")
            if a["transaction_ack"]=="UNKNOWN":require(a["committed_rows"] is None,"RESULT_UNKNOWN_COUNT")
            if a["transaction_ack"] in ("NOT_ATTEMPTED","ROLLBACK_ACKNOWLEDGED"):require(a["committed_rows"] is None or a["committed_rows"]==0,"RESULT_NONCOMMIT_COUNT")
            if a["commit_attempted"]:require(a["transaction_ack"] in ("UNKNOWN","ACKNOWLEDGED"),"RESULT_COMMIT_ACK_STATE")
            if not a["write_bearing"]:require(a["transaction_ack"]=="NOT_ATTEMPTED" and not a["commit_attempted"],"RESULT_NO_WRITE_ACK")
            if not a["db_started"]:require(a["after"]=="UNKNOWN","RESULT_NO_DB_READBACK")
        if a["state"]=="RETRYABLE":require(a["phase"]=="PRE_DB_TRANSPORT" and a["code"] in RETRY_CODES and a["db_started"] is False and a["write_bearing"] is False and a["commit_attempted"] is False and a["cleanup_closed"] is True and a["stdout_eof"] is True and a["stderr_eof"] is True and a["transaction_ack"]=="NOT_ATTEMPTED" and a["committed_rows"]==0,"RESULT_RETRY_PROOF")
        if index<len(attempts):require(a["state"]=="RETRYABLE","RESULT_NO_RETRY_AFTER_UNKNOWN")
    n=len(attempts);require(canonical(result["reservations"])==canonical({"gets":n,"body_bytes":n*Caps().body,"http_seconds":n*Caps().http_wall}),"RESULT_RESERVATIONS")
    usage=result["actual_usage"];require(type(usage) is dict and set(usage)=={"exchange_calls","body_bytes","http_seconds","wire_gets"},"RESULT_ACTUAL_USAGE")
    require(usage["wire_gets"]=="UNKNOWN","NO_WIRE_COUNT_INFERENCE")
    any_unknown=any(a["state"]=="UNKNOWN" for a in attempts)
    if any_unknown:require(all(usage[k] is None for k in ("exchange_calls","body_bytes","http_seconds")) and usage["wire_gets"]=="UNKNOWN" and result["db_write_bearing_attempts"] is None,"RESULT_UNKNOWN_NOT_ZERO")
    else:
        require(type(usage["exchange_calls"]) is int and type(usage["body_bytes"]) is int and type(usage["http_seconds"]) in (int,float) and finite(usage["http_seconds"]),"RESULT_USAGE_TYPES")
        for a in attempts:
            require(type(a["http_requests"]) is int and 0<=a["http_requests"]<=1 and type(a["body_bytes"]) is int and 0<=a["body_bytes"]<=Caps().body and type(a["http_elapsed"]) in (int,float) and finite(a["http_elapsed"]) and 0<=a["http_elapsed"]<=45,"RESULT_ATTEMPT_USAGE")
        require(usage["exchange_calls"]==sum(a["http_requests"] for a in attempts) and usage["body_bytes"]==sum(a["body_bytes"] for a in attempts) and usage["http_seconds"]==sum(a["http_elapsed"] for a in attempts),"RESULT_USAGE_SUMS")
        require(result["db_write_bearing_attempts"]==sum(a["write_bearing"] is True for a in attempts),"RESULT_DB_ATTEMPT_SUM")
    require(result["db_write_bearing_attempts"] is None or type(result["db_write_bearing_attempts"]) is int and 0<=result["db_write_bearing_attempts"]<=1,"RESULT_SINGLE_DB_ATTEMPT")
    require(result["state"] in ("COMPLETE","EXHAUSTED","TERMINAL","UNKNOWN","HOLD") and result["terminal_reason"] in ("OUTCOME_UNKNOWN","ATTEMPT_TERMINAL","RETRY_EXHAUSTED","NO_START_RESERVE","WORKFLOW_FAILURE") and result["transaction_ack"] in ("UNKNOWN","NOT_ATTEMPTED","ACKNOWLEDGED","ROLLBACK_ACKNOWLEDGED") and (result["committed_rows"] is None or type(result["committed_rows"]) is int and 0<=result["committed_rows"]<=Caps().rows),"RESULT_STATE")
    # FR1: derive aggregate facts from the final available attempt, regardless
    # of outer state. This preserves observed ACK/readback on late outer failure.
    evidence=attempts[-1] if attempts and attempts[-1]["state"]!="UNKNOWN" else None
    expected_ack=evidence["transaction_ack"] if evidence is not None else "UNKNOWN"
    expected_count=evidence["committed_rows"] if evidence is not None else None
    expected_after=evidence["after"] if evidence is not None else "UNKNOWN"
    require(result["transaction_ack"]==expected_ack and type(result["committed_rows"]) is type(expected_count) and result["committed_rows"]==expected_count and canonical(result["after"])==canonical(expected_after),"RESULT_AGGREGATE_FROM_ATTEMPT")
    # END FR1
    if result["state"] in ("COMPLETE","TERMINAL"):
        require(bool(attempts) and attempts[-1]["state"]=="TERMINAL" and attempts[-1]["cleanup_closed"] is True and attempts[-1]["stdout_eof"] is True and attempts[-1]["stderr_eof"] is True,"RESULT_TERMINAL_CLOSURE")
        a=attempts[-1];require(type(result["committed_rows"]) is type(a["committed_rows"]) and result["committed_rows"]==a["committed_rows"] and result["transaction_ack"]==a["transaction_ack"] and canonical(result["after"])==canonical(a["after"]),"RESULT_TERMINAL_ACCOUNTING")
        require((result["state"]=="COMPLETE")== (a["outcome"] in ("SUCCESS","NO_NEW_DATA","SKIPPED")),"RESULT_COMPLETE_OUTCOME")
    if result["state"]=="EXHAUSTED":require(len(attempts)==3 and all(a["state"]=="RETRYABLE" for a in attempts),"RESULT_EXHAUSTED_PROOF")
    require(result["new_rows"]=="UNKNOWN" and result["durability"]=="UNKNOWN" and result["native_accepted"] is False and result["live_qualified"] is False and type(result["synthetic_only"]) is bool,"RESULT_NO_PROMOTION")
    return True

def same_reader(a,b):
    return a is b or (getattr(a,"__func__",None) is not None and getattr(a,"__func__",None) is getattr(b,"__func__",None) and getattr(a,"__self__",None) is getattr(b,"__self__",None))

def require_readers(runtime,clock,utc):
    require(same_reader(runtime.clock,clock) and same_reader(runtime.utc,utc),"WORKFLOW_CLOCK_REBOUND")

def run_selected_workflow(admission,expected_sha,*,entry_origin,runtime=None):
    # entry_origin is captured by the trusted CLI/caller before config or imports;
    # its provenance is an invocation contract, never a caller qualification flag.
    require(type(entry_origin) is float and finite(entry_origin),"TRUSTED_ENTRY_ORIGIN")
    runtime=runtime or NativeRuntime();clock,utc=runtime.clock,runtime.utc
    p=validate_selected_admission(admission,expected_sha);private=canonical(p)
    require_readers(runtime,clock,utc)
    now=clock();wall=utc();later=clock();require(finite(later) and finite(now) and later>=now,"ENTRY_POST_UTC_CLOCK");now=later;require_readers(runtime,clock,utc);require(finite(now) and now>=entry_origin and type(wall) is datetime and wall.tzinfo==timezone.utc,"ENTRY_CLOCK_PAIR")
    issued,expiry=utc_value(p["issued_utc"]),utc_value(p["expires_utc"])
    day=wall.date().isoformat();cutoff=min(expiry,datetime.fromisoformat(day).replace(tzinfo=timezone.utc)+timedelta(days=1,microseconds=-1))
    require(issued<=wall<expiry and now<entry_origin+450 and (cutoff-wall).total_seconds()>=entry_origin+450-now,"ENTRY_FINITE_EXPIRY_RESERVE")
    frame={"workflow_id":p["workflow_id"],"admission_sha256":expected_sha,"origin":entry_origin,"end":entry_origin+450.0,"expires_utc":p["expires_utc"],"execution_day":day,"clock_domain":"CLOCK_MONOTONIC","clock_pair_monotonic":float(now),"clock_pair_utc":wall.isoformat()};validate_outer_frame(frame)
    frame_pin=canonical(frame);last=now
    def guard():
        nonlocal last
        require(canonical(frame)==frame_pin and canonical(p)==private,"WORKFLOW_BINDING_CHANGED")
        require_readers(runtime,clock,utc)
        value=clock();current=utc();later=clock();require(finite(later) and finite(value) and later>=value,"GUARD_POST_UTC_CLOCK");value=later;require_readers(runtime,clock,utc)
        require(finite(value) and value>=last and value<frame["end"] and type(current) is datetime and current.tzinfo==timezone.utc and current.date().isoformat()==day and issued<=current<cutoff,"WORKFLOW_CLOCK_EXPIRY")
        last=value;require(canonical(frame)==frame_pin and canonical(p)==private,"WORKFLOW_CALLBACK_MUTATION");return float(value)
    result={"schema":RESULT_SCHEMA,"workflow_id":p["workflow_id"],"admission_sha256":expected_sha,"call":p["call"],"frame":frame,"attempts":[],"reservations":{"gets":0,"body_bytes":0,"http_seconds":0},"actual_usage":{"exchange_calls":0,"body_bytes":0,"http_seconds":0.0,"wire_gets":"UNKNOWN"},"db_write_bearing_attempts":0,"state":"UNKNOWN","terminal_reason":"WORKFLOW_FAILURE","transaction_ack":"UNKNOWN","committed_rows":None,"after":"UNKNOWN","new_rows":"UNKNOWN","durability":"UNKNOWN","native_accepted":False,"live_qualified":False,"synthetic_only":type(runtime) is not NativeRuntime or runtime.injected}
    def packet(index,start):
        cid=deterministic_attempt_id(p["workflow_id"],index)
        call=dict(p["call"],call_id=cid,day=day,cutoff_utc=cutoff.isoformat(),t0=start)
        return {"schema":ATTEMPT_SCHEMA,"status":"ADMITTED","call":call,"caps":p["caps"],"runtime":p["runtime"],"db_plan":p["db_plan"],"selected_workflow":{"frame":json.loads(frame_pin),"index":index,"attempt_id":cid}}
    try:
        start=guard();require(start+150<=frame["end"],"NO_START_RESERVE")
        require(runtime.consume_workflow(packet(1,start),p["workflow_id"]) is True,"WORKFLOW_CONSUMPTION_UNVERIFIED");guard()
        for index in range(1,4):
            start=guard()
            if start+150>frame["end"]:result["state"]="HOLD";result["terminal_reason"]="NO_START_RESERVE";break
            inner=packet(index,start);raw=canonical(inner).encode();digest=hashlib.sha256(raw).hexdigest()
            result["reservations"]={"gets":index,"body_bytes":index*Caps().body,"http_seconds":index*Caps().http_wall}
            summary=dict(outcome="UNKNOWN",index=index,attempt_id=inner["call"]["call_id"],start=start,stop_end=start+150,state="UNKNOWN",phase="UNKNOWN",code="UNKNOWN",db_started=None,write_bearing=None,commit_attempted=None,http_requests=None,body_bytes=None,http_elapsed=None,transaction_ack="UNKNOWN",committed_rows=None,after="UNKNOWN",stdout_eof=None,stderr_eof=None,cleanup_closed=None)
            result["attempts"].append(summary)
            try:
                parent=run_live(raw,digest,runtime=runtime);guard()
                require(type(parent) is dict and parent.get("error_type") is None and parent.get("schema")=="finra-occurrence-parent-v1" and canonical(parent.get("binding"))==canonical(inner["call"]) and canonical(parent.get("selected_workflow"))==canonical(inner["selected_workflow"]) and parent.get("attempt_packet_sha256")==digest,"SELECTED_PARENT_IDENTITY")
                stop=parent.get("stop");final=parent.get("parent_finalization")
                require(type(stop) is dict and stop.get("exit_code")==0 and type(stop.get("exit_code")) is int and stop.get("reaped") is True and stop.get("stdout_eof") is True and stop.get("stderr_eof") is True and stop.get("term_sent") is False and stop.get("kill_sent") is False and type(final) is dict and final.get("state")=="CLOSED_WITHIN_BOUND" and final.get("stop_end")==start+150 and final.get("failures")==[],"SELECTED_PARENT_CLOSURE")
                child=parent.get("child_receipt");effects,retry=validate_selected_child(child,inner,digest)
                require(parent["transaction_ack"]==child["transaction_ack"] and type(parent["committed_rows"]) is type(child["committed_rows"]) and parent["committed_rows"]==child["committed_rows"] and canonical(parent["after"])==canonical(child["after"]),"SELECTED_PARENT_ACCOUNTING")
                summary.update(outcome=child.get("status"),state="RETRYABLE" if retry["eligible"] else "TERMINAL",phase=retry["phase"],code=retry["code"],**effects,transaction_ack=child["transaction_ack"],committed_rows=child["committed_rows"],after=child["after"],stdout_eof=True,stderr_eof=True,cleanup_closed=True)
                # transport_cleanup is internal evidence; omit it from the public summary.
                summary.pop("transport_cleanup")
                result["db_write_bearing_attempts"]+=int(effects["write_bearing"]);require(result["db_write_bearing_attempts"]<=1,"SINGLE_WRITE_BEARING_ATTEMPT")
                for key,field in (("exchange_calls","http_requests"),("body_bytes","body_bytes"),("http_seconds","http_elapsed")):result["actual_usage"][key]+=effects[field]
                result.update(transaction_ack=child["transaction_ack"],committed_rows=child["committed_rows"],after=child["after"])
                if retry["eligible"]:
                    if index==3:result["state"]="EXHAUSTED";result["terminal_reason"]="RETRY_EXHAUSTED"
                    continue
                if child["transaction_ack"]=="UNKNOWN":result["state"]="UNKNOWN";result["terminal_reason"]="OUTCOME_UNKNOWN"
                else:result["state"]="COMPLETE" if child.get("status") in ("SUCCESS","NO_NEW_DATA","SKIPPED") else "TERMINAL";result["terminal_reason"]="ATTEMPT_TERMINAL"
                break
            except BaseException:
                result.update(state="UNKNOWN",terminal_reason="OUTCOME_UNKNOWN",transaction_ack="UNKNOWN",committed_rows=None,after="UNKNOWN",db_write_bearing_attempts=None,actual_usage={"exchange_calls":None,"body_bytes":None,"http_seconds":None,"wire_gets":"UNKNOWN"});break
        guard()
    except BaseException:
        result["state"]="UNKNOWN";result["terminal_reason"]="WORKFLOW_FAILURE"
    require(len(canonical(exported(result)).encode())<=Caps().receipt,"SELECTED_RESULT_CAP")
    validate_selected_result(result,admission,expected_sha,entry_origin)
    return result

if __name__=="__main__":
    raise SystemExit(cli())
