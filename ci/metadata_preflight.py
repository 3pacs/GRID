"""Source-only aggregate metadata diagnostic; never a deployment admission.

Adapted from retained B3 descriptor checks, M3 ACL metadata and B5 aggregation.
No file-body reads, configuration evaluation, Git, DB, imports from GRID or writes.
The CLI describes its scope only. A separately reviewed operator invocation is
required before calling collect() on an installed host; no spent attempt is reused.
"""
import contextlib
import errno
import json
import os
import re
import selectors
import stat
import struct
import subprocess
import sys
import time

PROTOCOL = 'grid.release.metadata.preflight.v1'
UNIT = 'actions.runner.3pacs-GRID.grid-svr.service'
LIVE = '/data/grid_v4/grid_release'
RELEASES = LIVE + '.releases'
REALTIME = '/data/grid_v4/grid-realtime-pins/105045d172c2152093eb96eb0a9051f474af71da'
KEYS = ('Id', 'User', 'Group', 'WorkingDirectory', 'MainPID', 'ControlGroup', 'ActiveState')
META_KEYS = ('kind', 'uid', 'gid', 'mode', 'nlink', 'size', 'device', 'inode', 'mtime_ns', 'ctime_ns')
LIMITS = dict(seconds=30, entries=8192, depth=32, findings=64, output=65536)
TARGETS = (
    ('runner_root', '/home/grid/actions-runner', 'DIRECTORY'),
    ('runner_path', '/home/grid/actions-runner/.path', 'REGULAR'),
    ('runner_environment', '/home/grid/actions-runner/.env', 'REGULAR'),
    ('credential_metadata', '/data/grid_v4/grid_repo/.env', 'REGULAR'),
    ('release_parent', RELEASES, 'DIRECTORY'),
    ('previous_marker', RELEASES + '/.previous-release', 'REGULAR'),
    ('preservation_marker', RELEASES + '/.runtime-preservation', 'REGULAR'),
    ('retention_marker', RELEASES + '/.runtime-retention', 'REGULAR'),
    ('activation_marker', RELEASES + '/.activation-in-progress', 'REGULAR'),
    ('deploy_lock_metadata', RELEASES + '/.lock', 'REGULAR'),
    ('realtime_root', REALTIME, 'DIRECTORY'),
    ('python_alias', '/usr/bin/python3', 'SYMLINK'),
    ('python_target', '/usr/bin/python3.10', 'EXECUTABLE'),
    ('pip', '/usr/bin/pip', 'EXECUTABLE'),
    ('git', '/usr/bin/git', 'EXECUTABLE'),
    ('timeout', '/usr/bin/timeout', 'EXECUTABLE'),
    ('systemctl', '/usr/bin/systemctl', 'EXECUTABLE'),
)
CODES = frozenset(('PATH_SYNTAX', 'PATH_LINK', 'PATH_TYPE', 'PATH_OWNER',
    'GROUP_WRITE', 'OTHER_WRITE', 'SPECIAL_MODE', 'MULTIPLE_LINKS', 'NOT_EXECUTABLE',
    'ACL_PRESENT', 'ACL_UNKNOWN', 'PATH_RACE', 'IO_ABSENT', 'IO_INACCESSIBLE',
    'IO_OTHER', 'DEPTH_BOUND', 'ENTRY_BOUND', 'FINDING_BOUND', 'TIME_BOUND',
    'CLEANUP_UNKNOWN', 'UNEXPECTED_FAILURE', 'LINK_TARGET_SCOPE', 'UNIT_SHAPE',
    'UNIT_BINDING', 'UNIT_CHANGED', 'UNIT_TIMEOUT', 'UNIT_ACCESS', 'UNIT_OUTPUT',
    'PREREQUISITE', 'RUNTIME_BINDING'))
FATAL = frozenset(('CLEANUP_UNKNOWN', 'TIME_BOUND', 'ENTRY_BOUND', 'FINDING_BOUND'))
UNRUN = ('file_bodies_and_secret_values','runner_PATH_or_environment_semantics','pip_shebang_and_install_destination',
    'tool_versions_or_execution','regular_file_ACL','Git_commit_and_payload_integrity','generated_and_external_payload_integrity',
    'database_schema_session_locks','caller_fencing_and_loaded_code','rollback_runtime_qualification',
    'permissions_services_schema_deploy_changes')
CHECKS = (tuple((n,()) for n,_,_ in TARGETS) + (('live_reference',()),
    ('live_git',('live_reference',)),('live_scripts',('live_reference',)),('live_workflows',('live_reference',)),
    ('realtime_git',('realtime_root',)),('python_alias_target',('python_alias','python_target')),
    ('python_alias_binding',('python_alias_target',)),('runner_unit',('systemctl','runner_root')),
    ('runner_unit_recheck',('runner_unit',)),('live_reference_recheck',('live_reference',))) +
    tuple((n+'_recheck',(n,)) for n in ('runner_root','runner_path','runner_environment','python_target','pip','systemctl')))


class Refused(Exception):
    def __init__(self, code):
        self.code = code if code in CODES else 'UNEXPECTED_FAILURE'
        super().__init__(self.code)


def lexical(path):
    if (type(path) is not str or len(path) > 512 or
            not re.fullmatch(r'/[A-Za-z0-9_.+@/-]+', path) or
            any(p in ('', '.', '..') for p in path.split('/')[1:])):
        raise Refused('PATH_SYNTAX')
    return path


def snapshot(st):
    kind = ('DIRECTORY' if stat.S_ISDIR(st.st_mode) else 'REGULAR' if stat.S_ISREG(st.st_mode)
            else 'SYMLINK' if stat.S_ISLNK(st.st_mode) else 'OTHER')
    return dict(zip(META_KEYS, (kind, st.st_uid, st.st_gid, stat.S_IMODE(st.st_mode),
        st.st_nlink, st.st_size, st.st_dev, st.st_ino, st.st_mtime_ns, st.st_ctime_ns)))


def predicates(meta, uid, expected):
    """Collect all independent predicates, not only the first failed mode bit."""
    failures = []
    if meta['uid'] not in (0, uid): failures.append('PATH_OWNER')
    if expected == 'SYMLINK':
        if meta['kind'] != 'SYMLINK': failures.append('PATH_TYPE')
        return failures
    if meta['kind'] == 'SYMLINK': failures.append('PATH_LINK')
    elif meta['kind'] != ('REGULAR' if expected == 'EXECUTABLE' else expected): failures.append('PATH_TYPE')
    if meta['mode'] & 0o020: failures.append('GROUP_WRITE')
    if meta['mode'] & 0o002: failures.append('OTHER_WRITE')
    if meta['mode'] & 0o7000: failures.append('SPECIAL_MODE')
    if meta['kind'] == 'REGULAR' and meta['nlink'] != 1: failures.append('MULTIPLE_LINKS')
    if expected == 'EXECUTABLE' and not meta['mode'] & 0o111: failures.append('NOT_EXECUTABLE')
    return failures


def acl_shape(raw):
    """Validate bounded Linux POSIX ACL metadata without exporting ACL subjects."""
    if (type(raw) is not bytes or len(raw) < 4 or len(raw) > 260 or
            (len(raw)-4) % 8 or struct.unpack('<I', raw[:4])[0] != 2):
        raise Refused('ACL_UNKNOWN')
    rows = [struct.unpack('<HHI', raw[i:i+8]) for i in range(4, len(raw), 8)]
    seen = set()
    for tag, perm, who in rows:
        if (tag not in (1,2,4,8,16,32) or perm > 7 or (tag,who) in seen or
                (tag in (1,4,16,32) and who != 0xffffffff) or
                (tag in (2,8) and who == 0xffffffff)):
            raise Refused('ACL_UNKNOWN')
        seen.add((tag,who))
    tags = [r[0] for r in rows]
    if (any(tags.count(t) != 1 for t in (1,4,32)) or tags.count(16) > 1 or
            (any(t in tags for t in (2,8)) and 16 not in tags)):
        raise Refused('ACL_UNKNOWN')
    return 'PRESENT'


def io_code(error):
    return {errno.ENOENT:'IO_ABSENT', errno.EACCES:'IO_INACCESSIBLE',
            errno.EPERM:'IO_INACCESSIBLE', errno.ELOOP:'PATH_LINK',
            errno.ENOTDIR:'PATH_TYPE'}.get(error.errno, 'IO_OTHER')


class Budget:
    def __init__(self, clock=time.monotonic, limits=None):
        self.clock = clock
        self.limits = dict(LIMITS if limits is None else limits)
        self.deadline = clock() + self.limits['seconds']
        self.entries = 0
    def tick(self):
        if self.clock() >= self.deadline: raise Refused('TIME_BOUND')
    def entry(self):
        self.tick()
        if self.entries >= self.limits['entries']: raise Refused('ENTRY_BOUND')
        self.entries += 1


class Metadata:
    """Descriptor-bound backend. Regular files are opened only with O_PATH."""
    def __init__(self, budget, uid=1000):
        self.budget, self.uid = budget, uid
    def close(self, fd):
        try: os.close(fd)
        except OSError: raise Refused('CLEANUP_UNKNOWN') from None
    def directory_acl(self, fd, before):
        readable = None
        try:
            self.budget.tick()
            readable = os.open('.', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            if snapshot(os.fstat(readable)) != before: raise Refused('PATH_RACE')
            states = []
            for kind in ('access', 'default'):
                self.budget.tick()
                try: states.append(acl_shape(os.getxattr(readable, 'system.posix_acl_' + kind)))
                except OSError as error:
                    states.append('ABSENT' if error.errno == errno.ENODATA else 'UNKNOWN')
                except Refused: states.append('UNKNOWN')
            if snapshot(os.fstat(readable)) != before: raise Refused('PATH_RACE')
            return dict(zip(('access','default'), states))
        except OSError:
            return {'access':'UNKNOWN','default':'UNKNOWN'}
        finally:
            if readable is not None: self.close(readable)
    def record(self, fd, path, expected, count=True):
        if count: self.budget.entry()
        meta = snapshot(os.fstat(fd))
        failures = predicates(meta, self.uid, expected)
        acl = {'access':'UNRUN_FILE_METADATA_ONLY', 'default':'NOT_APPLICABLE'}
        if meta['kind']=='DIRECTORY': acl={'access':'UNRUN_UNTRUSTED_OWNER','default':'UNRUN_UNTRUSTED_OWNER'}
        if meta['kind'] == 'DIRECTORY' and meta['uid'] in (0, self.uid):
            acl = self.directory_acl(fd, meta)
            if 'UNKNOWN' in acl.values(): failures.append('ACL_UNKNOWN')
            # Presence alone is metadata, not a new permission-policy failure.
            # Protected current mode bits still gate descent. Future inheritance
            # and any requested permission repair remain outside this diagnostic.
        return {'path': path, 'metadata': meta, 'acl': acl, 'failures': failures,
                'descent_blocked':meta['kind']=='DIRECTORY' and bool(failures)}
    @contextlib.contextmanager
    def bound(self, path, expected, observe):
        """Hold and recheck every ancestor/name. Never follow a symlink here."""
        lexical(path)
        parts = path.split('/')[1:]
        if len(parts) > self.budget.limits['depth']: raise Refused('DEPTH_BOUND')
        chain = []
        try:
            self.budget.tick()
            fd = os.open('/', os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            chain.append((None, '/', fd, None))
            chain[-1]=(None,'/',fd,snapshot(os.fstat(fd)))
            row = self.record(fd, '/', 'DIRECTORY'); observe(row)
            if row['failures']: raise Refused('PREREQUISITE')
            current = ''
            for i, part in enumerate(parts):
                self.budget.tick()
                before = snapshot(os.stat(part, dir_fd=fd, follow_symlinks=False))
                child = os.open(part, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                chain.append((fd,part,child,None))
                opened = snapshot(os.fstat(child))
                chain[-1]=(fd,part,child,opened);fd = child
                if before != opened: raise Refused('PATH_RACE')
                current += '/' + part
                row = self.record(fd, current, expected if i == len(parts)-1 else 'DIRECTORY')
                observe(row)
                if row['failures']: raise Refused('PREREQUISITE')
            yield fd, row
        finally:
            problem = None
            # Recheck even when a child refused; all held descriptors are closed.
            for parent, name, held, before in reversed(chain):
                try:
                    self.budget.tick()
                    if before is None: raise Refused('PATH_RACE')
                    now = snapshot(os.fstat(held))
                    named = snapshot(os.stat(name, dir_fd=parent, follow_symlinks=False)) if parent is not None else snapshot(os.lstat('/'))
                    if now != before or named != before: problem = problem or 'PATH_RACE'
                except Refused as error: problem = problem or error.code
                except OSError: problem = problem or 'PATH_RACE'
            for _, _, held, _ in reversed(chain):
                try: self.close(held)
                except Refused: problem = 'CLEANUP_UNKNOWN'
            if problem: raise Refused(problem)
    def inspect(self, path, expected, observe):
        with self.bound(path, expected, observe) as (_, row): return row
    def link(self, path, observe):
        with self.bound(path, 'SYMLINK', observe) as (fd, row):
            # Empty readlink names the held O_PATH symlink itself on Linux.
            target = os.readlink('', dir_fd=fd)
            if snapshot(os.fstat(fd)) != row['metadata']: raise Refused('PATH_RACE')
            return target
    def tree(self, path, observe):
        with self.bound(path, 'DIRECTORY', observe) as (fd, row):
            self._tree(fd, path, 0, observe)
            return row
    def _tree(self, parent, path, depth, observe):
        if depth >= self.budget.limits['depth']: raise Refused('DEPTH_BOUND')
        readable = None; iterator = None; pending = None
        before = snapshot(os.fstat(parent))
        try:
            self.budget.tick()
            readable = os.open('.', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
            if snapshot(os.fstat(readable)) != before: raise Refused('PATH_RACE')
            iterator = os.scandir(readable)
            for entry in iterator:
                # Count every enumerated name, even if lexical parsing refuses it.
                self.budget.entry()
                child_path = path
                child = None
                try:
                    # Unrepresentable names are not echoed; record a coverage finding.
                    child_path = lexical(path + '/' + entry.name)
                    if len(child_path.split('/'))-1>self.budget.limits['depth']: raise Refused('DEPTH_BOUND')
                    named = snapshot(os.stat(entry.name, dir_fd=parent, follow_symlinks=False))
                    child = os.open(entry.name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
                    if snapshot(os.fstat(child)) != named: raise Refused('PATH_RACE')
                    row = self.record(child, child_path, 'DIRECTORY' if named['kind']=='DIRECTORY' else 'REGULAR',count=False)
                    observe(row)
                    if not row['failures'] and named['kind']=='DIRECTORY': self._tree(child, child_path, depth+1, observe)
                    if snapshot(os.fstat(child)) != named or snapshot(os.stat(entry.name,dir_fd=parent,follow_symlinks=False)) != named: raise Refused('PATH_RACE')
                except (Refused,OSError) as exc:
                    code=exc.code if isinstance(exc,Refused) else io_code(exc)
                    if code in FATAL: raise Refused(code)
                    observe({'path':child_path,'metadata':None,'acl':{'access':'UNKNOWN','default':'UNKNOWN'},'failures':[code],'descent_blocked':True})
                finally:
                    if child is not None: self.close(child)
            if snapshot(os.fstat(readable)) != before: raise Refused('PATH_RACE')
        except BaseException as error: pending = error
        finally:
            if iterator is not None:
                try: iterator.close()
                except BaseException: pending = Refused('CLEANUP_UNKNOWN')
            if readable is not None:
                try: self.close(readable)
                except Refused as error: pending = error
        if pending is not None: raise pending


def unit_properties(raw):
    if type(raw) is not bytes or len(raw) > 4096: raise Refused('UNIT_OUTPUT')
    try: lines = raw.decode('ascii').splitlines()
    except UnicodeError: raise Refused('UNIT_SHAPE') from None
    values = {}
    for line in lines:
        if '=' not in line: raise Refused('UNIT_SHAPE')
        key, value = line.split('=',1)
        if key not in KEYS or key in values: raise Refused('UNIT_SHAPE')
        values[key] = value
    if set(values) != set(KEYS): raise Refused('UNIT_SHAPE')
    if (values['Id'] != UNIT or values['User'] != 'grid' or values['Group'] not in ('','grid') or
            values['WorkingDirectory'] != '/home/grid/actions-runner' or
            values['ControlGroup'] != '/system.slice/' + UNIT or values['ActiveState'] != 'active' or
            not re.fullmatch(r'[1-9][0-9]{0,9}', values['MainPID']) or int(values['MainPID']) >= 2**31):
        raise Refused('UNIT_BINDING')
    # Never export unexpected property values or any environment/command lines.
    return {'id_matches': True, 'user_matches': True, 'group_matches': True,
            'cwd_matches': True, 'cgroup_matches': True, 'active': True,
            'pid': int(values['MainPID'])}


def unit_read(budget):
    """One fixed metadata command, sanitized env, no shell, bounded pipes/cleanup."""
    argv = ['/usr/bin/systemctl','show',UNIT,'--all','--no-pager','--property='+','.join(KEYS)]
    process = None; selector = None; streams = []; data = bytearray(); error = None; success=False
    end = min(budget.deadline, budget.clock()+2)
    try:
        budget.tick()
        process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, close_fds=True, start_new_session=True,
            env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','SYSTEMD_PAGER':'cat','SYSTEMD_COLORS':'0'})
        streams = [process.stdout, process.stderr]
        selector = selectors.DefaultSelector()
        for stream in streams: selector.register(stream,selectors.EVENT_READ,stream is process.stderr)
        while selector.get_map():
            left = end-budget.clock()
            if left <= 0: raise Refused('UNIT_TIMEOUT')
            for key,_ in selector.select(left):
                if budget.clock() >= end: raise Refused('UNIT_TIMEOUT')
                chunk = os.read(key.fileobj.fileno(),512)
                if not chunk: selector.unregister(key.fileobj); continue
                if key.data: raise Refused('UNIT_OUTPUT')
                data.extend(chunk)
                if len(data)>4096: raise Refused('UNIT_OUTPUT')
        rc=process.wait(timeout=max(.001,end-budget.clock()))
        if budget.clock()>=end: raise Refused('UNIT_TIMEOUT')
        if rc: raise Refused('UNIT_ACCESS')
        # A reaped leader does not prove its owned process group is absent.
        try: os.killpg(process.pid,0)
        except ProcessLookupError: pass
        else: raise Refused('CLEANUP_UNKNOWN')
        success=True
    except Refused as exc: error=exc
    except BaseException: error=Refused('UNIT_ACCESS')
    finally:
        uncertain = process is None
        cleanup_end=budget.clock()+.5
        def left():
            nonlocal uncertain
            remaining=cleanup_end-budget.clock()
            if remaining<=0: uncertain=True
            return max(0.,remaining)
        if process is not None and not success:
            left()
            try: os.killpg(process.pid,9)
            except ProcessLookupError: pass
            except BaseException: uncertain=True
            remaining=left()
            if remaining>0:
                try: process.wait(timeout=remaining)
                except BaseException: uncertain=True
                left()
            else: uncertain=True
        if selector is not None:
            left()
            try: selector.close()
            except BaseException: uncertain=True
            left()
        if process is not None:
            for stream in streams:
                left()
                try: stream.close()
                except BaseException: uncertain=True
                left()
            left()
            try: os.killpg(process.pid,0)
            except ProcessLookupError: pass
            except BaseException: uncertain=True
            else: uncertain=True
            left()
        left()
        if uncertain: error=Refused('CLEANUP_UNKNOWN')
    if error: raise error
    budget.tick()
    return unit_properties(bytes(data))


def collect(backend=None, budget=None, read_unit=None):
    """Evaluate every independently safe fixed target; no fail-first global gate."""
    budget = budget or Budget()
    backend = backend or Metadata(budget)
    read_unit = read_unit or (lambda: unit_read(budget))
    checks=[]; findings=[]; seen=set(); stopped=None; results={}; totals={'observed':0,'failed_entries':0}
    def observe(row):
        totals['observed'] += 1
        if row['failures']:
            totals['failed_entries'] += 1
            key=json.dumps(row,sort_keys=True)
            if key not in seen:
                if len(findings)>=budget.limits['findings']: raise Refused('FINDING_BOUND')
                seen.add(key);findings.append(row)
    def run(name, deps, fn):
        nonlocal stopped
        blocked=[d for d in deps if results[d]['status']!='PASS']
        row={'check':name,'status':'BLOCKED','code':stopped or 'PREREQUISITE','blocked_by':blocked,'value':None}
        if not stopped and not blocked:
            initial=totals['failed_entries']
            try:
                budget.tick();value=fn();budget.tick()
                row.update(status='FAIL' if totals['failed_entries']>initial else 'PASS',code='PREREQUISITE' if totals['failed_entries']>initial else 'NONE',value=value)
            except Refused as exc:
                row.update(status='FAIL',code=exc.code)
                if exc.code in FATAL: stopped=exc.code
            except OSError as exc: row.update(status='FAIL',code=io_code(exc))
            except Exception: row.update(status='FAIL',code='UNEXPECTED_FAILURE')
        checks.append(row);results[name]=row
        return row['value']
    for name,path,expected in TARGETS:
        run(name,(),lambda p=path,e=expected:backend.inspect(p,e,observe))
    def live_target():
        target=backend.link(LIVE,observe)
        if not re.fullmatch(re.escape(RELEASES)+r'/[A-Za-z0-9][A-Za-z0-9._-]{0,127}',target): raise Refused('LINK_TARGET_SCOPE')
        lexical(target)
        return target
    live=run('live_reference',(),live_target)
    for name,suffix in (('live_git','/.git'),('live_scripts','/scripts'),('live_workflows','/.github/workflows')):
        run(name,('live_reference',),lambda s=suffix:backend.tree(live+s,observe))
    run('realtime_git',('realtime_root',),lambda:backend.tree(REALTIME+'/.git',observe))
    def read_alias():
        value=backend.link('/usr/bin/python3',observe)
        if value not in ('python3.10','/usr/bin/python3.10'): raise Refused('LINK_TARGET_SCOPE')
        return value
    alias=run('python_alias_target',('python_alias','python_target'),read_alias)
    def alias_binding():
        if alias not in ('python3.10','/usr/bin/python3.10'): raise Refused('LINK_TARGET_SCOPE')
        return {'matches_retained_target':True}
    run('python_alias_binding',('python_alias_target',),alias_binding)
    unit=run('runner_unit',('systemctl','runner_root'),read_unit)
    def unit_recheck():
        if read_unit()!=unit: raise Refused('UNIT_CHANGED')
        return {'stable_snapshot':True}
    run('runner_unit_recheck',('runner_unit',),unit_recheck)
    def live_recheck():
        if live_target()!=live: raise Refused('PATH_RACE')
        return {'stable_reference_snapshot':True}
    run('live_reference_recheck',('live_reference',),live_recheck)
    # Do not let a unit/file mutation between independent reads become a pass.
    for name,path,expected in (t for t in TARGETS if t[0] in ('runner_root','runner_path','runner_environment','systemctl','python_target','pip')):
        def recheck(n=name,p=path,e=expected):
            now=backend.inspect(p,e,observe)
            if now!=results[n]['value']: raise Refused('PATH_RACE')
            return {'stable_snapshot':True}
        run(name+'_recheck',(name,),recheck)
    return {'protocol':PROTOCOL,'scope':'METADATA_ONLY_NOT_RELEASE_ADMISSION','status':'PARTIAL' if stopped or any(x['status']!='PASS' for x in checks) else 'COLLECTED',
        'checks':checks,'findings':findings,'totals':totals,'entries':budget.entries,'stop_reason':stopped,
        'unrun':list(UNRUN),'snapshot_atomic':False,'file_content_read':False,'credential_read':False,'database_contact':False,
        'release_authorized':False,'mutations':False}


def encode(record):
    raw=(json.dumps(record,ensure_ascii=True,allow_nan=False,separators=(',',':'))+'\n').encode('ascii')
    if len(raw)>LIMITS['output']: raise Refused('FINDING_BOUND')
    return raw


def screen(raw):
    """Reject unknown fields, malformed/duplicate JSON and unsafe success claims."""
    if type(raw) is not bytes or len(raw)>LIMITS['output']: raise Refused('UNIT_OUTPUT')
    def pairs(items):
        out={}
        for k,v in items:
            if k in out: raise Refused('UNIT_SHAPE')
            out[k]=v
        return out
    try: r=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(Refused('UNIT_SHAPE')))
    except (ValueError,UnicodeError): raise Refused('UNIT_SHAPE') from None
    expected={'protocol','scope','status','checks','findings','totals','entries','stop_reason','unrun','snapshot_atomic','file_content_read','credential_read','database_contact','release_authorized','mutations'}
    if type(r) is not dict or set(r)!=expected or r['protocol']!=PROTOCOL or r['scope']!='METADATA_ONLY_NOT_RELEASE_ADMISSION': raise Refused('UNIT_SHAPE')
    for k in ('snapshot_atomic','file_content_read','credential_read','database_contact','release_authorized','mutations'):
        if r[k] is not False: raise Refused('UNIT_SHAPE')
    if type(r['checks']) is not list or len(r['checks'])!=len(CHECKS) or type(r['findings']) is not list or len(r['findings'])>64: raise Refused('UNIT_SHAPE')
    if type(r['entries']) is not int or not 0<=r['entries']<=8192: raise Refused('UNIT_SHAPE')
    if r['stop_reason'] is not None and r['stop_reason'] not in FATAL: raise Refused('UNIT_SHAPE')
    if r['unrun']!=list(UNRUN): raise Refused('UNIT_SHAPE')
    if type(r['totals']) is not dict or set(r['totals'])!={'observed','failed_entries'}: raise Refused('UNIT_SHAPE')
    if any(type(x) is not int or not 0<=x<=8192 for x in r['totals'].values()) or r['totals']['failed_entries']>r['totals']['observed']: raise Refused('UNIT_SHAPE')
    names={};values={};fatal_seen=None
    def metadata_row(row):
        if type(row) is not dict or set(row)!={'path','metadata','acl','failures','descent_blocked'}: raise Refused('UNIT_SHAPE')
        if type(row['descent_blocked']) is not bool: raise Refused('UNIT_SHAPE')
        if row['path']!='/': lexical(row['path'])
        if type(row['failures']) is not list or any(x not in CODES for x in row['failures']): raise Refused('UNIT_SHAPE')
        if type(row['acl']) is not dict or set(row['acl'])!={'access','default'}: raise Refused('UNIT_SHAPE')
        if any(x not in ('ABSENT','PRESENT','UNKNOWN','UNRUN_FILE_METADATA_ONLY','UNRUN_UNTRUSTED_OWNER','NOT_APPLICABLE') for x in row['acl'].values()): raise Refused('UNIT_SHAPE')
        m=row['metadata']
        if m is None:
            if not row['failures'] or not row['descent_blocked']: raise Refused('UNIT_SHAPE')
            return
        if type(m) is not dict or set(m)!=set(META_KEYS) or m['kind'] not in ('DIRECTORY','REGULAR','SYMLINK','OTHER'): raise Refused('UNIT_SHAPE')
        if any(type(m[k]) is not int or not 0<=m[k]<2**64 for k in META_KEYS if k!='kind') or m['mode']>0o7777: raise Refused('UNIT_SHAPE')
        if row['descent_blocked'] != (m['kind']=='DIRECTORY' and bool(row['failures'])): raise Refused('UNIT_SHAPE')
    target_names={n for n,_,_ in TARGETS}
    target_specs={n:(p,e) for n,p,e in TARGETS}
    for row,(name,deps) in zip(r['checks'],CHECKS):
        if type(row) is not dict or set(row)!={'check','status','code','blocked_by','value'}: raise Refused('UNIT_SHAPE')
        if row['check']!=name: raise Refused('UNIT_SHAPE')
        if row['status'] not in ('PASS','FAIL','BLOCKED') or row['code'] not in CODES|{'NONE'}: raise Refused('UNIT_SHAPE')
        if fatal_seen is not None:
            if row['status']!='BLOCKED' or row['code']!=fatal_seen or row['value'] is not None: raise Refused('UNIT_SHAPE')
        elif row['code'] in FATAL:
            if row['status']!='FAIL': raise Refused('UNIT_SHAPE')
            fatal_seen=row['code']
        if row['status']=='PASS' and row['code']!='NONE': raise Refused('UNIT_SHAPE')
        blocked=[d for d in deps if names[d]!='PASS']
        if row['blocked_by']!=blocked or (blocked and row['status']!='BLOCKED'): raise Refused('UNIT_SHAPE')
        if row['status']=='BLOCKED' and (row['value'] is not None or not (blocked or row['code'] in FATAL)): raise Refused('UNIT_SHAPE')
        if row['status']!='PASS' and row['code']=='NONE': raise Refused('UNIT_SHAPE')
        value=row['value']
        if value is not None:
            if name in target_names or name in ('live_git','live_scripts','live_workflows','realtime_git'):
                metadata_row(value)
                if row['status']=='PASS' and value['failures']: raise Refused('UNIT_SHAPE')
                if name in target_specs:
                    expected_path,expected_kind=target_specs[name]
                else:
                    suffix={'live_git':'/.git','live_scripts':'/scripts','live_workflows':'/.github/workflows','realtime_git':'/.git'}[name]
                    expected_path=(REALTIME if name=='realtime_git' else values.get('live_reference',''))+suffix
                    expected_kind='DIRECTORY'
                if value['path']!=expected_path: raise Refused('UNIT_SHAPE')
                if row['status']=='PASS':
                    if value['metadata'] is None or predicates(value['metadata'],1000,expected_kind): raise Refused('UNIT_SHAPE')
                    if value['metadata']['kind']=='DIRECTORY' and any(x not in ('ABSENT','PRESENT') for x in value['acl'].values()): raise Refused('UNIT_SHAPE')
            elif name=='live_reference':
                if type(value) is not str or not re.fullmatch(re.escape(RELEASES)+r'/[A-Za-z0-9][A-Za-z0-9._-]{0,127}',value): raise Refused('UNIT_SHAPE')
            elif name=='python_alias_target':
                if value not in ('python3.10','/usr/bin/python3.10'): raise Refused('UNIT_SHAPE')
            elif name=='runner_unit':
                keys={'id_matches','user_matches','group_matches','cwd_matches','cgroup_matches','active','pid'}
                if type(value) is not dict or set(value)!=keys or any(value[k] is not True for k in keys-{'pid'}) or type(value['pid']) is not int or not 0<value['pid']<2**31: raise Refused('UNIT_SHAPE')
            else:
                key='matches_retained_target' if name=='python_alias_binding' else 'stable_reference_snapshot' if name=='live_reference_recheck' else 'stable_snapshot'
                if type(value) is not dict or value!={key:True}: raise Refused('UNIT_SHAPE')
        elif row['status']=='PASS': raise Refused('UNIT_SHAPE')
        names[name]=row['status']
        values[name]=value
    for finding in r['findings']:
        metadata_row(finding)
        if not finding['failures']: raise Refused('UNIT_SHAPE')
    wanted='PARTIAL' if r['stop_reason'] or any(x['status']!='PASS' for x in r['checks']) else 'COLLECTED'
    if r['stop_reason']!=fatal_seen: raise Refused('UNIT_SHAPE')
    if r['status']!=wanted: raise Refused('UNIT_SHAPE')
    encode(r)
    return r


def main(argv):
    if argv!=['--collect-fixed-profile']:
        print(json.dumps({'scope':'SOURCE_PREPARATION_ONLY','targets':TARGETS,'limits':LIMITS,
            'collection':'Future separate exact operator admission required; no transport, claim or retry enabled.'}))
        return 0
    # Fixed historical runtime identity; no inference that it remains installed.
    if (sys.platform!='linux' or sys.version_info[:3]!=(3,10,12) or not sys.flags.isolated or
            os.geteuid()!=1000 or sys.prefix!='/home/grid/task11-identity-private-v2' or sys.prefix==sys.base_prefix):
        return 2
    try:
        budget=Budget()
        record=collect(budget=budget);budget.tick()
        raw=encode(record);budget.tick()
        screen(raw);budget.tick()
        sys.stdout.buffer.write(raw);budget.tick()
        sys.stdout.buffer.flush();budget.tick()
        return 0
    except BaseException:
        return 2  # Zero stderr; absence of screened output is an uncertain failure.


if __name__=='__main__': raise SystemExit(main(sys.argv[1:]))
