import copy
import contextvars
import errno
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import struct
import sys
import types
import unittest
from unittest.mock import patch

P=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('candidate',P/'scripts/deploy_metadata_preflight.py')
M=importlib.util.module_from_spec(spec);spec.loader.exec_module(M)
EVENTS=[]
BOUNDARY_ACTIVE=contextvars.ContextVar('metadata_test_boundary_active',default=False)
def audit(event,args):
    if BOUNDARY_ACTIVE.get() and (event.startswith('socket.') or event in {'subprocess.Popen','os.system','os.exec','os.posix_spawn','os.chmod','os.chown','os.setxattr','os.kill','os.killpg'}):
        EVENTS.append(event);raise AssertionError('OFFLINE_BOUNDARY')
sys.addaudithook(audit)

class FakeOS:
    O_PATH=os.O_PATH;O_DIRECTORY=os.O_DIRECTORY;O_NOFOLLOW=os.O_NOFOLLOW;O_CLOEXEC=os.O_CLOEXEC;O_RDONLY=os.O_RDONLY
    def __init__(self):
        self.nodes={};self.fds={};self.serial=10;self.calls=[];self.links={};self.acls={};self.fail={};self.hook=None
        self.add('/',stat.S_IFDIR|0o755,uid=0)
    def add(self,path,mode=stat.S_IFREG|0o644,uid=1000,gid=1000,**extra):
        if path!='/':
            parent=path.rsplit('/',1)[0] or '/'
            if parent not in self.nodes:self.add(parent,stat.S_IFDIR|0o755,uid=0 if parent in ('/usr','/usr/bin','/data','/home') else 1000)
        v=dict(st_mode=mode,st_uid=uid,st_gid=gid,st_nlink=1,st_size=10,st_dev=1,st_ino=len(self.nodes)+1,st_mtime_ns=1,st_ctime_ns=1)
        v.update(extra);self.nodes[path]=v
    def resolve(self,path,dir_fd=None):
        if path=='/':return '/'
        if dir_fd is None:return path
        parent=self.fds[dir_fd]
        if path=='.' or path=='':return parent
        return parent.rstrip('/')+'/'+path
    def hit(self,action,path):
        self.calls.append((action,path))
        if self.hook:self.hook(action,path,self)
        if (action,path) in self.fail:raise self.fail[action,path]
        if path not in self.nodes:raise FileNotFoundError(errno.ENOENT,'never exported')
    def open(self,path,flags,dir_fd=None):
        q=self.resolve(path,dir_fd);self.hit('open',q)
        assert flags&self.O_NOFOLLOW and flags&self.O_CLOEXEC
        if not flags&self.O_PATH:
            assert flags&self.O_DIRECTORY and stat.S_ISDIR(self.nodes[q]['st_mode']), 'regular content open'
        self.serial+=1;self.fds[self.serial]=q;return self.serial
    def stat(self,path,dir_fd=None,follow_symlinks=None):
        assert follow_symlinks is False
        q=self.resolve(path,dir_fd);self.hit('stat',q);return types.SimpleNamespace(**self.nodes[q])
    def lstat(self,path):return self.stat(path,follow_symlinks=False)
    def fstat(self,fd):
        q=self.fds[fd];self.hit('fstat',q);return types.SimpleNamespace(**self.nodes[q])
    def close(self,fd):
        q=self.fds[fd];self.calls.append(('close',q))
        if ('close',q) in self.fail:raise self.fail['close',q]
        del self.fds[fd]
    def readlink(self,path,dir_fd):
        assert path=='';q=self.fds[dir_fd];self.hit('readlink',q);return self.links[q]
    def getxattr(self,fd,name):
        q=self.fds[fd];self.hit('acl',q)
        v=self.acls.get((q,name),OSError(errno.ENODATA,'absent'))
        if isinstance(v,Exception):raise v
        return v
    def scandir(self,fd):
        q=self.fds[fd];self.hit('scandir',q)
        names=[p[len(q)+1:] for p in self.nodes if p.startswith(q+'/') and '/' not in p[len(q)+1:]]
        parent=self
        class Iterator:
            def __iter__(self):
                for name in names:
                    parent.calls.append(('enumerated',q+'/'+name))
                    yield types.SimpleNamespace(name=name)
            def close(self):
                parent.calls.append(('iterator_close',q))
                if ('iterator_close',q) in parent.fail:raise parent.fail['iterator_close',q]
        return Iterator()

def filesystem():
    fs=FakeOS()
    for _,p,k in M.TARGETS:
        fs.add(p,(stat.S_IFDIR|0o755) if k=='DIRECTORY' else (stat.S_IFLNK|0o777) if k=='SYMLINK' else stat.S_IFREG|(0o755 if k=='EXECUTABLE' else 0o644))
    root=M.RELEASES+'/synthetic-sha'
    fs.add(M.LIVE,stat.S_IFLNK|0o777);fs.links[M.LIVE]=root
    fs.links['/usr/bin/python3']='python3.10'
    for base in (root+'/.git',root+'/scripts',root+'/.github/workflows',M.REALTIME+'/.git'):
        fs.add(base,stat.S_IFDIR|0o755);fs.add(base+'/one');fs.add(base+'/two')
    return fs

def raw_unit(**changes):
    d=dict(Id=M.UNIT,User='grid',Group='',WorkingDirectory='/home/grid/actions-runner',MainPID='321',ControlGroup='/system.slice/'+M.UNIT,ActiveState='active')
    d.update(changes)
    return ('\n'.join(k+'='+v for k,v in d.items())+'\n').encode()

class Tests(unittest.TestCase):
    def setUp(self):
        token=BOUNDARY_ACTIVE.set(True)
        self.addCleanup(BOUNDARY_ACTIVE.reset,token)
    def tearDown(self):self.assertEqual(EVENTS,[])
    def collect(self,fs=None,unit=None,limits=None):
        fs=fs or filesystem();b=M.Budget(clock=lambda:1,limits=limits)
        with patch.object(M,'os',fs):
            result=M.collect(M.Metadata(b),b,unit or (lambda:M.unit_properties(raw_unit())))
        return result,fs
    def states(self,r):return {x['check']:x for x in r['checks']}
    def test_complete_fixed_profile_and_screen(self):
        r,fs=self.collect();self.assertEqual(len(r['checks']),33);self.assertEqual(r['status'],'COLLECTED');self.assertEqual(r['findings'],[]);self.assertEqual(fs.fds,{})
        self.assertEqual(M.screen(M.encode(r)),r);self.assertFalse(r['release_authorized']);self.assertIn('regular_file_ACL',r['unrun'])
    def test_many_independent_failures_one_pass(self):
        fs=filesystem();fs.nodes['/home/grid/actions-runner/.path']['st_mode']=stat.S_IFREG|0o666
        fs.nodes['/home/grid/actions-runner/.env'].update(st_uid=2001,st_nlink=2,st_mode=stat.S_IFREG|0o664)
        fs.nodes['/usr/bin/pip']['st_mode']=stat.S_IFREG|0o644
        fs.nodes[M.REALTIME+'/.git/one']['st_mode']=stat.S_IFREG|0o666
        r,_=self.collect(fs,unit=lambda:M.unit_properties(raw_unit(User='wrong')));s=self.states(r)
        for key in ('runner_path','runner_environment','pip','realtime_git','runner_unit'):self.assertEqual(s[key]['status'],'FAIL',key)
        self.assertEqual(s['git']['status'],'PASS');self.assertEqual(s['python_target']['status'],'PASS')
        by={x['path']:x for x in r['findings']};self.assertEqual(set(by['/home/grid/actions-runner/.path']['failures']),{'GROUP_WRITE','OTHER_WRITE'})
        self.assertTrue({'PATH_OWNER','GROUP_WRITE','MULTIPLE_LINKS'}<=set(by['/home/grid/actions-runner/.env']['failures']))
        self.assertEqual(M.screen(M.encode(r)),r)
    def test_unsafe_parent_blocks_descendants_not_independent_targets(self):
        fs=filesystem();fs.nodes['/home/grid/actions-runner']['st_mode']=stat.S_IFDIR|0o775
        r,fs=self.collect(fs);s=self.states(r)
        self.assertEqual(s['runner_path']['status'],'FAIL');self.assertEqual(s['runner_unit']['status'],'BLOCKED');self.assertEqual(s['pip']['status'],'PASS')
        self.assertNotIn(('open','/home/grid/actions-runner/.path'),fs.calls)
    def test_symlink_directory_not_traversed_but_sibling_checked(self):
        fs=filesystem();p=M.REALTIME+'/.git/one';fs.nodes[p]['st_mode']=stat.S_IFLNK|0o777;fs.links[p]='/secret'
        r,fs=self.collect(fs);self.assertTrue(any(x['path']==p and 'PATH_LINK' in x['failures'] for x in r['findings']))
        self.assertNotIn(('readlink',p),fs.calls);self.assertIn(('open',M.REALTIME+'/.git/two'),fs.calls)
    def test_unexpected_live_link_scope_refuses_and_keeps_other_roots(self):
        fs=filesystem();fs.links[M.LIVE]='/etc/secret'
        r,fs=self.collect(fs);s=self.states(r)
        self.assertEqual(s['live_reference']['code'],'LINK_TARGET_SCOPE');self.assertEqual(s['live_git']['status'],'BLOCKED');self.assertEqual(s['realtime_git']['status'],'PASS');self.assertNotIn('/etc/secret',json.dumps(r))
    def test_dot_segments_control_chars_and_empty_components_rejected(self):
        for p in ('relative','/a/../b','/a/./b','/a//b','/a/','/a\nsecret','/a\x00b'):
            with self.subTest(p=p),self.assertRaises(M.Refused):M.lexical(p)
    def test_case_unrepresentable_child_is_private_coverage_failure(self):
        fs=filesystem();p=M.REALTIME+'/.git/bad\nPRIVATE';fs.add(p)
        r,_=self.collect(fs);self.assertTrue(any('PATH_SYNTAX' in x['failures'] for x in r['findings']));self.assertNotIn('PRIVATE',json.dumps(r))
    def test_lookup_inaccessible_does_not_hide_sibling_or_other_root(self):
        fs=filesystem();p=M.REALTIME+'/.git/one';fs.fail['stat',p]=PermissionError(errno.EACCES,'SECRET')
        r,fs=self.collect(fs);self.assertTrue(any(x['path']==p and x['failures']==['IO_INACCESSIBLE'] for x in r['findings']));self.assertIn(('open',M.REALTIME+'/.git/two'),fs.calls);self.assertNotIn('SECRET',json.dumps(r));self.assertEqual(M.screen(M.encode(r)),r)
    def test_acl_absence_present_unknown_and_malformed(self):
        fs=filesystem();p=M.REALTIME
        raw=struct.pack('<I',2)+b''.join(struct.pack('<HHI',t,7,0xffffffff) for t in (1,4,32))
        fs.acls[p,'system.posix_acl_default']=raw
        r,fs=self.collect(fs);self.assertEqual(self.states(r)['realtime_root']['value']['acl']['default'],'PRESENT');self.assertIn(('open',p+'/.git'),fs.calls);M.screen(M.encode(r))
        for raw in (b'',b'PRIVATE',struct.pack('<I',3),b'x'*261):
            fs=filesystem();fs.acls[p,'system.posix_acl_access']=raw;r,_=self.collect(fs);self.assertTrue(any('ACL_UNKNOWN' in x['failures'] for x in r['findings']));self.assertNotIn('PRIVATE',json.dumps(r))
    def test_acl_access_denial_is_unknown_not_absent(self):
        fs=filesystem();fs.acls[M.REALTIME,'system.posix_acl_access']=PermissionError(errno.EACCES,'private')
        r,_=self.collect(fs);row=next(x for x in r['findings'] if x['path']==M.REALTIME);self.assertEqual(row['acl']['access'],'UNKNOWN')
    def test_foreign_directory_owner_never_acl_opens_it(self):
        fs=filesystem();fs.nodes[M.REALTIME]['st_uid']=2000;r,fs=self.collect(fs)
        self.assertNotIn(('acl',M.REALTIME),fs.calls);self.assertEqual(self.states(r)['realtime_git']['status'],'BLOCKED')
    def test_multiple_unsafe_nested_directories_are_all_reported(self):
        fs=filesystem();root=M.REALTIME+'/.git'
        for name in ('a','b','c'):
            fs.add(root+'/'+name,stat.S_IFDIR|0o777);fs.add(root+'/'+name+'/unsafe_to_read')
        r,fs=self.collect(fs);paths={x['path'] for x in r['findings']};self.assertTrue({root+'/a',root+'/b',root+'/c'}<=paths)
        self.assertFalse(any('unsafe_to_read' in p for _,p in fs.calls))
    def test_name_substitution_detected_before_use(self):
        fs=filesystem();p='/home/grid/actions-runner/.path';seen=[0]
        def hook(action,path,backend):
            if path==p and action=='open':backend.nodes[path]['st_ino']+=100
        fs.hook=hook;r,fs=self.collect(fs);self.assertEqual(self.states(r)['runner_path']['code'],'PATH_RACE');self.assertEqual(fs.fds,{})
    def test_ancestor_mutation_detected_after_read(self):
        fs=filesystem();hit=[False]
        def hook(action,path,backend):
            if action=='open' and path=='/home/grid/actions-runner/.path' and not hit[0]:
                hit[0]=True;backend.nodes['/home/grid']['st_ctime_ns']+=1
        fs.hook=hook;r,_=self.collect(fs);self.assertEqual(self.states(r)['runner_path']['code'],'PATH_RACE')
    def test_nested_race_is_reported_and_sibling_continues(self):
        fs=filesystem();p=M.REALTIME+'/.git/one';seen=[0]
        def hook(action,path,backend):
            if action=='fstat' and path==p:
                seen[0]+=1
                if seen[0]==3:backend.nodes[p]['st_ctime_ns']+=1
        fs.hook=hook;r,fs=self.collect(fs);self.assertTrue(any(x['path']==p and 'PATH_RACE' in x['failures'] for x in r['findings']));self.assertIn(('open',M.REALTIME+'/.git/two'),fs.calls)
    def test_close_uncertainty_stops_remaining_checks(self):
        fs=filesystem();fs.fail['close','/home/grid/actions-runner']=OSError('private')
        r,fs=self.collect(fs);self.assertEqual(r['stop_reason'],'CLEANUP_UNKNOWN');self.assertTrue(all(x['status']=='BLOCKED' for x in r['checks'][1:]));self.assertIn(('close','/'),fs.calls);self.assertNotIn('private',json.dumps(r));M.screen(M.encode(r))
    def test_iterator_cleanup_uncertainty_is_fatal(self):
        fs=filesystem();fs.fail['iterator_close',M.REALTIME+'/.git']=OSError('private')
        r,_=self.collect(fs);self.assertEqual(r['stop_reason'],'CLEANUP_UNKNOWN');self.assertEqual(self.states(r)['runner_unit']['status'],'BLOCKED')
    def test_fstat_failure_does_not_leak_new_descriptor(self):
        fs=filesystem();fs.fail['fstat','/home/grid/actions-runner/.path']=OSError(errno.EIO,'private')
        r,fs=self.collect(fs);self.assertNotEqual(self.states(r)['runner_path']['status'],'PASS');self.assertEqual(fs.fds,{})
    def test_entry_bound_marks_all_future_dependencies_unrun(self):
        limits=dict(M.LIMITS,entries=5);r,fs=self.collect(limits=limits);self.assertEqual(r['stop_reason'],'ENTRY_BOUND');self.assertEqual(fs.fds,{});self.assertTrue(any(x['status']=='BLOCKED' for x in r['checks']))
    def test_finding_bound_is_explicit(self):
        fs=filesystem();fs.nodes['/home/grid/actions-runner/.path']['st_mode']=stat.S_IFREG|0o666;fs.nodes['/usr/bin/pip']['st_mode']=stat.S_IFREG|0o666
        r,_=self.collect(fs,limits=dict(M.LIMITS,findings=1));self.assertEqual(r['stop_reason'],'FINDING_BOUND');self.assertEqual(len(r['findings']),1)
    def test_timeout_preserves_cleanup_and_blocks_remaining(self):
        fs=filesystem();clock=[0]
        def hook(action,path,backend):
            if path=='/home/grid/actions-runner/.path':clock[0]=31
        fs.hook=hook;b=M.Budget(clock=lambda:clock[0])
        with patch.object(M,'os',fs):r=M.collect(M.Metadata(b),b,lambda:M.unit_properties(raw_unit()))
        self.assertEqual(r['stop_reason'],'TIME_BOUND');self.assertEqual(fs.fds,{})
    def test_unit_missing_duplicate_unknown_and_nonascii_rejected(self):
        for raw in (raw_unit()+b'User=grid\n',raw_unit()+b'Environment=PRIVATE\n',raw_unit().replace(b'Group=\n',b''),b'\xff'):
            with self.assertRaises(M.Refused):M.unit_properties(raw)
    def test_unit_multiple_binding_failures_do_not_export_values(self):
        r,_=self.collect(unit=lambda:M.unit_properties(raw_unit(User='PRIVATE',WorkingDirectory='/private')))
        self.assertEqual(self.states(r)['runner_unit']['code'],'UNIT_BINDING');self.assertNotIn('PRIVATE',json.dumps(r));self.assertNotIn('/private',json.dumps(r))
    def test_unit_pid_and_state_controls(self):
        for edit in ({'MainPID':'0'},{'MainPID':'2147483648'},{'MainPID':'1\nUser=grid'},{'ActiveState':'inactive'}):
            with self.assertRaises(M.Refused):M.unit_properties(raw_unit(**edit))
    def test_unit_change_blocks_stability_claim(self):
        values=iter([M.unit_properties(raw_unit()),M.unit_properties(raw_unit(MainPID='322'))]);r,_=self.collect(unit=lambda:next(values));self.assertEqual(self.states(r)['runner_unit_recheck']['code'],'UNIT_CHANGED')
    def test_alias_binding_failure_not_secret_value(self):
        fs=filesystem();fs.links['/usr/bin/python3']='/PRIVATE/value';r,_=self.collect(fs);self.assertEqual(self.states(r)['python_alias_target']['code'],'LINK_TARGET_SCOPE');self.assertNotIn('PRIVATE',json.dumps(r))
    def test_file_predicate_type_setid_linkcount_and_mode(self):
        m=M.snapshot(types.SimpleNamespace(st_mode=stat.S_IFREG|0o6666,st_uid=2001,st_gid=2001,st_nlink=2,st_size=5,st_dev=1,st_ino=1,st_mtime_ns=1,st_ctime_ns=1))
        self.assertTrue({'PATH_OWNER','GROUP_WRITE','OTHER_WRITE','SPECIAL_MODE','MULTIPLE_LINKS','NOT_EXECUTABLE'}<=set(M.predicates(m,1000,'EXECUTABLE')))
    def test_regular_file_never_read_and_regular_acl_disclosed(self):
        r,fs=self.collect();self.assertTrue(all(a not in ('read','read_file') for a,_ in fs.calls));row=self.states(r)['runner_environment']['value'];self.assertEqual(row['acl']['access'],'UNRUN_FILE_METADATA_ONLY')
    def test_screen_unknown_nested_secret_and_wrong_status(self):
        r,_=self.collect()
        edits=[lambda x:x.update(secret='PRIVATE'),lambda x:x['checks'][0]['value']['metadata'].update(secret='PRIVATE'),lambda x:x['checks'][0]['value']['acl'].update(secret='PRIVATE'),lambda x:x.update(release_authorized=True),lambda x:x.update(status='PARTIAL'),lambda x:x['checks'][0].update(check='unexpected'),lambda x:x['checks'][-1].update(blocked_by=['runner_root']),lambda x:x['unrun'].clear(),lambda x:x['checks'][0].update(value=None),lambda x:x['checks'][0]['value'].update(path='/wrong/root'),lambda x:x['checks'][0]['value']['metadata'].update(mode=0o777)]
        for edit in edits:
            v=copy.deepcopy(r);edit(v)
            with self.assertRaises(M.Refused):M.screen(M.encode(v))
    def test_screen_malformed_json_caps_duplicates_bool_numbers(self):
        r,_=self.collect();raw=M.encode(r)
        for bad in (raw+raw,b'[' ,b' '*65537,raw.replace(b'"protocol":',b'"protocol":"duplicate","protocol":',1),raw.replace(b'"entries":',b'"entries":true,"original_entries":',1)):
            with self.assertRaises((M.Refused,TypeError)):M.screen(bad)
    def test_unexpected_os_error_is_sanitized(self):
        fs=filesystem();fs.fail['open','/usr/bin/pip']=RuntimeError('PRIVATE_TOKEN');r,_=self.collect(fs);self.assertEqual(self.states(r)['pip']['code'],'UNEXPECTED_FAILURE');self.assertNotIn('PRIVATE_TOKEN',json.dumps(r))
    def test_no_product_import_transport_or_mutation_capabilities(self):
        import ast
        tree=ast.parse((P/'scripts/deploy_metadata_preflight.py').read_text())
        names={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertFalse(names&{'chmod','fchmod','chown','setxattr','connect','read_text','read_bytes','write_text','write_bytes'})
        self.assertFalse(any(isinstance(n,ast.ImportFrom) and n.module and n.module.split('.')[0] in ('config','psycopg','intelligence') for n in ast.walk(tree)))
    def command_fixture(self,chunks=None,clock=None,close_fail=False,wait_fail=False,launch_fail=False,leader_exited_group_alive=False,group_persists=False,close_effect=None):
        calls=[];streams=[];clock=clock or (lambda:1)
        group_alive=[leader_exited_group_alive]
        chunks=copy.deepcopy(chunks if chunks is not None else {1:[raw_unit(),b''],2:[b'']})
        class Stream:
            def __init__(self,n):self.n=n;streams.append(self)
            def fileno(self):return self.n
            def close(self):
                calls.append(('stream_close',self.n))
                if close_effect:close_effect()
                if close_fail:raise OSError('PRIVATE')
        class Process:
            pid=123
            stdout=Stream(1);stderr=Stream(2)
            def poll(self):return 0
            def wait(self,timeout):
                calls.append(('wait',timeout))
                if wait_fail:raise TimeoutError('PRIVATE')
                return 0
        class Selector:
            def __init__(self):self.items={}
            def register(self,s,events,data):self.items[s.n]=types.SimpleNamespace(fileobj=s,data=data)
            def get_map(self):return self.items
            def select(self,remaining):return [(v,None) for v in list(self.items.values())]
            def unregister(self,s):del self.items[s.n]
            def close(self):calls.append(('selector_close',))
        def popen(argv,**kwargs):
            calls.append(('popen',argv,kwargs))
            if launch_fail:raise OSError('PRIVATE')
            return Process()
        def read(fd,n):return chunks[fd].pop(0)
        def killpg(pid,sig):
            assert pid==123 and sig in (0,9)
            calls.append(('killpg',pid,sig))
            if sig==9:
                if not group_persists:group_alive[0]=False
                return
            if not group_alive[0]:raise ProcessLookupError()
        b=M.Budget(clock=clock)
        with patch.object(M.subprocess,'Popen',popen),patch.object(M.selectors,'DefaultSelector',Selector),patch.object(M.os,'read',read),patch.object(M.os,'killpg',killpg):
            try:return M.unit_read(b),None,calls
            except M.Refused as error:return None,error.code,calls
    def test_fixed_unit_command_success_and_no_environment_export(self):
        value,code,calls=self.command_fixture();self.assertIsNone(code);self.assertEqual(value['pid'],321)
        pop=next(x for x in calls if x[0]=='popen');self.assertEqual(pop[1],['/usr/bin/systemctl','show',M.UNIT,'--all','--no-pager','--property='+','.join(M.KEYS)])
        self.assertNotIn('shell',pop[2]);self.assertEqual(set(pop[2]['env']),{'PATH','LANG','LC_ALL','SYSTEMD_PAGER','SYSTEMD_COLORS'})
        self.assertTrue(pop[2]['start_new_session']);self.assertEqual([x for x in calls if x[0]=='stream_close'],[('stream_close',1),('stream_close',2)])
    def test_unit_stderr_and_stdout_caps_refuse_without_export(self):
        for chunks in ({1:[b''],2:[b'PRIVATE']},{1:[b'x'*4097],2:[b'']}):
            value,code,calls=self.command_fixture(chunks);self.assertIsNone(value);self.assertEqual(code,'UNIT_OUTPUT');self.assertEqual(len([x for x in calls if x[0]=='stream_close']),2)
    def test_unit_launch_reap_and_close_uncertainty_fail_closed(self):
        for kw in ({'launch_fail':True},{'wait_fail':True},{'close_fail':True}):
            value,code,_=self.command_fixture(**kw);self.assertIsNone(value);self.assertEqual(code,'CLEANUP_UNKNOWN')
    def test_unit_timeout_still_reaps_and_closes(self):
        times=iter([1,1,1,5,5,5]);value,code,calls=self.command_fixture(clock=lambda:next(times,5))
        self.assertIsNone(value);self.assertEqual(code,'UNIT_TIMEOUT');self.assertTrue(any(x[0]=='wait' for x in calls));self.assertEqual(len([x for x in calls if x[0]=='stream_close']),2)
    def test_exited_leader_descendant_is_killed_and_not_passed(self):
        value,code,calls=self.command_fixture(leader_exited_group_alive=True)
        self.assertIsNone(value);self.assertEqual(code,'CLEANUP_UNKNOWN');self.assertIn(('killpg',123,9),calls)
        self.assertEqual([x for x in calls if x[0]=='killpg'][-1],('killpg',123,0))
    def test_surviving_group_after_kill_is_uncertain(self):
        value,code,calls=self.command_fixture(leader_exited_group_alive=True,group_persists=True)
        self.assertIsNone(value);self.assertEqual(code,'CLEANUP_UNKNOWN');self.assertIn(('killpg',123,9),calls)
    def test_shared_cleanup_budget_covers_streams_and_group_confirmation(self):
        clock=[1]
        value,code,calls=self.command_fixture(clock=lambda:clock[0],close_effect=lambda:clock.__setitem__(0,clock[0]+.3))
        self.assertIsNone(value);self.assertEqual(code,'CLEANUP_UNKNOWN');self.assertEqual(len([x for x in calls if x[0]=='stream_close']),2)
    def test_screen_rejects_fatal_prefix_then_independent_pass(self):
        r,_=self.collect();rows=self.states(r)
        rows['runner_path'].update(status='FAIL',code='CLEANUP_UNKNOWN',value=None)
        rows['runner_path_recheck'].update(status='BLOCKED',code='PREREQUISITE',blocked_by=['runner_path'],value=None)
        r.update(status='PARTIAL',stop_reason='CLEANUP_UNKNOWN')
        with self.assertRaises(M.Refused):M.screen(M.encode(r))
    def test_screen_rejects_fatal_stop_reason_without_fatal_check(self):
        r,_=self.collect();r.update(status='PARTIAL',stop_reason='TIME_BOUND')
        with self.assertRaises(M.Refused):M.screen(M.encode(r))
    def test_every_rejected_name_counts_toward_entry_cap(self):
        fs=filesystem();root=M.REALTIME+'/.git'
        for i in range(100):fs.add(root+'/bad\n'+str(i))
        b=M.Budget(clock=lambda:1,limits=dict(M.LIMITS,entries=20));rows=[]
        with patch.object(M,'os',fs),self.assertRaises(M.Refused) as caught:
            M.Metadata(b).tree(root,rows.append)
        self.assertEqual(caught.exception.code,'ENTRY_BOUND');self.assertEqual(b.entries,20)
        self.assertLess(len([x for x in fs.calls if x[0]=='enumerated']),100);self.assertEqual(fs.fds,{})
    def run_deadline_main(self,phase=None):
        r,_=self.collect();clock=[0];budget=M.Budget(clock=lambda:clock[0]);output=io.BytesIO();calls=[]
        original_encode=M.encode;original_screen=M.screen
        def delayed(which,value):
            if phase==which:clock[0]=31
            return value
        class Buffer:
            def write(self,raw):calls.append('write');output.write(raw);delayed('write',None)
            def flush(self):calls.append('flush');delayed('flush',None)
        with patch.object(M,'Budget',return_value=budget),patch.object(M,'collect',side_effect=lambda **kw:delayed('collect',r)),patch.object(M,'encode',side_effect=lambda value:delayed('encode',original_encode(value))),patch.object(M,'screen',side_effect=lambda raw:delayed('screen',original_screen(raw))),patch.object(M.sys,'version_info',(3,10,12)),patch.object(M.sys,'flags',types.SimpleNamespace(isolated=True)),patch.object(M.sys,'prefix','/home/grid/task11-identity-private-v2'),patch.object(M.sys,'base_prefix','/usr'),patch.object(M.os,'geteuid',return_value=1000),patch.object(M.sys,'stdout',types.SimpleNamespace(buffer=Buffer())):
            rc=M.main(['--collect-fixed-profile'])
        return rc,output.getvalue(),calls
    def test_final_collection_encoding_screening_write_flush_deadlines(self):
        for phase in ('collect','encode','screen','write','flush'):
            with self.subTest(phase=phase):
                rc,raw,calls=self.run_deadline_main(phase);self.assertEqual(rc,2)
                if phase in ('collect','encode','screen'):self.assertEqual(raw,b'')
                if phase=='write':self.assertNotIn('flush',calls)
    def test_timely_final_output_is_screened_and_successful(self):
        rc,raw,calls=self.run_deadline_main();self.assertEqual(rc,0);M.screen(raw);self.assertEqual(calls,['write','flush'])

if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    print(json.dumps({'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'skips':len(result.skipped),'boundary_events':EVENTS,'python':sys.version}))
    raise SystemExit(0 if result.wasSuccessful() and not EVENTS else 1)
