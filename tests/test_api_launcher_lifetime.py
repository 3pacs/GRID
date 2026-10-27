"""Exact class bodies, inert OS only. No native imports or filesystem fixtures."""
import ast
from pathlib import Path
import stat
import threading
import types
import unittest


class Refusal(ValueError): pass
class Handoff(Exception): pass
def require(value,reason):
    if not value: raise Refusal(reason)
def sha(value,size=64):
    return isinstance(value,str) and len(value)==size and all(c in '0123456789abcdef' for c in value)


class FakeOS:
    O_RDONLY=0
    def __init__(self,target):
        self.target=target; self.events=[]; self.supports_fd={self.execve}
        self.reused={}; self.fail_close=None; self.fail_chdir=False; self.return_exec=False
    def stat(self,*args,**kwargs):
        self.events.append(('stat',)); return types.SimpleNamespace(st_mode=stat.S_IFLNK,st_uid=11,st_nlink=1)
    def readlink(self,*args,**kwargs): self.events.append(('readlink',)); return self.target
    def close(self,fd):
        self.events.append(('close',fd)); self.reused[fd]='UNRELATED_DESCRIPTOR'
        if fd==self.fail_close: raise OSError('uncertain close result')
    def fchdir(self,fd):
        self.events.append(('fchdir',fd))
        if self.fail_chdir: raise OSError('synthetic chdir failure')
    def execve(self,fd,argv,environment):
        self.events.append(('execve',fd))
        if not self.return_exec: raise Handoff()


class Directory:
    def __init__(self,fd): self.fd=fd; self.uid=11
    def regular(self,name,flags): return 55


def arrange(source=None):
    source=source or Path(__file__).resolve().parents[1]/'scripts/api_isolation_native_launcher.py'
    syntax=ast.parse(source.read_text(encoding='utf-8'))
    body=[n for n in syntax.body if isinstance(n,ast.ClassDef) and n.name=='PinnedLauncher']
    commit='b'*40; target='/data/grid_v4/api-isolated/releases/'+commit
    fake=FakeOS(target); allocated=iter((51,52,53,54)); events=[]
    authority=types.SimpleNamespace(
        verify_launch_descriptors=lambda *args:events.append('verify') or True,
        verify_bootstrap_program=lambda *args:events.append('bootstrap') or True)
    namespace={'os':fake,'stat':stat,'threading':threading,'require':require,'sha':sha,
               'directory_at':lambda *args:Directory(next(allocated)),
               'render_bound_program':lambda *args:events.append('render') or 'INERT_PROGRAM'}
    exec(compile(ast.Module(body=body,type_ignores=[]),str(source),'exec'),namespace)
    launcher=namespace['PinnedLauncher'](Directory(1),Directory(2),commit,target,authority)
    token,_=launcher.pin_current()
    spec={'interpreter':target+'/runtime/bin/python3.10','cwd':target+'/source'}
    launcher.verify_release(token,commit,spec)
    program=launcher.state_bound_program(token,spec,{'synthetic':True})
    argv=[spec['interpreter'],'-I','-S','-B','-c',program]
    return launcher,token,spec,argv,fake,events,namespace


class LauncherLifetimeTests(unittest.TestCase):
    def test_live_handoff_once_then_consumed_before_any_retry_os_call(self):
        launcher,token,spec,argv,fake,events,ns=arrange()
        with self.assertRaises(Handoff): launcher.exec_pinned(token,argv,{},spec['cwd'])
        self.assertEqual(fake.events[-2:],[('fchdir',52),('execve',55)])
        count=len(fake.events)
        for operation in (lambda:launcher.exec_pinned(token,argv,{},spec['cwd']),
                          lambda:launcher.verify_release(token,launcher.commit,spec),
                          lambda:launcher.state_bound_program(token,spec,{})):
            with self.assertRaises(Refusal): operation()
        self.assertEqual(len(fake.events),count)

    def test_closed_reused_descriptors_have_no_verification_or_os_use(self):
        launcher,token,spec,argv,fake,events,ns=arrange(); launcher.close()
        self.assertEqual(set(fake.reused),{51,52,53,54,55})
        before=list(fake.events); callbacks=list(events)
        for operation in (lambda:launcher.exec_pinned(token,argv,{},spec['cwd']),
                          lambda:launcher.verify_release(token,launcher.commit,spec),
                          lambda:launcher.state_bound_program(token,spec,{})):
            with self.assertRaises(Refusal): operation()
        self.assertEqual(fake.events,before); self.assertEqual(events,callbacks)

    def test_partial_close_revokes_all_and_never_retries_reused_number(self):
        launcher,token,spec,argv,fake,events,ns=arrange(); fake.fail_close=54
        with self.assertRaises(OSError): launcher.close()
        self.assertEqual(fake.events[-5:],[('close',55),('close',54),('close',53),('close',52),('close',51)])
        before=list(fake.events); launcher.close(); launcher.close()
        with self.assertRaises(Refusal): launcher.exec_pinned(token,argv,{},spec['cwd'])
        self.assertEqual(fake.events,before)

    def test_close_revokes_before_first_cleanup_callback(self):
        launcher,token,spec,argv,fake,events,ns=arrange(); original=fake.close
        def close(fd):
            with self.assertRaises(Refusal): launcher.exec_pinned(token,argv,{},spec['cwd'])
            original(fd)
        fake.close=close; launcher.close()
        self.assertFalse(any(row[0] in ('fchdir','execve') for row in fake.events))

    def test_chdir_failure_consumes_token(self):
        launcher,token,spec,argv,fake,events,ns=arrange(); fake.fail_chdir=True
        with self.assertRaises(OSError): launcher.exec_pinned(token,argv,{},spec['cwd'])
        before=list(fake.events)
        with self.assertRaises(Refusal): launcher.exec_pinned(token,argv,{},spec['cwd'])
        self.assertEqual(fake.events,before); launcher.close()

    def test_exec_return_does_not_restore_authority(self):
        launcher,token,spec,argv,fake,events,ns=arrange(); fake.return_exec=True
        launcher.exec_pinned(token,argv,{},spec['cwd']); before=list(fake.events)
        with self.assertRaises(Refusal): launcher.exec_pinned(token,argv,{},spec['cwd'])
        self.assertEqual(fake.events,before)

    def test_bootstrap_error_invalidates_old_program_without_os_calls(self):
        for mode in ('render','authority'):
            launcher,token,spec,argv,fake,events,ns=arrange(); before=list(fake.events)
            if mode=='render':
                def fail(*args): raise ValueError('invalid binding')
                ns['render_bound_program']=fail
            else: launcher.authority.verify_bootstrap_program=lambda *args:False
            with self.assertRaises((Refusal,ValueError)): launcher.state_bound_program(token,spec,{})
            with self.assertRaises(Refusal): launcher.exec_pinned(token,argv,{},spec['cwd'])
            self.assertEqual(fake.events,before); launcher.close()

    def test_reentrant_revocation_from_authority_cannot_regrant(self):
        for stage in ('verify','bootstrap'):
            launcher,token,spec,argv,fake,events,ns=arrange()
            def revoke(*args): launcher.close(); return True
            if stage=='verify':
                launcher.authority.verify_launch_descriptors=revoke
                call=lambda:launcher.verify_release(token,launcher.commit,spec)
            else:
                launcher.authority.verify_bootstrap_program=revoke
                call=lambda:launcher.state_bound_program(token,spec,{})
            with self.assertRaises(Refusal): call()
            before=list(fake.events)
            with self.assertRaises(Refusal): launcher.exec_pinned(token,argv,{},spec['cwd'])
            self.assertEqual(fake.events,before)

    def test_unknown_token_and_closed_launcher_refuse_without_os_work(self):
        launcher,token,spec,argv,fake,events,ns=arrange(); before=list(fake.events)
        with self.assertRaises(Refusal): launcher.exec_pinned(object(),argv,{},spec['cwd'])
        self.assertEqual(fake.events,before)
        launcher.close(); before=list(fake.events)
        with self.assertRaises(Refusal): launcher.pin_current()
        self.assertEqual(fake.events,before)

    def test_changed_spec_cannot_bind_program(self):
        launcher,token,spec,argv,fake,events,ns=arrange(); before=list(fake.events)
        with self.assertRaises(Refusal): launcher.state_bound_program(token,{**spec,'cwd':'/unrelated'}, {})
        self.assertEqual(fake.events,before)


if __name__=='__main__': unittest.main()
