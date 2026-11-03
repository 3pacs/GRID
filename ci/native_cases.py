"""Synthetic native backend cases only. Never call the installed fixed profile."""
import errno
import importlib.util
import os
from pathlib import Path
import shutil
import stat
import subprocess
import unittest
from unittest.mock import patch

HERE = Path(__file__).absolute().parent
spec = importlib.util.spec_from_file_location('pinned_metadata', HERE/'metadata_preflight.py')
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)
BASE = None
DIRTY = False


def remove_owned(path, identity):
    now = path.lstat()
    if (not stat.S_ISDIR(now.st_mode) or (now.st_dev, now.st_ino, now.st_uid) != identity
            or now.st_uid != os.geteuid() or not shutil.rmtree.avoids_symlink_attacks):
        raise RuntimeError('CLEANUP_IDENTITY')
    shutil.rmtree(path)
    if os.path.lexists(path):
        raise RuntimeError('CLEANUP_REMAINS')


class NativeCases(unittest.TestCase):
    def setUp(self):
        if DIRTY or BASE is None:
            raise RuntimeError('FIXTURE_NOT_ADMITTED')
        self.path = BASE/self._testMethodName
        self.path.mkdir(mode=0o700)
        s = self.path.lstat()
        self.identity = (s.st_dev, s.st_ino, s.st_uid)
        self.fd_before = set(os.listdir('/proc/self/fd'))
        self.rows = []
        self.backend = M.Metadata(M.Budget(), uid=os.geteuid())

    def tearDown(self):
        global DIRTY
        try:
            self.assertEqual(set(os.listdir('/proc/self/fd')), self.fd_before)
            remove_owned(self.path, self.identity)
        except BaseException:
            DIRTY = True
            raise

    def file(self, name='file', mode=0o600):
        path = self.path/name
        fd = os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, mode)
        try: os.write(fd, b'SYNTHETIC_BODY_MUST_NOT_BE_READ')
        finally: os.close(fd)
        return path

    def inspect(self, path, kind='REGULAR'):
        return self.backend.inspect(str(path), kind, self.rows.append)

    def expect(self, code, fn):
        with self.assertRaises(M.Refused) as result: fn()
        self.assertEqual(result.exception.code, code)

    def test_regular_metadata_no_body_open(self):
        p = self.file(); real_open = os.open; body_opens = []
        def checked(path, flags, *args, **kw):
            if str(path) == p.name and not flags & os.O_PATH: body_opens.append(True)
            return real_open(path, flags, *args, **kw)
        with patch.object(M.os, 'open', checked): row = self.inspect(p)
        self.assertEqual(body_opens, []); self.assertEqual(row['failures'], [])
        self.assertEqual(row['acl']['access'], 'UNRUN_FILE_METADATA_ONLY')

    def test_multiple_independent_mode_failures(self):
        a = self.file('group'); b = self.file('other'); self.file('good')
        os.chmod(a, 0o620); os.chmod(b, 0o602)
        self.backend.tree(str(self.path), self.rows.append)
        by = {r['path']:r for r in self.rows}
        self.assertIn('GROUP_WRITE', by[str(a)]['failures'])
        self.assertIn('OTHER_WRITE', by[str(b)]['failures'])
        self.assertEqual(by[str(self.path/'good')]['failures'], [])

    def test_unsafe_ancestor_blocks_deeper_open(self):
        sub = self.path/'unsafe'; sub.mkdir(); (sub/'child').write_bytes(b'synthetic')
        os.chmod(sub, 0o770); real_open = os.open; deeper = []
        def checked(path, *args, **kw):
            if str(path) == 'child': deeper.append(True)
            return real_open(path, *args, **kw)
        with patch.object(M.os, 'open', checked):
            self.expect('PREREQUISITE', lambda:self.inspect(sub/'child'))
        self.assertEqual(deeper, []); self.assertTrue(self.rows[-1]['descent_blocked'])

    def test_unexpected_symlink_not_followed(self):
        target = self.file('target'); link = self.path/'link'; link.symlink_to(target)
        self.expect('PREREQUISITE', lambda:self.inspect(link))
        self.assertIn('PATH_LINK', self.rows[-1]['failures'])
        self.assertEqual(self.rows[-1]['metadata']['kind'], 'SYMLINK')

    def test_held_symlink_read(self):
        link = self.path/'control'; link.symlink_to('synthetic-relative-target')
        self.assertEqual(self.backend.link(str(link), self.rows.append), 'synthetic-relative-target')

    def test_rename_after_open_detected(self):
        p = self.file()
        def changed():
            with self.backend.bound(str(p), 'REGULAR', self.rows.append):
                p.rename(self.path/'old'); self.file()
        self.expect('PATH_RACE', changed)

    def test_replacement_between_stat_and_open_detected(self):
        p = self.file(); real_open = os.open; hit = []
        def changed(path, flags, *args, **kw):
            if str(path) == p.name and flags & os.O_PATH and not hit:
                hit.append(True); p.rename(self.path/'old'); self.file()
            return real_open(path, flags, *args, **kw)
        with patch.object(M.os, 'open', changed):
            self.expect('PATH_RACE', lambda:self.inspect(p))
        self.assertEqual(hit, [True])

    def test_directory_acl_absence(self):
        row = self.inspect(self.path, 'DIRECTORY')
        self.assertEqual(row['acl'], {'access':'ABSENT', 'default':'ABSENT'})

    def test_directory_access_and_default_acl_presence(self):
        # Only our disposable directory. No setfacl on ancestors or user data.
        args = ['/usr/bin/setfacl', '-m', 'u:2001:r-x,m::r-x,d:u::rwx,d:u:2001:r-x,d:g::---,d:m::r-x,d:o::---', str(self.path)]
        completed = subprocess.run(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, timeout=2, env={'PATH':'/usr/bin:/bin','LC_ALL':'C'})
        self.assertEqual(completed.returncode, 0)
        completed = subprocess.run(['/usr/bin/getfacl','--omit-header','--numeric',str(self.path)],
            stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
            timeout=2,env={'PATH':'/usr/bin:/bin','LC_ALL':'C'})
        self.assertEqual(completed.returncode, 0)
        row = self.inspect(self.path, 'DIRECTORY')
        self.assertEqual(row['acl'], {'access':'PRESENT','default':'PRESENT'})
        self.assertEqual(row['failures'], [])

    def test_directory_acl_unknown_blocks(self):
        with patch.object(M.os, 'getxattr', side_effect=OSError(errno.EACCES,'synthetic')):
            self.expect('PREREQUISITE', lambda:self.inspect(self.path,'DIRECTORY'))
        self.assertIn('ACL_UNKNOWN', self.rows[-1]['failures'])

    def test_malformed_names_still_consume_budget(self):
        self.file('bad\nname1'); self.file('bad\nname2')
        # Bound() charges every ancestor; only one further entry permitted.
        budget = M.Budget(); self.backend = M.Metadata(budget, uid=os.geteuid())
        with self.backend.bound(str(self.path),'DIRECTORY',self.rows.append) as (fd,_):
            budget.limits['entries'] = budget.entries + 1
            self.expect('ENTRY_BOUND', lambda:self.backend._tree(fd,str(self.path),0,self.rows.append))
        self.assertTrue(any('PATH_SYNTAX' in r['failures'] for r in self.rows))
        self.assertFalse(any('\n' in r['path'] for r in self.rows))

    def test_hardlink_refused(self):
        p = self.file(); os.link(p,self.path/'linked')
        self.expect('PREREQUISITE',lambda:self.inspect(p))
        self.assertIn('MULTIPLE_LINKS',self.rows[-1]['failures'])

    def test_deadline_closes_held_descriptors(self):
        p = self.file()
        def expired():
            with self.backend.bound(str(p),'REGULAR',self.rows.append):
                self.backend.budget.deadline = 0
        self.expect('TIME_BOUND',expired)

    def test_cleanup_uncertainty_never_passes(self):
        p = self.file(); real_close = self.backend.close
        def uncertain(fd):
            real_close(fd); raise M.Refused('CLEANUP_UNKNOWN')
        with patch.object(self.backend,'close',uncertain):
            self.expect('CLEANUP_UNKNOWN',lambda:self.inspect(p))


NAMES = tuple(sorted(n for n in NativeCases.__dict__ if n.startswith('test_')))


class ScreenedResult(unittest.TestResult):
    def __init__(self):
        super().__init__(); self.rows = []
    def addSuccess(self, test):
        super().addSuccess(test); self.rows.append({'case':test._testMethodName,'status':'PASS'})
    def addFailure(self, test, err):
        super().addFailure(test,err); self.rows.append({'case':test._testMethodName,'status':'FAIL'})
    def addError(self, test, err):
        super().addError(test,err); self.rows.append({'case':test._testMethodName,'status':'ERROR'})
    def addSkip(self, test, reason):
        super().addSkip(test,reason); self.rows.append({'case':test._testMethodName,'status':'SKIP'})
    def stopTest(self, test):
        super().stopTest(test)
        if DIRTY: self.stop()


def run(base):
    global BASE
    BASE = base
    result = ScreenedResult()
    unittest.TestSuite(NativeCases(name) for name in NAMES).run(result)
    return {'protocol':'pr933.native.fixture.v1','cases':result.rows,'tests':result.testsRun,
        'failures':len(result.failures),'errors':len(result.errors),'skips':len(result.skipped),
        'cleanup':not DIRTY and list(base.iterdir()) == [],'native':True}
