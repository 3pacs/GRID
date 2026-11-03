"""Current #933 caller policy controls in an explicit owned UNIT namespace.

Shell callers are byte-identical; the existing fixture models only toy non-Git
identity and inherited /tmp ancestry. This does not qualify installed payloads.
"""
import importlib.util
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('unit_fixture', Path(__file__).with_name('source_policy_fixture.py'))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class CallerPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='tmp.', dir='/tmp')
        self.box = Path(self.temp.name)
        self.repo = fixture.install(ROOT, self.box, 'filesystem')
        self.live = self.box / 'live'
        self.releases = self.box / 'live.releases'
        self.releases.mkdir(mode=0o700)
        self.old = self.releases / 'old'
        self.old.mkdir(mode=0o700)
        (self.old / 'original').write_text('synthetic-original')
        self.live.symlink_to(self.old)
        self.env = dict(PATH=str(self.box / 'source-policy-unit-bin') + ':/usr/bin:/bin',
                        HOME=str(self.box), LANG='C', LC_ALL='C',
                        TASK11_POLICY_FIXTURE_ROOT=str(self.box), GRID_DEPLOY_TEST_SANDBOX=str(self.box),
                        GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null',
                        GIT_ALLOW_PROTOCOL='file', GIT_TERMINAL_PROMPT='0', DEPLOY_LOCK_WAIT_SECS='1')

    def tearDown(self):
        self.temp.cleanup()

    def swap(self, body='printf built > "$1/built"', mask=0o077):
        hook = self.box / 'hook'
        hook.write_text('#!/bin/bash\nset -euo pipefail\n' + body + '\n')
        hook.chmod(0o700)
        return subprocess.run(['/bin/bash', str(self.repo / 'scripts/deploy_release_swap.sh'),
                               str(self.live), 'new', str(hook)], env=self.env,
                              capture_output=True, text=True, timeout=10, umask=mask)

    def test_writable_legacy_root_refused_before_hook_without_normalization(self):
        self.old.chmod(0o775)
        p = self.swap()
        self.assertEqual(p.returncode, 5, p.stderr)
        self.assertEqual(self.live.resolve(), self.old)
        self.assertEqual(stat.S_IMODE(self.old.stat().st_mode), 0o775)
        self.assertFalse((self.releases / 'new/built').exists())
        self.assertIn('PATH_WRITABLE', p.stderr)

    def test_private_source_mode_and_mutable_runtime_preserved(self):
        runtime = self.old / 'runtime'
        runtime.mkdir(mode=0o700)
        runtime.chmod(0o777)
        (runtime / 'data').write_text('synthetic-state')
        p = self.swap()
        self.assertEqual(p.returncode, 0, p.stderr)
        new = self.releases / 'new'
        self.assertEqual(stat.S_IMODE(new.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((new / 'runtime').stat().st_mode), 0o777)
        self.assertEqual((new / 'runtime/data').read_text(), 'synthetic-state')

    def test_empty_candidate_under_permissive_umask_is_protected(self):
        self.live.unlink()
        p = self.swap(mask=0o002)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertFalse((self.releases / 'new').stat().st_mode & 0o022)
        self.assertEqual(self.live.resolve(), self.releases / 'new')

    def test_hook_introduced_writable_root_refused_before_promotion(self):
        p = self.swap('chmod 0775 "$1"; printf built > "$1/built"')
        self.assertEqual(p.returncode, 4, p.stderr)
        self.assertEqual(self.live.resolve(), self.old)
        self.assertFalse((self.releases / '.activation-in-progress').exists())
        self.assertEqual((self.old / 'original').read_text(), 'synthetic-original')

    def test_unsafe_rollback_target_refused_without_repair(self):
        p = self.swap()
        self.assertEqual(p.returncode, 0, p.stderr)
        (self.releases / '.activation-in-progress').unlink()
        self.old.chmod(0o775)
        p = subprocess.run(['/bin/bash', str(self.repo / 'scripts/deploy_release_rollback.sh'),
                            str(self.live), '--schema-compatible'], env=self.env,
                           capture_output=True, text=True, timeout=10)
        self.assertEqual(p.returncode, 5, p.stderr)
        self.assertEqual(self.live.resolve(), self.releases / 'new')
        self.assertEqual(stat.S_IMODE(self.old.stat().st_mode), 0o775)


if __name__ == '__main__':
    unittest.main(verbosity=2)
