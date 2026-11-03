"""Import and test-scope isolation controls for the metadata test's audit fence."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest


def load_tests_module():
    path = Path(__file__).with_name('test_deploy_metadata_preflight.py')
    spec = importlib.util.spec_from_file_location('metadata_scope_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ImportIsolationTests(unittest.TestCase):
    def test_import_preserves_unrelated_inert_subprocess(self):
        module = load_tests_module()
        self.assertFalse(module.BOUNDARY_ACTIVE.get())
        subprocess.run(['/usr/bin/true'], check=True, timeout=2)
        self.assertEqual(module.EVENTS, [])

    def test_all_protected_event_categories_still_refused_in_own_scope(self):
        module = load_tests_module()
        token = module.BOUNDARY_ACTIVE.set(True)
        try:
            for event in ('socket.connect', 'subprocess.Popen', 'os.system', 'os.exec',
                          'os.posix_spawn', 'os.chmod', 'os.chown', 'os.setxattr', 'os.kill', 'os.killpg'):
                with self.subTest(event=event), self.assertRaisesRegex(AssertionError, 'OFFLINE_BOUNDARY'):
                    module.audit(event, ())
        finally:
            module.BOUNDARY_ACTIVE.reset(token)
        self.assertFalse(module.BOUNDARY_ACTIVE.get())
        self.assertEqual(len(module.EVENTS), 10)

    def test_successful_case_restores_context_and_unrelated_execution(self):
        module = load_tests_module()
        result = unittest.TestResult()
        module.Tests('test_many_independent_failures_one_pass').run(result)
        self.assertTrue(result.wasSuccessful(), result.errors + result.failures)
        self.assertFalse(module.BOUNDARY_ACTIVE.get())
        subprocess.run(['/usr/bin/true'], check=True, timeout=2)
        self.assertEqual(module.EVENTS, [])

    def test_failed_case_also_restores_context(self):
        module = load_tests_module()

        class FailedFixture(module.Tests):
            def test_invented_failure(self):
                self.assertTrue(module.BOUNDARY_ACTIVE.get())
                raise AssertionError('intentional synthetic failure')

        result = unittest.TestResult()
        FailedFixture('test_invented_failure').run(result)
        self.assertEqual(len(result.failures), 1)
        self.assertFalse(module.BOUNDARY_ACTIVE.get())
        self.assertEqual(module.EVENTS, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
