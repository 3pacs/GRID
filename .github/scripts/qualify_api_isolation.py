"""Hosted synthetic qualification of the exact reviewed source capsule only."""
import argparse
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import sys
import tempfile
import unittest


class ReceiptResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.outcomes = []
    def addSuccess(self, test):
        super().addSuccess(test)
        self.outcomes.append({'id': test.id(), 'outcome': 'passed'})
    def addError(self, test, err):
        super().addError(test, err)
        self.outcomes.append({'id': test.id(), 'outcome': 'error'})
    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.outcomes.append({'id': test.id(), 'outcome': 'failed'})
    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.outcomes.append({'id': test.id(), 'outcome': 'skipped'})


def validate(result, expected):
    ids = [r['id'] for r in result.outcomes]
    return (result.testsRun == len(expected) and len(ids) == len(set(ids))
            and set(ids) == set(expected) and all(r['outcome'] == 'passed' for r in result.outcomes)
            and result.wasSuccessful() and not result.skipped
            and not result.expectedFailures and not result.unexpectedSuccesses)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=['pure', 'native'], required=True)
    args = ap.parse_args()
    if sys.platform != 'linux' or sys.version_info[:2] not in [(3, 10), (3, 11)]:
        raise SystemExit('Unsupported qualification runtime')
    repo = Path(__file__).resolve().parents[2]
    selection = json.loads((repo / '.github/qualification/api-isolation-selection.json').read_text())
    expected = selection['methods'][args.mode]
    if len(expected) != {'pure':89, 'native':9}[args.mode]:
        raise SystemExit('Unexpected method inventory')
    scratch = Path(os.environ['API_ISOLATION_SCRATCH'])
    if not scratch.is_absolute() or not scratch.is_dir():
        raise SystemExit('Owned scratch must be an existing absolute directory')
    oldcwd = Path.cwd()
    receipt = {'mode':args.mode, 'python':sys.version, 'platform':platform.platform(),
               'source_tree':selection['source_tree'], 'source_commit':selection['source_commit'],
               'receiving_sha':os.environ['GITHUB_SHA'], 'expected':len(expected),
               'qualified':False, 'cleanup':False}
    receipt['selection_sha256'] = hashlib.sha256((repo / '.github/qualification/api-isolation-selection.json').read_bytes()).hexdigest()
    receipt['qualification_runner_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    temp = tempfile.TemporaryDirectory(prefix='grid-api-qualification-owned-', dir=scratch)
    owned = Path(temp.name)
    try:
        capsule = owned / 'capsule'; capsule.mkdir()
        fixtures = owned / 'fixtures'; fixtures.mkdir()
        receipt['filesystem'] = {'device':owned.stat().st_dev, 'statvfs_block_size':os.statvfs(owned).f_bsize}
        for row in selection['files']:
            path = Path(row['path'])
            if path.is_absolute() or '..' in path.parts:
                raise ValueError('Invalid capsule path')
            source = repo / path
            data = source.read_bytes()
            if source.is_symlink() or hashlib.sha256(data).hexdigest() != row['sha256']:
                raise ValueError('Source pin mismatch: '+row['path'])
            dest = capsule / path; dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data); dest.chmod(int(row['mode'][-3:],8))
        tempfile.tempdir = str(fixtures)
        os.environ['TMPDIR'] = str(fixtures)
        sys.path[:0] = [str(capsule/'tests'),str(capsule/'scripts')]
        os.chdir(capsule)
        suite = unittest.defaultTestLoader.loadTestsFromNames(expected)
        if suite.countTestCases() != len(expected):
            raise ValueError('Actual collection count mismatch')
        result = unittest.TextTestRunner(verbosity=2, resultclass=ReceiptResult).run(suite)
        receipt['cases'] = result.outcomes
        receipt['tests_run'] = result.testsRun
        receipt['skipped'] = len(result.skipped)
        receipt['errors'] = len(result.errors)
        receipt['failures'] = len(result.failures)
        receipt['fixture_roots_removed'] = not any(fixtures.iterdir())
        receipt['own_children_alive'] = len(multiprocessing.active_children())
        receipt['qualified'] = validate(result, expected) and receipt['fixture_roots_removed'] and receipt['own_children_alive']==0
    finally:
        os.chdir(oldcwd)
        try:
            temp.cleanup()
            receipt['cleanup'] = not owned.exists()
        finally:
            output = repo / ('api-isolation-'+args.mode+'.json')
            output.write_text(json.dumps(receipt,indent=2)+'\n')
    if not receipt['qualified'] or not receipt['cleanup']:
        raise SystemExit('Qualification/count/cleanup failed; see bounded receipt')


if __name__ == '__main__':
    main()
