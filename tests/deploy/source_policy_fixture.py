"""Explicit UNIT fixture boundary, never installed in production scripts.

Copies byte-identical shell callers. Models only inherited /tmp namespace;
filesystem tests use a labelled identity double for toy non-Git trees. Real Git
fixtures retain production identity checks and substitute only owned local fetch.
"""
import hashlib
import importlib.util
import json
import os
import shutil
import stat
import sys
from pathlib import Path

TOKEN='.task11-source-policy-unit.json'
SCRIPTS=('deploy_release_swap.sh','deploy_release_rollback.sh','deploy_clear_activation_marker.sh','deploy_pin_candidate_sha.sh','deploy_verify_release_tree.sh')

def owned_box(value):
    box=Path(value)
    assert box.is_absolute() and str(box).startswith('/tmp/tmp.') and all(x not in ('.','..') for x in str(value).split('/'))
    assert box.resolve()==box
    st=box.lstat();assert stat.S_ISDIR(st.st_mode) and st.st_uid==os.geteuid() and not st.st_mode&0o077
    return box

def install(source,box,mode):
    box=owned_box(box);assert mode in ('filesystem','git')
    repo=box/'source-policy-unit';scripts=repo/'scripts';scripts.mkdir(parents=True,mode=0o700)
    original=Path(source)/'scripts';copied={}
    for name in SCRIPTS:
        target=scripts/name;shutil.copyfile(original/name,target);copied[name]=hashlib.sha256(target.read_bytes()).hexdigest()
    real=scripts/'_production_source_policy.py';shutil.copyfile(original/'deploy_source_policy.py',real)
    adapter=scripts/'deploy_source_policy.py';shutil.copyfile(Path(__file__),adapter)
    record={'kind':'EXPLICIT_UNIT_NAMESPACE_NOT_NATIVE_HOST_PROOF','mode':mode,'real_sha256':hashlib.sha256(real.read_bytes()).hexdigest(),'adapter_sha256':hashlib.sha256(adapter.read_bytes()).hexdigest(),'shell_postimage_sha256':copied}
    with (box/TOKEN).open('x') as f:json.dump(record,f)
    binpath=box/'source-policy-unit-bin';binpath.mkdir(mode=0o700)
    sudo=binpath/'sudo';sudo.write_text('#!/bin/sh\n# Never escalate in an offline unit fixture. Caller may use its own readlink fallback.\nexit 97\n');sudo.chmod(0o700)
    return repo

def run(args):
    box=owned_box(os.environ['TASK11_POLICY_FIXTURE_ROOT']);record=json.loads((box/TOKEN).read_text());assert record['kind']=='EXPLICIT_UNIT_NAMESPACE_NOT_NATIVE_HOST_PROOF'
    here=Path(__file__).parent;real=here/'_production_source_policy.py'
    assert hashlib.sha256(real.read_bytes()).hexdigest()==record['real_sha256'];assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==record['adapter_sha256']
    spec=importlib.util.spec_from_file_location('production_policy',real);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    original_directory=m.directory;original_identity=m.identity;original_candidate=m.candidate;original_pin=m.pin;original_git=m._git;local_origin=None
    def directory(path):
        path=m.lexical(path)
        if path!=box and box not in path.parents:raise m.Refused('UNIT_NAMESPACE_ESCAPE')
        cursor=box;m.trusted(cursor.lstat(),stat.S_ISDIR)
        for part in path.relative_to(box).parts:
            cursor=cursor/part;m.trusted(cursor.lstat(),stat.S_ISDIR)
        return path
    m.directory=directory
    def identity(root,expected=None,expected_tree=None):
        root=directory(root)
        if os.path.lexists(root/'.git'):return original_identity(root,expected,expected_tree)
        if record['mode']!='filesystem' or expected is not None or expected_tree is not None:raise m.Refused('UNIT_NO_GIT_EXPECTATION')
        return {'root':str(root),'head':'0'*40,'tree':'0'*40,'kind':'FILESYSTEM_UNIT_IDENTITY_DOUBLE_NOT_GIT_PROOF'}
    m.identity=identity
    def candidate(live,path):
        root=m.release(live,path)
        if record['mode']=='filesystem' and not os.path.lexists(root/'.git'):return identity(root)
        return original_candidate(live,path)
    m.candidate=candidate
    def unit_git(root,argv,deadline,cap=8192,network=False,command_timeout=3,home=None):
        if argv and argv[0]=='fetch':
            if local_origin is None or not network:raise m.Refused('UNIT_FETCH_SCOPE')
            directory(local_origin)
            command=['/usr/bin/git','-c','credential.helper=','-c','core.fsmonitor=false','-c','core.hooksPath=/dev/null','-c','protocol.allow=never','-c','protocol.file.allow=always','-C',str(root),'fetch','--no-tags','--no-recurse-submodules',str(local_origin),argv[-1]]
            env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','HOME':str(home),'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_TERMINAL_PROMPT':'0','GIT_ALLOW_PROTOCOL':'file'}
            code,out,err=m._capture(command,env,budget=min(command_timeout,m._budget(deadline)),output_cap=cap)
            if code:raise m.Refused('UNIT_LOCAL_FETCH_FAILED')
            return out.decode().strip()
        return original_git(root,argv,deadline,cap,network,command_timeout,home)
    m._git=unit_git
    def pin(root,url,expected):
        nonlocal local_origin
        value=url[7:] if url.startswith('file://') else url
        local_origin=directory(value)
        return original_pin(root,'https://github.com/3pacs/GRID.git',expected)
    m.pin=pin
    # Preserve fixed production refusals and diagnostics; only declared unit
    # namespace / identity / local transport seams above differ.
    return m.main(args)

if __name__=='__main__':
    try:
        if len(sys.argv)==5 and sys.argv[1]=='install':print(install(*sys.argv[2:]));raise SystemExit(0)
        raise SystemExit(run(sys.argv[1:]))
    except SystemExit:raise
    except BaseException:
        print('UNIT_FIXTURE_REFUSED',file=sys.stderr);raise SystemExit(95)
