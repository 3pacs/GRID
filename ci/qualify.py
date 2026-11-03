"""One proposed hosted synthetic qualification; no production-profile execution."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import stat
import sys

BRANCH = 'codex/task11-pr933-native-preflight-20261103-n1'
COLLECTOR_SHA = '8d0de6b38c5acf47ad2b83598c27ba30f94d3475283af01045c06c092088398f'
CASE_SHA = '1f54b56848af809dd0174d687ded2148b3bd4353cf36df0c9b7a40c712bab566'
CASE_COUNT = 14


class Refused(Exception): pass


def decode(raw):
    if type(raw) is not bytes or len(raw)>16384: raise Refused('RESULT_BOUND')
    def pairs(items):
        d = {}
        for k,v in items:
            if k in d: raise Refused('DUPLICATE_KEY')
            d[k] = v
        return d
    try: return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(Refused('JSON_CONSTANT')))
    except (ValueError,UnicodeError): raise Refused('JSON_SHAPE') from None


def screen_result(record,names):
    if type(record) is not dict or set(record)!={'protocol','cases','tests','failures','errors','skips','cleanup','native'}:
        raise Refused('RESULT_SHAPE')
    if record['protocol']!='pr933.native.fixture.v1' or record['native'] is not True or record['cleanup'] is not True:
        raise Refused('RESULT_NOT_QUALIFIED')
    if any(type(record[k]) is not int for k in ('tests','failures','errors','skips')):
        raise Refused('RESULT_TYPE')
    if record['tests']!=CASE_COUNT or any(record[k]!=0 for k in ('failures','errors','skips')):
        raise Refused('RESULT_NOT_QUALIFIED')
    if len(names)!=CASE_COUNT or record['cases']!=[{'case':n,'status':'PASS'} for n in names]:
        raise Refused('CASE_SET')
    return True


def mapped(raw,uid):
    if type(raw) is not str or len(raw)>4096: raise Refused('UID_MAP')
    rows=[]
    for line in raw.splitlines():
        if not re.fullmatch(r'\s*[0-9]+\s+[0-9]+\s+[1-9][0-9]*\s*',line): raise Refused('UID_MAP')
        start,outside,count=map(int,line.split());rows.append((start,count))
    if not rows: raise Refused('UID_MAP')
    return any(start<=uid<start+count for start,count in rows)


def event_allowed(event,env):
    return (env.get('GITHUB_REPOSITORY')=='3pacs/GRID' and env.get('GITHUB_EVENT_NAME')=='push'
        and env.get('GITHUB_REF')=='refs/heads/'+BRANCH and env.get('GITHUB_RUN_ATTEMPT')=='1'
        and bool(re.fullmatch('[0-9a-f]{40}',env.get('GITHUB_SHA','')))
        and type(event) is dict and event.get('created') is True and event.get('deleted') is False
        and event.get('forced') is False and event.get('before')=='0'*40
        and event.get('after')==env.get('GITHUB_SHA') and event.get('ref')==env.get('GITHUB_REF'))


def load(path,name,digest):
    if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
        raise Refused('SOURCE_HASH')
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def main():
    owned=None; identity=None; complete=False
    try:
        if len(sys.argv)!=1 or not sys.flags.isolated or not sys.dont_write_bytecode: raise Refused('STARTUP')
        if platform.system()!='Linux' or sys.version_info[:2]!=(3,10): raise Refused('PYTHON_PLATFORM')
        if platform.freedesktop_os_release().get('VERSION_ID')!='22.04' or platform.freedesktop_os_release().get('ID')!='ubuntu':
            raise Refused('OS_SCOPE')
        if os.environ.get('QUALIFICATION_RUNNER')!='github-hosted' or os.environ.get('RUNNER_ARCH')!='X64': raise Refused('HOSTED_SCOPE')
        uid=os.geteuid()
        if uid in (0,65534): raise Refused('RUNNER_UID')
        raw=Path('/proc/self/uid_map').read_text()
        if not all(mapped(raw,u) for u in (0,uid,2001)): raise Refused('UID_UNMAPPED')
        for n in ('setfacl','getfacl'):
            p=Path('/usr/bin')/n;s=p.lstat()
            if not stat.S_ISREG(s.st_mode) or s.st_uid!=0 or s.st_mode&0o022 or not s.st_mode&0o111: raise Refused('ACL_TOOL')
        event_path=Path(os.environ['GITHUB_EVENT_PATH'])
        with event_path.open('rb') as stream: event=decode(stream.read(16385))
        if not event_allowed(event,os.environ): raise Refused('EVENT_CONTEXT')
        run_id=os.environ.get('GITHUB_RUN_ID','')
        if not re.fullmatch('[1-9][0-9]{0,19}',run_id): raise Refused('RUN_ID')
        root=Path('/home/runner/task11-pr933-checkout-'+run_id+'-1')
        if Path(__file__).absolute().parent!=root/'ci': raise Refused('CHECKOUT_LOCATION')
        m=load(root/'ci/metadata_preflight.py','native_backend',COLLECTOR_SHA)
        # Unchanged descriptor/predicate implementation; parameter is actual fixture UID.
        backend=m.Metadata(m.Budget(),uid=uid)
        for p in ('/home','/home/runner',str(root),str(root/'ci')):
            r=backend.inspect(p,'DIRECTORY',lambda _:None)
            if r['acl']!={'access':'ABSENT','default':'ABSENT'}: raise Refused('ANCESTOR_ACL')
        cases=load(root/'ci/native_cases.py','native_cases',CASE_SHA)
        owned=root/'fixtures'; owned.mkdir(mode=0o700)
        st=owned.lstat();identity=(st.st_dev,st.st_ino,st.st_uid)
        violations=[]
        def audit(event,args):
            if event.startswith('socket.') or event in ('os.system','os.exec','os.posix_spawn','os.chown'):
                violations.append(event);raise RuntimeError('NATIVE_BOUNDARY')
            if event=='subprocess.Popen':
                argv=args[1]
                if (not isinstance(argv,list) or argv[0] not in ('/usr/bin/setfacl','/usr/bin/getfacl')
                        or not str(argv[-1]).startswith(str(owned)+'/test_')):
                    violations.append(event);raise RuntimeError('NATIVE_BOUNDARY')
            if event in ('os.chmod','os.setxattr','os.removexattr'):
                if not isinstance(args[0],(str,bytes,os.PathLike)) or not str(args[0]).startswith(str(owned)+'/test_'):
                    violations.append(event);raise RuntimeError('FIXTURE_MUTATION_BOUNDARY')
        sys.addaudithook(audit)
        record=cases.run(owned)
        if violations: raise Refused('AUDIT_BOUNDARY')
        screen_result(record,cases.NAMES)
        cases.remove_owned(owned,identity);owned=None
        facts={'protocol':'pr933.native.hosted.v1','head':os.environ['GITHUB_SHA'],
            'python':platform.python_version(),'fixture_uid':uid,'cases':record,
            'image_scope':'rolling-ubuntu-22.04-platform-setup-header-required',
            'collector_sha256':COLLECTOR_SHA,'native_case_sha256':CASE_SHA,
            'fixed_production_profile_run':False,'fixture_cleanup':True}
        raw=(json.dumps(facts,sort_keys=True)+'\n').encode()
        if len(raw)>16384: raise Refused('OUTPUT_BOUND')
        sys.stdout.buffer.write(raw);sys.stdout.buffer.flush();complete=True
        return 0
    except BaseException as exc:
        code=str(exc) if isinstance(exc,Refused) else 'NATIVE_REFUSED'
        print(json.dumps({'protocol':'pr933.native.refusal.v1','code':code,'qualified':False}),flush=True)
        return 2
    finally:
        # On failure retain fixture for the workflow's independently owned cleanup.
        # Never claim it was cleaned here or run another test after cleanup uncertainty.
        if not complete and owned is not None:
            pass


if __name__=='__main__': raise SystemExit(main())
