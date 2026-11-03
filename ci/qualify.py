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

BRANCH = 'codex/task11-pr933-native-preflight-20261103-n2'
COLLECTOR_SHA = '8d0de6b38c5acf47ad2b83598c27ba30f94d3475283af01045c06c092088398f'
CASE_SHA = '1f54b56848af809dd0174d687ded2148b3bd4353cf36df0c9b7a40c712bab566'
CASE_COUNT = 14


class Refused(Exception): pass


DIRECTORY_ROLES = ('HOME', 'RUNNER_HOME', 'CHECKOUT', 'SOURCE')
PREREQUISITE_CODES = frozenset(('NONE', 'ANCESTOR_ACL', 'PREREQUISITE',
    'PATH_SYNTAX', 'PATH_LINK', 'PATH_TYPE', 'PATH_OWNER', 'GROUP_WRITE',
    'OTHER_WRITE', 'SPECIAL_MODE', 'ACL_UNKNOWN', 'PATH_RACE', 'IO_ABSENT',
    'IO_INACCESSIBLE', 'IO_OTHER', 'DEPTH_BOUND', 'ENTRY_BOUND', 'FINDING_BOUND',
    'TIME_BOUND', 'CLEANUP_UNKNOWN', 'PREREQUISITE_SHAPE', 'PREREQUISITE_ERROR'))


def prerequisite_report(backend, module, root):
    """Observe four fixed roles; extra ACL presence never enables fixtures.

    Well-shaped ACL presence is permitted by the unchanged descriptor walker,
    so it is safe to diagnose the remaining roles before enforcing absence.
    A descriptor failure blocks all later roles: these paths form one chain.
    No observed ACL snapshot is promoted after a race or uncertain cleanup.
    """
    if not re.fullmatch(r'/home/runner/task11-pr933-checkout-[1-9][0-9]{0,19}-1',str(root)):
        raise Refused('CHECKOUT_LOCATION')
    paths=('/home','/home/runner',str(root),str(root/'ci'))
    rows=[]; blocker=None
    for role,path in zip(DIRECTORY_ROLES,paths):
        row={'role':role,'phase':'NOT_ATTEMPTED','status':'BLOCKED',
             'code':blocker['code'] if blocker else 'PREREQUISITE',
             'access_acl':'UNRUN','default_acl':'UNRUN',
             'blocked_by':[blocker['role']] if blocker else [],'causes':[]}
        if blocker is not None:
            rows.append(row);continue
        observed=[]
        try:
            value=backend.inspect(path,'DIRECTORY',observed.append)
            if (type(value) is not dict or value.get('path')!=path or
                    value.get('failures')!=[] or value.get('descent_blocked') is not False or
                    type(value.get('acl')) is not dict or set(value['acl'])!={'access','default'} or
                    any(v not in ('ABSENT','PRESENT') for v in value['acl'].values())):
                raise Refused('PREREQUISITE_SHAPE')
            a,d=value['acl']['access'],value['acl']['default']
            passed=a==d=='ABSENT'
            row.update(phase='FINAL_VERIFIED',status='PASS' if passed else 'FAIL',
                code='NONE' if passed else 'ANCESTOR_ACL',access_acl=a,default_acl=d)
        except BaseException as exc:
            if isinstance(exc,module.Refused): code=exc.code
            elif isinstance(exc,OSError): code=module.io_code(exc)
            elif isinstance(exc,Refused): code=str(exc)
            else: code='PREREQUISITE_ERROR'
            if code not in PREREQUISITE_CODES or code in ('NONE','ANCESTOR_ACL'):
                code='PREREQUISITE_ERROR'
            # Only closed error categories; no paths, ACL subjects, exception text
            # or raw metadata leave this boundary. A failed walk is not stable.
            final_seen=any(type(v) is dict and v.get('path')==path for v in observed)
            causes=sorted({c for v in observed if type(v) is dict
                for c in (v.get('failures') if type(v.get('failures')) is list else [])
                if type(c) is str and c in PREREQUISITE_CODES-{'NONE','ANCESTOR_ACL'}})
            row.update(phase='FINAL_UNVERIFIED' if final_seen else 'NOT_OBSERVED',
                status='FAIL' if final_seen else 'BLOCKED',code=code,
                access_acl='UNKNOWN' if final_seen else 'UNRUN',
                default_acl='UNKNOWN' if final_seen else 'UNRUN',causes=causes)
            blocker={'role':role,'code':code}
        rows.append(row)
    report={'protocol':'pr933.native.prerequisites.v1','phase':'BEFORE_FIXTURES',
        'directories':rows,'fixtures_allowed':all(r['status']=='PASS' for r in rows),
        'snapshot_atomic':False,'unsafe_descendants_read':False}
    screen_prerequisites(report)
    return report


def screen_prerequisites(record):
    """Exact four-role screen, closed provenance/status graph, max4096 bytes."""
    keys={'protocol','phase','directories','fixtures_allowed','snapshot_atomic','unsafe_descendants_read'}
    if type(record) is not dict or set(record)!=keys or record['protocol']!='pr933.native.prerequisites.v1' or record['phase']!='BEFORE_FIXTURES':
        raise Refused('PREREQUISITE_SHAPE')
    if (record['snapshot_atomic'] is not False or record['unsafe_descendants_read'] is not False
            or type(record['fixtures_allowed']) is not bool or type(record['directories']) is not list
            or len(record['directories'])!=4):raise Refused('PREREQUISITE_SHAPE')
    blocker=None
    for role,row in zip(DIRECTORY_ROLES,record['directories']):
        if type(row) is not dict or set(row)!={'role','phase','status','code','access_acl','default_acl','blocked_by','causes'}:
            raise Refused('PREREQUISITE_SHAPE')
        if (row['role']!=role or type(row['code']) is not str or row['code'] not in PREREQUISITE_CODES
                or type(row['phase']) is not str or type(row['causes']) is not list):
            raise Refused('PREREQUISITE_SHAPE')
        if (any(type(c) is not str or c not in PREREQUISITE_CODES-{'NONE','ANCESTOR_ACL'} for c in row['causes'])
                or row['causes']!=sorted(set(row['causes']))):raise Refused('PREREQUISITE_SHAPE')
        if blocker:
            if (row['phase'],row['status'],row['code'],row['access_acl'],row['default_acl'],row['blocked_by'],row['causes']) != (
                    'NOT_ATTEMPTED','BLOCKED',blocker['code'],'UNRUN','UNRUN',[blocker['role']],[]):
                raise Refused('PREREQUISITE_SHAPE')
            continue
        if row['blocked_by']!=[]:raise Refused('PREREQUISITE_SHAPE')
        if row['phase']=='FINAL_VERIFIED':
            if row['access_acl'] not in ('ABSENT','PRESENT') or row['default_acl'] not in ('ABSENT','PRESENT') or row['causes']:
                raise Refused('PREREQUISITE_SHAPE')
            expected=('PASS','NONE') if row['access_acl']==row['default_acl']=='ABSENT' else ('FAIL','ANCESTOR_ACL')
            if (row['status'],row['code'])!=expected:raise Refused('PREREQUISITE_SHAPE')
        else:
            expected={'FINAL_UNVERIFIED':('FAIL','UNKNOWN'),'NOT_OBSERVED':('BLOCKED','UNRUN')}.get(row['phase'])
            if (expected is None or (row['status'],row['access_acl'])!=expected
                    or row['default_acl']!=expected[1] or row['code'] in ('NONE','ANCESTOR_ACL')):
                raise Refused('PREREQUISITE_SHAPE')
            blocker={'role':role,'code':row['code']}
    if record['fixtures_allowed']!=all(r['status']=='PASS' for r in record['directories']):
        raise Refused('PREREQUISITE_SHAPE')
    raw=(json.dumps(record,sort_keys=True,allow_nan=False,separators=(',',':'))+'\n').encode('ascii')
    if len(raw)>4096:raise Refused('RESULT_BOUND')
    return raw


def require_prerequisites(backend,module,root,emit):
    record=prerequisite_report(backend,module,root)
    emit(record)
    if not record['fixtures_allowed']:
        if any(r['phase']!='FINAL_VERIFIED' for r in record['directories']):
            raise Refused('ANCESTOR_PREREQUISITE')
        raise Refused('ANCESTOR_ACL')
    return record


def emit_record(record,state):
    raw=(json.dumps(record,sort_keys=True,allow_nan=False)+'\n').encode('ascii')
    if state['bytes']+len(raw)>16384:raise Refused('OUTPUT_BOUND')
    state['bytes']+=len(raw)
    sys.stdout.buffer.write(raw);sys.stdout.buffer.flush()


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
    owned=None; identity=None; complete=False; output_state={'bytes':0}
    emit=lambda record:emit_record(record,output_state)
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
        require_prerequisites(backend,m,root,emit)
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
        emit(facts);complete=True
        return 0
    except BaseException as exc:
        code=str(exc) if isinstance(exc,Refused) else 'NATIVE_REFUSED'
        try:emit({'protocol':'pr933.native.refusal.v1','code':code,'qualified':False})
        except BaseException:pass  # A full/broken output sink cannot manufacture success.
        return 2
    finally:
        # On failure retain fixture for the workflow's independently owned cleanup.
        # Never claim it was cleaned here or run another test after cleanup uncertainty.
        if not complete and owned is not None:
            pass


if __name__=='__main__': raise SystemExit(main())
