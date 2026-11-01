"""Exact two-case disposable hosted ACL qualifier; no app/DB/provider imports."""
import base64,hashlib,importlib.util,json,os,platform,re,selectors,signal,stat,subprocess,sys,time,unittest
from pathlib import Path
PARENT='77b1e50addcc3e1de969be88c4f7896280b4fc8a'
BRANCH='codex/task11-acl-rolling-20261101-a2'
IMAGE_SCOPE='rolling-ubuntu-22.04-platform-header-required'
NAMES=('test_copied_explicit_source_acl_preserved','test_fresh_default_acl_safe_or_prebuild_refusal')
IDS=tuple('RootModeTests.'+n for n in NAMES)
EXPECTED_SOURCE={'scripts/deploy_release_swap.sh': '4806bf3b3ebbfdf9aa37be35b80589b307d827e81d93c8fd579fa36de4716ad0', 'scripts/deploy_release_rollback.sh': '1c58de5e5f42c7183f4e4154733bfbf2efe2ab1614942fe0029ce1184a506850', 'tests/deploy/test_deploy_release_root_modes.py': '0f9f4a224e5f37a8c1b8461f346133629a8b167d2e05df98be3f8dff07535b18'}
EXPECTED_WORKFLOWS={'.github/workflows/task11-acl-only.yml': '22ecffc4e6c028d24c18db3165c9a4d488e4e579c3c3f73df6cb123da6b5e46a','.github/workflows/claude-code-review.yml': 'd414367e6e5ab016f8c1a66633a63efbf45ca2a2accca2b526bbbdfac678b7cb', '.github/workflows/claude.yml': '8a384c6695a4b0e9f572a2518e411cb99f7907de7c0601a6e89c74fa4eccd301', '.github/workflows/dad-smoke.yml': '8a8e604f8360a6ed217d4645ec4a1acf51c85ff6eaae439b3419e9c1c00740ba', '.github/workflows/deploy.yml': 'a1460cf95694a9c7ff9ea4af903ee6064ecdab0089df5a95f96a40404085dca2', '.github/workflows/evals-freeze.yml': '20a1a5cd825e42388188035d2ffd84b80f049c20c158763dd05a760048d17cc7', '.github/workflows/gemini-task.yml': 'f958e99c53adf7e04a81470a3a1da79bd026e98ac752671a4174678ddb87ee0d', '.github/workflows/oct25-native-compatibility.yml': '731882dbdbb5e6ac0965538c485a476ff6a5719a7f9dd83ac25d84afa31be34a', '.github/workflows/ops-bringup.yml': '25f5fea92a8a36b69b6f0130db46dc36295d5f4be07fa6cba20cc0b4f9d07c0e', '.github/workflows/ops-exec.yml': '3e074c7240188f9a19ac4bc82e9a39defec37cda6044a60aa9f79dd7f007958a', '.github/workflows/python-floor-qualification.yml': 'd0d37e95eeb5e56029f4b9db04716d886bd2ed9aa6be786e89821acb50f797c1', '.github/workflows/quiver-synthetic-pg.yml': '2d62583afc4fb5a2060e1fddffe3801cb1903ab6bd5d4bc766307649151ce198', '.github/workflows/test.yml': '61096e7f3f97d01fa575595bc24423222de95137aca8379bbd3062b2ca86ad71'}
class Refused(Exception):pass

def mapped(text,uid=2001):
 rows=[]
 for line in text.splitlines():
  parts=line.split()
  if len(parts)!=3 or any(not re.fullmatch('[0-9]+',p) for p in parts):raise Refused('UID_MAP_SHAPE')
  a,b,n=map(int,parts)
  if n<=0:raise Refused('UID_MAP_SHAPE')
  rows.append((a,b,n))
 if not rows:raise Refused('UID_MAP_SHAPE')
 return any(a<=uid<a+n for a,b,n in rows)

def decode_result(raw):
 def unique(pairs):
  d={}
  for k,v in pairs:
   if k in d:raise Refused('DUPLICATE_RESULT_KEYS')
   d[k]=v
  return d
 try:return json.loads(raw,object_pairs_hook=unique)
 except Refused:raise
 except (ValueError,UnicodeError):raise Refused('INVALID_RESULT_JSON') from None

def judge(record,code,raw):
 keys={'protocol','ids','tests','failures','errors','skips','cleanup','success'}
 if type(record) is not dict or set(record)!=keys or record['protocol']!='task11.acl.result.v1':raise Refused('RESULT_SHAPE')
 if record['ids']!=list(IDS) or any(type(record[k]) is not int for k in ('tests','failures','errors','skips')):raise Refused('RESULT_TYPES_OR_IDS')
 if code!=0 or record['tests']!=2 or any(record[k]!=0 for k in ('failures','errors','skips')) or record['cleanup'] is not True or record['success'] is not True:raise Refused('NOT_TWO_PASSES')
 if type(raw) is not bytes or len(raw)>32768:raise Refused('OUTPUT_BOUND')
 text=raw.decode('utf-8',errors='strict')
 lines=[re.escape(name)+r' \(pinned_acl_contract.RootModeTests(?:\.'+re.escape(name)+r')?\) \.\.\. ok\n' for name in NAMES]
 grammar=''.join(lines)+r'\n-{70}\nRan 2 tests in [0-9]+(?:\.[0-9]+)?s\n\nOK\n'
 if re.fullmatch(grammar,text) is None:raise Refused('MALFORMED_TEST_OUTPUT')
 return True

def git(root,*args):
 env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_NO_LAZY_FETCH':'1','GIT_NO_REPLACE_OBJECTS':'1','GIT_OPTIONAL_LOCKS':'0'}
 r=subprocess.run(['/usr/bin/git','-c','core.fsmonitor=false','-c','core.hooksPath=/dev/null','-C',str(root),*args],env=env,capture_output=True,timeout=5)
 if r.returncode or len(r.stdout)>32768 or r.stderr:raise Refused('GIT_METADATA')
 return r.stdout.decode().strip()

def verify_source(root):
 for rel,digest in EXPECTED_SOURCE.items():
  f=root/rel
  if f.is_symlink() or hashlib.sha256(f.read_bytes()).hexdigest()!=digest:raise Refused('SOURCE_HASH')
 actual={p.relative_to(root).as_posix() for p in (root/'.github/workflows').iterdir() if p.suffix in ('.yml','.yaml')}
 if actual!=set(EXPECTED_WORKFLOWS)|{'.github/workflows/task11-acl-rolling.yml'}:raise Refused('WORKFLOW_SET')
 for rel,digest in EXPECTED_WORKFLOWS.items():
  if hashlib.sha256((root/rel).read_bytes()).hexdigest()!=digest:raise Refused('OTHER_WORKFLOW_DRIFT')

def child(root,result_path):
 verify_source(root)
 spec=importlib.util.spec_from_file_location('pinned_acl_contract',root/'tests/deploy/test_deploy_release_root_modes.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 cases=[m.RootModeTests(name) for name in NAMES]
 result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(cases))
 cleanup=all(hasattr(case,'root') and str(case.root).startswith('/tmp/tmp.') and not case.root.exists() and not case.root.is_symlink() for case in cases)
 success=result.wasSuccessful() and result.testsRun==2 and not result.skipped and cleanup
 record=dict(protocol='task11.acl.result.v1',ids=list(IDS),tests=result.testsRun,failures=len(result.failures),errors=len(result.errors),skips=len(result.skipped),cleanup=cleanup,success=bool(success))
 result_path.open('x').write(json.dumps(record,sort_keys=True)+'\n')
 return 0 if success else 2

def group_alive(pgid):
 try:os.killpg(pgid,0)
 except ProcessLookupError:return False
 except (PermissionError,OSError):raise Refused('TEST_GROUP_STATUS_UNKNOWN') from None
 return True

def stop_owned_group(p):
 deadline=time.monotonic()+2
 alive=group_alive(p.pid)
 if time.monotonic()>=deadline:raise Refused('TEST_GROUP_CLEANUP_UNKNOWN')
 if alive:
  try:os.killpg(p.pid,signal.SIGKILL)
  except ProcessLookupError:pass
 left=deadline-time.monotonic()
 if left<=0:raise Refused('TEST_GROUP_CLEANUP_UNKNOWN')
 p.wait(timeout=left)
 if time.monotonic()>=deadline:raise Refused('TEST_GROUP_CLEANUP_UNKNOWN')
 while group_alive(p.pid):
  left=deadline-time.monotonic()
  if left<=0:raise Refused('TEST_GROUP_CLEANUP_UNKNOWN')
  time.sleep(min(.01,left))
 if time.monotonic()>=deadline:raise Refused('TEST_GROUP_CLEANUP_UNKNOWN')
 return alive

def capture(argv,env,budget=30):
 started=time.monotonic();deadline=started+budget;p=None;sel=None;out=bytearray();err=bytearray()
 try:
  p=subprocess.Popen(argv,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
  if time.monotonic()>=deadline:raise Refused('TEST_TIMEOUT')
  sel=selectors.DefaultSelector();sel.register(p.stdout,selectors.EVENT_READ,'out');sel.register(p.stderr,selectors.EVENT_READ,'err')
  while sel.get_map():
   left=deadline-time.monotonic()
   if left<=0:raise Refused('TEST_TIMEOUT')
   ready=sel.select(left)
   if time.monotonic()>=deadline:raise Refused('TEST_TIMEOUT')
   for key,_ in ready:
    if time.monotonic()>=deadline:raise Refused('TEST_TIMEOUT')
    b=os.read(key.fileobj.fileno(),1024)
    if not b:sel.unregister(key.fileobj);continue
    (out if key.data=='out' else err).extend(b)
    if len(out)+len(err)>32768:raise Refused('TEST_OUTPUT_CAP')
  code=p.wait(timeout=max(.001,deadline-time.monotonic()))
  if time.monotonic()>=deadline:raise Refused('TEST_TIMEOUT')
  return code,bytes(out),bytes(err)
 finally:
  primary=sys.exc_info()[1];cleanup_unknown=False
  if sel is not None:
   try:sel.close()
   except BaseException:cleanup_unknown=True
  if p is not None:
   try:
    had_members=stop_owned_group(p)
    if primary is None and had_members:primary=Refused('TEST_DESCENDANTS_REMAINED')
   except BaseException:cleanup_unknown=True
   for stream in (p.stdout,p.stderr):
    if stream is not None:
     try:stream.close()
     except BaseException:cleanup_unknown=True
  if cleanup_unknown:
   error=Refused('TEST_CLEANUP_UNKNOWN')
   error.primary_code=str(primary) if isinstance(primary,Refused) else 'UNEXPECTED_CAPTURE_FAILURE' if primary else None
   raise error from None
  if primary is not None and sys.exc_info()[1] is None:raise primary

def parent(root,results):
 if os.environ.get('ACL_RUNNER_ENVIRONMENT')!='github-hosted' or os.environ.get('RUNNER_OS')!='Linux' or os.environ.get('RUNNER_ARCH')!='X64':raise Refused('RUNNER_IMAGE')
 os_release=platform.freedesktop_os_release()
 if os_release.get('ID')!='ubuntu' or os_release.get('VERSION_ID')!='22.04':raise Refused('OS_RELEASE')
 if not mapped(Path('/proc/self/uid_map').read_text()):raise Refused('UID_2001_UNMAPPED')
 if not all(Path('/usr/bin/'+n).is_file() and os.access('/usr/bin/'+n,os.X_OK) for n in ('setfacl','getfacl','cp','stat','flock')):raise Refused('TOOLS_MISSING')
 if os.environ.get('GITHUB_EVENT_NAME')!='push' or os.environ.get('GITHUB_REF')!='refs/heads/'+BRANCH or os.environ.get('GITHUB_REPOSITORY')!='3pacs/GRID' or os.environ.get('GITHUB_RUN_ATTEMPT')!='1':raise Refused('EVENT_CONTEXT')
 sha=os.environ.get('GITHUB_SHA','')
 event=decode_result(Path(os.environ['GITHUB_EVENT_PATH']).read_bytes())
 if event.get('created') is not True or event.get('deleted') is not False or event.get('forced') is not False or event.get('before')!='0'*40 or event.get('after')!=sha or event.get('ref')!='refs/heads/'+BRANCH:raise Refused('EVENT_PAYLOAD')
 if not re.fullmatch('[0-9a-f]{40}',sha) or git(root,'rev-parse','HEAD')!=sha:raise Refused('RECEIVING_COMMIT')
 raw=git(root,'cat-file','commit','HEAD');parents=re.findall(r'^parent ([0-9a-f]{40})$',raw,re.M)
 if parents!=[PARENT]:raise Refused('RECEIVING_PARENT')
 verify_source(root)
 results.mkdir(mode=0o700,exist_ok=False)
 facts=dict(protocol='task11.acl.preflight.v1',head=sha,tree=git(root,'rev-parse','HEAD^{tree}'),parent=PARENT,python=platform.python_version(),kernel=platform.release(),uid_map=Path('/proc/self/uid_map').read_text().strip(),os_release={'ID':os_release['ID'],'VERSION_ID':os_release['VERSION_ID']},image_environment_untrusted={'ImageOS':os.environ.get('ImageOS'),'ImageVersion':os.environ.get('ImageVersion')},image_scope=IMAGE_SCOPE,runner_environment='github-hosted',source_hashes=EXPECTED_SOURCE)
 for name in ('setfacl','getfacl','cp','stat','bash'):
  r=subprocess.run(['/usr/bin/'+name,'--version'],capture_output=True,text=True,timeout=2)
  if r.returncode:raise Refused('TOOL_VERSION')
  facts[name+'_version']=r.stdout.splitlines()[0]
 fs=subprocess.run(['/usr/bin/stat','-f','-c','%T','/tmp'],capture_output=True,text=True,timeout=2)
 if fs.returncode or len(fs.stdout)>128:raise Refused('FILESYSTEM_METADATA')
 facts['tmp_filesystem']=fs.stdout.strip()
 print('PREFLIGHT '+json.dumps(facts,sort_keys=True),flush=True)
 env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'}
 code,out,err=capture(['/usr/bin/python3','-I','-B',str(Path(__file__).resolve()),'--child',str(root),str(results/'result.json')],env)
 (results/'stdout.bin').write_bytes(out);(results/'stderr.bin').write_bytes(err)
 print('RAW_STDOUT_BASE64 '+base64.b64encode(out).decode(),flush=True);print('RAW_STDERR_BASE64 '+base64.b64encode(err).decode(),flush=True)
 print('RAW_SHA256 '+json.dumps(dict(stdout=hashlib.sha256(out).hexdigest(),stderr=hashlib.sha256(err).hexdigest())),flush=True)
 f=results/'result.json'
 if not f.is_file() or f.is_symlink() or f.stat().st_size>4096:raise Refused('RESULT_ABSENT_OR_BOUND')
 record=decode_result(f.read_bytes());judge(record,code,out+err)
 print('QUALIFICATION '+json.dumps(record,sort_keys=True),flush=True)
 return 0

def main():
 try:
  if len(sys.argv)==4 and sys.argv[1]=='--child':return child(Path(sys.argv[2]),Path(sys.argv[3]))
  if len(sys.argv)==4 and sys.argv[1]=='--qualify':return parent(Path(sys.argv[2]),Path(sys.argv[3]))
  raise Refused('ARGUMENTS')
 except BaseException as error:
  code=str(error) if isinstance(error,Refused) else 'UNEXPECTED_QUALIFIER_FAILURE'
  print('QUALIFICATION_REFUSED '+json.dumps({'code':code,'primary_code':getattr(error,'primary_code',None),'qualified':False}),flush=True);return 2
if __name__=='__main__':raise SystemExit(main())
