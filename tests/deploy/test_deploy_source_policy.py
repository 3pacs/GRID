"""Offline source policy tests; no product, DB, provider or service imports."""
import contextlib,importlib.util,os,stat,subprocess,sys,tempfile,time,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('policy',ROOT/'scripts/deploy_source_policy.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Tests(unittest.TestCase):
 @contextlib.contextmanager
 def fixture(self):
  with tempfile.TemporaryDirectory(prefix='task11-source-contract-') as tmp:
   p=Path(tmp);live=p/'live';releases=Path(str(live)+'.releases');releases.mkdir(mode=0o700);root=releases/'one';root.mkdir(mode=0o700)
   env={'PATH':'/usr/bin:/bin','HOME':tmp,'LANG':'C','LC_ALL':'C','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_AUTHOR_NAME':'Synthetic','GIT_AUTHOR_EMAIL':'synthetic@example.invalid','GIT_COMMITTER_NAME':'Synthetic','GIT_COMMITTER_EMAIL':'synthetic@example.invalid'}
   def git(*args):
    saved=os.umask(0o077)
    try:return subprocess.run(['/usr/bin/git','-C',str(root),*args],env=env,capture_output=True,check=True).stdout.decode().strip()
    finally:os.umask(saved)
   mask=os.umask(0o077)
   try:git('init','-q');(root/'source.py').write_text('value=1\n');git('add','source.py');git('commit','-qm','invented source')
   finally:os.umask(mask)
   live.symlink_to(root);original=m.directory
   # Only the fixture's inherited world-writable /tmp ancestor is synthetic.
   # The production ancestor predicate is tested independently, without bypass.
   def directory(path):
    path=m.lexical(path)
    if path==p or p in path.parents:
     relative=path.relative_to(p);items=[p];cursor=p
     for name in relative.parts:cursor=cursor/name;items.append(cursor)
     for item in items:m.trusted(item.lstat(),stat.S_ISDIR)
     return path
    return original(path)
   with patch.object(m,'directory',side_effect=directory):yield live,root,releases,git
 def test_directory_each_ancestor_mode_owner_link(self):
  p=Path('/protected/release');base=types.SimpleNamespace(st_mode=stat.S_IFDIR|0o755,st_uid=os.geteuid(),st_nlink=1)
  for bad in ('/','/protected','/protected/release'):
   for mode,uid,reason in ((stat.S_IFDIR|0o775,os.geteuid(),'PATH_WRITABLE'),(stat.S_IFDIR|0o755,2001 if os.geteuid()!=2001 else 2002,'PATH_OWNER'),(stat.S_IFLNK|0o777,os.geteuid(),'PATH_TYPE')):
    def lstat(path):return types.SimpleNamespace(st_mode=mode,st_uid=uid,st_nlink=1) if str(path)==bad else base
    with patch.object(Path,'lstat',lstat):
     with self.assertRaisesRegex(m.Refused,reason):m.directory(p)
 def test_lexical_refusals(self):
  for p in ('relative','/a/../b','/a/./b','/a\n'):
   with self.assertRaises(m.Refused):m.lexical(p)
 def test_real_repository_identity_and_tracked_bytes(self):
  with self.fixture() as (live,root,parent,git):
   receipt=m.repository(live,live,git('rev-parse','HEAD'));self.assertEqual(receipt['working_bytes'],'TRACKED_BYTES_CHECKED');self.assertEqual(receipt['tracked_files'],1)
 def test_dirty_tracked_bytes_refused(self):
  with self.fixture() as (live,root,parent,git):
   (root/'source.py').write_text('value=2\n')
   with self.assertRaisesRegex(m.Refused,'TRACKED_BYTES'):m.repository(live,root)
 def test_mutable_untracked_runtime_not_walked(self):
  with self.fixture() as (live,root,parent,git):
   (root/'.gitignore').write_text('runtime/\n');os.chmod(root/'.gitignore',0o600);git('add','.gitignore');git('commit','-qm','declared mutable data');(root/'runtime').mkdir(mode=0o777);os.chmod(root/'runtime',0o777);(root/'runtime/state').write_text('invented mutable data');self.assertEqual(m.repository(live,root)['tracked_files'],2)
 def test_staged_extra_source_cannot_hide_from_untracked_gate(self):
  with self.fixture() as (live,root,parent,git):
   (root/'staged-extra.py').write_text('synthetic staged source');git('add','staged-extra.py')
   with self.assertRaisesRegex(m.Refused,'INDEX_BINDING'):m.repository(live,root)
 def test_untracked_nonignored_input_refused(self):
  with self.fixture() as (live,root,parent,git):
   (root/'unreviewed.py').write_text('synthetic untracked source')
   with self.assertRaisesRegex(m.Refused,'UNTRACKED_INPUT'):m.repository(live,root)
 def test_writable_git_descendant_before_git_command(self):
  with self.fixture() as (live,root,parent,git):
   os.chmod(root/'.git/HEAD',0o666)
   with patch.object(m,'_git',side_effect=AssertionError('must refuse before Git')):
    with self.assertRaisesRegex(m.Refused,'PATH_WRITABLE'):m.repository(live,root)
   self.assertEqual(stat.S_IMODE((root/'.git/HEAD').stat().st_mode),0o666)
 def test_git_nested_symlink_and_hardlink_refused(self):
  with self.fixture() as (live,root,parent,git):
   extra=root/'.git/unsafe';extra.symlink_to(root/'source.py')
   with self.assertRaisesRegex(m.Refused,'PATH_TYPE'):m.repository(live,root)
   extra.unlink();os.link(root/'source.py',extra)
   with self.assertRaisesRegex(m.Refused,'FILE_LINK_COUNT'):m.repository(live,root)
 def test_external_git_store_and_config_include_refused(self):
  with self.fixture() as (live,root,parent,git):
   alt=root/'.git/objects/info/alternates';alt.write_text('/synthetic/outside\n');os.chmod(alt,0o600)
   with self.assertRaisesRegex(m.Refused,'EXTERNAL_GIT_STORE'):m.repository(live,root)
   alt.unlink();config=root/'.git/config';config.write_text(config.read_text()+'\n[include]\n path = /synthetic/outside\n')
   with self.assertRaisesRegex(m.Refused,'GIT_CONFIG_EXTERNAL'):m.repository(live,root)
 def test_bare_or_external_worktree_config_refused(self):
  with self.fixture() as (live,root,parent,git):
   config=root/'.git/config';old=config.read_text();config.write_text(old.replace('bare = false','bare = true'))
   with self.assertRaisesRegex(m.Refused,'GIT_WORKTREE_CONFIG'):m.repository(live,root)
   config.write_text(old+'\n[core]\n worktree = /synthetic/outside\n')
   with self.assertRaisesRegex(m.Refused,'GIT_WORKTREE_CONFIG'):m.repository(live,root)
 def test_exact_git_binding_refusals(self):
  with self.fixture() as (live,root,parent,git):
   original=m._git
   for args,value,code in ((['rev-parse','--show-toplevel'],'/other','GIT_TOPLEVEL'),(['rev-parse','--absolute-git-dir'],'/other/.git','GIT_DIRECTORY'),(['rev-parse','--is-bare-repository'],'true','GIT_BARE')):
    def altered(root,query,deadline,cap=8192):return value if query==args else original(root,query,deadline,cap)
    with patch.object(m,'_git',side_effect=altered):
     with self.assertRaisesRegex(m.Refused,code):m.repository(live,root)
 def test_internal_tracked_link_and_external_policy_refusal(self):
  with self.fixture() as (live,root,parent,git):
   (root/'alias.py').symlink_to('source.py');git('add','alias.py');git('commit','-qm','internal link');self.assertEqual(m.repository(live,root)['tracked_files'],2)
   (root/'model.gguf').symlink_to('/synthetic/external-model');git('add','model.gguf');git('commit','-qm','external artifact link')
   with self.assertRaisesRegex(m.Refused,'EXTERNAL_TRACKED_TARGET'):m.repository(live,root)
 def test_scoped_markers_and_lock_private_under_permissive_umask(self):
  with self.fixture() as (live,root,parent,git):
   mask=os.umask(0o002)
   try:m.prepare(parent);m.marker_write(parent,'.previous-release',str(root)+'\n')
   finally:os.umask(mask)
   self.assertEqual(stat.S_IMODE((parent/'.lock').stat().st_mode),0o600);self.assertEqual(stat.S_IMODE((parent/'.previous-release').stat().st_mode),0o600);self.assertEqual(m.marker_read(parent,'.previous-release'),str(root)+'\n')
 def test_unsafe_existing_marker_never_repaired(self):
  with self.fixture() as (live,root,parent,git):
   target=parent/'.previous-release';target.write_text('original\n');os.chmod(target,0o666)
   with self.assertRaisesRegex(m.Refused,'PATH_WRITABLE'):m.marker_write(parent,target.name,'replacement\n')
   self.assertEqual(target.read_text(),'original\n');self.assertEqual(stat.S_IMODE(target.stat().st_mode),0o666)
 def test_marker_symlink_hardlink_fifo_and_content_refuse(self):
  with self.fixture() as (live,root,parent,git):
   target=parent/'.previous-release';target.symlink_to(root/'source.py')
   with self.assertRaises(OSError):m.marker_read(parent,target.name)
   target.unlink();os.link(root/'source.py',target)
   with self.assertRaisesRegex(m.Refused,'FILE_LINK_COUNT'):m.marker_read(parent,target.name)
   target.unlink();os.mkfifo(target,0o600)
   with self.assertRaisesRegex(m.Refused,'PATH_TYPE'):m.marker_read(parent,target.name)
   target.unlink()
   for b in (b'x'*2049,b'\x00',b'\xff'):
    target.write_bytes(b);os.chmod(target,0o600)
    with self.assertRaises(m.Refused):m.marker_read(parent,target.name)
    target.unlink()
 def test_marker_replace_failure_preserves_original_and_cleans_temp(self):
  with self.fixture() as (live,root,parent,git):
   m.marker_write(parent,'.previous-release','old\n')
   with patch.object(m.os,'replace',side_effect=OSError('synthetic replace fault')):
    with self.assertRaises(OSError):m.marker_write(parent,'.previous-release','new\n')
   self.assertEqual(m.marker_read(parent,'.previous-release'),'old\n');self.assertEqual(list(parent.glob('.protected-marker-*')),[])
 def test_marker_post_replace_durability_failure_not_claimed_rollback(self):
  with self.fixture() as (live,root,parent,git):
   m.marker_write(parent,'.previous-release','old\n');original=m.os.fsync;count=[]
   def fsync(fd):
    count.append(fd)
    if len(count)==2:raise OSError('synthetic directory durability fault')
    return original(fd)
   with patch.object(m.os,'fsync',side_effect=fsync):
    with self.assertRaises(OSError):m.marker_write(parent,'.previous-release','new\n')
   self.assertEqual(m.marker_read(parent,'.previous-release'),'new\n')
 def test_retention_immutable_and_activation_exact_label(self):
  with self.fixture() as (live,root,parent,git):
   for name in ('.runtime-preservation','.runtime-retention','.lock'):
    with self.assertRaisesRegex(m.Refused,'MARKER_NOT_ORDINARY_WRITER'):m.marker_write(parent,name,'x')
   m.marker_write(parent,'.activation-in-progress','label=exact swapped_at=synthetic\n');self.assertFalse(m.marker_clear(parent,'exa'));self.assertFalse(m.marker_clear(parent,'other'));self.assertTrue(m.marker_clear(parent,'exact'))
 def test_rollback_namespace_and_service_exact_binding(self):
  with self.fixture() as (live,root,parent,git):
   expected=git('rev-parse','HEAD');self.assertEqual(m.binding(live,root,expected)['root'],str(root))
   import shutil
   twin=parent/'twin';shutil.copytree(root,twin)
   with self.assertRaisesRegex(m.Refused,'SERVICE_BINDING'):m.binding(live,twin,expected)
   outside=root.parent.parent/'outside';outside.mkdir(mode=0o700)
   with self.assertRaisesRegex(m.Refused,'RELEASE_SCOPE'):m.repository(live,outside)
   with self.assertRaisesRegex(m.Refused,'GIT_EXPECTED'):m.binding(live,root,'0'*40)
   other=parent/'other';other.symlink_to(root)
   with self.assertRaisesRegex(m.Refused,'PATH_TYPE'):m.repository(live,other)
 def test_budget_and_no_credential_or_network_environment(self):
  with self.assertRaisesRegex(m.Refused,'TRUST_BUDGET'):m._budget(time.monotonic()-1)
  with self.fixture() as (live,root,parent,git):
   original=m._capture;observed=[]
   def run(*a,**kw):observed.append(a[1]);return original(*a,**kw)
   with patch.object(m,'_capture',side_effect=run):m.repository(live,root)
   self.assertTrue(observed);self.assertTrue(all(not any(k.startswith(('PG','GH_TOKEN','GITHUB_TOKEN')) for k in e) for e in observed))
 def test_fresh_empty_candidate_pre_hook_and_strict_post_hook(self):
  with self.fixture() as (live,root,parent,git):
   fresh=parent/'fresh';fresh.mkdir(mode=0o700)
   self.assertEqual(m.candidate(live,fresh)['state'],'PROTECTED_EMPTY_BEFORE_BUILD')
   with self.assertRaises(FileNotFoundError):m.repository(live,fresh)
   (fresh/'untrusted').write_text('not an empty candidate')
   with self.assertRaisesRegex(m.Refused,'CANDIDATE_NOT_EMPTY'):m.candidate(live,fresh)
 def test_committed_executable_binding(self):
  with self.fixture() as (live,root,parent,git):
   code=root/'source.py';os.chmod(code,0o700)
   with self.assertRaisesRegex(m.Refused,'TRACKED_EXECUTABLE'):m.repository(live,root)
   git('add','source.py');git('commit','-qm','declared executable');self.assertEqual(m.repository(live,root)['tracked_files'],1)
   os.chmod(code,0o600)
   with self.assertRaisesRegex(m.Refused,'TRACKED_EXECUTABLE'):m.repository(live,root)
 def test_tracked_link_chain_no_blind_resolution_or_external_descent(self):
  with self.fixture() as (live,root,parent,git):
   (root/'second.py').symlink_to('source.py');(root/'first.py').symlink_to('second.py');git('add','first.py','second.py');git('commit','-qm','bounded internal chain')
   with patch.object(Path,'resolve',side_effect=AssertionError('blind resolve forbidden')):self.assertEqual(m.repository(live,root)['tracked_files'],3)
   original=Path.lstat;seen=[]
   (root/'second.py').unlink();(root/'second.py').symlink_to('/synthetic/outside');git('add','second.py');git('commit','-qm','external chain')
   def lstat(path,*a,**kw):
    seen.append(str(path));self.assertFalse(str(path).startswith('/synthetic'));return original(path,*a,**kw)
   with patch.object(Path,'lstat',lstat):
    with self.assertRaisesRegex(m.Refused,'EXTERNAL_TRACKED_TARGET'):m.repository(live,root)
 def test_ambient_git_override_not_in_shared_commands(self):
  with self.fixture() as (live,root,parent,git):
   original=m._capture;observed=[]
   def run(*a,**kw):observed.append((a[0],a[1]));return original(*a,**kw)
   override={'GIT_DIR':'/synthetic/other','GIT_WORK_TREE':'/synthetic/tree','GIT_CONFIG_COUNT':'1','GIT_CONFIG_KEY_0':'core.fsmonitor','GIT_CONFIG_VALUE_0':'must-not-run'}
   with patch.dict(os.environ,override),patch.object(m,'_capture',side_effect=run):m.repository(live,root)
   self.assertTrue(observed)
   for argv,env in observed:
    self.assertFalse(set(override)&set(env));self.assertIn('core.fsmonitor=false',argv);self.assertIn('core.hooksPath=/dev/null',argv);self.assertIn('credential.helper=',argv)
 def test_pin_explicit_fetch_exception_and_creation_mask(self):
  with self.fixture() as (live,root,parent,git):
   expected=git('rev-parse','HEAD');calls=[];mask=os.umask(0o027)
   try:
    def command(root,args,deadline,**kw):calls.append((args,kw,os.umask(0o777)));os.umask(calls[-1][2]);return ''
    with patch.object(m,'pin_inputs',return_value={}),patch.object(m,'identity',return_value={'head':expected}),patch.object(m,'_git',side_effect=command):self.assertEqual(m.pin(root,'https://github.com/3pacs/GRID.git',expected)['head'],expected)
    current=os.umask(0o777);os.umask(current);self.assertEqual(current,0o027)
   finally:os.umask(mask)
   self.assertEqual([x[0][0] for x in calls],['fetch','reset']);self.assertTrue(calls[0][1]['network']);self.assertEqual(calls[0][2],0o077);self.assertNotIn('network',calls[1][1])
   for url in ('file:///synthetic/origin','https://other.invalid/3pacs/GRID.git','https://github.com:bad/3pacs/GRID.git'):
    with patch.object(m,'pin_inputs',return_value={}),patch.object(m,'identity',return_value={'head':expected}),patch.object(m,'_git',side_effect=AssertionError('no fetch')):
     with self.assertRaisesRegex(m.Refused,'FETCH_URL'):m.pin(root,url,expected)
 def test_pin_refusal_never_echoes_synthetic_credential(self):
  import io
  error=io.StringIO()
  with patch.object(m,'pin_inputs',return_value={}),patch.object(m,'identity',return_value={}),contextlib.redirect_stderr(error):
   self.assertEqual(m.main(['pin','/synthetic/root','https://wrong:synthetic-private-value@github.com/3pacs/GRID.git','0'*40]),5)
  self.assertNotIn('synthetic-private-value',error.getvalue());self.assertEqual(error.getvalue().strip(),'SOURCE_POLICY_REFUSED FETCH_URL')
 def test_filter_or_external_attribute_config_refused(self):
  with self.fixture() as (live,root,parent,git):
   config=root/'.git/config';old=config.read_text()
   for section in ('[filter "external"]\n process = must-not-run\n','[core]\n attributesFile = /synthetic/outside\n','[remote "origin"]\n promisor = true\n'):
    config.write_text(old+'\n'+section)
    with self.assertRaisesRegex(m.Refused,'GIT_CONFIG_EXTERNAL'):m.repository(live,root)
 def test_git_process_group_and_output_bounds_inert(self):
  from types import SimpleNamespace
  stream=SimpleNamespace(close=lambda:None,fileno=lambda:12)
  proc=SimpleNamespace(pid=424242,stdout=stream,stderr=stream,wait=lambda timeout:0)
  selector=SimpleNamespace(register=lambda *a:None,get_map=lambda:{},close=lambda:None)
  with patch.object(m.subprocess,'Popen',return_value=proc),patch.object(m.selectors,'DefaultSelector',return_value=selector),patch.object(m,'stop_owned_group',return_value=True):
   with self.assertRaisesRegex(m.Refused,'GIT_PROCESS_DESCENDANTS_REMAINED'):m._capture(['/synthetic'],{})
  selector=SimpleNamespace(register=lambda *a:None,get_map=lambda:{1:1},select=lambda t:[(SimpleNamespace(fileobj=stream,data='out'),1)],close=lambda:None)
  with patch.object(m.subprocess,'Popen',return_value=proc),patch.object(m.selectors,'DefaultSelector',return_value=selector),patch.object(m.os,'read',return_value=b'oversize'),patch.object(m,'stop_owned_group',return_value=False):
   with self.assertRaisesRegex(m.Refused,'GIT_PROCESS_OUTPUT_CAP'):m._capture(['/synthetic'],{},output_cap=2)
 def test_wait_timeout_fetch_argv_never_disclosed(self):
  import io
  with self.fixture() as (live,root,parent,git):
   expected=git('rev-parse','HEAD');url='https://x-access-token:synthetic-private-value@github.com/3pacs/GRID.git';error=io.StringIO();output=io.StringIO()
   stream=types.SimpleNamespace(close=lambda:None)
   def wait(timeout):raise subprocess.TimeoutExpired(['/usr/bin/git','fetch',url],timeout)
   proc=types.SimpleNamespace(pid=424242,stdout=stream,stderr=stream,wait=wait)
   selector=types.SimpleNamespace(register=lambda *a:None,get_map=lambda:{},close=lambda:None)
   with patch.object(m,'pin_inputs',return_value={}),patch.object(m,'identity',return_value={}),patch.object(m.subprocess,'Popen',return_value=proc),patch.object(m.selectors,'DefaultSelector',return_value=selector),patch.object(m,'stop_owned_group',return_value=False),contextlib.redirect_stderr(error),contextlib.redirect_stdout(output):
    self.assertEqual(m.main(['pin',str(root),url,expected]),5)
   self.assertEqual(error.getvalue().strip(),'SOURCE_POLICY_REFUSED GIT_PROCESS_TIMEOUT');self.assertEqual(output.getvalue(),'');self.assertNotIn('synthetic-private-value',error.getvalue());self.assertNotIn('fetch',error.getvalue())
 def test_unexpected_capture_and_cli_exception_fixed_codes(self):
  import io
  with self.fixture() as (live,root,parent,git):
   expected=git('rev-parse','HEAD');url='https://x-access-token:synthetic-private-value@github.com/3pacs/GRID.git'
   for layer,code in (('_capture','GIT_COMMAND'),('pin','UNEXPECTED')):
    error=io.StringIO()
    with patch.object(m,'pin_inputs',return_value={}),patch.object(m,'identity',return_value={}),patch.object(m,layer,side_effect=RuntimeError('synthetic-private-value '+url)),contextlib.redirect_stderr(error):
     self.assertEqual(m.main(['pin',str(root),url,expected]),5)
    self.assertEqual(error.getvalue().strip(),'SOURCE_POLICY_REFUSED '+code);self.assertNotIn('synthetic-private-value',error.getvalue())
 def add_contract(self,root,git,**change):
  import json
  baseline=git('rev-parse','HEAD');tree=git('rev-parse','HEAD^{tree}');c=dict(schema='grid.protected-payload.v1',baseline_commit=baseline,baseline_tree=tree,default='PROTECT_ALL_COMMITTED_PATHS',exclusions=[],external=[]);c.update(change)
  path=root/'scripts/deploy_payload_contract.json';path.parent.mkdir(mode=0o700,exist_ok=True);path.write_text(json.dumps(c)+'\n');os.chmod(path,0o600);git('add','scripts/deploy_payload_contract.json');git('commit','-qm','commit-bound payload contract');return c
 def test_commit_bound_payload_contract_protects_itself(self):
  with self.fixture() as (live,root,parent,git):
   self.add_contract(root,git);self.assertEqual(m.repository(live,root)['tracked_files'],2)
   (root/'scripts/deploy_payload_contract.json').write_text('{}')
   with self.assertRaisesRegex(m.Refused,'TRACKED_BYTES'):m.repository(live,root)
 def test_contract_cannot_quietly_exclude_code_or_mark_unknown_verified(self):
  for change in (dict(exclusions=['source.py']),dict(default='EXCLUDE_EXECUTABLES'),dict(extra=True)):
   with self.fixture() as (live,root,parent,git):
    self.add_contract(root,git,**change)
    with self.assertRaisesRegex(m.Refused,'PAYLOAD_CONTRACT_SHAPE'):m.repository(live,root)
 def test_external_contract_remains_closed_without_owner_integrity(self):
  with self.fixture() as (live,root,parent,git):
   (root/'external-workflow.md').symlink_to('/synthetic/workflow.md');git('add','external-workflow.md');git('commit','-qm','external executable configuration');oid=git('rev-parse','HEAD:external-workflow.md')
   self.add_contract(root,git,external=[dict(path='external-workflow.md',link_oid=oid,declared_target='/synthetic/workflow.md',role='EXTERNAL_EXECUTABLE_WORKFLOW',target_integrity='UNRESOLVED',target_owner='UNRESOLVED')])
   with self.assertRaisesRegex(m.Refused,'EXTERNAL_PAYLOAD_POLICY_UNRESOLVED'):m.repository(live,root)
 def test_genuinely_unborn_git_target_pre_fetch_only(self):
  with self.fixture() as (live,root,parent,git):
   fresh=parent/'unborn';fresh.mkdir(mode=0o700);mask=os.umask(0o077)
   try:subprocess.run(['/usr/bin/git','-C',str(fresh),'init','-q'],env={'PATH':'/usr/bin:/bin','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null'},capture_output=True,check=True)
   finally:os.umask(mask)
   self.assertEqual(m.pin_inputs(fresh)['state'],'PROTECTED_UNBORN_BEFORE_FETCH');self.assertEqual(m.candidate(live,fresh)['state'],'PROTECTED_UNBORN_BEFORE_FETCH')
   with self.assertRaises(m.Refused):m.repository(live,fresh)
   (fresh/'unreviewed.py').write_text('not empty')
   with self.assertRaisesRegex(m.Refused,'UNBORN_NONEMPTY'):m.pin_inputs(fresh)
 def test_unknown_external_owner_or_integrity_never_becomes_verified(self):
  for mutation in ('integrity','owner','missing'):
   with self.fixture() as (live,root,parent,git):
    (root/'external.md').symlink_to('/synthetic/external.md');git('add','external.md');git('commit','-qm','declared unresolved external')
    row=dict(path='external.md',link_oid=git('rev-parse','HEAD:external.md'),declared_target='/synthetic/external.md',role='EXTERNAL_EXECUTABLE_WORKFLOW',target_integrity='UNRESOLVED',target_owner='UNRESOLVED')
    if mutation=='integrity':row['target_integrity']='VERIFIED'
    elif mutation=='owner':row['target_owner']='invented-owner'
    else:del row['target_owner']
    self.add_contract(root,git,external=[row])
    with self.assertRaisesRegex(m.Refused,'PAYLOAD_CONTRACT_SHAPE'):m.repository(live,root)
 def test_payload_baseline_tree_must_match_real_git_object(self):
  with self.fixture() as (live,root,parent,git):
   self.add_contract(root,git,baseline_tree='0'*40)
   with self.assertRaisesRegex(m.Refused,'PAYLOAD_BASELINE'):m.repository(live,root)
 def test_fixture_adapter_is_scoped_labelled_and_not_production(self):
  import io
  spec=importlib.util.spec_from_file_location('unit_adapter',ROOT/'tests/deploy/source_policy_fixture.py');ad=importlib.util.module_from_spec(spec);spec.loader.exec_module(ad)
  with tempfile.TemporaryDirectory(prefix='tmp.',dir='/tmp') as tmp:
   box=Path(tmp);mask=os.umask(0o077)
   try:repo=ad.install(ROOT,box,'filesystem')
   finally:os.umask(mask)
   inside=box/'toy';inside.mkdir(mode=0o700);output=io.StringIO();error=io.StringIO()
   with patch.dict(os.environ,{'TASK11_POLICY_FIXTURE_ROOT':str(box)}),patch.object(ad,'__file__',str(repo/'scripts/deploy_source_policy.py')),contextlib.redirect_stdout(output),contextlib.redirect_stderr(error):
    self.assertEqual(ad.run(['identity',str(inside)]),0);self.assertIn('FILESYSTEM_UNIT_IDENTITY_DOUBLE_NOT_GIT_PROOF',output.getvalue())
    self.assertEqual(ad.run(['identity','/synthetic/outside']),5);self.assertIn('UNIT_NAMESPACE_ESCAPE',error.getvalue())
    self.assertEqual(ad.run(['identity',str(inside),'0'*40]),5);self.assertIn('UNIT_NO_GIT_EXPECTATION',error.getvalue())
   self.assertNotIn('TASK11_POLICY_FIXTURE_ROOT',(ROOT/'scripts/deploy_source_policy.py').read_text())
 def test_fixture_adapter_refuses_unprotected_or_nonfixture_root(self):
  spec=importlib.util.spec_from_file_location('unit_adapter',ROOT/'tests/deploy/source_policy_fixture.py');ad=importlib.util.module_from_spec(spec);spec.loader.exec_module(ad)
  with tempfile.TemporaryDirectory(prefix='tmp.',dir='/tmp') as tmp:
   box=Path(tmp);os.chmod(box,0o775)
   with self.assertRaises(AssertionError):ad.owned_box(box)
   os.chmod(box,0o700)
  with self.assertRaises(AssertionError):ad.owned_box('/data/grid_v4')
 def test_relative_frontend_style_link_stays_commit_bound(self):
  with self.fixture() as (live,root,parent,git):
   mask=os.umask(0o077)
   try:
    (root/'app/src/lib').mkdir(parents=True,mode=0o700);(root/'shared').mkdir(mode=0o700);(root/'shared/logic.js').write_text('synthetic source');(root/'app/src/lib/alias.js').symlink_to('../../../shared/logic.js');git('add','app','shared');git('commit','-qm','portable committed alias')
   finally:os.umask(mask)
   self.assertEqual(m.repository(live,root)['tracked_files'],3)
 def test_code_beneath_data_or_output_is_not_excluded(self):
  for prefix in ('data','outputs'):
   with self.fixture() as (live,root,parent,git):
    mask=os.umask(0o077)
    try:(root/prefix).mkdir(mode=0o700);(root/prefix/'executable_input.py').write_text('x=1\n');git('add',prefix);git('commit','-qm','code in non-code-looking directory')
    finally:os.umask(mask)
    self.add_contract(root,git);(root/prefix/'executable_input.py').write_text('x=2\n')
    with self.assertRaisesRegex(m.Refused,'TRACKED_BYTES'):m.repository(live,root)
 def test_caller_sources_use_policy_before_git_and_markers(self):
  scripts=ROOT/'scripts'
  for name in ('deploy_release_swap.sh','deploy_release_rollback.sh','deploy_clear_activation_marker.sh'):
   text=(scripts/name).read_text();self.assertIn('source_policy prepare "$RELEASES_DIR"',text)
  self.assertIn('source_policy binding "$DEPLOY_PATH" "$cwd_resolved" "$EXPECTED_SHA"',(scripts/'deploy_verify_release_tree.sh').read_text())
  self.assertIn('source_policy pin "$CANDIDATE_DIR"',(scripts/'deploy_pin_candidate_sha.sh').read_text())
  for name in ('deploy_release_swap.sh','deploy_pin_candidate_sha.sh','deploy_verify_release_tree.sh'):
   self.assertNotRegex('\n'.join(x for x in (scripts/name).read_text().splitlines() if not x.lstrip().startswith('#')),r'\bgit\s')
  self.assertIn('source_policy candidate "$LIVE_PATH" "$CANDIDATE_DIR"',(scripts/'deploy_release_swap.sh').read_text());self.assertNotIn('echo "$previous_target" >',(scripts/'deploy_release_swap.sh').read_text());self.assertIn('source_policy marker-clear',(scripts/'deploy_clear_activation_marker.sh').read_text())
if __name__=='__main__':
 violations=[]
 def fence(event,args):
  if event.startswith('socket.'):
   violations.append(event);raise AssertionError('network forbidden')
  if event=='subprocess.Popen':
   if args[0]!='/usr/bin/git' or any(x in args[1] for x in ('fetch','push','clone','remote')):
    violations.append(event);raise AssertionError('only local inert Git fixture/metadata commands')
 sys.addaudithook(fence);run=unittest.main(verbosity=2,exit=False)
 print('AUDIT_BOUNDARY_VIOLATIONS',len(violations),file=sys.stderr)
 raise SystemExit(0 if run.result.wasSuccessful() and not violations else 1)
