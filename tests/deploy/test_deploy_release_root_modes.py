"""Bounded real Linux scripts/coreutils; synthetic temp trees and inert hooks only."""
import os,stat,subprocess,tempfile,unittest
from pathlib import Path
P=Path(__file__).resolve().parents[2]
class RootModeTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='tmp.',dir='/tmp');self.root=Path(self.temp.name);self.live=self.root/'live';self.releases=Path(str(self.live)+'.releases');self.releases.mkdir();self.old=self.releases/'old';self.old.mkdir();self.old.chmod(0o775);(self.old/'ordinary.txt').write_text('synthetic-original');self.live.symlink_to(self.old,target_is_directory=True)
  self.bin=self.root/'bin';self.bin.mkdir()
  for name in ('ssh','curl','wget','git','pip','npm','systemctl','journalctl','python3'):
   f=self.bin/name;f.write_text('#!/bin/sh\necho forbidden-external-tool >&2\nexit 97\n');f.chmod(0o755)
  self.env={'PATH':str(self.bin)+':/usr/bin:/bin','LANG':'C','GRID_DEPLOY_TEST_SANDBOX':str(self.root),'DEPLOY_LOCK_WAIT_SECS':'1'}
 def tearDown(self):self.temp.cleanup()
 def hook(self,body='printf built > "$1/new.txt"'):
  f=self.root/'hook.sh';f.write_text('#!/bin/bash\nset -euo pipefail\n'+body+'\n');f.chmod(0o755);return f
 def run_swap(self,version='candidate',label='new',body=None,umask=0o022):
  hook=self.hook(body) if body is not None else self.hook()
  return subprocess.run(['/bin/bash',str(P/'scripts/deploy_release_swap.sh'),str(self.live),label,str(hook)],env=self.env,capture_output=True,text=True,timeout=8,umask=umask)
 def assert_code(self,result,code):
  self.assertEqual(result.returncode,code,result.stderr[-3000:]);self.assertNotIn('forbidden-external-tool',result.stderr)
 def mode(self,path):return stat.S_IMODE(path.stat().st_mode)
 def test_green_copied_root_protected_runtime_data_preserved(self):
  self.old.chmod(0o755);data=self.old/'uploads';data.mkdir();data.chmod(0o777);(data/'data.txt').write_text('synthetic-state');(self.old/'.hidden').write_text('hidden-state');outside=self.root/'outside';outside.mkdir();(outside/'sentinel').write_text('unchanged');(self.old/'external-link').symlink_to(outside,target_is_directory=True)
  result=self.run_swap();self.assert_code(result,0);candidate=self.releases/'new';self.assertEqual(self.mode(candidate),0o755);self.assertEqual(self.mode(self.old),0o755);self.assertEqual(self.mode(candidate/'uploads'),0o777);self.assertEqual((candidate/'uploads/data.txt').read_text(),'synthetic-state');self.assertEqual((candidate/'.hidden').read_text(),'hidden-state');self.assertTrue((candidate/'external-link').is_symlink());self.assertEqual((outside/'sentinel').read_text(),'unchanged')
 def test_fresh_umask_002_tightened_for_new_root_only(self):
  self.live.unlink();result=self.run_swap(umask=0o002,body='mkdir "$1/runtime-data"');self.assert_code(result,0);self.assertEqual(self.mode(self.releases/'new'),0o755);self.assertEqual(self.mode(self.releases/'new/runtime-data'),0o775)
 def test_valid_755_and_file_execute_modes_preserved(self):
  self.old.chmod(0o755);f=self.old/'tool';f.write_text('inert');f.chmod(0o751);result=self.run_swap();self.assert_code(result,0);self.assertEqual(self.mode(self.releases/'new'),0o755);self.assertEqual(self.mode(self.releases/'new/tool'),0o751)
 def refuse_root_mode(self,mode):
  self.old.chmod(0o755);marker=self.releases/'.previous-release';marker.write_text('protected-previous');activation=self.releases/'.activation-in-progress';activation.write_text('protected-activation')
  result=self.run_swap(body='chmod '+mode+' "$1"');self.assert_code(result,4);self.assertEqual(self.live.resolve(),self.old);self.assertEqual(marker.read_text(),'protected-previous');self.assertEqual(activation.read_text(),'protected-activation');self.assertEqual((self.old/'ordinary.txt').read_text(),'synthetic-original');self.assertIn('migration may already have committed',result.stderr)
 def test_hook_group_write_refused_before_swap(self):self.refuse_root_mode('775')
 def test_hook_other_write_refused_before_swap(self):self.refuse_root_mode('757')
 def test_failed_hook_keeps_live_and_markers(self):
  self.old.chmod(0o755);marker=self.releases/'.previous-release';marker.write_text('protected');result=self.run_swap(body='printf failed > "$1/new.txt"; exit 9');self.assert_code(result,1);self.assertEqual(self.live.resolve(),self.old);self.assertEqual(marker.read_text(),'protected');self.assertEqual(self.mode(self.releases/'new'),0o755)
 def test_hook_symlink_root_escape_refused(self):
  self.old.chmod(0o755);outside=self.root/'outside';outside.mkdir();(outside/'sentinel').write_text('unchanged');result=self.run_swap(body='mv "$1" "$1.moved"; ln -s "'+str(outside)+'" "$1"');self.assert_code(result,4);self.assertEqual(self.live.resolve(),self.old);self.assertEqual((outside/'sentinel').read_text(),'unchanged');self.assertTrue((self.releases/'new').is_symlink())
 def test_wrong_owner_metadata_refuses_before_hook(self):
  f=self.bin/'stat';f.write_text('#!/bin/bash\nif [[ "$*" == *"%u %a"* ]]; then echo "2147483646 755"; else exec /usr/bin/stat "$@"; fi\n');f.chmod(0o755);result=self.run_swap();self.assert_code(result,5);self.assertEqual(self.live.resolve(),self.old);self.assertFalse((self.releases/'new/new.txt').exists())
 def test_consecutive_deploy_and_compatible_rollback(self):
  self.old.chmod(0o755);self.assert_code(self.run_swap(label='hold'),0);hold=self.releases/'hold';self.assert_code(self.run_swap(label='forward'),0);self.assertEqual((self.releases/'.previous-release').read_text().strip(),str(hold));self.assertEqual(self.mode(hold),0o755);self.assertEqual(self.mode(self.releases/'forward'),0o755)
  (self.releases/'.activation-in-progress').unlink();result=subprocess.run(['/bin/bash',str(P/'scripts/deploy_release_rollback.sh'),str(self.live),'--schema-compatible'],env=self.env,capture_output=True,text=True,timeout=8);self.assert_code(result,0);self.assertEqual(self.live.resolve(),hold)
 def test_unsafe_legacy_rollback_root_refused_without_mutation(self):
  self.old.chmod(0o755);self.assert_code(self.run_swap(),0);self.old.chmod(0o775);(self.releases/'.activation-in-progress').unlink();result=subprocess.run(['/bin/bash',str(P/'scripts/deploy_release_rollback.sh'),str(self.live),'--schema-compatible'],env=self.env,capture_output=True,text=True,timeout=8);self.assert_code(result,5);self.assertEqual(self.live.resolve(),self.releases/'new');self.assertEqual(self.mode(self.old),0o775)
 def set_acl_or_skip(self,path,spec):
  if not Path('/usr/bin/setfacl').exists() or not Path('/usr/bin/getfacl').exists():self.skipTest('local POSIX ACL utilities unavailable; no installation')
  result=subprocess.run(['/usr/bin/setfacl','-m',spec,str(path)],capture_output=True,text=True,timeout=2)
  if result.returncode!=0:self.skipTest('local ACL fixture rejected: '+result.stderr.replace(str(path),'<fixture>'))
 def acl(self,path):
  result=subprocess.run(['/usr/bin/getfacl','-c','-n',str(path)],capture_output=True,text=True,timeout=2);self.assertEqual(result.returncode,0,result.stderr);return result.stdout
 def test_copied_explicit_source_acl_preserved(self):
  self.old.chmod(0o755)
  self.set_acl_or_skip(self.old,'u:2001:r-x,m::r-x,d:u::rwx,d:u:2001:rwx,d:g::r-x,d:m::rwx,d:o::r-x')
  before=self.acl(self.old);self.assertIn('user:2001:r-x',before)
  self.assert_code(self.run_swap(),0);self.assertEqual(self.acl(self.releases/'new'),before);self.assertEqual(self.acl(self.old),before)
 def test_fresh_default_acl_safe_or_prebuild_refusal(self):
  self.set_acl_or_skip(self.releases,'d:u::rwx,d:u:2001:rwx,d:g::r-x,d:m::rwx,d:o::r-x')
  self.live.unlink();result=self.run_swap(umask=0o002);candidate=self.releases/'new';unsafe=bool(self.mode(candidate)&0o022)
  if unsafe:
   self.assert_code(result,5);self.assertFalse((candidate/'new.txt').exists());self.assertFalse(self.live.exists());self.assertFalse((self.releases/'.activation-in-progress').exists())
  else:
   self.assert_code(result,0);self.assertEqual(self.live.resolve(),candidate);self.assertEqual((candidate/'new.txt').read_text(),'built')
 def test_rollback_symlink_or_outside_target_refused(self):
  self.old.chmod(0o755);self.assert_code(self.run_swap(),0);(self.releases/'.activation-in-progress').unlink();outside=self.root/'outside';outside.mkdir();outside.chmod(0o755);link=self.releases/'linked';link.symlink_to(outside,target_is_directory=True)
  for target in (link,outside,self.releases/'..'/'outside'):
   with self.subTest(target=str(target)):
    (self.releases/'.previous-release').write_text(str(target))
    result=subprocess.run(['/bin/bash',str(P/'scripts/deploy_release_rollback.sh'),str(self.live),'--schema-compatible'],env=self.env,capture_output=True,text=True,timeout=8);self.assert_code(result,5);self.assertEqual(self.live.resolve(),self.releases/'new')
 def test_unsafe_copied_root_refuses_without_normalization(self):
  result=self.run_swap();self.assert_code(result,5);self.assertEqual(self.live.resolve(),self.old);self.assertEqual(self.mode(self.old),0o775);self.assertEqual(self.mode(self.releases/'new'),0o775);self.assertFalse((self.releases/'new/new.txt').exists())
 def test_private_0700_copy_and_fresh_umask_preserved(self):
  self.old.chmod(0o700);self.assert_code(self.run_swap(),0);self.assertEqual(self.mode(self.releases/'new'),0o700)
  self.live.unlink();self.assert_code(self.run_swap(label='private-fresh',umask=0o077),0);self.assertEqual(self.mode(self.releases/'private-fresh'),0o700)
 def rollback_dot_case(self,leaf):
  self.old.chmod(0o755);self.root.chmod(0o755);self.releases.chmod(0o755);self.assert_code(self.run_swap(),0);(self.releases/'.activation-in-progress').unlink()
  marker=self.releases/'.previous-release';raw=str(self.releases)+'/'+leaf;marker.write_text(raw);before=marker.read_bytes();live_before=os.readlink(self.live)
  result=subprocess.run(['/bin/bash',str(P/'scripts/deploy_release_rollback.sh'),str(self.live),'--schema-compatible'],env=self.env,capture_output=True,text=True,timeout=8)
  self.assert_code(result,5);self.assertEqual(os.readlink(self.live),live_before);self.assertEqual(marker.read_bytes(),before)
 def test_rollback_literal_parent_dotdot_refused(self):self.rollback_dot_case('..')
 def test_rollback_literal_current_dot_refused(self):self.rollback_dot_case('.')
 def test_rollback_trailing_slash_symlink_refused(self):
  self.old.chmod(0o755);self.assert_code(self.run_swap(),0);(self.releases/'.activation-in-progress').unlink();outside=self.root/'outside';outside.mkdir();outside.chmod(0o755);link=self.releases/'linked';link.symlink_to(outside,target_is_directory=True)
  marker=self.releases/'.previous-release';raw=str(link)+'/';marker.write_text(raw);before=marker.read_bytes();live_before=os.readlink(self.live)
  result=subprocess.run(['/bin/bash',str(P/'scripts/deploy_release_rollback.sh'),str(self.live),'--schema-compatible'],env=self.env,capture_output=True,text=True,timeout=8)
  self.assert_code(result,5);self.assertEqual(os.readlink(self.live),live_before);self.assertEqual(marker.read_bytes(),before)
if __name__=='__main__':unittest.main(verbosity=2)
