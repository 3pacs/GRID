"""FUTURE hosted Linux 3.10/3.11 fixture source; NOT RUN by this author.

Explicit invocation only: python -B native_api_isolation_fixture.py
Creates only a fresh TemporaryDirectory and its own child process; no service,
network, package install, application/DB import, target path or credential access.
Windows execution refuses before any filesystem fixture is created.
"""
import sys
if sys.platform != 'linux':
    raise SystemExit('78: Linux fixture SOURCE only; unsupported host')
import copy
import hashlib
import multiprocessing
import os
from pathlib import Path
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from api_isolation_posix import Directory, Lease, canonical
from api_isolation_native_fs import Tree, ReceivingSink, directory_at
from api_isolation_native_store import AtomicStore, ExecutorStore, DurableBroker
from api_bundle_contract import Refusal, json_hash
from api_isolation_native_kernel import NativeKernel


def pinned(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        s=os.fstat(fd); return Directory(fd,os.getuid(),(s.st_dev,s.st_ino))
    finally: os.close(fd)


def crash_child(path,identity,pipe):
    d=pinned(path); lease=Lease(d,identity)
    pipe.send('locked')
    if pipe.recv()=='exit-owned-child': os._exit(23)
    raise RuntimeError('unexpected fixture instruction')


class FixtureManager:
    """Synthetic manager; never imports/invokes systemd or external commands."""
    def __init__(self,response='unknown'): self.calls=[]; self.response=response
    def verify_never_issued(self,receipt): return receipt.get('fixture') is True
    def identity(self,p): return 'd'*64
    def observe_terminal(self,operation): return None
    def verify_terminal(self,operation,receipt): return False
    def submit_once(self,operation): self.calls.append(copy.deepcopy(operation)); return self.response


class NativeFixtures(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='grid-api-isolation-owned-')
        self.path=Path(self.temp.name); os.chmod(self.path,0o700)
        self.root=pinned(self.path); self.children=[]
        self.control=self.child('control'); self.inbox=self.child('inbox')
        self.control.write_new('executor.lock',b'')
        s=os.stat('executor.lock',dir_fd=self.control.fd,follow_symlinks=False)
        self.lock_identity=(s.st_dev,s.st_ino)
        self.genesis={'state':'NEVER_ISSUED','host':'fixture-host','service':'fixture-only',
                      'namespace':'owned-test','receipt_sha256':'c'*64,'fixture':True}
        self.control.write_new('executor-state.json',canonical({'schema':'grid.api-executor.v1','epoch':0,'active':None}))
        self.control.write_new('service-history.json',canonical({'schema':'grid.api-service-history.v1','genesis':self.genesis,'entries':[]}))
        self.lease=None
    def child(self,name):
        d=directory_at(self.root,name,True); self.children.append(d); return d
    def locked(self):
        self.lease=Lease(self.control,self.lock_identity); return self.lease
    def tearDown(self):
        if self.lease is not None and self.lease.fd is not None:
            self.lease.release_after_durable_terminal()  # fixture owns this lease
        for d in reversed(self.children): os.close(d.fd)
        os.close(self.root.fd)
        # Only the library-owned fresh root is removed, never a supplied path.
        self.temp.cleanup()

    def test_receive_publish_and_readback(self):
        lease=self.locked(); sink=ReceivingSink(self.inbox,lease)
        token=sink.begin_exclusive('a'*40,'b'*64)
        sink.write_exclusive(token,'source/api/main.py','100644',b'fixture\n')
        sink.write_exclusive(token,'runtime/bin/python3.10','100755',b'NOT_AN_INTERPRETER')
        inventory=sink.inventory(token)
        self.assertEqual(inventory['source/api/main.py']['sha256'],hashlib.sha256(b'fixture\n').hexdigest())
        sink.publish_no_replace(token,'b'*64)
        published=directory_at(self.inbox,'a'*40)
        try: self.assertEqual(Tree(published).inventory(),inventory)
        finally: os.close(published.fd)
        with self.assertRaises(FileExistsError): sink.begin_exclusive('a'*40,'b'*64)

    def test_hardlink_symlink_and_protected_inode_rejected_without_mutation(self):
        tree=self.child('tree'); t=Tree(tree)
        t.write_exclusive('regular','100644',b'keep')
        os.link('regular','alias',src_dir_fd=tree.fd,dst_dir_fd=tree.fd)
        with self.assertRaises(ValueError): t.inventory()
        self.assertEqual((self.path/'tree/regular').read_bytes(),b'keep')
        os.unlink('alias',dir_fd=tree.fd)
        s=os.stat('regular',dir_fd=tree.fd,follow_symlinks=False)
        with self.assertRaises(Refusal): Tree(tree,{(s.st_dev,s.st_ino)}).inventory()
        os.symlink('regular','link',dir_fd=tree.fd)
        with self.assertRaises(Refusal): t.inventory()

    def test_directory_link_escape_and_preexisting_publish_collision_refuse(self):
        lease=self.locked(); sink=ReceivingSink(self.inbox,lease)
        token=sink.begin_exclusive('a'*40,'b'*64)
        payload=sink.sessions[token]['payload']
        os.symlink(str(self.path/'control'),'source',dir_fd=payload.fd)
        with self.assertRaises(OSError): sink.write_exclusive(token,'source/escape','100644',b'bad')
        self.assertFalse((self.path/'control/escape').exists())
        token2=sink.begin_exclusive('c'*40,'b'*64)
        sink.write_exclusive(token2,'source/x','100644',b'x'); sink.inventory(token2)
        os.mkdir('c'*40,0o700,dir_fd=self.inbox.fd)
        with self.assertRaises(OSError): sink.publish_no_replace(token2,'b'*64)
        self.assertEqual(os.listdir(self.path/'inbox'/('c'*40)),[])

    def test_executor_epoch_cas_and_terminal_release(self):
        store=ExecutorStore(self.control,self.lock_identity); lease=self.locked()
        record={'plan':'a'*64,'epoch':1,'nonce':'fixture','fence':'b'*64}
        store.cas_record_durable(lease,None,record)
        with self.assertRaises(Refusal): store.cas_record_durable(lease,None,record)
        store.write_journal_durable(lease,'b'*64,{'plan':'a'*64,'fence':'b'*64,'state':'FIXTURE_TERMINAL'})
        store.finish_durable(lease,'b'*64,'FIXTURE_TERMINAL')
        self.assertIsNone(store.state()['active']); self.assertEqual(store.state()['epoch'],1)
        self.lease=Lease(self.control,self.lock_identity)
        store.cas_record_durable(self.lease,None,{**record,'epoch':2,'nonce':'new'})
        self.assertEqual(store.state()['epoch'],2)

    def test_own_child_crash_releases_same_lock_inode_without_stealing(self):
        context=multiprocessing.get_context('spawn'); parent,child=context.Pipe()
        process=context.Process(target=crash_child,args=(str(self.path/'control'),self.lock_identity,child))
        process.start()
        try:
            self.assertTrue(parent.poll(5)); self.assertEqual(parent.recv(),'locked')
            with self.assertRaises(BlockingIOError): Lease(self.control,self.lock_identity)
            parent.send('exit-owned-child'); process.join(5)
            self.assertEqual(process.exitcode,23)
            self.locked()
            s=os.stat('executor.lock',dir_fd=self.control.fd,follow_symlinks=False)
            self.assertEqual((s.st_dev,s.st_ino),self.lock_identity)
        finally:
            if process.is_alive(): process.terminate(); process.join(5)  # this exact owned child only
            parent.close(); child.close()

    def test_uncertain_broker_dispatch_persists_and_cannot_resend(self):
        lease=self.locked(); manager=FixtureManager(); broker=DurableBroker(AtomicStore(self.control),manager)
        operation={'plan':'a'*64,'fence':'b'*64,'host':'fixture-host','service':'grid-api.service',
                   'target':'/synthetic/fixture','nonce':'one','manager_identity_sha256':'d'*64}
        operation['operation_id']=json_hash(operation)
        broker.prepare(lease,operation,None)
        with self.assertRaises(Refusal): broker.dispatch(('restart','grid-api.service'),lease,'b'*64,operation)
        reloaded=DurableBroker(AtomicStore(self.control),manager)
        self.assertEqual(reloaded.history()['entries'][-1]['phase'],'DISPATCH_UNCERTAIN')
        with self.assertRaises(Refusal): reloaded.dispatch(('restart','grid-api.service'),lease,'b'*64,operation)
        self.assertEqual(len(manager.calls),1)
        self.assertIsNone(reloaded.observe_terminal_service_operation(lease,operation))

    def test_default_manager_refuses_and_record_name_cannot_replace_lock(self):
        lease=self.locked(); store=AtomicStore(self.control); broker=DurableBroker(store)
        with self.assertRaises(RuntimeError): broker.service_manager_identity({})
        with self.assertRaises(Refusal): store.cas('executor.lock',None,{},lease)

    def test_alias_expected_target_and_fsync_transition(self):
        lease=self.locked(); old='/synthetic/old'; new='/synthetic/new'
        os.symlink(old,'current',dir_fd=self.root.fd)
        with self.assertRaises(ValueError): self.root.replace_alias(new,old,(old,new),lease)
        self.assertEqual(os.readlink('current',dir_fd=self.root.fd),old)
        self.root.replace_alias(old,new,(old,new),lease)
        self.assertEqual(os.readlink('current',dir_fd=self.root.fd),new)


    def test_native_kernel_copies_independent_verified_bytes_without_held_mutation(self):
        from test_api_implementation import artifact  # synthetic source/runtime bytes only
        from api_isolation_artifact import receive
        manifest,raw,admission,unused=artifact()
        admission['expected']['bundle_uid']=os.getuid()
        files=receive(manifest,raw,admission)
        lease=self.locked(); sink=ReceivingSink(self.inbox,lease)
        token=sink.begin_exclusive(manifest['commit'],json_hash(manifest))
        for name,(mode,data) in files.items(): sink.write_exclusive(token,name,mode,data)
        sink.inventory(token); sink.publish_no_replace(token,json_hash(manifest))
        releases=self.child('releases'); held=self.child('held')
        Tree(held).write_exclusive('unchanged','100644',b'HELD_FIXTURE_BYTES')
        class FixtureAuthority:
            def verify_adoption(self,*args): return True
            def verify_baseline_and_evidence(self,*args): return True
        kernel=NativeKernel(self.control,self.lock_identity,self.root,self.inbox,releases,
                            manifest,admission['expected'],set(),FixtureAuthority())
        plan={'plan_sha256':'a'*64,'binding':{'candidate_commit':manifest['commit'],
              'candidate_tree':manifest['tree'],'bundle_manifest':json_hash(manifest)}}
        kernel.cas_record_durable(lease,None,{'plan':'a'*64,'epoch':1,'nonce':'fixture','fence':'b'*64})
        kernel.verify_bound_receipts_and_topology(plan)
        kernel.copy_bundle_exclusive(lease,'b'*64,plan); kernel.verify_staged_descriptors(plan)
        source=directory_at(self.inbox,manifest['commit']); dest=directory_at(releases,manifest['commit'])
        try:
            a=Tree(source).observations(); b=Tree(dest).observations()
            self.assertEqual({n:v['content'] for n,v in a.items()},{n:v['content'] for n,v in b.items()})
            self.assertFalse({(v['device'],v['inode']) for v in a.values()} & {(v['device'],v['inode']) for v in b.values()})
        finally: os.close(source.fd); os.close(dest.fd)
        self.assertEqual((self.path/'held/unchanged').read_bytes(),b'HELD_FIXTURE_BYTES')


if __name__=='__main__': unittest.main()
