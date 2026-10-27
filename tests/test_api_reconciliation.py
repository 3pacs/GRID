"""Focused F1/F2 controls; fake manager/OS only, no native effects."""
import ast
import copy
from pathlib import Path
import stat
import types
import unittest

import test_api_implementation as implementation
from test_api_implementation import Kernel, binding, adapter, txn


def approval(b,k):
    return {"plan_sha256":txn.plan(b)["plan_sha256"], "journal_sha256":txn.digest(k.journal),
            "lock_record_sha256":txn.digest(k.record), "owner_receipt":"f"*64,
            "compatibility_receipt":b["evidence"]["rollback_compatibility"]}


class ReconciliationTests(unittest.TestCase):
    def uncertain(self):
        b=binding(); k=Kernel(b)
        first=adapter.Adapter(k,implementation.AdapterTests().service(k,"unknown"))
        with self.assertRaises(txn.Refusal): txn.deploy(b,first)
        receipt=approval(b,k); k.crash()
        return b,k,receipt

    def test_unknown_request_survives_executor_crash_without_alias_or_second_request(self):
        b,k,receipt=self.uncertain(); target=k.target; record=copy.deepcopy(k.record)
        with self.assertRaisesRegex(txn.Refusal,"unresolved"):
            txn.recover(b,adapter.Adapter(k,implementation.AdapterTests().service(k)),receipt)
        self.assertEqual(k.target,target); self.assertEqual(k.record,record)
        self.assertEqual(k.trace.count("service"),1)
        self.assertEqual(len(k.operations),1)

    def test_pending_missing_mismatched_or_untrusted_receipts_refuse(self):
        for bad in ("pending","identity","manager","effects","untrusted","absent_request"):
            with self.subTest(bad=bad):
                b,k,receipt=self.uncertain(); k.settle()
                if bad=="pending": k.terminal["phase"]="running"
                if bad=="identity": k.terminal["operation_id"]="0"*64
                if bad=="manager": k.terminal["manager_identity_sha256"]="0"*64
                if bad=="effects": k.terminal["no_further_effects"]=False
                if bad=="untrusted": k.trusted=False
                if bad=="absent_request": k.terminal["outcome"]="not-found"
                with self.assertRaises(txn.Refusal): txn.recover(b,adapter.Adapter(k,implementation.AdapterTests().service(k)),receipt)
                self.assertEqual(k.trace.count("service"),1)

    def test_terminal_success_failure_cancelled_allow_separately_bound_rollback(self):
        for outcome in ("succeeded","failed","cancelled"):
            b,k,receipt=self.uncertain(); k.settle(outcome)
            old_id=k.operation["operation_id"]
            txn.recover(b,adapter.Adapter(k,implementation.AdapterTests().service(k)),receipt)
            self.assertEqual(k.target,b["old_release"])
            self.assertEqual(k.trace.count("service"),2)
            self.assertNotEqual(k.operation["operation_id"],old_id)
            self.assertEqual(len(k.operations),2)

    def test_acknowledgement_is_not_terminal(self):
        b=binding(); k=Kernel(b)
        def accepted(argv,lease,fence,operation):
            k.trace.append("service"); return "request-accepted"
        with self.assertRaisesRegex(txn.Refusal,"unresolved"):
            txn.deploy(b,adapter.Adapter(k,accepted))
        self.assertNotIn("verify-api",k.trace); self.assertNotIn("finish",k.trace)

    def test_exception_and_overwritten_transaction_journal_keep_operation_uncertain(self):
        b=binding(); k=Kernel(b)
        def disconnected(*args): raise ConnectionError("manager reply lost")
        with self.assertRaises(ConnectionError): txn.deploy(b,adapter.Adapter(k,disconnected))
        self.assertIsNotNone(k.operation)
        k.journal["state"]="PROMOTED"  # operation ledger is independent of state labels
        receipt=approval(b,k); k.crash()
        with self.assertRaisesRegex(txn.Refusal,"unresolved"):
            txn.recover(b,adapter.Adapter(k,implementation.AdapterTests().service(k)),receipt)
        self.assertEqual(len(k.operations),1)


class FakeOS:
    O_RDONLY=0; O_WRONLY=1; O_RDWR=2; O_TRUNC=512; O_APPEND=1024
    O_CREAT=64; O_EXCL=128; O_NOFOLLOW=131072; O_CLOEXEC=524288
    def __init__(self,nlink=2): self.events=[]; self.nlink=nlink
    def open(self,name,flags,mode,dir_fd):
        self.events.append("OPEN_TRUNCATED_EXISTING" if flags & self.O_TRUNC else "OPEN")
        self.flags=flags; return 7
    def fstat(self,fd):
        self.events.append("FSTAT_REJECT_HARDLINK" if self.nlink==2 else "FSTAT")
        return types.SimpleNamespace(st_mode=0o100600,st_nlink=self.nlink,st_uid=2001,st_dev=9)
    def close(self,fd): self.events.append("CLOSE")


def directory(source, fake):
    definition=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=="Directory")
    namespace={"os":fake,"stat":stat}
    exec(compile(ast.Module(body=[definition],type_ignores=[]),"inert-directory-ast","exec"),namespace)
    obj=object.__new__(namespace["Directory"]); obj.fd=1; obj.uid=2001; obj.device=9
    return obj


class OpenFlagTests(unittest.TestCase):
    def source(self):
        return (Path(__file__).resolve().parents[1]/"scripts/api_isolation_posix.py").read_text(encoding="utf-8")
    def test_destructive_unknown_and_unowned_create_flags_rejected_before_open(self):
        for flags,exclusive,name in ((513,False,"victim"),(1025,False,"victim"),(64,False,"victim"),
                                     (128,False,"victim"),(1<<25,False,"victim"),(1,False,"victim"),
                                     (2,False,"victim"),(0,True,"victim"),(513,True,"victim")):
            fake=FakeOS()
            with self.assertRaises(ValueError): directory(self.source(),fake).regular(name,flags,exclusive)
            self.assertEqual(fake.events,[])
    def test_read_and_existing_lock_still_reject_hardlinks_without_truncation(self):
        for name,flags in (("member",0),("executor.lock",2)):
            fake=FakeOS()
            with self.assertRaises(ValueError): directory(self.source(),fake).regular(name,flags)
            self.assertEqual(fake.events,["OPEN","FSTAT_REJECT_HARDLINK","CLOSE"])
            self.assertTrue(fake.flags & fake.O_NOFOLLOW)
    def test_exclusive_create_adds_safe_flags_and_validates_new_file(self):
        fake=FakeOS(1)
        self.assertEqual(directory(self.source(),fake).regular("new",fake.O_WRONLY,True),7)
        self.assertEqual(fake.events,["OPEN","FSTAT"])
        self.assertEqual(fake.flags,fake.O_WRONLY|fake.O_CREAT|fake.O_EXCL|fake.O_NOFOLLOW|fake.O_CLOEXEC)


if __name__=="__main__": unittest.main()
