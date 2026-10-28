"""Missing/stale history controls, with injected manager and durable-state model."""
import copy
import unittest
if __package__:
    from . import test_api_implementation as impl
    from . import test_api_reconciliation as reconciliation
else:
    import test_api_implementation as impl
    import test_api_reconciliation as reconciliation


class LedgerTests(unittest.TestCase):
    def test_fresh_history_requires_explicit_trusted_never_issued_proof(self):
        for bad in (None, {"state":"UNKNOWN"}, {"state":"NEVER_ISSUED","receipt":{}}):
            b=impl.binding(); k=impl.Kernel(b)
            k.read_service_operation=lambda lease: bad
            with self.assertRaises(impl.txn.Refusal):
                impl.txn.deploy(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)))
            self.assertNotIn("record",k.trace); self.assertNotIn("stage",k.trace)
        b=impl.binding(); k=impl.Kernel(b); k.trusted=False
        with self.assertRaises(impl.txn.Refusal):
            impl.txn.deploy(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)))
        b=impl.binding(); k=impl.Kernel(b)
        impl.txn.deploy(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)))
        self.assertEqual(k.trace[-1],"finish")

    def test_missing_ledger_after_ack_never_verifies_completes_or_releases(self):
        b=impl.binding(); k=impl.Kernel(b)
        def ack(argv,lease,fence,operation):
            k.operation=None; k.trace.append("service"); return "request-accepted"
        with self.assertRaisesRegex(impl.txn.Refusal,"missing/unknown"):
            impl.txn.deploy(b,impl.adapter.Adapter(k,ack))
        self.assertEqual(len(k.operations),1); self.assertIsNone(k.terminal)
        self.assertNotIn("verify-api",k.trace); self.assertNotIn("VERIFIED",k.trace)
        self.assertNotIn("finish",k.trace); self.assertTrue(k.lock.locked())
        self.assertEqual(k.journal["service_expectation"]["operation"],k.operations[0])
        self.assertEqual(k.journal["state"],"API_SERVICE_REQUEST_INTENT")

    def test_missing_after_crash_uses_durable_expected_operation(self):
        b,k,approval=reconciliation.ReconciliationTests().uncertain()
        k.operation=None; before=(k.target,copy.deepcopy(k.record),len(k.operations))
        with self.assertRaisesRegex(impl.txn.Refusal,"missing/unknown"):
            impl.txn.recover(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)),approval)
        self.assertEqual((k.target,k.record,len(k.operations)),before)

    def test_proven_empty_cannot_replace_known_issued_history(self):
        b,k,approval=reconciliation.ReconciliationTests().uncertain()
        empty=impl.Kernel(b).read_service_operation(None)
        k.read_service_operation=lambda lease: empty
        with self.assertRaisesRegex(impl.txn.Refusal,"differs from expected"):
            impl.txn.recover(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)),approval)
        self.assertEqual(len(k.operations),1)

    def test_missing_durable_journal_cannot_fall_back_to_older_empty_record(self):
        b,k,unused=reconciliation.ReconciliationTests().uncertain()
        k.journal=None
        approval=reconciliation.approval(b,k)
        old=copy.deepcopy(k.record)
        with self.assertRaisesRegex(impl.txn.Refusal,"missing durable recovery journal"):
            impl.txn.recover(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)),approval)
        self.assertEqual(k.record,old); self.assertEqual(len(k.operations),1)

    def test_valid_stale_terminal_operation_cannot_complete_new_request(self):
        b=impl.binding(); k=impl.Kernel(b); p=impl.txn.plan(b)
        a=impl.adapter.Adapter(k,impl.AdapterTests().service(k)); p["executor_fence"]=a.claim(p)
        a.restart_api(p)
        old_operation,old_receipt=copy.deepcopy(k.operation),copy.deepcopy(k.terminal)
        def stale(argv,lease,fence,operation):
            k.operation=old_operation; k.terminal=old_receipt; return "request-accepted"
        a.service=stale
        with self.assertRaisesRegex(impl.txn.Refusal,"differs from expected"): a.restart_api(p)
        self.assertNotEqual(k.operations[-1]["operation_id"],old_operation["operation_id"])
        self.assertEqual(k.journal["service_expectation"]["operation"],k.operations[-1])
        self.assertNotIn("finish",k.trace)

    def test_missing_between_api_observation_and_completion_retains_ownership(self):
        b=impl.binding(); k=impl.Kernel(b); p=impl.txn.plan(b)
        a=impl.adapter.Adapter(k,impl.AdapterTests().service(k)); p["executor_fence"]=a.claim(p)
        a.restart_api(p); a.verify_api(p,p["old"])
        k.operation=None; journal=copy.deepcopy(k.journal)
        with self.assertRaises(impl.txn.Refusal): a.complete(p,"VERIFIED")
        self.assertEqual(k.journal,journal); self.assertTrue(k.lock.locked())
        self.assertNotIn("finish",k.trace)

    def test_crash_before_operation_prepare_preserves_expected_intent(self):
        b=impl.binding(); k=impl.Kernel(b)
        def fail(*args): raise OSError("injected prepare persistence failure")
        k.prepare_service_operation_durable=fail
        with self.assertRaises(OSError): impl.txn.deploy(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)))
        self.assertEqual(k.journal["state"],"API_SERVICE_PREPARE_INTENT")
        self.assertEqual(k.journal["service_expectation"]["state"],"ISSUED")
        approval=reconciliation.approval(b,k); k.crash()
        with self.assertRaises(impl.txn.Refusal):
            impl.txn.recover(b,impl.adapter.Adapter(k,impl.AdapterTests().service(k)),approval)
        self.assertEqual(k.operations,[]); self.assertNotIn("service",k.trace)

    def test_expected_plan_and_target_are_required_for_verification_completion(self):
        for wrong in ("plan", "target"):
            b=impl.binding(); k=impl.Kernel(b); p=impl.txn.plan(b)
            a=impl.adapter.Adapter(k,impl.AdapterTests().service(k)); p["executor_fence"]=a.claim(p)
            a.restart_api(p)
            a.service_expectation["operation"][wrong]="foreign"
            with self.assertRaisesRegex(impl.txn.Refusal,"plan/target"):
                a.verify_api(p,p["old"])
            with self.assertRaisesRegex(impl.txn.Refusal,"plan/target"):
                a.complete(p,"VERIFIED")
            self.assertNotIn("finish",k.trace)


if __name__=="__main__": unittest.main()
