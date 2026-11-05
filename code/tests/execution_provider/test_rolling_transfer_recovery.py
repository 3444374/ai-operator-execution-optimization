"""Return local receipts before optional cleanup can refuse or be interrupted."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from tests.execution_provider import test_rolling_service_owner as original


class RollingTransferRecoveryTests(unittest.TestCase):
    prepare=original.RollingServiceOwnerTests.prepare

    def test_no_snapshot_take_returns_before_fallible_retirement_and_close_can_retry(self):
        for error in (RuntimeError,KeyboardInterrupt):
            with self.subTest(error=error.__name__):
                _,source,pool,owner,declaration=self.prepare()
                first=next(owner.evaluate(source,query_id=declaration.query_id))
                owner.close(timeout=3)
                child=owner.owner
                grant=owner._service_allocation
                hook=grant if error is RuntimeError else child
                name='retire_producer' if error is RuntimeError else '_service_retire_payloads'
                with patch.object(hook,name,side_effect=error('CPU retirement refused')) as retire:
                    taken=owner.take_unconsumed_results()
                    retire.assert_not_called()
                    self.assertEqual(len(taken),1)
                    self.assertNotEqual(taken[0]['call_id'],first['call_id'])
                    self.assertEqual(pool.snapshot()['allocations'],1)
                    saved=deepcopy(taken)
                    with self.assertRaises(error):
                        pool.retry_cleanup(grant.allocation_id,timeout=3)
                    self.assertEqual(taken,saved)
                    self.assertEqual(pool.snapshot()['allocations'],1)
                report=pool.retry_cleanup(grant.allocation_id,timeout=3)
                self.assertEqual(report['service_allocation']['status'],'released')
                self.assertEqual(pool.snapshot()['allocations'],0)
                self.assertEqual(taken,saved)
                self.assertTrue(child._service_payloads_retired_confirmed())

    def test_interrupted_local_take_restores_undelivered_rows(self):
        _,source,pool,owner,declaration=self.prepare()
        first=next(owner.evaluate(source,query_id=declaration.query_id))
        owner.close(timeout=3)
        prior=set(owner._delivered)
        interrupted=[]
        def read(actual):
            return actual.__dict__['_delivered']
        def store(actual,value):
            actual.__dict__['_delivered']=value
            if actual is owner and value!=prior and not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt('CPU interrupted local result transfer')
        with patch.object(type(owner),'_delivered',property(read,store),create=True):
            with self.assertRaises(KeyboardInterrupt):
                owner.take_unconsumed_results()
        self.assertEqual(interrupted,[True])
        self.assertEqual(owner._delivered,prior)
        self.assertTrue(owner.has_unconsumed_results())
        self.assertEqual(pool.snapshot()['allocations'],1)
        taken=owner.take_unconsumed_results()
        self.assertEqual(len(taken),1)
        self.assertNotEqual(taken[0]['call_id'],first['call_id'])
        self.assertEqual(pool.snapshot()['allocations'],1)
        self.assertEqual(owner.close(timeout=3)['service_allocation']['status'],'released')

    def test_original_cleanup_resumes_after_interrupted_last_owner_store(self):
        _,source,pool,owner,declaration=self.prepare()
        values=list(owner.evaluate(source,query_id=declaration.query_id))
        child,active=owner.owner,owner._active
        grant=owner._service_allocation
        interrupted=[]
        def read(actual):
            return actual.__dict__['owner']
        def store(actual,value):
            actual.__dict__['owner']=value
            if actual is owner and value is None and not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt('CPU interrupted original owner detach')
        with patch.object(type(owner),'owner',property(read,store),create=True):
            with self.assertRaises(KeyboardInterrupt):
                owner.close(timeout=3)
        self.assertEqual(interrupted,[True])
        self.assertIsNone(owner.owner)
        self.assertTrue(child._service_payloads_retired_confirmed())
        self.assertEqual(pool.snapshot()['allocations'],1)
        report=pool.retry_cleanup(grant.allocation_id,timeout=3)
        self.assertEqual(report['service_allocation']['status'],'released')
        self.assertIsNone(owner._active)
        self.assertEqual(pool.snapshot()['allocations'],0)
        self.assertEqual(active._report['calls'],values)

    def test_original_producer_remains_available_until_active_iterator_is_detached(self):
        _,source,pool,owner,declaration=self.prepare()
        list(owner.evaluate(source,query_id=declaration.query_id))
        child=owner.owner
        grant=owner._service_allocation
        interrupted=[]
        def read(actual):
            return actual.__dict__['_active']
        def store(actual,value):
            actual.__dict__['_active']=value
            if actual is owner and value is None and not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt('CPU interrupted active iterator detach')
        with patch.object(type(owner),'_active',property(read,store),create=True):
            with self.assertRaises(KeyboardInterrupt):
                owner.close(timeout=3)
        self.assertIs(owner.owner,child)
        self.assertEqual(pool.snapshot()['allocations'],1)
        self.assertEqual(pool.retry_cleanup(grant.allocation_id,timeout=3)['service_allocation']['status'],'released')
        self.assertEqual(pool.snapshot()['allocations'],0)


if __name__=='__main__':
    unittest.main()
