"""Private retired query data is released without a garbage-collection pass."""
from copy import deepcopy
import gc
import unittest
import weakref
from unittest.mock import patch

from tests.execution_provider import test_rolling_service_lifecycle as lifecycle


class RollingPrivatePayloadTests(unittest.TestCase):
    setup_owner = lifecycle.RollingServiceLifecycleTests.setup_owner

    def test_retired_actual_child_and_stream_hold_no_full_query_payload_with_GC_disabled(self):
        _, owner, source, grant, _ = self.setup_owner()
        stream = owner.evaluate(source, query_id='service-first')
        values = list(stream)
        old_iterator = deepcopy(stream.last_close_report)
        old_chain = owner.owner.last_close_report
        chain_snapshot = deepcopy(old_chain)
        child_ref, inner_ref = weakref.ref(owner.owner), weakref.ref(owner.owner._active)
        enabled = gc.isenabled()
        gc.disable()
        try:
            report = owner.close(timeout=3)
            self.assertTrue(grant.released)
            self.assertIsNone(owner.owner)
            self.assertIsNone(owner._active)
            if child_ref() is not None:
                child = child_ref()
                self.assertFalse(child._raw_receipts)
                self.assertIsNone(child._root_run)
                self.assertIsNone(child._active)
                self.assertNotIn('calls', child.last_close_report)
                self.assertTrue(child._service_payloads_retired_confirmed())
            if inner_ref() is not None:
                inner = inner_ref()
                for name in ('run', 'section', 'error', 'driver', 'pending', '_output', 'source'):
                    self.assertIsNone(getattr(inner, name), name)
                self.assertNotIn('calls', inner.last_close_report)
            self.assertEqual(old_chain, chain_snapshot)
            self.assertEqual(stream.last_close_report, old_iterator)
            self.assertEqual(report['calls'], old_iterator['calls'])
            self.assertEqual({v['call_id'] for v in values}, {v['call_id'] for v in report['calls']})
        finally:
            if enabled:
                gc.enable()
            owner.close(timeout=3)

    def test_payload_retirement_rejects_untransferred_results(self):
        _, owner, source, grant, _ = self.setup_owner()
        stream = owner.evaluate(source, query_id='service-first')
        next(stream)
        child = owner.owner
        original = deepcopy(child.calls)
        try:
            with self.assertRaisesRegex(RuntimeError, 'retire|transfer|certificate'):
                child._service_retire_payloads({}, object())
            self.assertEqual(child.calls, original)
            self.assertFalse(grant.released)
            report = owner.close(timeout=3)
            self.assertTrue(owner.has_unconsumed_results())
            with self.assertRaisesRegex(RuntimeError, 'retire|transfer|certificate'):
                child._service_retire_payloads({}, object())
            self.assertEqual(owner.take_unconsumed_results(expected=report['unconsumed_call_results']),
                             report['unconsumed_call_results'])
            self.assertTrue(grant.released)
        finally:
            owner.close(timeout=3)

    def test_interruption_after_stream_detachment_resumes_the_same_child(self):
        _, owner, source, grant, _ = self.setup_owner()
        values = list(owner.evaluate(source, query_id='service-first'))
        child = owner.owner
        stream = child._active
        original = stream.release_query_payloads
        def interrupted(*args):
            original(*args)
            raise KeyboardInterrupt('CPU interruption after stream detach')
        with patch.object(stream, 'release_query_payloads', side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                owner.close(timeout=3)
        certificate = owner._service_retirement
        self.assertIs(owner.owner, child)
        self.assertFalse(child._service_payloads_retired_confirmed())
        self.assertFalse(grant.released)
        self.assertEqual(owner._retired_report['calls_count'], len(values))
        self.assertTrue(owner.cleanup_confirmed())
        report = owner.close(timeout=3)
        self.assertEqual(report['calls_count'], len(values))
        self.assertIs(owner._service_retirement, certificate)
        self.assertTrue(grant.released)
        self.assertTrue(child._service_payloads_retired_confirmed())
        self.assertIsNone(owner.owner)

    def test_interruption_after_root_detachment_keeps_original_count_and_model_proof(self):
        _, owner, source, grant, _ = self.setup_owner()
        values = list(owner.evaluate(source, query_id='service-first'))
        child = owner.owner
        ready = deepcopy(owner.model_ready)
        def get_marker(actual):
            return actual.__dict__['_service_payloads_retired']
        def interrupt_marker(actual, value):
            if actual is child and value is True:
                raise KeyboardInterrupt('CPU interruption before retired marker')
            actual.__dict__['_service_payloads_retired'] = value
        with patch.object(type(child), '_service_payloads_retired',
                          property(get_marker, interrupt_marker), create=True):
            with self.assertRaises(KeyboardInterrupt):
                owner.close(timeout=3)
        self.assertIsNone(child._root_run)
        self.assertIsNone(child._active)
        self.assertFalse(child._service_payloads_retired_confirmed())
        self.assertFalse(grant.released)
        self.assertTrue(owner.cleanup_confirmed())
        self.assertEqual(child.last_close_report['model_owner']['model_ready'], ready)
        certificate = owner._service_retirement
        report = owner.close(timeout=3)
        self.assertEqual(report['calls_count'], len(values))
        self.assertIs(owner._service_retirement, certificate)
        self.assertTrue(child._service_payloads_retired_confirmed())
        self.assertTrue(grant.released)

    def test_exception_and_caller_reports_keep_original_rows_after_private_retirement(self):
        f, owner, source, grant, _ = self.setup_owner()
        held = next(key for key, value in f.replies.items() if value['text'] == 'False')
        f.replies[held]['status'] = 'unknown'
        f.write_replies()
        stream = owner.evaluate(source, query_id='service-first')
        with self.assertRaises(RuntimeError) as caught:
            list(stream)
        original_error_calls = deepcopy(caught.exception.chain_calls)
        child = owner.owner
        report = owner.close(timeout=3)
        snapshot = deepcopy(report)
        self.assertFalse(grant.released)
        taken = owner.take_unconsumed_results(expected=report['unconsumed_call_results'])
        self.assertEqual(taken, report['calls'])
        self.assertEqual(report, snapshot)
        self.assertEqual(caught.exception.chain_calls, original_error_calls)
        self.assertTrue(grant.released)
        self.assertTrue(child._service_payloads_retired_confirmed())
        self.assertFalse(child._raw_receipts)
        self.assertIsNone(child._root_run)
        self.assertIsNone(child._active)


if __name__ == '__main__':
    unittest.main()
