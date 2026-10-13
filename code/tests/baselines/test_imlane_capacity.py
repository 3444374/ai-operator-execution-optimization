"""Declared IMLane capacity before author submission; no author, SDK or HTTP."""
from dataclasses import asdict
import unittest

from src.baselines.text.frameworks.imlane_limits import IMLaneBatchLimits, IMLaneCapacityError
from src.baselines.text.frameworks.native_calls import NativeCall, _call_wire


def calls(count):
    return tuple(NativeCall('occurrence:'+str(i), i, b'{"text":"duplicate"}',
                            'http://localhost/fixture', (), 1.) for i in range(count))


class IMLaneCapacityTests(unittest.TestCase):
    def test_total_128_129_and_2000_are_separate_from_pending_128(self):
        limits=IMLaneBatchLimits(max_batch_rows=2000)
        for count in (128,129,2000):
            with self.subTest(count=count):
                observed=limits.inspect_calls(calls(count))
                self.assertEqual(observed['input_rows'],count)
                self.assertEqual(observed['max_pending_calls'],128)
                self.assertFalse(observed['pending_limit_is_execution_concurrency'])

    def test_legacy_128_and_declared_new_total_limit_reject_before_submission(self):
        for limits,count in ((IMLaneBatchLimits(),129),(IMLaneBatchLimits(max_batch_rows=2000),2001)):
            with self.subTest(count=count),self.assertRaises(IMLaneCapacityError) as caught:
                limits.inspect_calls(calls(count))
            self.assertFalse(caught.exception.details['author_submission_started'])
            self.assertEqual(caught.exception.details['actual'],count)
            self.assertEqual(caught.exception.details['maximum'],limits.max_batch_rows)

    def test_payload_and_stdin_wire_bytes_use_distinct_exact_scopes(self):
        values=calls(129)
        facts=IMLaneBatchLimits(max_batch_rows=2000).inspect_calls(values)
        self.assertEqual(facts['payload_bytes'],sum(len(c.payload) for c in values))
        self.assertEqual(facts['stdin_wire_bytes'],sum(len(_call_wire(c).encode())+1 for c in values))
        self.assertGreater(facts['stdin_wire_bytes'],facts['payload_bytes'])

    def test_single_payload_total_payload_and_wire_rejections_have_precise_reason(self):
        for options,reason in ((dict(max_item_payload_bytes=1),'single payload'),
                               (dict(max_input_payload_bytes=1),'total payload'),
                               (dict(max_input_wire_bytes=1),'stdin wire')):
            with self.subTest(options=options),self.assertRaises(IMLaneCapacityError) as caught:
                IMLaneBatchLimits(**options).inspect_calls(calls(1))
            self.assertIn(reason,caught.exception.details['reason'])
            self.assertFalse(caught.exception.details['author_submission_started'])

    def test_cpp_receives_the_same_complete_profile(self):
        limits=IMLaneBatchLimits(max_batch_rows=2000,max_pending_calls=64)
        self.assertEqual(list(map(int,limits.cpp_arguments())),list(asdict(limits).values()))
        self.assertFalse(limits.legacy)
        self.assertTrue(IMLaneBatchLimits().legacy)

    def test_bad_type_duplicate_identity_and_out_of_range_configuration_are_separate(self):
        with self.assertRaisesRegex(ValueError,'non-NativeCall'):
            IMLaneBatchLimits().inspect_calls((object(),))
        item=calls(1)[0]
        with self.assertRaisesRegex(ValueError,'duplicate call identity'):
            IMLaneBatchLimits().inspect_calls((item,item))
        for values in (dict(max_batch_rows=4097),dict(max_pending_calls=129),dict(max_input_payload_bytes=32*1024**2+1)):
            with self.subTest(values=values),self.assertRaises(ValueError):
                IMLaneBatchLimits(**values)


if __name__=='__main__':
    unittest.main()
