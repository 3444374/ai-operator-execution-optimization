"""Native transfer evidence must link exact remote IDs, not output-ID suffixes."""
import unittest

from src.experiments.native_stage_handoff import audit_native_transfers


class TransferAuditTests(unittest.TestCase):
    def report(self):
        return dict(arms=dict(split=dict(results=[dict(call_id='logical-row',
            stage_receipts=dict(prefill=[dict(kv_transfer_params=dict(remote_request_id='producer-id'))]))])))

    def log(self):
        return '\n'.join([
            "update_state_after_alloc: num_external_tokens=154, kv_transfer_params={'remote_request_id': 'producer-id'}",
            'start_load_kv for request unrelated-decoder-id from remote engine native-engine',
            'Finished recving KV transfer for request unrelated-decoder-id',
            "request_finished(unrelated-decoder-id), request_status=FINISHED_STOPPED, kv_transfer_params={'remote_request_id': 'producer-id', '_remote_blocks_processed': True}",
        ])

    def test_exact_remote_identity_connects_different_native_identifiers(self):
        result = audit_native_transfers(self.report(), self.log())
        self.assertTrue(result['complete'])
        self.assertEqual(result['transfers'][0]['external_tokens'], 154)

    def test_matching_output_count_cannot_replace_missing_receive_or_foreign_identity(self):
        for log in (self.log().replace('Finished recving', 'Not finished'),
                    self.log().replace('producer-id', 'foreign-id'),
                    self.log() + '\nFinished recving KV transfer for request unrelated-decoder-id'):
            self.assertFalse(audit_native_transfers(self.report(), log)['complete'])


if __name__ == '__main__':
    unittest.main()
