import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments import native_stage_pipeline as replay
from src.semantic_methods.lotus.records import identity


class ReplayEvidenceTest(unittest.TestCase):
    def test_resident_rounds_keep_independent_evidence_and_fail_on_changed_output(self):
        row = dict(identity('test', 'a', 0), messages_json=json.dumps([dict(role='user', content='hello')]))
        expected = replay._prepared([row])[0][0]

        class Owner:
            ready_report = {'engine_uid': 'fixture'}
            calls = 0

            def __init__(self, *args, **kwargs):
                pass

            def run_prepared(self, rows, **kwargs):
                self.calls += 1
                return [dict(expected, status='succeeded', prompt_token_ids_json='[1]',
                    output_token_ids_json='[2]', text='ok', finish_reason='stop',
                    prompt_tokens=1, completion_tokens=1)]

            def close(self, **kwargs):
                return dict(status='closed')

        config = dict(engine=dict(model_path='/unused', model_python='/model/python',
            driver_python='/driver/python'), generation=dict(max_tokens=1), rows=[row],
            arm='colocated', warmup_rounds=1, rounds=2, reserved_physical_calls=3,
            worker_timeout_s=10, startup_timeout_s=1, close_timeout_s=1)
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(replay.EngineSpec, 'validate_driver_environment'), \
                patch.object(replay, 'ReferenceChainEngineProcess', Owner), \
                patch.object(replay.StageEngineCleanup, 'stop', return_value=dict(process_exit_confirmed=True)):
            output = Path(temporary)/'run'
            report = replay.run(config, output)
            self.assertEqual(report['status'], 'passed')
            self.assertEqual(len(list(output.glob('round-*.json'))), 3)
            self.assertEqual([r['warmup'] for r in report['rounds']], [True, False, False])
            changed = dict(Owner().run_prepared([row])[0], text='different')
            self.assertNotEqual(replay.result_identity([changed], [expected]), report['result_sha256'])
            with self.assertRaises(RuntimeError):
                replay.result_identity([dict(changed, call_id='wrong')], [expected])
