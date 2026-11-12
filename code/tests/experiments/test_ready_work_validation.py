"""Cost provenance and real Core acceptance, using CPU responses only."""
from dataclasses import asdict
import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch

from src.experiments import ready_work_validation as ready
from src.experiments import record_chain_validation as checks
from src.experiments import record_chain_validation_run as entry
from src.semantic_methods.driver import ReadyRequest
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.experiments import test_record_chain_validation as fixtures


class ProfileTests(unittest.TestCase):
    setUp = fixtures.ConfigTests.setUp
    save = fixtures.ConfigTests.save

    def profile(self):
        config, engine, resources, generation = entry.configuration(self.path)
        case = dict(public_git_commit=config['public_git_commit'], input_sha256=config['input']['sha256'],
            engine=asdict(engine), resources=asdict(resources), generation=asdict(generation),
            status='completed', actual_calls=48, close=dict(status='closed', query_completed=True,
            model_owner=dict(model_ready=dict(native_execution_profile=dict(
                batch_invariant_requested=True, batch_invariant_active=True)))),
            calls=[dict(algorithm_stage=stage, model_elapsed_s=(i+1)/100) for i,stage in enumerate(ready.STAGES) for _ in range(16)])
        return ready.calibrate(case, source_sha256='f'*64)

    def test_calibration_and_profile_reject_changed_source_or_costs(self):
        profile = self.profile()
        self.assertEqual(profile['service_ns'], dict(map0=10000000, map1=20000000, keep=30000000))
        self.config.update(suite='ready-work', ray_roots={name:str(self.root/name) for name in entry.READY_WORK_CASES})
        path = self.root/'cost.json'; path.write_text(json.dumps(profile))
        self.config['ready_work_profile'] = dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        self.save(); self.assertEqual(len(entry.configuration(self.path)[0]['cases']), 4)
        self.config['resources']['query_timeout_s'] += 1; self.save()
        with self.assertRaisesRegex(ValueError, 'signature'): entry.configuration(self.path)
        path.write_text('{}')
        with self.assertRaisesRegex(ValueError, 'changed'): entry.configuration(self.path)

    def test_contended_selection_is_only_counted_after_matching_Core_acceptance(self):
        profile = self.profile(); selector = ready.ObservedReadySelector('ready-shortest',profile)
        candidates=(ReadyRequest(1,'a',2,1,1,'keep','model_calls'),ReadyRequest(2,'b',0,1,1,'map0','model_calls'))
        self.assertEqual(selector(candidates,1.1), (2,'shortest_work'))
        self.assertEqual(selector.report()['changed_accepted'],0)
        event=type('Event',(),dict(action='Core_offer_accepted',row=type('Row',(),dict(call_id='b'))(),stage_index=0,reason='shortest_work'))()
        selector.observe_event(event)
        self.assertEqual((selector.report()['contended_accepted'],selector.report()['changed_accepted']),(1,1))


class CoreBridgeTests(unittest.TestCase):
    def test_four_selectors_preserve_original_method_answers_and_count_actual_acceptance(self):
        from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
        from tests.execution_provider.test_lotus_record_local_tokens import QueryTokenTests
        from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
        from tests.execution_provider.test_lotus_record_local_query import Process
        from tests.semantic_methods.test_lotus_chain_composition import source_rows
        fixture=QueryTokenTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        chain=checks.make_chain('openai/Qwen2.5-7B-Instruct',GenerationSpec(max_tokens=8))
        records=source_rows(column='text',size=16); fixture.responses(chain,records)
        with tempfile.TemporaryDirectory() as name: resources=entry.make_resources(fixtures.config_for(name))
        profile=dict(service_ns=dict(map0=30,map1=10,keep=20),source_case_sha256='CPU-fixture',max_wait_s=1.0)
        baseline=None
        for name in entry.READY_WORK_CASES:
            selector=ready.ObservedReadySelector(name,profile)
            with self.subTest(case=name), ThreadedRuntime([]), patch.object(AsyncCoreModelActor,'_process_type',return_value=Process):
                actors=AsyncRecordsActorOwner.for_prepared(fixture.model,chain.stages[0].generation,resources)
                result=checks.run_case(records,chain,fixture.model,resources,control_dir=None,
                    actor_owner=actors,ready_selector=selector,on_ready_event=selector.observe_event)
                self.assertEqual(checks.replay_native_answers(records,chain,result)['recorded_answers_replayed'],48)
                self.assertEqual(selector.report()['accepted_choices'],48)
                outputs=[r.describe() for r in result['records']]
                if baseline is None: baseline=outputs
                self.assertEqual(outputs,baseline)
