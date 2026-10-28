"""Private-config and captured-receipt fixtures; never start Ray or native models."""
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.experiments import record_chain_validation as checks
from src.experiments import record_chain_validation_run as entry
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


def config_for(root):
    root = Path(root)
    csv = root / 'input.csv'
    csv.write_text('reviewId,reviewText\n' + ''.join(f'{i},synthetic review {i}\n' for i in range(16)))
    return dict(public_git_commit='a' * 40, model_name='openai/Qwen2.5-7B-Instruct',
        run_id='fixture-only', artifact_root=str(root / 'artifacts'), execution_enabled=False,
        engine=asdict(EngineSpec(str(root / 'model'), sys.executable, sys.executable,
            tokenizer_path=str(root / 'tokenizer'), max_num_seqs=4, max_model_calls=48)),
        generation=asdict(GenerationSpec(max_tokens=8)),
        resources=dict(cpu_workers=1, gpu_workers=1, prepare_batch_rows=1, model_batch_rows=1,
                       max_active_batches=4, startup_timeout_s=180, query_timeout_s=180),
        input=dict(existing_csv=str(csv), sha256=hashlib.sha256(csv.read_bytes()).hexdigest()),
        ray_roots={name:str(root / ('ray-' + name)) for name, _, _ in entry.CASES})


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='record-validation-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = config_for(self.root)
        self.path = self.root / 'config.json'
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.config))

    def test_actual_JSON_constructs_C4_engine_call_cap_and_all_four_cases(self):
        config, engine, resources, generation = entry.configuration(self.path)
        self.assertEqual(resources.max_active_batches, 4)
        self.assertEqual(engine.max_num_seqs, 4)
        self.assertEqual(engine.max_model_calls, 48)
        self.assertEqual(generation.max_tokens, 8)
        self.assertEqual(len(config['cases']), 4)
        self.assertEqual(len(entry.inputs(config)), 16)

    def test_metadata_key_and_missing_C_are_rejected_before_qualification(self):
        for error in ('extra', 'missing'):
            with self.subTest(error=error), patch.object(entry, 'objects') as objects:
                self.config = config_for(self.root)
                if error == 'extra': self.config['resources']['engine_capacity_is_not_actual_inflight'] = True
                else: self.config['resources'].pop('max_active_batches')
                self.save()
                with self.assertRaises((TypeError, ValueError)):
                    entry.qualify(self.path, self.root / 'ready.json')
                objects.assert_not_called()
                self.assertFalse((self.root / 'ready.json').exists())

    def test_changed_input_and_missing_records_fail(self):
        config, _, _, _ = entry.configuration(self.path)
        csv = Path(config['input']['existing_csv'])
        csv.write_text('reviewId,reviewText\n1,synthetic\n')
        with self.assertRaisesRegex(ValueError, 'changed'): entry.inputs(config)
        config['input']['sha256'] = hashlib.sha256(csv.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, 'first16'): entry.inputs(config)

    def test_check_never_loads_tokenizer_or_claims(self):
        with patch.object(entry, 'checkout_identity', return_value={'commit':'a'*40}), \
                patch.object(entry, 'objects') as objects:
            report = entry.check(self.path)
        self.assertEqual((report['models'], report['claims'], report['max_requests']), (0, 0, 192))
        objects.assert_not_called()

    def test_qualified_resource_snapshot_and_config_hash_are_enforced(self):
        source = {'commit':'a'*40, 'tree':'b'*40}
        ready = dict(status='passed_CPU_only_host_lease_pending', source=source, token_budget_required=True,
            config_sha256=hashlib.sha256(self.path.read_bytes()).hexdigest(),
            resources=asdict(entry.make_resources(self.config)))
        path = self.root / 'ready.json';path.write_text(json.dumps(ready))
        with patch.object(entry, 'checkout_identity', return_value=source):
            entry.qualified(self.path, path)
            ready['resources']['max_active_batches'] = 2;path.write_text(json.dumps(ready))
            with self.assertRaisesRegex(ValueError, 'changed'): entry.qualified(self.path, path)
            self.config['execution_enabled'] = True;self.save()
            with self.assertRaisesRegex(ValueError, 'changed'): entry.qualified(self.path, path)

    def test_worker_requires_explicit_execute_and_ready_before_model_action(self):
        with patch.object(entry, 'execute_worker') as worker, self.assertRaises(SystemExit):
            entry.main(['worker', '--config', str(self.path), '--case', 'fifo-count'])
        worker.assert_not_called()

    def test_disabled_case_stops_before_supervisor_and_lease(self):
        engine = EngineSpec.from_dict(self.config['engine'])
        with patch.object(entry, 'qualified', return_value=(self.config,engine,None,None,None)), \
                patch('integrations.imlane_records.chain_runtime.check_external_lease') as lease, \
                patch('src.experiments.lotus_records_supervisor.supervise') as supervisor:
            with self.assertRaisesRegex(ValueError, 'enabled'):
                entry.supervised_case(self.path, self.root / 'ready.json', 'fifo-count')
        lease.assert_not_called()
        supervisor.assert_not_called()

    def test_case_reuses_existing_supervisor_and_public_Git_entry(self):
        self.config['execution_enabled'] = True
        self.config['external_owner'] = {'fixture':'lease mock'}
        engine = EngineSpec.from_dict(self.config['engine'])
        with patch.object(entry, 'qualified', return_value=(self.config,engine,None,None,None)), \
                patch('integrations.imlane_records.chain_runtime.check_external_lease') as lease, \
                patch('src.experiments.lotus_records_supervisor.supervise', return_value={'status':'passed'}) as supervisor:
            entry.supervised_case(self.path, self.root / 'ready.json', 'fifo-count')
        lease.assert_called_once()
        command = supervisor.call_args.args[0]
        self.assertEqual(command[2], str(entry.ENTRY))
        self.assertIn('--execute-model', command)
        self.assertEqual(supervisor.call_args.kwargs['timeout_s'], 360)
        self.assertEqual(supervisor.call_args.kwargs['grace_s'], 90)


class BudgetRuleTests(unittest.TestCase):
    vector = [74,105,81,65,96,98,92,70,124,78,130,96,91,84,78,89]

    def test_recorded_real_vector_and_permutation_choose134_without_hardcoding(self):
        self.assertEqual(checks.initial_window(self.vector),134)
        self.assertEqual(checks.initial_window(list(reversed(self.vector))),134)
        self.assertEqual(checks.initial_window([3,3,5]),5)

    def test_raw_source_first_pair_is_not_the_minimum_pair(self):
        self.assertEqual(sum(self.vector[:2])-1,178)
        self.assertEqual(checks.initial_window(self.vector),134)
        self.assertLess(2*min(self.vector)-1,max(self.vector))
        self.assertTrue(all(self.vector[a]+self.vector[b]>134 for a in range(16) for b in range(a+1,16)))

    def test_insufficient_and_infeasible_vectors_stop_without_retuning(self):
        for costs in [[],[65],[1,2,3],[2,3,10]]:
            with self.subTest(costs=costs),self.assertRaisesRegex(ValueError,'no feasible'):
                checks.initial_window(costs)


class QualificationModeTests(unittest.TestCase):
    setUp = ConfigTests.setUp
    save = ConfigTests.save
    def test_count_objects_keep_tokenizer_and_inputs_but_never_construct_W(self):
        engine=EngineSpec.from_dict(self.config['engine'])
        generation=GenerationSpec.from_dict(self.config['generation'])
        token=object()
        with patch('transformers.AutoTokenizer.from_pretrained',return_value=token), \
                patch.object(entry,'make_budget',side_effect=AssertionError('count must not construct W')) as estimate:
            actual,records,chain,budget,W=entry.objects(self.config,engine,generation,require_token_budget=False)
        self.assertIs(actual,token);self.assertEqual(len(records),16)
        self.assertIsNone(budget);self.assertIsNone(W);estimate.assert_not_called()

    def test_count_qualifier_binds_false_scope_and_actual_C4(self):
        with patch.object(entry,'checkout_identity',return_value={'commit':'a'*40}), \
                patch.object(entry,'objects',return_value=(None,None,None,None,None)) as objects, \
                patch.object(entry.metadata,'version',side_effect=lambda name:'2.56.1' if name=='ray' else 'fixture'):
            report=entry.qualify(self.path,self.root/'count-ready.json',require_token_budget=False)
        self.assertIs(report['token_budget_required'],False)
        self.assertIsNone(report['W']);self.assertIsNone(report['token_estimator_signature'])
        self.assertEqual(report['resources']['max_active_batches'],4)
        self.assertEqual(objects.call_args.kwargs,{'require_token_budget':False})
        self.assertEqual((report['models'],report['claims']),(0,0))

    def test_token_qualifier_binds_true_scope_W_and_estimator_signature(self):
        budget=Mock(signature='c'*64)
        with patch.object(entry,'checkout_identity',return_value={'commit':'a'*40}), \
                patch.object(entry,'objects',return_value=(None,None,None,budget,{'active_tokens':134})) as objects, \
                patch.object(entry.metadata,'version',side_effect=lambda name:'2.56.1' if name=='ray' else 'fixture'):
            report=entry.qualify(self.path,self.root/'token-ready.json')
        self.assertIs(report['token_budget_required'],True)
        self.assertEqual(report['W'],{'active_tokens':134})
        self.assertEqual(report['token_estimator_signature'],'c'*64)
        self.assertEqual(objects.call_args.kwargs,{'require_token_budget':True})

    def test_wrong_or_missing_readiness_scope_rejects_before_case_launch(self):
        source={'commit':'a'*40,'tree':'b'*40}
        data=dict(status='passed_CPU_only_host_lease_pending',source=source,
            config_sha256=hashlib.sha256(self.path.read_bytes()).hexdigest(),
            resources=asdict(entry.make_resources(self.config)),token_budget_required=False)
        ready=self.root/'ready.json';ready.write_text(json.dumps(data))
        with patch.object(entry,'checkout_identity',return_value=source):
            entry.qualified(self.path,ready,require_token_budget=False)
            with self.assertRaisesRegex(ValueError,'changed'):
                entry.qualified(self.path,ready,require_token_budget=True)
            data.pop('token_budget_required');ready.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError,'changed'):
                entry.qualified(self.path,ready,require_token_budget=False)

    def test_no_requalification_after_any_budget_file_exists(self):
        artifact=Path(self.config['artifact_root']);artifact.mkdir()
        (artifact/'budget.sqlite').write_text('not an actual ledger; existence guard only')
        with patch.object(entry,'objects') as objects,self.assertRaisesRegex(FileExistsError,'never requalify'):
            entry.qualify(self.path,self.root/'ready.json',require_token_budget=False)
        objects.assert_not_called();self.assertFalse((self.root/'ready.json').exists())


class SourceTests(unittest.TestCase):
    def test_clean_exact_Git_then_dirty_or_changed_commit_rejects(self):
        with tempfile.TemporaryDirectory(prefix='validation-Git-') as name:
            root = Path(name);(root / 'code').mkdir();(root / 'code/file.py').write_text('pass\n')
            def git(*args):
                return subprocess.check_output(['git','-C',str(root),*args], text=True, stderr=subprocess.DEVNULL).strip()
            git('init');git('add','code');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-m','fixture')
            commit = git('rev-parse','HEAD')
            self.assertEqual(entry.checkout_identity(commit, repo=root)['commit'], commit)
            with self.assertRaises(ValueError): entry.checkout_identity('b'*40, repo=root)
            (root / 'code/file.py').write_text('changed\n')
            with self.assertRaises(ValueError): entry.checkout_identity(commit, repo=root)


class CapturedUidTests(unittest.TestCase):
    save = ConfigTests.save
    def setUp(self):
        ConfigTests.setUp(self)
        self.config['cases'] = [dict(case_id=n,token=t,successor=p) for n,t,p in entry.CASES]
        self.reports = []
        for i, case in enumerate(self.config['cases']):
            calls = [dict(call_id='c'+str(j),engine_uid='engine-'+str(i),status='succeeded',submission_confirmed=True,
                prompt_token_ids_json='[1]',output_token_ids_json='[2]',finish_reason='stop',prompt_tokens=1,completion_tokens=1,total_tokens=2) for j in range(48)]
            self.reports.append(dict(status='completed',public_git_commit='a'*40,calls=calls,outputs=[],
                W_exercised=False,successor_contention_exercised=False,timing={'full_owner_call_s':1},worker_wall_before_final_write_s=2))

    def compare(self):
        for case, report in zip(self.config['cases'], self.reports):
            path = self.root / 'cases' / case['case_id'];path.mkdir(parents=True,exist_ok=True)
            (path / 'case.json').write_text(json.dumps(report))
        return checks.compare(self.root, self.config)

    def test_positive_distinct_native_engines(self):
        self.assertTrue(self.compare()['outputs_strictly_comparable'])

    def test_null_empty_missing_and_nonstring_UIDs_reject(self):
        for value in (None,'',' ',17,False,[],{},' padded '):
            with self.subTest(value=value):
                for call in self.reports[0]['calls']:call['engine_uid'] = value
                with self.assertRaisesRegex(ValueError,'nonempty string'): self.compare()
        for call in self.reports[0]['calls']:call.pop('engine_uid')
        with self.assertRaisesRegex(ValueError,'nonempty string'): self.compare()

    def test_single_null_mixed_UID_and_cross_case_duplicate_reject(self):
        self.reports[0]['calls'][0]['engine_uid'] = None
        with self.assertRaises(ValueError): self.compare()
        self.reports[0]['calls'][0]['engine_uid'] = 'another'
        with self.assertRaisesRegex(ValueError,'exactly one'): self.compare()
        self.reports[0]['calls'][0]['engine_uid'] = 'engine-0'
        for call in self.reports[1]['calls']:call['engine_uid'] = 'engine-0'
        with self.assertRaisesRegex(ValueError,'reused'): self.compare()


class NormalQueryBridgeTests(unittest.TestCase):
    def test_four_options_use_actual_resources_and_same_original_method_reference(self):
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner, AsyncCoreModelActor
        from tests.execution_provider.test_lotus_record_local_tokens import QueryTokenTests
        from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
        from tests.execution_provider.test_lotus_record_local_query import Process
        from tests.semantic_methods.test_lotus_chain_composition import source_rows
        fixture = QueryTokenTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        chain = checks.make_chain('openai/Qwen2.5-7B-Instruct', GenerationSpec(max_tokens=8))
        records = source_rows(column='text',size=16)
        for i, row in enumerate(records): row.values['text'] = 'text '*(10+i%2)
        fixture.responses(chain, records)
        with tempfile.TemporaryDirectory() as name:
            config = config_for(name)
            resources = entry.make_resources(config)
        baseline = None
        for _, token, priority in entry.CASES:
            with self.subTest(token=token,priority=priority), ThreadedRuntime([]), \
                    patch.object(AsyncCoreModelActor,'_process_type',return_value=Process):
                budget, _ = checks.make_budget(fixture.tok,fixture.model,chain,records)
                actors = AsyncRecordsActorOwner.for_prepared(fixture.model,chain.stages[0].generation,resources)
                result = checks.run_case(records,chain,fixture.model,resources,control_dir=None,
                    token_budget=budget if token else None,priority=priority,actor_owner=actors)
                replay = checks.replay_native_answers(records,chain,result)
                self.assertEqual((len(result['calls']),replay['recorded_answers_replayed']),(48,48))
                self.assertEqual(result['close']['status'],'closed')
                outputs = [r.describe() for r in result['records']]
                if baseline is None: baseline = outputs
                self.assertEqual(outputs,baseline)
