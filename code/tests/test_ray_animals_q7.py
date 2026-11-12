import unittest
from src.baselines.image.ray_animals_q7.binding import TwoPassBinding, reconcile

class BindingTests(unittest.TestCase):
    def setUp(self):
        self.rows = [dict(call_id=str(i)) for i in range(64)]
        self.predictions = [dict(call_id=str(i), generated_text=str(i)) for i in range(64)]
    def test_reordered_identity(self):
        self.assertEqual(reconcile(self.rows, self.predictions[::-1]), [str(i) for i in range(64)])
    def test_missing_duplicate_unknown(self):
        for bad in (self.predictions[:-1], self.predictions[:-1]+self.predictions[:1],
                    self.predictions[:-1]+[dict(call_id='unknown',generated_text='True')]):
            with self.subTest(bad=bad[-1]):
                with self.assertRaises(ValueError): reconcile(self.rows,bad)
    def test_budget_and_complete(self):
        def execute(rows):
            return [dict(call_id=r['call_id'],generated_text='False') for r in rows[::-1]]
        binding=TwoPassBinding(execute,64)
        with self.assertRaises(ValueError): binding.require_complete()
        for _ in range(2): self.assertEqual(binding(['image']*64),['False']*64)
        binding.require_complete()
        with self.assertRaises(ValueError): binding(['image']*64)
    def test_failure_is_spent(self):
        binding=TwoPassBinding(lambda rows: [],64)
        with self.assertRaises(ValueError): binding(['image']*64)
        self.assertEqual(len(binding.passes),1)
        with self.assertRaises(ValueError): binding.require_complete()



class NativeGuardTests(unittest.TestCase):
    def test_actual_hook_rejects_next_attempt_for_frozen_source_size(self):
        import asyncio
        import json
        import os
        import sys
        import tempfile
        import time
        import types
        from pathlib import Path
        from unittest.mock import patch
        from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
        from src.experiments.cell_budget import CellBudgetLedger
        from src.baselines.image.ray_animals_q7.native_probe import native_observer_setup
        for count in (1,7,64):
            cap=2*count
            calls=[]
            class FakeAsyncLLM:
                def __init__(self):
                    raise AssertionError('constructor forbidden')
                async def generate(self, prompt, sampling_params, request_id):
                    calls.append(request_id)
                    yield types.SimpleNamespace(finished=True,request_id=request_id,outputs=[],prompt_token_ids=[])
            module=types.ModuleType('vllm.v1.engine.async_llm');module.AsyncLLM=FakeAsyncLLM
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);budget=AttemptBudget('q7-hook-fixture',cap)
                (root/'RUN-BOUND.json').write_text(json.dumps(dict(budget_id=budget.budget_id,physical_attempt_limit=cap)))
                ledger=CellBudgetLedger.create(root/'logical-unit.sqlite',budget,deadline_utc=time.time()+600)
                ledger.reserve_unit(budget.budget_id,cap);ledger.claim_shared_unit(budget.budget_id)
                env={'Q7_SOURCE':str(Path(__file__).resolve().parents[2]),'Q7_OUTPUT':tmp,'Q7_BUDGET_ID':budget.budget_id}
                with patch.dict(os.environ,env),patch.dict(sys.modules,{'vllm.v1.engine.async_llm':module}):
                    native_observer_setup()
                    engine=object.__new__(FakeAsyncLLM)
                    async def exercise():
                        for index in range(cap):
                            async for _ in engine.generate({},None,str(index)): pass
                        with self.assertRaises(BudgetExhausted):
                            async for _ in engine.generate({},None,'over-budget'): pass
                    asyncio.run(exercise())
                self.assertEqual(len(calls),cap)
                self.assertEqual(len(list(root.glob('native-output-*.json'))),cap)

class HostAdapterTests(unittest.TestCase):
    def test_existing_cleanup_helpers_and_residual_process_gate(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import Mock, patch
        from src.baselines.image.ray_animals_q7.host_adapter import StageSupervisor
        for residual in (False, True):
            with self.subTest(residual=residual), TemporaryDirectory() as tmp:
                host=SimpleNamespace(snapshot=Mock(return_value={}),private_json=Mock(),
                    assert_idle=Mock(),report_identities=Mock(return_value={123}),
                    live_owned=Mock(return_value=[123] if residual else []),cleanup_owned=Mock())
                api=SimpleNamespace(supervise=Mock(return_value={'worker_exit':0}))
                resources=dict(failures=[],remaining_new_shm_names=[],passed=True)
                monitor=SimpleNamespace(finish=Mock(return_value=resources))
                sockets=SimpleNamespace(cleanup_inactive_socket_nodes=Mock(return_value={}))
                semaphores=SimpleNamespace(cleanup_inactive_semaphores=Mock(return_value=[]))
                adapter=StageSupervisor(host,api,sockets,semaphores,{'shared_resources':{}},lambda _:monitor,60)
                with patch.object(Path,'read_text',return_value=''):
                    if residual:
                        with self.assertRaisesRegex(RuntimeError,'Owned process'):adapter(['fake'],0,Path(tmp))
                        sockets.cleanup_inactive_socket_nodes.assert_not_called()
                        semaphores.cleanup_inactive_semaphores.assert_not_called()
                    else:
                        receipt=adapter(['fake'],0,Path(tmp))
                        self.assertTrue(receipt['physical_idle'])
                        sockets.cleanup_inactive_socket_nodes.assert_called_once()
                        semaphores.cleanup_inactive_semaphores.assert_called_once()
                api.supervise.assert_called_once()


class HostFailureEvidenceTests(unittest.TestCase):
    def setUp(self):
        from copy import deepcopy
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import Mock
        from src.baselines.image.ray_animals_q7.host_adapter import StageSupervisor
        temporary = TemporaryDirectory(prefix='q7-rejected-stage-')
        self.addCleanup(temporary.cleanup)
        self.root, self.saved = Path(temporary.name), {}
        self.guard = RuntimeError('foreign Python, Ray or model work was observed; nothing was signalled')
        self.resources = dict(passed=False, failures=[], remaining_new_shm_names=['sem.mp-fixture'])

        def save(path, value):
            self.saved[path.name] = deepcopy(value)

        self.host = SimpleNamespace(snapshot=Mock(return_value={'foreign_work': ['fixture-observer']}),
            private_json=Mock(side_effect=save), assert_idle=Mock(side_effect=[None, self.guard]),
            report_identities=Mock(return_value={(123, 42.)}), live_owned=Mock(return_value=[]),
            cleanup_owned=Mock())
        self.api = SimpleNamespace(supervise=Mock(return_value={'worker_exit': 0}))
        self.monitor = SimpleNamespace(finish=Mock(return_value=self.resources))
        self.sockets = SimpleNamespace(cleanup_inactive_socket_nodes=Mock())
        self.semaphores = SimpleNamespace(cleanup_inactive_semaphores=Mock())
        self.adapter = StageSupervisor(self.host, self.api, self.sockets, self.semaphores,
            {'shared_resources': {}}, lambda _: self.monitor, 60)

    def test_foreign_guard_failure_records_exit_and_resources_without_unlink(self):
        import os
        from unittest.mock import patch
        with patch.dict(os.environ, TMPDIR='original-temp', RAY_TMPDIR='original-ray', VLLM_RPC_BASE_PATH='original-rpc'):
            with self.assertRaises(RuntimeError) as caught:
                self.adapter(['CPU-fixture'], 0, self.root)
            self.assertEqual(os.environ['TMPDIR'], 'original-temp')
            self.assertEqual(os.environ['RAY_TMPDIR'], 'original-ray')
            self.assertEqual(os.environ['VLLM_RPC_BASE_PATH'], 'original-rpc')
        self.assertIs(caught.exception, self.guard)
        self.host.cleanup_owned.assert_not_called()
        self.sockets.cleanup_inactive_socket_nodes.assert_not_called()
        self.semaphores.cleanup_inactive_semaphores.assert_not_called()
        exit = self.saved['EXIT.json']
        self.assertEqual(exit['status'], 'failed')
        self.assertEqual(exit['host_idle_guard'], 'rejected')
        self.assertTrue(exit['worker_exit_zero'])
        self.assertTrue(exit['owned_processes_exited'])
        self.assertFalse(exit['physical_idle'])
        self.assertFalse(exit['socket_cleanup_attempted'])
        self.assertFalse(exit['semaphore_cleanup_attempted'])
        self.assertEqual(self.saved['resources-at-failure.json'], self.resources)

    def test_worker_failure_remains_primary_if_guard_also_rejects(self):
        failure = ValueError('native worker fixture failure')
        failure.report = {'worker_exit': 1}
        self.api.supervise.side_effect = failure
        with self.assertRaises(ValueError) as caught:
            self.adapter(['CPU-fixture'], 0, self.root)
        self.assertIs(caught.exception, failure)
        self.assertFalse(self.saved['EXIT.json']['worker_exit_zero'])
        self.assertEqual(self.saved['EXIT.json']['failure']['message'], str(failure))
        self.assertEqual(self.saved['EXIT.json']['finalization_failure']['message'], str(self.guard))
        self.assertTrue(any('stage finalization also failed' in note for note in failure.__notes__))

    def test_resource_violation_still_prevents_unlink_after_idle_guard_passes(self):
        self.host.assert_idle.side_effect = None
        self.resources['failures'] = ['resource fixture violation']
        with self.assertRaisesRegex(RuntimeError, 'resource failure remains'):
            self.adapter(['CPU-fixture'], 0, self.root)
        self.assertEqual(self.saved['EXIT.json']['host_idle_guard'], 'passed')
        self.sockets.cleanup_inactive_socket_nodes.assert_not_called()
        self.semaphores.cleanup_inactive_semaphores.assert_not_called()

    def test_diagnostic_write_failure_cannot_replace_guard_failure(self):
        original = self.host.private_json.side_effect

        def save(path, value):
            if path.name == 'EXIT.json':
                raise OSError('fixture diagnostic storage failure')
            original(path, value)

        self.host.private_json.side_effect = save
        with self.assertRaises(RuntimeError) as caught:
            self.adapter(['CPU-fixture'], 0, self.root)
        self.assertIs(caught.exception, self.guard)
        self.assertIn('resources-at-failure.json', self.saved)
        self.assertTrue(any('exit evidence also failed' in note for note in self.guard.__notes__))

    def test_supervisor_evidence_write_failure_preserves_known_worker_exit(self):
        original = self.host.private_json.side_effect
        failure = OSError('fixture supervisor storage failure')

        def save(path, value):
            if path.name == 'supervisor.json':
                raise failure
            original(path, value)

        self.host.private_json.side_effect = save
        with self.assertRaises(OSError) as caught:
            self.adapter(['CPU-fixture'], 0, self.root)
        self.assertIs(caught.exception, failure)
        self.assertTrue(self.saved['EXIT.json']['worker_exit_zero'])
        self.assertTrue(self.saved['EXIT.json']['owned_processes_exited'])
        self.assertEqual(self.saved['EXIT.json']['host_idle_guard'], 'rejected')

    def test_resource_observation_failure_still_records_failed_exit(self):
        failure = OSError('fixture resource snapshot failure')
        self.monitor.finish.side_effect = failure
        with self.assertRaises(OSError) as caught:
            self.adapter(['CPU-fixture'], 0, self.root)
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.saved['EXIT.json']['status'], 'failed')
        self.assertEqual(self.saved['EXIT.json']['host_idle_guard'], 'not_reached')
        self.assertTrue(self.saved['EXIT.json']['worker_exit_zero'])
        self.assertTrue(self.saved['EXIT.json']['owned_processes_exited'])
        self.assertIsNone(self.saved['EXIT.json']['resource_passed'])
        self.semaphores.cleanup_inactive_semaphores.assert_not_called()

    def test_worker_error_with_safe_cleanup_writes_only_failed_exit(self):
        from pathlib import Path
        from unittest.mock import patch
        self.host.assert_idle.side_effect = None
        self.resources.update(passed=True, remaining_new_shm_names=[])
        for stage, worker_exit in enumerate((1, None)):
            with self.subTest(worker_exit=worker_exit):
                failure = ValueError('worker fixture failure')
                failure.report = {'worker_exit': worker_exit}
                self.api.supervise.side_effect = failure
                with patch.object(Path, 'read_text', return_value=''):
                    with self.assertRaises(ValueError) as caught:
                        self.adapter(['CPU-fixture'], stage, self.root)
                self.assertIs(caught.exception, failure)
                exit = self.saved['EXIT.json']
                self.assertEqual(exit['status'], 'failed')
                self.assertIs(exit['worker_exit_zero'], False if worker_exit == 1 else None)
                self.assertEqual(exit['host_idle_guard'], 'passed')
                self.assertTrue(exit['socket_cleanup_attempted'])
                self.assertTrue(exit['semaphore_cleanup_attempted'])
        writes = [call for call in self.host.private_json.call_args_list if call.args[0].name == 'EXIT.json']
        self.assertEqual(len(writes), 2)

    def test_last_resource_rejection_returns_failed_exit_without_changing_cleanup(self):
        from pathlib import Path
        from unittest.mock import patch
        self.host.assert_idle.side_effect = None
        self.monitor.finish.side_effect = [
            dict(passed=True, failures=[], remaining_new_shm_names=[]),
            dict(passed=False, failures=[], remaining_new_shm_names=['sem.mp-late-fixture']),
        ]
        self.sockets.cleanup_inactive_socket_nodes.return_value = {}
        self.semaphores.cleanup_inactive_semaphores.return_value = []
        with patch.object(Path, 'read_text', return_value=''):
            receipt = self.adapter(['CPU-fixture'], 0, self.root)
        self.assertEqual(receipt['status'], 'failed')
        self.assertEqual(self.saved['EXIT.json']['status'], 'failed')
        self.assertTrue(receipt['worker_exit_zero'])
        self.assertTrue(receipt['owned_processes_exited'])
        self.assertTrue(receipt['private_sockets_clear'])
        self.assertFalse(receipt['resource_passed'])
        self.assertFalse(receipt['physical_idle'])
        self.assertEqual(receipt['host_idle_guard'], 'passed')
        self.sockets.cleanup_inactive_socket_nodes.assert_called_once()
        self.semaphores.cleanup_inactive_semaphores.assert_called_once()
