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
        binding=TwoPassBinding(execute)
        with self.assertRaises(ValueError): binding.require_complete()
        for _ in range(2): self.assertEqual(binding(['image']*64),['False']*64)
        binding.require_complete()
        with self.assertRaises(ValueError): binding(['image']*64)
    def test_failure_is_spent(self):
        binding=TwoPassBinding(lambda rows: [])
        with self.assertRaises(ValueError): binding(['image']*64)
        self.assertEqual(len(binding.passes),1)
        with self.assertRaises(ValueError): binding.require_complete()



class NativeGuardTests(unittest.TestCase):
    def test_actual_hook_rejects_129_before_native_call(self):
        import asyncio
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
        calls=[]
        class FakeAsyncLLM:
            def __init__(self):
                raise AssertionError('constructor forbidden')
            async def generate(self, prompt, sampling_params, request_id):
                calls.append(request_id)
                yield types.SimpleNamespace(finished=True,request_id=request_id,outputs=[],prompt_token_ids=[])
        module=types.ModuleType('vllm.v1.engine.async_llm');module.AsyncLLM=FakeAsyncLLM
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);budget=AttemptBudget('q7-hook-fixture',128)
            ledger=CellBudgetLedger.create(root/'logical-unit.sqlite',budget,deadline_utc=time.time()+600)
            ledger.reserve_unit(budget.budget_id,128);ledger.claim_shared_unit(budget.budget_id)
            env={'Q7_SOURCE':str(Path(__file__).resolve().parents[2]),'Q7_OUTPUT':tmp,'Q7_BUDGET_ID':budget.budget_id}
            with patch.dict(os.environ,env),patch.dict(sys.modules,{'vllm.v1.engine.async_llm':module}):
                native_observer_setup()
                engine=object.__new__(FakeAsyncLLM)
                async def exercise():
                    for index in range(128):
                        async for _ in engine.generate({},None,str(index)): pass
                    with self.assertRaises(BudgetExhausted):
                        async for _ in engine.generate({},None,'129'): pass
                asyncio.run(exercise())
            self.assertEqual(len(calls),128)
            self.assertEqual(len(list(root.glob('native-output-*.json'))),128)

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
