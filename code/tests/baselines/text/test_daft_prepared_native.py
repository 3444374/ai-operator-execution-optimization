"""Real Daft expression/executor/CPU relations, explicitly recorded model replies."""

import asyncio
from types import ModuleType, SimpleNamespace
import unittest

from src.baselines.text.frameworks.daft_sembench_native import execute_prepared_map, native_sampling_params
from src.baselines.text.frameworks.daft_native_trace import NativeMapTrace
from tests.baselines.text.native_sdk_fixtures import module_entries, recorded_map_provider


class PreparedDaftTests(unittest.TestCase):
    def test_actual_sdk_executor_keeps_duplicate_prompt_occurrences_and_closes_model(self):
        requests = [dict(call_id=identity, prompt='same-prompt') for identity in ('b', 'a', 'c')]
        texts = ['False', 'True', 'ambiguous']
        reserved, ready = [], []
        with recorded_map_provider(requests, texts) as cpu:
            def result_ready(result):
                self.assertTrue(all(not engine.closed for engine in cpu.engines))
                ready.append(result['metadata']['result_ready_monotonic_ns'])
            result = execute_prepared_map(requests, model='CPU-recorded', sampling_params={'n': 1},
                on_result_ready=result_ready,
                before_generate=lambda call_id, request_id, prompt: reserved.append((call_id, prompt)))
            self.assertEqual(len(cpu.expressions), 1)
            self.assertTrue(all(engine.closed for engine in cpu.engines))
        self.assertTrue(all(not executor.loop_thread.is_alive() and executor.loop.is_closed()
                            for executor in cpu.executors))
        self.assertEqual(result['predictions'], [dict(call_id=row['call_id'], generated_text=text)
                         for row, text in zip(requests, texts)])
        self.assertEqual(sorted(reserved), sorted((row['call_id'], row['prompt']) for row in requests))
        trace = result['metadata']['native_api']
        self.assertEqual([trace[name] for name in ('generate_calls', 'accepted', 'final_events', 'unresolved')],
                         [3, 3, 3, 0])
        self.assertFalse(trace['errors'])
        self.assertIsNone(trace['first_engine_accept_ns'])
        self.assertLess(ready[0], result['metadata']['model_close_receipt']['close_started_monotonic_ns'])

    def test_budget_rejection_precedes_native_generate_and_restores_hooks(self):
        class Engine:
            delegated = 0
            def generate(self, *args, **kwargs):
                self.delegated += 1
                async def stream():
                    yield SimpleNamespace(finished=False)
                return stream()
            async def add_request(self, *args, **kwargs):
                return None
        engine = Engine()
        remaining = [1]
        def reserve(*args):
            if not remaining[0]:
                raise RuntimeError('CPU exhausted')
            remaining[0] -= 1
        with NativeMapTrace(before_generate=reserve) as trace:
            trace.attach(engine)
            stream = engine.generate('first', {}, 'id-1')
            asyncio.run(stream.aclose())
            with self.assertRaisesRegex(RuntimeError, 'CPU exhausted'):
                engine.generate('second', {}, 'id-2')
            self.assertEqual(engine.delegated, 1)
            self.assertEqual(trace.report()['generate_calls'], 1)
        self.assertNotIn('generate', engine.__dict__)
        self.assertNotIn('add_request', engine.__dict__)

    def test_plain_and_structured_sampling_preserve_effective_options(self):
        module, structured = ModuleType('vllm'), ModuleType('vllm.sampling_params')
        module.SamplingParams = lambda **kwargs: SimpleNamespace(**kwargs)
        structured.StructuredOutputsParams = lambda **kwargs: SimpleNamespace(**kwargs)
        values = dict(n=1, max_tokens=8192, temperature=0, seed=0, stop=[])
        with module_entries({'vllm': module, 'vllm.sampling_params': structured}):
            self.assertEqual(vars(native_sampling_params(values)), values)
            for format_values in ({'response_format': {'type': 'json_object'}},
                                  {'structured_outputs': {'json_object': True}}):
                params = native_sampling_params(dict(values, **format_values))
                self.assertTrue(params.structured_outputs.json_object)
                self.assertEqual({key: getattr(params, key) for key in values}, values)
            with self.assertRaises(ValueError):
                native_sampling_params(dict(values, response_format={'type': 'text'}))
            with self.assertRaises(ValueError):
                native_sampling_params(dict(values, response_format={'type': 'json_object'}, structured_outputs={}))
        self.assertEqual(values, dict(n=1, max_tokens=8192, temperature=0, seed=0, stop=[]))


if __name__ == '__main__':
    unittest.main()
