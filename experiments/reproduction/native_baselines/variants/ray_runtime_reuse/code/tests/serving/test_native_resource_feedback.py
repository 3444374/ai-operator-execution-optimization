"""Actual CPU IPC/native-stat adapter tests. No vLLM install or GPU needed."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from src.serving.backends.vllm_async_engine import NativeStats,AsyncEngineLimits
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
from src.semantic_methods.lotus.records import identity


class CPUFeedbackProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec,descriptors):
        fixture=Path(__file__).with_name('resource_feedback_fixture.py')
        return [sys.executable,str(fixture),*map(str,descriptors)]


class NativeFeedbackTests(unittest.TestCase):
    def test_native_stat_values_have_real_fields_and_cumulative_units(self):
        stats=NativeStats('one',clock=lambda:12.0)
        self.assertIsNone(stats.snapshot())
        stats.record(SimpleNamespace(num_running_reqs=3,num_waiting_reqs=1,kv_cache_usage=.8),
            SimpleNamespace(num_preempted_reqs=2,num_generation_tokens=5,
                finished_requests=[SimpleNamespace(finish_reason='stop'),SimpleNamespace(finish_reason='abort')]))
        a=stats.snapshot();self.assertEqual((a['running'],a['kv_usage'],a['preemptions_total'],a['finished_total']), (3,.8,2,1))
        stats.record(None,SimpleNamespace(num_preempted_reqs=0,num_generation_tokens=2,finished_requests=[]))
        self.assertEqual(stats.snapshot()['sequence'],a['sequence'])
        self.assertEqual(stats.snapshot()['observed_at_s'],12.0)
        stats.record(SimpleNamespace(num_running_reqs=0,num_waiting_reqs=0,kv_cache_usage=0),None)
        self.assertEqual(stats.snapshot()['generation_tokens_total'],7)
        self.assertEqual(stats.snapshot()['kv_usage'],0)

    def process(self):
        patcher=patch.object(EngineSpec,'validate_driver_environment',return_value={});patcher.start();self.addCleanup(patcher.stop)
        process=CPUFeedbackProcess(EngineSpec('/tmp/cpu-fixture',sys.executable,sys.executable),GenerationSpec(),
            limits=AsyncEngineLimits(max_inflight=4,result_slots=4,command_slots=4,
                control_slots=2,feedback_interval_s=.001))
        self.addCleanup(lambda:process.close(timeout=3))
        return process

    def row(self,index,text='slow'):
        return dict(identity('feedback','r'+str(index),index),messages_json=json.dumps([dict(role='user',content=text)]))

    def until(self,predicate):
        deadline=time.monotonic()+3
        while not predicate():
            self.assertLess(time.monotonic(),deadline);time.sleep(.002)

    def test_actual_feedback_terminal_and_close_share_channels_without_losing_calls(self):
        p=self.process()
        for i in range(4):self.assertEqual(p.submit(self.row(i),deadline_monotonic=time.monotonic()+4).status,'queued')
        self.until(lambda:p.snapshot()['native_feedback'] is not None)
        values=[]
        while len(values)<4:
            values.extend(p.poll(4));self.assertLess(len(values),5);time.sleep(.003)
        self.assertEqual(len({r.request_id for r in values}),4)
        self.assertTrue(all(r.engine_terminal_confirmed for r in values))
        self.assertEqual(sum(r.result['prompt_tokens'] for r in values),44)
        report=p.close(timeout=3)
        self.assertEqual(report['status'],'closed');self.assertEqual(report['held_requests'],0)
        self.assertEqual(p.process.returncode,0)

    def test_full_unconsumed_results_do_not_block_feedback_abort_or_close(self):
        p=self.process();accepted=[]
        for i in range(4):accepted.append(p.submit(self.row(i,'hold'),deadline_monotonic=time.monotonic()+5))
        self.until(lambda:p.snapshot()['accepted_requests']==4)
        self.until(lambda:(p.snapshot()['native_feedback'] or {}).get('gated_requests')==4)
        self.assertTrue(p.abort(accepted[0].request_id))
        report=p.close(timeout=3)
        self.assertTrue(report['process_exit_confirmed']);self.assertTrue(report['io_thread_closed'])
        events=p.poll(4)
        self.assertEqual(len(events),4)
        self.assertEqual(sum(e.result['prompt_tokens'] for e in events),44)
        self.assertEqual(p.close(timeout=1)['status'],'closed')

    def test_public_vllm_constructor_enables_custom_stats_without_default_logs_or_http(self):
        import asyncio
        from types import ModuleType
        from unittest.mock import MagicMock
        from src.serving.backends import vllm_async_engine as kernel
        modules={name:ModuleType(name) for name in ('vllm','vllm.engine.arg_utils',
            'vllm.entrypoints.chat_utils','vllm.renderers','vllm.sampling_params',
            'vllm.utils.mistral','vllm.v1.engine.async_llm')}
        modules['vllm'].__version__='0.25.1';modules['vllm'].SamplingParams=object
        modules['vllm.engine.arg_utils'].AsyncEngineArgs=lambda **kw:SimpleNamespace(**kw)
        modules['vllm.entrypoints.chat_utils'].load_chat_template=lambda path:'original template'
        modules['vllm.renderers'].ChatParams=lambda **kw:kw
        modules['vllm.sampling_params'].RequestOutputKind=SimpleNamespace(FINAL_ONLY='final')
        modules['vllm.sampling_params'].StructuredOutputsParams=object
        modules['vllm.utils.mistral'].is_mistral_tokenizer=lambda tokenizer:False
        calls=[]
        def create(args,**kwargs):
            calls.append((args,kwargs))
            return SimpleNamespace(renderer=SimpleNamespace(tokenizer=object()),model_config=SimpleNamespace(enable_prompt_embeds=False))
        modules['vllm.v1.engine.async_llm'].AsyncLLM=SimpleNamespace(from_engine_args=create)
        async def construct(enabled):
            return kernel.VllmAsyncEngine(EngineSpec('/tmp/CPU','/tmp/model','/tmp/driver'),GenerationSpec(),
                limits=AsyncEngineLimits(feedback_interval_s=.1 if enabled else 0))
        with patch.dict(sys.modules,modules),patch.object(EngineSpec,'validate_local',return_value={}), \
                patch.object(kernel.common,'_validate_current_runtime'), \
                patch.object(kernel.importlib.metadata,'version',return_value='0.25.1'), \
                patch.object(kernel.VllmAsyncEngine,'_sampling',return_value=None):
            plain=asyncio.run(construct(False));feedback=asyncio.run(construct(True))
        self.assertNotIn('stat_loggers',calls[0][1]);self.assertTrue(calls[1][0].disable_log_stats)
        logger=calls[1][1]['stat_loggers'][0](None,0)
        self.assertIs(logger,feedback._feedback)
        self.assertIsNone(plain.feedback_snapshot())
        self.assertEqual(calls[0][0].model,calls[1][0].model)
        self.assertEqual(calls[0][0].max_num_seqs,calls[1][0].max_num_seqs)
