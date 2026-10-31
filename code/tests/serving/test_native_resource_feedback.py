"""Actual CPU IPC/native-stat adapter tests. No vLLM install or GPU needed."""
from dataclasses import asdict,replace
import json
import atexit
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
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
    def test_public_CPU_actor_full_handoff_off_close_without_driver_fallback(self):
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
        from src.serving.backends import async_engine_process as transport
        tools=Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation'
        for mode,profile in (('on','full'),('on','handoff'),('off','full')):
            with self.subTest(mode=mode,profile=profile),tempfile.TemporaryDirectory()as folder, \
                    patch.dict(sys.modules),patch.dict(os.environ,SEM_RECORD_OBSERVATION=mode,SEM_RECORD_OBSERVER_ROOT=folder), \
                    patch.object(transport,'_frame',transport._frame),patch.object(transport,'json',transport.json):
                root=Path(folder);(root/'config.json').write_text(json.dumps(dict(observation=dict(
                    mode=mode,profile=profile,max_events_per_process=2000,max_event_bytes=65536,
                    max_pending_frame_metadata=50))))
                def load(name):
                    spec=importlib.util.spec_from_file_location(name,tools/(name+'.py'))
                    module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module);return module
                observer=load('record_observer');ipc=load('record_ipc_observer')
                class ObservedCPU(ipc.ObservedAsyncEngineProcess):
                    _command=staticmethod(CPUFeedbackProcess._command)
                actor_type=ipc.ObservedAsyncCoreModelActor if mode=='on'else AsyncCoreModelActor
                process_type=ObservedCPU if mode=='on'else CPUFeedbackProcess
                with patch.object(observer,'install_actor',lambda:observer.emit('actor_observer_installed')), \
                        patch.object(actor_type,'_process_type',return_value=process_type), \
                        patch.object(EngineSpec,'validate_driver_environment',return_value={}):
                    actor=actor_type(asdict(EngineSpec('/tmp/cpu-fixture',sys.executable,sys.executable)),
                        asdict(GenerationSpec()),1,asdict(AsyncEngineLimits(max_inflight=4,result_slots=4,
                            command_slots=4,control_slots=2,feedback_interval_s=.001)))
                    try:
                        actor.ready(time.monotonic()+5)
                        self.until(lambda:actor.snapshot()['ipc']['native_feedback']is not None)
                        self.assertEqual(actor.engine.snapshot()['accepted_requests'],0)
                        report=actor.close(3)
                        self.assertEqual(report['status'],'closed');self.assertTrue(report['producer_quiescence']['confirmed'])
                        if mode=='on':
                            # Do not call driver close_events before this assertion.
                            self.assertTrue(observer._closed)
                            status_path=next((root/'events').glob('*.status.json'));saved=status_path.read_bytes()
                            status=json.loads(saved)
                            self.assertTrue(status['flush_confirmed']and status['feedback_buffer']['drain_complete'])
                            self.assertEqual((status['dropped'],status['errors']),(0,0))
                            events=[json.loads(line)for p in(root/'events').glob('*.jsonl')for line in p.read_text().splitlines()]
                            startup,=[e for e in events if e['event']=='native_startup_capacity']
                            self.assertIsNone(startup['native_capacity'])  # Fixture has no effective vLLM config.
                            self.assertTrue(any(e['event']=='native_feedback_frame'and e['accepted']is True for e in events))
                            actor.snapshot();actor.close(3)
                            self.assertEqual(status_path.read_bytes(),saved)
                        else:
                            self.assertFalse(observer._closed);self.assertFalse((root/'events').exists())
                    finally:
                        actor.close(3);observer.close_events();atexit.unregister(observer.close_events)

    def test_public_CPU_IPC_audit_preserves_results_rejects_foreign_UID_and_flushes_bounded_metadata(self):
        tools=Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation'
        with tempfile.TemporaryDirectory()as folder,patch.dict(sys.modules), \
                patch.dict(os.environ,SEM_RECORD_OBSERVATION='on',SEM_RECORD_OBSERVER_ROOT=folder):
            root=Path(folder);(root/'config.json').write_text(json.dumps(dict(observation=dict(
                mode='on',profile='handoff',max_events_per_process=2000,max_event_bytes=4096))))
            def load(name):
                spec=importlib.util.spec_from_file_location(name,tools/(name+'.py'))
                module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module);return module
            observer=load('record_observer');ipc=load('record_ipc_observer')
            class ObservedCPU(ipc.ObservedAsyncEngineProcess):
                _command=staticmethod(CPUFeedbackProcess._command)
            try:
                with patch.object(EngineSpec,'validate_driver_environment',return_value={}):
                    p=ObservedCPU(EngineSpec('/tmp/cpu-fixture',sys.executable,sys.executable),GenerationSpec(),
                        limits=AsyncEngineLimits(max_inflight=4,result_slots=4,command_slots=4,
                            control_slots=2,feedback_interval_s=.001))
                try:
                    for i in range(4):self.assertEqual(p.submit(self.row(i),deadline_monotonic=time.monotonic()+4).status,'queued')
                    self.until(lambda:p.snapshot()['native_feedback']is not None)
                    p._receive(dict(op='feedback',snapshot=dict(engine_uid='b'*32,sequence=999,
                        observed_at_s=time.monotonic(),prompt='DO_NOT_PERSIST')),results=False)
                    self.assertNotEqual((p.snapshot()['native_feedback']or{}).get('sequence'),999)
                    rows=[]
                    while len(rows)<4:
                        rows.extend(p.poll(4));self.assertLess(len(rows),5);time.sleep(.002)
                    self.assertTrue(all(r.engine_terminal_confirmed for r in rows))
                    self.assertEqual(len({r.request_id for r in rows}),4)
                    self.assertEqual(p.close(timeout=3)['status'],'closed')
                finally:p.close(timeout=3)
                observer.close_events()
                raw=next((root/'events').glob('*.jsonl')).read_text();events=[json.loads(x)for x in raw.splitlines()]
                frames=[e for e in events if e['event']=='native_feedback_frame']
                self.assertTrue(any(e['accepted']is True for e in frames))
                self.assertTrue(any(e['reason']=='engine_uid_mismatch'and e['engine_uid']=='b'*32 for e in frames))
                self.assertTrue(all(e['source_sequence']is not None and e['received_at_s']is not None for e in frames))
                self.assertNotIn('DO_NOT_PERSIST',raw)
                status=json.loads(next((root/'events').glob('*.status.json')).read_text())
                self.assertEqual(status['feedback_buffer']['remaining'],0)
                self.assertTrue(status['feedback_buffer']['drain_complete']and status['flush_confirmed'])
            finally:
                observer.close_events();atexit.unregister(observer.close_events)

    def test_effective_capacity_reads_constructed_config_without_declared_fallback(self):
        from src.observability.metrics.vllm import effective_capacity_snapshot
        engine=SimpleNamespace(scheduler_config=SimpleNamespace(max_num_seqs=7,max_num_batched_tokens=999),
            model_config=SimpleNamespace(max_model_len=2048),
            cache_config=SimpleNamespace(num_gpu_blocks=None,block_size=16))
        observed=effective_capacity_snapshot(engine)
        self.assertEqual(observed['status'],'observed_effective_config')
        self.assertEqual((observed['max_num_seqs'],observed['max_num_batched_tokens']),(7,999))
        self.assertIsNone(observed['kv_cache_blocks']);self.assertIsNone(observed['kv_free_bytes'])
        self.assertIsNone(effective_capacity_snapshot(None)['max_num_seqs'])
        engine.scheduler_config.max_num_seqs=True
        self.assertIsNone(effective_capacity_snapshot(engine)['max_num_seqs'])

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
            return SimpleNamespace(renderer=SimpleNamespace(tokenizer=object()),model_config=SimpleNamespace(enable_prompt_embeds=False),
                vllm_config=SimpleNamespace(scheduler_config=SimpleNamespace(max_num_seqs=7,max_num_batched_tokens=999),
                    model_config=SimpleNamespace(max_model_len=2048),cache_config=SimpleNamespace(block_size=16,num_gpu_blocks=None)))
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
        self.assertNotIn('native_capacity',plain.ready())
        self.assertEqual(feedback.ready()['native_capacity']['max_num_seqs'],7)
        self.assertNotEqual(feedback.ready()['native_capacity']['max_num_seqs'],calls[1][0].max_num_seqs)
