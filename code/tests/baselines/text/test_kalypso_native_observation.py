"""Ported CPU regressions: no model, NVML, native installation or network."""
import asyncio
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from src.baselines.text.frameworks.kalypso_native import adapter
from src.baselines.text.frameworks.kalypso_native.native_server import install_hooks
from src.baselines.text.frameworks.kalypso_native.qualification import sampling_matches

def fake_events(n,qid='q',uid='fake-model'):
    return [event for i in range(n) for event in [
        dict(event='filter_result',idx=i,decision=i%2==0,verdict='true' if i%2==0 else 'false',query_id=qid),
        dict(event='native_API_end',idx=i,status='succeeded',request_id=f'{qid}-{i}',
            query_id=qid,pin=False,max_tokens=8,model_uid=uid,elapsed_s=.001)]]

class KalypsoObservationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.tmp = Path(self.directory.name)

    def test_uuid_sampler_rejects_wrong_identity_and_drift_without_publishing_samples(self):
        cfg=dict(physical_gpu_index=0,gpu_uuid_sha256='fixed-physical-UUID-hash')
        values=dict(values={'gpu_utilization_pct':32},missing={},sources={})
        calls=[]
        read=lambda index:cfg['gpu_uuid_sha256']
        sampler=adapter.uuid_bound_sample_source(cfg,lambda:calls.append(1) or values,read)
        result=sampler()
        self.assertEqual(result['values'],values['values'])
        self.assertEqual(result['sources']['physical_gpu_binding']['uuid_sha256'],cfg['gpu_uuid_sha256'])
        with self.assertRaises(ValueError):
            adapter.uuid_bound_sample_source(cfg,lambda:values,lambda index:'wrong')
        changing=iter([cfg['gpu_uuid_sha256'],cfg['gpu_uuid_sha256'],'wrong'])
        sampler=adapter.uuid_bound_sample_source(cfg,lambda:values,lambda index:next(changing))
        with self.assertRaises(ValueError):sampler()


    def test_default_uuid_sampler_uses_real_metric_reader_with_physical_UUID(self):
        physical=['GPU-CPU-ONLY-physical-0'];calls=[]
        def by_uuid(value):
            self.assertIsInstance(value,str)
            self.assertEqual(value,physical[0])
            calls.append(('UUID',value))
            return ('UUID',value)
        fake=SimpleNamespace(NVMLError=RuntimeError,nvmlInit=lambda:None,nvmlShutdown=lambda:None,
            nvmlDeviceGetHandleByIndex=lambda i:('index',i),nvmlDeviceGetHandleByUUID=by_uuid,
            nvmlDeviceGetUUID=lambda handle:physical[0].encode(),
            nvmlDeviceGetUtilizationRates=lambda handle:SimpleNamespace(gpu=42),
            nvmlDeviceGetMemoryInfo=lambda handle:SimpleNamespace(used=12345*1024**2,total=24564*1024**2),
            nvmlDeviceGetPowerUsage=lambda handle:250000)
        cfg=dict(physical_gpu_index=0,gpu_uuid_sha256=hashlib.sha256(physical[0].encode()).hexdigest())
        with patch.dict('sys.modules',{'pynvml':fake}),patch(
                'src.observability.records_telemetry.host_metadata',lambda **kw:{}):
            # Exercise the production default branch and fixed MetricSources, without an injected reader.
            sampler=adapter.uuid_bound_sample_source(cfg)
            result=sampler()
            expected=dict(gpu_utilization_pct=42,gpu_memory_used_mib=12345,
                gpu_memory_total_mib=24564,gpu_power_w=250)
            self.assertEqual({k:result['values'][k] for k in expected},expected)
            self.assertTrue(all(k not in result['missing'] for k in expected))
            self.assertEqual(result['sources']['physical_gpu_binding']['uuid_sha256'],cfg['gpu_uuid_sha256'])
            self.assertEqual(calls,[('UUID',physical[0])])
            physical[0]='GPU-CPU-ONLY-physical-1'
            with self.assertRaises(ValueError):sampler()
            with self.assertRaises(ValueError):adapter.uuid_bound_sample_source(cfg)
            self.assertEqual(len(calls),1)


    def test_missing_negative_duplicate_unknown_and_failed_terminal_rejected(self):
        rows=[dict(row_id='r'+str(i),source_position=i) for i in range(2)]
        response=dict(model_name='m',num_output_rows=1,results=[dict(idx=0)])
        for mutate in [lambda e:e.pop(),lambda e:e.append(copy.deepcopy(e[0])),
                       lambda e:e[0].update(idx=99),lambda e:e[1].update(status='unknown')]:
            events=fake_events(2);mutate(events)
            with self.assertRaises(ValueError):adapter.reconcile(rows,response,events,'m')
        for indices in [[0,0],[2],[True]]:
            with self.assertRaises(ValueError):
                adapter.reconcile(rows,dict(response,results=[dict(idx=i) for i in indices]),fake_events(2),'m')


    def test_zero_outputs_requires_all_negative_terminals(self):
        rows=[dict(row_id='r0',source_position=0)]
        e=fake_events(1);e[0].update(decision=False,verdict='false')
        out=adapter.reconcile(rows,dict(model_name='m',num_output_rows=0,results=[]),e,'m')
        self.assertFalse(out[0]['decision'])
        with self.assertRaises(ValueError):
            adapter.reconcile(rows,dict(model_name='m',num_output_rows=0,results=[]),[],'m')


    def test_observation_preserves_original_result_and_exception_budget_stops_before_next_call(self):
        output=SimpleNamespace(request_id='fresh-1',finish_reason='stop')
        class Executor:
            model='m';calls=0
            async def chatcomplete(self,*args,**kwargs):
                self.calls+=1
                if self.calls==2:raise LookupError('CPU original error')
                return output
        class Filter:
            async def _run_single(self,ctx,priority=0):return True
        unit=dict(call_cap=2,queries=[dict(query_id='q',expected_calls=2)])
        counter=install_hooks({},unit,self.tmp,Executor,Filter)
        executor=Executor();raw=SimpleNamespace(headers={'X-Kalypso-Observation':'q'})
        self.assertIs(asyncio.run(executor.chatcomplete(raw,[],8)),output)
        with self.assertRaises(LookupError):asyncio.run(executor.chatcomplete(raw,[],8))
        with self.assertRaises(RuntimeError):asyncio.run(executor.chatcomplete(raw,[],8))
        self.assertEqual(executor.calls,2);self.assertEqual(counter['reserved_calls'],2)


    def test_stop_none_and_empty_list_are_audit_equivalent_only(self):
        expected = dict(max_tokens=8, temperature=0., seed=42, top_p=1., frequency_penalty=.5, repetition_penalty=1.3, stop=[])
        actual = dict(expected, stop=None)
        self.assertTrue(sampling_matches(actual, expected))
        self.assertIsNone(actual['stop'])
        self.assertEqual(expected['stop'], [])
        for field in ('max_tokens', 'temperature', 'seed', 'top_p', 'frequency_penalty', 'repetition_penalty', 'stop'):
            changed = dict(actual)
            changed[field] = ['different'] if field == 'stop' else actual[field] + 1
            self.assertFalse(sampling_matches(changed, expected), field)
        self.assertFalse(sampling_matches(dict(actual, extra=True), expected))
