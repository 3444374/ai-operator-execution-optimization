"""Original queries and real worker ACK/HTTP accounting; graph replies are software.

Actual native Daft graph behavior is covered separately by
tests.baselines.text.test_daft_native_messages. No service or model runs here.
"""
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from src.experiments import native_daft_query as worker, native_daft_sembench as driver
from src.baselines.text.frameworks.daft_sembench_native import effective_openai_messages
from src.infrastructure.private_artifacts import content_digest, read_json, write_private_json
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.experiments import test_sembench_lotus_image_query as fixture


CHILD = r'''
import asyncio,importlib.abc,json,socket,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[4])
class NoEngine(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,*args):
  if fullname.split('.')[0] in {'daft','vllm','torch','transformers','lotus'}:
   raise AssertionError('software worker imported an engine/semantic SDK: '+fullname)
sys.meta_path.insert(0,NoEngine())
old_connect,old_bind=socket.socket.connect,socket.socket.bind
def guard(original):
 def call(sock,address):
  if sock.family in (socket.AF_INET,socket.AF_INET6):raise AssertionError('software worker accessed INET')
  return original(sock,address)
 return call
socket.socket.connect,socket.socket.bind=guard(old_connect),guard(old_bind)
import httpx
from src.experiments.native_daft_query import run_worker
from src.baselines.text.frameworks.daft_sembench_native import effective_openai_messages
from src.infrastructure.private_artifacts import content_digest,read_json
config,directory,plan=read_json(sys.argv[1]),Path(sys.argv[2]),read_json(sys.argv[3])
sent=[]
async def send(client,request,*args,**kwargs):
 body=json.loads(request.content)
 assert str(request.url)=='http://localhost:18473/v1/chat/completions'
 assert request.headers['authorization']=='Bearer test-key'
 sent.append(body)
 if plan['failure']:raise httpx.ReadTimeout('software API timeout',request=request)
 reply=dict(plan['responses'][content_digest(effective_openai_messages(body['messages']))])
 reply['id']='native-'+directory.parent.name+'-'+str(len(sent))
 return httpx.Response(200,request=request,json=reply)
httpx.AsyncClient.send=send
def execute(rows,*,model,sampling_params,provider_options,on_result_ready):
 async def all_rows():
  predictions=[]
  async with httpx.AsyncClient(headers={'authorization':'Bearer '+provider_options['api_key']}) as client:
   for row in rows:
    messages=row['messages'] if plan.get('raw_messages',False) else effective_openai_messages(row['messages'])
    body=dict(sampling_params,model=model,messages=messages)
    response=await client.post(provider_options['base_url'].rstrip('/')+'/chat/completions',json=body)
    response.raise_for_status()
    predictions.append(dict(call_id=row['call_id'],generated_text=response.json()['choices'][0]['message']['content']))
  return predictions
 result=dict(predictions=asyncio.run(all_rows()),metadata=dict(
  result_ready_monotonic_ns=time.monotonic_ns(),model_close_receipt=None,model_process_owned=False,
  message_encoding='daft.openai.system_single_user.parts.v1',target_renderer_equivalence_verified=None))
 on_result_ready(result)
 return result
run_worker(config,directory,execute_query=execute)
'''


class AttachedSupervisor:
    def __init__(self, plan):
        self.plan, self.calls = Path(plan), []

    def __call__(self, config_path, directory, **kwargs):
        self.calls.append((read_json(config_path), Path(directory)))
        def launch(command, output, **options):
            process = subprocess.Popen([sys.executable, '-B', '-c', CHILD, str(config_path), str(directory),
                str(self.plan), str(Path(__file__).resolve().parents[2])],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            until = time.monotonic() + options['timeout_s']
            try:
                while True:
                    options['monitor'](())
                    if process.poll() is not None:
                        if process.returncode:
                            raise RuntimeError('software attached worker failed')
                        return dict(status='software child exited', model_calls=0,
                                    actual_SDK=False, physical_GPU_exit_confirmed=None)
                    if time.monotonic() >= until:
                        raise TimeoutError('software attached child exceeded allowance')
                    time.sleep(.005)
            finally:
                try:
                    process.wait(timeout=options['grace_s'])
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=options['grace_s'])
                raw, _ = process.communicate(timeout=options['grace_s'])
                from src.infrastructure.redact import redact_text
                Path(directory, 'CPU-worker.log').write_text(redact_text(raw))
        with patch('src.experiments.lotus_records_supervisor.supervise', side_effect=launch):
            return worker.supervised_run(config_path, directory, **kwargs)


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK, 'requires pinned SemBench and existing LOTUS SDK')
class AttachedOriginalQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.NativeLotusImageQueryTests.setUpClass()

    def setUp(self):
        self.visual = fixture.NativeLotusImageQueryTests()
        self.visual.setUp()
        self.addCleanup(self.visual.doCleanups)
        guard = patch.dict(os.environ, CPU_DAFT_BASE='http://localhost:18473/v1', CPU_DAFT_KEY=fixture.KEY_VALUE)
        guard.start()
        self.addCleanup(guard.stop)

    def reference(self, scenario, query, inputs):
        service, _ = self.visual.config(scenario, query)
        policy = fixture.NativeHTTPReplies(self, scenario, query=query)
        responses = {}
        original = policy.respond
        def record(request):
            response = original(request)
            digest = content_digest(effective_openai_messages(json.loads(request.content)['messages']))
            value = response.json()
            if digest in responses:
                self.assertEqual(responses[digest]['choices'], value['choices'])
            responses[digest] = value
            return response
        policy.respond = record
        expected, _, _ = self.visual.invoke(service, inputs, policy, allowance=64)
        config = dict(schema='semloom.native_daft_sembench.v1', sembench_checkout=str(self.visual.checkout),
            data_dir=None if 'data_dir' not in inputs else str(inputs['data_dir']), scenario=scenario, query=query,
            query_id='attached-'+scenario+'-'+query, model_name=service['model_name'], model='local-image-fixture',
            sampling_params=GenerationSpec.from_dict(service['generation']).kwargs(), engine_kwargs={},
            provider_options=dict(max_retries=0), native_openai=dict(base_url_env='CPU_DAFT_BASE', api_key_env='CPU_DAFT_KEY'),
            max_model_calls=64, query_timeout_s=60, cleanup_s=3, stop_grace_s=.5)
        config.update(dict(image_join=True) if scenario == 'mmqa' else dict(visual_query=True))
        config.update({name: str(value) if isinstance(value, Path) else value for name, value in inputs.items()
                       if name != 'data_dir'})
        return expected, policy, responses, config

    def test_original_multistage_queries_keep_complete_sink_messages_and_ACK(self):
        for scenario, query, inputs, counts in (('animals', 'Q7', self.visual.animals_inputs(), [4,4]),
                ('mmqa', 'Q2b', self.visual.mmqa_inputs(), [4,4]),
                ('ecomm', 'Q12', self.visual.ecomm_inputs(), [4,3])):
            with self.subTest(scenario=scenario):
                expected, policy, responses, config = self.reference(scenario, query, inputs)
                root = self.visual.root / ('native-'+scenario)
                plan = self.visual.root / (scenario+'-HTTP-plan.json')
                write_private_json(plan, dict(responses=responses, failure=False))
                supervise = AttachedSupervisor(plan)
                report = driver.run_query(config, root, model_python=sys.executable, supervise=supervise)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(report['result'], expected['result'])
                self.assertEqual(report['quality'], expected['quality'])
                self.assertEqual([len(read_json(Path(cfg['requests_path']))['requests']) for cfg, _ in supervise.calls], counts)
                self.assertEqual(report['native_HTTP_audit']['successful_responses'], sum(counts))
                self.assertEqual(report['native_HTTP_attempts'], sum(counts))
                self.assertEqual(report['native_HTTP_audit']['unresolved_attempts'], 0)
                self.assertFalse(report['native_HTTP_audit']['raw_HTTP_call_id_association_available'])
                self.assertIsNone(report['model_ready_before_source'])
                self.assertIsNone(report['model_close_receipt'])
                self.assertIsNone(report['cost'])
                self.assertFalse(report['cost_available'])
                self.assertTrue(report['query_thread_closed'])
                self.assertEqual(report['baseline_name'], 'LOTUS over Daft native AI')
                self.assertEqual(report['message_encoding'], 'daft.openai.system_single_user.parts.v1')
                self.assertIsNone(report['target_renderer_equivalence_verified'])
                self.assertLessEqual(report['sink_ready_ns'], report['semantic_audit_started_ns'])
                for index, (_, directory) in enumerate(supervise.calls):
                    terminal = read_json(directory/'worker.json')
                    self.assertEqual(terminal['status'], 'completed')
                    self.assertTrue(terminal['result_release']['confirmed'])
                    self.assertFalse(terminal['model_process_owned'])
                    self.assertFalse(terminal['remote_service_shutdown_owned'])
                    self.assertEqual(terminal['native_api']['reserved_attempts'], counts[index])
                    self.assertEqual(terminal['native_api']['http_send_entries'], counts[index])
                    self.assertNotIn(fixture.KEY_VALUE, json.dumps(terminal))
                    if index == len(supervise.calls)-1:
                        self.assertLessEqual(report['sink_ready_ns'], terminal['result_release']['released_monotonic_ns'])
                packets = [read_json(Path(cfg['requests_path'])) for cfg, _ in supervise.calls]
                actual = Counter(content_digest(row['messages']) for packet in packets for row in packet['requests'])
                self.assertEqual(actual, Counter(content_digest(row['body']['messages']) for row in policy.requests))
                encoded = [dict(batch_index=index, call_id=row['call_id'],
                    original_messages_sha256=content_digest(row['messages']),
                    effective_messages_sha256=content_digest(effective_openai_messages(row['messages'])))
                    for index, packet in enumerate(packets) for row in packet['requests']]
                self.assertEqual(report['native_HTTP_audit']['message_occurrences'], encoded)
                expected_http = Counter(row['effective_messages_sha256'] for row in encoded)
                observed_http = Counter(call['messages_sha256'] for _, directory in supervise.calls
                    for call in read_json(directory/'worker.json')['native_api']['requests'])
                self.assertEqual(observed_http, expected_http)
                changed = sum(row['original_messages_sha256'] != row['effective_messages_sha256'] for row in encoded)
                self.assertEqual(changed, 4 if scenario == 'ecomm' else 0)

    def test_unencoded_text_HTTP_is_rejected_after_the_complete_original_sink(self):
        expected, _, responses, config = self.reference('ecomm', 'Q12', self.visual.ecomm_inputs())
        plan = self.visual.root/'raw-HTTP-plan.json'
        write_private_json(plan, dict(responses=responses, failure=False, raw_messages=True))
        supervise = AttachedSupervisor(plan)
        with self.assertRaisesRegex(ValueError, 'effective complete message batch') as caught:
            driver.run_query(config, self.visual.root/'raw-native', model_python=sys.executable, supervise=supervise)
        report = caught.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['result'], expected['result'])
        self.assertNotIn('quality', report)
        self.assertEqual(report['physical_attempts_reserved'], 7)
        self.assertLessEqual(report['sink_ready_ns'], report['semantic_audit_started_ns'])

    def test_empty_filter_survivors_keep_the_original_KeyError_and_four_HTTP_attempts(self):
        _, policy, responses, config = self.reference('ecomm', 'Q12', self.visual.ecomm_inputs())
        for request in policy.requests:
            body = request['body']
            if body.get('response_format') != {'type': 'json_object'}:
                responses[content_digest(effective_openai_messages(body['messages']))]['choices'][0]['message']['content'] = 'False'
        plan = self.visual.root/'empty-HTTP-plan.json'
        write_private_json(plan, dict(responses=responses, failure=False))
        supervise = AttachedSupervisor(plan)
        with self.assertRaises(KeyError) as caught:
            driver.run_query(config, self.visual.root/'empty-native', model_python=sys.executable, supervise=supervise)
        report = caught.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertNotIn('result', report)
        self.assertNotIn('quality', report)
        self.assertIsNone(report.get('sink_ready_ns'))
        self.assertEqual(report['physical_attempts_reserved'], 4)
        self.assertEqual(len(supervise.calls), 1)
        terminal = read_json(supervise.calls[0][1]/'worker.json')
        self.assertEqual(terminal['native_api']['http_send_entries'], 4)
        self.assertEqual(terminal['native_api']['unresolved'], 0)

    def test_failed_HTTP_keeps_reserved_unknown_attempt_and_no_success_sink(self):
        _, _, responses, config = self.reference('animals', 'Q1', self.visual.animals_inputs())
        plan = self.visual.root/'failed-HTTP-plan.json'
        write_private_json(plan, dict(responses=responses, failure=True))
        supervise = AttachedSupervisor(plan)
        output = self.visual.root/'failed-native'
        with self.assertRaises(RuntimeError):
            driver.run_query(config, output, model_python=sys.executable, supervise=supervise)
        report = read_json(output/'driver.json')
        self.assertEqual(report['status'], 'failed')
        self.assertNotIn('result', report)
        self.assertNotIn('quality', report)
        self.assertEqual(report['physical_attempts_reserved'], 1)
        terminal = read_json(supervise.calls[0][1]/'worker.json')
        self.assertEqual(terminal['native_api']['reserved_attempts'], 1)
        self.assertEqual(terminal['native_api']['unresolved'], 1)
        self.assertFalse(terminal['result_ready_recorded'])

    def test_provider_configuration_keeps_credentials_external_and_rejects_conflicting_ownership(self):
        _, _, _, config = self.reference('animals', 'Q1', self.visual.animals_inputs())
        for change in (dict(engine_kwargs=dict(max_model_len=32768)),
                       dict(provider_options=dict(api_key='conflicting')),
                       dict(native_openai=dict(base_url_env='CPU_DAFT_BASE'))):
            with self.subTest(change=change), self.assertRaises(ValueError):
                worker.native_openai_options(dict(config, **change))
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(KeyError):
            worker.native_openai_options(config)


if __name__ == '__main__':
    unittest.main()
