"""Real isolated caller IPC/exit; Serve and generation replies remain software."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from src.experiments import native_ray_serve_process as process
from src.infrastructure.private_artifacts import read_json, write_private_json
from src.infrastructure.redact import redact_text


CHILD = r'''
import importlib.abc,socket,sys
class NoEngine(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name.split('.')[0] in {'ray','vllm','torch','transformers','lotus'}:
   raise AssertionError('software caller imported actual SDK: '+name)
sys.meta_path.insert(0,NoEngine())
def offline(event,args):
 if event in ('socket.connect','socket.bind') and args[0].family in (socket.AF_INET,socket.AF_INET6):
  raise AssertionError('software caller accessed INET')
sys.addaudithook(offline)
from src.experiments.native_ray_serve_process import serve
from tests.experiments.test_sembench_native_ray import SoftwareServeSession
class Owner(SoftwareServeSession):
 def start(self):
  if sys.argv[2]=='start-failure':raise ValueError('software caller startup failed')
  super().start()
 def predict(self,*args,**kwargs):
  if sys.argv[2]=='prediction-failure':raise ValueError('software caller prediction failed')
  return super().predict(*args,**kwargs)
 def close(self):
  if sys.argv[2]=='start-failure':return
  super().close()
serve(sys.argv[1],native_factory=Owner)
'''


def known_child_supervisor(command, directory, *, timeout_s, grace_s, **kwargs):
    """One known Popen child only; host/GPU tree qualification remains separate."""
    directory = Path(directory)
    directory.mkdir(mode=0o700)
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        output, _ = child.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        child.kill()
        output, _ = child.communicate(timeout=grace_s)
        raise
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=grace_s)
    (directory/'worker.log').write_text(redact_text(output))
    if child.returncode:
        raise RuntimeError('software native caller failed; see private worker.log')
    return dict(status='CPU software child exited', worker_exit=child.returncode,
                scope='one known child; host/GPU cleanup unverified', model_calls=0)


class NativeServeProcessTests(unittest.TestCase):
    def setUp(self):
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = CellBudgetLedger.create(self.root/'budget.sqlite', AttemptBudget('isolated', 4),
                                            deadline_utc=time.time()+60)
        self.ledger.reserve_unit('session', 4)
        self.ledger.claim_shared_unit('session')
        self.options = dict(model='software', model_source='unused', app_name='owned', deployment_name='model',
            engine_kwargs={}, processor_options={}, runtime_options={}, model_runtime_env=dict(
                py_executable=sys.executable, env_vars=dict(PYTHONPATH=str(Path(__file__).resolve().parents[2]),
                    SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(dict(path=str(self.ledger.path), id='isolated',
                        limit=4, unit_id='session', output=str(self.root), observe_model_actor_identity=True)))))

    def owner(self, mode='success'):
        owner = process.NativeServeProcess(model_python=sys.executable, directory=self.root/'caller',
            startup_timeout_s=10, session_timeout_s=30, cleanup_s=3, native_options=self.options,
            supervise=known_child_supervisor)
        owner._command = lambda path: [sys.executable, '-B', '-c', CHILD, str(path), mode]
        return owner

    def test_one_real_caller_handles_two_batches_and_native_close_without_driver_SDK(self):
        owner = self.owner()
        owner.start()
        try:
            for i in range(2):
                batch = owner.predict([dict(call_id='row-'+str(i), messages=[dict(role='user', content='{"text":"good"}')])],
                    sampling_params=dict(n=1, structured_outputs={'json_object':True}))
                self.assertEqual(batch['predictions'][0]['generated_text'], '{"keep": true}')
            self.assertEqual(len(owner.report['batches']), 2)
            self.assertFalse(owner.done.is_set())
            self.assertNotIn('model_actor_id', owner.identity_snapshot())
        finally:
            self.ledger.close_shared_unit('session')
            owner.close()
        owner.close()
        self.assertEqual(owner.report['supervision']['worker_exit'], 0)
        self.assertEqual(read_json(self.root/'caller/terminal.json')['status'], 'closed')
        self.assertTrue(owner.done.is_set())

    def test_concurrent_group_threads_keep_each_command_and_reply_associated(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier

        owner=self.owner();owner.start();barrier=Barrier(2)
        def predict(index):
            barrier.wait(timeout=5)
            key='thread-'+str(index)
            batch=owner.predict([dict(call_id=key,messages=[dict(role='user',content='{"text":"good"}')])],
                sampling_params=dict(n=1,structured_outputs={'json_object':True}))
            return key,batch['predictions'][0]['call_id']
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                pairs=list(pool.map(predict,range(2)))
            self.assertTrue(all(expected==actual for expected,actual in pairs))
            self.assertEqual(len(owner.report['batches']),2)
        finally:
            self.ledger.close_shared_unit('session');owner.close()
        self.assertEqual(read_json(self.root/'caller/terminal.json')['status'],'closed')

    def test_expired_work_allowance_cannot_publish_another_prediction_command(self):
        owner=self.owner();owner.start()
        try:
            owner.work_deadline=time.monotonic()-1
            with self.assertRaisesRegex(TimeoutError,'before submission'):
                owner.predict([dict(call_id='late',messages=[])],sampling_params={'n':1})
            self.assertFalse((owner.directory/'command-1.json').exists())
        finally:
            self.ledger.close_shared_unit('session');owner.close()

    def test_native_startup_and_prediction_failures_keep_error_and_exit(self):
        for mode in ('start-failure', 'prediction-failure'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                self.root = Path(temporary)
                # Rebind the same existing finite ledger; failed software work
                # must not create a replacement allowance.
                self.options['model_runtime_env']['env_vars']['SEMLOOM_RAY_NATIVE_OBSERVER'] = json.dumps(dict(
                    path=str(self.ledger.path), id='isolated', limit=4, unit_id='session', output=str(self.root)))
                owner = self.owner(mode)
                try:
                    with self.assertRaisesRegex(RuntimeError, 'software caller'):
                        owner.start()
                        owner.predict([dict(call_id='row', messages=[dict(role='user',content='{"text":"good"}')])],
                            sampling_params=dict(n=1, structured_outputs={'json_object':True}))
                finally:
                    self.ledger.close_shared_unit('session')
                    with self.assertRaises(RuntimeError):
                        owner.close()
                self.assertFalse(owner.thread.is_alive())
                terminal = read_json(self.root/'caller/terminal.json')
                self.assertEqual(terminal['status'], 'failed')
                self.assertEqual(terminal['error_type'], 'ValueError')

    def test_large_image_packet_has_no_generic_small_JSON_limit(self):
        path = self.root/'actual-large.json'
        data = 'data:image/png;base64,'+'A'*(8*1024**2+1)
        write_private_json(path, dict(image=data))
        self.assertEqual(process._read_packet(path)['image'], data)

    def test_full_driver_evaluates_two_fresh_sources_while_caller_stays_isolated(self):
        from src.experiments.sembench_native_ray import run_serve_session
        from tests.experiments.test_sembench_native_ray import ServeSessionTests

        fixture = ServeSessionTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        config = fixture.config()
        def factory(value, directory, runtime):
            options = dict(self.options, model_runtime_env=runtime)
            owner = process.NativeServeProcess(model_python=sys.executable, directory=directory,
                startup_timeout_s=10, session_timeout_s=30, cleanup_s=3, native_options=options,
                supervise=known_child_supervisor)
            owner._command = lambda path: [sys.executable, '-B', '-c', CHILD, str(path), 'success']
            return owner
        with patch('src.experiments.native_ray_serve_process._native_serve_process', side_effect=factory):
            report = run_serve_session(config, fixture.root/'session', checkout='software', task_factory=fixture.tasks)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(report['physical_attempts_reserved'], 4)
        self.assertEqual(len(fixture.reads), 2)
        self.assertIsNone(report['warm_qualified'])
        self.assertTrue(report['native']['evaluation_in_driver'])
        self.assertEqual(report['native']['supervision']['worker_exit'], 0)

    def test_identity_uses_native_actor_and_actual_engine_child_births(self):
        record = dict(event='native_constructor_returned', pid=100, node_id='native-node',
                      model_actor_id='native-actor', model_actor_create_time=12.)
        (self.root/'native-events-100.jsonl').write_text(json.dumps(record)+'\n')
        child = SimpleNamespace(pid=101, cmdline=lambda: ['VLLM::EngineCore'], create_time=lambda: 13.)
        replica = SimpleNamespace(create_time=lambda: 12., children=lambda recursive: [child])
        owner = self.owner()
        with patch('psutil.Process', return_value=replica):
            snapshot = owner.identity_snapshot()
        self.assertEqual(snapshot['model_actor_id'], 'native-actor')
        self.assertEqual(snapshot['model_processes'], [dict(pid=101, create_time=13.)])
        self.assertGreater(snapshot['observed_monotonic_ns'], 0)
        with patch('psutil.Process', return_value=SimpleNamespace(create_time=lambda: 20.)):
            self.assertEqual(owner.identity_snapshot()['model_processes'], [])

    def test_SDK_gate_only_constructs_native_configuration_objects(self):
        from tests.baselines.text.test_ray_native_serve_session import Config

        native = SimpleNamespace(ray=SimpleNamespace(__version__='2.56.1', is_initialized=lambda: False),
            LLMConfig=Config, ServeDeploymentProcessorConfig=Config, ChatCompletionRequest=Config,
            NativeObserverCallback=object)
        config = dict(self.options, model_python='native-only',
            sampling_params=dict(n=1, max_tokens=32, structured_outputs={'json_object':True}))
        with patch('src.baselines.text.frameworks.ray_native_serve_session._load_native', return_value=native), \
                patch('importlib.metadata.version', side_effect=lambda name: {'ray':'2.56.1','vllm':'0.22.0'}[name]), \
                patch.dict(sys.modules, torch=SimpleNamespace(cuda=SimpleNamespace(is_initialized=lambda: False))):
            value = process.check_sdk(config)
        self.assertEqual(value['model_calls'], 0)
        self.assertFalse(value['model_constructed'])
        self.assertFalse(value['ray_runtime_started'])
        self.assertFalse(value['evaluation_dependencies_required'])

    def test_actor_setup_only_records_requested_identity_without_request_sidecars(self):
        from src.baselines.text.frameworks.ray_native_observer import setup

        modules = {}
        for name in ('vllm', 'vllm.v1', 'vllm.v1.engine', 'vllm.v1.engine.async_llm'):
            modules[name] = ModuleType(name)
            modules[name].__path__ = []
        for capture in (True, False):
            with self.subTest(capture=capture):
                calls = []
                output_value = object()
                class Engine:
                    def __init__(self):
                        pass
                    async def generate(self, prompt, sampling_params, request_id):
                        calls.append(request_id)
                        yield output_value
                modules['vllm.v1.engine.async_llm'].AsyncLLM = Engine
                def context():
                    self.assertTrue(capture, 'offline observer requested extra actor identity')
                    return SimpleNamespace(get_node_id=lambda: 'software-node', get_actor_id=lambda: 'software-actor')
                modules['ray'] = SimpleNamespace(get_runtime_context=context)
                spec = json.loads(self.options['model_runtime_env']['env_vars']['SEMLOOM_RAY_NATIVE_OBSERVER'])
                output = self.root/str(capture)
                output.mkdir()
                spec.update(output=str(output), observe_model_actor_identity=capture)
                with patch.dict(sys.modules, modules), patch.dict(os.environ, SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(spec)), \
                        patch('importlib.metadata.version', return_value='software-version'):
                    setup()
                    engine = Engine()
                    self.assertEqual(calls, [])
                    before = {path.name: path.read_bytes() for path in output.glob('native-events-*.jsonl')}
                    async def generate():
                        return [row async for row in engine.generate({}, SimpleNamespace(n=1), 'software-request')]
                    returned = asyncio.run(generate())
                self.assertIs(returned[0], output_value)
                self.assertEqual(calls, ['software-request'])
                self.assertEqual(list(output.glob('native-output-*.json')), [])
                self.assertEqual(before, {path.name: path.read_bytes() for path in output.glob('native-events-*.jsonl')})
                if capture:
                    rows = [json.loads(row) for raw in before.values() for row in raw.decode().splitlines()]
                    ready = next(row for row in rows if row['event'] == 'native_constructor_returned')
                    self.assertEqual(ready['pid'], os.getpid())
                    self.assertEqual(ready['model_actor_id'], 'software-actor')
                    self.assertEqual(ready['node_id'], 'software-node')
                    self.assertGreater(ready['model_actor_create_time'], 0)
                else:
                    self.assertEqual(before, {})

    def test_Serve_callback_preserves_native_worker_hook_and_node_initialization(self):
        events = []
        native_hook = 'ray.llm._internal.serve._worker_process_setup_hook'
        hook_env = '__RAY_WORKER_PROCESS_SETUP_HOOK_ENV_VAR'
        runtime = dict(worker_process_setup_hook=native_hook, env_vars={hook_env: native_hook})
        context = SimpleNamespace(runtime_env=runtime, placement_group=object(), run_init_node=True)

        class CallbackBase:
            def __init__(self):
                self.ctx = context
            async def on_before_node_init(self):
                events.append('native_before_node')
            async def on_after_node_init(self):
                events.append('native_after_node')

        modules = {}
        for name in ('ray', 'ray.llm', 'ray.llm._internal', 'ray.llm._internal.common',
                     'ray.llm._internal.common.callbacks', 'ray.llm._internal.common.callbacks.base'):
            modules[name] = ModuleType(name)
            modules[name].__path__ = []
        modules['ray.llm._internal.common.callbacks.base'].CallbackBase = CallbackBase
        path = Path(process.__file__).parents[1]/'baselines/text/frameworks/ray_native_serve_observer.py'
        spec = importlib.util.spec_from_file_location('software_serve_observer', path)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules), patch.dict(os.environ, {hook_env: native_hook}):
            spec.loader.exec_module(module)
            callback = module.NativeObserverCallback()
            async def start():
                await callback.on_before_node_init()
                events.append('native_initialize_node')
                await callback.on_after_node_init()
                events.append('native_model_constructor')
            with patch.object(module, 'setup', side_effect=lambda: events.append('observer_installed')) as install:
                asyncio.run(start())
            install.assert_called_once_with()
            self.assertEqual(os.environ[hook_env], native_hook)
            self.assertIs(callback.ctx, context)
            self.assertEqual(runtime, dict(worker_process_setup_hook=native_hook, env_vars={hook_env: native_hook}))
            with patch.object(module, 'setup', side_effect=ValueError('observer failed')):
                with self.assertRaisesRegex(ValueError, 'observer failed'):
                    asyncio.run(callback.on_after_node_init())
        self.assertEqual(events[:5], ['native_before_node', 'native_initialize_node',
            'native_after_node', 'observer_installed', 'native_model_constructor'])


if __name__ == '__main__':
    unittest.main()
