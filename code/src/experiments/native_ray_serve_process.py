"""One isolated native Serve caller, with driver-owned tasks and evaluation.

Only native request batches cross this process boundary. Existing Ray Serve
owns inference and the existing records supervisor owns process-tree cleanup.
"""

import argparse
from copy import deepcopy
import json
import math
from pathlib import Path
import threading
import time

from src.infrastructure.private_artifacts import (
    new_private_directory, publish_private_json, read_json,
)
from src.infrastructure.redact import redact_text


def _read_packet(path):
    # These exclusive private packets already have a finite actual file size.
    # The generic small-JSON default must not truncate legitimate image batches.
    return read_json(path, max_bytes=path.stat().st_size)


def _owner_summary(owner):
    return {key: value for key, value in owner.report.items() if key != 'batches'}


def serve(config_path, *, native_factory=None):
    from src.baselines.text.frameworks.ray_native_serve_session import NativeServeSession

    config = _read_packet(Path(config_path))
    directory = Path(config['directory'])
    owner = (native_factory or NativeServeSession)(**config['native_options'])
    failure = None
    try:
        owner.start()
        publish_private_json(directory/'ready.json', dict(status='ready', owner=_owner_summary(owner)))
        index = 1
        while True:
            path = directory/('command-'+str(index)+'.json')
            while not path.exists():
                if (directory/'stop').exists():
                    break
                if time.monotonic() >= config['work_deadline']:
                    raise TimeoutError('native Serve session exceeded its work allowance')
                time.sleep(.01)
            if not path.exists():
                break
            command = _read_packet(path)
            if command['op'] == 'close':
                break
            if command['op'] != 'predict':
                raise ValueError('native Serve process accepts predict or close only')
            batch = owner.predict(command['requests'], sampling_params=command['sampling_params'])
            publish_private_json(directory/('reply-'+str(index)+'.json'), dict(status='completed', batch=batch))
            index += 1
    except BaseException as error:
        failure = error
    finally:
        try:
            owner.close()
        except BaseException as error:
            if failure is None:
                failure = error
            else:
                failure.add_note('native Serve close also failed: '+type(error).__name__)
        publish_private_json(directory/'terminal.json', dict(
            status='closed' if failure is None else 'failed', owner=_owner_summary(owner),
            error_type=None if failure is None else type(failure).__name__,
            error=None if failure is None else redact_text(str(failure)),
            notes=[] if failure is None else [redact_text(note) for note in getattr(failure, '__notes__', [])]))
    if failure is not None:
        raise failure


class NativeServeProcess:
    """A single persistent native caller; no semantic or batching policy."""

    def __init__(self, *, model_python, directory, startup_timeout_s, session_timeout_s,
                 cleanup_s, native_options, supervise=None):
        from src.baselines.text.frameworks.ray_native_serve_session import check_options

        check_options(native_options['runtime_options'], native_options['processor_options'])
        for name, value in (('startup_timeout_s', startup_timeout_s), ('session_timeout_s', session_timeout_s),
                            ('cleanup_s', cleanup_s)):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(name+' must be finite and positive')
        if cleanup_s >= session_timeout_s:
            raise ValueError('native Serve cleanup must fit inside its session allowance')
        self.model_python, self.directory = model_python, Path(directory)
        self.startup_timeout_s, self.session_timeout_s, self.cleanup_s = startup_timeout_s, session_timeout_s, cleanup_s
        self.native_options, self.supervise = deepcopy(native_options), supervise
        self.report = dict(profile='native-ray-serve-json-isolated-caller', events={}, batches=[],
            caller_profile='isolated-native-caller', caller_python=model_python,
            evaluation_in_driver=True, physical_gpu_exit_confirmed=None)
        self.sequence, self.thread, self.failure, self.closed = 0, None, None, False
        self.done = threading.Event()

    def _command(self, path):
        return ['env', 'PYTHONPATH='+str(Path(__file__).resolve().parents[2]),
                self.model_python, '-B', '-m', __name__, '--serve', str(path)]

    def _wait(self, path, deadline):
        while True:
            if path.exists():
                return _read_packet(path)
            terminal = self.directory/'terminal.json'
            if terminal.exists():
                value = _read_packet(terminal)
                self.report.update(value['owner'], native_caller_terminal=value)
                if value['status'] == 'closed':
                    raise EOFError('native Serve caller closed before its complete reply')
                raise RuntimeError('native Serve caller '+str(value['error_type'])+': '+str(value['error']))
            if self.done.is_set():
                if self.failure is not None:
                    raise self.failure
                raise EOFError('native Serve caller exited before its complete reply')
            if time.monotonic() >= deadline:
                raise TimeoutError('native Serve caller did not reply within its allowance')
            time.sleep(.01)

    def start(self):
        if self.thread is not None or self.closed:
            raise RuntimeError('native Serve caller cannot restart')
        from src.experiments.lotus_records_supervisor import supervise

        new_private_directory(self.directory)
        started = time.monotonic()
        self.deadline = started+self.session_timeout_s
        self.work_deadline = self.deadline-min(self.cleanup_s, self.session_timeout_s/3)
        path = self.directory/'native-config.json'
        publish_private_json(path, dict(directory=str(self.directory), native_options=self.native_options,
                                       work_deadline=self.work_deadline))
        def run():
            try:
                self.report['supervision'] = (self.supervise or supervise)(self._command(path),
                    self.directory/'supervision', timeout_s=self.session_timeout_s, grace_s=self.cleanup_s,
                    natural_exit_grace_s=self.cleanup_s)
            except BaseException as error:
                self.failure = error
                if hasattr(error, 'report'):
                    self.report['supervision'] = error.report
            finally:
                self.done.set()
        self.thread = threading.Thread(target=run, name='native-ray-serve-caller')
        self.thread.start()
        ready = self._wait(self.directory/'ready.json', min(self.work_deadline, started+self.startup_timeout_s))
        self.report.update(ready['owner'])
        self.report['native_caller_startup_seconds'] = time.monotonic()-started

    def predict(self, requests, *, sampling_params):
        if self.thread is None or self.closed:
            raise RuntimeError('native Serve caller is not open')
        self.sequence += 1
        publish_private_json(self.directory/('command-'+str(self.sequence)+'.json'),
            dict(op='predict', requests=requests, sampling_params=sampling_params))
        value = self._wait(self.directory/('reply-'+str(self.sequence)+'.json'), self.work_deadline)
        self.report['batches'].append(value['batch'])
        return value['batch']

    def identity_snapshot(self):
        """Fresh host identities, bound to the native actor constructor event."""
        import psutil

        output = Path(json.loads(self.native_options['model_runtime_env']['env_vars']['SEMLOOM_RAY_NATIVE_OBSERVER'])['output'])
        events = [json.loads(line) for path in output.glob('native-events-*.jsonl') for line in path.read_text().splitlines()]
        actors = {tuple(row.get(key) for key in ('pid', 'node_id', 'model_actor_id', 'model_actor_create_time'))
                  for row in events if row['event'] == 'native_constructor_returned'}
        value = dict(observed_monotonic_ns=time.monotonic_ns(), model_processes=[],
                     scope='native actor context plus fresh host PID/birth; not cache or weight identity')
        if len(actors) != 1:
            return value
        pid, node, actor, birth = next(iter(actors))
        if not all((pid, node, actor, birth)):
            return value
        try:
            actor_process = psutil.Process(pid)
            if actor_process.create_time() != birth:
                return value
            value.update(node_id=node, model_actor_id=actor, model_actor_pid=pid, model_actor_create_time=birth)
            for child in actor_process.children(recursive=True):
                command = child.cmdline()
                if command and command[0].startswith('VLLM::EngineCore'):
                    value['model_processes'].append(dict(pid=child.pid, create_time=child.create_time()))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            value['model_processes'] = []
        value['observed_monotonic_ns'] = time.monotonic_ns()
        return value

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.thread is None:
            return
        # Stop is also visible when a failed command left no usable reply.
        (self.directory/'stop').touch(mode=0o600, exist_ok=False)
        self.sequence += 1
        publish_private_json(self.directory/('command-'+str(self.sequence)+'.json'), dict(op='close'))
        self.thread.join(timeout=max(0, self.deadline-time.monotonic()))
        terminal = self.directory/'terminal.json'
        if terminal.exists():
            value = _read_packet(terminal)
            self.report.update(value['owner'], native_caller_terminal=value)
        else:
            value = None
        if self.thread.is_alive():
            raise TimeoutError('native Serve caller cleanup did not finish within its session allowance')
        if value is not None and value['status'] != 'closed':
            raise RuntimeError('native Serve caller '+str(value['error_type'])+': '+str(value['error']))
        if self.failure is not None:
            raise self.failure
        if value is None:
            raise RuntimeError('native Serve caller did not confirm native close')


def check_sdk(config):
    """Import and validate the native caller config, without starting Ray/model."""
    import sys
    from importlib.metadata import version
    from src.baselines.text.frameworks.ray_native_serve_session import _load_native, _input, check_options

    check_options(config['runtime_options'], config['processor_options'])
    sdk = _load_native()
    if sdk.ray.__version__ != '2.56.1':
        raise ValueError('the native Serve caller is fixed to Ray 2.56.1')
    sdk.LLMConfig(model_loading_config=dict(model_id=config['model'], model_source=config['model_source']),
        engine_kwargs=config['engine_kwargs'], runtime_env=dict(py_executable=config['model_python']),
        deployment_config=dict(name=config['deployment_name'], num_replicas=1))
    sdk.ServeDeploymentProcessorConfig(app_name=config['app_name'], deployment_name=config['deployment_name'],
        dtype_mapping={'ChatCompletionRequest': sdk.ChatCompletionRequest}, should_continue_on_error=False,
        **config['processor_options'])
    request = _input(dict(messages=[dict(role='user', content='configuration check only')]),
                     model=config['model'], sampling_params=config['sampling_params'])
    sdk.ChatCompletionRequest(**request['request_kwargs'])
    torch = sys.modules.get('torch')
    return dict(status='checked_native_caller_config_only', python=sys.executable,
        versions={name: version(name) for name in ('ray', 'vllm')},
        ray_runtime_started=sdk.ray.is_initialized(), CUDA_initialized=None if torch is None else torch.cuda.is_initialized(),
        model_calls=0, model_constructed=False, evaluation_dependencies_required=False,
        scope='native public Serve/processor/request configuration; not deployment or model qualification')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--serve')
    action.add_argument('--check-sdk')
    args = parser.parse_args(argv)
    if args.check_sdk:
        print(json.dumps(check_sdk(_read_packet(Path(args.check_sdk))), ensure_ascii=False, allow_nan=False))
    else:
        serve(args.serve)


if __name__ == '__main__':
    main()
