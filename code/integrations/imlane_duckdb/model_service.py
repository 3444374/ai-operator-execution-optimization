"""Own one external vLLM service beside the native DuckDB/IMLane query.

The caller owns the host lease, physical GPU/SHM checks and the total deadline.
No model SDK is imported here. Native vLLM owns model execution and scheduling.
"""

import json
import os
from pathlib import Path
import signal
import socket
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

import psutil

from integrations.imlane_records.owner import StopBudget, spawn
from src.infrastructure import owned_processes as owned
from src.infrastructure.private_artifacts import open_private_text, require_outside_git, write_private_json
from src.infrastructure.redact import redact_argument_list, redact_text


_IDENTITY_OPTIONS = {
    '--model', '--served-model-name', '--host', '--port', '--dtype',
    '--generation-config', '--override-generation-config', '--config', '-c',
    '--uds', '--grpc', '--api-server-count', '--headless', '--api-key',
    '--distributed-executor-backend', '--tensor-parallel-size', '-tp',
    '--pipeline-parallel-size', '-pp', '--data-parallel-size', '-dp', '-dpl', '-dph',
}
_RESOLVED_FIELDS = {
    'model_config': ('dtype', 'max_model_len', 'generation_config'),
    'scheduler_config': ('max_num_seqs', 'max_num_batched_tokens', 'max_model_len',
                         'policy', 'async_scheduling', 'enable_chunked_prefill'),
    'cache_config': ('gpu_memory_utilization', 'kv_cache_memory_bytes', 'block_size',
                     'enable_prefix_caching'),
    'parallel_config': ('tensor_parallel_size', 'pipeline_parallel_size', 'data_parallel_size'),
}


def _error(error):
    return dict(type=type(error).__name__, error=redact_text(str(error)))


def _deadline(value):
    owned._seconds(value, 'absolute monotonic deadline')
    return value


def _check_port(port):
    # The caller's host lease covers this check and the following service start.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(('127.0.0.1', port))
        probe.listen(1)


def _command(config):
    required = {'model_python', 'model', 'served_model_name', 'host', 'port', 'dtype', 'server_options'}
    if set(config) != required:
        raise ValueError('model_service requires exactly: ' + ', '.join(sorted(required)))
    if config['host'] != '127.0.0.1' or config['dtype'] != 'bfloat16':
        raise ValueError('model service requires host 127.0.0.1 and dtype bfloat16')
    if type(config['port']) is not int or not 1 <= config['port'] <= 65535:
        raise ValueError('model service requires an explicit nonzero TCP port')
    if not isinstance(config['served_model_name'], str) or not config['served_model_name'].strip():
        raise ValueError('served_model_name must be nonempty text')
    python, model = Path(config['model_python']).absolute(), Path(config['model']).resolve()
    if not python.is_file() or not model.is_dir():
        raise ValueError('model_python and local model directory must already exist')
    options = config['server_options']
    if not isinstance(options, list) or any(not isinstance(value, str) or not value or '\0' in value
                                            for value in options):
        raise ValueError('server_options must be an explicit argv list')
    for value in options:
        flag = value.split('=', 1)[0].replace('_', '-')
        if (flag == '--' or flag in _IDENTITY_OPTIONS
                or flag.startswith(('--data-parallel-', '--tensor-parallel-', '--pipeline-parallel-'))
                or (flag.startswith('--') and any(option.startswith(flag) for option in _IDENTITY_OPTIONS))):
            raise ValueError('server_options cannot override service identity or topology: ' + flag)
    # Keep the venv invocation path; resolving its python symlink loses that env.
    return [str(python), '-m', 'vllm.entrypoints.openai.api_server', '--model', str(model),
            '--served-model-name', config['served_model_name'], '--host', '127.0.0.1',
            '--port', str(config['port']), '--dtype', 'bfloat16', '--generation-config', 'vllm', *options]


class ModelService:
    def __init__(self, config, directory, *, environment, deadline, cleanup_deadline=None,
                 stop_budget=None, stop_grace_s=8):
        self.command = _command(config)
        self.directory = Path(directory).resolve()
        require_outside_git(self.directory)
        self.environment = dict(environment)
        self.deadline = _deadline(deadline)
        self.cleanup_deadline = cleanup_deadline
        owned._seconds(stop_grace_s, 'stop_grace_s')
        self.stop_grace_s = stop_grace_s
        self.stop_budget = stop_budget or StopBudget(threading.Event())
        self.served_model_name = config['served_model_name']
        self.port = config['port']
        self.base_url = 'http://127.0.0.1:' + str(config['port'])
        self.url = self.base_url + '/v1/chat/completions'
        self.ready_info = None
        self.process = self.writer = self.observer = None
        self.created = None
        self.spawn_attempted = False
        self.known = set()
        self.stop_observer = threading.Event()
        self.observation_error = None
        self.closed = False
        self.entered = False
        self.report = dict(schema='semloom.imlane.duckdb.q3.model_service.v1', status='configured',
            command=redact_argument_list(self.command), generation_config='vllm',
            generation_config_source='explicit project SamplingParams choice',
            query_deadline_initial=self.deadline, active_deadline=self.deadline,
            external_resource_verification='required: host lease/supervisor physical GPU and SHM checks',
            physical_resources_confirmed=None)

    def _write(self, phase):
        write_private_json(self.directory / ('model_service.' + phase + '.json'), self.report)

    def _check_liveness(self):
        self.stop_budget.check()
        if self.observation_error is not None:
            raise self.observation_error
        if self.process is not None and self.process.poll() is not None:
            raise RuntimeError('vLLM service exited: ' + str(self.process.returncode))

    def check(self):
        self._check_liveness()
        if time.monotonic() >= self.deadline:
            raise TimeoutError('model service work deadline exceeded')

    def _observe(self):
        owned._observe(self.known, self.process.pid, self.created)

    def _sample(self):
        try:
            while not self.stop_observer.wait(.1):
                self._observe()
                # Native READY has its own timestamp. The caller reads it before
                # handing over deadlines; sampling latency cannot invalidate it.
                self._check_liveness()
        except BaseException as error:
            if not self.stop_observer.is_set():
                self.observation_error = error
                self.report['observation_error'] = _error(error)

    def _get(self, path, *, json_body=True):
        self.check()
        headers = {}
        if self.environment.get('VLLM_API_KEY'):
            headers['Authorization'] = 'Bearer ' + self.environment['VLLM_API_KEY']
        request = Request(self.base_url + path, headers=headers, method='GET')
        # This service is loopback-only, irrespective of inherited proxy settings.
        with build_opener(ProxyHandler({})).open(
                request, timeout=min(1.0, max(.001, self.deadline - time.monotonic()))) as response:
            return json.load(response) if json_body else response.status

    def _ready(self):
        while True:
            self.check()
            try:
                self._get('/health', json_body=False)
                models = self._get('/v1/models')
                model = next((row for row in models['data'] if row['id'] == self.served_model_name), None)
                if model is None:
                    raise ValueError('ready service does not expose the requested served_model_name')
                length = model.get('max_model_len')
                if length is not None and (type(length) is not int or length < 1):
                    raise ValueError('service max_model_len must be positive or unknown')
                self.ready_info = dict(served_model_name=model['id'], max_model_len=length,
                    server_info=dict(status='unknown', resolved=None))
                break
            except HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504):
                    raise
                time.sleep(min(.1, max(0.0, self.deadline - time.monotonic())))
            except (URLError, TimeoutError, ConnectionError):
                # Only readiness GETs retry. No generation endpoint is called here.
                time.sleep(min(.1, max(0.0, self.deadline - time.monotonic())))
        try:
            info = self._get('/server_info?config_format=json')
            config = info['vllm_config']
            if not isinstance(config, dict):
                raise ValueError('server_info vllm_config is not a JSON object')
            resolved = {section: {field: config.get(section, {}).get(field) for field in fields}
                        for section, fields in _RESOLVED_FIELDS.items() if isinstance(config.get(section, {}), dict)}
            self.ready_info['server_info'] = dict(status='available', resolved=resolved)
        except (HTTPError, URLError, TimeoutError, ConnectionError, ValueError, KeyError, TypeError) as error:
            # v0.25.1 registers this route only in explicit server dev mode.
            self.ready_info['server_info']['reason'] = _error(error)
        self.check()

    def __enter__(self):
        if self.entered or self.closed:
            raise RuntimeError('model service context may be entered only once')
        self.entered = True
        try:
            self.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            for phase in ('started', 'ready', 'final'):
                if (self.directory / ('model_service.' + phase + '.json')).exists():
                    raise FileExistsError('model service evidence already exists')
            self.check()
            log = self.directory / 'model_service.log'
            with open_private_text(log):
                pass
            _check_port(self.port)
            self.report.update(status='starting', started_monotonic=time.monotonic())
            self.spawn_attempted = True
            self.process, self.writer = spawn(self.command, self.environment, log)
            self.report['process'] = dict(pid=self.process.pid, create_time=None, pgid=None)
            self.created = psutil.Process(self.process.pid).create_time()
            if os.getpgid(self.process.pid) != self.process.pid:
                raise RuntimeError('model service did not start in its own process group')
            self.known.add((self.process.pid, self.created))
            self.report['process'] = dict(pid=self.process.pid, create_time=self.created, pgid=self.process.pid)
            self._write('started')
            self._observe()
            self.observer = threading.Thread(target=self._sample, daemon=True)
            self.observer.start()
            self._ready()
            self.report.update(status='ready', ready_monotonic=time.monotonic(), ready_info=self.ready_info)
            self._write('ready')
            return self
        except BaseException as error:
            self.report.update(status='failed', error=_error(error))
            try:
                self.close()
            except BaseException as cleanup:
                error.add_note('model service startup cleanup also failed: ' + redact_text(str(cleanup)))
            raise

    def _close_deadline(self, value):
        if value is None:
            value = self.cleanup_deadline() if callable(self.cleanup_deadline) else self.cleanup_deadline
        return _deadline(self.deadline if value is None else value)

    def begin_cleanup(self, deadline):
        """Hand a saved native READY over to the caller's single cleanup window."""
        self.deadline = _deadline(deadline)
        self.report.update(active_deadline=self.deadline, cleanup_window_deadline=self.deadline)
        if 'cleanup_began_monotonic' not in self.report:
            self.report['cleanup_began_monotonic'] = time.monotonic()

    def close(self, *, deadline=None):
        if self.closed:
            return self.report
        errors = []
        try:
            until = self._close_deadline(deadline)
        except BaseException as error:
            errors.append(error)
            until = time.monotonic()
        cleanup = dict(deadline=until, forced_kill=False, signal_events=[], exit_events=[],
                       cleanup_errors=[], observed_owned_exit_confirmed=False)
        self.report['cleanup'] = cleanup
        self.report['active_deadline'] = self.deadline

        def attempt(action):
            try:
                return action()
            except BaseException as error:
                errors.append(error)
                cleanup['signal_events'].extend(getattr(error, 'signal_events', []))
                return None

        def send(identities, number):
            events = owned._signal(identities, self.process.pid, self.created, number)
            cleanup['signal_events'].extend(events)

        def wait(deadline, phase):
            owned._wait(self.known, self.process.pid, self.created, deadline,
                        exit_events=cleanup['exit_events'], phase=phase, watch=self.known)

        with self.stop_budget.cleanup():
            try:
                self.stop_observer.set()
                if self.observer is not None:
                    attempt(lambda: self.observer.join(max(0.0, until - time.monotonic())))
                if self.observation_error is not None:
                    errors.append(self.observation_error)
                if self.process is not None:
                    if self.created is None:
                        errors.append(RuntimeError('spawned model owner identity is unconfirmed'))
                    else:
                        attempt(self._observe)
                        if self.report['status'] == 'ready' and self.process.poll() is not None:
                            errors.append(RuntimeError('model service exited before owner close'))
                        # Let the API owner perform native shutdown before descendants.
                        attempt(lambda: send({(self.process.pid, self.created)}, signal.SIGTERM))
                        attempt(lambda: wait(min(until, time.monotonic() + self.stop_grace_s), 'owner_term'))
                        remaining = attempt(lambda: owned._live(self.known))
                        if remaining:
                            attempt(lambda: send(self.known, signal.SIGTERM))
                            attempt(lambda: wait(max(time.monotonic(), until - self.stop_grace_s), 'descendants_term'))
                        remaining = attempt(lambda: owned._live(self.known))
                        if remaining:
                            cleanup['forced_kill'] = True
                            attempt(lambda: send(self.known, signal.SIGKILL))
                            attempt(lambda: wait(until, 'forced_kill'))
                        remaining = attempt(lambda: owned._live(self.known))
                        cleanup['owned_survivors'] = (None if remaining is None else
                            [dict(pid=p.pid, create_time=p.create_time()) for p in remaining])
                        attempt(lambda: self.process.wait(timeout=max(0.0, until - time.monotonic())))
                    cleanup['owner_returncode'] = self.process.returncode
                    if self.process.returncode != 0:
                        errors.append(RuntimeError('vLLM service owner did not exit zero: '
                                                   + str(self.process.returncode)))
                else:
                    cleanup['owned_survivors'] = []
                    if self.spawn_attempted:
                        errors.append(RuntimeError('service spawn did not return an owner identity'))
                if self.writer is not None:
                    attempt(lambda: self.writer.join(max(0.0, until - time.monotonic())))
                cleanup['observer_stopped'] = self.observer is None or not self.observer.is_alive()
                cleanup['log_writer_stopped'] = self.writer is None or not self.writer.is_alive()
                if not cleanup['observer_stopped'] or not cleanup['log_writer_stopped']:
                    errors.append(RuntimeError('model owner observation/log thread did not stop'))
                cleanup['observed_owned_exit_confirmed'] = (
                    cleanup.get('owned_survivors') == [] and (self.process is None or self.created is not None)
                    and cleanup['observer_stopped'] and cleanup['log_writer_stopped'] and not errors)
                if cleanup['forced_kill']:
                    errors.append(RuntimeError('model service required forced SIGKILL'))
                if cleanup.get('owned_survivors'):
                    errors.append(RuntimeError('model service owned processes remain alive'))
            except BaseException as error:
                errors.append(error)
            finally:
                cleanup['cleanup_errors'] = [_error(error) for error in errors]
                self.closed = True
                failed = self.report['status'] == 'failed' or bool(errors)
                self.report.update(status='failed' if failed else 'closed', finished_monotonic=time.monotonic())
                try:
                    self._write('final')
                except BaseException as error:
                    errors.append(error)
                    self.report.update(status='failed', final_report_error=_error(error))
        if errors:
            raise errors[0]
        return self.report

    def __exit__(self, kind, error, traceback):
        if error is not None:
            self.report.update(status='failed', error=_error(error))
        try:
            self.close()
        except BaseException as cleanup:
            if error is None:
                raise
            error.add_note('model service cleanup also failed: ' + redact_text(str(cleanup)))
        return False
