"""Own one external vLLM service beside the native DuckDB/IMLane query.

The caller owns the host lease, physical GPU/SHM checks and the total deadline.
No model SDK is imported here. Native vLLM owns model execution and scheduling.
"""

from copy import deepcopy
import json
import os
from pathlib import Path
import signal
import stat
import threading
import time
import uuid

import psutil

from integrations.imlane_records.owner import StopBudget, spawn
from src.infrastructure import owned_processes as owned
from src.infrastructure.private_artifacts import (
    open_private_text, publish_private_json, read_json, require_outside_git, write_private_json,
)
from src.infrastructure.redact import redact_argument_list, redact_text


CODE = Path(__file__).resolve().parents[2]
READY_SCHEMA = 'semloom.imlane.duckdb.q3.model_ipc_ready.v1'
SHUTDOWN_SCHEMA = 'semloom.imlane.duckdb.q3.model_ipc_shutdown.v1'


def _error(error):
    return dict(type=type(error).__name__, error=redact_text(str(error)))


def _deadline(value):
    owned._seconds(value, 'absolute monotonic deadline')
    return value


def _configuration(config):
    required = {'model_python', 'model', 'served_model_name', 'dtype', 'engine_options'}
    if type(config) is not dict or set(config) != required:
        raise ValueError('model_service requires exactly: ' + ', '.join(sorted(required)))
    if config['dtype'] != 'bfloat16':
        raise ValueError('model service requires dtype bfloat16')
    if not isinstance(config['served_model_name'], str) or not config['served_model_name'].strip():
        raise ValueError('served_model_name must be nonempty text')
    python, model = Path(config['model_python']).absolute(), Path(config['model']).resolve()
    if not python.is_file() or not model.is_dir():
        raise ValueError('model_python and local model directory must already exist')
    options = config['engine_options']
    if type(options) is not dict or any(type(key) is not str or not key for key in options):
        raise ValueError('engine_options must be an explicit native keyword object')
    if options.keys() & {'model', 'dtype', 'generation_config', 'override_generation_config', 'served_model_name'}:
        raise ValueError('engine_options cannot override model, dtype, generation or served model identity')
    json.dumps(options, allow_nan=False)
    return dict(config, model_python=str(python), model=str(model), engine_options=deepcopy(options))


def _identity(info):
    return dict(device=info.st_dev, inode=info.st_ino)


class ModelService:
    def __init__(self, config, directory, *, environment, deadline, cleanup_deadline=None,
                 stop_budget=None, stop_grace_s=8):
        self.config = _configuration(config)
        self.directory = Path(directory).resolve()
        require_outside_git(self.directory)
        self.config_path = self.directory / 'model_worker.config.json'
        self.ready_path = self.directory / 'model_worker.ready.json'
        self.shutdown_path = self.directory / 'model_worker.shutdown.json'
        # AF_UNIX has a short path limit. Choosing this name allocates nothing.
        self.socket_directory = Path('/tmp') / ('semloom-imlane-' + uuid.uuid4().hex)
        self.socket_path = self.socket_directory / 'rpc.sock'
        self.socket_directory_identity = self.socket_identity = None
        self.command = [self.config['model_python'], '-m',
                        'integrations.imlane_duckdb.model_ipc_worker', '--config', str(self.config_path)]
        self.environment = dict(environment)
        self.environment['PYTHONPATH'] = str(CODE) + (
            os.pathsep + self.environment['PYTHONPATH'] if self.environment.get('PYTHONPATH') else '')
        self.deadline = _deadline(deadline)
        self.cleanup_deadline = cleanup_deadline
        owned._seconds(stop_grace_s, 'stop_grace_s')
        self.stop_grace_s = stop_grace_s
        self.stop_budget = stop_budget or StopBudget(threading.Event())
        self.served_model_name = config['served_model_name']
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
            transport='private Unix socket; author DuckDB/ArrowLane unchanged',
            socket_path=str(self.socket_path), engine_options_fields=sorted(self.config['engine_options']),
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

    def _ready(self):
        while True:
            self.check()
            if self.ready_path.exists():
                break
            time.sleep(min(.05, max(0.0, self.deadline - time.monotonic())))
        ready = read_json(self.ready_path)
        if (ready.get('schema') != READY_SCHEMA or type(ready.get('worker_pid')) is not int
                or ready['worker_pid'] != self.process.pid
                or psutil.Process(self.process.pid).create_time() != self.created
                or ready.get('served_model_name') != self.served_model_name
                or type(ready.get('engine_instances')) is not int or ready['engine_instances'] != 1):
            raise ValueError('IPC readiness differs from the owned model worker identity')
        if type(ready.get('max_model_len')) is not int or ready['max_model_len'] < 1:
            raise ValueError('IPC readiness needs the actual positive max_model_len')
        if type(ready.get('native_capacity')) is not dict:
            raise ValueError('IPC readiness needs a native_capacity object')
        directory = self.socket_directory.lstat()
        if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.getuid()
                or stat.S_IMODE(directory.st_mode) != 0o700
                or _identity(directory) != self.socket_directory_identity):
            raise ValueError('IPC readiness directory ownership changed')
        info = self.socket_path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError('IPC readiness needs an owned mode-0600 Unix socket')
        bound = ready.get('socket_identity')
        if (type(bound) is not dict or set(bound) != {'device', 'inode'}
                or any(type(value) is not int for value in bound.values()) or bound != _identity(info)):
            raise ValueError('IPC readiness differs from the bound socket identity')
        self.socket_identity = _identity(info)
        self.ready_info = {key: ready.get(key) for key in (
            'worker_pid', 'served_model_name', 'max_model_len', 'engine_instances', 'native_capacity', 'vllm_version')}
        self.report['socket_identity'] = self.socket_identity
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
            if any(path.exists() or path.is_symlink() for path in
                   (self.config_path, self.ready_path, self.shutdown_path)):
                raise FileExistsError('model worker evidence already exists')
            self.check()
            self.socket_directory.mkdir(mode=0o700)
            info = self.socket_directory.lstat()
            self.socket_directory_identity = _identity(info)
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
                raise ValueError('IPC socket directory must be owned and mode 0700')
            child = {key: self.config[key] for key in ('model', 'served_model_name', 'dtype', 'engine_options')}
            child.update(socket_path=str(self.socket_path), ready_path=str(self.ready_path),
                         shutdown_path=str(self.shutdown_path))
            write_private_json(self.config_path, child)
            log = self.directory / 'model_service.log'
            with open_private_text(log):
                pass
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

    def _cleanup_socket(self, cleanup):
        result = cleanup['socket_cleanup'] = dict(directory_removed=False, socket_removed_by_owner=False)
        if self.socket_directory_identity is None:
            result['directory_created'] = False
            return
        result['directory_created'] = True
        if cleanup['observed_owned_exit_confirmed'] is not True:
            raise RuntimeError('retain IPC socket directory until owned process exit is confirmed')
        directory = self.socket_directory.lstat()
        if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.getuid()
                or _identity(directory) != self.socket_directory_identity):
            raise RuntimeError('IPC socket directory ownership changed; retaining it')
        try:
            info = self.socket_path.lstat()
        except FileNotFoundError:
            result['socket_already_removed'] = True
        else:
            if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid()
                    or self.socket_identity is None or _identity(info) != self.socket_identity):
                raise RuntimeError('IPC socket ownership changed or is unconfirmed; retaining it')
            self.socket_path.unlink()
            result['socket_removed_by_owner'] = True
        # An unexpected member prevents rmdir; nothing else in /tmp is scanned.
        if _identity(self.socket_directory.lstat()) != self.socket_directory_identity:
            raise RuntimeError('IPC socket directory was replaced during cleanup')
        self.socket_directory.rmdir()
        result['directory_removed'] = True

    def close(self, *, deadline=None):
        if self.closed:
            return self.report
        errors = []
        try:
            until = self._close_deadline(deadline)
        except BaseException as error:
            errors.append(error)
            until = time.monotonic()
        cleanup = dict(deadline=until, native_shutdown_deadline=until - self.stop_grace_s,
                       fallback_term=False, fallback_term_identities=[], forced_kill=False,
                       signal_events=[], exit_events=[], cleanup_errors=[],
                       observed_owned_exit_confirmed=False)
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
                        shutdown = dict(schema=SHUTDOWN_SCHEMA,
                            native_shutdown_deadline=cleanup['native_shutdown_deadline'])
                        attempt(lambda: publish_private_json(self.shutdown_path, shutdown))
                        self.report['shutdown_request'] = shutdown
                        # Native shutdown owns the whole graceful window. The
                        # final stop_grace_s is reserved only for failed teardown.
                        attempt(lambda: send({(self.process.pid, self.created)}, signal.SIGTERM))
                        attempt(lambda: wait(cleanup['native_shutdown_deadline'], 'native_shutdown'))
                        remaining = attempt(lambda: owned._live(self.known))
                        if remaining:
                            cleanup['fallback_term'] = True
                            cleanup['fallback_term_identities'] = attempt(lambda:
                                [dict(pid=p.pid, create_time=p.create_time()) for p in remaining])
                            # Split only the remaining final reserve; never
                            # extend the caller's absolute cleanup deadline.
                            cleanup['fallback_term_deadline'] = min(
                                until, (time.monotonic() + until) / 2)
                            attempt(lambda: send(self.known, signal.SIGTERM))
                            attempt(lambda: wait(cleanup['fallback_term_deadline'], 'fallback_term'))
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
                if self.process is not None and self.process.returncode != 0:
                    errors.append(RuntimeError('vLLM service owner did not exit zero: '
                                               + str(self.process.returncode)))
                if cleanup['forced_kill']:
                    errors.append(RuntimeError('model service required forced SIGKILL'))
                if cleanup['fallback_term']:
                    errors.append(RuntimeError('model service required TERM fallback after native shutdown'))
                if cleanup.get('owned_survivors'):
                    errors.append(RuntimeError('model service owned processes remain alive'))
                attempt(lambda: self._cleanup_socket(cleanup))
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
