"""IPC owner contracts with process/socket-file doubles; no model or server starts."""

import builtins
import importlib
import os
from pathlib import Path
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import psutil

from integrations.imlane_duckdb import model_service as service_module
from src.infrastructure.private_artifacts import read_json, write_private_json


class Process:
    def __init__(self, pid, birth, *, exit_on_term=True):
        self.pid, self.birth = pid, birth
        self.returncode = None
        self.exit_on_term = exit_on_term
        self.signals, self.family = [], []
        self.on_signal = None

    @property
    def info(self):
        return dict(pid=self.pid, create_time=self.birth)

    def create_time(self):
        return self.birth

    def status(self):
        return psutil.STATUS_RUNNING if self.returncode is None else psutil.STATUS_ZOMBIE

    def children(self, recursive=True):
        return self.family

    def cmdline(self):
        return ['cpu-fixture']

    def name(self):
        return 'cpu-fixture'

    def send_signal(self, number):
        self.signals.append(number)
        if number == signal.SIGKILL or self.exit_on_term:
            self.returncode = -signal.SIGKILL if number == signal.SIGKILL else 0
        if self.on_signal is not None:
            self.on_signal(number)

    def poll(self):
        return self.returncode

    def wait(self, timeout):
        if self.returncode is None:
            raise subprocess.TimeoutExpired('cpu-fixture', timeout)
        return self.returncode


class ModelServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = dict(model_python=sys.executable, model=str(self.root),
            served_model_name='cpu-fixture', dtype='bfloat16', engine_options={})
        self.process = Process(31001, 10.0)
        self.writer = Mock(is_alive=Mock(return_value=False))
        self.services, self.socket_directories, self.fake_sockets = [], [], set()
        self.ready_transform = lambda ready: ready
        self.publish_ready = self.worker_unlinks = self.socket_file_is_socket = True
        original_import, original_lstat = builtins.__import__, Path.lstat

        def guarded_import(name, *args, **kwargs):
            if (name.split('.', 1)[0] in {'vllm', 'ray', 'torch', 'daft', 'lotus', 'transformers'}
                    or name.startswith(('src.scheduling', 'src.execution_provider', 'src.serving'))):
                raise AssertionError('unexpected model/executor import: ' + name)
            return original_import(name, *args, **kwargs)

        def process(pid):
            for candidate in [self.process, *self.process.family]:
                if candidate.pid == pid:
                    return candidate
            raise psutil.NoSuchProcess(pid)

        def socket_stat(path, *args, **kwargs):
            info = original_lstat(path, *args, **kwargs)
            if path in self.fake_sockets and self.socket_file_is_socket:
                fields = list(info)
                fields[stat.ST_MODE] = stat.S_IFSOCK | stat.S_IMODE(info.st_mode)
                return os.stat_result(fields)
            return info

        def spawn(command, environment, log):
            service = self.services[-1]
            self.fake_sockets.add(service.socket_path)
            service.socket_path.write_bytes(b'fixture socket inode')
            service.socket_path.chmod(0o600)
            if self.publish_ready:
                self.publish(service)

            def stop(number):
                if self.process.returncode is not None and self.worker_unlinks:
                    service.socket_path.unlink(missing_ok=True)
            self.process.on_signal = stop
            return self.process, self.writer

        self.spawn = Mock(side_effect=spawn)
        self.addCleanup(self.cleanup_directories)
        for guard in (
                patch.object(builtins, '__import__', side_effect=guarded_import),
                patch.object(subprocess, 'Popen', side_effect=AssertionError('real process start')),
                patch.object(socket.socket, 'connect', side_effect=AssertionError('socket connect')),
                patch.object(socket.socket, 'bind', side_effect=AssertionError('socket bind')),
                patch.object(service_module, 'spawn', self.spawn),
                patch.object(service_module.psutil, 'Process', side_effect=process),
                patch.object(service_module.psutil, 'process_iter', side_effect=lambda *a:
                             [self.process, *self.process.family]),
                patch.object(service_module.os, 'getpgid', return_value=self.process.pid),
                patch.object(Path, 'lstat', socket_stat)):
            guard.start()
            self.addCleanup(guard.stop)

    def cleanup_directories(self):
        # These exact paths were allocated by this test; never scan a /tmp prefix.
        for directory in self.socket_directories:
            if directory.is_dir():
                for child in directory.iterdir():
                    child.unlink()
                directory.rmdir()

    def publish(self, service):
        ready = dict(schema=service_module.READY_SCHEMA, worker_pid=self.process.pid,
            served_model_name='cpu-fixture', max_model_len=32768, engine_instances=1,
            native_capacity=dict(scheduler_config=dict(max_num_seqs=43)), vllm_version='CPU-fixture',
            socket_identity=service_module._identity(service.socket_path.lstat()))
        write_private_json(service.ready_path, self.ready_transform(ready))

    def service(self, **kwargs):
        values = dict(environment={'CUDA_VISIBLE_DEVICES': 'allocated-device'},
                      deadline=time.monotonic() + 5,
                      cleanup_deadline=lambda: time.monotonic() + .05, stop_grace_s=.01)
        values.update(kwargs)
        result = service_module.ModelService(self.config, self.root / ('service-' + str(len(self.services))), **values)
        self.services.append(result)
        self.socket_directories.append(result.socket_directory)
        return result

    def final(self, service):
        return read_json(service.directory / 'model_service.final.json')

    def test_import_and_constructor_allocate_no_process_directory_or_socket(self):
        importlib.reload(service_module)
        with patch.object(service_module, 'spawn', self.spawn):
            service = self.service()
        self.assertFalse(service.directory.exists())
        self.assertFalse(service.socket_directory.exists())
        self.assertLess(len(os.fsencode(service.socket_path)), 108)
        self.assertEqual(service.socket_directory.parent, Path('/tmp'))
        self.spawn.assert_not_called()
        self.assertFalse(hasattr(service, 'url'))
        self.assertEqual(service.report['status'], 'configured')

    def test_native_command_config_and_environment_preserve_explicit_options(self):
        self.config['engine_options'] = dict(max_num_seqs=43, enable_prefix_caching=True)
        service = self.service(environment={'CUDA_VISIBLE_DEVICES': 'allocated-device', 'PYTHONPATH': 'fixture-import'})
        with service:
            command, environment, log = self.spawn.call_args.args
            self.assertEqual(command, [str(Path(sys.executable).absolute()), '-m',
                'integrations.imlane_duckdb.model_ipc_worker', '--config', str(service.config_path)])
            child = read_json(service.config_path)
            self.assertEqual(set(child), {'model', 'served_model_name', 'dtype', 'engine_options',
                                         'socket_path', 'ready_path', 'shutdown_path'})
            self.assertEqual(child['engine_options'], self.config['engine_options'])
            self.assertNotIn('max_model_len', child['engine_options'])
            self.assertNotIn('max_num_batched_tokens', child['engine_options'])
            self.assertNotIn('tensor_parallel_size', child['engine_options'])
            self.assertEqual(environment['CUDA_VISIBLE_DEVICES'], 'allocated-device')
            self.assertEqual(environment['PYTHONPATH'], str(service_module.CODE) + os.pathsep + 'fixture-import')
            self.assertNotIn('OMP_NUM_THREADS', environment)
            self.assertEqual(log.name, 'model_service.log')
            self.assertEqual(stat.S_IMODE(service.socket_directory.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(service.config_path.stat().st_mode), 0o600)
        self.assertEqual(service.report['status'], 'closed')
        self.assertIsNone(service.report['physical_resources_confirmed'])

    def test_reserved_options_legacy_http_and_invalid_values_fail_before_start(self):
        changes = [dict(engine_options={key: 'override'}) for key in
                   ('model', 'dtype', 'generation_config', 'override_generation_config', 'served_model_name')]
        changes += [dict(dtype='float16'), dict(engine_options=[]), dict(engine_options={1: 'bad'}),
                    dict(engine_options=dict(gpu_memory_utilization=float('nan'))),
                    dict(model='not-a-local-model'), dict(host='127.0.0.1', port=18091, server_options=[])]
        for change in changes:
            with self.subTest(change=change), self.assertRaises((ValueError, TypeError)):
                service_module.ModelService(dict(self.config, **change), self.root / 'unused',
                    environment={}, deadline=time.monotonic() + 1)
        self.spawn.assert_not_called()
        self.assertFalse((self.root / 'unused').exists())

    def test_engine_options_are_snapshotted_at_the_configuration_boundary(self):
        self.config['engine_options'] = dict(compilation_config=dict(mode=0))
        service = self.service()
        self.config['engine_options']['compilation_config']['mode'] = 3
        with service:
            self.assertEqual(read_json(service.config_path)['engine_options']['compilation_config'], dict(mode=0))

    def test_file_ready_attests_owner_and_socket_without_any_model_probe(self):
        service = self.service()
        with service:
            self.assertEqual(service.ready_info['max_model_len'], 32768)
            self.assertEqual(service.ready_info['engine_instances'], 1)
            self.assertEqual(service.ready_info['native_capacity']['scheduler_config']['max_num_seqs'], 43)
            self.assertEqual(service.socket_identity, service_module._identity(service.socket_path.lstat()))
            self.assertEqual(service.ready_info['worker_pid'], self.process.pid)
            for phase in ('started', 'ready'):
                self.assertTrue((service.directory / ('model_service.' + phase + '.json')).is_file())
        cleanup = self.final(service)['cleanup']
        self.assertTrue(cleanup['observed_owned_exit_confirmed'])
        self.assertTrue(cleanup['socket_cleanup']['directory_removed'])
        self.assertFalse(service.socket_directory.exists())

    def test_query_owner_observation_checks_fresh_birth_and_bound_socket(self):
        service = self.service()
        with service:
            first = service.observe_owner()
            second = service.observe_owner()
            self.assertGreaterEqual(second['observed_monotonic_ns'], first['observed_monotonic_ns'])
            self.assertEqual(first['worker_pid'], self.process.pid)
            for field in ('worker_pid', 'worker_create_time', 'socket_identity', 'engine_instances'):
                self.assertEqual(first[field], second[field])
            self.process.birth += 1
            try:
                with self.assertRaisesRegex(RuntimeError, 'worker or its bound socket changed'):
                    service.observe_owner()
            finally:
                self.process.birth -= 1
            original = service.socket_identity
            service.socket_identity = dict(original, inode=original['inode']+1)
            try:
                with self.assertRaisesRegex(RuntimeError, 'worker or its bound socket changed'):
                    service.observe_owner()
            finally:
                service.socket_identity = original

    def test_delayed_ready_waits_only_within_the_work_deadline(self):
        self.publish_ready = False
        service = self.service()
        with patch.object(service_module.time, 'sleep', side_effect=lambda unused: self.publish(service)) as wait:
            service.__enter__()
        wait.assert_called_once()
        service.close()

    def test_wrong_ready_owner_model_context_and_socket_identity_are_rejected(self):
        for changed in (dict(worker_pid=31003), dict(served_model_name='wrong'), dict(engine_instances=2),
                        dict(max_model_len=None), dict(native_capacity=None),
                        dict(socket_identity=dict(device=1, inode=-1))):
            self.process.returncode = None
            self.ready_transform = lambda ready, changed=changed: dict(ready, **changed)
            service = self.service()
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                service.__enter__()
            self.assertEqual(self.final(service)['status'], 'failed')
            self.assertFalse(service.socket_directory.exists())

    def test_regular_file_cannot_attest_a_bound_unix_socket(self):
        self.socket_file_is_socket = False
        service = self.service()
        with self.assertRaisesRegex(ValueError, 'Unix socket'):
            service.__enter__()
        self.assertEqual(self.final(service)['status'], 'failed')

    def test_socket_with_group_or_world_access_cannot_become_ready(self):
        service = self.service()

        def expose(ready):
            service.socket_path.chmod(0o666)
            return ready

        self.ready_transform = expose
        with self.assertRaisesRegex(ValueError, 'mode-0600'):
            service.__enter__()
        self.assertEqual(self.final(service)['status'], 'failed')

    def test_preexisting_private_directory_is_never_removed_on_startup_failure(self):
        service = self.service()
        service.socket_directory.mkdir(mode=0o700)
        marker = service.socket_directory / 'previous-owner'
        marker.write_bytes(b'keep unrelated previous object')
        with self.assertRaises(FileExistsError):
            service.__enter__()
        self.spawn.assert_not_called()
        self.assertEqual(marker.read_bytes(), b'keep unrelated previous object')
        self.assertFalse(self.final(service)['cleanup']['socket_cleanup']['directory_created'])

    def test_expired_startup_writes_final_without_allocating_socket_or_process(self):
        service = self.service(deadline=time.monotonic() - 1)
        with self.assertRaises(TimeoutError):
            service.__enter__()
        self.spawn.assert_not_called()
        self.assertFalse(service.socket_directory.exists())
        self.assertEqual(self.final(service)['status'], 'failed')

    def test_shutdown_is_published_once_before_owner_term_with_the_absolute_deadline(self):
        service = self.service()
        service.__enter__()
        old_stop = self.process.on_signal
        observed = []

        def stop(number):
            observed.append(read_json(service.shutdown_path))
            old_stop(number)

        self.process.on_signal = stop
        deadline = time.monotonic() + .03
        service.close(deadline=deadline)
        before = service.shutdown_path.read_bytes()
        service.close(deadline=deadline + 100)
        self.assertEqual(observed, [dict(schema=service_module.SHUTDOWN_SCHEMA,
                                       native_shutdown_deadline=deadline - service.stop_grace_s)])
        self.assertEqual(service.shutdown_path.read_bytes(), before)
        self.assertEqual(self.final(service)['cleanup']['deadline'], deadline)

    def test_repeated_ready_handoff_preserves_the_initial_cleanup_start(self):
        service = self.service()
        initial = service.deadline
        with service:
            deadline = time.monotonic() + 20
            with patch.object(service_module.time, 'monotonic', return_value=123.0):
                service.begin_cleanup(deadline)
            with patch.object(service_module.time, 'monotonic', return_value=initial + 1):
                service.begin_cleanup(deadline)
                service.check()
            self.assertEqual(service.report['cleanup_began_monotonic'], 123.0)
            self.assertEqual(service.report['query_deadline_initial'], initial)
            self.assertEqual(service.deadline, deadline)

    def test_observer_does_not_permanently_timeout_before_native_ready_is_read(self):
        service = self.service()
        with service:
            service.stop_observer.set()
            service.observer.join()
            service.deadline = time.monotonic() - .001
            service.stop_observer = Mock(wait=Mock(side_effect=[False, True]), is_set=Mock(return_value=False))
            service._sample()
            self.assertIsNone(service.observation_error)
            with self.assertRaises(TimeoutError):
                service.check()
            service.begin_cleanup(time.monotonic() + 20)
            service.check()

    def test_native_shutdown_longer_than_stop_reserve_does_not_term_children_early(self):
        child = Process(31002, 11.0)
        self.process.family = [child]
        self.process.exit_on_term = False
        service = self.service(stop_grace_s=8)
        service.__enter__()
        service.stop_observer.set()
        service.observer.join()
        now = [100.0]

        def native_shutdown(identities, pgid, created, deadline, **unused):
            self.assertEqual(deadline, 212.0)
            self.assertLessEqual(120.0, deadline)
            self.assertEqual(child.signals, [])
            now[0] = 120.0  # Native close takes20s, exceeding the8s final reserve.
            self.process.returncode = child.returncode = 0
            service.socket_path.unlink()

        with patch.object(service_module.time, 'monotonic', side_effect=lambda: now[0]), \
             patch.object(service_module.owned, '_wait', side_effect=native_shutdown) as wait:
            service.close(deadline=220.0)
        wait.assert_called_once()
        self.assertEqual(self.process.signals, [signal.SIGTERM])
        self.assertEqual(child.signals, [])
        self.assertEqual(self.final(service)['status'], 'closed')

    def test_leftover_child_term_recovers_resources_but_remains_failed(self):
        child = Process(31002, 11.0)
        self.process.family = [child]
        service = self.service()
        with self.assertRaisesRegex(RuntimeError, 'TERM fallback'):
            with service:
                pass
        cleanup = self.final(service)['cleanup']
        self.assertEqual(child.signals, [signal.SIGTERM])
        self.assertTrue(cleanup['fallback_term'])
        self.assertEqual(cleanup['owned_survivors'], [])
        self.assertTrue(cleanup['socket_cleanup']['directory_removed'])
        event, = [event for event in cleanup['signal_events'] if event['pid'] == child.pid]
        self.assertGreaterEqual(event['requested_monotonic'], cleanup['native_shutdown_deadline'])

    def test_forced_kill_is_failed_even_when_all_observed_processes_exit(self):
        self.process.family = [Process(31002, 11.0, exit_on_term=False)]
        service = self.service()
        with self.assertRaisesRegex(RuntimeError, 'forced SIGKILL'):
            with service:
                pass
        cleanup = self.final(service)['cleanup']
        self.assertTrue(cleanup['forced_kill'])
        self.assertEqual(cleanup['owned_survivors'], [])
        self.assertEqual(self.process.family[0].signals, [signal.SIGTERM, signal.SIGKILL])

    def test_nonzero_owner_exit_is_failure_even_when_all_resources_exit(self):
        service = self.service()
        service.__enter__()
        stop = self.process.on_signal

        def fail_close(number):
            stop(number)
            self.process.returncode = 3

        self.process.on_signal = fail_close
        with self.assertRaisesRegex(RuntimeError, 'did not exit zero: 3'):
            service.close()
        cleanup = self.final(service)['cleanup']
        self.assertEqual(cleanup['owner_returncode'], 3)
        self.assertEqual(cleanup['owned_survivors'], [])
        self.assertTrue(cleanup['socket_cleanup']['directory_removed'])

    def test_matching_leftover_socket_is_removed_only_after_confirmed_exit(self):
        self.worker_unlinks = False
        service = self.service()
        with service:
            pass
        self.assertTrue(self.final(service)['cleanup']['socket_cleanup']['socket_removed_by_owner'])
        self.assertFalse(service.socket_directory.exists())

    def test_replaced_socket_is_retained_with_failure(self):
        self.worker_unlinks = False
        service = self.service()
        service.__enter__()
        old = service.socket_directory / 'old-socket'
        service.socket_path.rename(old)
        service.socket_path.write_bytes(b'replacement must not be removed')
        service.socket_path.chmod(0o600)
        with self.assertRaisesRegex(RuntimeError, 'ownership changed'):
            service.close()
        self.assertTrue(service.socket_path.exists())
        self.assertTrue(old.exists())
        self.assertEqual(self.final(service)['status'], 'failed')

    def test_replaced_socket_directory_is_retained_even_when_empty(self):
        self.worker_unlinks = False
        service = self.service()
        service.__enter__()
        moved = service.socket_directory.with_name(service.socket_directory.name + '-moved')
        service.socket_directory.rename(moved)
        self.socket_directories.append(moved)
        service.socket_directory.mkdir(mode=0o700)
        with self.assertRaisesRegex(RuntimeError, 'directory ownership changed'):
            service.close()
        self.assertTrue(service.socket_directory.is_dir())
        self.assertTrue(moved.is_dir())

    def test_unknown_initial_birth_never_signals_or_removes_a_live_socket(self):
        service = self.service()
        original = psutil.AccessDenied(self.process.pid)
        with patch.object(service_module.psutil, 'Process', side_effect=original):
            with self.assertRaises(psutil.AccessDenied) as caught:
                service.__enter__()
        self.assertIs(caught.exception, original)
        self.assertEqual(self.process.signals, [])
        self.assertTrue(service.socket_path.exists())
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])

    def test_reused_owner_birth_is_never_signalled_or_reported_clean(self):
        service = self.service()
        service.__enter__()
        self.process.birth = 20.0
        with self.assertRaisesRegex(RuntimeError, 'reused'):
            service.close()
        self.assertEqual(self.process.signals, [])
        self.assertTrue(service.socket_directory.exists())
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])

    def test_spawn_failure_keeps_original_error_and_unknown_ownership(self):
        original = OSError('fixture process factory failed')
        self.spawn.side_effect = original
        service = self.service()
        with self.assertRaises(OSError) as caught:
            service.__enter__()
        self.assertIs(caught.exception, original)
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])

    def test_term_wait_failures_write_final_and_preserve_query_exception(self):
        service = self.service()
        service.__enter__()
        original = LookupError('original native query failed')
        with patch.object(service_module.owned, '_signal', side_effect=PermissionError('fixture TERM denied')), \
             patch.object(self.process, 'wait', side_effect=RuntimeError('fixture wait failed')):
            self.assertFalse(service.__exit__(LookupError, original, None))
        final = self.final(service)
        self.assertEqual(final['error']['type'], 'LookupError')
        self.assertEqual(final['status'], 'failed')
        self.assertTrue(any('TERM denied' in note for note in original.__notes__))
        self.assertTrue(any('wait failed' in item['error'] for item in final['cleanup']['cleanup_errors']))
        self.assertTrue(service.socket_path.exists())

    def test_log_writer_not_stopped_is_not_resource_success(self):
        self.writer.is_alive.return_value = True
        service = self.service()
        with self.assertRaisesRegex(RuntimeError, 'thread did not stop'):
            with service:
                pass
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])


if __name__ == '__main__':
    unittest.main()
