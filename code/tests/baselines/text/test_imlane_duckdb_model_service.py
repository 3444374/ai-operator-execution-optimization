"""Owned-service contracts with process/HTTP doubles; no SDK or model starts."""

import builtins
from copy import deepcopy
import importlib
import io
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

import psutil

from integrations.imlane_duckdb import model_service as service_module


class Process:
    def __init__(self, pid, birth, *, exit_on_term=True):
        self.pid, self.birth = pid, birth
        self.returncode = None
        self.exit_on_term = exit_on_term
        self.signals = []
        self.family = []

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
            served_model_name='cpu-fixture', host='127.0.0.1', port=18091,
            dtype='bfloat16', server_options=[])
        self.process = Process(31001, 10.0)
        self.writer = Mock(is_alive=Mock(return_value=False))
        self.spawn = Mock(return_value=(self.process, self.writer))
        original_import = builtins.__import__

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

        for guard in (
                patch.object(builtins, '__import__', side_effect=guarded_import),
                patch.object(subprocess, 'Popen', side_effect=AssertionError('real process start')),
                patch.object(socket.socket, 'connect', side_effect=AssertionError('network connect')),
                patch.object(socket.socket, 'bind', side_effect=AssertionError('network bind')),
                patch.object(service_module, 'spawn', self.spawn),
                patch.object(service_module, '_check_port'),
                patch.object(service_module.psutil, 'Process', side_effect=process),
                patch.object(service_module.psutil, 'process_iter', side_effect=lambda *a:
                             [self.process, *self.process.family]),
                patch.object(service_module.os, 'getpgid', return_value=self.process.pid)):
            guard.start()
            self.addCleanup(guard.stop)

    def service(self, **kwargs):
        values = dict(environment={'CUDA_VISIBLE_DEVICES': 'allocated-device'},
                      deadline=time.monotonic() + 5,
                      cleanup_deadline=lambda: time.monotonic() + .05, stop_grace_s=.01)
        values.update(kwargs)
        result = service_module.ModelService(self.config, self.root / 'service', **values)

        def get(path, *, json_body=True):
            if path == '/health':
                return 200
            if path == '/v1/models':
                return dict(data=[dict(id='cpu-fixture', max_model_len=32768)])
            raise HTTPError(result.base_url + path, 404, 'not enabled', {}, None)

        result._get = Mock(side_effect=get)
        return result

    def final(self, service):
        return json.loads((service.directory / 'model_service.final.json').read_text())

    def test_import_and_constructor_allocate_no_process_directory_or_network(self):
        importlib.reload(service_module)
        # Reload imports the same pure spawn helper; restore this test's seam.
        with patch.object(service_module, 'spawn', self.spawn):
            service = self.service()
        self.assertFalse(service.directory.exists())
        self.spawn.assert_not_called()
        self.assertIsNone(service.process)
        self.assertEqual(service.report['status'], 'configured')

    def test_native_command_omits_capacity_and_thread_defaults_and_keeps_environment(self):
        self.config['server_options'] = ['--max-num-seqs', '43', '--enable-prefix-caching']
        service = self.service()
        with service:
            command, environment, log = self.spawn.call_args.args
            self.assertEqual(command[:3], [str(Path(sys.executable).absolute()), '-m',
                                          'vllm.entrypoints.openai.api_server'])
            self.assertEqual(command[command.index('--generation-config') + 1], 'vllm')
            self.assertEqual(command[-3:], self.config['server_options'])
            self.assertNotIn('--max-model-len', command)
            self.assertNotIn('--max-num-batched-tokens', command)
            self.assertNotIn('--tensor-parallel-size', command)
            self.assertEqual(environment, {'CUDA_VISIBLE_DEVICES': 'allocated-device'})
            self.assertEqual(log.name, 'model_service.log')
        self.assertIsNone(service.report['physical_resources_confirmed'])
        self.assertEqual(service.report['status'], 'closed')

    def test_identity_topology_and_config_overrides_fail_before_start(self):
        for option in ('--port=18092', '--model', '--generation_config', '--generation-conf',
                       '--config', '-c', '--api-key', '--api-server-count', '-tp',
                       '--tensor-parallel-size=2', '--data-parallel-size-local', '--uds', '--'):
            with self.subTest(option=option), self.assertRaisesRegex(ValueError, 'override'):
                self.config['server_options'] = [option]
                self.service()
        self.spawn.assert_not_called()

    def test_invalid_boundary_values_and_remote_model_name_are_rejected(self):
        for key, value in (('host', '0.0.0.0'), ('port', 0), ('port', True),
                           ('dtype', 'float16'), ('server_options', None),
                           ('model', 'not-a-local-model-directory')):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                configured = deepcopy(self.config)
                configured[key] = value
                service_module.ModelService(configured, self.root / 'unused',
                    environment={}, deadline=time.monotonic() + 1)
        self.assertFalse((self.root / 'unused').exists())

    def test_readiness_is_get_only_and_keeps_unavailable_server_info_unknown(self):
        service = self.service()
        with service:
            self.assertEqual([call.args[0] for call in service._get.call_args_list],
                             ['/health', '/v1/models', '/server_info?config_format=json'])
            self.assertEqual(service.ready_info['max_model_len'], 32768)
            self.assertEqual(service.ready_info['server_info']['status'], 'unknown')
            self.assertIsNone(service.ready_info['server_info']['resolved'])
            for phase in ('started', 'ready'):
                self.assertTrue((service.directory / ('model_service.' + phase + '.json')).is_file())
        saved = self.final(service)
        self.assertTrue(saved['cleanup']['observed_owned_exit_confirmed'])
        self.assertFalse(saved['cleanup']['forced_kill'])
        self.assertEqual(self.process.signals, [signal.SIGTERM])

    def test_server_info_saves_only_resolved_fields_and_never_environment(self):
        service = self.service()
        service._get.side_effect = [200, dict(data=[dict(id='cpu-fixture')]),
            dict(vllm_config=dict(model_config=dict(dtype='bfloat16', max_model_len=50000,
                generation_config='vllm', hf_token='fixture-private-value'),
                scheduler_config=dict(max_num_seqs=77, max_num_batched_tokens=12345),
                unrelated='private'), vllm_env=dict(private='fixture-private-value'),
                system_env=dict(private='fixture-private-value'))]
        with service:
            resolved = service.ready_info['server_info']['resolved']
            self.assertEqual(resolved['scheduler_config']['max_num_seqs'], 77)
            self.assertIsNone(service.ready_info['max_model_len'])
        self.assertNotIn('fixture-private-value', json.dumps(self.final(service)))
        self.assertNotIn('vllm_env', json.dumps(service.report))
        self.assertNotIn('hf_token', json.dumps(service.report))

    def test_http_get_disables_proxies_and_uses_only_the_declared_service_key(self):
        service = self.service(environment={'VLLM_API_KEY': 'fixture-private-value'})
        opener = Mock()
        response = io.BytesIO(b'{"data": []}')
        opener.open.return_value = response
        with patch.object(service_module, 'build_opener', return_value=opener) as factory:
            result = service_module.ModelService._get(service, '/v1/models')
        request = opener.open.call_args.args[0]
        self.assertEqual(request.get_method(), 'GET')
        self.assertEqual(request.get_header('Authorization'), 'Bearer fixture-private-value')
        self.assertEqual(factory.call_args.args[0].proxies, {})
        self.assertEqual(result, dict(data=[]))
        self.assertNotIn('fixture-private-value', json.dumps(service.report))

    def test_transient_health_failure_retries_but_model_identity_failure_is_preserved(self):
        service = self.service()
        original = ValueError('ready service does not expose the requested served_model_name')
        service._get.side_effect = [URLError('not listening'), 200, dict(data=[])]
        with self.assertRaisesRegex(ValueError, str(original)):
            service.__enter__()
        self.assertEqual(service._get.call_count, 3)
        self.assertEqual(self.final(service)['status'], 'failed')
        self.assertEqual(self.process.signals, [signal.SIGTERM])

    def test_auth_error_is_not_retried_until_the_work_deadline(self):
        service = self.service()
        error = HTTPError(service.base_url, 401, 'fixture unauthorized', {}, None)
        service._get.side_effect = error
        with self.assertRaises(HTTPError) as caught:
            service.__enter__()
        self.assertIs(caught.exception, error)
        self.assertEqual(service._get.call_count, 1)
        self.assertEqual(self.final(service)['status'], 'failed')

    def test_expired_startup_preserves_a_final_record_without_starting(self):
        service = self.service(deadline=time.monotonic() - 1)
        with self.assertRaises(TimeoutError):
            service.__enter__()
        self.spawn.assert_not_called()
        self.assertEqual(self.final(service)['status'], 'failed')
        self.assertTrue(self.final(service)['cleanup']['observed_owned_exit_confirmed'])

    def test_explicit_close_rebinds_the_total_deadline_without_a_new_stage_budget(self):
        service = self.service(cleanup_deadline=lambda: (_ for _ in ()).throw(AssertionError('wrong deadline')))
        service.__enter__()
        deadline = time.monotonic() + .03
        service.close(deadline=deadline)
        self.assertEqual(self.final(service)['cleanup']['deadline'], deadline)
        self.assertEqual(service.close(), service.report)
        self.assertEqual(self.process.signals, [signal.SIGTERM])

    def test_native_ready_rebinds_monitor_to_the_shared_cleanup_window(self):
        service = self.service()
        initial = service.deadline
        with service:
            deadline = time.monotonic() + 20
            service.begin_cleanup(deadline)
            with patch.object(service_module.time, 'monotonic', return_value=initial + 1):
                service.check()
            self.assertEqual(service.report['query_deadline_initial'], initial)
            self.assertEqual(service.report['cleanup_window_deadline'], deadline)
            failure = TimeoutError('original query deadline already failed')
            service.observation_error = failure
            service.begin_cleanup(deadline + 1)
            with self.assertRaises(TimeoutError) as caught:
                service.check()
            self.assertIs(caught.exception, failure)
            # The saved failure must also make close fail; preserve it explicitly.
            service.observation_error = None

    def test_repeated_ready_observation_keeps_the_first_cleanup_start(self):
        service = self.service()
        with service:
            deadline = time.monotonic() + 20
            with patch.object(service_module.time, 'monotonic', return_value=123.0):
                service.begin_cleanup(deadline)
            with patch.object(service_module.time, 'monotonic', return_value=124.0):
                service.begin_cleanup(deadline)
            self.assertEqual(service.report['cleanup_began_monotonic'], 123.0)
            self.assertEqual(service.deadline, deadline)
            self.assertEqual(service.report['active_deadline'], deadline)

    def test_observer_does_not_turn_delayed_native_ready_observation_into_query_timeout(self):
        service = self.service()
        with service:
            service.stop_observer.set()
            service.observer.join()
            service.deadline = time.monotonic() - .001
            service.stop_observer = Mock(wait=Mock(side_effect=[False, True]),
                                         is_set=Mock(return_value=False))
            service._sample()
            self.assertIsNone(service.observation_error)
            with self.assertRaisesRegex(TimeoutError, 'work deadline'):
                service.check()
            service.begin_cleanup(time.monotonic() + 20)
            service.check()
        self.assertEqual(self.final(service)['status'], 'closed')

    def test_descendant_term_follows_owner_grace_and_is_birth_matched(self):
        child = Process(31002, 11.0)
        self.process.family = [child]
        service = self.service()
        with service:
            pass
        self.assertEqual(self.process.signals, [signal.SIGTERM])
        self.assertEqual(child.signals, [signal.SIGTERM])
        self.assertFalse(self.final(service)['cleanup']['forced_kill'])

    def test_forced_kill_marks_failure_even_when_all_observed_processes_exit(self):
        self.process.family = [Process(31002, 11.0, exit_on_term=False)]
        service = self.service()
        with self.assertRaisesRegex(RuntimeError, 'forced SIGKILL'):
            with service:
                pass
        final = self.final(service)
        self.assertEqual(final['status'], 'failed')
        self.assertTrue(final['cleanup']['forced_kill'])
        self.assertEqual(final['cleanup']['owned_survivors'], [])
        self.assertEqual(self.process.family[0].signals, [signal.SIGTERM, signal.SIGKILL])

    def test_nonzero_owner_exit_fails_even_when_all_processes_are_gone(self):
        original_signal = self.process.send_signal

        def exit_nonzero(number):
            original_signal(number)
            self.process.returncode = 3

        service = self.service()
        with patch.object(self.process, 'send_signal', side_effect=exit_nonzero):
            with self.assertRaisesRegex(RuntimeError, 'did not exit zero: 3'):
                with service:
                    pass
        final = self.final(service)
        self.assertEqual(final['status'], 'failed')
        self.assertEqual(final['cleanup']['owner_returncode'], 3)
        self.assertEqual(final['cleanup']['owned_survivors'], [])
        self.assertFalse(final['cleanup']['forced_kill'])

    def test_term_wait_failures_still_write_final_and_keep_the_primary_exception(self):
        service = self.service()
        original = LookupError('original query failed')
        failure = PermissionError('fixture TERM denied')
        with self.assertRaises(LookupError) as caught:
            with service:
                with patch.object(service_module.owned, '_signal', side_effect=failure), \
                     patch.object(self.process, 'wait', side_effect=RuntimeError('fixture wait failed')):
                    # Call the real context exit while the cleanup faults remain installed.
                    service.__exit__(LookupError, original, None)
                    raise original
        self.assertIs(caught.exception, original)
        final = self.final(service)
        self.assertEqual(final['status'], 'failed')
        self.assertEqual(final['error']['type'], 'LookupError')
        self.assertTrue(any(item['type'] == 'PermissionError' for item in final['cleanup']['cleanup_errors']))
        self.assertTrue(any('wait failed' in item['error'] for item in final['cleanup']['cleanup_errors']))
        self.assertTrue(any('TERM denied' in note for note in original.__notes__))

    def test_reused_owner_identity_is_never_signalled_or_reported_clean(self):
        service = self.service()
        service.__enter__()
        self.process.birth = 20.0
        with self.assertRaisesRegex(RuntimeError, 'reused'):
            service.close()
        self.assertEqual(self.process.signals, [])
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])

    def test_unknown_initial_birth_identity_does_not_claim_clean_or_blindly_signal(self):
        service = self.service()
        original = psutil.AccessDenied(self.process.pid)
        with patch.object(service_module.psutil, 'Process', side_effect=original):
            with self.assertRaises(psutil.AccessDenied) as caught:
                service.__enter__()
        self.assertIs(caught.exception, original)
        self.assertEqual(self.process.signals, [])
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])
        self.assertIn('identity is unconfirmed', self.final(service)['cleanup']['cleanup_errors'][0]['error'])

    def test_spawn_failure_preserves_the_original_error_without_claiming_an_empty_owner(self):
        original = OSError('fixture process factory failed')
        self.spawn.side_effect = original
        service = self.service()
        with self.assertRaises(OSError) as caught:
            service.__enter__()
        self.assertIs(caught.exception, original)
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])
        self.assertEqual(self.final(service)['status'], 'failed')

    def test_observer_and_log_writer_failure_is_not_resource_success(self):
        self.writer.is_alive.return_value = True
        service = self.service()
        with self.assertRaisesRegex(RuntimeError, 'thread did not stop'):
            with service:
                pass
        self.assertFalse(self.final(service)['cleanup']['observed_owned_exit_confirmed'])


if __name__ == '__main__':
    unittest.main()
