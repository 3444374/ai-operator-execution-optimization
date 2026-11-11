"""Native batch IPC client boundaries without a model or process launch."""

import builtins
from copy import deepcopy
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

from integrations.imlane_duckdb import model_provider as provider
from integrations.imlane_duckdb.model_wire import encode_frame
from src.baselines.text.sembench_movie_native import q3_decision, q3_messages
from src.infrastructure.private_artifacts import content_digest


class SoftwareSocket:
    def __init__(self, reply):
        self.reply = reply
        self.timeouts, self.connections, self.requests = [], [], []
        self.response = b''
        self.closed = False
        self.close_error = self.read_error = None

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def connect(self, path):
        self.connections.append(path)

    def sendall(self, data):
        size, = struct.unpack('!Q', data[:8])
        assert size == len(data) - 8
        request = json.loads(data[8:])
        self.requests.append(request)
        self.response = encode_frame(self.reply(request))

    def recv(self, count):
        if self.read_error is not None:
            raise self.read_error
        size = min(7, count)
        body, self.response = self.response[:size], self.response[size:]
        return body

    def close(self):
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class IMLaneModelProviderTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='imlane-provider-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.receipts = self.root / 'receipts'
        self.receipts.mkdir(mode=0o700)
        self.path = self.root / 'provider.json'
        self.config = dict(schema=provider.CONFIG_SCHEMA, socket_path='/tmp/fixture-model/rpc.sock',
            model='fixture-model', sampling_params=dict(max_tokens=8192, temperature=0, top_p=1,
                n=1, stop=None, presence_penalty=0, frequency_penalty=0, repetition_penalty=1),
            request_timeout_s=10, receipt_dir=str(self.receipts), provider_mode='software_ipc')
        self.configure()
        self.messages = [q3_messages('雪 «review»\nfirst'), q3_messages('second')]
        self.texts = ['Answer: True', 'Answer: False']
        self.socket = SoftwareSocket(self.reply)
        self.factory = patch.object(provider.socket, 'socket', return_value=self.socket).start()
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {provider.CONFIG_ENV: str(self.path)}).start()
        patch.object(subprocess, 'Popen', side_effect=AssertionError('process launch forbidden')).start()
        original_import = builtins.__import__

        def cpu_import(name, *args, **kwargs):
            if name.split('.', 1)[0] in {'vllm', 'torch', 'transformers', 'ray', 'daft', 'lotus'}:
                raise AssertionError('unexpected model/framework import: ' + name)
            return original_import(name, *args, **kwargs)

        patch.object(builtins, '__import__', side_effect=cpu_import).start()
        provider._configuration.cache_clear()
        self.addCleanup(provider._configuration.cache_clear)

    def configure(self, **changes):
        self.config.update(changes)
        self.path.write_text(json.dumps(self.config), encoding='utf-8')

    def reply(self, request):
        return dict(schema=request['schema'], batch_id=request['batch_id'], model=request['model'],
            status='succeeded', calls=[dict(position=i, request_id=request['batch_id'] + '-' + str(i),
                attempted=True, status='succeeded', generated_text=self.texts[i],
                requested_max_tokens=request['sampling_params']['max_tokens'],
                resolved_max_tokens=request['sampling_params']['max_tokens'],
                finish_reason='stop', usage=dict(prompt_tokens=20, completion_tokens=2))
                for i in range(len(request['messages_batch']))])

    def saved(self, phase):
        return [json.loads(path.read_text()) for path in self.receipts.glob('*.' + phase + '.json')]

    def test_one_ipc_for_entire_author_batch_preserves_messages_sampling_and_position(self):
        original = deepcopy(self.messages)
        self.assertEqual(provider.infer_batch(self.messages), self.texts)
        self.assertEqual(self.messages, original)
        self.factory.assert_called_once_with(socket.AF_UNIX, socket.SOCK_STREAM)
        self.assertEqual(self.socket.connections, [self.config['socket_path']])
        request, = self.socket.requests
        self.assertEqual(request['messages_batch'], original)
        self.assertEqual(request['sampling_params'], self.config['sampling_params'])
        self.assertNotIn('stream', request['sampling_params'])
        started, = self.saved('started')
        final, = self.saved('terminal')
        self.assertEqual(started['status'], 'unknown')
        self.assertIsNone(started['engine_generate_invocations'])
        self.assertEqual((final['status'], final['engine_generate_invocations']), ('succeeded', 2))
        self.assertTrue(final['ipc_attempted'])
        self.assertTrue(final['client_closed'])
        self.assertTrue(self.socket.closed)
        self.assertIsNone(final['physical_model_attempts'])
        self.assertEqual(final['request_sha256'], content_digest(request))
        self.assertEqual(final['response_sha256'], content_digest(final['response']))
        for i, call in enumerate(final['calls']):
            self.assertNotIn('occurrence_id', call)
            self.assertEqual(call['requested_payload'], dict(self.config['sampling_params'],
                model=self.config['model'], messages=original[i]))
            self.assertEqual(call['messages_sha256'], content_digest(original[i]))
            self.assertEqual(call['response'], final['response']['calls'][i])

    def test_optional_occurrences_stay_in_receipts_not_original_model_request(self):
        messages = [self.messages[0], self.messages[0]]
        self.assertEqual(provider.infer_batch(messages, occurrence_ids=['later', 'first']), self.texts)
        request, = self.socket.requests
        self.assertEqual(request['messages_batch'], messages)
        self.assertEqual(set(request), {'schema', 'batch_id', 'model', 'sampling_params',
                                       'messages_batch', 'deadline_monotonic'})
        for phase in ('started', 'terminal'):
            receipt, = self.saved(phase)
            self.assertEqual([(row['position'], row['occurrence_id']) for row in receipt['calls']],
                             [(0, 'later'), (1, 'first')])

    def test_invalid_occurrence_bindings_fail_before_ipc(self):
        for ids in ([], ['only'], ['same', 'same'], [None, 'valid'], ['valid', ''], ('a', 'b')):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                provider.infer_batch(self.messages, occurrence_ids=ids)
        self.factory.assert_not_called()
        self.assertEqual(self.saved('started'), [])

    def test_explicit_sampling_options_are_forwarded_without_short_output_defaults(self):
        self.config['sampling_params'].update(seed=42, min_tokens=0, top_k=-1)
        self.configure()
        provider.infer_batch(self.messages)
        sampling = self.socket.requests[0]['sampling_params']
        self.assertEqual(sampling['max_tokens'], 8192)
        self.assertEqual(sampling['stop'], None)
        self.assertEqual(sampling['seed'], 42)
        self.assertEqual(sampling['top_k'], -1)

    def test_129_rows_remain_one_rpc_without_client_row_limit(self):
        self.texts = ['Answer: True'] * 129
        rows = [q3_messages(str(i)) for i in range(129)]
        self.assertEqual(provider.infer_batch(rows), self.texts)
        self.assertEqual(len(self.socket.requests), 1)
        self.assertEqual(len(self.saved('terminal')[0]['calls']), 129)

    def test_empty_generation_is_preserved_for_original_default_true_parser(self):
        self.texts[0] = ''
        texts = provider.infer_batch(self.messages)
        self.assertEqual(texts[0], '')
        self.assertTrue(q3_decision(texts[0]))

    def test_length_is_recorded_without_rewriting_model_text(self):
        def reply(request):
            result = self.reply(request)
            result['calls'][0]['finish_reason'] = 'length'
            return result

        self.socket.reply = reply
        self.assertEqual(provider.infer_batch(self.messages), self.texts)
        self.assertTrue(self.saved('terminal')[0]['calls'][0]['truncated'])

    def test_completed_failed_row_preserves_all_other_results_and_does_not_retry(self):
        def reply(request):
            value = self.reply(request)
            value['status'] = 'failed'
            value['calls'][0].update(status='failed', error_type='ValueError', error='planned failure')
            return value

        self.socket.reply = reply
        with self.assertRaisesRegex(provider.ModelBatchError, 'planned failure'):
            provider.infer_batch(self.messages)
        final, = self.saved('terminal')
        self.assertEqual(final['status'], 'failed')
        self.assertEqual(final['calls'][1]['response']['generated_text'], self.texts[1])
        self.assertEqual(final['engine_generate_invocations'], 2)
        self.assertTrue(final['client_closed'])
        self.assertEqual(len(self.socket.requests), 1)

    def test_response_identity_or_position_mismatch_fails_without_reordering(self):
        for name in ('model', 'batch_id', 'schema', 'position', 'finish_reason'):
            def reply(request, field=name):
                result = self.reply(request)
                if field in ('position', 'finish_reason'):
                    result['calls'][0][field] = -1 if field == 'position' else 'abort'
                else:
                    result[field] = 'different'
                return result

            self.socket.reply = reply
            with self.subTest(name=name), self.assertRaises(ValueError):
                provider.infer_batch(self.messages)
        self.assertTrue(self.socket.closed)

    def test_transport_failure_keeps_model_attempts_unknown(self):
        self.socket.read_error = ConnectionResetError('software disconnect')
        with self.assertRaises(ConnectionResetError):
            provider.infer_batch(self.messages)
        final, = self.saved('terminal')
        self.assertEqual(final['status'], 'unknown')
        self.assertTrue(final['ipc_attempted'])
        self.assertIsNone(final['engine_generate_invocations'])
        self.assertIsNone(final['calls'][0]['attempted'])
        self.assertEqual(len(self.socket.requests), 1)
        self.assertTrue(final['client_closed'])

    def test_unknown_native_abort_keeps_completed_batch_unknown(self):
        def reply(request):
            value = self.reply(request)
            value['status'] = 'unknown'
            value['calls'][0].update(status='unknown', error_type='TimeoutError')
            return value

        self.socket.reply = reply
        with self.assertRaises(provider.ModelBatchError):
            provider.infer_batch(self.messages)
        final, = self.saved('terminal')
        self.assertEqual(final['status'], 'unknown')
        self.assertEqual(final['engine_generate_invocations'], 2)
        self.assertIsNone(final['physical_model_attempts'])
        self.assertTrue(final['client_closed'])

    def test_expired_query_never_opens_socket_or_submits_generation(self):
        self.configure(deadline_monotonic=time.monotonic() - 1)
        with self.assertRaises(TimeoutError):
            provider.infer_batch(self.messages)
        self.factory.assert_not_called()
        final, = self.saved('terminal')
        self.assertFalse(final['ipc_attempted'])
        self.assertEqual(final['calls'][0]['status'], 'not_started')
        self.assertFalse(final['calls'][0]['attempted'])

    def test_query_deadline_bounds_entire_send_and_partial_response_reads(self):
        self.configure(deadline_monotonic=time.monotonic() + 1)
        provider.infer_batch(self.messages)
        self.assertGreater(len(self.socket.timeouts), 3)
        self.assertTrue(all(0 < value <= 1 for value in self.socket.timeouts))
        self.assertLessEqual(self.socket.timeouts[-1], self.socket.timeouts[0])

    def test_client_close_failure_is_not_reported_as_success(self):
        self.socket.close_error = RuntimeError('planned close failure')
        with self.assertRaisesRegex(RuntimeError, 'planned close failure'):
            provider.infer_batch(self.messages)
        self.assertIsNone(self.saved('terminal')[0]['client_closed'])
        self.assertEqual(self.saved('terminal')[0]['status'], 'unknown')

    def test_primary_failure_is_preserved_when_client_close_also_fails(self):
        self.socket.read_error = ConnectionResetError('first failure')
        self.socket.close_error = RuntimeError('cleanup failure')
        with self.assertRaisesRegex(ConnectionResetError, 'first failure') as raised:
            provider.infer_batch(self.messages)
        self.assertIn('IPC client close', raised.exception.__notes__[0])

    def test_terminal_evidence_failure_preserves_primary_error(self):
        self.socket.read_error = ConnectionResetError('first failure')
        publish = provider.publish_private_json

        def write(path, value):
            if str(path).endswith('.terminal.json'):
                raise OSError('planned evidence failure')
            return publish(path, value)

        with patch.object(provider, 'publish_private_json', side_effect=write):
            with self.assertRaises(ConnectionResetError) as raised:
                provider.infer_batch(self.messages)
        self.assertIn('terminal receipt', raised.exception.__notes__[0])
        self.assertEqual(len(self.saved('started')), 1)
        self.assertEqual(len(self.saved('terminal')), 0)

    def test_invalid_or_old_http_configuration_cannot_allocate_resources(self):
        for change in ({'url': 'http://127.0.0.1'}, {'socket_path': 'relative.sock'},
                       {'provider_mode': 'software_http'}, {'request_timeout_s': 0},
                       {'schema': 'semloom.imlane.duckdb.q3.model_provider.v1'},
                       {'sampling_params': dict(max_tokens=4096, temperature=0, top_p=1, n=2)}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                provider.configuration(dict(self.config, **change))
        self.factory.assert_not_called()

    def test_missing_or_invalid_messages_fail_before_dispatch(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(RuntimeError):
            provider.infer_batch(self.messages)
        for rows in ([[]], ['not messages'], None):
            with self.subTest(rows=rows), self.assertRaises((ValueError, TypeError)):
                provider.infer_batch(rows)
        self.factory.assert_not_called()

    def test_empty_author_batch_creates_no_socket_or_receipt(self):
        self.assertEqual(provider.infer_batch([]), [])
        self.factory.assert_not_called()
        self.assertEqual(list(self.receipts.iterdir()), [])
