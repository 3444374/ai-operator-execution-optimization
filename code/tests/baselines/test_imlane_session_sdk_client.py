"""SDK child orchestration with local factories; no HTTP, author, Ray or model."""
from contextlib import contextmanager, ExitStack
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from src.baselines.common.private_artifacts import write_private_json
from src.baselines.text.frameworks.native_calls import NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.semantic_methods.lotus import native as native_module, batch as batch_module
from src.semantic_methods.lotus.sdk import PreparedLotusCall

_PATH = Path(__file__).resolve().parents[2] / 'integrations/sema/imlane_session_sdk_client.py'
_SPEC = importlib.util.spec_from_file_location('session_sdk_client_fixture', _PATH)
client = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(client)


class LocalSdk:
    def __init__(self):
        self.lm = None
        self.native_instances = []
        self.fixture = None
        self.query_ids = []
        self.close_error = self.iterator_close_error = self.corrupt = None
        state = self

        class LM:
            def __init__(self, model, **kwargs):
                self.model, self.kwargs = model, kwargs

            def __call__(self, messages):
                return self._process_uncached_messages([(message, None) for message in messages],
                                                      self.kwargs, False, 'local SDK substitute')

        class Frame:
            def __init__(self, data):
                self.data = data

            def sem_map(self, instruction, suffix, return_raw_outputs):
                if instruction != 'Echo {text}.' or return_raw_outputs is not True:
                    raise AssertionError('original sem_map entry arguments changed')
                prompts = [[dict(role='user', content=text)] for text in self.data['text']]
                return {suffix: state.lm(prompts)}

        @contextmanager
        def settings_context(**options):
            old, state.lm = state.lm, options['lm']
            if options['enable_cache'] is not False:
                raise AssertionError('cache unexpectedly enabled')
            try:
                yield
            finally:
                state.lm = old

        self.sdk = SimpleNamespace(LM=LM, tokenizer=object(), pd=SimpleNamespace(DataFrame=Frame),
            lotus=SimpleNamespace(settings=SimpleNamespace(context=settings_context)),
            versions={'lotus-ai': 'local substitute'}, modules={}, source_hashes={})

    def prepared(self, lm, messages, kwargs):
        raw = json.dumps(dict(model=client.MODEL, messages=messages), ensure_ascii=False).encode()
        return PreparedLotusCall(raw, kwargs['api_base'], kwargs['api_key'], kwargs['timeout'])

    def query_result(self, query, rows, root):
        state = self

        class Result:
            decisions = []

            def execute(self):
                state.query_ids.append(query.query_id)
                source = query.model_rows(rows)
                outputs = state.lm([[dict(role='user', content=row['text'])] for row in source])
                self.decisions = [(r['row_id'], value == 'True') for r, value in zip(source, outputs)]
                kept = [r for r, value in zip(source, outputs) if value == 'True']
                pairs = ([('positive_review_cnt', str(len(kept)))] if query.query_id == 3
                         else [(r['row_id'], r['review_id']) for r in kept[:5]])
                if state.corrupt == 'query_count' and query.query_id == 3:
                    pairs = [('positive_review_cnt', '5')]
                yield from pairs
        return Result()

    def fixture_factory(self, maximum, stopped, abort, *, q1, journal_root, response_factory):
        state = self

        class LocalFixture:
            base = 'http://localhost:8123'
            active = peak = 0
            journal_errors = []

            def __init__(self):
                self.receipts, self.responses = [], []
                self.connections, self.closed_connections = set(), set()
                self.allowance = client._HELPERS.PostAllowance(maximum, stopped, abort, payload_identity=q1)
                self.response_factory = response_factory
                self.closed = False

            @property
            def posts(self):
                return len(self.receipts)

            def settled(self):
                pass

            def close(self):
                self.closed = True

            def respond(self, call):
                headers = list(call.headers) + [('Host', urlsplit(call.endpoint).netloc),
                                                ('Content-Length', str(len(call.payload)))]
                if state.corrupt == 'request_header':
                    headers[1] = ('Authorization', 'Bearer changed-local-value')
                row = dict(method='POST', path='/v1/chat/completions', headers=headers,
                    payload_sha256=client._sha(call.payload), payload_bytes=len(call.payload),
                    call_id=None, connection='local-fixture', cookie='')
                self.receipts.append(row)
                self.allowance.receipt(row, self.posts)
                body = self.response_factory(row['path'], row, b'{}')
                response_headers = (('Content-Type', 'application/json'), ('X-Repeat', 'one'),
                                    ('X-Repeat', 'two'), ('Content-Length', str(len(body))))
                self.responses.append(dict(status_code=200, headers=response_headers, http_version='HTTP/1.1',
                    request_payload_sha256=row['payload_sha256'], body_bytes=len(body), body_sha256=client._sha(body)))
                if state.corrupt == 'response_header':
                    response_headers = tuple(pair for pair in response_headers if pair != ('X-Repeat', 'two'))
                return FullModelResponse(200, response_headers, body)
        self.fixture = LocalFixture()
        return self.fixture

    def native_factory(self, backend, options, *, imlane_config):
        if backend != 'imlane' or options.concurrency != 8:
            raise AssertionError('public native factory arguments changed')
        state = self
        config = imlane_config
        bridge = json.loads(Path(config.bridge_config_path).read_text())
        marker = Path(bridge['work_root']) / ('.uncertain-' + bridge['namespace'] + '-101-202.json')

        class Native:
            max_batch_rows = config.max_batch_rows
            max_batch_bytes = config.max_input_payload_bytes
            last_cleanup_errors = ()

            def __init__(self):
                self.config, self.number, self.last_close_report = config, 0, None
                self.process = SimpleNamespace(pid=301, returncode=None)
                self.process.poll = lambda: self.process.returncode
                self._imlane = SimpleNamespace(_uncertain=marker, last_close_report=None,
                    delegate=SimpleNamespace(_process=None))
                self.retained = []
                self.closed = False

            def iter_batch(self, calls, **options):
                if marker.exists():
                    raise RuntimeError('author work is unsettled; explicit owner cleanup required')
                self.number += 1
                self._imlane.delegate._process = self.process
                responses = [state.fixture.respond(c) for c in calls]
                if self.number == 1:
                    self.retained = responses
                if state.corrupt == 'lifecycle' and self.number == 3:
                    object.__setattr__(self.retained[0], 'body', b'changed after simulated Lane reuse')
                if state.corrupt == 'budget_retry':
                    state.fixture.respond(calls[0])
                received = 0
                clean = False
                try:
                    for index in reversed(range(len(calls))):
                        received += 1
                        yield NativeCallOutcome(calls[index].call_id, calls[index].row_id, responses[index])
                    clean = True
                finally:
                    if not clean or not config.reuse_bridge:
                        self.process.returncode = 0 if clean else -15
                    context_count = int(not config.reuse_bridge or self.number == 1)
                    report = dict(clean=clean, received=received, prepared=len(calls),
                        bridge_stopped=not clean or not config.reuse_bridge,
                        bridge_lifecycle='session' if config.reuse_bridge else 'per_batch',
                        bridge_pid=self.process.pid, batch_id='local-batch-' + str(self.number),
                        returncode=self.process.returncode, external_coordinator_requires_cleanup=not clean,
                        remote_http_stopped='unavailable', bridge_stats=dict(submitted=len(calls), delivered=received,
                        lifecycle=dict(context_creations=context_count, registrations=context_count,
                                       author_calls=len(calls), output_copies=received)))
                    self.last_close_report = self._imlane.last_close_report = report
                    if not clean:
                        write_private_json(marker, report)
                        if state.iterator_close_error is not None:
                            raise state.iterator_close_error

            def close(self):
                self.closed = True
                self.process.returncode = 0 if not marker.exists() else -15
                if state.close_error is not None and self.number:
                    raise state.close_error

        result = Native()
        self.native_instances.append(result)
        return result

    @contextmanager
    def substitute(self):
        openai = SimpleNamespace(OpenAIError=type('LocalOpenAIError', (Exception,), {}))
        with ExitStack() as stack:
            stack.enter_context(patch.dict('sys.modules', {'openai': openai, 'litellm': SimpleNamespace(cache=None)}))
            stack.enter_context(patch.object(client, 'load_sdk', return_value=self.sdk))
            stack.enter_context(patch.object(client, 'Fixture', side_effect=self.fixture_factory))
            stack.enter_context(patch.object(client, 'NativeCallExecutor', side_effect=self.native_factory))
            stack.enter_context(patch.object(client, 'MovieQueryResult', side_effect=self.query_result))
            stack.enter_context(patch.object(client.MovieDataset, 'preflight', return_value={'role': 'software_fixture'}))
            stack.enter_context(patch.object(batch_module, 'validate_source'))
            stack.enter_context(patch.object(native_module, 'validate_batch',
                side_effect=lambda lm, data, kwargs, **opts: self.prepared(lm, data[0][0], kwargs)))
            stack.enter_context(patch.object(native_module, 'prepare_call', side_effect=self.prepared))
            stack.enter_context(patch.object(native_module, 'parse_native_response',
                side_effect=lambda lm, messages, kwargs, prepared, outcome, **opts:
                    json.loads(outcome.response.body)['choices'][0]['message']['content']))
            stack.enter_context(patch.object(client.time, 'sleep'))
            yield


class SessionSdkClientTests(unittest.TestCase):
    def args(self, root, *, case='healthy', reuse=False):
        bridge = root / 'bridge.json'
        bridge.write_text(json.dumps(dict(work_root=str(root), namespace='local-sdk-substitute')))
        config = root / 'config.json'
        config.write_text(json.dumps(dict(runtime='cpython', bridge_config_path=str(bridge),
            coordinator_pid=101, coordinator_start_ticks=202, author_build_identity=str(root / 'build.json'),
            max_batch_rows=128, reuse_bridge=reuse)))
        manifest = root / 'source-lock.json'
        manifest.write_text(json.dumps(dict(candidate_commit='9766e784ea9d1f0c16a56a0b81a369847081bbce',
            files_sha256={p: client._sha((client.CODE.parent / p).read_bytes()) for p in client._REQUIRED_SOURCE})))
        return SimpleNamespace(config=config, tokenizer=root / 'tokenizer.json', checkout=root / 'checkout',
                               output=root / 'child', source_manifest=manifest, case=case)

    def run_local(self, *, case='healthy', reuse=False, corrupt=None, close_error=None,
                  parser_error=None, iterator_close_error=None):
        directory = tempfile.TemporaryDirectory(prefix='imlane-session-sdk-local-')
        self.addCleanup(directory.cleanup)
        args = self.args(Path(directory.name), case=case, reuse=reuse)
        sdk = LocalSdk(); sdk.corrupt, sdk.close_error = corrupt, close_error
        sdk.iterator_close_error = iterator_close_error
        with sdk.substitute(), patch.object(client, 'parse_lotus_response', side_effect=parser_error):
            facts = client.run_client(args)
        self.assertEqual(json.loads((args.output / 'summary.json').read_text()), json.loads(json.dumps(facts)))
        return facts, sdk, args

    def test_default_and_reuse_keep_one_factory_and_original_four_batches(self):
        for reuse in (False, True):
            with self.subTest(reuse=reuse):
                facts, sdk, args = self.run_local(reuse=reuse)
                self.assertTrue(facts['passed'], facts['first_error'])
                self.assertEqual(len(sdk.native_instances), 1)
                self.assertEqual([b['rows'] for b in facts['batches']], [4, 12, 6, 6])
                self.assertEqual(sdk.query_ids, [1, 2, 3])
                self.assertEqual(facts['fixture']['maximum'], 36)
                self.assertEqual(facts['fixture']['actual_posts'], 28)
                self.assertEqual([v['after_batch'] for v in facts['output_lifecycle']], [3, 4])
                self.assertEqual(facts['query_results'][-1]['result'], [('positive_review_cnt', '6')])
                self.assertTrue(facts['close']['bridge_stopped'])
                self.assertTrue(facts['cleanup']['fixture_closed'])
                rows = client.MovieDataset(args.checkout, args.output / 'movie-input/manifest.json',
                    expected_manifest_digest=facts['movie_preflight'].get('manifest_content_digest') or
                    json.loads((args.output / 'movie-input/manifest.json').read_text())['sha256'],
                    role='software_fixture').load_rows()
                self.assertEqual(len({r['row_id'] for r in rows}), 12)
                self.assertEqual(len({r['review_id'] for r in rows}), 6)
                self.assertEqual(sum(r['movie_id'] == 'taken_3' for r in rows), 6)

    def test_sdk_parser_callback_stop_keeps_unknown_and_rejects_same_coordinator_owner(self):
        error = ValueError('local stand-in for original SDK bad JSON')
        for reuse in (False, True):
            with self.subTest(reuse=reuse):
                facts, sdk, _ = self.run_local(case='parser', reuse=reuse, parser_error=error)
                self.assertTrue(facts['passed'], facts)
                self.assertTrue(facts['first_error']['expected'])
                self.assertEqual(facts['first_error']['error'], str(error))
                self.assertEqual(len(facts['responses']), 1)
                self.assertEqual(facts['fixture']['maximum'], 12)
                self.assertEqual(facts['fixture']['actual_posts'], 4)
                self.assertEqual(len(sdk.native_instances), 2)
                self.assertEqual(sdk.native_instances[-1].number, 0)
                self.assertTrue(facts['unknown']['first_marker_unchanged'])
                self.assertTrue(facts['unknown']['new_owner_rejected'])
                self.assertFalse(facts['close']['clean'])
                self.assertEqual(facts['close']['remote_http_stopped'], 'unavailable')

    def test_cleanup_failure_cannot_replace_first_sdk_parser_error_or_pass(self):
        first = ValueError('first SDK parser failure')
        facts, sdk, _ = self.run_local(case='parser', reuse=True, parser_error=first,
                                      close_error=OSError('owned bridge close failure'))
        self.assertFalse(facts['passed'])
        self.assertEqual(facts['first_error']['error'], str(first))
        self.assertEqual(facts['first_error']['type'], 'ValueError')
        self.assertTrue(facts['cleanup']['errors'])
        self.assertTrue(sdk.fixture.closed)

    def test_iterator_close_failure_retains_sdk_first_error_notes_and_cleanup_failure(self):
        first = ValueError('first SDK parser exception')
        facts, sdk, _ = self.run_local(case='parser', reuse=True, parser_error=first,
                                      iterator_close_error=OSError('iterator close fixture failure'))
        self.assertFalse(facts['passed'])
        self.assertEqual(facts['first_error']['error'], str(first))
        self.assertTrue(facts['first_error']['expected'])
        self.assertTrue(facts['first_error']['notes'])
        self.assertEqual(facts['cleanup']['errors'][0]['stage'], 'sdk_close')
        self.assertTrue(facts['unknown']['new_owner_rejected'])
        self.assertTrue(sdk.fixture.closed)

    def test_full_request_headers_response_headers_and_retained_bytes_are_checked(self):
        for corruption, message in (('request_header', 'request headers'),
                                    ('response_header', 'response metadata'),
                                    ('lifecycle', 'Lane reuse'), ('query_count', 'COUNT')):
            with self.subTest(corruption=corruption):
                facts, _, _ = self.run_local(reuse=True, corrupt=corruption)
                self.assertFalse(facts['passed'])
                self.assertIn(message, facts['first_error']['error'])
                self.assertTrue(facts['cleanup']['fixture_closed'])

    def test_unreserved_extra_post_stops_work_and_retains_budget_failure(self):
        facts, sdk, _ = self.run_local(reuse=True, corrupt='budget_retry')
        self.assertFalse(facts['passed'])
        self.assertTrue(facts['fixture']['violations'])
        self.assertTrue(sdk.fixture.closed)

    def test_manifest_missing_tampered_and_outside_source_fail_before_factories(self):
        with tempfile.TemporaryDirectory(prefix='sdk-lock-rejection-') as temporary:
            args = self.args(Path(temporary))
            original = json.loads(args.source_manifest.read_text())
            for problem in ('missing', 'tampered', 'escape'):
                altered = json.loads(json.dumps(original))
                if problem == 'missing':
                    altered['files_sha256'].pop(next(iter(client._REQUIRED_SOURCE)))
                elif problem == 'tampered':
                    altered['files_sha256'][next(iter(client._REQUIRED_SOURCE))] = '0' * 64
                else:
                    altered['files_sha256']['../outside-source'] = '0' * 64
                args.source_manifest.write_text(json.dumps(altered))
                with self.subTest(problem=problem), self.assertRaises(ValueError):
                    client.verify_source_manifest(args.source_manifest)

    def test_manifest_rejection_never_constructs_sdk_fixture_or_native_factory(self):
        with tempfile.TemporaryDirectory(prefix='sdk-before-factories-') as temporary:
            args = self.args(Path(temporary))
            lock = json.loads(args.source_manifest.read_text())
            lock['files_sha256'].pop('code/integrations/sema/imlane_session_sdk_client.py')
            args.source_manifest.write_text(json.dumps(lock))
            with patch.object(client, 'load_sdk') as sdk, patch.object(client, 'Fixture') as fixture, \
                 patch.object(client, 'NativeCallExecutor') as native:
                facts = client.run_client(args)
            self.assertFalse(facts['passed'])
            self.assertEqual(facts['first_error']['type'], 'ValueError')
            sdk.assert_not_called(); fixture.assert_not_called(); native.assert_not_called()


if __name__ == '__main__':
    unittest.main()
