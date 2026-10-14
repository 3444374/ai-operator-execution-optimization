"""Finite LOTUS SDK child for an already owned author Coordinator; zero models."""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from copy import deepcopy
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path, PurePosixPath
import signal
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.native_calls import NativeCallExecutor
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.postgresql.imlane_query import load_imlane_call_configs
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.postgresql.sembench_queries import MovieDataset, MovieQuery, MovieQueryResult
from src.semantic_methods.lotus.batch import lotus_executor
from src.semantic_methods.lotus.native import LotusNativeBatchExecutor
from src.semantic_methods.lotus.sdk import parse_lotus_response

_SPEC = importlib.util.spec_from_file_location(
    'imlane_session_fixture_helpers', Path(__file__).with_name('verify_imlane_async_calls.py'))
_HELPERS = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_HELPERS)
Fixture, StopBudget = _HELPERS.Fixture, _HELPERS.StopBudget

MODEL = 'Qwen2.5-1.5B-Instruct'
TOKEN = 'local-fixture'
_REQUIRED_SOURCE = {
    'code/integrations/sema/imlane_session_sdk_client.py',
    'code/integrations/sema/verify_imlane_async_calls.py',
    'code/src/baselines/text/frameworks/native_calls.py',
    'code/src/baselines/text/frameworks/imlane_calls.py',
    'code/src/baselines/text/frameworks/imlane_limits.py',
    'code/src/baselines/text/frameworks/prepared_map.py',
    'code/src/baselines/text/frameworks/lotus_pg.py',
    'code/src/baselines/text/sembench_movie.py',
    'code/src/baselines/common/private_artifacts.py',
    'code/src/baselines/common/redact.py',
    'code/src/execution_provider/adapters/imlane_session.py',
    'code/src/execution_provider/adapters/sema_imlane.py',
    'code/src/execution_provider/adapters/model_config.py',
    'code/src/execution_provider/adapters/full_response.py',
    'code/src/semantic_methods/lotus/native.py',
    'code/src/semantic_methods/lotus/batch.py',
    'code/src/semantic_methods/lotus/sdk.py',
    'code/src/semantic_methods/lotus/limits.py',
    'code/src/experiments/postgresql/sembench_queries.py',
    'code/src/experiments/postgresql/query_workloads.py',
    'code/src/experiments/postgresql/query_inputs.py',
    'code/src/experiments/postgresql/imlane_query.py',
}


def _expect(condition, message):
    if not condition:
        raise AssertionError(message)


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def verify_source_manifest(path):
    lock = json.loads(Path(path).read_text())
    files = lock.get('files_sha256')
    commit = lock.get('candidate_commit')
    if (type(commit) is not str or len(commit) != 40
            or any(c not in '0123456789abcdef' for c in commit)):
        raise ValueError('SDK child source manifest requires the candidate commit')
    if type(files) is not dict or not _REQUIRED_SOURCE <= set(files):
        raise ValueError('SDK child source manifest is incomplete')
    for name, expected in files.items():
        relative = PurePosixPath(name)
        target = (CODE.parent / name).resolve()
        if (relative.is_absolute() or '..' in relative.parts or not target.is_relative_to(CODE.parent)
                or type(expected) is not str or len(expected) != 64
                or any(c not in '0123456789abcdef' for c in expected)):
            raise ValueError('SDK child source manifest has an invalid entry')
        if _sha(target.read_bytes()) != expected:
            raise ValueError('SDK child candidate source differs: ' + name)
    return dict(candidate_commit=lock.get('candidate_commit'), files_sha256=files)


def load_sdk(tokenizer):
    import lotus
    import pandas as pd
    from lotus.models import LM
    from tokenizers import Tokenizer
    from src.semantic_methods.lotus.sdk import validate_source, SOURCE_HASHES, LITELLM_SOURCE_HASHES
    validate_source()
    modules = {name: __import__(name) for name in ('lotus', 'litellm', 'openai', 'pandas', 'tokenizers')}
    hashes = {name + '/' + relative: _sha((Path(modules[name].__file__).parent / relative).read_bytes())
              for name, paths in (('lotus', SOURCE_HASHES), ('litellm', LITELLM_SOURCE_HASHES))
              for relative in paths}
    return SimpleNamespace(lotus=lotus, pd=pd, LM=LM, tokenizer=Tokenizer.from_file(str(tokenizer)),
        versions={name: importlib.metadata.version(name)
                  for name in ('lotus-ai', 'litellm', 'openai', 'pandas', 'tokenizers', 'pyarrow')},
        modules={name: module.__file__ for name, module in modules.items()}, source_hashes=hashes)


def movie_fixture(root, checkout):
    examples = [('movie-row-' + str(i), 'taken_3' if i % 2 == 0 else 'fixture_other',
                 f'Synthetic review occurrence {i:02d}: ' + ('positive' if i % 2 == 0 or i in (1, 5, 9) else 'negative'),
                 'POSITIVE' if i % 2 == 0 or i in (1, 5, 9) else 'NEGATIVE',
                 'duplicate-review-' + str(i // 2)) for i in range(12)]
    manifest = prepare(root, 'movie', examples, {'source': 'synthetic SemBench software fixture'}, max_rows=12)
    dataset = MovieDataset(checkout, root / 'manifest.json', expected_manifest_digest=manifest['sha256'],
                           role='software_fixture')
    return dataset, {row[0]: row[3] == 'POSITIVE' for row in examples}


def response_record(full):
    return dict(status_code=full.status_code, headers=[list(pair) for pair in full.headers],
        http_version=full.http_version, body_bytes=len(full.body), body_sha256=_sha(full.body),
        body_base64=base64.b64encode(full.body).decode('ascii'))


class CapturedCalls:
    """Observe and reserve original complete calls; the author still owns execution."""
    def __init__(self, native, fixture, case, facts):
        self.native, self.fixture, self.case, self.facts = native, fixture, case, facts
        self.max_batch_rows, self.max_batch_bytes = native.max_batch_rows, native.max_batch_bytes
        self.last_close_report = None
        self.last_cleanup_errors = ()
        self.label, self.contents, self.plans = None, (), {}
        self.calls, self.full = {}, {}
        self.first_retained = ()
        self.lock = threading.Lock()
        self.parser_error = None
        self.lm = self.messages = self.kwargs = None
        self.prepared = {}

    def select(self, label, contents):
        self.label, self.contents = label, tuple(contents)

    def on_batch(self, data, kwargs):
        self.messages, self.kwargs, self.prepared = data, kwargs, {}

    def on_prepared(self, index, value):
        self.prepared[index] = value

    def on_response(self, index, full):
        if self.case == 'parser':
            # LM normally consumes returned OpenAIError values after its batch finishes.
            # This explicit probe uses its actual SDK parser to stop this iterator early.
            try:
                parse_lotus_response(self.lm, self.messages[index][0], self.kwargs,
                                     self.prepared[index], full)
            except Exception as error:
                self.parser_error = error
                raise
            raise AssertionError('bad JSON unexpectedly passed the original SDK parser')

    def response_factory(self, path, receipt, default_body):
        with self.lock:
            plan = self.plans[receipt['payload_sha256']]
        _expect(path == '/v1/chat/completions', 'SDK request path changed')
        # Delay the first call only; no request header/payload or author scheduling change.
        time.sleep(plan['delay_s'])
        return plan['body']

    def iter_batch(self, calls, **options):
        calls = tuple(calls)
        _expect(len(calls) == len(self.contents), 'SDK uncached batch shape changed')
        _expect(len({c.call_id for c in calls}) == len(calls), 'SDK batch repeats a call identity')
        _expect(len({_sha(c.payload) for c in calls}) == len(calls), 'fixture prompts must distinguish occurrences')
        self.fixture.allowance.reserve(calls)
        batch = dict(label=self.label, rows=len(calls), call_ids=[c.call_id for c in calls], delivery_order=[], close=None)
        self.facts['batches'].append(batch)
        with self.lock:
            self.plans = {}
            for index, call in enumerate(calls):
                body = (b'{broken-json' if self.case == 'parser' else json.dumps(dict(
                    id='fixture-' + call.call_id, object='chat.completion', created=0, model=MODEL,
                    choices=[dict(index=0, message=dict(role='assistant', content=self.contents[index]),
                                  finish_reason='stop')],
                    usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3))).encode())
                self.plans[_sha(call.payload)] = dict(body=body, delay_s=.4 if index == 0 else 0)
                self.calls[call.call_id] = (call, body)
                self.facts['requests'].append(dict(batch=self.label, call_id=call.call_id, row_id=call.row_id,
                    endpoint=call.endpoint, payload_sha256=_sha(call.payload), payload_bytes=len(call.payload),
                    headers=[list(pair) for pair in call.headers]))
        iterator = self.native.iter_batch(calls, **options)
        try:
            for outcome in iterator:
                _expect(outcome.call_id in batch['call_ids'] and outcome.row_id == calls.index(self.calls[outcome.call_id][0]),
                        'SDK response occurrence changed')
                _expect(outcome.row_id not in batch['delivery_order'], 'SDK response repeats an occurrence')
                batch['delivery_order'].append(outcome.row_id)
                if outcome.response is not None:
                    expected = self.calls[outcome.call_id][1]
                    _expect(outcome.response.body == expected, 'SDK response body differs from fixture')
                    self.full[outcome.call_id] = outcome.response
                    self.facts['responses'].append(dict(batch=self.label, call_id=outcome.call_id,
                        row_id=outcome.row_id, **response_record(outcome.response)))
                yield outcome
        finally:
            try:
                iterator.close()
            finally:
                self.last_close_report = deepcopy(self.native.last_close_report)
                self.last_cleanup_errors = self.native.last_cleanup_errors
                batch['close'] = self.last_close_report

    def retain_first(self):
        self.first_retained = tuple((identity, full, response_record(full)) for identity, full in self.full.items())

    def recheck_first(self, batch_number):
        _expect(all(response_record(full) == saved for _, full, saved in self.first_retained),
                'retained first-batch response changed after Lane reuse')
        self.facts['output_lifecycle'].append(dict(after_batch=batch_number, retained_responses=len(self.first_retained),
                                                  complete_response_bytes_unchanged=True))

    def verify_fixture(self, complete):
        expected = Counter(_sha(c.payload) for c, _ in self.calls.values())
        actual = Counter(row['payload_sha256'] for row in self.fixture.receipts)
        _expect(actual == expected if complete else not actual - expected, 'fixture request multiplicity changed')
        for row in self.fixture.receipts:
            call = next(c for c, _ in self.calls.values() if _sha(c.payload) == row['payload_sha256'])
            _expect(row['method'] == 'POST' and row['path'] == urlsplit(call.endpoint).path
                    and row['payload_bytes'] == len(call.payload), 'complete SDK request changed')
            headers = Counter((name.lower(), value) for name, value in row['headers'])
            explicit = Counter((name.lower(), value) for name, value in call.headers)
            _expect(not explicit - headers, 'original SDK request headers changed')
            remaining = headers - explicit
            allowed = {'host', 'accept', 'accept-encoding', 'user-agent', 'content-length'}
            _expect(all(name in allowed for name, _ in remaining), 'unexpected extra SDK request header')
            _expect(headers[('content-length', str(len(call.payload)))] == 1, 'SDK request length header changed')
            _expect(headers[('host', urlsplit(call.endpoint).netloc)] == 1, 'SDK request Host header changed')
        matched = []
        for identity, full in self.full.items():
            call, body = self.calls[identity]
            record = response_record(full)
            candidates = [r for r in self.fixture.responses if r['request_payload_sha256'] == _sha(call.payload)
                          and r['body_sha256'] == record['body_sha256']]
            _expect(len(candidates) == 1, 'SDK response has no unique fixture receipt')
            original = candidates[0]
            _expect(full.body == body and all(record[k] == original[k] for k in
                ('status_code', 'http_version', 'body_sha256', 'body_bytes'))
                and tuple(full.headers) == tuple(tuple(p) for p in original['headers']),
                'complete SDK response metadata changed')
            _expect([v for n, v in full.headers if n.lower() == 'x-repeat'] == ['one', 'two'],
                    'duplicate response headers were lost')
            matched.append(record['body_sha256'])
        _expect(not self.fixture.allowance.violations and not self.fixture.journal_errors,
                'fixture budget or receipt journal failed')
        if complete:
            _expect(len(matched) == len(self.calls) == len(self.fixture.responses), 'SDK responses are incomplete')


def verify_lifecycle(batches, reuse):
    _expect([b['rows'] for b in batches] == [4, 12, 6, 6], 'expected original SDK batches did not execute')
    for number, batch in enumerate(batches):
        close = batch['close']
        _expect(close['clean'] is True and close['received'] == batch['rows'], 'healthy SDK batch was not drained')
        stats = close['bridge_stats']
        _expect(stats['submitted'] == stats['delivered'] == batch['rows'], 'bridge counters differ from SDK calls')
        _expect(stats['lifecycle'] == dict(context_creations=int(not reuse or number == 0),
            registrations=int(not reuse or number == 0), author_calls=batch['rows'], output_copies=batch['rows']),
            'bridge context/function/output ownership counters changed')
        if not reuse:
            _expect(close['bridge_stopped'] is True and close['returncode'] == 0
                    and close['bridge_lifecycle'] == 'per_batch', 'default bridge did not reach normal EOF')
    if reuse:
        _expect(len({b['close']['bridge_pid'] for b in batches}) == 1
                and len({b['close']['batch_id'] for b in batches}) == len(batches),
                'healthy SDK session did not retain one bridge with distinct batch fences')
    _expect(any(b['delivery_order'] != list(range(b['rows'])) for b in batches), 'fixture did not exercise reordering')


def verify_unknown(native, config, captured):
    marker = native._imlane._uncertain
    _expect(marker.is_file(), 'SDK parser stop did not persist unknown author work')
    before = marker.read_bytes()
    close = native._imlane.last_close_report
    _expect(close['clean'] is False and close['external_coordinator_requires_cleanup'] is True
            and close['bridge_stopped'] is True and close['remote_http_stopped'] == 'unavailable',
            'SDK parser stop was incorrectly reported as clean or remotely cancelled')
    probe = NativeCallExecutor('imlane', NativeGraphOptions(concurrency=8), imlane_config=config)
    iterator = probe.iter_batch((next(iter(captured.calls.values()))[0],), timeout_s=1)
    rejected = None
    try:
        next(iterator)
    except RuntimeError as error:
        if 'unsettled' not in str(error):
            raise
        rejected = error
    finally:
        iterator.close()
        probe.close()
    _expect(rejected is not None and marker.read_bytes() == before, 'new owner admitted unknown work or replaced first marker')
    return dict(marker_path=str(marker), marker_sha256=_sha(before), marker=json.loads(before),
        new_owner_rejected=True, rejection=redact_text(str(rejected)), probe='NativeCallExecutor.iter_batch',
        first_marker_unchanged=True, remote_http_stopped='unavailable')


def run_client(args, *, stopped=None, stop_budget=None):
    new_private_directory(args.output)
    stopped = stopped or threading.Event()
    stop_budget = stop_budget or StopBudget(stopped)
    facts = dict(schema='semloom.imlane_session_sdk.v1', passed=False, case=args.case, models=0, gpu=0, retries=0,
        requests=[], responses=[], batches=[], query_results=[], output_lifecycle=[], first_error=None,
        unknown=None, close=None, cleanup=dict(errors=[], executor_closed=False, fixture_closed=False,
        external_coordinator_owner='main runner; cleanup requires its separate confirmation'))
    native = fixture = captured = selected = None
    primary = None
    work_passed = False
    abort_errors = []
    maximum = 36 if args.case == 'healthy' else 12

    def abort():
        stopped.set()
        if native is not None:
            try:
                native.close()
            except BaseException as error:
                abort_errors.append(error)

    try:
        facts['source'] = verify_source_manifest(args.source_manifest)
        mode = json.loads(args.config.read_text())['runtime']
        config = load_imlane_call_configs({mode: args.config})[mode]
        facts.update(runtime=config.runtime, reuse_bridge=config.reuse_bridge)
        sdk = load_sdk(args.tokenizer)
        facts.update(module_versions=sdk.versions, modules=sdk.modules, sdk_source_hashes=sdk.source_hashes)
        fixture = Fixture(maximum, stopped, abort, q1=True, journal_root=args.output,
            response_factory=lambda *values: captured.response_factory(*values))
        model = FixedModelConfig(fixture.base + '/v1/chat/completions', MODEL, 5000, TOKEN)
        lm = sdk.LM('openai/' + MODEL, api_base=fixture.base + '/v1', api_key=TOKEN, temperature=0, top_p=1,
            max_tokens=8, max_batch_size=8, stop=['\n'], num_retries=0, max_retries=0, timeout=5, tokenizer=sdk.tokenizer)
        native = NativeCallExecutor('imlane', NativeGraphOptions(concurrency=8), imlane_config=config)
        captured = CapturedCalls(native, fixture, args.case, facts)
        captured.lm = lm
        selected = LotusNativeBatchExecutor(captured, model, query_id='sdk-session', operator_id='original-method',
            max_batch_rows=config.max_batch_rows, on_batch=captured.on_batch, on_prepared=captured.on_prepared,
            on_response=captured.on_response, retain_full_responses=True, cancelled=stopped.is_set)
        facts['public_entry'] = dict(factory='NativeCallExecutor', backend='imlane',
            factory_type=type(native).__name__, delegate_type=type(native._imlane).__name__,
            bridge_delegate_type=type(native._imlane.delegate).__name__,
            method_executor='LotusNativeBatchExecutor', selection='lotus_executor', map='pandas.DataFrame.sem_map',
            query='MovieDataset(software_fixture) -> MovieQueryResult.execute',
            parser_probe=('on_response invokes original SDK replay to stop consumption; normal LM may consume error values'
                          if args.case == 'parser' else None))
        with sdk.lotus.settings.context(lm=lm, enable_cache=False), lotus_executor(lm, selected):
            captured.select('sem_map', ['map-' + str(i) for i in range(4)])
            mapped = sdk.pd.DataFrame({'text': ['synthetic-map-' + str(i) for i in range(4)]}).sem_map(
                'Echo {text}.', suffix='_sdk_fixture', return_raw_outputs=True)
            _expect(args.case == 'healthy', 'parser probe did not stop its iterator')
            _expect(list(mapped['_sdk_fixture']) == list(captured.contents), 'original sem_map reordered outputs')
            captured.retain_first()
            dataset, decisions = movie_fixture(args.output / 'movie-input', args.checkout)
            facts['movie_preflight'] = dataset.preflight()
            rows = dataset.load_rows()
            arm = 'lotus-method-native-imlane' + ('-ray' if mode == 'ray' else '')
            for query_id in (1, 2, 3):
                stop_budget.check()
                query = MovieQuery(dataset, query_id)
                query.check_executor_shape(arm, rows, imlane_config=config)
                model_rows = query.model_rows(rows)
                captured.select('q' + str(query_id), ['True' if decisions[r['row_id']] else 'False' for r in model_rows])
                query_root = args.output / ('q' + str(query_id)); query_root.mkdir(mode=0o700)
                result = MovieQueryResult(query, rows, query_root)
                pairs = list(result.execute())
                expected_decisions = [(r['row_id'], decisions[r['row_id']]) for r in model_rows]
                kept = [r for r in model_rows if decisions[r['row_id']]]
                expected = ([('positive_review_cnt', str(len(kept)))] if query_id == 3
                            else [(r['row_id'], r['review_id']) for r in kept[:5]])
                _expect(result.decisions == expected_decisions and pairs == expected,
                        'original Filter order/LIMIT/COUNT differs from independent fixture decisions')
                facts['query_results'].append(dict(query_id=query_id, decisions=result.decisions, result=pairs,
                    source_rows=len(model_rows), independent_order_limit_count_verified=True))
                if query_id >= 2:
                    captured.recheck_first(query_id + 1)
        fixture.settled()
        captured.verify_fixture(complete=True)
        verify_lifecycle(facts['batches'], config.reuse_bridge)
        _expect(fixture.posts == fixture.allowance.reserved == 28, 'healthy SDK POST count changed')
        work_passed = True
    except BaseException as error:
        primary = error
        if args.case == 'parser' and captured is not None and error is captured.parser_error:
            try:
                _expect(len(captured.full) < len(captured.calls) == 4, 'SDK parser probe consumed the complete batch')
                native.close()
                facts['unknown'] = verify_unknown(native, config, captured)
                fixture.settled()
                captured.verify_fixture(complete=False)
                _expect(0 < fixture.posts <= 4 and fixture.allowance.reserved == 4, 'parser POST count changed')
                work_passed = True
            except BaseException as check_error:
                facts['parser_verification_error'] = dict(type=type(check_error).__name__, error=redact_text(str(check_error)))
    finally:
        if selected is not None:
            facts['cleanup']['errors'] += [dict(stage='sdk_' + stage, type=type(error).__name__,
                error=redact_text(str(error))) for stage, error in selected.last_cleanup_errors]
        with stop_budget.cleanup():
            for name, resource in (('executor', native), ('fixture', fixture)):
                if resource is None:
                    continue
                try:
                    process = native._imlane.delegate._process if name == 'executor' else None
                    resource.close()
                    facts['cleanup'][name + '_closed'] = True
                    if name == 'executor':
                        facts['close'] = deepcopy(native._imlane.last_close_report or native.last_close_report)
                        facts['cleanup']['owned_bridge_stopped'] = process is None or process.poll() is not None
                        _expect(facts['cleanup']['owned_bridge_stopped'], 'SDK child bridge remains alive')
                        if facts['close'] is not None:
                            facts['close'].update(bridge_stopped=facts['cleanup']['owned_bridge_stopped'],
                                bridge_close_returncode=None if process is None else process.returncode,
                                close_scope='owned bridge after public close; Coordinator still belongs to main runner')
                except BaseException as error:
                    facts['cleanup']['errors'].append(dict(stage=name, type=type(error).__name__, error=redact_text(str(error))))
                    if primary is None:
                        primary = error
                    else:
                        primary.add_note('SDK child ' + name + ' cleanup also failed: ' + type(error).__name__)
        facts['cleanup']['errors'] += [dict(stage='budget_abort', type=type(e).__name__, error=redact_text(str(e))) for e in abort_errors]
        if fixture is not None:
            facts['fixture'] = dict(actual_posts=fixture.posts, maximum=maximum, reserved=fixture.allowance.reserved,
                receipts=fixture.receipts, responses=fixture.responses, violations=fixture.allowance.violations,
                journal_errors=fixture.journal_errors, connections=len(fixture.connections),
                closed_connections=len(fixture.closed_connections), peak=fixture.peak)
        if primary is not None:
            facts['first_error'] = dict(type=type(primary).__name__, module=type(primary).__module__,
                error=redact_text(str(primary)), notes=[redact_text(n) for n in getattr(primary, '__notes__', ())],
                expected=args.case == 'parser' and captured is not None and primary is captured.parser_error)
        facts['passed'] = work_passed and not facts['cleanup']['errors'] and not stopped.is_set()
        write_private_json(args.output / 'summary.json', facts)
    return facts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'tokenizer', 'checkout', 'output', 'source-manifest'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--case', choices=('healthy', 'parser'), required=True)
    args = parser.parse_args(argv)
    stopped = threading.Event()
    budget = StopBudget(stopped)
    previous = signal.signal(signal.SIGTERM, budget.signal)
    try:
        facts = run_client(args, stopped=stopped, stop_budget=budget)
        print(json.dumps(dict(passed=facts['passed'], case=args.case, models=0, gpu=0)))
        return 0 if facts['passed'] else 1
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == '__main__':
    raise SystemExit(main())
