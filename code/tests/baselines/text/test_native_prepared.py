"""Timing placement and native request preservation with in-process fixtures."""
from contextlib import contextmanager
import re
import types
import unittest
from unittest.mock import patch

from src.baselines.common.contracts import ChatRequest
from src.baselines.text.frameworks import semantic_map
from src.baselines.text.products import duckdb_ai


VALUES = [
    {'source_example_id': 'row-a', 'input_text': "重复 'quoted'\nreview"},
    {'source_example_id': 'row-b', 'input_text': "重复 'quoted'\nreview"},
]
PLAN = types.SimpleNamespace(instruction="Classify '完整影评'.", max_tokens=8)
MODEL = types.SimpleNamespace(endpoint_url='http://127.0.0.1:8000/v1/chat/completions',
                              model_id='fixture', bearer_token=None, timeout_ms=30000)


@contextmanager
def lotus_fixture(*, fail=False, cache_hits=0):
    state = types.SimpleNamespace(calls=[], frames=[], loads=[], options=[])
    original_lm = object()
    settings = types.SimpleNamespace(lm=original_lm, enable_cache=True)
    def configure(**kwargs):
        vars(settings).update(kwargs)
    settings.configure = configure
    class LM:
        def __init__(self, name, **options):
            state.options.append((name, options))
            self.stats = types.SimpleNamespace(cache_hits=cache_hits)
    class Frame:
        def __init__(self, data):
            self.data = data
            state.frames.append(data)
        def sem_map(self, instruction, **options):
            state.calls.append((instruction, options))
            if fail:
                raise RuntimeError('native LOTUS query failed')
            return {'row_id': self.data['row_id'], '_answer': ['POSITIVE'] * len(VALUES)}
    pandas = types.ModuleType('pandas')
    pandas.DataFrame = Frame
    lotus = types.ModuleType('lotus')
    lotus.settings = settings
    models = types.ModuleType('lotus.models')
    models.LM = LM
    tokenizers = types.ModuleType('tokenizers')
    def from_file(path):
        state.loads.append(path)
        return path
    tokenizers.Tokenizer = types.SimpleNamespace(from_file=from_file)
    with patch.dict('sys.modules', {'pandas': pandas, 'lotus': lotus,
            'lotus.models': models, 'tokenizers': tokenizers}), \
            patch.object(semantic_map.importlib.metadata, 'version', return_value='1.2.4'):
        state.settings, state.original_lm = settings, original_lm
        yield state


@contextmanager
def daft_fixture(*, fail=False):
    state = types.SimpleNamespace(calls=[], frames=[], providers=[], iterators=[], prompts=[],
                                  runner_calls=[])
    class Expression:
        def __init__(self, evaluate):
            self.evaluate = evaluate
        def __add__(self, other):
            return Expression(lambda row: self.evaluate(row) + other.evaluate(row))
    class Rows:
        def __init__(self, rows, expression):
            self.rows = iter(rows)
            self.expression = expression
            self.closed = False
            state.iterators.append(self)
        def __iter__(self):
            return self
        def __next__(self):
            if self.closed:
                raise StopIteration
            row = next(self.rows)
            text = self.expression.evaluate(row)
            state.prompts.append(text)
            if fail:
                raise RuntimeError('native Daft query failed')
            return {'row_id': row['row_id'], 'output': 'POSITIVE'}
        def close(self):
            self.closed = True
    class Frame:
        def __init__(self, data):
            self.data = data
            state.frames.append(data)
        def with_column(self, name, expression):
            self.expression = expression
            return self
        def select(self, *_names):
            return self
        def iter_rows(self):
            rows = [dict(zip(self.data, row)) for row in zip(*self.data.values())]
            return Rows(rows, self.expression)
    class Provider:
        def __init__(self, **options):
            state.providers.append(options)
    def prompt(expression, **options):
        state.calls.append(options)
        return expression
    daft = types.ModuleType('daft')
    daft.__version__ = '0.7.21'
    def set_runner_native(**options):
        if state.runner_calls:
            raise RuntimeError('Compute runtime num worker threads already set')
        state.runner_calls.append(options)
    daft.set_runner_native = set_runner_native
    daft.from_pydict = Frame
    daft.col = lambda name: Expression(lambda row: row[name])
    daft.lit = lambda value: Expression(lambda _row: value)
    provider = types.ModuleType('daft.ai.openai.provider')
    provider.OpenAIProvider = Provider
    functions = types.ModuleType('daft.functions')
    functions.prompt = prompt
    with patch.dict('sys.modules', {'daft': daft, 'daft.functions': functions,
            'daft.ai.openai.provider': provider}):
        yield state


class SQLResult:
    def __init__(self, rows):
        self.rows = rows
    def fetchall(self):
        return self.rows
    def fetchone(self):
        return self.rows[0] if self.rows else None


class DuckConnection:
    def __init__(self, *, fail_at=None, result_rows=None, version='v1.5.4'):
        self.fail_at = fail_at
        self.result_rows = result_rows
        self.version = version
        self.statements, self.inserted, self.prompts = [], [], []
        self.closed = False
    def execute(self, statement):
        self.statements.append(statement)
        if self.fail_at and self.fail_at in statement:
            raise RuntimeError('native DuckDB query failed')
        if statement == 'SELECT version()':
            return SQLResult([(self.version,)])
        if 'duckdb_extensions()' in statement:
            return SQLResult([('0.4.14', 'community')])
        if 'ai_try_complete' in statement:
            if '|| raw_text' in statement:
                prefix = re.search(r"ai_try_complete\('((?:''|[^'])*)' \|\| raw_text", statement)
                self.prompts.extend(prefix.group(1).replace("''", "'") + text
                                    for _identity, text in self.inserted)
            else:
                self.prompts.extend(text for _identity, text in self.inserted)
            return SQLResult(self.result_rows if self.result_rows is not None else
                             [(identity, 'POSITIVE', None) for identity, _text in self.inserted])
        return SQLResult([])
    def executemany(self, statement, rows):
        self.statements.append(statement)
        if self.fail_at and self.fail_at in statement:
            raise RuntimeError('native DuckDB load failed')
        self.inserted = list(rows)
    def close(self):
        self.closed = True


@contextmanager
def duck_fixture(connection):
    module = types.ModuleType('duckdb')
    module.connect = lambda _path: connection
    with patch.dict('sys.modules', {'duckdb': module}):
        yield


class PreparedNativeTests(unittest.TestCase):
    def test_lotus_prepares_raw_rows_and_tokenizer_before_query(self):
        with lotus_fixture() as state:
            with semantic_map.prepare_rows('lotus-map', VALUES, PLAN, MODEL,
                    concurrency=16, tokenizer_path='/fixture/tokenizer.json') as prepared:
                self.assertEqual(state.calls, [])
                self.assertEqual(state.loads, ['/fixture/tokenizer.json'])
                self.assertEqual(state.frames[0]['review_text'], [v['input_text'] for v in VALUES])
                self.assertEqual(state.frames[0]['row_id'], ['row-a', 'row-b'])
                self.assertFalse(state.settings.enable_cache)
                self.assertEqual(list(prepared.execute()), [('row-a', 'POSITIVE'), ('row-b', 'POSITIVE')])
                name, options = state.options[0]
                self.assertEqual(name, 'openai/fixture')
                self.assertEqual(options['num_retries'], 0)
                self.assertEqual(options['max_retries'], 0)
                self.assertEqual(options['max_batch_size'], 16)
            self.assertIs(state.settings.lm, state.original_lm)
            self.assertTrue(state.settings.enable_cache)
            with self.assertRaisesRegex(RuntimeError, 'closed'):
                list(prepared.execute())

    def test_lotus_native_instruction_matches_application_entry(self):
        with lotus_fixture() as state:
            list(semantic_map._lotus(VALUES, PLAN, MODEL, 16, None))
            old_call, old_options = state.calls[0], state.options[0]
            with semantic_map.prepare_rows('lotus-map', VALUES, PLAN, MODEL,
                    concurrency=16) as prepared:
                list(prepared.execute())
            self.assertEqual(state.calls[1], old_call)
            self.assertEqual(state.options[1], old_options)

    def test_lotus_error_restores_settings_without_another_query(self):
        with lotus_fixture(fail=True) as state:
            with self.assertRaisesRegex(RuntimeError, 'native LOTUS query failed'):
                with semantic_map.prepare_rows('lotus-map', VALUES, PLAN, MODEL,
                        concurrency=16) as prepared:
                    list(prepared.execute())
            self.assertEqual(len(state.calls), 1)
            self.assertIs(state.settings.lm, state.original_lm)
            self.assertTrue(state.settings.enable_cache)

    def test_lotus_cache_use_is_rejected(self):
        with lotus_fixture(cache_hits=1) as state:
            with self.assertRaisesRegex(ValueError, 'cached response'):
                with semantic_map.prepare_rows('lotus-map', VALUES, PLAN, MODEL,
                        concurrency=16) as prepared:
                    list(prepared.execute())
            self.assertEqual(len(state.calls), 1)
            self.assertIs(state.settings.lm, state.original_lm)

    def test_daft_query_constructs_native_prompts_after_raw_source_is_ready(self):
        with daft_fixture() as state:
            with semantic_map.prepare_rows('daft-prompt', VALUES, PLAN, MODEL,
                    concurrency=64) as prepared:
                self.assertEqual(state.calls, [])
                self.assertEqual(state.prompts, [])
                self.assertEqual(set(state.frames[0]), {'row_id', 'review_text'})
                self.assertEqual(list(prepared.execute()), [('row-a', 'POSITIVE'), ('row-b', 'POSITIVE')])
                self.assertEqual(state.prompts, [semantic_map.application_prompt(PLAN.instruction, v['input_text'])
                                                 for v in VALUES])
                self.assertEqual(state.calls[0]['max_retries'], 0)
                self.assertEqual(state.calls[0]['on_error'], 'raise')
                self.assertEqual(state.calls[0]['concurrency'], 64)
            self.assertTrue(state.iterators[0].closed)

    def test_daft_requests_match_original_native_entry(self):
        with daft_fixture() as state:
            list(semantic_map._daft(VALUES, PLAN, MODEL, 64, 8))
            old_prompts, old_options = list(state.prompts), dict(state.calls[0])
            old_provider = state.providers[0]
        with daft_fixture() as state:
            with semantic_map.prepare_rows('daft-prompt', VALUES, PLAN, MODEL,
                    concurrency=64) as prepared:
                list(prepared.execute())
            self.assertEqual(state.prompts, old_prompts)
            old_options.pop('provider')
            options = dict(state.calls[0])
            options.pop('provider')
            self.assertEqual(options, old_options)
            self.assertEqual(state.providers[0], old_provider)

    def test_daft_repeated_execution_keeps_one_native_runtime_configuration(self):
        with daft_fixture() as state:
            with semantic_map.prepare_rows('daft-prompt', VALUES, PLAN, MODEL,
                    concurrency=64) as prepared:
                first = list(prepared.execute())
                second = list(prepared.execute())
            self.assertEqual(first, second)
            self.assertEqual(state.runner_calls, [{'num_threads': 8}])
            self.assertEqual(len(state.prompts), 4)

    def test_daft_does_not_hide_native_second_thread_configuration_rejection(self):
        with daft_fixture() as state:
            with semantic_map.prepare_rows('daft-prompt', VALUES, PLAN, MODEL,
                    concurrency=64):
                pass
            with self.assertRaisesRegex(RuntimeError, 'worker threads already set'):
                with semantic_map.prepare_rows('daft-prompt', VALUES, PLAN, MODEL,
                        concurrency=64):
                    self.fail('second process-wide thread configuration was accepted')
            self.assertEqual(state.prompts, [])

    def test_daft_partial_consumption_closes_the_native_iterator(self):
        with daft_fixture() as state:
            with semantic_map.prepare_rows('daft-prompt', VALUES, PLAN, MODEL,
                    concurrency=64) as prepared:
                stream = prepared.execute()
                self.assertEqual(next(stream), ('row-a', 'POSITIVE'))
            self.assertTrue(state.iterators[0].closed)
            self.assertEqual(len(state.prompts), 1)
            self.assertEqual(list(stream), [])

    def test_daft_error_closes_iterator_without_retry(self):
        with daft_fixture(fail=True) as state:
            with self.assertRaisesRegex(RuntimeError, 'native Daft query failed'):
                with semantic_map.prepare_rows('daft-prompt', VALUES, PLAN, MODEL,
                        concurrency=64) as prepared:
                    list(prepared.execute())
            self.assertTrue(state.iterators[0].closed)
            self.assertEqual(len(state.prompts), 1)

    def test_duckdb_loads_only_raw_rows_and_generates_exact_prompt_in_select(self):
        connection = DuckConnection()
        with duck_fixture(connection):
            with semantic_map.prepare_rows('duckdb-ai', VALUES, PLAN, MODEL,
                    concurrency=64) as prepared:
                self.assertEqual(connection.prompts, [])
                self.assertEqual(connection.inserted, [(i, v['input_text']) for i, v in enumerate(VALUES)])
                self.assertTrue(any('raw_text VARCHAR' in q for q in connection.statements))
                self.assertFalse(any('ai_try_complete' in q for q in connection.statements))
                self.assertEqual(list(prepared.execute()), [('row-a', 'POSITIVE'), ('row-b', 'POSITIVE')])
                self.assertEqual(connection.prompts, [semantic_map.application_prompt(PLAN.instruction, v['input_text'])
                                                      for v in VALUES])
                settings = '\n'.join(connection.statements)
                self.assertIn('SET duckdb_ai_retry_count = 0', settings)
                self.assertIn('SET duckdb_ai_cache = false', settings)
                self.assertIn('SET duckdb_ai_max_concurrent_requests = 64', settings)
            self.assertTrue(connection.closed)

    def test_duckdb_requests_and_results_match_original_adapter(self):
        config = duckdb_ai.DuckDBAiConfig('http://127.0.0.1:8000/v1', 'fixture', 'EMPTY', 8)
        requests = tuple(ChatRequest(i, semantic_map.application_prompt(PLAN.instruction, v['input_text']),
                                     0, 0, 8, 8, 'fixture', 0) for i, v in enumerate(VALUES))
        old_connection, new_connection = DuckConnection(), DuckConnection()
        old = duckdb_ai.run_duckdb_ai_complete(requests, config,
                                             connection_factory=lambda _config: old_connection)
        with duckdb_ai.prepare_duckdb_ai_projection(
                [(i, v['input_text']) for i, v in enumerate(VALUES)], PLAN.instruction, config,
                connection_factory=lambda _config: new_connection) as prepared:
            new = list(prepared.execute())
        self.assertEqual(new_connection.prompts, old_connection.prompts)
        self.assertEqual(new, [(r.doc_id, r.output_text) for r in old])
        self.assertTrue(new_connection.closed)

    def test_duckdb_load_and_query_errors_close_without_retry(self):
        for failure in ('LOAD ai', 'INSERT INTO', 'ai_try_complete'):
            with self.subTest(failure=failure):
                connection = DuckConnection(fail_at=failure)
                with duck_fixture(connection), self.assertRaisesRegex(RuntimeError, 'native DuckDB'):
                    with semantic_map.prepare_rows('duckdb-ai', VALUES, PLAN, MODEL,
                            concurrency=64) as prepared:
                        list(prepared.execute())
                self.assertTrue(connection.closed)
                self.assertEqual(sum(failure in query for query in connection.statements), 1)

    def test_duckdb_bad_identity_is_rejected_before_query_and_closes_connection(self):
        connection = DuckConnection(version='v1.5.5')
        with duck_fixture(connection), self.assertRaisesRegex(RuntimeError, 'DuckDB1.5.4'):
            with semantic_map.prepare_rows('duckdb-ai', VALUES, PLAN, MODEL,
                    concurrency=64):
                self.fail('incorrect runtime was accepted')
        self.assertTrue(connection.closed)
        self.assertEqual(connection.prompts, [])

    def test_duckdb_bad_or_failed_rows_are_rejected(self):
        cases = ([(0, 'POSITIVE', None), (0, 'POSITIVE', None)],
                 [(0, 'POSITIVE', None), (1, None, 'length')],
                 [(0, 'POSITIVE', None), (1, None, None)])
        for rows in cases:
            with self.subTest(rows=rows):
                connection = DuckConnection(result_rows=rows)
                with duck_fixture(connection), self.assertRaises(ValueError):
                    with semantic_map.prepare_rows('duckdb-ai', VALUES, PLAN, MODEL,
                            concurrency=64) as prepared:
                        list(prepared.execute())
                self.assertTrue(connection.closed)
                self.assertEqual(sum('ai_try_complete' in q for q in connection.statements), 1)

    def test_prepared_source_rejects_missing_duplicate_or_oversized_occurrences(self):
        for values in ([], [VALUES[0], VALUES[0]], VALUES * 2049):
            with self.subTest(rows=len(values)), self.assertRaises(ValueError):
                with semantic_map.prepare_rows('daft-prompt', values, PLAN, MODEL,
                        concurrency=64):
                    self.fail('invalid source reached native execution')


if __name__ == '__main__':
    unittest.main()
