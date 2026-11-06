"""Declared bridge slots through the actual carrier and standalone C++ limits."""
from pathlib import Path
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import asdict, replace
import shutil
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from integrations.imlane_records.client import RecordsBridge
from integrations.imlane_records.chain_backend import IMLaneChainBackend
from integrations.imlane_records.records_host import RecordsResults
from integrations.imlane_records.filter_workload import PROTOCOL
from integrations.imlane_records.transport_limits import for_owner
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.semantic_methods.lotus.record_chain import LotusRecordChain, StageSpec
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class BridgeCapacityTests(unittest.TestCase):
    def test_filter_resident_and_cascade_import_the_shared_movie_query(self):
        from integrations.imlane_records import cascade_host, resident, verify_filter
        from src.experiments import movie_records_query

        self.assertIs(verify_filter.run_movie_query, movie_records_query.run_movie_query)
        self.assertIs(resident.verify_filter, verify_filter)
        self.assertIs(cascade_host.FilterScope, verify_filter.FilterScope)

    def test_Movie_filter_open_passes_its_declared_capacity_without_starting_a_bridge(self):
        from src.experiments import imlane_movie_formal as movie

        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4)
        method = FilterMethod('openai/fixture', 'Keep {text}')
        engine = EngineSpec('/tmp/bridge-model', '/tmp/model-python', '/tmp/driver-python', max_model_calls=1)
        scan = SimpleNamespace(describe=lambda: {}, payload_columns=['text'], predicate=None)
        source = SimpleNamespace(scan_spec=lambda _method: scan,
            rows=lambda: iter([dict(row_id='row', source_position=0, text='CPU')]),
            expected=lambda query: [dict(row_id='row', source_position=0)])
        stream = SimpleNamespace(fileno=lambda: -1)
        process = SimpleNamespace(stdin=stream, stdout=stream, stderr=object(), poll=lambda: 0)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            scope = SimpleNamespace(root=root, port=1234, env={}, children=[],
                __enter__=Mock(), track_root=Mock(), close=Mock())
            args = SimpleNamespace(output=root, bridge_binary=root/'bridge',
                stop_budget=SimpleNamespace(cleanup=nullcontext))
            limits = for_owner(args, resources, max_query_rows=2)
            ready = dict(event='ready', protocol=PROTOCOL, max_pending_blocks=4, transport_limits=limits.describe())
            with patch.object(movie.verify_filter, 'FilterScope', return_value=scope), \
                    patch.object(movie, 'Popen', return_value=process) as launch, \
                    patch.object(movie.verify, '_capture_stderr', return_value=None), \
                    patch('integrations.imlane_records.client.os.set_blocking'), \
                    patch.object(RecordsBridge, '_read', return_value=ready):
                carrier = movie._open_filter(args, dict(model_environment={'CUDA_VISIBLE_DEVICES': 'CPU'}),
                    (method, engine, GenerationSpec(), resources, source), 'query', {})
            self.assertEqual(launch.call_args.args[0], [str(args.bridge_binary), '1234',
                str(movie.verify_filter.OWNED/'filter_udf.py'), 'infer_records', '4', limits.argv(), PROTOCOL])
            self.assertEqual(carrier.max_pending, 4)
            scope.track_root.assert_called_once_with(process)

    def test_public_configuration_entries_accept_the_declared_window(self):
        from src.experiments import lotus_records_chain as chain_entry, lotus_records_cascade as cascade_entry
        from src.experiments import movie_records_session as session
        from tests.experiments.test_movie_records_session import PlanTests

        resources = RecordsResources(prepare_batch_rows=17, model_batch_rows=17, max_active_batches=4)
        engine = EngineSpec('/tmp/bridge-main', '/tmp/model-python', '/tmp/driver-python', max_model_calls=16384)
        generation = GenerationSpec()
        method = FilterMethod('openai/fixture', 'Keep {text}')
        chain = LotusRecordChain((StageSpec('keep', method, generation),), query_id='query')
        config = dict(schema='semloom.records_chain_run.v1', backend=chain_entry.BACKENDS[0],
            chain=chain.describe(), engine=asdict(engine), resources=asdict(resources),
            source_sha256='0'*64, owner_sha256='0'*64)
        with patch.object(chain_entry, 'read_json', return_value=config):
            self.assertEqual(chain_entry.read_config('unused')[3].request_capacity(), 4)
        cascade = dict(schema='semloom.records_cascade_run.v1', backend=cascade_entry.BACKEND,
            query_id='query', method=asdict(method), helper_model_name='openai/helper',
            engines=dict(main=asdict(engine), helper=asdict(replace(engine, model_path='/tmp/bridge-helper'))),
            generations=dict(main=asdict(generation), helper=asdict(replace(generation, logprobs=1))),
            cascade_args={}, resources=asdict(resources), max_calls=32768,
            source_sha256='0'*64, owner_sha256='0'*64)
        with patch.object(cascade_entry, 'read_json', return_value=cascade):
            self.assertEqual(cascade_entry.read_config('unused')[3].request_capacity(), 4)
        fixture = PlanTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        plan = deepcopy(fixture.plan)
        plan.update(backend='imlane', resources=asdict(resources), profile=asdict(resources))
        plan['queries'] = [dict(plan['queries'][0], query_id='query-'+str(i)) for i in range(65)]
        plan['engine']['max_model_calls'] = 16384
        checked = session.check_plan(plan)
        self.assertEqual((checked['inference_calls'], len(checked['queries'])), (0, 65))

    def test_native_backend_accepts_a_finite_declaration_above_the_old_window(self):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=129)
        engine = EngineSpec('/tmp/bridge-model', '/tmp/model-python', '/tmp/driver-python')
        with tempfile.TemporaryDirectory() as directory, \
                patch('integrations.imlane_records.chain_backend.assigned_environment', return_value={}):
            backend = IMLaneChainBackend(engine=engine, resources=resources,
                author_args=SimpleNamespace(output=directory), model_environment={})
            self.assertEqual(backend.resources.request_capacity(), 129)
            self.assertFalse(backend._used)

    def test_carrier_uses_the_ready_capacity_without_a_second_window(self):
        for count in (1, 4, 129):
            with self.subTest(count=count):
                submit = Mock()
                client = SimpleNamespace(meta=dict(protocol=PROTOCOL, max_pending_blocks=count), submit=submit)
                carrier = RecordsResults(iter(range(count+1)), client, query_id='query',
                    mapping=SimpleNamespace(encode=list), stage_id='map', block_prefix='block',
                    control_dir='/tmp/cpu-bridge-capacity', deadline=time.monotonic()+60,
                    close_owner=lambda: None, block_rows=1)
                carrier._fill()
                self.assertIsNone(carrier.primary)
                self.assertEqual(len(carrier.pending), count)
                self.assertEqual(submit.call_count, count)
                carrier._fill()
                self.assertEqual(submit.call_count, count)
                carrier.pending.pop(next(iter(carrier.pending)))
                carrier._fill()
                self.assertEqual(submit.call_count, count+1)

    def test_ready_receipt_matches_the_requested_count_exactly(self):
        stream = SimpleNamespace(fileno=lambda: -1)
        process = SimpleNamespace(stdin=stream, stdout=stream, poll=lambda: 0)
        for requested, actual in ((1, 1), (4, 4), (129, 129), (1, True), (4, 2), (4, None)):
            abort = Mock()
            limits = for_owner(SimpleNamespace(), RecordsResources(max_active_batches=requested), max_query_rows=2)
            ready = dict(event='ready', protocol=PROTOCOL, max_pending_blocks=actual, transport_limits=limits.describe())
            with self.subTest(requested=requested, actual=actual), \
                    patch('integrations.imlane_records.client.os.set_blocking'), \
                    patch.object(RecordsBridge, '_read', return_value=ready):
                if type(actual) is int and actual == requested:
                    bridge = RecordsBridge(process, abort_owner=abort, max_pending=requested, protocol=PROTOCOL, limits=limits)
                    self.assertEqual(bridge.meta, ready)
                    abort.assert_not_called()
                else:
                    with self.assertRaisesRegex(RuntimeError, 'startup differs'):
                        RecordsBridge(process, abort_owner=abort, max_pending=requested, protocol=PROTOCOL, limits=limits)
                    abort.assert_called_once()


class BridgeLimitsCPPTests(unittest.TestCase):
    def test_actual_limits_and_decimal_parser_without_author_runtime(self):
        compiler = shutil.which('c++')
        if compiler is None:
            self.skipTest('C++17 compiler unavailable')
        source = Path(__file__).with_name('imlane_limits_checks.cpp')
        includes = Path(__file__).resolve().parents[2]/'integrations/imlane_records'
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary)/'limits-checks'
            subprocess.run([compiler, '-std=c++17', str(source), '-I'+str(includes),
                            '-o', str(binary)], check=True, capture_output=True, text=True, timeout=30)
            subprocess.run([str(binary)], check=True, capture_output=True, text=True, timeout=10)
