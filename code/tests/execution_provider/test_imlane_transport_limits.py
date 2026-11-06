"""Finite IMLane declarations and real method bookkeeping; no native/model owner."""

from dataclasses import asdict, replace
import csv
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import lotus
import pyarrow as pa

from integrations.imlane_records.chain_workload import ChainBatches, batch_rows
from integrations.imlane_records.client import RecordsBridge
from integrations.imlane_records.echo_udf import INPUT, OUTPUT
from integrations.imlane_records.filter_workload import Occurrences, preflight_transport
from integrations.imlane_records.filter_udf import FilterWorker
from integrations.imlane_records.resident import IMLaneResidentSession, control_block
from integrations.imlane_records.resident_udf import ResidentWorker
from integrations.imlane_records.transport_limits import (
    ARROW_IPC_OVERHEAD_BYTES, LANE_BYTES, TransportLimits, close_block, for_owner, preflight_blocks,
)
from src.baselines.text.cascade_records_source import MaterializedCascadeSource
from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.semantic_methods.lotus.record_types import RecordOccurrence
from src.semantic_methods.lotus.cascade import CascadeStage, cascade_identity
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, _prepared


class SoftwareEngine:
    """Deterministic transport fixture, with no child, tokenizer or model."""

    def __init__(self, *args, **kwargs):
        self.ready_report = dict(engine_uid='CPU-software-transport')
        self.batches = []

    def run_prepared(self, prepared):
        identities, _ = _prepared(prepared)
        self.batches.append(len(identities))
        return [dict(item, engine_uid=self.ready_report['engine_uid'], status='succeeded',
            submission_confirmed=True, text='True', prompt_tokens=2, completion_tokens=1, total_tokens=3,
            prompt_token_ids_json='[1,2]', output_token_ids_json='[3]',
            engine_request_id='fixture/' + item['call_id']) for item in identities]

    def run_query(self, prepared, **kwargs):
        return self.run_prepared(prepared)


class TransportBoundsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for boundary in (patch.object(subprocess, 'Popen', side_effect=AssertionError('process start')),
                patch.object(socket.socket, 'connect', side_effect=AssertionError('network connection'))):
            boundary.start()
            self.addCleanup(boundary.stop)
        self.resources = RecordsResources(prepare_batch_rows=17, model_batch_rows=17, max_active_batches=4)
        self.args = SimpleNamespace(output=self.root)

    def limits(self, **values):
        return replace(for_owner(self.args, self.resources, max_query_rows=4098), **values)

    def wire(self, count=1):
        mapping = Occurrences('query', max_calls=count, columns=('text',))
        return mapping.encode([dict(identity('query', 'row-' + str(i), i), text='雪\n\0') for i in range(count)])

    def test_input_output_byte_accounting_matches_actual_flat_arrow_buffers(self):
        limits = self.limits()
        for count in (1, 4, 17):
            wire = self.wire(count)
            table = pa.Table.from_pylist(wire, schema=INPUT)
            exact = sum(buffer.size for column in table.columns for chunk in column.chunks
                        for buffer in chunk.buffers() if buffer is not None)
            self.assertEqual(limits.check_input(wire), exact)
            replace(limits, max_input_bytes=exact).check_input(wire)
            wire[-1]['record_json'] += 'x'
            with self.assertRaisesRegex(ValueError, 'Arrow byte'):
                replace(limits, max_input_bytes=exact).check_input(wire)
            result = [dict(call_id=row['call_id'], row_id=row['row_id'], outcome_json='雪\n\0') for row in wire]
            output = pa.Table.from_pylist(result, schema=OUTPUT)
            exact = sum(buffer.size for column in output.columns for chunk in column.chunks
                        for buffer in chunk.buffers() if buffer is not None)
            self.assertEqual(limits.check_output(result), exact)
            replace(limits, max_output_bytes=exact).check_output(result)
            result[-1]['outcome_json'] += 'x'
            with self.assertRaisesRegex(ValueError, 'Arrow byte'):
                replace(limits, max_output_bytes=exact).check_output(result)

    def test_no_model_echo_accepts_the_declared_17_row_cpu_block(self):
        from integrations.imlane_records.echo_udf import infer_records

        rows = [dict(call_id='echo-'+str(i), row_id=i, query_id='query', stage_id='filter',
            record_json=json.dumps({'text':'source '+str(i)})) for i in range(17)]
        self.limits().check_input(rows)
        output = infer_records(pa.Table.from_pylist(rows, schema=INPUT)).to_pylist()
        self.assertEqual(len(output), 17)
        for source, result in zip(rows, output, strict=True):
            envelope = json.loads(result['outcome_json'])
            self.assertFalse(envelope['model_entry_attempted'])
            self.assertEqual(envelope['result']['record'], json.loads(source['record_json']))
            self.assertEqual(result['call_id'], source['call_id'])

    def test_lane_shm_rss_and_command_exact_boundaries(self):
        limits = self.limits(max_input_bytes=LANE_BYTES - 2*ARROW_IPC_OVERHEAD_BYTES - 1048576)
        with self.assertRaisesRegex(ValueError, 'Lane'):
            replace(limits, max_input_bytes=limits.max_input_bytes+1)
        with self.assertRaisesRegex(ValueError, 'SHM'):
            replace(limits, owner_shm_bytes=LANE_BYTES-1)
        limits = self.limits()
        exact = (limits.max_pending_blocks+1)*(limits.max_input_bytes+limits.max_output_bytes)
        exact += limits.max_command_bytes+limits.max_response_bytes
        replace(limits, owner_rss_bytes=exact)
        with self.assertRaisesRegex(ValueError, 'RSS'):
            replace(limits, owner_rss_bytes=exact-1)
        wire = self.wire()
        exact = len(limits.check_submit('block', wire))
        replace(limits, max_command_bytes=exact).check_submit('block', wire)
        with self.assertRaisesRegex(ValueError, 'wire allowance'):
            replace(limits, max_command_bytes=exact-1).check_submit('block', wire)

    def test_movie_source_4097_and_filter_batches_4_17_keep_exact_values(self):
        path = self.root/'movie.csv'
        with path.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['reviewId', 'id', 'reviewText', 'scoreSentiment'])
            writer.writerows((str(i), 'taken_3', 'text ' + str(i), 'POSITIVE') for i in range(4097))
        manifest = prepare_movie_source(path, self.root/'movie', max_rows=4097, task='movie-q1',
            expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        source = MovieRecordSource.from_manifest(manifest)
        method = source.task.method('openai/CPU-fixture')
        self.assertEqual(source.preflight(method)['logical_limits']['max_rows'], 4097)
        for batch in (4, 17):
            resources = replace(self.resources, prepare_batch_rows=batch, model_batch_rows=batch)
            limits = for_owner(self.args, resources, max_query_rows=4098)
            self.assertEqual(preflight_transport(source, method, resources, 'query', limits), 4097)
        self.assertEqual(source.rows()[-1]['reviewText'], 'text 4096')
        self.assertEqual(len(source.expected('query')), 4097)

    def test_packed_chain_4097_logical_rows_have_241_wire_rows_at_batch_17(self):
        rows = tuple(RecordOccurrence('row-'+str(i), i, i, dict(text='original')) for i in range(4097))
        method = FilterMethod('openai/CPU-fixture', 'Keep {text}', resolved_input_columns=('text',))
        source = MaterializedCascadeSource(self.root, rows, max_rows=len(rows), payload_columns=('text',), method=method)
        self.assertEqual(source.preflight(method)['logical_limits']['max_rows'], 4097)
        limits = for_owner(self.args, self.resources, packed=True, max_query_rows=242)
        mapping = ChainBatches('query', rows, max_batch_rows=17)
        self.assertEqual(preflight_blocks(limits, rows, block_rows=17, block_prefix='chain', encode=mapping.encode), 241)
        self.assertEqual(len(mapping.inputs), 241)
        first = next(iter(mapping.inputs.values()))
        self.assertEqual(len(batch_rows(first, max_rows=17)), 17)
        with self.assertRaisesRegex(ValueError, 'logical row'):
            batch_rows(first, max_rows=16)
        limits.check_submit(*close_block('query', 'finite-chain', 'chain'))
        self.assertEqual(limits.max_block_rows, 1)

    def test_chain_phase_derives_wire_count_before_the_author_boundary(self):
        from integrations.imlane_records.chain_backend import IMLaneChainBackend
        from tests.semantic_methods.test_lotus_chain_composition import chain_for

        rows = tuple(RecordOccurrence('row-'+str(i), i, i, dict(text='original')) for i in range(4097))
        engine = EngineSpec(str(self.root/'model'), sys.executable, sys.executable, max_model_calls=4097)
        self.args.stop_budget = SimpleNamespace(stopped=threading.Event())
        backend = IMLaneChainBackend(engine=engine, resources=self.resources, author_args=self.args,
            model_environment={'CUDA_VISIBLE_DEVICES':'0'})
        with patch.object(EngineSpec, 'validate_driver_environment'), \
                patch.object(backend, '_open_phase', side_effect=RuntimeError('CPU author boundary')) as allocate:
            stream = backend.evaluate(iter(rows), chain_for('M', column='text'), expected=rows)
            with self.assertRaisesRegex(RuntimeError, 'CPU author boundary'):
                next(stream)
        config = allocate.call_args.args[1]
        limits = TransportLimits.from_dict(config['transport_limits'])
        self.assertEqual((limits.max_block_rows, limits.max_query_rows, limits.max_queries), (1, 242, 1))
        self.assertEqual(config['engine']['max_model_calls'], 4097)

    def test_cascade_stage_derives_4097_calls_plus_close_before_the_author_boundary(self):
        from integrations.imlane_records.cascade_host import IMLaneCascadeBackend

        records = tuple(RecordOccurrence('row-'+str(i), i, i, dict(text='original')) for i in range(4097))
        method = FilterMethod('openai/CPU-fixture', 'Keep {text}', resolved_input_columns=('text',))
        source = MaterializedCascadeSource(self.root, records, max_rows=4097, method=method, payload_columns=('text',))
        engine = EngineSpec(str(self.root/'model'), sys.executable, sys.executable, max_model_calls=4097)
        self.args.stop_budget = SimpleNamespace(check=lambda:None)
        backend = IMLaneCascadeBackend(source, engines=dict(main=engine, helper=replace(engine, model_path=str(self.root/'helper'))),
            resources=self.resources, author_args=self.args, model_environment={'CUDA_VISIBLE_DEVICES':'0'})
        effective = dict(max_completion_tokens=8, temperature=0, top_p=1, logprobs=False)
        rows = tuple(dict(cascade_identity('query', row.row_id, row.source_position, 0,
            model_role='main', stage='fallback'), text=row.values['text']) for row in records)
        stage = CascadeStage('fallback','main',method.model_name,method.instruction,method.default,
            method.system_prompt,('True','False'),effective,rows,('text',))
        # Model/tokenizer deployment qualification is a separate external boundary.
        backend._checked, backend._remaining_calls = True, 4097
        backend._methods, backend._instructions = {'main':method}, {'main':method.instruction}
        backend._effective, backend.scan = {'main':effective}, source.scan_spec(method)
        with patch.object(backend, '_open_stage', side_effect=RuntimeError('CPU author boundary')) as allocate, \
                self.assertRaisesRegex(RuntimeError, 'CPU author boundary'):
            backend(stage)
        config = allocate.call_args.args[1]
        limits = TransportLimits.from_dict(config['transport_limits'])
        self.assertEqual((limits.max_block_rows, limits.max_query_rows, limits.max_queries), (17, 4098, 1))
        self.assertEqual(config['engine']['max_model_calls'], 4097)

    def test_oversized_source_rejects_before_author_scope_construction(self):
        from src.experiments import imlane_movie_formal as movie
        method = FilterMethod('openai/CPU-fixture', 'Keep {text}')
        scan = RecordScan((('row_id','string'), ('source_position','int64'), ('text','string')), ('text',))
        source = SimpleNamespace(expected=lambda query: [identity(query, 'row', 0)], scan_spec=lambda _: scan,
            rows=lambda: [dict(row_id='row', source_position=0, text='x'*65536)])
        with patch.object(movie.verify_filter, 'FilterScope') as allocate, self.assertRaisesRegex(ValueError, 'Arrow byte'):
            movie._open_filter(self.args, {}, (method, None, None, self.resources, source), 'query', {})
        allocate.assert_not_called()

    def test_response_frame_exact_boundary_and_complete_ready_binding(self):
        limits = self.limits()
        payload = b'IMLANE_RECORDS {"event":"poll","ok":true,"ready":[]}\n'
        client = RecordsBridge.__new__(RecordsBridge)
        client.limits, client.log = replace(limits, max_response_bytes=len(payload)), Mock()
        client._buffer = bytearray(payload)
        self.assertEqual(client._read(time.monotonic()+1)['event'], 'poll')
        client._buffer = bytearray(payload[:-1]+b' '+b'\n')
        with self.assertRaisesRegex(RuntimeError, 'declared allowance'):
            client._read(time.monotonic()+1)
        process = SimpleNamespace(stdin=SimpleNamespace(fileno=lambda:-1), stdout=SimpleNamespace(fileno=lambda:-1),
            poll=lambda:0)
        for advertised in (None, dict(limits.describe(), max_query_rows=4099)):
            abort = Mock()
            ready = dict(event='ready', protocol='imlane.records.v2', max_pending_blocks=4, transport_limits=advertised)
            with patch('integrations.imlane_records.client.os.set_blocking'), \
                    patch.object(RecordsBridge, '_read', return_value=ready), self.assertRaisesRegex(RuntimeError, 'startup differs'):
                RecordsBridge(process, abort_owner=abort, max_pending=4, limits=limits, protocol='imlane.records.v2')
            abort.assert_called_once()

    def test_transport_failure_preserves_primary_and_cleanup_evidence_without_reuse(self):
        limits = self.limits()
        process = SimpleNamespace(stdin=SimpleNamespace(fileno=lambda:-1), stdout=SimpleNamespace(fileno=lambda:-1),
            poll=lambda:0)
        ready = dict(event='ready', protocol='imlane.records.v2', max_pending_blocks=4, transport_limits=limits.describe())
        abort = Mock(side_effect=RuntimeError('unconfirmed owner cleanup'))
        with patch('integrations.imlane_records.client.os.set_blocking'), \
                patch.object(RecordsBridge, '_read', side_effect=[ready, TimeoutError('native timeout')]), \
                patch.object(RecordsBridge, '_wait'), \
                patch('integrations.imlane_records.client.os.write', side_effect=lambda _fd, wire:len(wire)):
            client = RecordsBridge(process, abort_owner=abort, max_pending=4, limits=limits, protocol='imlane.records.v2')
            with self.assertRaisesRegex(TimeoutError, 'native timeout') as failure:
                client.begin('query')
            self.assertIn('Owner cleanup remains unconfirmed: RuntimeError', failure.exception.__notes__)
            with self.assertRaisesRegex(RuntimeError, 'terminal'):
                client.begin('another-query')
            abort.assert_called_once()

    def test_resident_65_queries_4161_calls_share_one_software_transport_without_refunds(self):
        calls = {'query-'+str(i): 4097 if i == 0 else 1 for i in range(65)}
        engine = EngineSpec(str(self.root/'model'), sys.executable, sys.executable, max_model_calls=sum(calls.values()))
        generation = GenerationSpec(max_tokens=8)
        session = IMLaneResidentSession(engine, generation, self.resources, author_args=self.args,
            model_environment={'CUDA_VISIBLE_DEVICES':'0'}, deadline_monotonic=time.monotonic()+120, query_calls=calls)
        self.assertEqual((session.limits.max_query_rows, session.limits.max_queries), (4099, 67))
        for operation, query in (('session_start', session.session_id+':startup'), ('session_close', session.session_id+':close')):
            session.limits.check_submit(*control_block(operation, query))
        config = dict(engine=asdict(engine), generation=asdict(generation), resources=asdict(self.resources),
            query_id=session.session_id, control_dir=str(self.root), deadline_monotonic=time.monotonic()+120,
            model_environment={'CUDA_VISIBLE_DEVICES':'0'}, query_calls=calls, transport_limits=session.limits.describe())
        worker = ResidentWorker(config, engine_type=SoftwareEngine)
        worker.start()
        method = FilterMethod('openai/CPU-fixture', 'Keep {text}', resolved_input_columns=('text',))
        scan = RecordScan((('row_id','string'), ('source_position','int64'), ('text','string')), ('text',))
        for query, count in calls.items():
            control = self.root/query
            control.mkdir()
            query_config = dict(config, method=asdict(method), scan=scan.describe(), query_id=query,
                control_dir=str(control), engine=asdict(replace(engine, max_model_calls=count)))
            worker.begin(query_config)
            with self.assertRaises(ValueError):
                worker.begin(query_config)  # Overlap remains rejected.
            mapping = Occurrences(query, max_calls=count, columns=('text',))
            for start in range(0, count, 17):
                wire = mapping.encode([dict(identity(query, 'row-'+str(i), i), text='source '+str(i))
                    for i in range(start, min(count,start+17))])
                session.limits.check_input(wire)
                self.assertTrue(all(result['status'] == 'ok' for result in worker.run(wire)))
            worker.end(query)
            with self.assertRaises(ValueError):
                worker.begin(query_config)  # Completed calls cannot be replayed or refunded.
        self.assertEqual((worker.initializations, worker.ready_count), (1, 1))
        self.assertEqual((len(worker.finished), worker.reserved_calls), (65, 4161))
        self.assertEqual(sum(worker.model.batches), 4161)
        self.assertEqual(max(worker.model.batches), 17)

    def test_actual_filter_arrow_udf_uses_declared_17_rows_and_rejects_row_18_before_worker(self):
        from integrations.imlane_records import filter_udf

        method = FilterMethod('openai/CPU-fixture', 'Keep {text}', resolved_input_columns=('text',))
        scan = RecordScan((('row_id','string'), ('source_position','int64'), ('text','string')), ('text',))
        config = dict(method=asdict(method), resources=asdict(self.resources),
            engine=asdict(EngineSpec(str(self.root/'model'), sys.executable, sys.executable, max_model_calls=4097)),
            generation=asdict(GenerationSpec(max_tokens=8)), query_id='query', control_dir=str(self.root),
            deadline_monotonic=time.monotonic()+30, model_environment={}, scan=scan.describe(),
            transport_limits=self.limits().describe())
        path = self.root/'worker.json'
        path.write_text(json.dumps(config))
        worker = FilterWorker(config, engine_type=SoftwareEngine)
        with patch.dict('os.environ', IMLANE_FILTER_CONFIG=str(path)), \
                patch.object(filter_udf, 'owned_worker', return_value=worker) as allocate:
            output = filter_udf.infer_records(pa.Table.from_pylist(self.wire(17), schema=INPUT))
            self.assertEqual(len(output), 17)
            self.assertTrue(all(json.loads(row['outcome_json'])['status'] == 'ok' for row in output.to_pylist()))
            self.assertEqual(worker.kernel.batches, [17])
            allocate.reset_mock()
            with self.assertRaisesRegex(ValueError, 'wire row allowance'):
                filter_udf.infer_records(pa.Table.from_pylist(self.wire(18), schema=INPUT))
            allocate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
