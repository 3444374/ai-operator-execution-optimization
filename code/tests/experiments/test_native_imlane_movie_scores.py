"""Q9 CLI refuses changed artifacts or a spent permanent unit before model startup."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import pandas as pd
from pandas.testing import assert_frame_equal
from src.baselines.text import sembench_movie_q9 as binding
from src.experiments import native_imlane_movie_scores as runner
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure.private_artifacts import content_digest, file_sha256, json_metric
from src.serving.backends.vllm_protocol import GenerationSpec

class Q9PreflightTests(unittest.TestCase):
    def test_spent_unit_is_not_retried_or_given_a_new_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            budget=AttemptBudget('q9-fixture',256)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',budget,deadline_utc=time.time()+30)
            ledger.reserve_unit('cold',256);unit=ledger.claim_shared_unit('cold')
            unit.reserve('0'*64)
            identity=root/'identity.json';identity.write_text(json.dumps(dict(Reviews_csv_sha256='input')))
            config=root/'config.json';config.write_text(json.dumps(dict(input_identity=str(identity),data_dir=str(root),
                build_record='build.json',
                request_budget=dict(path=str(ledger.path),budget_id=budget.budget_id,limit=256,unit_id='cold'))))
            factory=Mock()
            with patch.object(runner,'verified_build',return_value={'binary_path':'driver'}), patch.object(runner,'file_sha256',return_value='input'):
                with self.assertRaisesRegex(ValueError,'continuation/retry forbidden'):
                    runner.run(config,root/'out',service_factory=factory)
            factory.assert_not_called();self.assertEqual(unit.attempts,1)
            self.assertFalse((root/'out').exists())

    def test_original_dso_is_required_before_model_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            record=Path(tmp)/'build.json'
            record.write_text(json.dumps(dict(schema='semloom.imlane.movie_scores_build.v1',status='built',duckdb_library={'sha256':'wrong'})))
            with self.assertRaisesRegex(ValueError,'original qualified DOUBLE DSO'):
                runner.verified_build(record)

    def test_build_binds_current_driver_kernel_ack_and_artifact_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            binary=root/'q9';binary.write_bytes(b'fixture executable')
            library=root/'libduckdb.so';library.write_bytes(b'fixture original DSO')
            record=dict(schema='semloom.imlane.movie_scores_build.v1',status='built',
                binary_path=str(binary),binary=runner.file_identity(binary),
                duckdb_library_path=str(library),duckdb_library=runner.file_identity(library))
            for name,filename in [('driver_source','q9.cpp'),('kernel','q9_udf.py'),('relation_handoff','relation_handoff.hpp')]:
                record[name]=runner.file_identity(runner.KERNEL.parent/filename)
            path=root/'build.json';path.write_text(json.dumps(record))
            with patch.object(runner,'ORIGINAL_DSO_SHA256',record['duckdb_library']['sha256']):
                self.assertEqual(runner.verified_build(path),record)
                record['relation_handoff']['sha256']='0'*64;path.write_text(json.dumps(record))
                with self.assertRaisesRegex(ValueError,'relation_handoff.hpp'):
                    runner.verified_build(path)


def receipts(directory, rows, calls, texts, sampling):
    """Native-shaped saved responses, explicitly software data and no model."""
    messages = [json.loads(call['messages_json']) for call in calls]
    replies = [dict(request_id='response-' + str(i), generated_text=text,
        finish_reason='stop', requested_max_tokens=sampling['max_tokens'],
        resolved_max_tokens=sampling['max_tokens']) for i, text in enumerate(texts)]
    request = dict(batch_id='fixture', sampling_params=sampling, messages_batch=messages)
    response = dict(batch_id='fixture', calls=replies)
    batch = dict(status='succeeded', client_closed=True, worker_pid=123,
        started_monotonic_ns=2, finished_monotonic_ns=3, batch_id='fixture',
        request=request, response=response, request_sha256=content_digest(request),
        response_sha256=content_digest(response), engine_generate_invocations=len(rows),
        calls=[dict(position=i, attempted=True, status='succeeded', response=value,
                    response_id=value['request_id'], messages_sha256=content_digest(messages[i]))
               for i, value in enumerate(replies)])
    sources = [dict(occurrence_id=row['occurrence_id'], messages_sha256=content_digest(message))
               for row, message in zip(rows, messages, strict=True)]
    scores = [float(text) if text in ('1', '4.5', '2') else 3. for text in texts]
    predictions = [dict(source, generated_text=text, score=score)
        for source, text, score in zip(sources, texts, scores, strict=True)]
    udf = dict(status='succeeded', worker_pid=123, sources=sources, predictions=predictions,
        provider_calls=[dict(started_monotonic_ns=1, finished_monotonic_ns=4)])
    (directory / 'model-calls' / 'fixture.started.json').write_text('{}')
    (directory / 'model-calls' / 'fixture.terminal.json').write_text(json.dumps(batch))
    (directory / 'udf-receipts' / 'q9-batch-fixture.json').write_text(json.dumps(udf))
    return dict(return_type='DOUBLE', full_projection_materialized=True, source_rows=len(rows),
        mapped_occurrences=[dict(row, score=score) for row, score in zip(rows, scores, strict=True)][::-1],
        native_projection_materialized_seconds=0., result_ready_monotonic_ns=time.monotonic_ns())


CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')


@unittest.skipUnless(CHECKOUT, 'set SEMLOOM_NATIVE_TEST_CHECKOUT to the pinned source')
class Q9SourceSizeTests(unittest.TestCase):
    def setUp(self):
        import lotus
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.enterContext(lotus.settings.context(enable_cache=False))

    def source(self, count, *, limit=None):
        rows = [dict(id='ant_man_and_the_wasp_quantumania', reviewId='review-' + str(i // 2),
            reviewText='software review ' + str(i // 2), originalScore=str(i % 5 + 1) + '/5')
            for i in range(count)]
        unrelated = dict(id='other-movie', reviewId='excluded', reviewText='excluded', originalScore='1/5')
        pd.DataFrame([unrelated, *rows, unrelated]).to_csv(self.root / 'Reviews.csv', index=False)
        config = dict(sembench_checkout=CHECKOUT, data_dir=str(self.root), model_name='openai/fixture',
            sampling_params=GenerationSpec(max_tokens=32, stop=(), seed=0).kwargs(),
            run_id='q9-software', query_seconds=60, cleanup_seconds=10, model_service={},
            provider_mode='software_ipc', request_timeout_s=10,
            author_root=str(self.root), method_python='unused-software-only', build_record='unused.json')
        limit = count if limit is None else limit
        ledger = CellBudgetLedger.create(self.root / 'budget.sqlite', AttemptBudget('q9-size', limit),
                                         deadline_utc=time.time() + 60)
        ledger.reserve_unit('cold', limit)
        unit = ledger.claim_shared_unit('cold')
        config['request_budget'] = dict(path=str(ledger.path), budget_id='q9-size', limit=limit, unit_id='cold')
        identity = dict(Reviews_csv_sha256=file_sha256(self.root / 'Reviews.csv'), messages_sha256=[])
        if count:
            prepared = binding.prepare(config, self.root / 'reference', 'q9-software-cold')
            identity['messages_sha256'] = [content_digest(json.loads(call['messages_json'])) for call in prepared[2]]
        path = self.root / 'identity.json'
        path.write_text(json.dumps(identity))
        config['input_identity'] = str(path)
        path = self.root / 'config.json'
        path.write_text(json.dumps(config))
        return path, config, unit, rows

    def run_size(self, count):
        path, config, unit, source = self.source(count)
        events = []
        class Service:
            socket_path = self.root / 'unused.sock'
            served_model_name = 'fixture'
            def __enter__(current):
                ready = time.monotonic()
                current.report = dict(started_monotonic=ready - .01, ready_monotonic=ready)
                events.append('model-ready')
                return current
            def check(current): pass
            def close(current, **_kwargs): events.append('model-close')
        texts = [('1', '4.5', 'invalid', '6', '2')[i % 5] for i in range(count)]
        def execute(args, report, *, sink, audit, **_kwargs):
            prepared = json.loads((args.output / 'input/prepared.json').read_text())
            rows, calls = prepared['rows'], prepared['calls']
            for call in calls:
                unit.reserve(content_digest(json.loads(call['messages_json'])))
            native = receipts(args.output, rows, calls, texts, config['sampling_params'])
            result = sink(native)
            events.append('sink')
            ready = time.monotonic_ns()
            report.update(result_ready_monotonic_ns=ready, full_sink_observed_monotonic_ns=ready,
                          native_alive_after_sink=True, status='completed')
            (args.output / 'query.json').write_text(json.dumps(native))
            audited = audit(native, result)
            events.append('audit')
            return result, audited
        factory = Mock(return_value=Service())
        owner = dict(api_pid=123, api_process_created=1., ready_sha256='software', gpu_model_identity=[])
        with patch.object(runner, 'verified_build', return_value={'binary_path': 'unused-software-driver'}), \
                patch.object(runner, 'identity', return_value=owner), \
                patch.object(runner, 'run_relation_query', side_effect=execute):
            report = runner.run(path, self.root / 'out', service_factory=factory)
        self.assertEqual(events, ['model-ready', 'sink', 'audit', 'model-close'])
        self.assertEqual(report['model_calls'], count)
        self.assertEqual(unit.attempts, count)
        with self.assertRaises(BudgetExhausted):
            unit.reserve('0' * 64)
        self.assertEqual(unit.attempts, count)
        self.assertEqual(factory.call_args.args[0]['request_budget']['limit'], count)
        round_report, = report['rounds']
        self.assertEqual(round_report['ipc']['generate_API_calls'], count)
        self.assertGreaterEqual(round_report['complete_query_seconds'], round_report['model_ready_to_result_seconds'])
        expected = pd.DataFrame(dict(reviewId=[row['reviewId'] for row in source],
            reviewScore=[(1., 4.5, 3., 3., 2.)[i % 5] for i in range(count)]))
        query, operation, calls, rows = binding.prepare(config, self.root / 'verify', 'q9-software-cold')
        native = json.loads((self.root / 'out/cold/query.json').read_text())
        actual = runner.finish_movie_scores(native, query, rows)
        assert_frame_equal(actual, expected)
        self.assertEqual(round_report['quality'], json_metric(query.evaluate(expected)))
        checked, audited = binding.finish(query, operation, calls, rows, native, self.root / 'out/cold/udf-receipts')
        assert_frame_equal(checked, expected)
        self.assertEqual(audited['unique_occurrences'], count)
        self.assertEqual(audited['unique_reviewIds'], len({row['reviewId'] for row in source}))
        self.assertEqual([row['reviewText'] for row in rows], [row['reviewText'] for row in source])
        self.assertEqual([row['source_position'] for row in rows], list(range(count)))
        self.assertEqual([content_digest(json.loads(call['messages_json'])) for call in calls],
                         json.loads(Path(config['input_identity']).read_text())['messages_sha256'])
        native['mapped_occurrences'].pop()
        with self.assertRaisesRegex(ValueError, 'occurrence coverage'):
            runner.finish_movie_scores(native, query, rows)
        return unit

    def test_one_original_occurrence(self): self.run_size(1)

    def test_seven_occurrences_keep_duplicate_business_ids_and_original_sink(self):
        self.run_size(7)
        directory = self.root / 'out/cold'
        rows = json.loads((directory / 'input/prepared.json').read_text())['rows']
        path = directory / 'model-calls/fixture.terminal.json'
        saved = path.read_text()
        batch = json.loads(saved)
        batch['response']['calls'][0]['resolved_max_tokens'] = 16
        batch['calls'][0]['response']['resolved_max_tokens'] = 16
        batch['response_sha256'] = content_digest(batch['response'])
        path.write_text(json.dumps(batch))
        with self.assertRaises(AssertionError): runner.audit_ipc(directory, rows)
        batch = json.loads(saved)
        duplicate = batch['calls'][0]['response_id']
        batch['calls'][-1]['response_id'] = duplicate
        batch['calls'][-1]['response']['request_id'] = duplicate
        batch['response']['calls'][-1]['request_id'] = duplicate
        batch['response_sha256'] = content_digest(batch['response'])
        path.write_text(json.dumps(batch))
        with self.assertRaises(AssertionError): runner.audit_ipc(directory, rows)

    def test_seventeen_occurrences_use_the_actual_source_budget(self): self.run_size(17)

    def test_budget_mismatch_fails_before_model_start(self):
        path, _config, unit, _rows = self.source(7, limit=8)
        factory = Mock()
        with patch.object(runner, 'verified_build', return_value={'binary_path': 'unused'}):
            with self.assertRaisesRegex(ValueError, 'per prepared source occurrence'):
                runner.run(path, self.root / 'out', service_factory=factory)
        factory.assert_not_called()
        self.assertEqual(unit.attempts, 0)
        self.assertEqual(json.loads((self.root / 'out/worker.json').read_text())['status'], 'failed')

    def test_insufficient_budget_does_not_start_a_partial_query(self):
        path, _config, unit, _rows = self.source(7, limit=6)
        factory = Mock()
        with patch.object(runner, 'verified_build', return_value={'binary_path': 'unused'}):
            with self.assertRaisesRegex(ValueError, 'per prepared source occurrence'):
                runner.run(path, self.root / 'out', service_factory=factory)
        factory.assert_not_called()
        self.assertEqual(unit.attempts, 0)

    def test_empty_original_selection_fails_before_model_start(self):
        path, _config, unit, _rows = self.source(0, limit=1)
        factory = Mock()
        with patch.object(runner, 'verified_build', return_value={'binary_path': 'unused'}):
            with self.assertRaisesRegex(ValueError, 'nonempty original selection'):
                runner.run(path, self.root / 'out', service_factory=factory)
        factory.assert_not_called()
        self.assertEqual(unit.attempts, 0)
