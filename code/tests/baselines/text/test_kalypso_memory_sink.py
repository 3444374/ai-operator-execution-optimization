"""Controlled-clock checks for the native HTTP relation, without network or models."""
from contextlib import ExitStack
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text import sembench_movie
from src.baselines.text.frameworks.kalypso_native import adapter
from src.baselines.text.movie_tasks import MovieTask
from tests.baselines.text.test_kalypso_native_observation import fake_events


class ControlledClock:
    def __init__(self):
        self.ns=0

    def __call__(self):
        return self.ns

    def advance(self, seconds):
        self.ns+=seconds*1_000_000_000


class KalypsoMemorySinkTests(unittest.TestCase):
    def setUp(self):
        from tests.experiments.test_movie_records_session import PlanTests
        helper=PlanTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        self.manifest=Path(helper.manifests['movie-q1'])
        self.root=self.manifest.parent.parent
        self.clock=ControlledClock()
        self.actions=[]
        self.cfg=dict(model='CPU-model',native_instruction='unchanged native instruction',
            native_trace=str(self.root/'trace.jsonl'),sembench='CPU-only',
            url='http://127.0.0.1:1',query_s=60)
        self.spec=dict(query_id='q',manifest=str(self.manifest),manifest_sha256=adapter.sha(self.manifest),
            task='movie-q1',expected_calls=2)
        self.body=json.dumps(dict(model_name='CPU-model',num_output_rows=1,results=[dict(idx=0)])).encode()
        self.events=fake_events(2,uid='CPU-model')
        self.directory=self.root/'memory-client/q'

    def emit(self, name):
        self.actions.append((name,self.clock()))
        return self.clock()

    def fixture(self, *, trace_error=None, save_error=None, transport_error=None, source_preflight_seconds=0):
        stack=ExitStack()
        self.addCleanup(stack.close)
        original_open=Path.open
        original_write_bytes=Path.write_bytes
        original_loads=json.loads
        original_frame=MovieTask.result_frame
        original_encode=MovieTask.encode_result
        original_reconcile=adapter.reconcile
        from src.baselines.text.movie_records import MovieRecordSource
        original_source=MovieRecordSource.from_manifest
        clock=self.clock

        def open_path(path, *args, **kwargs):
            if path.name=='native-input.csv' and args and args[0]=='x':
                clock.advance(2)
            return original_open(path,*args,**kwargs)

        def write_bytes(path, data):
            self.actions.append(('persist:'+path.name,clock()))
            clock.advance(31)
            return original_write_bytes(path,data)

        def loads(data, *args, **kwargs):
            if data is self.body:
                clock.advance(7)
            return original_loads(data,*args,**kwargs)

        def frame(task, *args):
            clock.advance(11)
            return original_frame(task,*args)

        def encode(task, value):
            self.actions.append(('encode',clock()))
            clock.advance(19)
            return original_encode(task,value)

        def trace(*args):
            self.actions.append(('read_trace',clock()))
            clock.advance(13)
            if trace_error is not None:
                raise trace_error
            return self.events

        def reconcile(*args):
            self.actions.append(('reconcile',clock()))
            clock.advance(17)
            return original_reconcile(*args)

        def evaluate(*args):
            self.actions.append(('evaluate',clock()))
            clock.advance(23)
            return dict(original_metric=dict(f1_score=1.0))

        def source(*args):
            clock.advance(source_preflight_seconds)
            return original_source(*args)

        def save(path, value):
            self.actions.append(('persist:'+path.name,clock()))
            clock.advance(29)
            if path.name=='request.json' and save_error is not None:
                raise save_error
            path.write_text(json.dumps(value))

        test=self
        class Response:
            status=200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                clock.advance(1)
            def read(self, limit):
                test.assertEqual(limit,32*1024**2+1)
                clock.advance(5)
                return test.body

        def request(req, timeout):
            self.actions.append(('POST',clock()))
            self.assertEqual(req.full_url,self.cfg['url']+'/v1/semantic/query')
            self.assertEqual(timeout,self.cfg['query_s'])
            payload=original_loads(req.data)
            self.assertEqual(payload['ops'],[dict(op='sem_filter',args=dict(prompt=self.cfg['native_instruction']))])
            self.assertFalse(payload['blocking'])
            self.assertTrue(Path(payload['data_path']).is_file())
            self.assertEqual({path.name for path in self.directory.iterdir()},{'native-input.csv'})
            clock.advance(3)
            if transport_error is not None:
                raise transport_error
            return Response()

        for context in (
            patch.object(adapter.time,'monotonic_ns',clock),
            patch.object(adapter,'load_dependencies',return_value=save),
            patch.object(adapter.request,'build_opener',return_value=SimpleNamespace(open=request)),
            patch.object(Path,'open',open_path),
            patch.object(Path,'write_bytes',write_bytes),
            patch.object(adapter.json,'loads',loads),
            patch.object(MovieTask,'result_frame',frame),
            patch.object(MovieTask,'encode_result',encode),
            patch.object(adapter,'read_trace',trace),
            patch.object(adapter,'reconcile',reconcile),
            patch.object(MovieRecordSource,'from_manifest',side_effect=source),
            patch.object(sembench_movie,'evaluate_original',evaluate),
        ):
            stack.enter_context(context)
        driver=adapter.NativeQueryDriver(self.cfg,dict(queries=[self.spec]),self.directory.parent)
        driver.model_uid='CPU-model'
        return driver

    def test_complete_relation_precedes_trace_encoding_quality_and_every_audit_write(self):
        driver=self.fixture()
        report=driver.query(self.spec,self.emit)
        names=[name for name,at in self.actions]
        sink=self.actions[names.index('memory_sink_ready')][1]
        self.assertEqual(sink,29_000_000_000)
        self.assertEqual(report['memory_sink_timing']['memory_sink_ready_ns'],sink)
        for name in ('read_trace','reconcile','encode','evaluate','persist:request.json',
                     'persist:occurrences.json','persist:response.body','persist:HTTP-clock.json'):
            self.assertLess(names.index('memory_sink_ready'),names.index(name),name)
        self.assertLess(names.index('encode'),names.index('result_ready'))
        self.assertLess(names.index('result_ready'),names.index('evaluate'))
        expected=dict(source_preflight=0,source_and_required_input=2,native_HTTP_and_JSON_parse=16,
            original_relation=11,trace_audit=30,result_encoding=19,quality_evaluation=23,
            audit_persistence=176)
        self.assertEqual({name:value['elapsed_s'] for name,value in report['memory_sink_timing']['phases'].items()},expected)
        http=json.loads((self.directory/'HTTP-clock.json').read_text())
        self.assertEqual(http['elapsed_s'],9)
        self.assertEqual(http['returned_ns'],11_000_000_000)
        self.assertEqual((self.directory/'response.body').read_bytes(),self.body)
        self.assertEqual(report['calls'],dict(expected=2,native_completed=2))
        self.assertEqual(report['quality']['row_audit']['true_positive'],1)
        self.assertFalse(driver.defer_http_evidence)

    def test_source_preflight_has_its_own_event_and_is_not_subtracted_afterwards(self):
        from src.observability.native_query_timing import query_timing

        report=self.fixture(source_preflight_seconds=17).query(self.spec,self.emit)
        receipt=report['memory_sink_timing']
        self.assertEqual(receipt['source_started_ns'],17_000_000_000)
        self.assertEqual(receipt['memory_sink_ready_ns'],46_000_000_000)
        timing=query_timing(dict(query_start_ns=0,backend_report=report))
        self.assertEqual(timing['source_to_sink_seconds'],29)
        self.assertEqual(timing['caller_to_memory_sink_seconds'],46)

    def test_bound_query_reader_scans_original_csv_without_hashing_or_cached_rows(self):
        from src.baselines.text import movie_records

        source=movie_records.MovieRecordSource.from_manifest(self.manifest)
        expected=source.rows()
        read=source.bind_query_rows()
        with patch.object(movie_records,'_sha256',side_effect=AssertionError('query scan reran preflight')):
            self.assertEqual(read(),expected)
        path=Path(json.loads(self.manifest.read_text())['original_csv'])
        path.write_text(path.read_text().replace(expected[0]['reviewText'],'changed query text'))
        with self.assertRaisesRegex(ValueError,'scan differs'):
            read()

    def test_trace_failure_preserves_sink_raw_HTTP_and_original_exception_with_recording_failure(self):
        primary=LookupError('original trace failure')
        driver=self.fixture(trace_error=primary,save_error=OSError('request evidence unavailable'))
        with self.assertRaises(LookupError) as caught:
            driver.query(self.spec,self.emit)
        self.assertIs(caught.exception,primary)
        self.assertEqual(primary.query_report['status'],'failed')
        timing=primary.query_report['memory_sink_timing']
        self.assertEqual(timing['memory_sink_ready_ns'],29_000_000_000)
        self.assertEqual(timing['trace_audit_status'],'failed')
        self.assertEqual(timing['phases']['audit_persistence']['status'],'failed')
        self.assertIn('OSError',primary.__notes__[0])
        self.assertTrue((self.directory/'occurrences.json').is_file())
        self.assertTrue((self.directory/'HTTP-clock.json').is_file())
        self.assertEqual((self.directory/'response.body').read_bytes(),self.body)
        self.assertFalse((self.directory/'quality.json').exists())
        self.assertNotIn('result_ready',[name for name,at in self.actions])
        persisted=json.loads((self.directory/'memory-sink-timing.json').read_text())
        self.assertEqual(persisted['memory_sink_ready_ns'],timing['memory_sink_ready_ns'])
        self.assertEqual(persisted['evidence_write_errors'],['OSError'])

    def test_HTTP_failure_has_no_sink_and_retains_request_clock_without_replay(self):
        primary=TimeoutError('native response outcome unknown')
        driver=self.fixture(transport_error=primary)
        with self.assertRaises(TimeoutError) as caught:
            driver.query(self.spec,self.emit)
        self.assertIs(caught.exception,primary)
        self.assertIsNone(primary.query_report['memory_sink_timing']['memory_sink_ready_ns'])
        self.assertEqual([name for name,at in self.actions].count('POST'),1)
        self.assertNotIn('memory_sink_ready',[name for name,at in self.actions])
        self.assertTrue((self.directory/'request.json').is_file())
        http=json.loads((self.directory/'HTTP-clock.json').read_text())
        self.assertEqual(http['status'],'failed_or_unknown')
        self.assertEqual(http['error_type'],'TimeoutError')
        self.assertFalse(http['automatically_replayed'])

    def test_invalid_JSON_has_no_sink_and_retains_the_received_body(self):
        self.body=b'not JSON'
        driver=self.fixture()
        with self.assertRaises(json.JSONDecodeError) as caught:
            driver.query(self.spec,self.emit)
        self.assertIsNone(caught.exception.query_report['memory_sink_timing']['memory_sink_ready_ns'])
        self.assertEqual((self.directory/'response.body').read_bytes(),self.body)
        self.assertNotIn('read_trace',[name for name,at in self.actions])

    def test_model_UID_audit_failure_cannot_qualify_an_observed_sink(self):
        self.events[-1]['model_uid']='other-model'
        driver=self.fixture()
        with self.assertRaisesRegex(ValueError,'model UID') as caught:
            driver.query(self.spec,self.emit)
        self.assertEqual(caught.exception.query_report['status'],'failed')
        self.assertEqual(caught.exception.query_report['memory_sink_timing']['trace_audit_status'],'failed')
        self.assertIn('memory_sink_ready',[name for name,at in self.actions])
        self.assertNotIn('result_ready',[name for name,at in self.actions])

    def test_post_preserves_evidence_contract_for_existing_private_query_subclasses(self):
        driver=self.fixture()
        self.directory.mkdir()
        csv_path=self.directory/'native-input.csv'
        csv_path.write_text('data\ngood\nbad\n')
        response=driver.post(dict(data_path=str(csv_path),blocking=False,
            ops=[dict(op='sem_filter',args=dict(prompt=self.cfg['native_instruction']))]),'q',self.directory)
        self.assertEqual(response['num_output_rows'],1)
        self.assertTrue((self.directory/'HTTP-clock.json').is_file())
        self.assertEqual((self.directory/'response.body').read_bytes(),self.body)

    def test_complete_HTTP_relation_preserves_filter_projection_count_ratio_and_repeated_business_IDs(self):
        rows=[dict(row_id='r'+str(i),source_position=i,reviewId='duplicate',id='taken_3') for i in range(8)]
        events=fake_events(8)
        for event in events:
            if event['event']=='filter_result':
                event.update(decision=True,verdict='yes')
        response=dict(model_name='m',num_output_rows=8,results=[dict(idx=i) for i in reversed(range(8))])
        restored=adapter.response_decisions(rows,response,'m')
        audited=adapter.reconcile(rows,response,events,'m')
        expected={1:dict(reviewId=['duplicate']*5),2:dict(reviewId=['duplicate']*5),
                  3:dict(positive_review_cnt=8),4:dict(positivity_ratio=1.)}
        for number in range(1,5):
            task=MovieTask(number)
            self.assertEqual(task.encode_result(task.result_frame(rows,restored)),expected[number])
            self.assertEqual(task.encode_result(task.result_frame(rows,restored)),
                             task.encode_result(task.result_frame(rows,audited)))
        empty=adapter.response_decisions(rows,dict(model_name='m',num_output_rows=0,results=[]),'m')
        self.assertFalse(any(row['decision'] for row in empty))

    def test_invalid_native_response_relation_cannot_reach_sink(self):
        rows=[dict(row_id='r0',source_position=0)]
        for indices in ([True],[-1],[1],[0,0]):
            with self.subTest(indices=indices),self.assertRaises(ValueError):
                adapter.response_decisions(rows,dict(model_name='m',num_output_rows=len(indices),
                    results=[dict(idx=index) for index in indices]),'m')

    def test_Map_texts_keep_original_outputs_and_order_for_required_parser(self):
        response=dict(model_name='m',num_output_rows=2,results=[
            dict(idx=1,outputs=[dict(op='SemMap',value='Answer: 2')]),
            dict(idx=0,outputs=[dict(op='SemMap',value='unparsed text')])])
        self.assertEqual(adapter.response_map_texts(response,'m',2),['unparsed text','Answer: 2'])
        for results in (response['results'][:1],
                [dict(idx=0,outputs=[]),response['results'][0]],
                [dict(idx=0,outputs=[dict(value=None)]),response['results'][0]]):
            with self.subTest(results=results),self.assertRaises(ValueError):
                adapter.response_map_texts(dict(response,num_output_rows=len(results),results=results),'m',2)


if __name__=='__main__':
    unittest.main()
