"""Real LOTUS accounting; no model, Ray or network execution."""
import csv,hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from src.baselines.text.movie_records import MovieRecordSource,prepare_movie_source
from src.experiments import lotus_records_movie as movie
from src.semantic_methods.lotus.records import CallLedger,identity
from src.serving.backends.vllm_engine import GenerationSpec


class Results:
    def __init__(self,values,receipts=None,error=None):
        self.values=iter(values);self.closes=0;self.error=error
        self.last_close_report=dict(status='closed',unconsumed_call_results=[] if receipts is None else receipts)
    def __iter__(self):return self
    def __next__(self):return next(self.values)
    def close(self):
        self.closes+=1
        if self.error:raise self.error


class CompletedQueryAccountingTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        root=Path(temporary.name);path=root/'Reviews.csv'
        with path.open('w',newline='') as stream:
            writer=csv.writer(stream);writer.writerow(['reviewId','id','reviewText','scoreSentiment'])
            writer.writerows([[str(i),'taken_3','good '+str(i),'POSITIVE'] for i in range(4)])
        manifest=prepare_movie_source(path,root/'prepared',expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),max_rows=4,task='movie-q3')
        self.source=MovieRecordSource.from_manifest(manifest)
        self.expected=self.source.expected('cpu-accounting')
        self.rows=[dict(row,status='succeeded',submission_confirmed=True,text='True',finish_reason='stop',
            prompt_tokens=11,completion_tokens=4,total_tokens=15,engine_request_id='cpu-'+str(i),engine_uid='cpu')
            for i,row in enumerate(self.expected)]
        self.checkout=Path(movie.__file__).resolve().parents[3].parent/'packet/sembench'

    def query(self,result,observer=None):
        return movie.run_movie_query(self.source,self.checkout,self.source.task.method('openai/cpu-fixture'),
            GenerationSpec(),lambda:result,query_id='cpu-accounting',observer=observer)

    def test_complete_empty_close_does_not_record_already_accounted_calls_again(self):
        ready=[False];recorded=[];record=CallLedger.record
        def capture(ledger,row):
            recorded.append((row['call_id'],ready[0]));return record(ledger,row)
        def event(name):
            if name=='result_ready':ready[0]=True
        result=Results(self.rows)
        with patch.object(CallLedger,'record',capture):report=self.query(result,event)
        self.assertEqual(sum(after for _,after in recorded),0)
        self.assertEqual(len(recorded),4);self.assertEqual(result.closes,1)
        self.assertEqual(report['calls']['counts']['succeeded'],4)
        self.assertEqual(report['calls']['usage']['total_tokens'],60)

    def test_bad_receipt_container_still_fails_after_preserving_known_usage(self):
        result=Results(self.rows);result.last_close_report['unconsumed_call_results']=None
        with self.assertRaises(ValueError) as caught:self.query(result)
        self.assertEqual(caught.exception.query_report['calls']['usage']['total_tokens'],60)

    def test_close_failure_keeps_original_error_and_known_calls(self):
        error=RuntimeError('CPU close failed');result=Results(self.rows,error=error)
        with self.assertRaises(RuntimeError) as caught:self.query(result)
        self.assertIs(caught.exception,error)
        self.assertEqual(error.query_report['calls']['counts']['succeeded'],4)
        self.assertEqual(error.query_report['calls']['usage']['total_tokens'],60)

    def test_nonempty_conflicting_or_foreign_close_receipt_is_not_skipped(self):
        for row in (dict(self.rows[0],text='False'),dict(self.rows[0],**identity('foreign','r',0))):
            with self.subTest(call=row['call_id']),self.assertRaises(ValueError) as caught:
                self.query(Results(self.rows,receipts=[row]))
            self.assertEqual(caught.exception.query_report['calls']['counts']['succeeded'],4)

    def test_missing_calls_and_late_known_result_still_fill_remaining_unknown(self):
        with self.assertRaises(ValueError) as caught:self.query(Results(self.rows[:1],receipts=self.rows[1:2]))
        report=caught.exception.query_report
        self.assertEqual((report['calls']['counts']['succeeded'],report['calls']['counts']['unknown']),(2,2))
        self.assertEqual(report['calls']['usage']['known_prompt_tokens'],22)

    def test_standalone_merge_still_validates_an_external_snapshot(self):
        ledger=CallLedger(self.expected,max_calls=4)
        for row in self.rows:ledger.record(row)
        report=dict(calls=ledger.report(),status='completed')
        report['calls']['records'][0]['row_id']='wrong'
        with self.assertRaises(ValueError):movie.merge_unconsumed_call_results(report,self.expected,dict(unconsumed_call_results=[]))
