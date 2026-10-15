"""CPU-only input contracts and actual pinned LOTUS image HTTP fixture checks."""
import base64
from contextlib import contextmanager
from functools import wraps
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import importlib.metadata
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import subprocess
import sys
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest
from src.baselines.image.sembench_animals import (
    AnimalsDataset, MANIFEST_SCHEMA, SEMBENCH_COMMIT, classification_audit, image_identity,
)


def fresh_daft_runtime(test):
    """Daft's compute-thread count is process-global and can only be set once."""
    @wraps(test)
    def run(self):
        target='test_sembench_animals.'+'.'.join(self.id().split('.')[-2:])
        if os.environ.get('SEMLOOM_ANIMALS_NATIVE_CHILD') == target:
            return test(self)
        env=dict(os.environ,SEMLOOM_ANIMALS_NATIVE_CHILD=target)
        env['PYTHONPATH']=str(Path(__file__).parent)+os.pathsep+env.get('PYTHONPATH','')
        completed=subprocess.run([sys.executable,'-m','unittest',target],env=env,
                                 stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        self.assertEqual(completed.returncode,0,completed.stdout[-8000:])
    return run


class AnimalsInputsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        from PIL import Image
        self.rows = []
        for i, color in enumerate(('white','black','white','black')):
            path = self.root/f'{i}.png'
            Image.new('RGB',(16+i,12),color).save(path)
            self.rows.append(dict(row_id=f'animals-row-{i}', ImagePath=str(path),
                Species='ZEBRA' if i%2==0 else 'IMPALA', City='Nairobi',StationID='Station_A',
                image=image_identity(path)))
        self.value = dict(schema=MANIFEST_SCHEMA,sembench_commit=SEMBENCH_COMMIT,
                          role='software_fixture',rows=self.rows)
        self.path = self.root/'manifest.json'
        self.freeze()

    def freeze(self):
        self.value.pop('sha256',None)
        self.value['sha256'] = content_digest(self.value)
        self.path.write_text(json.dumps(self.value))
        self.dataset = AnimalsDataset(self.root,self.path,self.value['sha256'])

    def test_reference_labels_are_excluded_from_query_frame(self):
        if importlib.util.find_spec('pandas') is None:
            self.skipTest('pandas is not installed in this interpreter')
        self.assertNotIn('Species',self.dataset.frame().columns)
        self.assertIn('Species',self.dataset.frame(references=True).columns)

    def test_manifest_edit_fails_before_source_use(self):
        self.value['rows'][0]['Species']='IMPALA'
        self.path.write_text(json.dumps(self.value))
        with self.assertRaisesRegex(ValueError,'identity'):
            self.dataset.read()

    def test_encoded_or_pixel_changes_fail_closed(self):
        from PIL import Image
        Image.new('RGB',(16,12),'red').save(self.rows[0]['ImagePath'])
        with self.assertRaisesRegex(ValueError,'pixels'):
            self.dataset.read()

    def test_occurrence_reordering_is_rejected(self):
        self.rows.reverse()
        self.freeze()
        with self.assertRaisesRegex(ValueError,'occurrence'):
            self.dataset.read()

    def test_short_fixture_cannot_claim_official_sf64(self):
        self.value['role']='official_animals_sf64'
        self.freeze()
        with self.assertRaisesRegex(ValueError,'complete original catalog'):
            self.dataset.read()

    def test_count_cancellation_does_not_hide_row_errors(self):
        values=[(r['row_id'],i in (1,2)) for i,r in enumerate(self.rows)]
        self.assertEqual(classification_audit(self.rows,values),dict(
            true_positive=1,false_positive=1,true_negative=1,false_negative=1,evaluated_rows=4))

    def test_duplicate_missing_or_non_boolean_decisions_are_rejected(self):
        good=[(r['row_id'],True) for r in self.rows]
        for values in (good[:-1],good[::-1],[(r['row_id'],1) for r in self.rows]):
            with self.subTest(values=values),self.assertRaises(ValueError):
                classification_audit(self.rows,values)


@contextmanager
def image_http_fixture(*, gzip_response=False, delay_s=0):
    received=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):
            pass
        def do_POST(self):
            from PIL import Image
            value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            parts=[part for msg in value['messages'] if type(msg['content']) is list
                   for part in msg['content'] if part.get('type')=='image_url']
            if len(parts)!=1:
                self.send_error(400);return
            raw=base64.b64decode(parts[0]['image_url']['url'].split(',',1)[1],validate=True)
            with Image.open(io.BytesIO(raw)) as image:
                positive=image.convert('RGB').getpixel((0,0))==(255,255,255)
            received.append(value)
            time.sleep(delay_s)
            body=json.dumps(dict(id='fixture-response',object='chat.completion',created=1,
                model=value['model'],choices=[dict(index=0,finish_reason='stop',
                    message=dict(role='assistant',content='Answer: '+str(positive)))],
                usage=dict(prompt_tokens=10,completion_tokens=4,total_tokens=14))).encode()
            if gzip_response:
                import gzip
                body=gzip.compress(body)
            self.send_response(200);self.send_header('Content-Type','application/json')
            if gzip_response:
                self.send_header('Content-Encoding','gzip')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/v1/chat/completions',received
    finally:
        server.shutdown();server.server_close();thread.join(5)


@unittest.skipUnless(os.environ.get('SEMLOOM_TEST_SEMBENCH_ROOT'),
                     'actual pinned SemBench checkout must be explicitly selected')
class ActualAnimalsLibrariesTests(unittest.TestCase):
    """Real LOTUS/SDK/SQL evaluation; the endpoint is a pixel-reading CPU fixture."""
    setUp = AnimalsInputsTests.setUp
    freeze = AnimalsInputsTests.freeze

    @fresh_daft_runtime
    def test_daft_complete_image_requests_stay_within_four_http_slots(self):
        from PIL import Image
        from tokenizers import Tokenizer,models
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.sembench_animals import run_query
        for i in range(4, 16):
            path=self.root/f'{i}.png'
            Image.new('RGB',(16+i,12),'white' if i%2==0 else 'black').save(path)
            self.rows.append(dict(row_id=f'animals-row-{i}', ImagePath=str(path),
                Species='ZEBRA' if i%2==0 else 'IMPALA', City='Nairobi',StationID='Station_A',
                image=image_identity(path)))
        self.freeze()
        dataset=AnimalsDataset(Path(os.environ['SEMLOOM_TEST_SEMBENCH_ROOT']),self.path,self.value['sha256'])
        tokenizer=self.root/'capacity-tokenizer.json'
        Tokenizer(models.WordLevel({'[UNK]':0},unk_token='[UNK]')).save(str(tokenizer))
        with image_http_fixture(gzip_response=True,delay_s=.2) as (url,received):
            summary=run_query(dataset,FixedModelConfig(url,'fixture-vlm',10000),
                arm='daft',output=self.root/'capacity',tokenizer_path=tokenizer,
                concurrency=4,max_tokens=32,observation_mode='performance')
        self.assertEqual(len(received),16)
        self.assertEqual(summary['quality']['predicted'],8)
        self.assertTrue(summary['resource_gate']['passed'])
        self.assertLessEqual(summary['resource_gate']['observed_upstream_peak'],4)
        self.assertEqual(summary['native_options']['concurrency'],4)
        trace=[json.loads(line) for line in (self.root/'capacity/http-trace.jsonl').read_text().splitlines()]
        events=[]
        for row in trace:
            events.extend(((row['upstream_dispatch_started_monotonic_ns'],1),
                           (row['upstream_response_body_read_completed_monotonic_ns'],-1)))
        active=peak=0
        for _,change in sorted(events):
            active+=change
            peak=max(peak,active)
        self.assertEqual(active,0)
        self.assertLessEqual(peak,4)

    def test_original_query_keeps_pixels_messages_parser_count_and_evaluator(self):
        from tokenizers import Tokenizer,models
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.sembench_animals import run_query
        self.dataset=AnimalsDataset(Path(os.environ['SEMLOOM_TEST_SEMBENCH_ROOT']),
                                    self.path,self.value['sha256'])
        tokenizer=self.root/'tokenizer.json'
        Tokenizer(models.WordLevel({'[UNK]':0},unk_token='[UNK]')).save(str(tokenizer))
        arms=os.environ.get('SEMLOOM_TEST_ANIMALS_ARMS','lotus-native').split(',')
        for number,arm in enumerate(arms):
            with self.subTest(arm=arm),image_http_fixture(gzip_response=True) as (url,received):
                summary=run_query(self.dataset,FixedModelConfig(url,'fixture-vlm',10000),
                    arm=arm,output=self.root/('run-'+arm),tokenizer_path=tokenizer,
                    concurrency=4,max_tokens=32,observation_mode='performance',
                    ray_temp_root=Path(tempfile.gettempdir())/('sba-'+str(os.getpid())+'-'+str(number)))
                self.assertEqual(summary['status'],'completed')
                self.assertEqual(summary['model_calls'],4)
                self.assertEqual(summary['quality']['predicted'],2)
                self.assertEqual(summary['quality']['original_metric']['relative_error'],0)
                self.assertEqual(summary['classification_audit']['false_positive'],0)
                self.assertEqual(len(received),4)
                self.assertTrue(all('Species' not in json.dumps(v) for v in received))
                self.assertEqual(summary['warmup_model_calls'],0)

    def test_evaluation_failure_keeps_summary_when_version_metadata_is_missing(self):
        from tokenizers import Tokenizer,models
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.sembench_animals import run_query
        dataset=AnimalsDataset(Path(os.environ['SEMLOOM_TEST_SEMBENCH_ROOT']),self.path,self.value['sha256'])
        tokenizer=self.root/'failure-tokenizer.json'
        Tokenizer(models.WordLevel({'[UNK]':0},unk_token='[UNK]')).save(str(tokenizer))
        version=importlib.metadata.version
        def missing(name):
            if name=='duckdb':
                raise importlib.metadata.PackageNotFoundError(name)
            return version(name)
        output=self.root/'expected-failure'
        with image_http_fixture() as (url,_), \
             patch('src.experiments.sembench_animals.evaluate_original',side_effect=RuntimeError('evaluation fixture failure')), \
             patch('src.experiments.sembench_animals.importlib.metadata.version',side_effect=missing):
            with self.assertRaisesRegex(RuntimeError,'evaluation fixture failure'):
                run_query(dataset,FixedModelConfig(url,'fixture-vlm',10000),arm='lotus-native',
                    output=output,tokenizer_path=tokenizer,max_tokens=32,observation_mode='performance')
        summary=json.loads((output/'summary.json').read_text())
        self.assertEqual(summary['status'],'failed')
        self.assertEqual(summary['error_type'],'RuntimeError')
        self.assertEqual(summary['versions']['duckdb']['status'],'unavailable')
        self.assertEqual(summary['model_calls'],4)

    @fresh_daft_runtime
    def test_resource_failure_keeps_quality_results_and_close_report(self):
        from tokenizers import Tokenizer,models
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.sembench_animals import run_query
        dataset=AnimalsDataset(Path(os.environ['SEMLOOM_TEST_SEMBENCH_ROOT']),self.path,self.value['sha256'])
        tokenizer=self.root/'resource-tokenizer.json'
        Tokenizer(models.WordLevel({'[UNK]':0},unk_token='[UNK]')).save(str(tokenizer))
        output=self.root/'expected-resource-failure'
        with image_http_fixture() as (url,_), \
             patch('src.experiments.sembench_animals.observed_http_peak',return_value=5):
            with self.assertRaisesRegex(ValueError,'observed HTTP concurrency'):
                run_query(dataset,FixedModelConfig(url,'fixture-vlm',10000),arm='daft',
                    output=output,tokenizer_path=tokenizer,max_tokens=32,observation_mode='performance')
        summary=json.loads((output/'summary.json').read_text())
        self.assertEqual(summary['status'],'failed')
        self.assertFalse(summary['resource_gate']['passed'])
        self.assertEqual(summary['quality']['predicted'],2)
        self.assertEqual(summary['model_calls'],4)
        self.assertIn('executor_close_report',summary)
        self.assertEqual(json.loads((output/'original-result.json').read_text())['data'],[[2]])
        self.assertEqual(len(json.loads((output/'filter-decisions.json').read_text())),4)
        self.assertEqual(len((output/'http-trace.jsonl').read_text().splitlines()),4)


if __name__=='__main__':
    unittest.main()
