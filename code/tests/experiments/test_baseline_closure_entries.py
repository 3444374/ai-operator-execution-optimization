"""CPU integration boundaries; no model, native service or external call."""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments import native_query_lifecycle as lifecycle
from src.baselines.text.frameworks.kalypso_native import adapter
from tests.experiments import test_movie_records_session as session_fixture


class CombinedLifecycleTests(unittest.TestCase):
    def test_daft_model_reuse_and_ray_runtime_reuse_are_different_receipts(self):
        for backend, mode, model_reuse in [('ray-data','runtime_reuse_only',False),
                                           ('daft','native_single_thread_session',True)]:
            fixture=session_fixture.LifecycleTests();fixture.setUp()
            try:
                report=lifecycle.run_sequence(fixture.queries,fixture.factory,fixture.output,
                    backend=backend,lifecycle=mode,clock=fixture.clock)
                self.assertEqual(len(report['owners']),1)
                self.assertEqual(report['cross_query_model_reuse_requested'],model_reuse)
                self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[53]*3)
            finally:fixture.doCleanups()

    def test_new_daft_mode_is_not_silently_used_by_ray(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError,'requires daft'):
                lifecycle.run_sequence([dict(query_id='q',phase='measured')],None,Path(root)/'run',
                    backend='ray-data',lifecycle='native_single_thread_session')
            self.assertFalse((Path(root)/'run').exists())


class KalypsoClientLifecycleTests(unittest.TestCase):
    def test_attached_model_startup_is_external_and_not_measured_by_client(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);proof=root/'loaded-model.json'
            proof.write_text(json.dumps(dict(model='fixture-model',model_uid='external-model',
                hidden_size=3584,first_parameter_shape=[1,3584],
                first_parameter_device='cuda:0',dtype='torch.bfloat16')))
            with patch.object(adapter,'load_dependencies'):
                driver=adapter.NativeQueryDriver(dict(model='fixture-model',
                    loaded_model_proof=str(proof)),{},root/'client')
                ready=driver.start()
            timing=lifecycle._lifecycle_timing(dict(owners=[dict(owner_id='client',
                first_query_id='first',owner_start_ns=1,ready=ready)],queries=[],
                model_load_count_observed=None))
        self.assertEqual(ready['status'],'attached_ready')
        self.assertEqual(driver.model_uid,'external-model')
        self.assertEqual(timing['owners'][0]['model_initialization_scope'],
            'external GPU owner lifecycle; not measured by client attach')
        self.assertIsNone(timing['owners'][0]['startup_seconds'])

    def test_attached_native_driver_uses_neutral_lifecycle_and_restores_registry(self):
        events=[];before=lifecycle.BACKENDS,lifecycle.RESIDENT
        class Driver:
            def __init__(self,cfg,unit,path):pass
            def start(self):events.append('start');return {'status':'ready'}
            def query(self,spec,emit):
                events.append(spec['query_id']);emit('result_ready')
                return dict(status='completed',calls=dict(records=[]))
            def close(self):events.append('close');return dict(status='closed')
        with tempfile.TemporaryDirectory() as root,patch.object(adapter,'load_dependencies'),patch.object(adapter,'NativeQueryDriver',Driver):
            unit=dict(queries=[dict(query_id='first',phase='measured'),dict(query_id='reuse',phase='measured')])
            result=adapter.run_attached(dict(native_launch='author_defaults',model='fixture-model',port=18103),unit,Path(root)/'run')
        self.assertEqual(events,['start','first','reuse','close'])
        self.assertEqual(result['status'],'completed')
        self.assertEqual((lifecycle.BACKENDS,lifecycle.RESIDENT),before)
        self.assertEqual(len(result['queries']),2)
        self.assertTrue(all(q['complete_query_timing']['API_return_s'] is not None for q in result['queries']))

    def test_client_failure_preserves_unknown_and_restores_registry(self):
        before=lifecycle.BACKENDS,lifecycle.RESIDENT
        class Driver:
            def __init__(self,*args):pass
            def start(self):return {}
            def query(self,*args):raise TimeoutError('sent request has uncertain result')
            def close(self):return dict(status='closed')
        with tempfile.TemporaryDirectory() as root,patch.object(adapter,'load_dependencies'),patch.object(adapter,'NativeQueryDriver',Driver):
            with self.assertRaisesRegex(TimeoutError,'uncertain') as caught:
                adapter.run_attached(dict(native_launch='author_defaults',model='fixture-model',port=18103),dict(queries=[dict(query_id='first',phase='measured')]),Path(root)/'run')
        self.assertEqual((lifecycle.BACKENDS,lifecycle.RESIDENT),before)
        self.assertIsNone(caught.exception.session_report['queries'][0]['query_api_return_ns'])


class SourcePreparationTests(unittest.TestCase):
    def setUp(self):
        file=Path(__file__).resolve().parents[3]/'code/scripts/experiments/materialize_native_baseline.py'
        spec=importlib.util.spec_from_file_location('test_named_source_preparer',file)
        self.module=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.module)
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.repo=self.root/'repo';self.repo.mkdir();self.assets=self.root/'assets';self.assets.mkdir()
        (self.repo/'code').mkdir();(self.repo/'code/a.py').write_text('a=1\n')
        (self.assets/'b.py').write_text('b=2\n')
        self.manifest=dict(variants=dict(explicit=dict(original_source_revision='snapshot:fixture',code_projection_sha256='fixture',
            code_files_sha256={'code/a.py':self.module.digest(self.repo/'code/a.py'),'code/b.py':self.module.digest(self.assets/'b.py')},overrides={'code/b.py':'b.py'})))
        self.save()
    def save(self):(self.assets/'MANIFEST.json').write_text(json.dumps(self.manifest))
    def prepare(self,name='explicit'):return self.module.materialize(self.repo,self.assets,name,self.root/'output')
    def test_exact_files_materialize_without_running_their_contents(self):
        r=self.prepare();self.assertEqual(r['models_started'],0);self.assertEqual(r['files'],2)
        self.assertEqual((self.root/'output/code/b.py').read_text(),'b=2\n')
    def test_unknown_variant_is_rejected_before_output_creation(self):
        with self.assertRaises(ValueError):self.prepare('unknown')
        self.assertFalse((self.root/'output').exists())
    def test_tampered_common_file_fails_before_output_creation(self):
        (self.repo/'code/a.py').write_text('changed')
        with self.assertRaisesRegex(ValueError,'source differs'):self.prepare()
        self.assertFalse((self.root/'output').exists())
    def test_tampered_variant_file_is_not_copied(self):
        (self.assets/'b.py').write_text('changed')
        with self.assertRaises(ValueError):self.prepare()
    def test_path_escape_is_not_materialized(self):
        self.manifest['variants']['explicit']['code_files_sha256']['../escape']='invalid';self.save()
        with self.assertRaises(ValueError):self.prepare()
        self.assertFalse((self.root/'escape').exists())
    def test_existing_output_is_not_overwritten(self):
        self.prepare();(self.root/'output/retain').write_text('original')
        with self.assertRaises(FileExistsError):self.prepare()
        self.assertEqual((self.root/'output/retain').read_text(),'original')
