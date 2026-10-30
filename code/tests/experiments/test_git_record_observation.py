"""Git gates, private on/off preparation and exact original-file recovery; no model."""
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.infrastructure.private_artifacts import content_digest, file_identity

TOOL = Path(__file__).resolve().parents[2] / 'scripts/profiling/record_observation'
spec = importlib.util.spec_from_file_location('record_observation_workflow', TOOL / 'workflow.py')
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


class PrivateCaptureFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name in ('host.py', 'host.json', 'input.csv'):
            (self.root / name).write_text('private fixture, never public payload')
        self.source = dict(head='1'*40, tree='2'*40)
        self.tools = dict(head='3'*40, tree='4'*40)
        self.base = dict(source_root=str(self.root/'source'), expected_source=self.source,
            artifact_root=str(self.root/'old-artifacts'), run_id='original', cell_id='original',
            workload=dict(kind='record_chain', name='MMF', rows=4, expected_calls=12,
                input=dict(csv=str(self.root/'input.csv'), **file_identity(self.root/'input.csv'))),
            host_helpers=dict(path=str(self.root/'host.py'), **file_identity(self.root/'host.py')),
            host_config=dict(path=str(self.root/'host.json'), **file_identity(self.root/'host.json')),
            units=dict(chain=12), engine=dict(max_model_calls=12, max_num_seqs=128,driver_python=sys.executable),
            generation=dict(seed=0, stop=['private fixture']),
            resources=dict(max_active_batches=2, model_batch_rows=1),
            environment=dict(KEEP_PRIVATE_BINDING='unchanged'),
            observation=dict(mode='off', profile='handoff', max_events_per_process=100,
                             max_event_bytes=65536))
        self.base_path = self.root/'base.json'
        self.base_path.write_text(json.dumps(self.base))
        self.ready = dict(status='passed_CPU_fixtures', tool_source=self.tools,
            public_source=self.source, base_config_sha256=file_identity(self.base_path)['sha256'],
            observer_sha256=workflow.runtime_hashes(),input_binding=workflow.input_binding(self.base))
        self.ready_path = self.root/'CPU-READY.json'
        self.ready_path.write_text(json.dumps(self.ready))
        self.addCleanup(patch.stopall)
        patch.object(workflow, 'tool_identity', return_value=self.tools).start()
        patch.object(workflow, 'git_identity', return_value=self.source).start()

    def prepare(self, name='new', mode='on'):
        root = self.root/name
        workflow.prepare(self.base_path, self.ready_path, root, name, mode, 'handoff')
        return root/'PREPARED-CONFIG.json'

    def test_on_off_preserve_work_and_site_bindings_without_changing_C(self):
        before = self.base_path.read_bytes()
        configs = [json.loads(self.prepare(mode, mode).read_text()) for mode in ('off','on')]
        for config in configs:
            for key in ('engine','generation','resources','environment','workload','units','expected_source'):
                self.assertEqual(config[key], self.base[key])
            self.assertFalse(config['execution_enabled'])
        self.assertEqual([c['observation']['mode'] for c in configs], ['off','on'])
        self.assertEqual(self.base_path.read_bytes(), before)

    def test_changed_qualified_base_or_observer_rejects_before_creating_cell(self):
        for field in ('base_config_sha256','observer_sha256'):
            bad = deepcopy(self.ready)
            bad[field] = 'changed' if field.endswith('sha256') and field.startswith('base') else {}
            self.ready_path.write_text(json.dumps(bad))
            with self.assertRaises(ValueError): self.prepare(field)
            self.assertFalse((self.root/field).exists())

    def test_changed_input_and_invalid_budget_fail_before_model_or_output(self):
        (self.root/'input.csv').write_text('changed private input')
        with self.assertRaises(ValueError): self.prepare()
        self.assertFalse((self.root/'new').exists())
        (self.root/'input.csv').write_text('private fixture, never public payload')
        for bad in ('bad:budget','../escape','..','original'):
            with self.assertRaises(ValueError): self.prepare(bad)

    def test_native_budget_mismatch_is_rejected_not_silently_rewritten(self):
        self.base['engine']['max_model_calls'] = 999
        with self.assertRaises(ValueError): workflow.validate_base(self.base)
        self.assertEqual(self.base['engine']['max_model_calls'], 999)

    def test_existing_cell_is_never_overwritten(self):
        path = self.prepare()
        before = path.read_bytes()
        with self.assertRaises(FileExistsError): self.prepare()
        self.assertEqual(path.read_bytes(), before)

    def test_run_checks_ready_source_and_only_invokes_existing_controller(self):
        path = self.prepare()
        with patch.object(workflow.subprocess, 'run') as controller:
            workflow.run(path)
            args,kwargs=controller.call_args
            self.assertEqual(args[0][-1],str(path.parent.resolve()))
            self.assertEqual(args[0][0],self.base['engine']['driver_python'])
            self.assertEqual(Path(args[0][-2]),TOOL/'control_resource_C2.py')
            self.assertEqual(kwargs['env']['PYTHONPATH'].split(os.pathsep)[0],
                             str(Path(self.base['source_root'])/'code'))
            self.assertTrue(kwargs['check'])
        changed = json.loads(path.read_text())
        changed['tool_source']['tree'] = 'changed'
        path.write_text(json.dumps(changed))
        with patch.object(workflow.subprocess, 'run') as controller:
            with self.assertRaises(ValueError): workflow.run(path)
            controller.assert_not_called()

    def test_run_without_execute_flag_fails_before_controller(self):
        with patch.object(workflow.subprocess, 'run') as controller:
            with self.assertRaises(SystemExit): workflow.main(['run','--config',str(self.base_path)])
            controller.assert_not_called()

    def test_prepared_generation_or_C_change_cannot_borrow_original_ready(self):
        path = self.prepare()
        original = json.loads(path.read_text())
        for key, value in (('generation',dict(seed=99)),('resources',dict(max_active_batches=3))):
            changed = deepcopy(original)
            changed[key] = value
            path.write_text(json.dumps(changed))
            with patch.object(workflow.subprocess, 'run') as controller:
                with self.assertRaises(ValueError): workflow.run(path)
                controller.assert_not_called()

    def test_fresh_controller_environment_imports_target_not_cached_tool_modules(self):
        target=Path(self.base['source_root'])/'code'
        (target/'src/experiments').mkdir(parents=True)
        for name in ('src/__init__.py','src/experiments/__init__.py','src/experiments/attempt_ledger.py'):
            (target/name).write_text('ORIGIN="target"\n')
        path=self.prepare()
        original_run=subprocess.run
        observed=[]
        def child(command,**options):
            program='import json,src,src.experiments.attempt_ledger as a;print(json.dumps([src.__file__,a.__file__]))'
            result=original_run([command[0],'-B','-c',program],**options,stdout=subprocess.PIPE,text=True)
            observed.extend(json.loads(result.stdout))
            return result
        with patch.object(workflow.subprocess,'run',side_effect=child):workflow.run(path)
        self.assertEqual([Path(p).resolve()for p in observed],
            [(target/'src/__init__.py').resolve(),(target/'src/experiments/attempt_ledger.py').resolve()])

    def test_changed_legal_Movie_manifest_cannot_reuse_ready(self):
        manifest=self.root/'movie-manifest.json'
        manifest.write_text('{"valid_input":"first"}')
        self.base['workload']=dict(kind='movie_session',name='MovieQ3',expected_calls=12)
        self.base['units']=dict(q3=12)
        self.base['movie_plan']=dict(engine=self.base['engine'],queries=[dict(manifest=str(manifest))])
        self.base_path.write_text(json.dumps(self.base))
        def preflight(command,**options):
            value=dict(expected_calls=12,queries=[dict(query_id='q3',expected_calls=12,
                manifest_sha256=file_identity(manifest)['sha256'])])
            return subprocess.CompletedProcess(command,0,stdout=json.dumps(value))
        with patch.object(workflow.subprocess,'run',side_effect=preflight):
            ready=dict(self.ready,base_config_sha256=file_identity(self.base_path)['sha256'],
                       input_binding=workflow.input_binding(self.base))
            self.ready_path.write_text(json.dumps(ready))
            path=self.prepare()
            manifest.write_text('{"valid_input":"replacement"}')
            with self.assertRaises(ValueError):workflow.check(path)
            with self.assertRaises(ValueError):self.prepare('another')

    def test_base_change_during_CPU_qualification_never_publishes_ready(self):
        def execute(*args, **kwargs):
            self.base_path.write_text(self.base_path.read_text() + ' ')
            return subprocess.CompletedProcess([],0,stdout='fixture passed')
        with patch.object(workflow.subprocess,'run',side_effect=execute):
            with self.assertRaises(ValueError):
                workflow.qualify(self.base_path,self.root/'cpu-race',self.tools['head'])
        self.assertFalse((self.root/'cpu-race/CPU-READY.json').exists())

    def test_CPU_failure_retains_output_and_does_not_launch_probes_or_models(self):
        result = subprocess.CompletedProcess([], 1, stdout='fixture CPU failure')
        with patch.object(workflow.subprocess, 'run', return_value=result) as run:
            with self.assertRaises(RuntimeError):
                workflow.qualify(self.base_path, self.root/'cpu', self.tools['head'])
        self.assertEqual(run.call_count, 1)
        self.assertTrue((self.root/'cpu/unit-run.json').exists())
        self.assertFalse((self.root/'cpu/CPU-READY.json').exists())


class OriginalIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root/'native.bin').write_bytes(b'private original\x00unchanged\n')
        (self.root/'failed.json').write_text('{"status":"failed","calls":0}')
        self.manifest = self.root/'ORIGINALS.json'

    def create(self):
        return workflow.originals_manifest(self.root, self.manifest)

    def test_exact_bytes_including_failed_attempts_roundtrip_and_manifest_is_exclusive(self):
        report = self.create()
        self.assertEqual(report['files'], 2)
        self.assertEqual(workflow.verify_originals(self.root,self.manifest)['files'], 2)
        self.assertIsNone(workflow.verify_originals(self.root,self.manifest)['capture_complete'])
        with self.assertRaises(FileExistsError): self.create()

    def test_changed_missing_and_extra_files_are_each_rejected(self):
        self.create()
        native = self.root/'native.bin'
        original = native.read_bytes()
        native.write_bytes(b'changed')
        with self.assertRaises(ValueError): workflow.verify_originals(self.root,self.manifest)
        native.unlink()
        with self.assertRaises(ValueError): workflow.verify_originals(self.root,self.manifest)
        native.write_bytes(original)
        (self.root/'unexpected').write_text('new')
        with self.assertRaises(ValueError): workflow.verify_originals(self.root,self.manifest)

    def test_escape_duplicate_and_tampered_manifest_are_rejected(self):
        self.create()
        original = json.loads(self.manifest.read_text())
        for name in ('../outside','/absolute','native.bin'):
            value = deepcopy(original)
            if name == 'native.bin': value['files'].append(deepcopy(value['files'][0]))
            else: value['files'][0]['path'] = name
            payload = {k:v for k,v in value.items() if k!='manifest_sha256'}
            value['manifest_sha256'] = content_digest(payload)
            self.manifest.write_text(json.dumps(value))
            with self.assertRaises(ValueError): workflow.verify_originals(self.root,self.manifest)
        original['manifest_sha256'] = 'bad'
        self.manifest.write_text(json.dumps(original))
        with self.assertRaises(ValueError): workflow.verify_originals(self.root,self.manifest)

    def test_symlink_does_not_escape_original_root(self):
        (self.root/'linked').symlink_to(self.root/'native.bin')
        with self.assertRaises(ValueError): self.create()


class GitSourceTests(unittest.TestCase):
    def test_exact_head_tree_and_untracked_code_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            def git(*args):
                return subprocess.check_output(['git',*args],cwd=repo,text=True,stderr=subprocess.DEVNULL).strip()
            git('init')
            (repo/'code').mkdir()
            source = repo/'code/source.py'
            source.write_text('original\n')
            git('add','code')
            git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid',
                '-c','commit.gpgsign=false','commit','-m','fixture')
            expected = dict(head=git('rev-parse','HEAD'),tree=git('rev-parse','HEAD^{tree}'))
            self.assertEqual(workflow.git_identity(repo,expected),expected)
            (repo/'code/overlay.py').write_text('private overlay')
            with self.assertRaises(ValueError): workflow.git_identity(repo,expected)
            (repo/'code/overlay.py').unlink()
            source.write_text('modified\n')
            with self.assertRaises(ValueError): workflow.git_identity(repo,expected)


class MovieInputBindingTests(unittest.TestCase):
    def test_real_target_preflight_distinguishes_two_valid_inputs_at_same_manifest_path(self):
        import csv
        from dataclasses import asdict
        import shutil
        from src.baselines.text.movie_records import prepare_movie_source
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            manifests=[]
            for tag in ('first','replacement'):
                path=root/(tag+'.csv')
                with path.open('w',newline='') as stream:
                    writer=csv.writer(stream)
                    writer.writerow(['reviewId','id','reviewText','scoreSentiment'])
                    writer.writerows([['1','taken_3',tag,'POSITIVE'],['2','other','fixture','NEGATIVE']])
                manifests.append(prepare_movie_source(path,root/tag,
                    expected_csv_sha256=file_identity(path)['sha256'],max_rows=2,task='movie-q1'))
            model=root/'model.json';model.write_text('{}')
            engine=asdict(EngineSpec(str(model),sys.executable,sys.executable,max_model_calls=2))
            plan=dict(schema='semloom.movie_session_plan.v1',backend='semloom-core',lifecycle='isolated',
                model_name='openai/cpu-fixture',generation=asdict(GenerationSpec(max_tokens=4)),engine=engine,
                resources=asdict(RecordsResources(prepare_batch_rows=1,model_batch_rows=1)),
                profile=dict(mode='async',max_inflight=1),queries=[dict(query_id='q1',task='movie-q1',
                    manifest=str(manifests[0]),phase='measured')])
            config=dict(source_root=str(workflow.REPO),movie_plan=plan,
                workload=dict(kind='movie_session',expected_calls=2),units=dict(q1=2))
            first=workflow.input_binding(config)
            for path in manifests[1].parent.iterdir():
                if path.is_file():shutil.copyfile(path,manifests[0].parent/path.name)
            second=workflow.input_binding(config)
            self.assertEqual(first['expected_calls'],second['expected_calls'])
            self.assertNotEqual(first['queries'][0]['manifest_sha256'],second['queries'][0]['manifest_sha256'])


if __name__ == '__main__': unittest.main()
