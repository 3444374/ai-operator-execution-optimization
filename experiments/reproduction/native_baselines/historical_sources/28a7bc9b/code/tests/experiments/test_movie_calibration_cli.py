"""Actual maintained CLI/controller/worker/Core with explicit CPU-only providers.

Ray/Daft/host/tokenizer are fixtures; inference uses the existing real CPU IPC
child. Selection durations are synthetic, never a hardware performance result.
"""
from contextlib import ExitStack,redirect_stdout
from copy import deepcopy
from dataclasses import asdict,replace
import ast
import csv
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments import movie_records_matrix as entry,lotus_records_movie
from src.baselines.text.movie_records import prepare_movie_source,MovieRecordSource
from src.semantic_methods.lotus import engine as methods
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
from src.semantic_methods.lotus.records import RecordsResources
from src.infrastructure.runner_lease import HOST_SCOPE_DIR_NAME,LEASE_NAME
from src.infrastructure.environment import MachineObservation
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.experiments.calibration import core as adapter
from src.scheduling.runtime import calibration
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime

SOURCE=Path(entry.__file__).resolve().parents[3]
UPSTREAM=SOURCE.parent/'packet/sembench'


class CLIIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import duckdb  # Keep native module identity across Runtime's sys.modules fixtures.
        from src.baselines.text.sembench_movie import _load
        _load(UPSTREAM,'CPU_fixture_evaluator_warm','src/scenario/movie/evaluation/evaluate.py')
        cls.formatter=methods.LotusFilterMethod
        cls.bind=staticmethod(methods.bind_filter_method)

    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve();self.packet=self.root/'packet';self.packet.mkdir()
        (self.packet/'sembench').symlink_to(UPSTREAM,target_is_directory=True)
        csv_path=self.root/'Reviews.csv'
        with csv_path.open('w',newline='') as stream:
            writer=csv.writer(stream);writer.writerow(['reviewId','id','reviewText','scoreSentiment'])
            writer.writerows([[str(i),'taken_3','CPU fixture review '+str(i),'POSITIVE' if i<2 else 'NEGATIVE'] for i in range(4)])
        self.manifest=prepare_movie_source(csv_path,self.root/'input',expected_csv_sha256=entry.sha(csv_path),max_rows=4,task='movie-q3')
        self.source=MovieRecordSource.from_manifest(self.manifest)
        spec=EngineSpec(str(self.root/'CPU fixture model'),sys.executable,sys.executable,
                        max_num_seqs=4,max_model_calls=8)
        resources=RecordsResources(cpu_workers=2,prepare_batch_rows=1,model_batch_rows=1,
                                   max_active_batches=4,startup_timeout_s=5,query_timeout_s=8)
        self.plan=dict(schema='semloom.movie_session_plan.v1',backend='semloom-core',lifecycle='resident',
            model_name='openai/cpu-fixture',engine=asdict(spec),generation=asdict(GenerationSpec()),
            resources=asdict(resources),profile=dict(mode='async',max_inflight=4),
            queries=[dict(query_id='warm',task='movie-q3',phase='warmup',manifest=str(self.manifest)),
                     dict(query_id='target',task='movie-q3',phase='measured',manifest=str(self.manifest))])
        self.write(self.packet/'session.json',self.plan)
        self.group=dict(plan='session.json',max_calls=8,backend='semloom-core',profile='measured-offline',
            repetition=1,group='CPU-fixture',calibration=dict(cache_dir=str(self.root/'selection-cache'),
            candidates=[dict(max_inflight=1,cpu_workers=1),dict(max_inflight=3,cpu_workers=2)],
            repetitions=3,near_peak_ratio=.97,timeout_s=120))
        self.lease_scope=self.root/'artifacts'/HOST_SCOPE_DIR_NAME;self.lease_scope.mkdir(parents=True)
        self.cfg=dict(driver_python=sys.executable,engine=asdict(spec),budget_id='CPU-fixture',budget_limit=56,wall_s=600,
            lease_scope=str(self.lease_scope),gpu_idle_max_mib=1,ipc_prefix_base='/sabcd0000',
            rpc_root_prefix=str(self.root/'rpc-'),ray_root_prefix=str(self.root/'ray-'),environment_file='CPU-fixture-env',
            expected_template_sha256=hashlib.sha256(b'CPU fixture template').hexdigest(),
            finite_generation_profiles=[asdict(GenerationSpec())],driver_versions=dict(driver='CPU-fixture'))
        receipt=dict(status='passed',source_commit='CPU-fixture-source',model_revision='CPU-fixture-model',
            model_files={'CPU-fixture':dict(sha256='a'*64)},model_versions=dict(vllm='CPU-fixture'),
            tokenizer_artifacts_sha256={'CPU-fixture-tokenizer':'b'*64},template_sha256=self.cfg['expected_template_sha256'])
        self.write(self.packet/'runtime/model-preflight.json',receipt)
        formatter=self.formatter(self.bind(self.source.task.method(self.plan['model_name'])))
        prepared=formatter.prepare_rows([dict(raw,**key) for raw,key in zip(self.source.rows(),self.source.expected('ref'),strict=True)])
        refs={row['row_id']:dict(expected_prompt_token_ids=list(range(11)),messages_sha256=hashlib.sha256(
            json.dumps(json.loads(row['messages_json']),ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()) for row in prepared}
        messages=self.packet/'messages.json';self.write(messages,[])
        reference=self.packet/'reference.json';self.write(reference,dict(messages_sha256=entry.sha(messages),rows=refs))
        self.write(self.packet/'input-bindings.json',{'movie-q3':dict(manifest=str(self.manifest),manifest_sha256=entry.sha(self.manifest),
            token_reference=str(reference),token_reference_sha256=entry.sha(reference),messages=str(messages),messages_sha256=entry.sha(messages),calls=4)})
        tree=ast.parse((SOURCE.parent/'packet/token_audit.py').read_text())
        ns=dict(json=json,hashlib=hashlib,PER_QUERY_CALLS=4)
        functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('normalize_ids','audit_rows')]
        exec(compile(ast.Module(body=functions,type_ignores=[]),'CPU-fixture-token-audit','exec'),ns)
        self.audit=SimpleNamespace(**{n:ns[n] for n in ('normalize_ids','audit_rows')})
        self.drivers=[];self.children=[]

    @staticmethod
    def write(path,value):
        path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))

    def run_cli(self,name,*,fixed=False,conversion_error=None):
        output=self.root/name;cfg=deepcopy(self.cfg);cfg['budget_id']=name
        cfg.update(rpc_root_prefix=str(self.root/(name+'-rpc-')),ray_root_prefix=str(self.root/(name+'-ray-')),
                   ipc_prefix_base='/s'+hashlib.sha256(name.encode()).hexdigest()[:8])
        group=deepcopy(self.group)
        if fixed:
            group.pop('calibration');plan=deepcopy(self.plan);plan['profile']['max_inflight']=1
            self.write(self.packet/'session.json',plan)
        from src.experiments.movie_matrix_support import scan_jobs
        samples=0 if fixed else len(scan_jobs(group,0))
        cap=8*(1+samples)
        cfg['budget_limit']=cap
        self.write(self.packet/'base-config.json',cfg)
        self.write(self.packet/'owner-groups.json',[group])
        self.write(self.packet/'matrix.json',dict(execution_enabled=True,source_binding=dict(final_source='CPU-fixture-source',final_lock='CPU-fixture-lock'),
            budget=dict(max_new_calls=cap,query_count=2*(1+samples),max_total_wall_s=600,launch_deadline_s=540,
                        cleanup_reserve_s=60,automatic_retries=0,model_repair_reserve=0)))
        binding=SimpleNamespace(HERE=self.packet,SOURCE=SOURCE,COMMIT='CPU-fixture-source',LOCK='CPU-fixture-lock',
            read=entry.read,read_report=entry.read,sha=entry.sha,save=self.write,verify=lambda digest:cfg,
            runtime_binding=lambda cfg:dict(driver_python=sys.executable,model_python=sys.executable,
                model_path=cfg['engine']['model_path'],tokenizer_path=cfg['engine']['model_path']))
        lease_path=self.lease_scope/LEASE_NAME
        def acquire(cfg):
            lease_path.write_text('CPU fixture lease, not a server lease')
            return SimpleNamespace(path=lease_path,release=lambda:lease_path.unlink())
        gpu=dict(uuid='GPU-00000000-0000-0000-0000-000000000000',name='CPU fixture device',total_mib=1)
        host=SimpleNamespace(protected=lambda cfg:None,verify_assets=lambda cfg:None,idle=lambda *a,**kw:{},
            assigned_gpu=lambda cfg:gpu,acquire_owner_lease=acquire,monitoring_flags=lambda cfg:{},
            FullMonitor=lambda *a:object(),api=lambda:None,identities=lambda report:set(),
            helper=SimpleNamespace(check_lease=lambda lease:None,live_owned=lambda *a:[],error_record=lambda e:dict(type=type(e).__name__)))
        from contextlib import contextmanager
        @contextmanager
        def durable(*args,**kwargs):yield  # Existing ledger/result code still executes; no real provider.
        runtime=SimpleNamespace(binding=binding,host=host,evidence=SimpleNamespace(durable_terminals=durable),audit=self.audit)
        original_driver=entry.MovieMatrix;real_run=subprocess.run;original_ready=AsyncCoreModelActor._ready_report
        def ready(actor):
            result=dict(original_ready(actor),pid=actor.engine.process.pid)
            self.children.append(actor.engine.process)
            return result
        def construct(*args):
            driver=original_driver(*args)
            driver.finalizers=SimpleNamespace(optional_performance=lambda *a:SimpleNamespace(close=lambda:None),
                unit_final=lambda cfg,out,supervised,monitor,**kw:self.physical(out))
            self.drivers.append(driver);return driver
        def supervise(argv,out,**limits):
            driver=self.drivers[-1]
            driver.worker('CPU-fixture-packet',int(argv[argv.index('--index')+1]),Path(argv[argv.index('--context')+1]),out)
            return dict(status='passed',observed_owned_exit_confirmed=True)
        def command(argv,**kwargs):
            if 'audit' not in argv:return real_run(argv,**kwargs)
            tokenizer=SimpleNamespace(chat_template='CPU fixture template',decode=lambda *a,**kw:'fixed fixture text')
            modules={'transformers':SimpleNamespace(AutoTokenizer=SimpleNamespace(from_pretrained=lambda *a,**kw:tokenizer)),
                     'torch':SimpleNamespace(cuda=SimpleNamespace(is_initialized=lambda:False))}
            with patch.dict(sys.modules,modules),patch.dict(os.environ,CUDA_VISIBLE_DEVICES=''):
                self.drivers[-1].token_audit(Path(argv[argv.index('--report')+1]),Path(argv[argv.index('--audit-output')+1]))
            return SimpleNamespace(returncode=0,stdout='CPU fixture audit only',stderr='')
        real_measurement=adapter.core_runtime_measurement
        def objective(row):
            if conversion_error is not None:raise conversion_error
            value=real_measurement(row)
            return replace(value,elapsed_s=30. if row['candidate']['max_inflight']==1 else 10.)
        with patch.dict(os.environ,{}),ThreadedRuntime(self.source.rows()) as fake,ExitStack() as stack:
            fake.stack.enter_context(patch.object(methods,'LotusFilterMethod',self.formatter))
            fake.stack.enter_context(patch.object(methods,'bind_filter_method',side_effect=self.bind))
            state=[False];fake.ray.is_initialized=lambda:state[0]
            fake.ray.init=lambda **kw:state.__setitem__(0,True);fake.ray.shutdown=lambda:state.__setitem__(0,False)
            stack.enter_context(patch.object(entry,'load_runtime',return_value=runtime))
            stack.enter_context(patch.object(entry,'MovieMatrix',side_effect=construct))
            stack.enter_context(patch('src.experiments.lotus_records_supervisor.supervise',side_effect=supervise))
            stack.enter_context(patch.object(subprocess,'run',side_effect=command))
            stack.enter_context(patch.object(AsyncCoreModelActor,'_ready_report',ready))
            stack.enter_context(patch('integrations.imlane_records.chain_runtime.check_external_lease',return_value={}))
            stack.enter_context(patch.object(lotus_records_movie,'model_preflight',return_value={}))
            stack.enter_context(patch('src.infrastructure.environment.observe_machine',return_value=MachineObservation(
                'CPU-fixture-machine','CPU-fixture','3.12',8,('CPU fixture device',),(1,),('CPU-fixture-driver',))))
            stack.enter_context(patch.object(adapter,'core_runtime_measurement',side_effect=objective))
            with redirect_stdout(StringIO()):
                entry.main(['run','--packet',str(self.packet),'--packet-sha','CPU-fixture-packet','--output-dir',str(output),'--execute-model'])
            configs=[e[2]['max_concurrency']-2 for e in fake.events if e[:2]==('actor','AsyncCoreModelActor')]
        self.assertTrue(all(child.poll() is not None for child in self.children))
        self.assertFalse(lease_path.exists())
        return entry.read(output/'campaign.json'),output,configs

    def physical(self,out):
        self.assertTrue(all(child.poll() is not None for child in self.children))
        report=dict(independent_physical_exit=True,original_supervisor_passed=True,CPU_fixture_no_GPU=True)
        self.write(out/'independent-physical.json',report);return report

    def test_public_runtime_miss_hit_and_fixed_mode_through_actual_cli_and_cpu_core(self):
        first,out,configs=self.run_cli('first')
        self.assertEqual(first['status'],'completed')
        self.assertEqual(configs,[1,3,1,3,1,3,3])
        public=first['units'][0]['calibration_run']
        self.assertFalse(public['calibration']['cache_hit'])
        self.assertEqual(public['calibration']['scan_cost']['samples'],6)
        self.assertEqual(first['scan_succeeded_calls'],48)
        self.assertEqual(first['target_succeeded_calls'],8)
        target=entry.read(out/'units/g000/query.json')
        self.assertTrue(all(q['backend_report']['core_execution']['performance_tuned'] for q in target['queries']))
        self.assertEqual({q['backend_report']['core_execution']['max_inflight'] for q in target['queries']},{3})
        replay,replay_out,configs=self.run_cli('cache-replay')
        self.assertEqual(configs,[3])
        costs=replay['units'][0]['calibration_run']['calibration']
        self.assertTrue(costs['cache_hit']);self.assertEqual(costs['scan_cost']['samples'],0)
        self.assertEqual(costs['calibration_cost']['samples'],6)
        self.assertEqual(replay['scan_succeeded_calls'],0)
        fixed,fixed_out,configs=self.run_cli('fixed-reference',fixed=True)
        self.assertEqual(configs,[1]);self.assertNotIn('calibration_run',fixed['units'][0])
        raw=entry.read(fixed_out/'units/g000/query.json')
        self.assertTrue(all(q['backend_report']['core_execution']['performance_tuned'] is False for q in raw['queries']))
        self.assertEqual(sum(u['requests'] for u in first['budget']['units'] if u['claimed']),56)
        self.assertEqual(sum(u['requests'] for u in replay['budget']['units'] if u['claimed']),8)
        self.assertEqual(sum(u['requests'] for u in fixed['budget']['units'] if u['claimed']),8)
        print('CPU_CLI_RECEIPT '+json.dumps(dict(real_model_calls=0,GPU=0,synthetic_model_calls=72,
            first=dict(scan_samples=6,scan_calls=48,target_calls=8,selected_max_inflight=3),
            replay=dict(scan_samples=0,target_calls=8,historical_scan_samples=6),
            fixed=dict(scan_samples=0,target_calls=8,max_inflight=1,performance_tuned=False),
            all_observed_CPU_children_exited=all(child.poll() is not None for child in self.children),
            timings='synthetic selection objective; no hardware performance conclusion')))

    def test_callback_failure_keeps_known_work_and_does_not_start_target_or_retry(self):
        from src.scheduling.runtime.calibration import CalibrationScanError
        with self.assertRaises(CalibrationScanError):
            self.run_cli('failed-conversion',conversion_error=RuntimeError('CPU fixture measurement handoff failure'))
        output=self.root/'failed-conversion';report=entry.read(output/'campaign.json')
        self.assertEqual(report['status'],'failed')
        self.assertEqual(report['scan_succeeded_calls'],8)
        self.assertEqual(report['target_succeeded_calls'],0)
        self.assertEqual(sum(u['requests'] for u in report['budget']['units'] if u['claimed']),8)
        self.assertFalse((output/'units/g000').exists())
        self.assertFalse((output/'units/g002').exists())
        self.assertEqual(report['lease_status'],'released')
        self.assertTrue(all(child.poll() is not None for child in self.children))

    def test_increasing_search_uses_public_runtime_and_existing_CLI_slots_then_zero_scan_hit(self):
        self.group['calibration']['candidates']=[dict(max_inflight=1,cpu_workers=2)]
        self.group['calibration']['search']=dict(parameter='max_inflight',
            capacity_limits=dict(engine_max_num_seqs=3,Core_record_slots=3))
        report,out,configs=self.run_cli('increasing')
        self.assertEqual(report['status'],'completed')
        self.assertEqual(configs,[1,1,1,2,2,2,3,3,3,2])  # Equal speed: smaller near-peak C wins.
        resolution=report['units'][0]['calibration_run']['calibration']
        self.assertEqual(resolution['search']['stop_reason'],'capacity_limit')
        self.assertFalse(resolution['search']['platform_confirmed'])
        self.assertEqual(report['scan_succeeded_calls'],72)
        hit,_,configs=self.run_cli('increasing-hit')
        self.assertEqual(configs,[2]);self.assertEqual(hit['scan_succeeded_calls'],0)
        self.assertTrue(hit['units'][0]['calibration_run']['calibration']['cache_hit'])

    def test_sync_calibration_rejects_before_allocating_a_budget(self):
        self.plan['profile']=dict(mode='sync',max_inflight=4)
        self.write(self.packet/'session.json',self.plan)
        with self.assertRaisesRegex(ValueError,'async only'):
            self.run_cli('unsupported-sync-scan')
        self.assertFalse((self.root/'unsupported-sync-scan/budget.sqlite').exists())
        self.assertEqual(self.children,[])


if __name__=='__main__':unittest.main(verbosity=2)
