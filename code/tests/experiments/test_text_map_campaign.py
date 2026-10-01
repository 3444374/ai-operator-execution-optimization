"""A finite declared stage cannot omit candidates, reset quota or retune evaluation."""
from dataclasses import asdict
import copy
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import write_private_json
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.postgresql import text_map_campaign as campaign
from src.experiments.postgresql.text_map_comparison import ROLES
from tests.experiments.test_text_map_comparison import recording, reference


def setup(root):
    model=root/'model.json'
    write_private_json(model,dict(endpoint_url='http://127.0.0.1:1/v1/chat/completions',model_id='fixture',timeout_ms=1000))
    environment=root/'environment.json';write_private_json(environment,dict(status='ok'))
    manifest=prepare(root/'inputs','movie',[('first','movie','text','POSITIVE'),('second','movie','other','NEGATIVE')],{},max_rows=2)
    receipt=root/'installation.json'
    write_private_json(receipt,dict(table='inputs',rows=2,immutable=True,manifest_sha256=manifest['sha256'],
                                  source_sha256=manifest['files']['raw.jsonl']['sha256']))
    transport=root/'transport.json'
    write_private_json(transport,dict(address='127.0.0.1:6379',workers=1,batch_rows=2,window_bytes=2097152,object_bytes=4194304))
    candidates=[]
    for role in ROLES:
        options={}
        if role=='pg-daft-ray':options.update(map_transport_config=str(transport),map_transport_sha256=reference(transport)['sha256'])
        if role=='ray-data':options['ray_address']='127.0.0.1:6379'
        cfg=QueryConfig(role,'pg' if role in ('pg-http','pg-daft-ray') else role,'map','inputs',max_posts=2,**options)
        candidates.append(dict(id=role,role=role,config=asdict(cfg)))
    return dict(schema=campaign.SCHEMA,stage='tuning',repeats=3,candidates=candidates,
        orders=[list(ROLES[i:]+ROLES[:i]) for i in range(4)],max_posts=32,max_seconds=300,
        manifest=reference(root/'inputs/manifest.json'),model=reference(model),installation=reference(receipt),
        environment=reference(environment),service_signature='f'*64,dsn_env='TEXT_MAP_FIXTURE_DSN',
        budget_path=str(root/'budget.sqlite'),budget_id='fixture',output_root=str(root/'stage'),pg_log=str(root/'pg.log'))


def write_spec(root,spec):
    path=root/'spec.json';path.write_text(json.dumps(spec));return path


class CampaignTests(unittest.TestCase):
    def test_arrow_transport_cannot_enter_the_daft_ray_campaign(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spec = setup(root)
            path = root/'transport.json'
            value = json.loads(path.read_text())
            value['payload_backend'] = 'arrow'
            path.write_text(json.dumps(value))
            candidate = next(c for c in spec['candidates'] if c['role'] == 'pg-daft-ray')
            candidate['config']['map_transport_sha256'] = reference(path)['sha256']
            candidate['config']['map_payload_backend'] = 'arrow'
            with self.assertRaisesRegex(ValueError, 'Daft payload'):
                campaign.preflight(spec)

    def test_main_profile_rejects_undersupplied_ray_and_unequal_native_resources(self):
        from src.experiments.postgresql.text_map_candidates import main_candidates
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);spec=setup(root)
            spec.update(profile='main',candidates=main_candidates('inputs',2,
                ray_address='127.0.0.1:6379',transport_path=str(root/'transport.json'),
                transport_sha256=reference(root/'transport.json')['sha256']))
            keys=[c['id'] for c in spec['candidates']]
            spec.update(orders=[keys]*4,max_posts=96)
            self.assertEqual(campaign.preflight(spec)['queries'],48)
            for field,value in [('ray_batch_rows',32),('ray_async_batches_per_actor',1),('ray_read_blocks',1)]:
                bad=copy.deepcopy(spec)
                next(c for c in bad['candidates'] if c['id']=='ray-data-c128')['config'][field]=value
                with self.subTest(field=field),self.assertRaises(ValueError):campaign.preflight(bad)
            bad=copy.deepcopy(spec)
            next(c for c in bad['candidates'] if c['role']=='daft-native')['config']['daft_num_threads']=4
            with self.assertRaises(ValueError):campaign.preflight(bad)

    def test_preflight_requires_exact_complete_schedule_and_existing_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            spec=setup(Path(directory))
            check=campaign.preflight(spec)
            self.assertEqual(check['queries'],16);self.assertEqual(check['max_posts'],32)
            self.assertFalse(check['database_contacted'])
            for field,value in [('max_posts',31),('orders',[list(ROLES)]*3),('service_signature','missing')]:
                bad=copy.deepcopy(spec);bad[field]=value
                with self.subTest(field=field),self.assertRaises(ValueError):campaign.preflight(bad)
            bad=copy.deepcopy(spec);bad['orders'][1][0]=bad['orders'][1][1]
            with self.assertRaises(ValueError):campaign.preflight(bad)
            bad=copy.deepcopy(spec);bad['candidates'][-1]['config']['ray_address']='127.0.0.1:6380'
            with self.assertRaises(ValueError):campaign.preflight(bad)
            Path(spec['model']['path']).write_text('{}')
            with self.assertRaises(ValueError):campaign.preflight(spec)

    def fake_worker(self,spec,fail_at=None):
        self.launched=[]
        def supervise(command,output,close_budget,**kwargs):
            cfg=json.loads(Path(command[command.index('--config')+1]).read_text())
            role=('pg-daft-ray' if cfg['map_transport_config'] else 'pg-http') if cfg['arm']=='pg' else cfg['arm']
            self.launched.append(cfg['unit_id'])
            ledger=CellBudgetLedger(Path(spec['budget_path']),AttemptBudget(spec['budget_id'],spec['max_posts']))
            ledger.reserve_unit(cfg['unit_id'],2);shared=ledger.claim_shared_unit(cfg['unit_id'])
            shared.reserve('e'*64)
            if len(self.launched)==fail_at:
                close_budget()
                raise RuntimeError('controlled stage failure')
            shared.reserve('e'*64)
            ref=recording(output/'unit',role,cfg['unit_id'],1,manifest=campaign.preflight(spec)['manifest_sha256'])
            path=Path(ref['path']);value=json.loads(path.read_text())
            value.update(config=cfg,model_config_sha256=spec['model']['sha256'])
            path.write_text(json.dumps(value));close_budget()
        return supervise

    def test_stage_runs_declared_order_and_never_starts_another_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);spec=setup(root)
            CellBudgetLedger.create(Path(spec['budget_path']),AttemptBudget('fixture',32),deadline_utc=time.time()+290)
            worker=self.fake_worker(spec)
            with patch.dict('os.environ',{'TEXT_MAP_FIXTURE_DSN':'unused fixture'}),patch.object(campaign,'supervise',side_effect=worker):
                result=campaign.run_stage(write_spec(root,spec))
            self.assertEqual(self.launched,[f'{key}-r{repeat}' for repeat,order in enumerate(spec['orders']) for key in order])
            self.assertEqual(result['budget']['allocated_requests'],32)
            self.assertFalse(result['next_stage_started'])
            self.assertEqual(set(result['selected']),set(ROLES))
            with patch.dict('os.environ',{'TEXT_MAP_FIXTURE_DSN':'unused fixture'}):
                with self.assertRaises(ValueError):campaign.run_stage(root/'spec.json')

    def test_failure_preserves_spent_quota_and_stops_remaining_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);spec=setup(root)
            CellBudgetLedger.create(Path(spec['budget_path']),AttemptBudget('fixture',32),deadline_utc=time.time()+290)
            with patch.dict('os.environ',{'TEXT_MAP_FIXTURE_DSN':'unused fixture'}),patch.object(campaign,'supervise',side_effect=self.fake_worker(spec,fail_at=2)):
                with self.assertRaisesRegex(RuntimeError,'controlled stage failure'):
                    campaign.run_stage(write_spec(root,spec))
            self.assertEqual(len(self.launched),2)
            failure=json.loads((root/'stage/failure.json').read_text())
            self.assertEqual(failure['budget']['allocated_requests'],4)
            self.assertEqual(len(failure['completed']),1)
            self.assertFalse((root/'stage/result.json').exists())

    def test_stage_rejects_changed_configuration_or_wrong_query_identity(self):
        for field, value in (('concurrency', 3), ('unit_id', 'another-query')):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root=Path(directory);spec=setup(root)
                ledger=CellBudgetLedger.create(Path(spec['budget_path']),AttemptBudget('fixture',32),
                                               deadline_utc=time.time()+290)
                worker=self.fake_worker(spec)
                def changed_worker(command, output, close_budget, **kwargs):
                    worker(command, output, close_budget, **kwargs)
                    path=output/'unit/summary.json'
                    saved=json.loads(path.read_text())
                    saved['config'][field]=value
                    path.write_text(json.dumps(saved))
                with patch.dict('os.environ',{'TEXT_MAP_FIXTURE_DSN':'unused fixture'}), \
                        patch.object(campaign,'supervise',side_effect=changed_worker):
                    with self.assertRaisesRegex(ValueError,'declared stage identities'):
                        campaign.run_stage(write_spec(root,spec))
                self.assertEqual(len(self.launched),1)
                self.assertEqual(ledger.snapshot()['allocated_requests'],2)
                failure=json.loads((root/'stage/failure.json').read_text())
                self.assertEqual(failure['completed'],[])
                self.assertFalse((root/'stage/result.json').exists())

    def test_evaluation_preflight_rejects_retuning_before_any_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);spec=setup(root)
            selected=dict(schema='semloom.text_map_comparison.v1',stage='tuning',identities=['a'*64,'b'*64,spec['model']['sha256']],
                service_signature=spec['service_signature'],selected={c['role']:campaign.configuration_identity(c['config']) for c in spec['candidates']})
            path=root/'selected.json';write_private_json(path,selected)
            spec.update(stage='evaluation',tuning_result=reference(path))
            self.assertEqual(campaign.preflight(spec)['max_posts'],32)
            spec['candidates'][0]['config']['concurrency']=3
            with self.assertRaises(ValueError):campaign.preflight(spec)
