import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments.postgresql import m1_campaign as campaign
from src.experiments.postgresql.m1_selection import select_capacity, paired_comparison, supply_observation
from src.experiments.postgresql.query_workloads import prepare
from src.execution_provider.semantic_map import SemanticMapPlan
from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION

POLICY = dict(epsilon=.03, max_relative_spread=.1, min_repeats=3,
              supply_level=.9, min_supply_fraction=.8, middle_fraction=.6)


def observations(seconds, capacity=4):
    return [dict(repeat=i+1, status='passed', completed_rows=128, expected_rows=128,
                 actual_posts=128, quality=dict(invalid=0, false_negative=4),
                 manifest_sha256='input', jct_seconds=s,
                 supply=dict(middle_supplied_fraction=.95)) for i, s in enumerate(seconds)]


def configuration(root):
    inputs = root/'inputs'; inputs.mkdir()
    manifests = {}
    for split in ('tuning', 'evaluation'):
        manifests[split] = prepare(inputs/split, 'movie',
            ((split+str(i), 'movie-'+split, 'review', 'POSITIVE') for i in range(128)), {}, max_rows=128)
    groups = [dict(id=arm+str(c), arm=arm, control='request', capacity=c, event_content='full')
              for arm in ('pg', 'pg-source-direct') for c in (4, 16, 64)]
    keys = [g['id'] for g in groups]
    return dict(schema=campaign.SCHEMA, stage='screening', split='tuning',
        inputs_root=str(inputs), manifest_sha256={k: v['sha256'] for k,v in manifests.items()},
        model_id='fixture', service_signature='fixture-fixed-service', groups=groups,
        resources=dict(window=128, input_bytes=128*1048576, result_bytes=128*1048576,
                       pg_window_bytes=256*1048576, pg_staging_bytes=4*1048576),
        orders=[keys, keys[1:]+keys[:1], list(reversed(keys)), keys[2:]+keys[:2]],
        max_posts=128*6*4, max_seconds=600, query_timeout_s=30, selection_policy=POLICY,
        output_root=str(root/'run'), model_config=str(root/'model.json'), pg_log=str(root/'pg.log'),
        budget_id='fixture', budget_path=str(root/'budget'))


class CapacitySelectionTests(unittest.TestCase):
    def test_platform_uses_effective_complete_throughput_and_keeps_wrong_answers(self):
        samples = {4: observations([2,2,2]), 16: observations([1,1.01,1]), 64: observations([1,1,1])}
        value = select_capacity(samples, capacities=[4,16,64], policy=POLICY)
        self.assertEqual(value['selected'],16)
        self.assertEqual(value['status'],'platform_candidate')
        self.assertFalse(value['saturation_proven'])

    def test_no_platform_when_highest_capacity_still_improves(self):
        samples = {4:observations([4]*3),16:observations([2]*3),64:observations([1]*3)}
        value = select_capacity(samples, capacities=[4,16,64], policy=POLICY)
        self.assertIsNone(value['selected'])
        self.assertEqual(value['status'],'no_platform_observed')

    def test_peak_without_sustained_supply_cannot_select(self):
        samples = {4:observations([2]*3),16:observations([1]*3),64:observations([1]*3)}
        samples[64][1]['supply']['middle_supplied_fraction'] = .01
        value = select_capacity(samples, capacities=[4,16,64], policy=POLICY)
        self.assertEqual(value['status'],'inconclusive_supply')
        self.assertIsNone(value['selected'])

    def test_near_medians_do_not_hide_large_repeat_variation(self):
        samples = {4:observations([2]*3),16:observations([.8,1,1.2]),64:observations([1]*3)}
        self.assertEqual(select_capacity(samples, capacities=[4,16,64], policy=POLICY)['status'],
                         'inconclusive_repeat_variation')

    def test_missing_failed_nonfinite_or_mismatched_results_are_not_dropped(self):
        base={4:observations([2]*3),16:observations([1]*3),64:observations([1]*3)}
        for field, value in [('status','failed'),('completed_rows',127),('actual_posts',129),
                             ('jct_seconds',float('nan')),('manifest_sha256','other')]:
            with self.subTest(field=field):
                samples=copy.deepcopy(base);samples[64][0][field]=value
                with self.assertRaises(ValueError):select_capacity(samples,capacities=[4,16,64],policy=POLICY)
        with self.assertRaises(ValueError):select_capacity({4:base[4],16:base[16]},capacities=[4,16,64],policy=POLICY)
        samples=copy.deepcopy(base);samples[16].pop()
        with self.assertRaises(ValueError):select_capacity(samples,capacities=[4,16,64],policy=POLICY)

    def test_independent_pairs_report_loss_and_output_changes(self):
        first, second=observations([2]*3),observations([1]*3)
        for a,b in zip(first,second):a['output_values']={'row':'a'};b['output_values']={'row':'b'}
        value=paired_comparison(first,second,policy=POLICY)
        self.assertEqual(value['status'],'throughput_loss_in_all_pairs')
        self.assertEqual(value['paired_output_difference_counts'],[1,1,1])
        first[0]['jct_seconds']=.9
        self.assertEqual(paired_comparison(first,second,policy=POLICY)['status'],'inconclusive_repeat_variation')

    def test_supply_area_keeps_startup_and_drain_without_peak_shortcut(self):
        events=[dict(event='http_started',monotonic_ns=10),dict(event='http_finished',monotonic_ns=20)]
        value=supply_observation(events,start_ns=0,end_ns=100,capacity=1,policy=POLICY)
        self.assertEqual(value['peak'],1)
        self.assertEqual(value['active_request_seconds'],10/1e9)
        self.assertEqual(value['middle_supplied_fraction'],0)
        with self.assertRaises(ValueError):supply_observation(events[:1],start_ns=0,end_ns=100,capacity=1,policy=POLICY)


class PreflightTests(unittest.TestCase):
    def test_c64_with_32mib_results_is_rejected_without_pg_or_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);c=configuration(root);c['resources']['result_bytes']=32*1048576
            report=campaign.preflight(c)
            self.assertEqual(report['checks']['pg64']['limits']['core_results'],32)
            self.assertEqual(report['checks']['pg64']['status'],'unreachable')
            config=root/'config.json';config.write_text(json.dumps(c))
            with patch.object(campaign,'CellBudgetLedger') as ledger, patch.object(campaign,'install_input_table') as install:
                with self.assertRaisesRegex(ValueError,'unreachable'):campaign.run_campaign(config)
                ledger.assert_not_called();install.assert_not_called()
            self.assertTrue((root/'run/preflight.json').exists())

    def test_uniform_resources_admit_128_candidates_statically(self):
        with tempfile.TemporaryDirectory() as directory:
            c=configuration(Path(directory));report=campaign.preflight(c)
            self.assertEqual(report['status'],'ready')
            self.assertEqual(report['checks']['pg64']['limits']['core_results'],128)
            self.assertEqual(report['expected_posts'],3072)
            self.assertFalse(report['checks']['pg64']['runtime_supply_proven'])

    def test_input_pg_and_staging_can_independently_block(self):
        with tempfile.TemporaryDirectory() as directory:
            base=configuration(Path(directory))
            for resource, reason in [('input_bytes','core_input'),('pg_window_bytes','pg_retained'),
                                     ('pg_staging_bytes','pg_staging')]:
                c=copy.deepcopy(base);c['resources'][resource]=65536
                self.assertIn(reason,campaign.preflight(c)['checks']['pg64']['blockers'])

    def test_legacy_matrix_and_stale_total_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            c=configuration(Path(directory));c['max_posts']=44544
            with self.assertRaisesRegex(ValueError,'POST allocation'):campaign.preflight(c)
            del c['schema']
            with self.assertRaisesRegex(ValueError,'retired'):campaign.preflight(c)

    def test_bad_round_or_missing_direct_point_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            c=configuration(Path(directory));c['orders'][1][0]=c['orders'][1][1]
            with self.assertRaisesRegex(ValueError,'exactly once'):campaign.preflight(c)

    def test_holdout_cannot_choose_its_own_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);c=configuration(root);c.update(stage='evaluation',split='evaluation',reference_group='pg16')
            source=root/'selection.json';source.write_text('{}')
            import hashlib
            c.update(selection_source=str(source),selection_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
            with self.assertRaisesRegex(ValueError,'tuning decision'):campaign.preflight(c)

    def test_query_failure_preserves_partial_evidence_and_stops_following_units(self):
        from contextlib import nullcontext
        from types import SimpleNamespace
        import time
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);c=configuration(root);config=root/'config.json';config.write_text(json.dumps(c))
            class Gateway:
                calls=0
                def __init__(self,cfg,**kw):self.config=cfg;self.gateway=SimpleNamespace(pid=1)
                def __enter__(self):return self
                def __exit__(self,*_):return False
                def run_query(self,*a,**kw):Gateway.calls+=1;raise RuntimeError('fixture failure')
            with patch.dict('sys.modules',{'psycopg':SimpleNamespace(connect=lambda *a,**k:nullcontext(object()))}), \
                 patch.dict('os.environ',{'SEMLOOM_TEST_PG_DSN':'fixture'}), \
                 patch.object(campaign,'load_fixed_model_config',return_value=SimpleNamespace(model_id='fixture')), \
                 patch.object(campaign,'CellBudgetLedger') as ledger, patch.object(campaign,'install_input_table'), \
                 patch.object(campaign,'PersistentMapGateway',Gateway), patch.object(campaign,'run_query') as direct:
                ledger.return_value.snapshot.return_value=dict(allocated_requests=0,deadline_utc=time.time()+500)
                with self.assertRaisesRegex(RuntimeError,'fixture failure'):campaign.run_campaign(config)
                direct.assert_not_called()
            self.assertEqual(Gateway.calls,1)
            self.assertEqual(json.loads((root/'run/failure.json').read_text())['status'],'failed')


class CampaignExecutionTests(unittest.TestCase):
    def test_explicit_screening_runs_both_paths_and_never_starts_evaluation(self):
        from contextlib import nullcontext
        from types import SimpleNamespace
        import hashlib
        import time
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);c=configuration(root);path=root/'config.json';path.write_text(json.dumps(c))
            calls=[]
            def summary(cfg, output):
                output.mkdir(parents=True);(output/'q0').mkdir()
                calls.append(cfg.arm)
                manifest=campaign.load_manifest(root/'inputs/tuning/manifest.json')
                seconds=2 if cfg.concurrency==4 else 1
                start=1000000;end=start+int(seconds*1e9);events=[]
                count=manifest['rows'];capacity=cfg.concurrency
                for offset in range(0,count,capacity):
                    begin=start+int(offset/count*seconds*1e9)
                    finish=start+int(min(count,offset+capacity)/count*seconds*1e9)
                    for _ in range(min(capacity,count-offset)):events.append(dict(event='http_started',monotonic_ns=begin))
                    for _ in range(min(capacity,count-offset)):events.append(dict(event='http_finished',monotonic_ns=finish))
                (output/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
                data=''.join(json.dumps(dict(received_ns=end,row=[str(i),'POSITIVE']))+'\n' for i in range(count))
                (output/'q0/results.jsonl').write_text(data)
                return dict(status='passed',manifest_sha256=manifest['sha256'],query_preparation_started_ns=start,
                    execution=dict(t_query_terminal_ns=end,recorded_rows=count,query_jct_seconds=seconds,
                                   results_sha256=hashlib.sha256(data.encode()).hexdigest()),
                    resources={},evaluation=dict(actual_posts=count,quality=dict(invalid=0,false_negative=3)))
            class Gateway:
                def __init__(self,cfg,**kw):self.config=cfg;self.root=kw['root'];self.gateway=SimpleNamespace(pid=1)
                def __enter__(self):return self
                def __exit__(self,*_):return False
                def run_query(self,unit,**kw):return summary(self.config,self.root/unit)
            class Sampler:
                def __init__(self,*a,**k):pass
                def __enter__(self):return self
                def __exit__(self,*_):return False
                def summary(self):return dict(samples=0)
            def direct(cfg,**kw):return summary(cfg,kw['root'])
            with patch.dict('sys.modules',{'psycopg':SimpleNamespace(connect=lambda *a,**k:nullcontext(object()))}), \
                 patch.dict('os.environ',{'SEMLOOM_TEST_PG_DSN':'fixture'}), \
                 patch.object(campaign,'load_fixed_model_config',return_value=SimpleNamespace(model_id='fixture')), \
                 patch.object(campaign,'CellBudgetLedger') as ledger,patch.object(campaign,'install_input_table'), \
                 patch.object(campaign,'PersistentMapGateway',Gateway),patch.object(campaign,'run_query',side_effect=direct), \
                 patch.object(campaign,'ProcessSampler',Sampler),patch.object(campaign,'analyze_waiting_positions',return_value={}):
                ledger.return_value.snapshot.return_value=dict(allocated_requests=0,deadline_utc=time.time()+500)
                campaign.run_campaign(path)
            result=json.loads((root/'run/comparison.json').read_text())
            self.assertEqual(result['actual_posts'],3072)
            self.assertEqual(len(calls),24)
            self.assertEqual(calls.count('pg-source-direct'),12)
            self.assertFalse(result['next_stage_started'])
            self.assertEqual(result['selection']['pg']['selected'],16)
            self.assertEqual(result['selection']['pg-source-direct']['selected'],16)
            self.assertTrue(all(r['manifest_sha256']==c['manifest_sha256']['tuning'] for r in result['rows']))

    def test_work_tuning_keeps_no_advantage_and_no_trigger_results(self):
        groups=[dict(id='request',control='request'),dict(id='work',control='token',work_limit=4096)]
        c=dict(stage='strategy-tuning',groups=groups,reference_group='request',selection_policy=POLICY)
        rows=[]
        for group,times in [('request',[1]*3),('work',[1.5]*3)]:
            for row in observations(times):
                row.update(group=group,warmup=False,evaluation={'organization':{'compute_lifecycle':{'work_only_block_count':1}}})
                rows.append(row)
        result=campaign.summarize(c,rows)
        self.assertIsNone(result['work_candidate'])
        self.assertEqual(result['paired_comparisons']['work']['status'],'throughput_loss_in_all_pairs')
        for row in rows:
            row['jct_seconds']=1
            row['evaluation']['organization']['compute_lifecycle']['work_only_block_count']=0
        self.assertIsNone(campaign.summarize(c,rows)['work_candidate'])

    def test_resource_areas_and_tokens_do_not_turn_limits_into_rss(self):
        from src.experiments.postgresql.m1_measurement import retained_resource_areas,token_usage
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'rss.jsonl'
            path.write_text('\n'.join(json.dumps(v) for v in [
                dict(monotonic_ns=0,rss_bytes={'a':10,'b':100},cpu_seconds={'a':1}),
                dict(monotonic_ns=2000000000,rss_bytes={'a':20,'b':0},cpu_seconds={'a':2})]))
            events=[dict(event='core_submitted',monotonic_ns=0,usage={'result_bytes':1048576}),
                    dict(event='core_job_drained',monotonic_ns=1000000000,usage={'result_bytes':0})]
            result=retained_resource_areas(events,path)
            self.assertEqual(result['logical_unit_seconds']['result_bytes'],1048576)
            self.assertEqual(result['rss_byte_seconds'],{'a':20,'b':200})
            self.assertEqual(result['sampled_cpu_seconds'],{'a':1})
            self.assertEqual(token_usage([],1)['status'],'unavailable')
            usage=token_usage([dict(event='core_map_completion',prompt_tokens=10,output_tokens=2)],1)
            self.assertEqual(usage['output_tokens'],2)


class StageSelectionTests(unittest.TestCase):
    def test_evaluation_accepts_recorded_design_and_rejects_later_change(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);c=configuration(root)
            c.update(stage='evaluation',split='evaluation',reference_group='pg16')
            design={k:c[k] for k in ('groups','resources','selection_policy','service_signature','manifest_sha256')}
            source=root/'selected.json'
            source.write_text(json.dumps(dict(status='ready_for_independent_evaluation',evaluation_design=design)))
            c.update(selection_source=str(source),selection_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(campaign.preflight(c)['status'],'ready')
            c['resources']['result_bytes']+=1048576
            with self.assertRaisesRegex(ValueError,'tuning decision'):campaign.preflight(c)

    def test_work_stage_requires_matching_screen_and_preserves_context(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);c=configuration(root)
            c['groups']=[dict(id='selected',arm='pg',control='request',capacity=16,event_content='full'),
                         dict(id='high',arm='pg',control='request',capacity=64,event_content='full'),
                         dict(id='work',arm='pg',control='token',capacity=64,event_content='full',work_limit=4096)]
            keys=[g['id'] for g in c['groups']]
            c.update(stage='strategy-tuning',reference_group='selected',orders=[keys]*4,max_posts=128*3*4,
                context_tokens=4096,model_revision='fixture',tokenizer_path='/path/to/fixture',tokenizer_fingerprint='a'*64)
            work=root/'inputs/tuning/work.json';work.write_text(json.dumps(dict(work=[200]*128)))
            c['work_sha256']={'tuning':hashlib.sha256(work.read_bytes()).hexdigest()}
            source=root/'screening.json'
            screen=dict(status='completed',stage='screening',resources=c['resources'],selection_policy=POLICY,
                service_signature=c['service_signature'],manifest_sha256=c['manifest_sha256'],
                selection={'pg':dict(status='platform_candidate',selected=16,median_rates={'4':64,'16':128,'64':128})})
            source.write_text(json.dumps(screen))
            c.update(screening_source=str(source),screening_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
            report=campaign.preflight(c)
            self.assertFalse(report['checks']['work']['work']['nonbinding_for_described_input'])
            self.assertEqual(report['checks']['work']['work']['context_tokens'],4096)
            different_path=copy.deepcopy(c);different_path['groups'][0]['arm']='pg-source-direct'
            with self.assertRaisesRegex(ValueError,'request-count reference'):campaign.preflight(different_path)
            screen['selection']['pg']['status']='no_platform_observed';source.write_text(json.dumps(screen))
            c['screening_sha256']=hashlib.sha256(source.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError,'selected request reference'):campaign.preflight(c)

    def test_group_request_count_and_log_modes_do_not_form_implicit_matrix(self):
        with tempfile.TemporaryDirectory() as directory:
            c=configuration(Path(directory));c['groups'][0]['event_content']='compact'
            with self.assertRaisesRegex(ValueError,'observation stage'):campaign.preflight(c)
