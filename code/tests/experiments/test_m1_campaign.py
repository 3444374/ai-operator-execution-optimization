import unittest
from src.experiments.postgresql.m1_campaign import CAPACITIES, MAX_POSTS, select_capacity


class CapacitySelectionTests(unittest.TestCase):
    def test_near_ties_choose_smaller_capacity_using_only_complete_tuning_times(self):
        values={4:[2.0,2.0],8:[1.05,1.05],16:[1.0,1.0],32:[.99,.99],64:[.99,1.01]}
        self.assertEqual(select_capacity(values)['selected'],16)
        with self.assertRaises(ValueError):select_capacity({k:v for k,v in values.items() if k!=4})
        values[4]=[1.0]
        with self.assertRaises(ValueError):select_capacity(values)
        self.assertEqual(5*3*512+6*6*1024,MAX_POSTS)

    def test_finite_schedule_and_nonbinding_work_stop_without_model_calls(self):
        from contextlib import nullcontext
        import json,os,tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        from src.experiments.postgresql import m1_campaign as campaign
        for selected in (16,32):
            with self.subTest(selected=selected),tempfile.TemporaryDirectory() as directory:
                base=Path(directory);inputs=base/'inputs';inputs.mkdir()
                (inputs/'evaluation').mkdir();(inputs/'evaluation/work.json').write_text(json.dumps(dict(work=[200]*1024)))
                config=dict(output_root=str(base/'run'),inputs_root=str(inputs),model_config=str(base/'model.json'),
                    pg_log=str(base/'pg.log'),max_posts=MAX_POSTS,max_seconds=1800,
                    manifest_sha256=dict(tuning='tune',evaluation='eval'),budget_id='fixture',budget_path=str(base/'budget'),
                    model_id='fixture',work_limits=dict(tight=4096,wide=12800),model_revision='fixture',service_signature='fixture',
                    tokenizer_path=str(base/'tokenizer'),tokenizer_fingerprint='a'*64)
                config_path=base/'config.json';config_path.write_text(json.dumps(config));calls=[]
                class Gateway:
                    def __init__(self,cfg,**kwargs):self.config=cfg;self.root=kwargs['root'];self.gateway=SimpleNamespace(pid=os.getpid())
                    def __enter__(self):self.root.mkdir();return self
                    def __exit__(self,*_):return False
                    def run_query(self,unit,**kwargs):
                        (self.root/unit).mkdir();calls.append((self.config,kwargs))
                        seconds=1 if self.config.concurrency>=selected else 2
                        return dict(manifest_sha256='fixture',query_preparation_started_ns=1,
                            execution=dict(t_query_terminal_ns=1+int(seconds*1e9),query_jct_seconds=seconds),
                            evaluation=dict(quality=dict(invalid=0)))
                def manifest(path):return dict(kind='movie',rows=512 if path.parent.name=='tuning' else 1024,
                    sha256='tune' if path.parent.name=='tuning' else 'eval')
                with patch.dict('sys.modules',{'psycopg':SimpleNamespace(connect=lambda *a,**k:nullcontext(object()))}), \
                     patch.dict(os.environ,{'SEMLOOM_TEST_PG_DSN':'fixture'}), \
                     patch.object(campaign,'PersistentMapGateway',Gateway),patch.object(campaign,'CellBudgetLedger'), \
                     patch.object(campaign,'load_manifest',side_effect=manifest),patch.object(campaign,'install_input_table'), \
                     patch.object(campaign,'read_prepared',return_value=[]), \
                     patch.object(campaign,'analyze_waiting_positions',return_value=dict(preparation_to_eof_seconds=1)):
                    campaign.run_campaign(config_path)
                result=json.loads((base/'run/comparison.json').read_text())
                self.assertEqual(len(calls),15 if selected==16 else 51)
                self.assertEqual(result['actual_posts'],7680 if selected==16 else MAX_POSTS)
                self.assertEqual(result['status'],'inconclusive' if selected==16 else 'completed')
                self.assertTrue(all(k['trace_flow']==(cfg.event_content=='full') for cfg,k in calls))
