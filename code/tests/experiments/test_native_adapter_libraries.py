"""Opt-in fixed native libraries against a local fixture; no GPU or model."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
import uuid

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.native_adapter_query import run_prepared_map_query
from tests.experiments.test_native_adapter_query import fixture_server


@unittest.skipUnless(os.environ.get('SEMLOOM_ACTUAL_ARM'), 'requires a fresh process and pinned native libraries')
class NativeAdapterLibraryTests(unittest.TestCase):
    def test_full_native_graph_method_and_raw_response_association(self):
        arm=os.environ['SEMLOOM_ACTUAL_ARM']
        self.assertIn(arm,('fixed-map-native-daft','fixed-map-native-ray','fixed-map-semloom'))
        options=NativeGraphOptions()
        ray_temp_root=Path('/root/autodl-tmp/rnia.'+uuid.uuid4().hex[:8])
        physical=(RayMapConfig('resolved-by-owned-runtime',2,2,2097152,8388608)
                  if arm=='fixed-map-semloom' else None)
        root=Path(tempfile.mkdtemp(dir=os.environ.get('SEMLOOM_FIXTURE_ARTIFACT_ROOT')))
        with fixture_server(delay=.01) as (endpoint,requests):
            budget=AttemptBudget('native-fixture',8)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',budget,deadline_utc=time.time()+120)
            result=run_prepared_map_query(arm,
                load_source=lambda:iter(dict(row_id=str(i),text='duplicate fixture text') for i in range(8)),
                plan=SemanticMapPlan('fixture instruction','fixture',16),
                model=FixedModelConfig(endpoint,'fixture',3000),ledger=ledger,unit_id='library-fixture',
                root=root/'query',options=options,physical=physical,ray_temp_root=ray_temp_root,
                query_timeout_s=45,max_rows=8)
            self.assertEqual(result['status'],'passed')
            self.assertEqual(len(requests),8)
            self.assertEqual(result['actual_posts'],8)
            self.assertEqual(result['call_timing']['request_e2e']['count'],8)
            self.assertLessEqual(result['observed_peak_http'],options.concurrency)
            self.assertEqual(result['execution']['recorded_rows'],8)
            self.assertEqual(result['preparation_model_posts'],0)
            if arm=='fixed-map-semloom':
                self.assertFalse(any(result['core_cleanup']['final_core_usage'].values()))
            output=os.environ.get('SEMLOOM_FIXTURE_SUMMARY')
            if output:
                Path(output).write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':unittest.main()
