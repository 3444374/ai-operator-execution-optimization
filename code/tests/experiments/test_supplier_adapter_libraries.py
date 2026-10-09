"""Opt-in delivered supplier query entries, always using a local fixture."""
import importlib.util
import base64
import hashlib
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
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
from tests.experiments.test_native_adapter_query import fixture_server


@unittest.skipUnless(os.environ.get('SEMLOOM_SUPPLIER_ACTUAL_ARM'), 'requires an explicitly selected supplier library fixture')
class SupplierAdapterLibraryTests(unittest.TestCase):
    def test_supplier_query_entry_records_actual_calls_and_method_receipts(self):
        arm=os.environ['SEMLOOM_SUPPLIER_ACTUAL_ARM']
        core='semloom' in arm
        chain=arm.startswith('lotus-two-map')
        count=4*(2 if chain else 1)
        root=Path(tempfile.mkdtemp(dir=os.environ.get('SEMLOOM_FIXTURE_ARTIFACT_ROOT')))
        with fixture_server(delay=.01,content='"ok"' if arm.startswith('sema-') else 'ok') as (endpoint,requests):
            budget=AttemptBudget('supplier-fixture',count)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',budget,deadline_utc=time.time()+180)
            result=run_supplier_query(arm,
                load_source=lambda:iter(dict(row_id=str(i),text='same fixture text') for i in range(4)),
                plan=SemanticMapPlan('Return ok.','fixture',16),model=FixedModelConfig(endpoint,'fixture',5000),
                ledger=ledger,unit_id='supplier-fixture',root=root/'query',
                options=NativeGraphOptions(num_threads=4),
                physical=RayMapConfig('resolved',2,2,2097152,8388608) if core else None,
                ray_temp_root=Path('/root/autodl-tmp/rsni.'+uuid.uuid4().hex[:8]) if core else None,
                query_timeout_s=120,
                stages=[dict(instruction='Summarize {text}.',output_column='summary'),
                        dict(instruction='Classify {summary}.',output_column='label')] if chain else None,
                sema_binary=Path(os.environ['SEMLOOM_SEMA_BINARY']) if arm.startswith('sema-') else None,
                duckdb_library=Path(os.environ['SEMLOOM_DUCKDB_LIBRARY']) if arm.startswith('duckdb-') else None,
                reference_outputs={str(i):'ok' for i in range(4)},allowed_outputs=('ok',))
            self.assertEqual(result['status'],'passed')
            self.assertEqual(result['actual_posts'],count)
            self.assertEqual(len(requests),count)
            self.assertEqual(result['execution']['recorded_rows'],4)
            bodies=[json.loads(line) for line in (root/'query/upstream-response-bodies.jsonl').read_text().splitlines()]
            self.assertEqual(len(bodies),count)
            for body in bodies:
                raw=base64.b64decode(body['response_body_base64'],validate=True)
                self.assertEqual(hashlib.sha256(raw).hexdigest(),body['response_body_sha256'])
                self.assertEqual(json.loads(raw)['choices'][0]['message']['content'],'"ok"' if arm.startswith('sema-') else 'ok')
            if arm.startswith('lotus-'):
                self.assertEqual(result['call_timing']['request_e2e']['count'],count)
                if chain:self.assertEqual(len(result['call_timing']['successor_waits']),4)
            output=os.environ.get('SEMLOOM_FIXTURE_SUMMARY')
            if output:Path(output).write_text(json.dumps(result,indent=2)+'\n')


if __name__=='__main__':unittest.main()
