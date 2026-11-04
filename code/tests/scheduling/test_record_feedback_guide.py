"""The linked current guide selects the actual public policy with valid fields."""
import json
from pathlib import Path
import re
import sys
import unittest

from src.execution_provider.adapters.records_profile import resolve_core_profile
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec


class FeedbackGuideTests(unittest.TestCase):
    def test_current_guide_example_parses_on_both_public_paths_without_legacy_hints(self):
        path=Path(__file__).resolve().parents[2]/'src/scheduling/runtime/calibration.README.md'
        section=path.read_text().split('<a id="resource-request-public-current"></a>',1)[1]
        example=json.loads(re.search(r'```json\n(.*?)\n```',section,re.S).group(1))
        engine=EngineSpec('/tmp/guide-model',sys.executable,sys.executable)
        resources=RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=4)
        for rolling in (False,True):
            result=resolve_core_profile(engine,resources,example,rolling_refill=rolling)
            self.assertEqual((result['max_inflight'],result['owner_max_inflight']),(1,4))
            self.assertEqual(result['admission']['kind'],'resource_feedback')
            self.assertNotIn('search_mode',result['admission'])
            self.assertNotIn('search_policy',result['admission'])
        self.assertIn('不接受跨查询',section)
        self.assertIn('legacy_resource_feedback',section)


if __name__=='__main__':unittest.main()
