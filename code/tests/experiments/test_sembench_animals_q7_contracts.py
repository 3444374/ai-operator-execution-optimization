"""Q7 counter/acceptance contracts; full CPU HTTP qualification is separate."""

from pathlib import Path
from types import SimpleNamespace
import unittest

from src.experiments.sembench_animals import run_query


class Q7ContractsTests(unittest.TestCase):
    def test_profile_mismatch_rejected_before_source_use(self):
        dataset=SimpleNamespace(preflight=lambda:self.fail('profile must reject before source use'))
        with self.assertRaisesRegex(ValueError,'profile'):
            run_query(dataset,SimpleNamespace(model_id='fixed'),arm='lotus-native',output=Path('unused'),
                      tokenizer_path=None,observation_mode='performance',query_id=7,
                      media_profile=dict(model=dict(id='fixed',revision='r1'),
                          generation=dict(temperature=0,top_p=1,max_tokens=32,retries=0),
                          service_preprocess=dict(processor_revision='r1',parameters={'resize':'none'})))

    def test_q1_default_cannot_silently_enable_retention(self):
        dataset=SimpleNamespace(preflight=lambda:self.fail('option must reject before source use'))
        with self.assertRaisesRegex(ValueError,'explicit Q7'):
            run_query(dataset,None,arm='lotus-native',output=None,tokenizer_path=None,
                      observation_mode='performance',media_retention_bytes=1)


if __name__=='__main__': unittest.main()
