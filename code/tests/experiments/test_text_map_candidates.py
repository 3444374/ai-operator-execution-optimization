"""Native supply knobs and comparison roles cannot silently substitute ablations."""
import copy
from pathlib import Path
import tempfile
import unittest

from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.text_map_candidates import CAPACITIES,native_candidate,main_candidates
from src.experiments.postgresql.text_map_comparison import MAIN_ROLES,summarize,SCHEMA
from tests.experiments.test_text_map_comparison import recording,reference
from src.baselines.common.private_artifacts import write_private_json


class NativeCandidatesTests(unittest.TestCase):
    def test_ray_capacity_uses_async_batches_and_sufficient_reader_blocks(self):
        for capacity in CAPACITIES:
            cfg=QueryConfig(**native_candidate('ray-data',capacity,'inputs',512,ray_address='127.0.0.1:6379')['config'])
            self.assertEqual(cfg.ray_actors*cfg.ray_async_batches_per_actor,capacity)
            self.assertEqual(cfg.ray_batch_rows,1)
            self.assertGreaterEqual(cfg.ray_read_blocks,cfg.ray_actors)
            self.assertEqual(cfg.ray_num_cpus,8)
        with self.assertRaises(ValueError):native_candidate('ray-data',32,'inputs',512)

    def test_native_daft_is_separate_from_semloom_and_non_map_is_rejected(self):
        cfg=QueryConfig(**native_candidate('daft-native',64,'inputs',512)['config'])
        self.assertEqual(cfg.arm,'daft-native');self.assertIsNone(cfg.map_transport_config)
        self.assertEqual(cfg.daft_read_partitions,4)
        with self.assertRaises(ValueError):QueryConfig('bad','daft-native','movie-q3','inputs')
        with self.assertRaises(ValueError):QueryConfig('bad','daft-native','map','inputs',concurrency=1)

    def test_main_candidates_have_equal_opportunities_and_omit_local_ablation(self):
        candidates=main_candidates('inputs',512,ray_address='127.0.0.1:6379',
            transport_path='fixture.json',transport_sha256='a'*64)
        self.assertEqual(len(candidates),12)
        for role in MAIN_ROLES:
            self.assertEqual([v['config']['concurrency'] for v in candidates if v['role']==role],list(CAPACITIES))
        self.assertNotIn('pg-http',[v['role'] for v in candidates])

    def test_main_profile_requires_daft_and_does_not_accept_local_ablation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);candidates=[]
            for role in MAIN_ROLES:
                refs=[recording(root/(role+str(i)),role,role+str(i),i+1) for i in range(4)]
                candidates.append(dict(id=role,role=role,warmup=refs[:1],measured=refs[1:]))
            spec=dict(schema=SCHEMA,profile='main',stage='tuning',repeats=3,candidates=candidates)
            result=summarize(spec)
            self.assertEqual(set(result['selected']),set(MAIN_ROLES))
            self.assertNotIn('pg-http',result['selected'])
            bad=copy.deepcopy(spec);bad['candidates'].pop()
            with self.assertRaises(ValueError):summarize(bad)
            bad=copy.deepcopy(spec);bad['profile']='legacy-four-paths'
            with self.assertRaises(ValueError):summarize(bad)
