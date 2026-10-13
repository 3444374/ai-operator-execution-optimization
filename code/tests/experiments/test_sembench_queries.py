"""Data/identity/config rejection tests; no optional library or model runtime."""
from dataclasses import replace
from contextlib import ExitStack
from io import StringIO
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.baselines.common.private_artifacts import content_digest
from src.baselines.text.sembench_movie import verify_checkout, SEMBENCH_COMMIT
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.postgresql.sembench_queries import MovieDataset, MovieQuery, LOTUS_MOVIE_ARMS
from src.experiments.postgresql.sembench_query_cli import load_config, _env_path, arm_options
from src.experiments.postgresql.query_workloads import prepare, file_identity
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query


def fixture_dataset(root, checkout):
    examples = [('movie-row-'+str(i), 'taken_3' if i < 6 else 'another_movie',
                 'fixture-positive' if i not in (4, 7) else 'fixture-negative',
                 'NEGATIVE' if i in (3, 7) else 'POSITIVE',
                 'same-original-id' if i < 2 else 'review-'+str(i)) for i in range(8)]
    manifest = prepare(root, 'movie', iter(examples), {'source': 'synthetic SemBench software fixture'}, max_rows=8)
    return MovieDataset(checkout, root/'manifest.json', expected_manifest_digest=manifest['sha256'], role='software_fixture')


class MovieDataTests(unittest.TestCase):
    def test_selected_imlane_capacity_accepts_one_2000_row_query_and_rejects_2001(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            rows = [dict(row_id='movie-row-'+str(i), movie_id='taken_3' if i<120 else 'other') for i in range(2001)]
            for arm, mode in zip(LOTUS_MOVIE_ARMS[-2:], ('cpython','ray')):
                selected = IMLaneCallConfig(mode, '/tmp/bridge.json', 1, 1, '/tmp/build.json', max_batch_rows=2000)
                self.assertEqual(MovieQuery(dataset,1).check_executor_shape(arm,rows[:2000],imlane_config=selected),2000)
                with self.assertRaisesRegex(ValueError,'one 2001-row uncached batch'):
                    MovieQuery(dataset,1).check_executor_shape(arm,rows,imlane_config=selected)
                self.assertEqual(MovieQuery(dataset,2).check_executor_shape(arm,rows,imlane_config=selected),120)
                with self.assertRaisesRegex(ValueError,'matching ready-runtime'):
                    MovieQuery(dataset,1).check_executor_shape(arm,rows[:2000],imlane_config=replace(selected,runtime='ray' if mode=='cpython' else 'cpython'))

    def test_supplier_shape_check_receives_selected_config_before_budget_reservation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            selected = IMLaneCallConfig('cpython','/tmp/bridge.json',1,1,'/tmp/build.json',max_batch_rows=2000)
            ledger = Mock()
            cause = ValueError('synthetic stop before reservation')
            with patch.object(MovieDataset,'preflight',return_value={}), \
                 patch.object(MovieQuery,'check_executor_shape',side_effect=cause) as checking:
                with self.assertRaises(ValueError) as observed:
                    run_supplier_query(arm=LOTUS_MOVIE_ARMS[-2], plan=SemanticMapPlan('original', 'fixture', 8),
                        model=FixedModelConfig('http://localhost/v1/chat/completions','fixture',1000),
                        ledger=ledger,unit_id='fixture',root=root/'query',options=NativeGraphOptions(),
                        imlane_call_configs={'cpython':selected},sembench_query=MovieQuery(dataset,1))
            self.assertIs(observed.exception,cause)
            self.assertIs(checking.call_args.kwargs['imlane_config'],selected)
            ledger.reserve_unit.assert_not_called()

    def test_duplicate_public_review_ids_are_independent_occurrences_without_labels_in_query_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            rows = dataset.load_rows()
            self.assertEqual(len({r['row_id'] for r in rows}), 8)
            self.assertEqual(rows[0]['review_id'], rows[1]['review_id'])
            self.assertEqual(rows[0]['text'], rows[1]['text'])
            self.assertEqual(set(rows[0]), {'row_id', 'movie_id', 'review_id', 'text'})
            self.assertEqual([len(MovieQuery(dataset, q).model_rows(rows)) for q in (1, 2, 3)], [8, 6, 6])
            self.assertEqual(len(dataset.references(rows)), 8)

    def test_missing_hash_corrupt_file_and_different_label_occurrences_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            rows = dataset.load_rows()
            with self.assertRaisesRegex(ValueError, 'selected identity'):
                replace(dataset, expected_manifest_digest='0'*64).load_rows()
            raw = dataset.manifest.parent/'raw.jsonl'
            saved = raw.read_bytes()
            raw.write_bytes(saved+b'[]\n')
            with self.assertRaisesRegex(ValueError, 'file identity'):
                dataset.load_rows()
            raw.write_bytes(saved)
            rows[0]['review_id'] = 'another-public-id'
            with self.assertRaisesRegex(ValueError, 'label occurrence'):
                dataset.references(rows)

    def test_reordered_source_positions_fail_even_with_new_valid_fixture_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            raw = dataset.manifest.parent/'raw.jsonl'
            rows = raw.read_text().splitlines()
            rows[0], rows[1] = rows[1], rows[0]
            raw.write_text('\n'.join(rows)+'\n')
            manifest = json.loads(dataset.manifest.read_text())
            manifest['files']['raw.jsonl'] = file_identity(raw)
            manifest['sha256'] = content_digest({k: v for k, v in manifest.items() if k != 'sha256'})
            dataset.manifest.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'source position'):
                replace(dataset, expected_manifest_digest=manifest['sha256']).load_rows()

    def test_fixture_cannot_claim_selected_original_benchmark_role(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            with self.assertRaisesRegex(ValueError, 'selected CSV and manifest'):
                replace(dataset, role='original_movie_sf2000')
            for invalid in (0, 4, True, '1'):
                with self.assertRaises(ValueError):
                    MovieQuery(dataset, invalid)

    def test_imlane_rejects_original_large_batch_without_silent_splitting(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            rows = [dict(row_id='movie-row-'+str(i), movie_id='taken_3') for i in range(129)]
            for arm in LOTUS_MOVIE_ARMS[-2:]:
                with self.assertRaisesRegex(ValueError, 'one 129-row uncached batch'):
                    MovieQuery(dataset, 1).check_executor_shape(arm, rows)
                self.assertEqual(MovieQuery(dataset, 1).check_executor_shape(arm, rows[:128]), 128)
            self.assertEqual(MovieQuery(dataset, 1).check_executor_shape(LOTUS_MOVIE_ARMS[1], rows), 129)

    def test_git_trust_is_per_command_and_dirty_or_other_revision_is_rejected(self):
        with patch('src.baselines.text.sembench_movie.subprocess.check_output', side_effect=[SEMBENCH_COMMIT+'\n', '']) as read:
            root = verify_checkout('/tmp/original-sembench')
            for call in read.call_args_list:
                self.assertEqual(call.args[0][:5], ['git', '-c', 'safe.directory='+str(root), '-C', str(root)])
        for returned in [('0'*40, ''), (SEMBENCH_COMMIT, ' M src/query.py')]:
            with patch('src.baselines.text.sembench_movie.subprocess.check_output', side_effect=returned):
                with self.assertRaisesRegex(ValueError, 'pinned source'):
                    verify_checkout('/tmp/original-sembench')

    def test_no_generic_map_source_or_map_quality_can_enter_original_movie_query(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            query = MovieQuery(fixture_dataset(root/'data', root/'source'), 1)
            args = dict(arm=LOTUS_MOVIE_ARMS[0], plan=SemanticMapPlan('ignored by original method', 'fixture', 8),
                        model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions', 'fixture', 1000),
                        ledger=None, unit_id='fixture', root=root/'query', options=NativeGraphOptions(), sembench_query=query)
            for override in ({'load_source': lambda: iter(())}, {'allowed_outputs': ('POSITIVE',)},
                             {'reference_outputs': {}}, {'stages': []}):
                with self.subTest(override=tuple(override)), self.assertRaisesRegex(ValueError, 'selected relation'):
                    run_supplier_query(**args, **override)
            self.assertFalse((root/'query').exists())


class MovieConfigTests(unittest.TestCase):
    def test_readonly_checkout_can_be_git_while_private_paths_keep_rejecting_git(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            checkout=root/'sembench'
            subprocess.run(['git','init','--quiet',str(checkout)],check=True,capture_output=True)
            private=root/'model.json'
            private.write_text('{}')
            inside=checkout/'model.json'
            inside.write_text('{}')
            with patch.dict('os.environ',{'MOVIE_SOURCE':str(checkout),'MOVIE_MODEL':str(private),
                                          'MOVIE_INSIDE_MODEL':str(inside)}):
                self.assertEqual(_env_path('MOVIE_SOURCE',private=False),checkout.resolve())
                self.assertEqual(_env_path('MOVIE_MODEL'),private.resolve())
                with self.assertRaisesRegex(ValueError,'outside a Git checkout'):
                    _env_path('MOVIE_SOURCE')
                with self.assertRaisesRegex(ValueError,'outside a Git checkout'):
                    _env_path('MOVIE_INSIDE_MODEL')

    def test_cli_data_and_runtime_preflight_use_the_same_selected_capacity(self):
        from dataclasses import asdict
        from types import SimpleNamespace
        from src.experiments.postgresql import sembench_query_cli as cli
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data',root/'source')
            rows = [dict(row_id='movie-row-'+str(i),movie_id='other') for i in range(2000)]
            selected = IMLaneCallConfig('cpython',str(root/'bridge.json'),1,1,str(root/'build.json'),max_batch_rows=2000)
            config_path = root/'call.json'
            config_path.write_text(json.dumps(asdict(selected)))
            config = dict(dataset=dict(checkout_env='CHECKOUT',manifest_env='MANIFEST',csv_env='CSV',manifest_content_digest='0'*64),
                arms=[LOTUS_MOVIE_ARMS[-2]],queries=[1],warmups=0,repeats=1,max_attempts=2000,
                native_options={},model_config_env='MODEL',output_env='OUTPUT',imlane_config_envs={'cpython':'CALL'},
                query_timeout_s=1,owner_timeout_s=240)
            env = {name:str(root/name.lower()) for name in ('CHECKOUT','MANIFEST','CSV','MODEL','OUTPUT')}
            env['CALL'] = str(config_path)
            captured = []
            class Executor:
                def __init__(self, config, options):
                    captured.append(config)
                    self.max_batch_rows = config.max_batch_rows
                    self.delegate = SimpleNamespace(config={'bridge_sha256':'b201be8891493a2885ff9faae6d755bfe1eb7c20e10bdb2cda3f5f25c56cf5c6'})
                def close(self):
                    pass
            with ExitStack() as stack:
                stack.enter_context(patch.dict('os.environ',env,clear=True))
                stack.enter_context(patch.object(cli,'MovieDataset',return_value=dataset))
                stack.enter_context(patch.object(MovieDataset,'preflight',side_effect=lambda:{'versions':{}}))
                stack.enter_context(patch.object(MovieDataset,'load_rows',return_value=rows))
                model_loading=stack.enter_context(patch.object(cli,'load_fixed_model_config',return_value=FixedModelConfig('http://localhost/v1/chat/completions','fixture',1000)))
                stack.enter_context(patch('src.baselines.text.frameworks.imlane_calls.IMLaneCallExecutor',Executor))
                stack.enter_context(patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',return_value={}))
                ready=cli.preflight(config)
                self.assertEqual(captured[0].max_batch_rows,2000)
                self.assertEqual(ready[-1]['author_runtimes'][config['arms'][0]]['effective_max_batch_rows'],2000)
                output=StringIO()
                with patch.object(cli,'load_config',return_value=config),patch('sys.stdout',output):
                    cli.main(['--config','fixture.json','--data-preflight'])
                shape=json.loads(output.getvalue())
                self.assertTrue(shape['complete_matrix_ready'])
                self.assertEqual(shape['input_shape_coverage'][0]['configured_max_batch_rows'],2000)
                self.assertFalse(shape['execution_started'])
                value=asdict(selected);value.pop('max_batch_rows')
                config_path.write_text(json.dumps(value))
                model_loading.reset_mock();captured.clear()
                with self.assertRaisesRegex(ValueError,'at most 128 rows'):
                    cli.preflight(config)
                model_loading.assert_not_called()
                self.assertEqual(captured,[])
                self.assertFalse(Path(env['OUTPUT']).exists())
    def test_multiple_query_progress_files_are_distinct_and_final_matrix_is_written_once(self):
        from types import SimpleNamespace
        from src.experiments.postgresql.sembench_query_cli import run
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root/'data', root/'source')
            model = FixedModelConfig('http://127.0.0.1:1/v1/chat/completions', 'fixture', 1000)
            config = dict(arms=[LOTUS_MOVIE_ARMS[0]], queries=[1, 2, 3], warmups=0, repeats=1,
                          max_attempts=20, owner_timeout_s=240, query_timeout_s=20, native_options={})
            class Group:
                def __init__(self, *args, **kwargs):
                    pass
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    pass
                def run(self, arm, **kwargs):
                    return dict(status='passed', actual_posts=8 if kwargs['sembench_query'].query_id==1 else 6,
                                full_query_seconds=1, execution={'ready_query_seconds': .5}, quality={'fixture': True})
            ready = (dataset, model, NativeGraphOptions(), None, None, {}, root/'output', None, {'fixture': True})
            with patch('src.experiments.postgresql.sembench_query_cli.preflight', return_value=ready), \
                    patch('src.experiments.postgresql.sembench_query_cli.PersistentAdapterGroup', Group):
                value = run(config)
            self.assertEqual(value['status'], 'passed')
            self.assertEqual(len(list((root/'output').glob('matrix-progress-*.json'))), 3)
            self.assertEqual(len(json.loads((root/'output/matrix.json').read_text())['cells']), 3)

    def test_example_has_exact_three_queries_six_owners_and_matching_post_count(self):
        path = Path(__file__).resolve().parents[3]/'deploy/autodl/sembench_lotus_queries.example.json'
        config = load_config(path)
        self.assertEqual(tuple(config['arms']), LOTUS_MOVIE_ARMS)
        self.assertEqual(config['queries'], [1, 2, 3])
        self.assertEqual(config['max_attempts'], (2000+120+120)*6*4)
        from src.execution_provider.adapters.ray_map_transport import RayMapConfig
        RayMapConfig(**config['physical'])

    def test_missing_environment_and_unknown_config_fields_do_not_fall_back(self):
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaisesRegex(ValueError, 'required SemBench path'):
                _env_path('SEMBENCH_MANIFEST')
        path = Path(__file__).resolve().parents[3]/'deploy/autodl/sembench_lotus_queries.example.json'
        config = load_config(path)
        config['map_instruction'] = 'unrequested replacement'
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary)/'config.json'
            target.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, 'unsupported fields'):
                load_config(target)

    def test_native_lower_concurrency_remains_per_executor_and_unknown_arm_is_rejected(self):
        config = dict(arms=list(LOTUS_MOVIE_ARMS), native_options={'concurrency': 4},
                      arm_native_options={LOTUS_MOVIE_ARMS[-1]: {'concurrency': 6}})
        for arm in LOTUS_MOVIE_ARMS[:-1]:
            self.assertEqual(arm_options(config, arm).concurrency, 4)
        self.assertEqual(arm_options(config, LOTUS_MOVIE_ARMS[-1]).concurrency, 6)
        config['arm_native_options']['another-executor'] = {'concurrency': 8}
        with self.assertRaisesRegex(ValueError, 'explicitly selected arms'):
            arm_options(config, LOTUS_MOVIE_ARMS[0])


if __name__ == '__main__':
    unittest.main()
