"""Medical authored text assets and original query/evaluator bindings, CPU only."""

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.baselines.text import sembench_medical as medical
from src.baselines.text import sembench_source
from src.experiments import sembench_text_query as entry
from src.serving.backends.vllm_protocol import GenerationSpec


class MedicalTextInputsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='medical-text-inputs-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / 'authored data; literal path'
        self.source.mkdir()
        self.output = self.root / 'text output'
        self.frames = {
            'text_symptoms_data': pd.DataFrame(dict(patient_id=[7, 3],
                symptoms=['A quoted "symptom", with\na newline', None], symptom_id=[1, 2])),
            'patient_data': pd.DataFrame(dict(patient_id=[3, 7, 9], age=[30, 50, 70])),
            'patient_data_with_labels': pd.DataFrame(dict(patient_id=[3, 7, 9],
                age=[30, 50, 70], text_diagnosis=['acne', 'allergy', 'none'])),
        }
        for name, frame in self.frames.items():
            frame.to_csv(self.source / (name + '.csv'), index=False)

    def test_preparation_copies_only_three_exact_authored_csvs_without_setup(self):
        original = {path.name: path.read_bytes() for path in self.source.iterdir()}
        with patch.object(medical, 'verify_checkout') as verify, patch.object(medical, 'load_module') as load:
            report = medical.prepare_text_inputs('checkout', self.source, self.output)
        verify.assert_called_once_with('checkout')
        load.assert_not_called()
        self.assertEqual(original, {path.name: path.read_bytes() for path in self.source.iterdir()})
        self.assertEqual(set(report['files']), set(medical.text_files()))
        self.assertEqual(report['scale_factor'], 11112)
        self.assertEqual(report['model_calls'], 0)
        self.assertFalse(report['downloaded'])
        self.assertFalse(report['images_generated'])
        self.assertFalse(report['audio_generated'])
        self.assertEqual(set(self.output.iterdir()), {self.output / 'data', self.output / 'manifest.json'})
        for name, identity in report['files'].items():
            payload = (self.output / name).read_bytes()
            self.assertEqual(payload, original[Path(name).name])
            self.assertEqual(identity['sha256'], hashlib.sha256(payload).hexdigest())
            self.assertEqual(identity['bytes'], len(payload))
            self.assertEqual(identity['rows'], len(self.frames[Path(name).stem]))
            self.assertEqual(stat.S_IMODE((self.output / name).stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.output / 'data').stat().st_mode), 0o700)
        self.assertEqual(json.loads((self.output / 'manifest.json').read_text()), report)

    def test_existing_scale_copies_its_authored_suffix_without_resampling(self):
        for name, frame in self.frames.items():
            frame.iloc[:1].to_csv(self.source / (name + '_25.csv'), index=False)
        with patch.object(medical, 'verify_checkout'):
            report = medical.prepare_text_inputs('checkout', self.source, self.output, 25)
        self.assertTrue(all(name.endswith('_25.csv') for name in report['files']))
        self.assertEqual(report['files']['data/text_symptoms_data_25.csv']['rows'], 1)
        self.assertEqual(report['files']['data/patient_data_25.csv']['rows'], 1)

    def test_missing_asset_or_missing_requested_scale_does_not_create_output(self):
        with patch.object(medical, 'verify_checkout'):
            with self.assertRaises(FileNotFoundError):
                medical.prepare_text_inputs('checkout', self.source, self.output, 25)
            self.assertFalse(self.output.exists())
            (self.source / 'patient_data_with_labels.csv').unlink()
            with self.assertRaises(FileNotFoundError):
                medical.prepare_text_inputs('checkout', self.source, self.output)
        self.assertFalse(self.output.exists())

    def test_fresh_output_preserves_previous_evidence(self):
        self.output.mkdir()
        sentinel = self.output / 'sentinel'
        sentinel.write_text('retain authored inputs')
        with patch.object(medical, 'verify_checkout'), self.assertRaises(FileExistsError):
            medical.prepare_text_inputs('checkout', self.source, self.output)
        self.assertEqual(sentinel.read_text(), 'retain authored inputs')

    def test_invalid_scale_fails_before_loading_source_or_creating_output(self):
        with patch.object(medical, 'verify_checkout') as verify:
            for value in (0, True, '25', 25.0):
                with self.subTest(scale=value), self.assertRaises(ValueError):
                    medical.prepare_text_inputs('checkout', self.source, self.output, value)
        verify.assert_not_called()
        self.assertFalse(self.output.exists())


@dataclass
class RetrievalMetric:
    precision: float
    recall: float
    f1_score: float


class MedicalOriginalBindingTests(unittest.TestCase):
    def test_dispatch_keeps_original_run_arguments_and_local_csv_binding(self):
        root = Path('/tmp/original-medical-inputs').resolve()
        marker = object()
        loader = Mock(return_value=marker)
        calls = []
        for query in medical.TEXT_QUERY_IDS + medical.VISUAL_QUERY_IDS:
            def run(data_dir, scale_factor):
                calls.append((data_dir, scale_factor))
                return module.pd.read_csv(Path(data_dir) / 'data/text_symptoms_data_25.csv')

            module = SimpleNamespace(run=run, pd=pd)
            with patch.object(medical, 'load_module', return_value=module) as load:
                result = medical.run_original_query('checkout', 'Q' + query, loader,
                    data_dir=root, scale_factor=25)
            self.assertIs(result, marker)
            load.assert_called_once_with('checkout', 'sembench_medical_q' + query,
                                         'files/medical/query/lotus/Q' + query + '.py')
            self.assertIs(module.pd.DataFrame, pd.DataFrame)
        self.assertEqual(calls, [(str(root), 25)] * 6)
        self.assertEqual(loader.call_count, 6)
        loader.assert_called_with('data/text_symptoms_data_25.csv')

    def test_unsupported_queries_fail_before_original_setup(self):
        with patch.object(medical, 'load_module') as load:
            for value in (2, '5', '6', '7', '', True, 1.0):
                with self.subTest(query=value), self.assertRaises(ValueError):
                    medical.run_original_query('checkout', value, Mock(), data_dir='/tmp')
        load.assert_not_called()

    def test_evaluation_binds_only_required_relations_and_preserves_q1_series(self):
        labels = pd.DataFrame(dict(patient_id=[7], text_diagnosis=['allergy']))
        symptoms = pd.DataFrame(dict(patient_id=[7], symptoms=['fixture']))
        result = pd.Series([7], index=pd.Index([9], name='source'), name='patient_id')
        original = result.copy(deep=True)
        calls = []

        class Evaluator:
            def __init__(self):
                raise AssertionError('original constructor must not read unrelated modality CSVs')

            def _get_ground_truth(self, query):
                self_test.assertIs(self.patient_df, labels)
                self_test.assertIs(self.symptoms_text_df, symptoms)
                self_test.assertEqual(self.scale_factor, 25)
                self_test.assertFalse(hasattr(self, 'audio_df'))
                self_test.assertTrue(self._results_path.is_dir())
                calls.append((query, self._results_path))
                return self.patient_df

            def _evaluate_single_query(self, query, frame, truth):
                self_test.assertEqual(query, 1)
                pd.testing.assert_frame_equal(frame, original.to_frame())
                self_test.assertIs(truth, labels)
                return RetrievalMetric(1, 1, 1)

        self_test = self
        with patch.object(medical, 'load_module', return_value=SimpleNamespace(MedicalEvaluator=Evaluator)):
            report = medical.evaluate_original('checkout', 1, result, labels, symptoms, scale_factor=25)
        self.assertEqual(report['original_metric'], dict(precision=1, recall=1, f1_score=1))
        self.assertEqual(report['sembench_commit'], medical.SEMBENCH_COMMIT)
        self.assertEqual(calls[0][0], 1)
        self.assertFalse(calls[0][1].exists())
        pd.testing.assert_series_equal(result, original)

    def test_medical_query_source_is_in_existing_pinned_checkout_validation(self):
        with patch.object(sembench_source.subprocess, 'check_output',
                          side_effect=[medical.SEMBENCH_COMMIT, ' M files/medical/query/lotus/Q1.py']):
            with self.assertRaisesRegex(ValueError, 'pinned source'):
                sembench_source.verify_checkout('/tmp/medical-source')

    def test_config_accepts_medical_patient_scale_and_passes_it_to_query(self):
        config = dict(schema='semloom.sembench_text_run.v1', scenario='medical', query='10',
            query_id='medical-q10', backend='lotus-native', model_name='fixture', scale_factor=25,
            generation={}, engine=dict(model_path='/tmp/model', model_python='/tmp/python',
                driver_python='/tmp/python', max_model_calls=1), resources={},
            profile=dict(native_lm_max_batch_size=2))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'config.json'
            path.write_text(json.dumps(config))
            value, _, generation, _ = entry.configuration(path)
        self.assertEqual(value, config)
        with patch('src.baselines.text.sembench_text.SemBenchTextQuery') as query:
            entry.query_from_config(config, 'checkout', 'data', generation)
        query.assert_called_once_with('checkout', 'data', 'medical', '10',
            execution_id='medical-q10', model_name='fixture',
            generation=GenerationSpec(max_tokens=8192, stop=()), scale_factor=25, ranking=None)

    def test_original_query_generation_defaults_and_explicit_overrides(self):
        config = dict(schema='semloom.sembench_text_run.v1', scenario='medical', query='10',
            query_id='medical-q10', backend='lotus-native', model_name='fixture', generation={},
            engine=dict(model_path='/tmp/model', model_python='/tmp/python', driver_python='/tmp/python',
                        max_model_calls=1200), resources={}, profile=dict(native_lm_max_batch_size=2))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'config.json'
            for supplied, expected_tokens, expected_stop in (({}, 8192, ()),
                    ({'max_tokens': 64}, 64, ()), ({'stop': ['DONE']}, 8192, ('DONE',)),
                    ({'max_tokens': 32, 'stop': ['\n'], 'temperature': 0.25}, 32, ('\n',))):
                with self.subTest(generation=supplied):
                    config['generation'] = supplied
                    path.write_text(json.dumps(config))
                    _, _, generation, _ = entry.configuration(path)
                    self.assertEqual(generation.max_tokens, expected_tokens)
                    self.assertEqual(generation.stop, expected_stop)
                    self.assertEqual(generation.temperature, supplied.get('temperature', 0.0))
            for scenario in ('movie', 'mmqa'):
                config['scenario'] = scenario
                for supplied, expected in (({}, GenerationSpec(max_tokens=8192, stop=())),
                        ({'max_tokens': 8, 'stop': ['\n']}, GenerationSpec())):
                    with self.subTest(scenario=scenario, generation=supplied):
                        config['generation'] = supplied
                        path.write_text(json.dumps(config))
                        _, _, generation, _ = entry.configuration(path)
                        self.assertEqual(generation, expected)


if __name__ == '__main__':
    unittest.main()
