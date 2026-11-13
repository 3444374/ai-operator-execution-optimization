"""Neutral Animals task parity with fixed original queries and software replies."""

import base64
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from src.baselines.image import sembench_animals as original
from src.baselines.image.sembench_native_animals import AnimalsFilterTask
from src.infrastructure.private_artifacts import content_digest
from tests.baselines.image import test_sembench_animals_original as fixture


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK, 'requires pinned source and original SDK/evaluator fixtures')
class NativeAnimalsFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.OriginalAnimalsImageTests.setUpClass()

    def setUp(self):
        self.reference = fixture.OriginalAnimalsImageTests()
        self.addCleanup(self.reference.doCleanups)
        self.reference.setUp()
        self.dataset = self.reference.dataset

    def task(self, query='Q1', *, dataset=None):
        data = self.dataset if dataset is None else dataset
        return AnimalsFilterTask(data.checkout, data.manifest, data.expected_digest,
                                 query=query, execution_id='independent-images')

    def replies(self, requests, flags):
        return [dict(call_id=row['call_id'], generated_text=json.dumps(dict(keep=flag)))
                for row, flag in zip(requests, flags, strict=True)]

    def assert_quality_equal(self, actual, expected):
        self.assertEqual(actual['original_metric'], expected['original_metric'])
        self.assertEqual(actual['predicted'], expected['predicted'])
        self.assertEqual(actual['sembench_commit'], expected['sembench_commit'])
        # Original Q10 SQL has no ORDER BY for its tied gold rows.
        if isinstance(actual['ground_truth'], list):
            canonical = lambda rows: sorted(json.dumps(row, sort_keys=True) for row in rows)
            self.assertEqual(canonical(actual['ground_truth']), canonical(expected['ground_truth']))
        else:
            self.assertEqual(actual['ground_truth'], expected['ground_truth'])

    def test_all_three_original_queries_keep_complete_sinks_scores_and_RGB_PNG_input(self):
        for query in (1, 3, 10):
            with self.subTest(query=query):
                task = self.task(query)
                evaluation = task.prepare_evaluation()
                with patch.object(original, 'image_identity', side_effect=AssertionError('query repeated pixel preflight')):
                    source = task.read_source()
                    requests = task.requests(source)
                result = task.finish(source, list(reversed(self.replies(requests, [True, True, False, False]))))
                expected, quality, batches = self.reference.run_query(query, ['True', 'True', 'False', 'False'])
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assert_quality_equal(task.evaluate(source, result, evaluation=evaluation), quality)
                task.verify_source(source)
                self.assertEqual(list(source.image_uris), self.reference.image_uris(batches[0]))
                self.assertEqual(task.model_request_count(source), 4)
                self.assertEqual(task.describe(source)['expected_model_calls'], 4)
                self.assertEqual(task.response_format, {'type': 'json_object'})
                self.assertNotIn('Species', source.frame.columns)
                self.assertNotIn('REFERENCE_ONLY_', json.dumps(requests))
                self.assertNotIn('CityA', json.dumps(requests))
                self.assertNotEqual(requests[0]['messages'], batches[0][0])
                self.assertFalse(task.describe(source)['lotus_sdk_required'])

    def test_repeated_images_remain_distinct_ordered_occurrences_without_a_size_cap(self):
        dataset = self.reference.make_dataset(count=65)
        task = self.task('Q1', dataset=dataset)
        task.prepare_evaluation()
        source = task.read_source()
        requests = task.requests(source)
        self.assertEqual(len(requests), 65)
        self.assertEqual(len({row['call_id'] for row in requests}), 65)
        self.assertEqual(source.image_uris[0], source.image_uris[4])
        self.assertEqual(source.frame.row_id.tolist(), ['animals-row-' + str(i) for i in range(65)])
        self.assertEqual(task.finish(source, self.replies(requests, [True] * 65)).iloc[0, 0], 65)
        self.assertEqual(task.describe(source)['source']['role'], 'software_fixture')

    def test_q7_two_full_filters_match_original_messages_intersection_and_score(self):
        dataset = self.reference.make_dataset(count=7)
        task = self.task('Q7', dataset=dataset)
        evaluation = task.prepare_evaluation()
        source = task.read_source()
        requests = task.requests(source)
        flags = [True, True, False, False, True, False, False,
                 False, False, True, True, False, True, False]
        result = task.finish(source, list(reversed(self.replies(requests, flags))))
        expected, quality, batches = self.reference.run_query(7,
            ['True' if keep else 'False' for keep in flags], dataset=dataset)
        self.assertEqual([len(batch) for batch in batches], [7, 7])
        self.assertEqual(task.encode_result(result), task.encode_result(expected))
        self.assertEqual(result.city.tolist(), ['CityA', 'CityB'])
        self.assert_quality_equal(task.evaluate(source, result, evaluation=evaluation), quality)
        self.assertEqual(list(source.image_uris[:7]), self.reference.image_uris(batches[0]))
        self.assertEqual(list(source.image_uris[7:]), self.reference.image_uris(batches[1]))
        self.assertEqual(source.image_uris[0], source.image_uris[4])
        self.assertEqual(len(source.frame), 7)
        self.assertEqual(task.model_request_count(source), 14)
        self.assertEqual(len({row['call_id'] for row in requests}), 14)
        self.assertTrue(all('zebra' in row['messages'][0]['content'] for row in requests[:7]))
        self.assertTrue(all('impala' in row['messages'][0]['content'] for row in requests[7:]))
        self.assertEqual(task.describe(source)['source']['rows'], 7)
        self.assertEqual(task.describe(source)['expected_model_calls'], 14)
        self.assertEqual(task.describe(source)['input_modality'], 'image')
        self.assertIn('not LOTUS sequential LM batches', task.describe(source)['execution_shape'])
        task.verify_source(source)

    def test_q7_empty_first_filter_still_requires_both_complete_input_passes(self):
        task = self.task('Q7')
        evaluation = task.prepare_evaluation()
        source = task.read_source()
        requests = task.requests(source)
        self.assertEqual(len(requests), 8)
        for flags in ([False] * 4 + [True] * 4,
                [True, False, True, False] + [False, True, False, True]):
            with self.subTest(flags=flags):
                result = task.finish(source, self.replies(requests, flags))
                expected, quality, batches = self.reference.run_query(7,
                    ['True' if keep else 'False' for keep in flags])
                self.assertEqual([len(batch) for batch in batches], [4, 4])
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assertTrue(result.empty)
                self.assertEqual(list(result.columns), ['city'])
                self.assert_quality_equal(task.evaluate(source, result, evaluation=evaluation), quality)
        with self.assertRaisesRegex(ValueError, 'cover every'):
            task.finish(source, self.replies(requests[:4], flags[:4]))

    def test_q7_really_reads_both_image_passes_and_audits_each_consumed_snapshot(self):
        task = self.task('Q7')
        task.prepare_evaluation()
        path = Path(self.dataset.read()['rows'][0]['ImagePath'])
        original_bytes = path.read_bytes()
        read_bytes = Path.read_bytes
        for changed_pass in (1, 2):
            counts = [0]
            def fresh_read(current):
                if current != path:
                    return read_bytes(current)
                counts[0] += 1
                if counts[0] == changed_pass:
                    self.reference.Image.new('RGB', (5, 3), 'black').save(path)
                    try:
                        return read_bytes(current)
                    finally:
                        path.write_bytes(original_bytes)
                return read_bytes(current)
            with self.subTest(changed_pass=changed_pass):
                with patch.object(Path, 'read_bytes', new=fresh_read), \
                        patch.object(original, 'image_identity', side_effect=AssertionError('query repeated audit')):
                    source = task.read_source()
                self.assertEqual(counts[0], 2)
                self.assertEqual(len(source.image_bytes), 8)
                self.assertNotEqual(source.image_bytes[0], source.image_bytes[4])
                self.assertNotEqual(source.image_uris[0], source.image_uris[4])
                with self.assertRaisesRegex(ValueError, 'actual Animals image bytes'):
                    task.verify_source(source)

    def test_empty_survivors_follow_original_count_and_empty_group_schemas(self):
        dataset = self.reference.make_dataset(no_zebras=True)
        for query in (1, 3, 10):
            with self.subTest(query=query):
                task = self.task(query, dataset=dataset)
                evaluation = task.prepare_evaluation()
                source = task.read_source()
                result = task.finish(source, self.replies(task.requests(source), [False] * 4))
                expected, quality, _ = self.reference.run_query(query, ['False'] * 4, dataset=dataset)
                self.assertEqual(task.encode_result(result), task.encode_result(expected))
                self.assert_quality_equal(task.evaluate(source, result, evaluation=evaluation), quality)

    def test_missing_duplicate_unknown_and_non_boolean_predictions_fail(self):
        task = self.task()
        task.prepare_evaluation()
        source = task.read_source()
        replies = self.replies(task.requests(source), [True] * 4)
        for values in (replies[:-1], replies + replies[:1],
                [dict(replies[0], call_id='unknown'), *replies[1:]]):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, 'occurrence|cover every'):
                task.finish(source, values)
        for text in ('True', '{"keep": 1}', '{"keep": "true"}', '{"keep": true, "keep": false}',
                '{"keep": true, "extra": 0}', '{}'):
            with self.subTest(text=text), self.assertRaises((ValueError, json.JSONDecodeError)):
                task.finish(source, [dict(replies[0], generated_text=text), *replies[1:]])

    def test_preparation_does_not_replace_fresh_query_image_reads(self):
        task = self.task()
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('construction read source')):
            self.task()
        with self.assertRaisesRegex(ValueError, 'prepare_evaluation'):
            task.read_source()
        evaluation = task.prepare_evaluation()
        first = task.read_source()
        path = Path(first.frame.iloc[0].ImagePath)
        old = path.read_bytes()
        self.reference.Image.new('RGBA', (9, 7), (1, 2, 3, 4)).save(path)
        fresh = task.read_source()
        self.assertNotEqual(first.image_uris[0], fresh.image_uris[0])
        with self.reference.Image.open(BytesIO(base64.b64decode(fresh.image_uris[0].split(',', 1)[1]))) as image:
            self.assertEqual((image.mode, image.size), ('RGB', (9, 7)))
        path.write_bytes(old)
        # Restoring the path cannot hide bytes actually consumed by this query.
        with self.assertRaisesRegex(ValueError, 'actual Animals image bytes'):
            task.verify_source(fresh)
        task.verify_source(first)
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            task.read_source()
        self.assertIn('Species', evaluation.frame.columns)

    def test_manifest_changes_and_invalid_manifest_roles_are_not_empty_success(self):
        task = self.task()
        task.prepare_evaluation()
        value = json.loads(Path(self.dataset.manifest).read_text())
        value['rows'][0]['City'] = 'changed'
        Path(self.dataset.manifest).write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'manifest changed'):
            task.read_source()
        for rows, role in (([], 'software_fixture'), (value['rows'], 'official_animals_sf64')):
            value.update(rows=rows, role=role)
            value['sha256'] = content_digest({k: v for k, v in value.items() if k != 'sha256'})
            Path(self.dataset.manifest).write_text(json.dumps(value))
            other = AnimalsFilterTask(self.dataset.checkout, self.dataset.manifest, value['sha256'])
            with self.assertRaisesRegex(ValueError, 'nonempty|complete original catalog'):
                other.prepare_evaluation()
        with self.assertRaisesRegex(ValueError, 'single-image'):
            self.task('Q2')

    def test_frozen_reference_is_scored_without_later_manifest_label_reads(self):
        task = self.task('Q10')
        evaluation = task.prepare_evaluation()
        source = task.read_source()
        result = task.finish(source, self.replies(task.requests(source), [True, True, False, False]))
        expected = task.evaluate(source, result, evaluation=evaluation)
        with patch.object(original.AnimalsDataset, 'read', side_effect=AssertionError('scoring reread data')):
            self.assert_quality_equal(task.evaluate(source, result, evaluation=evaluation), expected)
        self.assertEqual(expected['original_metric']['f1_score'], 1)

    def test_fresh_process_executes_and_scores_without_LOTUS_or_backend_imports(self):
        code = r'''
import importlib.abc, json, socket, sys
class Deny(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        names = ('lotus', 'litellm', 'vllm', 'ray', 'daft', 'src.semantic_methods.lotus',
                 'src.baselines.text.sembench_text', 'src.execution_provider')
        if any(fullname == name or fullname.startswith(name + '.') for name in names):
            raise AssertionError('forbidden dependency: ' + fullname)
sys.meta_path.insert(0, Deny())
connect = socket.socket.connect
def offline(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        raise AssertionError('external network access')
    return connect(sock, address)
socket.socket.connect = offline
from src.baselines.image.sembench_native_animals import AnimalsFilterTask
task = AnimalsFilterTask(sys.argv[1], sys.argv[2], sys.argv[3])
evaluation = task.prepare_evaluation()
source = task.read_source()
requests = task.requests(source)
replies = [dict(call_id=row['call_id'], generated_text=json.dumps({'keep': i < 2}))
           for i, row in enumerate(requests)]
result = task.finish(source, replies)
quality = task.evaluate(source, result, evaluation=evaluation)
task.verify_source(source)
assert task.encode_result(result)['data'] == [[2]]
assert quality['predicted'] == quality['ground_truth'] == 2
assert not any(name == 'lotus' or name.startswith('lotus.') for name in sys.modules)
print(json.dumps(dict(status='passed', requests=len(requests), complete_count=2, model_calls=0)))
'''
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
            PYTHONPATH=str(Path(__file__).resolve().parents[3]))
        result = subprocess.run([sys.executable, '-B', '-c', code, str(self.dataset.checkout),
            str(self.dataset.manifest), self.dataset.expected_digest], capture_output=True, text=True,
            timeout=30, env=env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(json.loads(result.stdout)['complete_count'], 2)


if __name__ == '__main__':
    unittest.main()
