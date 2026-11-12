"""Medical relation corner cases and duplicate-message receipt identity."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.experiments.native_imlane_sembench import audit_ipc
from src.experiments.native_imlane_relation import finish_mmqa_filters
from tests.experiments.test_native_imlane_relation import CHECKOUT


class OccurrenceAuditTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        messages = [{'role': 'user', 'content': 'exact repeated original message'}]
        self.requests = [dict(call_id=i, messages=messages) for i in ('source-a', 'source-b')]
        self.predictions = [dict(call_id='source-a', generated_text='True'),
                            dict(call_id='source-b', generated_text='False')]
        self.sampling = {'max_tokens': 8192}
        self.receipt = dict(status='succeeded', client_closed=True,
            request=dict(sampling_params=self.sampling, messages_batch=[messages, messages]), calls=[])
        for position, (name, text) in enumerate((('source-b', 'False'), ('source-a', 'True'))):
            self.receipt['calls'].append(dict(occurrence_id=name, position=position, status='succeeded',
                attempted=True, requested_payload=dict(messages=messages), response=dict(
                    position=position, generated_text=text, rendered_prompt_token_ids=[1,2],
                    requested_max_tokens=8192, resolved_max_tokens=8192,
                    budget_attempt=position+1, request_id='request-' + str(position))))
        (self.root / 'one.started.json').write_text('{}')

    def audit(self, receipt):
        (self.root / 'one.terminal.json').write_text(json.dumps(receipt))
        return audit_ipc(self.root, self.requests, [[1,2], [1,2]], self.predictions, self.sampling, by_occurrence=True)

    def test_identical_messages_with_different_answers_follow_occurrence_not_arrival(self):
        result = self.audit(self.receipt)
        self.assertEqual([row['call_id'] for row in result['calls']], ['source-b', 'source-a'])
        self.assertEqual([row['response']['generated_text'] for row in result['calls']], ['False', 'True'])

    def test_missing_duplicate_foreign_positions_messages_tokens_and_swapped_answers_fail(self):
        mutations = [lambda r: r['calls'][0].pop('occurrence_id'),
            lambda r: r['calls'][0].update(occurrence_id='source-a'),
            lambda r: r['calls'][0].update(occurrence_id='foreign'),
            lambda r: r['calls'][0].update(position=1),
            lambda r: r['calls'][0]['response'].update(position=1),
            lambda r: r['calls'][0]['requested_payload'].update(messages=[]),
            lambda r: r['calls'][0]['response'].update(rendered_prompt_token_ids=[2,1]),
            lambda r: r['calls'][0]['response'].update(generated_text='True')]
        for mutate in mutations:
            receipt = deepcopy(self.receipt); mutate(receipt)
            with self.subTest(receipt=receipt), self.assertRaises(ValueError): self.audit(receipt)

    def test_missing_terminal_receipt_fails(self):
        with self.assertRaisesRegex(ValueError, 'receipt'):
            audit_ipc(self.root, self.requests, [[1,2], [1,2]], self.predictions, self.sampling, by_occurrence=True)

    def test_audit_uses_actual_declared_generation_and_rejects_silent_clamping(self):
        self.sampling['max_tokens'] = 32
        for call in self.receipt['calls']:
            call['response'].update(requested_max_tokens=32, resolved_max_tokens=32)
        self.assertEqual(self.audit(self.receipt)['native_generate_terminal_count'], 2)
        self.receipt['calls'][0]['response']['resolved_max_tokens'] = 16
        with self.assertRaisesRegex(ValueError, 'tokens'):
            self.audit(self.receipt)


@unittest.skipUnless(CHECKOUT, 'set SEMLOOM_NATIVE_TEST_CHECKOUT to the pinned source')
class MedicalRelationTests(unittest.TestCase):
    def test_q1_retains_patient_series_order_duplicates_nulls_and_empty(self):
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root / 'data').mkdir()
            symptoms = pd.DataFrame(dict(patient_id=[1., 1., None, 2.], symptoms=['same', 'same', None, 'other']))
            symptoms.to_csv(root / 'data/text_symptoms_data.csv', index=False)
            pd.DataFrame(dict(patient_id=[1., 2.], disease=['Allergy', 'Allergy'])).to_csv(
                root / 'data/patient_data_with_labels.csv', index=False)
            query = SemBenchTextQuery(CHECKOUT, root, 'medical', '1', execution_id='medical-q1-fixture',
                model_name='cpu-no-model', generation=GenerationSpec(max_tokens=8192), scale_factor=11112)
            sources = [dict(occurrence_id='row-' + str(i), source_position=i,
                            title=str(query.operation.frame.iloc[i]['patient_id'])) for i in range(4)]
            for chosen in ([3, 2, 1, 0], [1, 0], [2], []):
                native = dict(return_type='TINYINT', full_filter_materialized_before_title_sink=True,
                    source_rows=4, eligible_rows=4, accepted_occurrences=[sources[i] for i in chosen])
                result = finish_mmqa_filters(native, {'1': (query, sources)})['1']
                pd.testing.assert_series_equal(result, query.operation.frame.iloc[sorted(chosen)]['patient_id'])

    def test_original_join_duplicates_nulls_empty_and_reordered_membership(self):
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root / 'data').mkdir()
            # Duplicate patients, duplicate symptoms and null join keys retain
            # pandas' original inner join semantics and mean skips null ages.
            patients = pd.DataFrame(dict(patient_id=[1.,1.,2.,None,99.], age=[20.,40.,None,60.,90.]))
            symptoms = pd.DataFrame(dict(patient_id=[1.,1.,2.,None,77.], symptoms=['same','same',None,'null key','unmatched']))
            patients.to_csv(root/'data/patient_data.csv', index=False)
            patients.to_csv(root/'data/patient_data_with_labels.csv', index=False)
            symptoms.to_csv(root/'data/text_symptoms_data.csv', index=False)
            query = SemBenchTextQuery(CHECKOUT, root, 'medical', '4', execution_id='medical-fixture',
                model_name='cpu-no-model', generation=GenerationSpec(max_tokens=8192), scale_factor=11112)
            expected = patients.join(symptoms.set_index('patient_id'), on='patient_id', how='inner')
            pd.testing.assert_frame_equal(query.operation.frame, expected)
            sources = [dict(occurrence_id='row-' + str(i), source_position=i,
                            title=str(expected.iloc[i]['patient_id'])) for i in range(len(expected))]
            for chosen in (list(reversed(range(len(expected)))), [3,0,2], [], [4]):
                native = dict(return_type='TINYINT', full_filter_materialized_before_title_sink=True,
                    source_rows=len(sources), eligible_rows=len(sources),
                    accepted_occurrences=[sources[i] for i in chosen])
                result = finish_mmqa_filters(native, {'4': (query, sources)})['4']
                pd.testing.assert_frame_equal(result, pd.DataFrame(dict(average_acne_age=[expected.iloc[sorted(chosen)].age.mean()])))
            for change in ('duplicate', 'foreign', 'wrong-position', 'wrong-label'):
                native['accepted_occurrences'] = [deepcopy(sources[0])]
                if change == 'duplicate': native['accepted_occurrences'].append(sources[0])
                elif change == 'foreign': native['accepted_occurrences'][0]['occurrence_id'] = 'unknown'
                elif change == 'wrong-position': native['accepted_occurrences'][0]['source_position'] = 2
                else: native['accepted_occurrences'][0]['title'] = 'different'
                with self.subTest(change=change), self.assertRaises(ValueError):
                    finish_mmqa_filters(native, {'4': (query, sources)})


if __name__ == '__main__': unittest.main()
