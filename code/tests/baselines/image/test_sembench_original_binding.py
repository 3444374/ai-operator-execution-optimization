"""Original visual task ownership with real public queries and no model calls."""

from contextlib import ExitStack
from pathlib import Path
import unittest
from unittest.mock import patch

from src.baselines.sembench_lotus import OriginalLotusQuery
from src.baselines.text.sembench_text import capture_first_operator
from tests.experiments import test_sembench_lotus_image_query as fixture


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK, 'requires pinned SemBench source and LOTUS SDK')
class OriginalVisualBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.NativeLotusImageQueryTests.setUpClass()

    def setUp(self):
        self.source = fixture.NativeLotusImageQueryTests()
        self.source.setUp()
        self.addCleanup(self.source.doCleanups)

    def bind(self, scenario, query, arguments, **config):
        config, _ = self.source.config(scenario, query, **config)
        checked = self.source.entry.check(config, self.source.checkout, **arguments)['inputs']
        return OriginalLotusQuery(self.source.checkout, scenario, query, inputs=checked, **arguments)

    def test_binding_uses_checked_snapshot_without_repeating_table_reads_or_describe(self):
        import pandas as pd
        from src.baselines.image import sembench_animals, sembench_mmqa
        from src.baselines.text import sembench_cars, sembench_medical, sembench_ecomm

        cases = (
            ('animals', 'Q7', self.source.animals_inputs(), {}, sembench_animals),
            ('mmqa', 'Q2b', self.source.mmqa_inputs(), {}, sembench_mmqa),
            ('cars', 'Q3', self.source.cars_inputs(), dict(scale_factor=7), sembench_cars),
            ('medical', 'Q9', self.source.medical_inputs(), dict(scale_factor=29), sembench_medical),
            ('ecomm', 'Q12', self.source.ecomm_inputs(), {}, sembench_ecomm),
        )
        for scenario, query, arguments, options, module in cases:
            with self.subTest(scenario=scenario):
                config, _ = self.source.config(scenario, query, **options)
                checked = self.source.entry.check(config, self.source.checkout, **arguments)['inputs']
                with ExitStack() as stack:
                    for reader in ('read_csv', 'read_parquet'):
                        stack.enter_context(patch.object(pd, reader, side_effect=AssertionError('binding read query data')))
                    if scenario == 'animals':
                        # Preserve the established bind_frame identity check;
                        # the task must not repeat the full preflight itself.
                        stack.enter_context(patch.object(module.AnimalsDataset, 'preflight',
                            side_effect=AssertionError('binding repeated preflight')))
                    else:
                        name = 'describe_inputs' if scenario == 'mmqa' else 'describe_visual_inputs'
                        stack.enter_context(patch.object(module, name,
                            side_effect=AssertionError('binding repeated preflight')))
                    task = OriginalLotusQuery(self.source.checkout, scenario, query, inputs=checked, **arguments)
                self.assertIs(task.inputs, checked)
                kind, frame, args, kwargs = capture_first_operator(task.execute)
                calls = len(frame) * len(args[0]) if kind == 'join' else len(frame)
                first = (checked['rows'] if scenario == 'animals' else checked['exact_join_candidates']
                         if scenario == 'mmqa' else checked['minimum_model_calls']
                         if scenario == 'ecomm' else checked['expected_model_calls'])
                self.assertEqual(calls, first)
                task.verify_inputs()

    def test_original_function_reads_fresh_source_and_verifier_rejects_changed_snapshot(self):
        import pandas as pd

        arguments = self.source.mmqa_inputs()
        task = self.bind('mmqa', 'Q2a', arguments)
        pd.DataFrame(dict(ID=[99], Track=['changed track'])).to_csv(
            Path(arguments['data_dir'])/'ap_warrior.csv', index=False)
        kind, frame, args, kwargs = capture_first_operator(task.execute)
        self.assertEqual(kind, 'join')
        self.assertEqual(len(args[0]), 1)
        self.assertEqual(args[0]['ID'].tolist(), [99])
        with self.assertRaisesRegex(ValueError, 'MMQA visual input changed'):
            task.verify_inputs()

    def test_followup_count_uses_actual_result_while_branch_counts_remain_unknown(self):
        task = self.bind('mmqa', 'Q2b', self.source.mmqa_inputs())
        self.assertEqual(task.expected_batch_sizes([]), [4, 0])
        self.assertEqual(task.expected_batch_sizes([1, 2, 3]), [4, 3])
        self.assertIsNone(task.first_operator_occurrences)
        branches = self.bind('ecomm', 'Q10', self.source.ecomm_outfit_inputs('Q10'))
        self.assertIsNone(branches.expected_batch_sizes([1, 2, 3]))
        self.assertEqual(branches.first_operator_occurrences,
                         branches.inputs['first_operator']['expected_model_calls'])

    def test_original_evaluator_receives_unmodified_result_and_established_gold_identity(self):
        from src.baselines.text import sembench_ecomm

        task = self.bind('ecomm', 'Q12', self.source.ecomm_inputs())
        result, quality = object(), object()
        with patch.object(sembench_ecomm, 'evaluate_original', return_value=quality) as evaluate:
            self.assertIs(task.evaluate(result), quality)
        self.assertIs(evaluate.call_args.args[2], result)
        self.assertIs(evaluate.call_args.kwargs['source_identity'], task.inputs['gold_source_identity'])


if __name__ == '__main__':
    unittest.main()
