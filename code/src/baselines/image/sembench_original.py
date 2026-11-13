"""Complete original LOTUS visual tasks, independent of the execution backend.

The caller supplies an established input snapshot. Binding loads the original
query, not its data; execute() retains the original fresh source reads and all
data-dependent operators. This module owns no model, scheduler, clock or budget.
"""

from pathlib import Path


class OriginalVisualQuery:
    """Bind original source, complete query and evaluation for a checked task."""

    def __init__(self, checkout, scenario, query, *, inputs, data_dir=None,
                 manifest=None, manifest_digest=None):
        self.checkout, self.scenario, self.query = checkout, scenario, query
        self.inputs = inputs
        self.data_dir = None if data_dir is None else Path(data_dir)
        if scenario == 'animals':
            from . import sembench_animals as module

            self.source = module.AnimalsDataset(Path(checkout), Path(manifest), manifest_digest)
            self._execute = module.bind_original_query(checkout, self.source.bind_frame(), inputs['query_id'])
        elif scenario == 'mmqa':
            from . import sembench_mmqa as module

            self._execute = module.bind_original_query(checkout, data_dir, query)
        elif scenario in ('cars', 'medical'):
            import pandas as pd
            if scenario == 'cars':
                from src.baselines.text import sembench_cars as module
            else:
                from src.baselines.text import sembench_medical as module

            self._execute = module.bind_original_query(checkout, query,
                lambda name, **kwargs: pd.read_csv(self.data_dir / name, **kwargs),
                data_dir=data_dir, scale_factor=inputs['scale_factor'])
        elif scenario == 'ecomm':
            import pandas as pd
            from src.baselines.text import sembench_ecomm as module

            self._execute = module.bind_original_query(checkout, query,
                lambda name, **kwargs: pd.read_parquet(self.data_dir / name, **kwargs), data_dir=data_dir)
        else:
            raise ValueError('original visual tasks support animals, mmqa, cars, medical and ecomm')
        self._module = module

    def execute(self):
        """Run all original operators using their actual intermediate relations."""
        return self._execute()

    def verify_inputs(self):
        """Audit the established input snapshot after the complete result."""
        if self.scenario == 'animals':
            self.source.read()
        elif self.scenario == 'mmqa':
            if self._module.describe_inputs(self.checkout, self.data_dir, self.query) != self.inputs:
                raise ValueError('MMQA visual input changed while executing the original query')
        else:
            self._module.verify_visual_inputs(self.inputs)

    def evaluate(self, result):
        """Keep original gold loading, evaluator and result conversion semantics."""
        if self.scenario == 'animals':
            return self._module.evaluate_original(self.checkout, result,
                self.source.frame(references=True), self.inputs['query_id'])
        if self.scenario == 'ecomm':
            return self._module.evaluate_original(self.checkout, self.query, result,
                data_dir=self.data_dir, source_identity=self.inputs['gold_source_identity'])
        if self.scenario in ('cars', 'medical'):
            return self._module.evaluate_visual_original(self.checkout, self.query, result, self.inputs)
        return self._module.evaluate_original(self.checkout, self.query, result)

    def expected_batch_sizes(self, result):
        """Return known native call shapes; later work can depend on the result."""
        if self.scenario == 'animals':
            return [self.inputs['rows']] * self.inputs['native_filter_passes']
        if self.scenario == 'mmqa':
            sizes = [self.inputs['exact_join_candidates']]
            if self.inputs['query'] == 'Q2b':
                sizes.append(len(result))
            return sizes
        if self.scenario in ('cars', 'medical'):
            return [self.inputs['expected_model_calls']]
        if self.inputs['query'] == 'Q12':
            return [self.inputs['filter_occurrences'], len(result)]
        # Branches/joins/grouped TopK remain in the original function. Its first
        # frontier is known; do not invent later counts before native execution.
        return None

    @property
    def first_operator_occurrences(self):
        return (self.inputs['first_operator']['expected_model_calls']
                if self.scenario == 'ecomm' else None)
