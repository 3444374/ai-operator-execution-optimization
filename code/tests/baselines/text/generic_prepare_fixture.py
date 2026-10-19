"""Actual formatter wrappers proving the CPU fixture's distinct worker seed."""

import json
import os

from src.baselines.text.frameworks.ray_data_lotus_records import PrepareBatch
from src.execution_provider.adapters.lotus_records_actor_owner import _PrepareActor
from src.semantic_methods.lotus.engine import LotusFilterMethod


def _worker_proof(method):
    import lotus

    actual = os.environ.get("PYTHONHASHSEED")
    assert actual == os.environ["RECORDS_EXPECTED_CHILD_HASHSEED"]
    driver_order = json.loads(os.environ["RECORDS_EXPECTED_COLUMN_ORDER"])
    assert list(method.resolved_input_columns) == driver_order
    local_order = lotus.nl_expression.parse_cols(method.instruction)
    assert set(local_order) == set(driver_order) and local_order != driver_order
    return actual


class CheckedFilterMethod(LotusFilterMethod):
    def __init__(self, method):
        super().__init__(method)
        _worker_proof(method)


class CheckedPrepareBatch(PrepareBatch):
    def __init__(self, method, deadline):
        super().__init__(method, deadline)
        self.fixture_seed = _worker_proof(self.method.method)

    def __call__(self, batch):
        import pyarrow as pa

        values = super().__call__(batch).to_pylist()
        return pa.Table.from_pylist([dict(row, prepare_python_hash_seed=self.fixture_seed)
                                    for row in values])


class CheckedPrepareActor(_PrepareActor):
    def __init__(self, method):
        super().__init__(method)
        self.fixture_seed = _worker_proof(self.method.method)

    def prepare(self, rows, deadline):
        return [dict(row, prepare_python_hash_seed=self.fixture_seed)
                for row in super().prepare(rows, deadline)]
