"""Explicit no-model UDF entry point; the production entry has no fixture flag."""
from integrations.imlane_records import filter_udf
from tests.integrations.imlane_filter_fixture import FixedEngineProcess


class FixedWorker(filter_udf.FilterWorker):
    def __init__(self, config):
        super().__init__(config, engine_type=FixedEngineProcess)


def infer_records(table):
    original = filter_udf.FilterWorker
    filter_udf.FilterWorker = FixedWorker
    try:
        return filter_udf.infer_records(table)
    finally:
        filter_udf.FilterWorker = original
