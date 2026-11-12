"""Input conversion used by the original SemBench CSV-based evaluators."""

from io import StringIO


def csv_result_frame(result):
    """Match GenericRunner.save_results followed by GenericEvaluator.read_csv.

    Keep Series column names, dtype/NA inference and the original empty-file
    behavior. This is evaluation work after the original full query sink.
    """
    import pandas as pd

    try:
        return pd.read_csv(StringIO(result.to_csv(index=False)))
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
