"""Pinned MMQA Q5 source, unchanged LOTUS query and original JSON evaluator."""

from dataclasses import asdict
from functools import partial
from pathlib import Path
from types import SimpleNamespace

from src.baselines.text.sembench_source import SEMBENCH_COMMIT, load_module
from src.infrastructure.private_artifacts import file_identity


INPUT_FILE = "lizzy_caplan_text_data.csv"


def input_identity(data_dir):
    return file_identity(Path(data_dir).resolve() / INPUT_FILE)


def run_original_query(checkout, data_dir, *, input_observer=None):
    """Keep the author's title selection, sem_agg arguments and _output projection."""
    return bind_original_query(checkout, data_dir, input_observer=input_observer)()


def bind_original_query(checkout, data_dir, *, input_observer=None):
    """Bind the native aggregation before timing its actual source and hierarchy."""
    import pandas as pd

    module = load_module(checkout, "sembench_mmqa_aggregate_lotus",
                         "src/scenario/mmqa/runner/lotus_runner/lotus_runner.py")

    def load_data(filename, **kwargs):
        if filename != INPUT_FILE:
            raise ValueError("MMQA Q5 requested an unexpected relation")
        frame = pd.read_csv(Path(data_dir).resolve() / filename, **kwargs)
        if input_observer is not None:
            input_observer(frame)
        return frame

    return partial(module.LotusRunner._execute_q5, SimpleNamespace(load_data=load_data))


def evaluate_original(checkout, result):
    """Evaluate the original _output column against the author's Q5 JSON."""
    module = load_module(checkout, "sembench_mmqa_aggregate_evaluator",
                         "src/scenario/mmqa/evaluation/evaluate.py")
    evaluator = object.__new__(module.MMQAEvaluator)
    truth = Path(checkout).resolve() / "files/mmqa/query/natural_language/q5.json"
    metric = evaluator._evaluate_single_query("5", result, str(truth))
    return dict(original_metric=asdict(metric), sembench_commit=SEMBENCH_COMMIT,
                ground_truth=file_identity(truth))
