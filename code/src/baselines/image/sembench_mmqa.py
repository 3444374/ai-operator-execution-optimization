"""Original MMQA Q2a/Q2b/Q7 and their exact visual-Join input budget.

LOTUS owns the Join, optional Extract and relational sink. The caller supplies
its native LM; this module neither creates a model nor implements a provider.
"""

import csv
from contextlib import contextmanager
from dataclasses import asdict
from functools import wraps
import os
from pathlib import Path

from src.baselines.image.sembench_animals import image_identity
from src.baselines.text.sembench_source import SEMBENCH_COMMIT, load_module, verify_checkout
from src.infrastructure.private_artifacts import file_identity


IMAGE_QUERY_IDS = ("2a", "2b", "7")


def _query_id(query_id):
    query = str(query_id).lower().removeprefix("q")
    if query not in IMAGE_QUERY_IDS:
        raise ValueError("MMQA visual queries are Q2a, Q2b and Q7")
    return query


def describe_inputs(checkout, data_dir, query_id):
    """Read actual files and count all exact Join candidates without model work."""
    query = _query_id(query_id)
    verify_checkout(checkout)
    data = Path(data_dir).resolve()
    filename = "tampa_international_airport.csv" if query == "7" else "ap_warrior.csv"
    required = {"Airlines"} if query == "7" else {"ID", "Track"}
    table = data / filename
    with table.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames
        if columns is None or not required <= set(columns):
            raise ValueError("original " + filename + " is missing query input columns")
        rows = sum(1 for _ in reader)
    directory = data / "images"
    # Match the original os.listdir order and case-sensitive extension filter.
    # Sorting or deduplicating here would describe a different native input.
    images = [dict(path=str(directory / name), **image_identity(directory / name))
              for name in os.listdir(directory) if name.endswith((".png", ".jpg"))]
    candidates = len(images) * rows
    return dict(schema="semloom.sembench_mmqa_visual_inputs.v1", query="Q" + query,
        sembench_commit=SEMBENCH_COMMIT, data_dir=str(data),
        table=dict(path=str(table), **file_identity(table), rows=rows, columns=columns),
        images=images, image_occurrences=len(images), exact_join_candidates=candidates,
        maximum_followup_extracts=candidates if query == "2b" else 0,
        maximum_model_calls=candidates * (2 if query == "2b" else 1),
        actual_followup_extracts=None if query == "2b" else 0,
        original_source_difference=("Q7 native implementation joins all airline rows; it does not apply "
            "the natural-language question's Europe destination restriction" if query == "7" else None),
        policy="exact", join_strategy="zs-cot", warmup_calls=0, retries=0,
        native_join_strategy="ReasoningStrategy.ZS_COT",
        model_calls=0, join_left="table" if query == "7" else "images",
        execution_order=("table rows outer; original os.listdir images inner" if query == "7" else
                         "original os.listdir images outer; table rows inner"),
        budget_scope="one pass; Q2b Extract count equals actual Join matches, including repeated images")


@contextmanager
def _native_query_api():
    """Bind the pinned runner's strategy and empty Extract schema in its worker."""
    from lotus.sem_ops.sem_extract import SemExtractDataFrame
    from lotus.sem_ops.sem_join import SemJoinDataframe
    from lotus.types import ReasoningStrategy

    original_join = SemJoinDataframe.__call__
    original_extract = SemExtractDataFrame.__call__

    @wraps(original_join)
    def join(accessor, *args, **kwargs):
        if kwargs.get("strategy") == "zs-cot":
            kwargs["strategy"] = ReasoningStrategy.ZS_COT
        return original_join(accessor, *args, **kwargs)

    @wraps(original_extract)
    def extract(accessor, input_cols, output_cols, *args, **kwargs):
        result = original_extract(accessor, input_cols, output_cols, *args, **kwargs)
        if len(result.index) == 0:
            return result.reindex(columns=result.columns.union(output_cols, sort=False))
        return result

    SemJoinDataframe.__call__ = join
    SemExtractDataFrame.__call__ = extract
    try:
        yield
    finally:
        SemJoinDataframe.__call__ = original_join
        SemExtractDataFrame.__call__ = original_extract


def run_original_query(checkout, data_dir, query_id):
    """Call the unchanged full exact query without setup, warmup or preset matches."""
    import pandas as pd

    query = _query_id(query_id)
    module = load_module(checkout, "sembench_mmqa_visual_lotus",
                         "src/scenario/mmqa/runner/lotus_runner/lotus_runner.py")
    data = Path(data_dir).resolve()
    owner = object.__new__(module.LotusRunner)
    owner.data_path, owner.policy = str(data), "exact"

    def load(name, **kwargs):
        expected = "tampa_international_airport.csv" if query == "7" else "ap_warrior.csv"
        if name != expected:
            raise ValueError("unexpected MMQA visual source")
        return pd.read_csv(data / name, **kwargs)

    owner.load_data = load
    with _native_query_api():
        return getattr(owner, "_execute_q" + query)()


def evaluate_original(checkout, query_id, result):
    """Use the selected variant's author JSON and unchanged evaluator."""
    query = _query_id(query_id)
    module = load_module(checkout, "sembench_mmqa_visual_evaluator",
                         "src/scenario/mmqa/evaluation/evaluate.py")
    truth = Path(checkout).resolve() / "files/mmqa/query/natural_language" / ("q" + query + ".json")
    evaluator = object.__new__(module.MMQAEvaluator)
    metric = evaluator._evaluate_single_query(query, result, str(truth))
    return dict(original_metric=asdict(metric), sembench_commit=SEMBENCH_COMMIT)
