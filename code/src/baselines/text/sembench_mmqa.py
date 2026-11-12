"""Pinned MMQA text inputs, original query functions and original evaluators.

The upstream generator owns sampling, and the upstream query owns relational
selection and postprocessing. This module creates no model or execution owner.
"""

import csv
from dataclasses import asdict
from pathlib import Path
import shutil
from types import SimpleNamespace

from src.baselines.text.sembench_source import SEMBENCH_COMMIT, load_module
from src.infrastructure.private_artifacts import (
    file_identity, new_private_directory, write_private_json,
)


TEXT_QUERY_IDS = ("1", "3a", "3b", "3c", "3d", "3e", "3f", "3g", "4", "6a", "6b", "6c")
TEXT_FILES = ("ben_piazza_text_data.csv", "lizzy_caplan_text_data.csv", "tampa_international_airport.csv")


def _text_query_id(query_id):
    if type(query_id) not in (str, int):
        raise ValueError("MMQA query_id must identify one supported text query")
    query = str(query_id).lower().removeprefix("q")
    if query not in TEXT_QUERY_IDS:
        raise ValueError("supported MMQA text queries are Q1, Q3a-Q3g, Q4 and Q6a-Q6c")
    return query


def _csv_identity(path):
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames
        rows = sum(1 for _ in reader)
    return dict(file_identity(path), rows=rows, columns=columns)


def prepare_text_inputs(checkout, source_dir, output_dir, scale_factor=25):
    """Generate only text relations from existing author assets, without downloads.

    Upstream setup chooses directories and may download; generate_data also
    prepares images. Bind the caller's locations and call only its text methods.
    The original constructor's supported scale range remains 25 through 1000.
    """
    if type(scale_factor) is not int or not 25 <= scale_factor <= 1000:
        raise ValueError("the pinned MMQA generator supports scale_factor 25 through 1000")
    module = load_module(checkout, "sembench_mmqa_data",
                         "src/scenario/mmqa/preparation/generate_data.py")
    source, output = Path(source_dir).resolve(), Path(output_dir).resolve()
    if not source.is_dir():
        raise ValueError("MMQA source_dir must contain the existing author CSV assets")
    inputs = (*module.FIXED_FILES, module.BASE_BEN_PIAZZA_TEXT_DATA_FILENAME,
              module.BASE_LIZZY_CAPLAN_TEXT_DATA_FILENAME, module.BASE_TAMPA_AIRPORT_FILENAME,
              module.ADDITIONAL_MOVIES_FILENAME, module.ADDITIONAL_AIRPORTS_FILENAME)
    source_files = {name: _csv_identity(source / name) for name in inputs}
    new_private_directory(output)

    generator = object.__new__(module.MMQADataGenerator)
    generator.source_data_dir, generator.output_data_dir = str(source), str(output)
    generator.scale_factor = scale_factor
    # Upstream _copy_fixed_files shells out with unquoted paths. Copy the same
    # bytes directly so caller-selected paths never become shell commands.
    for name in module.FIXED_FILES:
        shutil.copyfile(source / name, output / name)
    generator._generate_ben_piazza_text_data_per_scale_factor()
    generator._generate_lizzy_caplan_text_data_per_scale_factor()
    generator._generate_airport_text_data_per_scale_factor()
    files = {}
    for name in (*module.FIXED_FILES, *TEXT_FILES):
        (output / name).chmod(0o600)
        files[name] = _csv_identity(output / name)
    report = dict(schema="semloom.mmqa_text_inputs.v1", sembench_commit=SEMBENCH_COMMIT,
                  scale_factor=scale_factor, source_dir=str(source), output_dir=str(output),
                  source_files=source_files, files=files, model_calls=0,
                  images_generated=False, downloaded=False)
    write_private_json(output / "manifest.json", report)
    return report


def run_original_query(checkout, query_id, load_data):
    """Execute the unchanged finite text query without benchmark setup or warmup."""
    return bind_original_query(checkout, query_id, load_data)()


def bind_original_query(checkout, query_id, load_data):
    """Load and bind the native method before the measured source scan."""
    query = _text_query_id(query_id)
    module = load_module(checkout, "sembench_mmqa_lotus",
                         "src/scenario/mmqa/runner/lotus_runner/lotus_runner.py")
    owner = SimpleNamespace(load_data=load_data)
    method = getattr(module.LotusRunner, "_execute_q" + query)
    return lambda: method(owner)


def evaluate_original(checkout, query_id, result):
    """Keep each variant's author JSON and original metric computation."""
    import pandas as pd

    query = _text_query_id(query_id)
    module = load_module(checkout, "sembench_mmqa_evaluator",
                         "src/scenario/mmqa/evaluation/evaluate.py")
    # The official runner writes this Series to CSV before the evaluator reads
    # a DataFrame. Match that shape without changing its index, values or name.
    if query == "1" and isinstance(result, pd.Series):
        result = result.to_frame()
    evaluator = object.__new__(module.MMQAEvaluator)
    truth = Path(checkout).resolve() / "files/mmqa/query/natural_language" / ("q" + query + ".json")
    metric = evaluator._evaluate_single_query(query, result, str(truth))
    return dict(original_metric=asdict(metric), sembench_commit=SEMBENCH_COMMIT)
