"""Pinned MMQA Q5 source, unchanged LOTUS query and original JSON evaluator."""

from dataclasses import asdict
from functools import partial
import hashlib
from importlib import import_module
import json
from pathlib import Path
from types import SimpleNamespace

from src.baselines.text.sembench_source import SEMBENCH_COMMIT, load_module
from src.infrastructure.private_artifacts import file_identity, json_metric


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


def load_token_counter(counter):
    """Load the declared local LOTUS counter without model lookup or downloads."""
    from tokenizers import Tokenizer

    path = Path(counter["path"]).resolve()
    if file_identity(path)["sha256"] != counter["sha256"]:
        raise ValueError("the local token counter does not match its declared SHA256")
    return Tokenizer.from_file(str(path))


class _LeafPrepared(BaseException):
    """Private non-model stop at the unchanged SDK's first LM invocation."""


def leaf_plan(checkout, data_dir, lm):
    """Stop at the first real SDK LM batch after native token counting/chunking."""
    import lotus

    module = import_module("lotus.sem_ops.sem_agg")
    accessor_type, lm_type = module.SemAggDataframe, type(lm)
    original_agg, original_call, original_count = accessor_type.__call__, lm_type.__call__, lm.count_tokens
    original_documents = module.sem_agg
    prepared = dict(token_count_calls=[])

    def count_tokens(value):
        count = original_count(value)
        payload = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
        prepared["token_count_calls"].append(dict(sha256=hashlib.sha256(payload.encode()).hexdigest(),
                                                 bytes=len(payload.encode()), tokens=count))
        return count

    def observe_source(frame):
        prepared["source_rows"] = len(frame)
        prepared["source_columns"] = list(frame.columns)

    def observe_input(accessor, instruction, *args, **kwargs):
        frame = accessor._obj
        prepared["selection"] = dict(rows=len(frame), source_indices=json_metric(frame.index.tolist()),
            titles=frame["title"].tolist(), text_characters=sum(len(value) for value in frame["text"]),
            text_utf8_bytes=sum(len(value.encode()) for value in frame["text"]),
            title_utf8_bytes=sum(len(value.encode()) for value in frame["title"]),
            instruction=instruction, operator_args=json_metric(args), operator_kwargs=json_metric(kwargs))
        return original_agg(accessor, instruction, *args, **kwargs)

    def observe_documents(docs, *args, **kwargs):
        prepared["native_documents"] = len(docs)
        prepared["long_context_strategy"] = docs.strategy.name.lower() if isinstance(docs, module.ChunkedDocument) else None
        return original_documents(docs, *args, **kwargs)

    def capture(current, messages, *args, **kwargs):
        if current is not lm:
            raise RuntimeError("Q5 preflight encountered another LM")
        prepared.update(leaf_messages=messages, leaf_message_tokens=[original_count(message) for message in messages],
                        lm_args=json_metric(args), lm_kwargs=json_metric(kwargs))
        raise _LeafPrepared()

    accessor_type.__call__, lm_type.__call__, lm.count_tokens = observe_input, capture, count_tokens
    module.sem_agg = observe_documents
    try:
        with lotus.settings.context(lm=lm, enable_cache=False):
            try:
                run_original_query(checkout, data_dir, input_observer=observe_source)
            except _LeafPrepared:
                return prepared
        raise RuntimeError("the original Q5 query did not reach native aggregation")
    finally:
        accessor_type.__call__, lm_type.__call__, lm.count_tokens = original_agg, original_call, original_count
        module.sem_agg = original_documents


def describe_work(plan):
    """Keep reduction counts unknown until real summaries determine the tree."""
    batches = len(plan["leaf_messages"])
    return dict(leaf_requests=batches, expected_model_calls=1 if batches == 1 else None,
        minimum_model_calls=batches if batches == 1 else batches + 1,
        maximum_model_calls=1 if batches == 1 else None,
        total_request_rule="exactly one leaf request" if batches == 1 else
            "leaf requests plus native reduction requests determined by actual summary token lengths; caller budget bounds attempts")
