"""Project Movie Q3 binding to the public Ray Data LLM processor.

Ray owns chat templating, tokenization, model actors, requests and batching.
This is a native API workload binding, not the original SemBench runner.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from copy import deepcopy
from functools import partial
from typing import Any


_PROCESSOR_OPTIONS = frozenset((
    "batch_size", "concurrency", "max_concurrent_batches",
    "chat_template_stage", "tokenize_stage", "detokenize_stage",
))
_USAGE_FIELDS = (
    "num_input_tokens", "num_generated_tokens", "request_id", "batch_uuid",
    "time_taken_llm", "params", "metrics",
)


def _eligible(row):
    return row["id"] == "taken_3"


def _preprocess(row, *, sampling_params):
    from src.baselines.text.sembench_movie_native import q3_messages

    # The native engine consumes/mutates sampling dictionaries, including
    # structured_outputs. Each occurrence must own its parameters.
    return dict(messages=q3_messages(row["reviewText"]),
                sampling_params=deepcopy(sampling_params))


def _postprocess(row):
    from src.baselines.text.sembench_movie_native import q3_decision

    text = row["generated_text"]
    return dict(occurrence_id=row["occurrence_id"], reviewId=row["reviewId"], id=row["id"],
                generated_text=text, decision=q3_decision(text),
                native_usage={name: row[name] for name in _USAGE_FIELDS if name in row})


def _positive(row):
    return row["decision"]


def _options(values):
    values = deepcopy(dict(values or {}))
    unknown = values.keys() - _PROCESSOR_OPTIONS
    if unknown:
        raise ValueError("unsupported native Ray processor options: " + ", ".join(sorted(unknown)))
    for name in ("chat_template_stage", "tokenize_stage", "detokenize_stage"):
        # Ray 2.56.1 replaces stage batch_size with processor batch_size in
        # StatefulStage.get_dataset_map_batches_kwargs; do not advertise it.
        stage = values.get(name)
        stage_batch_size = (stage.get("batch_size") if isinstance(stage, Mapping)
                            else getattr(stage, "batch_size", None))
        if stage_batch_size is not None:
            raise ValueError("native Ray stage batch_size is ignored; use processor batch_size")
    return values


def execute_q3(
    rows: Sequence[Mapping[str, Any]], *, model: str, sampling_params: Mapping[str, Any],
    engine_kwargs: Mapping[str, Any] | None = None,
    processor_options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute full-cohort Q3 with one materialized native inference result.

    Rows retain their occurrence IDs, including duplicate business review IDs.
    The shared workload module owns Filter messages and the original decision
    parser. Returned raw replies permit a separate invalid-response audit.

    Unspecified processor/engine settings retain native defaults. Metadata
    distinguishes constructor settings from unobserved resolved model capacity
    and physical attempts. A newly initialized Ray runtime is closed on return;
    an existing caller runtime is retained. Neither case promises model reuse.
    """
    options = _options(processor_options)
    source = [dict(row) for row in rows]
    expected = Counter((row["occurrence_id"], row["reviewId"], row["id"])
                       for row in source if _eligible(row))
    metadata = dict(
        backend="ray-data-llm-native", workload_binding="project-sembench-movie-q3",
        scheduler_owner="Ray Data LLM processor / native vLLM engine",
        requested_config=dict(model_source=model, sampling_params=deepcopy(dict(sampling_params)),
                              engine_kwargs=deepcopy(dict(engine_kwargs or {})),
                              processor_options=deepcopy(options)),
        processor_config=None, stages=[], ray_version=None,
        input_rows=len(source), logical_inference_rows=sum(expected.values()),
        native_capacity=dict(status="unobserved", source="not exposed by public processor result"),
        retry_policy=dict(adapter_overrides={}, native_source_version="2.56.1",
                          actor_pool_source_defaults=dict(max_restarts=-1, max_task_retries=-1),
                          physical_attempts=None),
        lifecycle=dict(profile="query", runtime_owner="none", ray_runtime_closed=None,
                       model_process_exit_confirmed=None),
    )
    result = dict(predictions=[], positive_review_cnt=0, metadata=metadata)
    if not source:
        return result
    result["positive_review_cnt"] = None

    import ray
    import ray.data

    owns_runtime = not ray.is_initialized()
    metadata["ray_version"] = ray.__version__
    metadata["lifecycle"]["runtime_owner"] = "binding" if owns_runtime else "caller"
    primary = None
    try:
        context = ray.data.DataContext.get_current()
        metadata["retry_policy"]["actor_init_retry_on_errors"] = context.actor_init_retry_on_errors
        metadata["retry_policy"]["actor_init_max_retries"] = context.actor_init_max_retries
        metadata["retry_policy"]["actor_task_retry_on_errors"] = context.actor_task_retry_on_errors
        metadata["retry_policy"]["retried_map_errors"] = context.retried_map_errors
        metadata["retry_policy"]["max_map_retries"] = context.max_map_retries
        metadata["data_context"] = dict(max_tasks_in_flight_per_actor=context.max_tasks_in_flight_per_actor)
        selected = ray.data.from_items(source).filter(_eligible)
        if not expected:
            result["positive_review_cnt"] = selected.count()
        else:
            from ray.data.llm import build_processor, vLLMEngineProcessorConfig

            config = vLLMEngineProcessorConfig(model_source=model,
                engine_kwargs=deepcopy(dict(engine_kwargs or {})), **options)
            processor = build_processor(config,
                preprocess=partial(_preprocess, sampling_params=deepcopy(dict(sampling_params))),
                postprocess=_postprocess)
            metadata["processor_config"] = config.model_dump(mode="json")
            metadata["stages"] = processor.list_stage_names()
            predictions = processor(selected).materialize()
            output = predictions.take_all()
            result["predictions"] = output
            if Counter((row["occurrence_id"], row["reviewId"], row["id"]) for row in output) != expected:
                raise ValueError("native Ray predictions have missing, duplicate or unknown occurrences")
            result["positive_review_cnt"] = predictions.filter(_positive).count()
    except BaseException as error:
        primary = error
        error.native_q3_report = result
    finally:
        if owns_runtime:
            try:
                ray.shutdown()
                closed = not ray.is_initialized()
                metadata["lifecycle"]["ray_runtime_closed"] = closed
                if not closed:
                    raise RuntimeError("native Ray runtime remains initialized after shutdown")
            except BaseException as error:
                metadata["lifecycle"]["cleanup_error_type"] = type(error).__name__
                if primary is None:
                    primary = error
                    error.native_q3_report = result
                else:
                    primary.add_note("native Ray runtime cleanup failed: " + type(error).__name__)
    if primary is not None:
        raise primary
    return result
