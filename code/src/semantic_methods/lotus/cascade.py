"""Pinned LOTUS helper-LM control with a records-only evaluation seam.

The installed accessor and threshold learner execute their original code objects
with private globals. Only document expansion and model evaluation are replaced;
no module, installed file, LM instance method, or threshold algorithm is patched.
"""

from copy import deepcopy
from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import distribution
import hashlib
import json
import math
from types import FunctionType, SimpleNamespace

from .engine import (
    FILTER_SOURCE_HASHES, engine_response, lm_report, validate_engine_protocol, validate_filter_source,
)
from .records import MOVIE_Q3_INSTRUCTION
from .sdk import LOTUS_COMMIT


CASCADE_SOURCE_HASHES = dict(FILTER_SOURCE_HASHES, **{
    "sem_ops/cascade_utils.py": "14fc1140939567559abde71976c1f3d21906864d720aa324f983f9dc8cf3b594",
    "types.py": "73b0ebb4a8a9606cdec6df27353d57fb28ce6e2f5800c539caee892eb4f2cc6b",
    "utils.py": "2d8d38af8230f447a354f460e25a8f523f1dc1aa5edebf977a8297ed04458ebe",
})
STAGES = ("helper_score", "threshold_sample", "fallback")
IDENTITY_FIELDS = ("query_id", "model_role", "algorithm_stage", "row_id",
                   "source_position", "call_index", "call_id")


def validate_cascade_source():
    validate_filter_source()
    root = distribution("lotus-ai").locate_file("lotus")
    for relative, expected in CASCADE_SOURCE_HASHES.items():
        if hashlib.sha256((root / relative).read_bytes()).hexdigest() != expected:
            raise ValueError("records cascade source differs at lotus/" + relative)
    return dict(profile="lotus-helper-cascade-records-v1", lotus_commit=LOTUS_COMMIT,
                source_hashes=dict(CASCADE_SOURCE_HASHES),
                control="original accessor and learner code objects; private records globals")


def cascade_identity(query_id, row_id, source_position, call_index, *, model_role, stage):
    if any(type(value) is not str or not value.strip() for value in (query_id, row_id)):
        raise ValueError("cascade query and row IDs must be nonempty strings")
    if any(type(value) is not int or value < 0 for value in (source_position, call_index)):
        raise ValueError("cascade positions and call ordinals must be nonnegative integers")
    if stage not in STAGES or model_role != ("helper" if stage == "helper_score" else "main"):
        raise ValueError("cascade role and stage differ")
    parts = [query_id, model_role, stage, row_id, source_position, call_index]
    return dict(query_id=query_id, model_role=model_role, algorithm_stage=stage,
                row_id=row_id, source_position=source_position, call_index=call_index,
                call_id="lotus-cascade-v1:" + json.dumps(parts, ensure_ascii=False,
                                                        separators=(",", ":")))


@dataclass(frozen=True)
class CascadeStage:
    """Logical evaluations; a backend chooses its own physical batches and resources."""

    name: str
    model_role: str
    model_name: str
    instruction: str
    default: bool
    system_prompt: str | None
    output_tokens: tuple[str, str]
    effective_kwargs: dict
    records: tuple[dict, ...]

    @property
    def logprobs(self):
        return self.effective_kwargs["logprobs"]


def _generation_options(effective):
    options = dict(effective)
    want = options.pop("logprobs")
    top = options.pop("top_logprobs", 10)
    if type(want) is not bool or (want and (type(top) is not int or not 1 <= top <= 20)):
        raise ValueError("helper logprobs require an explicit supported top_logprobs count")
    allowed = {"max_completion_tokens", "temperature", "top_p", "seed", "stop",
               "presence_penalty", "frequency_penalty"}
    if options.keys() - allowed:
        raise ValueError("unsupported cascade generation options: " +
                         ", ".join(sorted(options.keys() - allowed)))
    options["max_tokens"] = options.pop("max_completion_tokens")
    options["logprobs"] = top if want else None
    options.setdefault("stop", ())
    return options


def local_generation(stage):
    """Map only supported effective LM options; unsupported options fail before allocation."""
    from src.serving.backends.vllm_engine import GenerationSpec

    options = _generation_options(stage.effective_kwargs)
    return GenerationSpec(**options)


def _effective(lm, model_kwargs, logprobs):
    if "logprobs" in model_kwargs:
        raise ValueError("sem_filter owns the per-stage logprobs option")
    options = {**deepcopy(lm.kwargs), **deepcopy(model_kwargs), "logprobs": logprobs}
    if logprobs:
        options.setdefault("top_logprobs", 10)
    allowed = {"max_completion_tokens", "temperature", "top_p", "seed", "stop",
               "presence_penalty", "frequency_penalty", "logprobs", "top_logprobs"}
    for values in (lm.kwargs, model_kwargs):
        if values.keys() - allowed:
            raise ValueError("unsupported cascade generation options: " +
                             ", ".join(sorted(values.keys() - allowed)))
    # Validate ordinary values without requiring local helper execution support.
    # A backend then validates actual logprobs capability before model allocation.
    from src.serving.backends.vllm_engine import GenerationSpec
    GenerationSpec(**dict(_generation_options(options), logprobs=None))
    return options


def _bind(function, replacements):
    function = getattr(function, "__wrapped__", function)
    rebound = FunctionType(function.__code__, {**function.__globals__, **replacements},
                           function.__name__, function.__defaults__, function.__closure__)
    rebound.__kwdefaults__ = function.__kwdefaults__
    return rebound


def _references(frame, columns):
    # The original controller only needs these records and their positional order.
    # Actual dtype-aware preparation is owned by the selected backend worker.
    if columns != ["reviewText"]:
        raise ValueError("this text cascade supports only the reviewText input column")
    return frame[["row_id", "source_position", "reviewText"]].to_dict("records")


def validate_cascade(frame, main_lm, helper_lm, cascade_args, *, max_rows, max_calls,
                     model_kwargs=None, strategy=None, examples=None, helper_examples=None):
    """Check the supported method, source and policies before creating a backend owner."""
    import lotus
    import pandas as pd
    from lotus.models import LM
    from lotus.types import CascadeArgs, ProxyModel, SerializationFormat

    validate_cascade_source()
    if (not isinstance(main_lm, LM) or not isinstance(helper_lm, LM)
            or main_lm is helper_lm):
        raise ValueError("distinct original main and helper LM objects are required")
    if lotus.settings.enable_cache or lotus.settings.serialization_format != SerializationFormat.DEFAULT:
        raise ValueError("records cascade requires disabled result caches and DEFAULT serialization")
    for lm in (main_lm, helper_lm):
        if lm.rate_limit is not None or lm.tpm_limit is not None:
            raise ValueError("records cascade does not implement rate or TPM limiting")
    if strategy is not None or examples is not None or helper_examples is not None:
        raise ValueError("first Movie cascade profile does not support reasoning or examples")
    if not isinstance(cascade_args, CascadeArgs) or cascade_args.proxy_model != ProxyModel.HELPER_LM:
        raise ValueError("an original helper-LM CascadeArgs object is required")
    # Revalidate mutable Pydantic instances before execution.
    args = CascadeArgs(**cascade_args.model_dump())
    if args.map_instruction is not None or args.map_examples is not None or args.join_cascade_strategy is not None:
        raise ValueError("Movie Filter does not support join/map cascade options")
    instruction = args.helper_filter_instruction or MOVIE_Q3_INSTRUCTION
    if lotus.nl_expression.parse_cols(instruction) != ["reviewText"]:
        raise ValueError("helper instruction must reference only the reviewText column")
    for name in ("recall_target", "precision_target", "failure_probability", "sampling_percentage",
                 "cascade_IS_weight"):
        value = getattr(args, name)
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError("invalid cascade probability: " + name)
    if not 0 < args.failure_probability < 1 or args.sampling_percentage <= 0:
        raise ValueError("failure_probability and sampling_percentage must be positive")
    if args.filter_pos_cascade_threshold is None and args.cascade_IS_weight == 1:
        raise ValueError("first records profile requires cascade_IS_weight < 1 for finite correction weights")
    for name in ("cascade_IS_max_sample_range", "cascade_num_calibration_quantiles"):
        if type(getattr(args, name)) is not int or getattr(args, name) < 1:
            raise ValueError("cascade calibration and sampling sizes must be positive integers")
    if args.cascade_IS_random_seed is not None and (type(args.cascade_IS_random_seed) is not int
                                                  or not 0 <= args.cascade_IS_random_seed < 2**32):
        raise ValueError("invalid cascade random seed")
    for name in ("filter_pos_cascade_threshold", "filter_neg_cascade_threshold"):
        value = getattr(args, name)
        if value is not None and (not math.isfinite(value) or not 0 <= value <= 1):
            raise ValueError("cascade thresholds must be finite probabilities")
    for value, name in ((max_rows, "max_rows"), (max_calls, "max_calls")):
        if type(value) is not int or value < 1:
            raise ValueError(name + " must be a positive integer")
    if (not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique
            or set(frame.columns) != {"row_id", "source_position", "reviewText"}):
        raise ValueError("cascade input must contain only occurrence IDs, position and reviewText")
    if len(frame) > max_rows:
        raise ValueError("cascade input exceeds max_rows")
    rows = _references(frame, ["reviewText"])
    seen = set()
    for row in rows:
        checked = cascade_identity("preflight", row["row_id"], row["source_position"], 0,
                                   model_role="helper", stage="helper_score")
        if checked["call_id"] in seen or type(row["reviewText"]) is not str:
            raise ValueError("cascade input contains duplicate occurrences or non-text values")
        row["reviewText"].encode("utf-8")
        seen.add(checked["call_id"])
    sample_size = (int(len(rows) * args.sampling_percentage)
                   if args.filter_pos_cascade_threshold is None else 0)
    if rows and args.filter_pos_cascade_threshold is None and sample_size < 1:
        raise ValueError("learned cascade requires at least one sampled occurrence")
    upper = 2 * len(rows) + sample_size
    if max_calls < upper:
        raise ValueError("max_calls must cover helper + sample + all possible main reviews")
    for lm in (main_lm, helper_lm):
        for token in ("True", "False"):
            if len(lm.encode_text(token)) != 1:
                raise ValueError("cascade requires single-token output labels in both tokenizers")
    kwargs = {} if model_kwargs is None else model_kwargs
    if type(kwargs) is not dict:
        raise ValueError("model_kwargs must be an object")
    return args, upper, {
        "helper": _effective(helper_lm, kwargs, True),
        "main": _effective(main_lm, kwargs, False),
    }


def cascade_response(result, lm, *, logprobs):
    """Preserve original LM extraction while requiring real helper token metadata."""
    from litellm.types.utils import ModelResponse

    response = engine_response(result, lm)
    validate_engine_protocol(result)
    if not logprobs:
        return response
    content = json.loads(result.get("logprobs_json", "null"))
    token_ids = json.loads(result.get("output_token_ids_json", "null"))
    if (type(content) is not list or not content or type(token_ids) is not list
            or len(content) != len(token_ids) or len(token_ids) != result["completion_tokens"]):
        raise ValueError("helper requires complete generated token IDs and token logprobs")
    if any(type(token) is not int or token < 0 for token in token_ids):
        raise ValueError("helper generated token IDs must be nonnegative integers")
    for item in content:
        if type(item) is not dict or type(item.get("token")) is not str:
            raise ValueError("helper logprobs require generated token text")
        tops = item.get("top_logprobs")
        if type(tops) is not list or not tops:
            raise ValueError("helper token is missing its top logprobs")
        for entry in [item, *tops]:
            value = entry.get("logprob") if type(entry) is dict else None
            if (type(entry) is not dict or type(entry.get("token")) is not str
                    or type(value) not in (int, float) or not math.isfinite(value) or value > 0):
                raise ValueError("helper contains malformed token logprobs")
    payload = response.model_dump()
    payload["choices"][0]["logprobs"] = dict(content=content)
    return ModelResponse(**payload)


class _StageCalls:
    """Per-stage evidence remains available when a graph or semantic parse fails."""

    def __init__(self, stage):
        self.stage = deepcopy(stage)
        self.expected = {row["call_id"]: {key: row[key] for key in IDENTITY_FIELDS}
                         for row in stage.records}
        self.results = {}

    def record(self, result):
        if type(result) is not dict or any(key not in result for key in IDENTITY_FIELDS):
            raise ValueError("cascade result lacks complete call identity")
        checked = cascade_identity(
            result["query_id"], result["row_id"], result["source_position"], result["call_index"],
            model_role=result["model_role"], stage=result["algorithm_stage"],
        )
        if any(result[key] != checked[key] for key in IDENTITY_FIELDS):
            raise ValueError("cascade result call identity is not canonical")
        key = result.get("call_id")
        expected = self.expected.get(key)
        if expected is None or key in self.results or any(result.get(k) != v for k, v in expected.items()):
            raise ValueError("cascade result changed, duplicated or invented call identity")
        if result.get("status") not in ("succeeded", "failed", "unknown"):
            raise ValueError("cascade result lacks terminal status")
        for name in ("prompt_tokens", "completion_tokens"):
            value = result.get(name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("cascade usage must be known nonnegative integers or None")
        confirmed = result.get("submission_confirmed", result["status"] == "succeeded")
        if type(confirmed) is not bool or (result["status"] == "succeeded" and not confirmed):
            raise ValueError("successful cascade calls require confirmed submission")
        self.results[key] = deepcopy(dict(result, submission_confirmed=confirmed))

    def unconfirmed(self, error):
        for key, expected in self.expected.items():
            if key not in self.results:
                self.results[key] = dict(expected, status="unknown", submission_confirmed=False,
                                         prompt_tokens=None, completion_tokens=None,
                                         error_type=type(error).__name__)

    def ordered(self):
        return [self.results[key] for key in self.expected if key in self.results]


class CascadeRun:
    """One query's original control, logical calls, and backend-owned stage results."""

    def __init__(self, evaluate, *, query_id, max_calls):
        if not callable(evaluate):
            raise TypeError("cascade evaluate must be a records-stage callable")
        cascade_identity(query_id, "preflight", 0, 0, model_role="helper", stage="helper_score")
        self.evaluate = evaluate
        self.query_id, self.max_calls = query_id, max_calls
        self.stages = []
        self.events = []
        self.active_stage = None
        self._ordinal = 0

    def dispatch(self, docs, model, instruction, default=True, *, logprobs=False,
                 system_prompt=None, output_tokens=("True", "False"), **options):
        from lotus.sem_ops.postprocessors import filter_postprocess
        from lotus.types import SemanticFilterOutput

        check = getattr(self.evaluate, "check_lifecycle", None)
        if check is not None:
            check()
        role = "helper" if model is self.helper_lm else "main"
        if model is not (self.helper_lm if role == "helper" else self.main_lm):
            raise ValueError("undeclared cascade model role")
        name = "helper_score" if role == "helper" else self.active_stage or "fallback"
        if logprobs != (role == "helper"):
            raise ValueError("original cascade stage logprobs policy changed")
        records = []
        for doc in docs:
            call = cascade_identity(self.query_id, doc["row_id"], doc["source_position"],
                                    self._ordinal, model_role=role, stage=name)
            self._ordinal += 1
            records.append(dict(call, reviewText=doc["reviewText"]))
        if self._ordinal > self.max_calls:
            raise ValueError("cascade max_calls exceeded before stage allocation")
        stage = CascadeStage(name, role, model.model, instruction, default, system_prompt,
                             tuple(output_tokens), deepcopy(self.effective[role]), tuple(records))
        journal = _StageCalls(stage)
        self.stages.append(journal)
        self.events.append(dict(event="stage_declared", stage=name, calls=len(records)))
        iterator = None
        error = None
        try:
            iterator = iter(self.evaluate(stage))
            for result in iterator:
                journal.record(result)
            if len(journal.results) != len(records):
                raise ValueError("cascade stage omitted declared evaluations")
        except BaseException as exc:
            error = exc
            journal.unconfirmed(exc)
            raise
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                try:
                    close()
                except BaseException as exc:
                    if error is None:
                        raise
                    error.add_note("cascade stage iterator close failed: " + type(exc).__name__)
        results = journal.ordered()
        responses, failures = [], []
        for result in results:
            try:
                response = engine_response(result, model)
                responses.append(response)
                validate_engine_protocol(result)
            except (ValueError, TypeError, KeyError) as exc:
                failures.append(exc)
        # Native LM accounts for the entire successful response batch before parsing.
        for response in responses:
            model._update_stats(response, is_cached=False)
        if failures:
            raise RuntimeError("cascade stage contains failed, unknown or malformed engine output") from failures[0]
        try:
            responses = [cascade_response(result, model, logprobs=logprobs) for result in results]
        except (ValueError, TypeError, KeyError) as exc:
            raise RuntimeError("cascade stage contains malformed token metadata") from exc
        texts = [model._get_top_choice(response) for response in responses]
        parsed = filter_postprocess(texts, model, default, output_tokens=output_tokens)
        logs = [model._get_top_choice_logprobs(response) for response in responses] if logprobs else None
        self.events.append(dict(event="stage_completed", stage=name, calls=len(records)))
        if check is not None:
            check()
        return SemanticFilterOutput(raw_outputs=parsed.raw_outputs, outputs=parsed.outputs,
                                    explanations=parsed.explanations, logprobs=logs)

    def report(self):
        records = [row for journal in self.stages for row in journal.ordered()]
        usage = {}
        for name in ("prompt_tokens", "completion_tokens"):
            known = [row.get(name) for row in records if row.get(name) is not None]
            usage["known_" + name] = sum(known)
            usage["unknown_" + name + "_calls"] = len(records) - len(known)
            usage[name] = sum(known) if len(known) == len(records) else None
        usage["total_tokens"] = (None if any(usage[k] is None for k in ("prompt_tokens", "completion_tokens"))
                                 else usage["prompt_tokens"] + usage["completion_tokens"])
        return dict(schema="semloom.lotus_cascade_calls.v1", max_calls=self.max_calls,
                    records=deepcopy(records), usage=usage, events=deepcopy(self.events),
                    stages=[dict(name=j.stage.name, model_role=j.stage.model_role,
                                 effective_kwargs=deepcopy(j.stage.effective_kwargs),
                                 expected=len(j.expected), recorded=len(j.results)) for j in self.stages])


def run_cascade(frame, main_lm, helper_lm, cascade_args, evaluate, *, query_id,
                max_rows, max_calls, default=True, system_prompt=None, model_kwargs=None,
                strategy=None, examples=None, helper_examples=None):
    """Run ordinary Movie Q3 Filter+COUNT with the original global helper controller.

    Input uses a position-based index internally and retains occurrence IDs. Empty
    input returns zero without model allocation (the original percentile raises).
    No labels, expanded messages, physical pool or scheduler enter this interface.
    """
    import lotus

    args, upper, effective = validate_cascade(
        frame, main_lm, helper_lm, cascade_args, max_rows=max_rows, max_calls=max_calls,
        model_kwargs=model_kwargs, strategy=strategy, examples=examples, helper_examples=helper_examples,
    )
    if type(default) is not bool or (system_prompt is not None and type(system_prompt) is not str):
        raise ValueError("invalid cascade parser or system prompt policy")
    run = CascadeRun(evaluate, query_id=query_id, max_calls=max_calls)
    run.main_lm, run.helper_lm, run.effective = main_lm, helper_lm, effective
    preflight = getattr(evaluate, "preflight", None)
    if preflight is not None:
        preflight(main_lm, helper_lm, effective, max_possible_calls=upper)
    module = import_module("lotus.sem_ops.sem_filter")
    learner = _bind(module.learn_filter_cascade_thresholds, {"sem_filter": run.dispatch})

    def learn(**kwargs):
        run.events.append(dict(event="global_sampling_completed"))
        run.active_stage = "threshold_sample"
        try:
            thresholds = learner(**kwargs)
        finally:
            run.active_stage = None
        run.events.append(dict(event="thresholds_learned", positive=float(thresholds[0]),
                               negative=float(thresholds[1])))
        return thresholds

    def calibrated(scores, settings):
        from lotus.sem_ops.cascade_utils import calibrate_llm_logprobs

        if any(not math.isfinite(score) or not 0 <= score <= 1 for score in scores):
            raise ValueError("original helper scoring produced nonfinite or invalid probabilities")
        run.events.append(dict(event="global_helper_completed", occurrences=len(scores),
                               positive_probs=list(scores)))
        result = calibrate_llm_logprobs(scores, settings)
        run.events.append(dict(event="global_calibration_completed", proxy_scores=list(result)))
        return result

    def sampled(scores, settings):
        from lotus.sem_ops.cascade_utils import importance_sampling

        indices, factors = importance_sampling(scores, settings)
        run.events.append(dict(event="sample_selected", indices=indices.tolist(),
                               correction_factors=factors.tolist()))
        return indices, factors

    controller = _bind(module.SemFilterDataframe.__call__, {
        "task_instructions": SimpleNamespace(df2multimodal_info=_references),
        "sem_filter": run.dispatch, "learn_filter_cascade_thresholds": learn,
        "calibrate_llm_logprobs": calibrated, "importance_sampling": sampled,
    })
    try:
        normalized = frame.reset_index(drop=True).copy()
        with lotus.settings.context(lm=main_lm, helper_lm=helper_lm, enable_cache=False):
            if normalized.empty:
                result, stats = normalized.assign(_filter=[]), {}
            else:
                result, stats = controller(
                    SimpleNamespace(_obj=normalized), MOVIE_Q3_INSTRUCTION,
                    return_all=True, return_raw_outputs=True, return_explanations=True,
                    return_stats=True, default=default, system_prompt=system_prompt,
                    cascade_args=args, **(model_kwargs or {}),
                )
        check = getattr(evaluate, "check_lifecycle", None)
        if check is not None:
            check()
        decisions = result.get("_filter", []).tolist() if len(result) else []
        report = dict(schema="semloom.lotus_helper_cascade.v1", status="completed",
                    query_id=query_id, positive_review_cnt=sum(decisions),
                    decisions=[dict(row_id=row["row_id"], source_position=row["source_position"],
                                    decision=bool(decisions[i]),
                                    raw_output=result.iloc[i].get("raw_output_filter", ""),
                                    explanation=result.iloc[i].get("explanation_filter"))
                               for i, row in enumerate(normalized.to_dict("records"))],
                    stats={key: value.model_dump(mode="json") if hasattr(value, "model_dump") else value
                           for key, value in stats.items()},
                    calls=run.report(), lm=dict(main=lm_report(main_lm), helper=lm_report(helper_lm)),
                    max_possible_calls=upper, empty_input_extension=normalized.empty,
                    positional_index_normalized=True, production_core_used=False,
                    method_source=validate_cascade_source())
        if check is not None:
            check()
        return report
    except BaseException as exc:
        exc.query_report = dict(status="failed", query_id=query_id, calls=run.report(),
                                lm=dict(main=lm_report(main_lm), helper=lm_report(helper_lm)),
                                error_type=type(exc).__name__)
        raise


class CascadePreparer:
    """Original dtype-aware formatter instantiated only inside a backend CPU worker."""

    def __init__(self, stage):
        validate_cascade_source()
        import lotus
        from lotus.cache import InMemoryCache
        from lotus.models import LM
        from lotus.types import SerializationFormat

        if lotus.settings.serialization_format != SerializationFormat.DEFAULT or lotus.settings.enable_cache:
            raise ValueError("cascade worker requires DEFAULT serialization and disabled result cache")

        self.stage = stage
        self.lm = LM(stage.model_name, cache=InMemoryCache(max_size=1))

    def prepare_rows(self, rows):
        import pandas as pd
        from lotus.templates import task_instructions

        if not rows:
            return []
        frame = pd.DataFrame(rows)
        docs = task_instructions.df2multimodal_info(frame, ["reviewText"])
        return [dict({key: row[key] for key in IDENTITY_FIELDS}, messages_json=json.dumps(
            task_instructions.filter_formatter(
                self.lm, doc, self.stage.instruction, system_prompt=self.stage.system_prompt,
                output_tokens=self.stage.output_tokens,
            ), ensure_ascii=False, allow_nan=False))
            for row, doc in zip(rows, docs, strict=True)]
