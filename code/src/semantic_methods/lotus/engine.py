"""Ordinary text Filter formatting and native LOTUS result interpretation.

The execution backends receive records and return engine results. This module
does not create a model, send HTTP, run a vendor graph, or own its scheduling.
"""

from dataclasses import asdict, replace
from importlib.metadata import distribution, version
import hashlib
import json
import os
import time

from .records import FilterMethod
from .sdk import IMAGE_SOURCE_HASHES, LOTUS_COMMIT, SOURCE_HASHES


FILTER_SOURCE_HASHES = {
    name: digest for name, digest in SOURCE_HASHES.items() if name != "sem_ops/sem_map.py"
}
FILTER_SOURCE_HASHES["sem_ops/sem_filter.py"] = IMAGE_SOURCE_HASHES["sem_ops/sem_filter.py"]


def validate_filter_source():
    """Check the actual formatter, parser, LM accounting, and accessor source."""
    versions = {}
    for package, expected in (("lotus-ai", "1.2.4"), ("litellm", "1.95.0"), ("openai", "2.50.0")):
        actual = version(package)
        if actual != expected:
            raise ValueError(f"records Filter requires {package} {expected}")
        versions[package] = actual
    root = distribution("lotus-ai").locate_file("lotus")
    for relative, expected in FILTER_SOURCE_HASHES.items():
        if hashlib.sha256((root / relative).read_bytes()).hexdigest() != expected:
            raise ValueError("records Filter source differs at lotus/" + relative)
    return dict(profile="lotus-ordinary-filter-records-v1", lotus_commit=LOTUS_COMMIT,
                versions=versions, source_hashes=dict(FILTER_SOURCE_HASHES))


def make_lm(method, generation):
    """Construct only LOTUS's accounting/configuration object, with no SDK call."""
    from lotus.models import LM

    if type(method) is not FilterMethod:
        raise TypeError("an explicit ordinary Filter method is required")
    options = dict(top_p=generation.top_p, stop=list(generation.stop),
                   presence_penalty=generation.presence_penalty,
                   frequency_penalty=generation.frequency_penalty)
    if generation.seed is not None:
        options["seed"] = generation.seed
    validate_generation(method, generation)
    _filter_instruction(method, require_bound=False)
    return LM(method.model_name, temperature=generation.temperature,
              max_tokens=generation.max_tokens, **options)


def validate_generation(method, generation):
    if type(method) is not FilterMethod:
        raise TypeError("an explicit ordinary Filter method is required")
    method.describe()
    if generation.repetition_penalty != 1 or generation.logprobs is not None:
        raise ValueError("ordinary Filter does not support repetition overrides or helper logprobs")
    if getattr(generation, "response_format", None) is not None:
        raise ValueError("ordinary Filter does not support response_format overrides")


def validate_lm(lm, method, generation):
    """Reject unsupported cache/rate policies before creating any execution owner."""
    import lotus
    from lotus.types import SerializationFormat

    if lotus.settings.enable_cache:
        raise ValueError("records Filter requires result caching disabled before execution")
    if lotus.settings.serialization_format != SerializationFormat.DEFAULT:
        raise ValueError("records Filter requires the original DEFAULT serialization")
    if lm.model != method.model_name:
        raise ValueError("LOTUS and local-engine method model identities differ")
    if lm.rate_limit is not None or lm.tpm_limit is not None:
        raise ValueError("records Filter does not implement LM rate or TPM limiting")
    expected = make_lm(method, generation).kwargs
    if lm.kwargs != expected:
        raise ValueError("LOTUS effective generation options differ from the local engine")


def _input_instruction(instruction, resolved_input_columns, *, require_bound=True):
    import lotus

    parsed = lotus.nl_expression.parse_cols(instruction)
    if not parsed:
        raise ValueError("ordinary record instruction must reference an input column")
    if resolved_input_columns is None:
        if require_bound:
            raise ValueError("unbound input columns; bind the method in the caller before worker preparation")
        columns = parsed
    else:
        if set(parsed) != set(resolved_input_columns):
            raise ValueError("resolved_input_columns differ from the original instruction references")
        columns = list(resolved_input_columns)
    return columns, lotus.nl_expression.nle2str(instruction, columns)


def _filter_instruction(method, *, require_bound=True):
    if type(method) is not FilterMethod:
        raise TypeError("an explicit ordinary Filter method is required")
    method.describe()
    return _input_instruction(method.instruction, method.resolved_input_columns, require_bound=require_bound)


def bind_filter_method(method):
    """Freeze the caller's original LOTUS column order before serialization.

    A previously bound method retains that order after validating its reference
    set. Worker preparation requires this value and never binds an unbound method.
    """
    validate_filter_source()
    columns, _ = _filter_instruction(method, require_bound=False)
    return replace(method, resolved_input_columns=tuple(columns))


class _FilterFormatter:
    """One original Filter preparation path for single calls and record stages."""

    def __init__(self, method, model):
        from lotus.templates import task_instructions

        self.method, self.model = FilterMethod.from_dict(method.describe()), model
        self.columns, self.instruction = _filter_instruction(self.method)
        self.instructions = task_instructions

    def messages(self, rows, *, index=None, allow_nulls=False):
        import pandas as pd

        if type(allow_nulls) is not bool:
            raise TypeError("allow_nulls must be a boolean")
        if not rows:
            return []
        if any(name not in row or not (type(row[name]) is str or allow_nulls and row[name] is None)
               for row in rows for name in self.columns):
            raise TypeError("Filter records require present text input columns")
        frame = pd.DataFrame({name: [row[name] for row in rows] for name in self.columns}, index=index)
        docs = self.instructions.df2multimodal_info(frame, self.columns)
        return [self.instructions.filter_formatter(
            self.model, doc, self.instruction, system_prompt=self.method.system_prompt,
            output_tokens=self.method.output_tokens,
        ) for doc in docs]


class LotusFilterMethod(_FilterFormatter):
    """One worker-local formatter; supplied call identities remain unchanged."""

    def __init__(self, method):
        validate_filter_source()
        import lotus
        from lotus.models import LM
        from lotus.types import SerializationFormat

        if type(method) is not FilterMethod:
            raise TypeError("an explicit ordinary Filter method is required")
        if lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("records Filter requires DEFAULT serialization")
        _filter_instruction(method)
        # The formatter uses LM's model-name methods. Construction creates no
        # model weights, provider client, request executor, or network connection.
        super().__init__(method, LM(method.model_name, max_tokens=8))

    def prepare_rows(self, rows):
        started = time.monotonic_ns()
        messages = self.messages(rows)
        result = []
        for row, message in zip(rows, messages, strict=True):
            identity_keys = ("query_id", "model_role", "algorithm_stage", "row_id",
                             "source_position", "call_index", "call_id")
            value = {name: row[name] for name in identity_keys}
            value.update(messages_json=json.dumps(message, ensure_ascii=False, allow_nan=False),
                         prepare_worker_pid=os.getpid(), prepare_started_ns=started,
                         prepare_finished_ns=time.monotonic_ns())
            result.append(value)
        return result


def prepare_rows(rows, method):
    return LotusFilterMethod(method).prepare_rows(rows)


def validate_engine_output(result):
    """Validate terminal answer structure without accounting or postprocessing."""
    tokens = [result.get(key) for key in ("prompt_tokens", "completion_tokens", "total_tokens")]
    if any(type(value) is not int or value < 0 for value in tokens):
        raise ValueError("successful engine output requires authoritative token usage")
    if tokens[2] != tokens[0] + tokens[1]:
        raise ValueError("engine total token usage differs from its components")
    if result.get("status") != "succeeded" or type(result.get("text")) is not str:
        raise ValueError("only successful engine text is a semantic answer")
    if result.get("finish_reason") not in ("stop", "length"):
        raise ValueError("unfinished or aborted engine output is not a semantic answer")
    return tokens


def engine_response(result, lm):
    """Build a native accounting response; semantic use also checks the protocol."""
    from litellm.types.utils import ModelResponse

    tokens = validate_engine_output(result)
    return ModelResponse(
        id=result["engine_request_id"], model=lm.model, object="chat.completion",
        created=int(time.time()),
        choices=[dict(index=0, finish_reason=result["finish_reason"],
                      message=dict(role="assistant", content=result["text"]))],
        usage=dict(zip(("prompt_tokens", "completion_tokens", "total_tokens"), tokens)),
    )


def validate_engine_protocol(result):
    """A retained receipt is accounting evidence, not permission to parse a batch."""
    if "engine_protocol_error" in result:
        raise ValueError("engine batch protocol failed; retained receipts cannot be semantic answers")


def interpret_results(results, lm, method):
    """Preserve native accounting-before-choice/parse behavior for successful calls.

    The independent query ledger also retains failed/unknown calls and known
    usage that cannot be represented as a valid native chat response.
    """
    from lotus.sem_ops.postprocessors import filter_postprocess

    responses, invalid = [], []
    for result in results:
        try:
            response = engine_response(result, lm)
            responses.append(response)
            validate_engine_protocol(result)
        except ValueError as error:
            invalid.append((result, error))
    for response in responses:
        # Original LOTUS owns physical/virtual usage and finite usage limits.
        lm._update_stats(response, is_cached=False)
    if invalid:
        error = RuntimeError("Filter execution contains failed, unknown, or invalid engine calls")
        error.engine_failures = invalid
        raise error from invalid[0][1]
    texts = [lm._get_top_choice(response) for response in responses]
    parsed = filter_postprocess(texts, lm, method.default, output_tokens=method.output_tokens)
    return [dict(result, decision=decision, explanation=explanation, raw_output=raw)
            for result, decision, explanation, raw in
            zip(results, parsed.outputs, parsed.explanations, parsed.raw_outputs, strict=True)]


def lm_report(lm):
    stats = lm.stats
    value = stats.model_dump() if hasattr(stats, "model_dump") else asdict(stats)
    return dict(stats=value, cost_available=False,
                cost_reason="local engine compute is not assigned an API dollar price")
