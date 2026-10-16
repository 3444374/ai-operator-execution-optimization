"""Ordinary Movie Filter formatting and native LOTUS result interpretation.

The execution backends receive records and return engine results. This module
does not create a model, send HTTP, run a vendor graph, or own its scheduling.
"""

from dataclasses import asdict
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
    return LM(method.model_name, temperature=generation.temperature,
              max_tokens=generation.max_tokens, **options)


def validate_generation(method, generation):
    if type(method) is not FilterMethod:
        raise TypeError("an explicit ordinary Filter method is required")
    if generation.repetition_penalty != 1 or generation.logprobs is not None:
        raise ValueError("ordinary Filter does not support repetition overrides or helper logprobs")


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


class LotusFilterMethod:
    """One worker-local formatter; no driver LM is serialized to a worker."""

    def __init__(self, method):
        validate_filter_source()
        import lotus
        from lotus.models import LM
        from lotus.templates import task_instructions
        from lotus.types import SerializationFormat

        if type(method) is not FilterMethod:
            raise TypeError("an explicit ordinary Filter method is required")
        if lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("records Filter requires DEFAULT serialization")
        # The formatter uses LM's model-name methods. Construction creates no
        # model weights, provider client, request executor, or network connection.
        self.model = LM(method.model_name, max_tokens=8)
        self.method = method
        self.columns = lotus.nl_expression.parse_cols(method.instruction)
        if self.columns != ["reviewText"]:
            raise ValueError("ordinary Movie Q3 must reference only reviewText")
        self.instruction = lotus.nl_expression.nle2str(method.instruction, self.columns)
        self.instructions = task_instructions

    def prepare_rows(self, rows):
        import pandas as pd

        started = time.monotonic_ns()
        if not rows:
            return []
        for row in rows:
            if type(row.get("reviewText")) is not str:
                raise TypeError("Movie records require an unchanged text review")
        frame = pd.DataFrame({"reviewText": [row["reviewText"] for row in rows]})
        # This is the original dtype-aware preparation. A plain string column
        # is text; no column-name heuristic reinterprets it as an image.
        docs = self.instructions.df2multimodal_info(frame, self.columns)
        result = []
        for row, doc in zip(rows, docs, strict=True):
            messages = self.instructions.filter_formatter(
                self.model, doc, self.instruction, system_prompt=self.method.system_prompt,
                output_tokens=self.method.output_tokens,
            )
            identity_keys = ("query_id", "model_role", "algorithm_stage", "row_id",
                             "source_position", "call_index", "call_id")
            value = {name: row[name] for name in identity_keys}
            value.update(messages_json=json.dumps(messages, ensure_ascii=False, allow_nan=False),
                         prepare_worker_pid=os.getpid(), prepare_started_ns=started,
                         prepare_finished_ns=time.monotonic_ns())
            result.append(value)
        return result


def prepare_rows(rows, method):
    return LotusFilterMethod(method).prepare_rows(rows)


def engine_response(result, lm):
    """Translate engine output to the original LM response type, without HTTP replay."""
    from litellm.types.utils import ModelResponse

    tokens = [result.get(key) for key in ("prompt_tokens", "completion_tokens", "total_tokens")]
    if any(type(value) is not int or value < 0 for value in tokens):
        raise ValueError("successful engine output requires authoritative token usage")
    if tokens[2] != tokens[0] + tokens[1]:
        raise ValueError("engine total token usage differs from its components")
    if result.get("status") != "succeeded" or type(result.get("text")) is not str:
        raise ValueError("only successful engine text is a semantic answer")
    if result.get("finish_reason") not in ("stop", "length"):
        raise ValueError("unfinished or aborted engine output is not a semantic answer")
    return ModelResponse(
        id=result["engine_request_id"], model=lm.model, object="chat.completion",
        created=int(time.time()),
        choices=[dict(index=0, finish_reason=result["finish_reason"],
                      message=dict(role="assistant", content=result["text"]))],
        usage=dict(zip(("prompt_tokens", "completion_tokens", "total_tokens"), tokens)),
    )


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
        except ValueError as error:
            invalid.append((result, error))
        else:
            responses.append(response)
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
