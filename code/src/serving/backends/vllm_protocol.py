"""Shared vLLM values, interpreter checks and request/result conversion.

This module loads no model and owns no synchronous or asynchronous instance.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys

VLLM_VERSION = "0.25.1"
MAX_LOGPROBS = 20
IDENTITY_FIELDS = (
    "query_id", "model_role", "algorithm_stage", "row_id", "source_position",
    "call_index", "call_id",
)


def _integer(value: object, name: str, *, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}; bool is not accepted")


def _number(value: object, name: str, *, low: float, high: float | None = None,
            inclusive_low: bool = True) -> None:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number; bool is not accepted")
    if value < low or (not inclusive_low and value == low):
        raise ValueError(f"{name} is below its supported range")
    if high is not None and value > high:
        raise ValueError(f"{name} is above its supported range")


def _string(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")


@dataclass(frozen=True)
class GenerationSpec:
    max_tokens: int = 8
    temperature: float = 0.0
    top_p: float = 1.0
    seed: int | None = None
    stop: tuple[str, ...] = ("\n",)
    presence_penalty: float = 0.0
    frequency_penalty: float = 0.0
    repetition_penalty: float = 1.0
    logprobs: int | None = None
    response_format: dict[str, str] | None = None

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "stop", tuple(self.stop))
        if self.response_format is not None:
            object.__setattr__(self, "response_format", dict(self.response_format))

    def _validate(self) -> None:
        _integer(self.max_tokens, "max_tokens", minimum=1)
        _number(self.temperature, "temperature", low=0.0, high=2.0)
        _number(self.top_p, "top_p", low=0.0, high=1.0, inclusive_low=False)
        if self.seed is not None:
            _integer(self.seed, "seed")
        if not isinstance(self.stop, (tuple, list)) or any(
            not isinstance(item, str) or not item for item in self.stop
        ):
            raise ValueError("stop must be a sequence of nonempty strings")
        _number(self.presence_penalty, "presence_penalty", low=-2.0, high=2.0)
        _number(self.frequency_penalty, "frequency_penalty", low=-2.0, high=2.0)
        _number(self.repetition_penalty, "repetition_penalty", low=0.0,
                inclusive_low=False)
        if self.logprobs is not None:
            _integer(self.logprobs, "logprobs")
            if self.logprobs > MAX_LOGPROBS:
                raise ValueError(f"logprobs exceeds the local engine limit of {MAX_LOGPROBS}")
        if self.response_format is not None and (
            type(self.response_format) is not dict
            or set(self.response_format) != {"type"}
            or type(self.response_format["type"]) is not str
            or self.response_format["type"] != "json_object"
        ):
            raise ValueError("response_format supports only None or {'type': 'json_object'}")

    @classmethod
    def from_dict(cls, value: dict) -> GenerationSpec:
        if not isinstance(value, dict):
            raise TypeError("generation must be an object")
        return cls(**value)

    def kwargs(self) -> dict:
        """Return plain sampling options; the model boundary materializes structured outputs."""
        self._validate()
        values = asdict(self)
        response_format = values.pop("response_format")
        values.update(stop=list(self.stop), n=1)
        if response_format is not None:
            values["structured_outputs"] = {"json_object": True}
        return values


_PROBE = """
import importlib.metadata, importlib.util, json, sys
try:
    version = importlib.metadata.version('vllm')
except importlib.metadata.PackageNotFoundError:
    version = None
print(json.dumps({
    'executable': sys.executable, 'prefix': sys.prefix, 'base_prefix': sys.base_prefix,
    'python_version': list(sys.version_info[:3]), 'vllm_version': version,
    'lotus_available': importlib.util.find_spec('lotus') is not None,
}))
"""


def _probe_python(executable: str) -> dict:
    result = subprocess.run(
        [executable, "-I", "-c", _PROBE], check=True, capture_output=True,
        text=True, timeout=10.0,
    )
    identity = json.loads(result.stdout)
    if not isinstance(identity, dict):
        raise ValueError("interpreter probe did not return an object")
    for name in ("executable", "prefix", "base_prefix"):
        _string(identity.get(name), f"interpreter.{name}")
    return identity


def _prefix(value: str) -> Path:
    return Path(value).expanduser().resolve()


@dataclass(frozen=True)
class EngineSpec:
    model_path: str
    model_python: str
    driver_python: str
    tokenizer_path: str | None = None
    dtype: str | None = None
    max_model_len: int | None = None
    max_num_seqs: int | None = None
    max_num_batched_tokens: int | None = None
    gpu_memory_utilization: float | None = None
    seed: int = 0
    chat_template: str | None = None
    max_model_calls: int | None = None
    enable_mfu_metrics: bool = False

    def __post_init__(self) -> None:
        if type(self.enable_mfu_metrics) is not bool:
            raise TypeError("enable_mfu_metrics must be boolean")
        for name in ("model_path", "model_python", "driver_python"):
            _string(getattr(self, name), name)
        if self.tokenizer_path is not None:
            _string(self.tokenizer_path, "tokenizer_path")
        if self.dtype is not None and self.dtype not in {"auto", "half", "float16", "bfloat16", "float", "float32"}:
            raise ValueError("unsupported dtype")
        for name in ("max_model_len", "max_num_seqs", "max_num_batched_tokens"):
            if getattr(self, name) is not None:
                _integer(getattr(self, name), name, minimum=1)
        _integer(self.seed, "seed")
        if self.gpu_memory_utilization is not None:
            _number(self.gpu_memory_utilization, "gpu_memory_utilization", low=0.0,
                    high=1.0, inclusive_low=False)
        if self.chat_template is not None:
            _string(self.chat_template, "chat_template")
        if self.max_model_calls is not None:
            _integer(self.max_model_calls, "max_model_calls", minimum=1)

    def optional_kwargs(self) -> dict:
        return {name: getattr(self, name) for name in (
            "dtype", "max_model_len", "max_num_seqs", "max_num_batched_tokens",
            "gpu_memory_utilization") if getattr(self, name) is not None}

    @classmethod
    def from_dict(cls, value: dict) -> EngineSpec:
        if not isinstance(value, dict):
            raise TypeError("engine must be an object")
        return cls(**value)

    def validate_local(self) -> dict:
        """Read local artifacts and probe environments without loading either ML stack."""
        for name in ("model_path", "tokenizer_path"):
            value = getattr(self, name)
            if value is not None and (not Path(value).is_absolute() or not Path(value).is_dir()):
                raise ValueError(f"{name} must be an existing absolute local directory")
        for name in ("model_python", "driver_python"):
            path = Path(getattr(self, name))
            if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
                raise ValueError(f"{name} must be an existing absolute Python executable")
        model = _probe_python(self.model_python)
        driver = _probe_python(self.driver_python)
        if _prefix(model["prefix"]) == _prefix(driver["prefix"]):
            raise ValueError("model and driver must have different Python environment prefixes")
        if model.get("vllm_version") != VLLM_VERSION:
            raise ValueError(f"model environment must contain vllm=={VLLM_VERSION}")
        if model.get("lotus_available") is not False:
            raise ValueError("model environment must be separate from the LOTUS installation")
        return {"model": model, "driver": driver}

    def validate_driver_environment(self) -> dict:
        """Bind public graph preflight to the interpreter that owns LOTUS preparation."""
        identity = self.validate_local()
        driver = identity["driver"]
        if (_prefix(sys.prefix) != _prefix(driver["prefix"])
                or Path(sys.executable).absolute().parent.resolve() !=
                Path(driver["executable"]).absolute().parent.resolve()
                or _prefix(sys.base_prefix) != _prefix(driver["base_prefix"])):
            raise RuntimeError("construct the records graph with its configured driver_python")
        return identity


def _validate_current_runtime(identity: dict) -> None:
    model = identity["model"]
    if _prefix(sys.prefix) != _prefix(model["prefix"]):
        raise RuntimeError("construct VllmEngine with model_python, outside the driver environment")
    # Compare the environment and executable directory, not the resolved binary:
    # different venvs commonly symlink to the same base Python binary.
    if Path(sys.executable).absolute().parent.resolve() != (
        Path(model["executable"]).absolute().parent.resolve()
    ) or _prefix(sys.base_prefix) != _prefix(model["base_prefix"]):
        raise RuntimeError("current Python does not match the probed model interpreter")
    if any(name == "lotus" or name.startswith("lotus.") for name in sys.modules):
        raise RuntimeError("LOTUS must not be imported in the local model process")


def _token_ids(value: object, name: str) -> list[int] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or any(
        type(item) is not int or item < 0 for item in value
    ):
        raise ValueError(f"{name} must contain nonnegative integer token IDs")
    return list(value)


def _chat_logprobs(token_ids: list[int] | None, values: object, count: int,
                   get_tokenizer) -> dict:
    """Map genuine v0.25.1 output using its OpenAI chat conversion and order.

    Unlike the server's missing-value sentinel branch, incomplete requested
    probabilities fail validation. A real -inf is clamped by the official rule.
    """
    if (token_ids is None or not isinstance(values, Sequence)
            or isinstance(values, (str, bytes)) or len(values) != len(token_ids)):
        raise ValueError("requested logprobs must match every generated token ID")
    tokenizer = None

    def entry(token_id, value):
        nonlocal tokenizer
        _integer(token_id, "logprob token ID")
        probability, decoded = value.logprob, value.decoded_token
        if (type(probability) not in (int, float) or probability != probability
                or probability > 0):
            raise ValueError("raw token logprob must be nonpositive and not NaN")
        if decoded is not None and not isinstance(decoded, str):
            raise ValueError("decoded logprob token must be text or None")
        token = decoded
        if token is None:
            try:
                if tokenizer is None:
                    tokenizer = get_tokenizer()
                token = tokenizer.decode([token_id])
            except Exception as exc:
                raise ValueError("requested logprob token cannot be decoded") from exc
        if not isinstance(token, str):
            raise ValueError("decoded logprob token must be text")
        return dict(token=token, logprob=max(probability, -9999.0),
                    bytes=None if decoded is None else list(decoded.encode("utf-8", errors="replace")))

    content = []
    for token_id, step in zip(token_ids, values, strict=True):
        if (not isinstance(step, dict) or token_id not in step
                or len(step) > count + 1):
            raise ValueError("requested logprobs require the actual sampled token and bounded candidates")
        # Validate even candidates outside the server's first-K projection.
        candidates = {key: entry(key, value) for key, value in step.items()}
        chosen = dict(candidates[token_id])
        chosen["top_logprobs"] = [
            dict(item, bytes=list(item["token"].encode("utf-8", errors="replace")))
            for item in list(candidates.values())[:count]
        ]
        content.append(chosen)
    return dict(content=content)


def _prepared(rows: list[dict]) -> tuple[list[dict], list[list[dict]]]:
    if not isinstance(rows, list):
        raise ValueError("prepared rows must be a finite list")
    identities, conversations, seen = [], [], set()
    for row in rows:
        if not isinstance(row, dict) or any(name not in row for name in IDENTITY_FIELDS):
            raise ValueError("prepared row is missing complete call identity")
        identity = {name: row[name] for name in IDENTITY_FIELDS}
        for name in ("query_id", "model_role", "algorithm_stage", "call_id"):
            _string(identity[name], name)
        if type(identity["row_id"]) not in (str, int):
            raise ValueError("row_id must be a string or integer")
        for name in ("source_position", "call_index"):
            _integer(identity[name], name)
        if identity["call_id"] in seen:
            raise ValueError("call_id must be unique within a batch")
        seen.add(identity["call_id"])
        if not isinstance(row.get("messages_json"), str):
            raise ValueError("messages_json must encode an untemplated conversation")
        conversation = json.loads(row["messages_json"])
        if not isinstance(conversation, list) or not conversation:
            raise ValueError("messages_json must contain a nonempty conversation")
        for message in conversation:
            if not isinstance(message, dict) or set(message) != {"role", "content"}:
                raise ValueError("ordinary-filter messages require role and content only")
            if message["role"] not in {"system", "user", "assistant"}:
                raise ValueError("ordinary-filter message role is unsupported")
            content = message["content"]
            if not isinstance(content, str):
                if not isinstance(content, list) or any(
                    not isinstance(part, dict) or set(part) != {"type", "text"}
                    or part["type"] != "text" or not isinstance(part["text"], str)
                    for part in content
                ):
                    raise ValueError("ordinary-filter messages support text content only")
        identity["messages_sha256"] = hashlib.sha256(json.dumps(
            conversation, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        ).encode()).hexdigest()
        identities.append(identity)
        conversations.append(conversation)
    return identities, conversations


def result_record(identity: dict, *, engine_uid: str, start: float, end: float,
                  status: str, submitted: bool, error_type: str | None = None) -> dict:
    """Build the existing terminal envelope without inferring output or usage."""
    return {
        **identity, "status": status, "text": None,
        "prompt_token_ids_json": "null", "output_token_ids_json": "null",
        "logprobs_json": "null", "prompt_tokens": None, "completion_tokens": None,
        "total_tokens": None, "finish_reason": None, "engine_request_id": None,
        "engine_uid": engine_uid, "model_start_monotonic": start,
        "model_end_monotonic": end, "model_elapsed_s": max(0.0, end - start),
        "submission_confirmed": submitted, "deadline_expired": False,
        "error_type": error_type,
    }


def result_from_output(identity: dict, output, *, engine_uid: str, start: float,
                       end: float, generation: GenerationSpec, get_tokenizer) -> dict:
    """Convert one associated RequestOutput, retaining evidence before validation errors."""
    result = result_record(identity, engine_uid=engine_uid, start=start, end=end,
                           status="failed", submitted=True)
    result["engine_request_id"] = output.request_id
    try:
        prompt_ids = _token_ids(getattr(output, "prompt_token_ids", None),
                                "prompt_token_ids")
        result["prompt_token_ids_json"] = json.dumps(prompt_ids)
        result["prompt_tokens"] = None if prompt_ids is None else len(prompt_ids)
        choices = getattr(output, "outputs", None)
        if not isinstance(choices, (list, tuple)) or len(choices) != 1:
            raise ValueError("expected one completion choice")
        choice = choices[0]
        token_ids = _token_ids(getattr(choice, "token_ids", None), "token_ids")
        result["output_token_ids_json"] = json.dumps(token_ids)
        result["completion_tokens"] = None if token_ids is None else len(token_ids)
        if result["prompt_tokens"] is not None and token_ids is not None:
            result["total_tokens"] = result["prompt_tokens"] + len(token_ids)
        result["finish_reason"] = getattr(choice, "finish_reason", None)
        if (output.finished is not True or type(getattr(choice, "index", None)) is not int
                or choice.index != 0 or result["finish_reason"] not in {"stop", "length"}
                or not isinstance(getattr(choice, "text", None), str)):
            raise ValueError("completion is not a valid finished single-choice result")
        if generation.logprobs is not None:
            result["logprobs_json"] = json.dumps(_chat_logprobs(
                token_ids, getattr(choice, "logprobs", None),
                generation.logprobs, get_tokenizer,
            )["content"], ensure_ascii=False, allow_nan=False)
        result.update(status="succeeded", text=choice.text)
    except (AttributeError, TypeError, ValueError) as exc:
        result["error_type"] = type(exc).__name__
    return result



def sampling_params(values, sampling_type, structured_outputs_type):
    """Materialize the same sampling values for either native execution API."""
    options = dict(values)
    if "structured_outputs" in options:
        options["structured_outputs"] = structured_outputs_type(**options["structured_outputs"])
    return sampling_type(**options)
