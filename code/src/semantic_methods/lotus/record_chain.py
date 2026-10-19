"""Ordinary LOTUS record methods composed into finite worker-local sequences.

Record membership, data columns and model evaluations have separate identities.
This module prepares and interprets finite worker-local batches; the backend
owns their physical batching, model instance and lifetime.
"""

from copy import deepcopy
from dataclasses import asdict, dataclass, field
import hashlib
from importlib.metadata import distribution
import json
import os
import time

from .engine import engine_response, lm_report, validate_filter_source
from .records import FilterMethod
from .sdk import SOURCE_HASHES
from src.serving.backends.vllm_engine import GenerationSpec, IDENTITY_FIELDS


CHAIN_SOURCE_HASHES = {
    "sem_ops/sem_map.py": SOURCE_HASHES["sem_ops/sem_map.py"],
    "sem_ops/sem_extract.py": "0f58eda86d645b85b8659591d61366a86809a4d189dbec6c491fe5b4628c163a",
    "types.py": "73b0ebb4a8a9606cdec6df27353d57fb28ce6e2f5800c539caee892eb4f2cc6b",
}


def _name(value, label):
    if type(value) is not str or not value.strip():
        raise ValueError(label + " must be a nonempty string")


def validate_chain_source():
    evidence = validate_filter_source()
    root = distribution("lotus-ai").locate_file("lotus")
    for relative, digest in CHAIN_SOURCE_HASHES.items():
        if hashlib.sha256((root / relative).read_bytes()).hexdigest() != digest:
            raise ValueError("records chain source differs at lotus/" + relative)
    return dict(evidence, profile="lotus-ordinary-record-methods-v1",
                source_hashes={**evidence["source_hashes"], **CHAIN_SOURCE_HASHES})


@dataclass(frozen=True)
class TextFilterMethod:
    """Ordinary text Filter with explicit input references in its instruction.

    The existing FilterMethod remains the separate fixed Movie Q3 contract.
    """

    model_name: str
    instruction: str
    default: bool = True
    output_tokens: tuple[str, str] = ("True", "False")
    system_prompt: str | None = None

    def __post_init__(self):
        for name in ("model_name", "instruction"):
            _name(getattr(self, name), "Filter " + name)
        if type(self.default) is not bool:
            raise ValueError("Filter default must be a boolean")
        if type(self.output_tokens) is not tuple or self.output_tokens != ("True", "False"):
            raise ValueError("ordinary text Filter requires the original True/False output tokens")
        if self.system_prompt is not None and type(self.system_prompt) is not str:
            raise ValueError("Filter system_prompt must be a string or None")


@dataclass(frozen=True)
class MapMethod:
    model_name: str
    instruction: str
    output_column: str = "summary"
    system_prompt: str | None = None

    def __post_init__(self):
        for name in ("model_name", "instruction", "output_column"):
            _name(getattr(self, name), "Map " + name)
        if self.system_prompt is not None and type(self.system_prompt) is not str:
            raise ValueError("Map system_prompt must be a string or None")


@dataclass(frozen=True)
class ExtractMethod:
    model_name: str
    input_columns: tuple[str, ...]
    output_columns: dict[str, str | None]
    extract_quotes: bool = False

    def __post_init__(self):
        _name(self.model_name, "Extract model_name")
        if type(self.input_columns) not in (tuple, list) or not self.input_columns:
            raise ValueError("Extract requires input columns")
        for name in self.input_columns:
            _name(name, "Extract input column")
        if len(set(self.input_columns)) != len(self.input_columns):
            raise ValueError("Extract input columns must be unique")
        if type(self.output_columns) is not dict or not self.output_columns:
            raise ValueError("Extract requires output column descriptions")
        for name, description in self.output_columns.items():
            _name(name, "Extract output column")
            if description is not None and type(description) is not str:
                raise ValueError("Extract descriptions must be strings or None")
        if type(self.extract_quotes) is not bool:
            raise ValueError("extract_quotes must be a boolean")
        object.__setattr__(self, "input_columns", tuple(self.input_columns))
        object.__setattr__(self, "output_columns", deepcopy(self.output_columns))


@dataclass(frozen=True)
class RecordOccurrence:
    """One relation member; values may retain grouping and correlation columns.

    source_refs preserves named source identities (for example left/right) as
    opaque JSON metadata. It does not execute an association or imply Join support.
    """

    row_id: str
    source_position: int
    source_index: str | int
    values: dict
    source_refs: dict = field(default_factory=dict)
    index_updates: dict = field(default_factory=dict)

    def __post_init__(self):
        _name(self.row_id, "row_id")
        if type(self.source_position) is not int or self.source_position < 0:
            raise ValueError("source_position must be a nonnegative integer")
        if type(self.source_index) not in (str, int):
            raise ValueError("source_index must be a string or integer label")
        if type(self.values) is not dict or any(type(k) is not str for k in self.values):
            raise ValueError("record values must be a dictionary with string columns")
        if type(self.source_refs) is not dict or type(self.index_updates) is not dict:
            raise ValueError("source_refs and index_updates must be objects")
        # Cross-worker values are finite JSON, not LM/client or framework objects.
        payload = [self.values, self.source_refs, self.index_updates]
        if json.loads(json.dumps(payload, allow_nan=False)) != payload:
            raise ValueError("record data must preserve its types across JSON serialization")
        object.__setattr__(self, "values", deepcopy(self.values))
        object.__setattr__(self, "source_refs", deepcopy(self.source_refs))
        object.__setattr__(self, "index_updates", deepcopy(self.index_updates))

    def describe(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        return cls(**deepcopy(value))


def stage_identity(query_id, record, stage_id, *, model_role="main", call_index=0):
    """An extensible stage name and evaluation ordinal, independent of membership."""
    for name, value in (("query_id", query_id), ("stage_id", stage_id), ("model_role", model_role)):
        _name(value, name)
    if type(call_index) is not int or call_index < 0:
        raise ValueError("call_index must be a nonnegative integer")
    call_id = "lotus-record-method-v1:" + json.dumps(
        [query_id, stage_id, model_role, record.row_id, record.source_position, call_index],
        ensure_ascii=False, separators=(",", ":"),
    )
    return dict(query_id=query_id, model_role=model_role, algorithm_stage=stage_id,
                row_id=record.row_id, source_position=record.source_position,
                call_index=call_index, call_id=call_id)


@dataclass(frozen=True)
class StageSpec:
    stage_id: str
    method: FilterMethod | TextFilterMethod | MapMethod | ExtractMethod
    generation: GenerationSpec
    model_role: str = "main"

    def __post_init__(self):
        _name(self.stage_id, "stage_id")
        _name(self.model_role, "model_role")
        if type(self.method) not in (FilterMethod, TextFilterMethod, MapMethod, ExtractMethod):
            raise TypeError("an explicit ordinary record method is required")
        self.method.__post_init__()
        if type(self.generation) is not GenerationSpec:
            raise TypeError("generation must use GenerationSpec")
        self.generation.__post_init__()
        response_format = getattr(self.generation, "response_format", None)
        expected = {"type": "json_object"} if type(self.method) is ExtractMethod else None
        if response_format != expected:
            raise ValueError("ordinary Extract requires json_object; Filter/Map require no response format")
        if self.generation.repetition_penalty != 1:
            raise ValueError("this ordinary LOTUS profile does not support repetition overrides")

    @property
    def kind(self):
        if type(self.method) in (FilterMethod, TextFilterMethod):
            return "filter"
        return "map" if type(self.method) is MapMethod else "extract"

    def describe(self):
        return dict(stage_id=self.stage_id, method_type=type(self.method).__name__,
                    method=asdict(self.method), generation=asdict(self.generation),
                    model_role=self.model_role)

    @classmethod
    def from_dict(cls, value):
        kinds = {kind.__name__: kind for kind in (FilterMethod, TextFilterMethod, MapMethod, ExtractMethod)}
        options = deepcopy(value)
        kind = kinds[options.pop("method_type")]
        if kind is TextFilterMethod and "output_tokens" in options["method"]:
            options["method"]["output_tokens"] = tuple(options["method"]["output_tokens"])
        options["method"] = (FilterMethod.from_dict(options["method"]) if kind is FilterMethod
                             else kind(**options["method"]))
        options["generation"] = GenerationSpec.from_dict(options["generation"])
        return cls(**options)


class LotusRecordMethod:
    """Original ordinary formatter/parser and accounting for one named stage."""

    def __init__(self, stage):
        import lotus
        from lotus.models import LM
        from lotus.templates import task_instructions
        from lotus.types import SerializationFormat

        stage.__post_init__()
        validate_chain_source()
        if lotus.settings.enable_cache or lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("record methods require caching disabled and DEFAULT serialization")
        self.stage = StageSpec.from_dict(stage.describe())
        self.method = self.stage.method
        generation = self.stage.generation
        options = dict(max_tokens=generation.max_tokens, temperature=generation.temperature,
                       top_p=generation.top_p, stop=list(generation.stop),
                       presence_penalty=generation.presence_penalty,
                       frequency_penalty=generation.frequency_penalty)
        if generation.seed is not None:
            options["seed"] = generation.seed
        if generation.logprobs is not None:
            options.update(logprobs=True, top_logprobs=generation.logprobs)
        self.lm = LM(self.method.model_name, **options)
        self.instructions = task_instructions
        if type(self.method) is ExtractMethod:
            self.columns = list(self.method.input_columns)
            self.instruction = None
        else:
            self.columns = lotus.nl_expression.parse_cols(self.method.instruction)
            self.instruction = lotus.nl_expression.nle2str(self.method.instruction, self.columns)
        if not self.columns:
            raise ValueError("a record method must reference an input column")

    def prepare_rows(self, records, *, query_id):
        import lotus
        import pandas as pd
        from lotus.types import SerializationFormat

        if lotus.settings.enable_cache or lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("record methods require caching disabled and DEFAULT serialization")
        for record in records:
            if any(type(record.values.get(name)) is not str for name in self.columns):
                raise ValueError("ordinary record methods require present text input columns")
        if not records:
            return []
        started = time.monotonic_ns()
        frame = pd.DataFrame([record.values for record in records],
                             index=[record.source_index for record in records])
        docs = self.instructions.df2multimodal_info(frame, self.columns)
        result = []
        for record, doc in zip(records, docs, strict=True):
            if self.stage.kind == "filter":
                messages = self.instructions.filter_formatter(
                    self.lm, doc, self.instruction, system_prompt=self.method.system_prompt,
                    output_tokens=self.method.output_tokens)
            elif type(self.method) is MapMethod:
                messages = self.instructions.map_formatter(
                    self.lm, doc, self.instruction, system_prompt=self.method.system_prompt)
            else:
                messages = self.instructions.extract_formatter(
                    self.lm, doc, self.method.output_columns, self.method.extract_quotes)
            result.append(dict(
                stage_identity(query_id, record, self.stage.stage_id, model_role=self.stage.model_role),
                messages_json=json.dumps(messages, ensure_ascii=False, allow_nan=False),
                prepare_worker_pid=os.getpid(), prepare_started_ns=started,
                prepare_finished_ns=time.monotonic_ns(),
            ))
        return result

    def interpret_results(self, results):
        from lotus.sem_ops.postprocessors import extract_postprocess, filter_postprocess, map_postprocess

        responses, invalid = [], []
        for result in results:
            try:
                responses.append(engine_response(result, self.lm))
            except ValueError as error:
                invalid.append(error)
        for response in responses:
            self.lm._update_stats(response, is_cached=False)
        if invalid:
            raise RuntimeError("record method has failed, unknown, or invalid engine calls") from invalid[0]
        texts = [self.lm._get_top_choice(response) for response in responses]
        if self.stage.kind == "filter":
            return filter_postprocess(texts, self.lm, self.method.default,
                                      output_tokens=self.method.output_tokens)
        if type(self.method) is MapMethod:
            return map_postprocess(texts, self.lm, False)
        return extract_postprocess(texts, self.lm, False)


class LotusRecordChain:
    """Compose supported record methods without introducing a query compiler.

    Extract is terminal because its original index-label assignment is applied
    at final relation assembly. Nonterminal label updates need a separate contract.
    """

    def __init__(self, stages, *, query_id):
        _name(query_id, "query_id")
        if type(stages) is not tuple or not stages or any(type(s) is not StageSpec for s in stages):
            raise ValueError("a finite nonempty tuple of StageSpec is required")
        if len({s.stage_id for s in stages}) != len(stages):
            raise ValueError("each operator occurrence needs a distinct stage_id")
        if any(s.kind == "extract" for s in stages[:-1]):
            raise ValueError("Extract must be terminal before final index-label assignment")
        self.query_id = query_id
        self.methods = tuple(LotusRecordMethod(s) for s in stages)
        self.stages = tuple(m.stage for m in self.methods)

    def describe(self):
        return dict(stages=[s.describe() for s in self.stages], query_id=self.query_id)

    @classmethod
    def from_dict(cls, value):
        return cls(tuple(StageSpec.from_dict(s) for s in value["stages"]), query_id=value["query_id"])

    def run_batch(self, records, infer):
        if type(records) is not list or any(type(r) is not RecordOccurrence for r in records):
            raise TypeError("chain input must be a finite list of RecordOccurrence")
        keys = [(r.row_id, r.source_position) for r in records]
        if len(set(keys)) != len(keys):
            raise ValueError("chain input contains duplicate relation occurrences")
        if any(r.index_updates for r in records):
            raise ValueError("assemble final index updates before reusing a relation")
        current = [RecordOccurrence.from_dict(r.describe()) for r in records]
        columns = list(dict.fromkeys(k for record in current for k in record.values))
        for stage in self.stages:
            if stage.kind == "map" and stage.method.output_column not in columns:
                columns.append(stage.method.output_column)
        calls, stats, parents, raw_results, prepared = [], {}, {}, [], []
        previous_parents = {}
        try:
            for method in self.methods:
                stage = method.stage
                prepared, raw_results = [], []
                previous_parents = dict(parents)
                prepared = method.prepare_rows(current, query_id=self.query_id)
                if not prepared:
                    continue
                results = infer(prepared, generation=stage.generation)
                if type(results) is not list:
                    raise TypeError("model results must be a finite list")
                raw_results = deepcopy(results)
                expected = {r["call_id"]: r for r in prepared}
                received = {}
                for result in results:
                    if type(result) is not dict:
                        raise TypeError("each model result must be an object")
                    call_id = result.get("call_id")
                    item = expected.get(call_id)
                    if item is None or call_id in received or any(
                        result.get(k) != item[k] for k in IDENTITY_FIELDS
                    ):
                        raise ValueError("model changed or duplicated a stage evaluation identity")
                    received[call_id] = result
                if set(received) != set(expected):
                    raise ValueError("model omitted a stage evaluation result")
                ordered = [deepcopy(received[p["call_id"]]) for p in prepared]
                for record, result in zip(current, ordered, strict=True):
                    key = (record.row_id, record.source_position)
                    result["parent_call_ids"] = list(parents.get(key, ()))
                    parents[key] = (result["call_id"],)
                    calls.append(result)
                parsed = method.interpret_results(ordered)
                stats[stage.stage_id] = dict(lm_report(method.lm), scope="worker_method_lifetime")
                if stage.kind == "filter":
                    for call, decision in zip(calls[-len(ordered):], parsed.outputs, strict=True):
                        call["decision"] = decision
                    current = [record for record, keep in zip(current, parsed.outputs, strict=True) if keep]
                else:
                    for record, output in zip(current, parsed.outputs, strict=True):
                        if type(stage.method) is MapMethod:
                            record.values[stage.method.output_column] = output
                        else:
                            # Data keys never overwrite record or call identities.
                            record.values.update(output)
                            record.index_updates.update(output)
                            for name in output:
                                if name not in columns:
                                    columns.append(name)
            return dict(records=current, calls=calls, stage_stats=stats, columns=columns)
        except BaseException as error:
            # A broken result association or transport must not erase calls
            # already declared to that transport. Keep uniquely associated
            # returned usage; the remaining evaluations are unconfirmed.
            retained = {c["call_id"] for c in calls}
            for item in prepared:
                if item["call_id"] in retained:
                    continue
                matches = [r for r in raw_results if type(r) is dict and all(
                    r.get(k) == item[k] for k in IDENTITY_FIELDS)]
                call = deepcopy(matches[0]) if len(matches) == 1 else dict(
                    **{k: item[k] for k in IDENTITY_FIELDS}, status="unknown",
                    submission_confirmed=False, prompt_tokens=None, completion_tokens=None,
                    total_tokens=None, error_type=type(error).__name__,
                )
                call["parent_call_ids"] = list(previous_parents.get(
                    (item["row_id"], item["source_position"]), ()))
                calls.append(call)
            error.chain_calls = deepcopy(calls)
            error.chain_stage_stats = {
                m.stage.stage_id: dict(lm_report(m.lm), scope="worker_method_lifetime") for m in self.methods
            }
            error.chain_input_ids = keys
            error.chain_raw_results = raw_results
            raise


class LotusFilterMapExtract(LotusRecordChain):
    """Compatibility constructor for the first three-method acceptance example."""

    def __init__(self, stages, *, query_id):
        if (type(stages) is not tuple or len(stages) != 3
                or any(type(s) is not StageSpec for s in stages)
                or [s.kind for s in stages] != ["filter", "map", "extract"]):
            raise ValueError("this example requires Filter -> Map -> Extract")
        if stages[1].method.output_column not in stages[2].method.input_columns:
            raise ValueError("Extract must consume the new Map output column")
        super().__init__(stages, query_id=query_id)


def records_to_frame(records, *, columns=None):
    """One final relation assembly, with original Extract label assignment.

The union of Extract's actual keys defines added columns, as in its accessor;
no declared-schema filtering or missing-key completion is applied to parsing.
"""
    import pandas as pd

    records = sorted(records, key=lambda record: record.source_position)
    columns = (list(dict.fromkeys(k for record in records for k in record.values))
               if columns is None else list(columns))
    frame = pd.DataFrame([{k: r.values.get(k) for k in columns} for r in records],
                         index=[r.source_index for r in records], columns=columns)
    for record in records:
        for key, value in record.index_updates.items():
            if key not in frame.columns:
                frame[key] = None
            frame.loc[record.source_index, key] = value
    return frame
