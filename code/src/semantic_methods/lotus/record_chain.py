"""Ordinary LOTUS record methods composed into finite worker-local sequences.

Record membership, data columns and model evaluations have separate identities.
This module prepares and interprets finite worker-local batches; the backend
owns their physical batching, model instance and lifetime.
"""

from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
import hashlib
from importlib.metadata import distribution
import json
from numbers import Integral
import os
import time

from .engine import (
    _FilterFormatter, _input_instruction, bind_filter_method, engine_response, lm_report,
    validate_engine_protocol, validate_filter_source, validate_generation,
)
from .records import FilterMethod, _resolved_columns
from .sdk import SOURCE_HASHES
from src.serving.backends.vllm_engine import GenerationSpec, IDENTITY_FIELDS

# The old import and serialized tag remain readable; Filter has one value type.
TextFilterMethod = FilterMethod


CHAIN_SOURCE_HASHES = {
    "sem_ops/sem_map.py": SOURCE_HASHES["sem_ops/sem_map.py"],
    "sem_ops/sem_extract.py": "0f58eda86d645b85b8659591d61366a86809a4d189dbec6c491fe5b4628c163a",
    "types.py": "73b0ebb4a8a9606cdec6df27353d57fb28ce6e2f5800c539caee892eb4f2cc6b",
}


def _name(value, label):
    if type(value) is not str or not value.strip():
        raise ValueError(label + " must be a nonempty string")


def _unconfirmed_call(item, error_type):
    return dict(**{k: item[k] for k in IDENTITY_FIELDS}, status="unknown",
                submission_confirmed=False, prompt_tokens=None, completion_tokens=None,
                total_tokens=None, error_type=error_type)


def _parent_seeds(keys, initial_parents):
    """Validate a finite occurrence-to-parent value contract shared by workers and collectors."""
    keys = set(keys)
    if initial_parents is None:
        return {key: () for key in keys}
    if type(initial_parents) is not dict or set(initial_parents) != keys:
        raise ValueError("initial_parents must exactly cover input occurrence keys")
    for key, parents in initial_parents.items():
        if (type(key) is not tuple or len(key) != 2 or type(key[0]) is not str
                or type(key[1]) is not int or type(parents) is not tuple):
            raise ValueError("initial_parents requires occurrence tuples and unique parent tuples")
        for call_id in parents:
            _name(call_id, "parent call_id")
        if len(set(parents)) != len(parents):
            raise ValueError("parent call_ids must be unique")
    return deepcopy(initial_parents)


def _ordered_columns(records, columns):
    if columns is not None and (type(columns) not in (list, tuple)
            or any(type(name) is not str for name in columns)
            or len(set(columns)) != len(columns)):
        raise ValueError("columns must be a finite ordered sequence of unique names")
    return list(dict.fromkeys(list(columns or ()) + [key for record in records for key in record.values]))


def validate_chain_source():
    evidence = validate_filter_source()
    root = distribution("lotus-ai").locate_file("lotus")
    for relative, digest in CHAIN_SOURCE_HASHES.items():
        if hashlib.sha256((root / relative).read_bytes()).hexdigest() != digest:
            raise ValueError("records chain source differs at lotus/" + relative)
    return dict(evidence, profile="lotus-ordinary-record-methods-v1",
                source_hashes={**evidence["source_hashes"], **CHAIN_SOURCE_HASHES})


@dataclass(frozen=True)
class MapMethod:
    model_name: str
    instruction: str
    output_column: str = "summary"
    system_prompt: str | None = None
    resolved_input_columns: tuple[str, ...] | None = None

    def __post_init__(self):
        for name in ("model_name", "instruction", "output_column"):
            _name(getattr(self, name), "Map " + name)
        if self.system_prompt is not None and type(self.system_prompt) is not str:
            raise ValueError("Map system_prompt must be a string or None")
        object.__setattr__(self, "resolved_input_columns", _resolved_columns(self.resolved_input_columns))


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
    method: FilterMethod | MapMethod | ExtractMethod
    generation: GenerationSpec
    model_role: str = "main"

    def __post_init__(self):
        _name(self.stage_id, "stage_id")
        _name(self.model_role, "model_role")
        if type(self.method) not in (FilterMethod, MapMethod, ExtractMethod):
            raise TypeError("an explicit ordinary record method is required")
        self.method.__post_init__()
        if type(self.generation) is not GenerationSpec:
            raise TypeError("generation must use GenerationSpec")
        self.generation.__post_init__()
        if type(self.method) is FilterMethod:
            validate_generation(self.method, self.generation)
        response_format = getattr(self.generation, "response_format", None)
        expected = {"type": "json_object"} if type(self.method) is ExtractMethod else None
        if response_format != expected:
            raise ValueError("ordinary Extract requires json_object; Filter/Map require no response format")
        if self.generation.repetition_penalty != 1:
            raise ValueError("this ordinary LOTUS profile does not support repetition overrides")

    @property
    def kind(self):
        if type(self.method) is FilterMethod:
            return "filter"
        return "map" if type(self.method) is MapMethod else "extract"

    def describe(self):
        return dict(stage_id=self.stage_id, method_type=type(self.method).__name__,
                    method=asdict(self.method), generation=asdict(self.generation),
                    model_role=self.model_role)

    def bind(self):
        """Resolve caller-owned Filter/Map order; Extract already declares order."""
        self.__post_init__()
        validate_chain_source()
        if type(self.method) is FilterMethod:
            method = bind_filter_method(self.method)
        elif type(self.method) is MapMethod:
            columns, _ = _input_instruction(self.method.instruction,
                                            self.method.resolved_input_columns, require_bound=False)
            method = replace(self.method, resolved_input_columns=tuple(columns))
        else:
            method = self.method
        return replace(self, method=method)

    @classmethod
    def from_dict(cls, value):
        kinds = {kind.__name__: kind for kind in (FilterMethod, MapMethod, ExtractMethod)}
        kinds["TextFilterMethod"] = FilterMethod
        options = deepcopy(value)
        kind = kinds[options.pop("method_type")]
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
        if type(self.method) in (FilterMethod, MapMethod):
            self.columns, self.instruction = _input_instruction(
                self.method.instruction, self.method.resolved_input_columns)
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
        if type(self.method) is FilterMethod:
            self._filter_formatter = _FilterFormatter(self.method, self.lm)
            self.columns = self._filter_formatter.columns
            self.instruction = self._filter_formatter.instruction
        elif type(self.method) is ExtractMethod:
            self.columns = list(self.method.input_columns)
            self.instruction = None
        if not self.columns:
            raise ValueError("a record method must reference an input column")

    def prepare_rows(self, records, *, query_id, allow_nulls=False):
        import lotus
        import pandas as pd
        from lotus.types import SerializationFormat

        if lotus.settings.enable_cache or lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("record methods require caching disabled and DEFAULT serialization")
        if type(allow_nulls) is not bool:
            raise TypeError("allow_nulls must be a boolean")
        for record in records:
            if any(name not in record.values or not (type(record.values[name]) is str
                   or allow_nulls and record.values[name] is None) for name in self.columns):
                raise ValueError("ordinary record methods require present text input columns")
        if not records:
            return []
        started = time.monotonic_ns()
        if self.stage.kind == "filter":
            messages = self._filter_formatter.messages(
                [record.values for record in records], index=[record.source_index for record in records],
                allow_nulls=allow_nulls)
        else:
            frame = pd.DataFrame([record.values for record in records],
                                 index=[record.source_index for record in records])
            docs = self.instructions.df2multimodal_info(frame, self.columns)
            if type(self.method) is MapMethod:
                messages = [self.instructions.map_formatter(
                    self.lm, doc, self.instruction, system_prompt=self.method.system_prompt) for doc in docs]
            else:
                messages = [self.instructions.extract_formatter(
                    self.lm, doc, self.method.output_columns, self.method.extract_quotes) for doc in docs]
        result = []
        for record, message in zip(records, messages, strict=True):
            result.append(dict(
                stage_identity(query_id, record, self.stage.stage_id, model_role=self.stage.model_role),
                messages_json=json.dumps(message, ensure_ascii=False, allow_nan=False),
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
                validate_engine_protocol(result)
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

    Executable segments end at Extract. A relation-barrier plan must be split,
    and its owner must complete index-label assignment before the next segment.
    """

    def __init__(self, stages, *, query_id, relation_barriers=False, _require_bound=False):
        _name(query_id, "query_id")
        if type(stages) is not tuple or not stages or any(type(s) is not StageSpec for s in stages):
            raise ValueError("a finite nonempty tuple of StageSpec is required")
        if len({s.stage_id for s in stages}) != len(stages):
            raise ValueError("each operator occurrence needs a distinct stage_id")
        if type(relation_barriers) is not bool:
            raise TypeError("relation_barriers must be a boolean")
        if not relation_barriers and any(s.kind == "extract" for s in stages[:-1]):
            raise ValueError("Extract must be terminal before final index-label assignment")
        if type(_require_bound) is not bool:
            raise TypeError("_require_bound must be a boolean")
        if not _require_bound:
            stages = tuple(stage.bind() for stage in stages)
        self.query_id = query_id
        self.relation_barriers = relation_barriers
        self.methods = tuple(LotusRecordMethod(s) for s in stages)
        self.stages = tuple(m.stage for m in self.methods)

    def describe(self):
        return dict(stages=[s.describe() for s in self.stages], query_id=self.query_id,
                    **({"relation_barriers": True} if self.relation_barriers else {}))

    @classmethod
    def from_dict(cls, value):
        """Receive caller-bound wire values; never resolve order in a worker."""
        return cls(tuple(StageSpec.from_dict(s) for s in value["stages"]),
                   query_id=value["query_id"], _require_bound=True,
                   **({"relation_barriers": value["relation_barriers"]} if "relation_barriers" in value else {}))

    def relation_segments(self):
        """Return batch-local segments; the owner alone completes relations between them."""
        if not self.relation_barriers:
            return (self,)
        segments, start = [], 0
        for position, stage in enumerate(self.stages):
            if stage.kind == "extract" or position == len(self.stages) - 1:
                segments.append(LotusRecordChain(self.stages[start:position + 1], query_id=self.query_id,
                                                  _require_bound=True))
                start = position + 1
        return tuple(segments)

    def start_batch(self, records, *, initial_parents=None, columns=None, allow_nulls=False):
        """Create paused method state; this does not submit model work."""
        return ChainRun(self, records, initial_parents=initial_parents,
                        columns=columns, allow_nulls=allow_nulls)

    def start_relation(self, records, *, initial_parents=None, columns=None, allow_nulls=False):
        """Pause the same finite method state after each Extract.

        The caller owns the complete source-ordered relation and must verify successful
        source EOF/coverage before complete_relation. No model owner or route is
        created here, and physical execution remains the caller's responsibility.
        """
        if not self.relation_barriers:
            raise ValueError("relation execution requires explicit relation_barriers=True")
        return ChainRun(self, records, initial_parents=initial_parents, columns=columns,
                        allow_nulls=allow_nulls, _relation_execution=True)

    def run_batch(self, records, infer, *, initial_parents=None, columns=None, allow_nulls=False):
        run = self.start_batch(records, initial_parents=initial_parents,
                               columns=columns, allow_nulls=allow_nulls)
        try:
            while run.prepare():
                run.complete(infer(run.prepared, generation=run.method.stage.generation))
            return run.result()
        except BaseException as error:
            run.failure(error)
            raise


class RecordLocalSection:
    """One delegated F/M interval; the original result accountant owns its receipts.

    Methods are shared with the query, never reconstructed. The caller accepts
    actual MethodRun finals; this object neither infers nor interprets responses.
    """

    def __init__(self, chain, records, parents, columns, allow_nulls, start, stop):
        from .record_results import ChainBatchResults

        self.chain, self._start, self._stop = chain, start, stop
        self.records = tuple(RecordOccurrence.from_dict(r.describe()) for r in records)
        keys = [(r.row_id, r.source_position) for r in self.records]
        self.initial_parents = _parent_seeds(keys, {key: parents[key] for key in keys})
        self.columns, self.allow_nulls = tuple(columns), allow_nulls
        self._results = ChainBatchResults(self.records, chain,
            initial_parents=self.initial_parents, columns=self.columns)
        self._closed = False

    def accept(self, envelope):
        if self._closed:
            raise ValueError("record-local section is already closed")
        self._results.accept(envelope)

    def _adoption(self, run, source_exhausted):
        from .record_results import _source_binding

        self._results.verify_success(source_exhausted=source_exhausted)
        if (run.position != self._start or self.chain.stages != run.chain.stages[self._start:self._stop]
                or self.chain.methods != run.chain.methods[self._start:self._stop]):
            raise ValueError("record-local section changed its method interval")
        if (self._results.bindings != {r.row_id: _source_binding(r) for r in run.current}
                or self.initial_parents != {key: run.parents[key] for key in self.initial_parents}
                or self.columns != tuple(run.columns) or self.allow_nulls != run.allow_nulls):
            raise ValueError("record-local section changed its input or parent binding")
        records = sorted([RecordOccurrence.from_dict(r.describe()) for r in self._results.pending],
                         key=lambda r: r.source_position)
        originals = {r.row_id: r for r in self.records}
        written = {stage.method.output_column for stage in self.chain.stages if stage.kind == "map"}
        for record in records:
            source = originals[record.row_id]
            if (record.index_updates or set(record.values) != set(source.values) | written
                    or any(record.values[k] != v for k, v in source.values.items() if k not in written)):
                raise ValueError("record-local result changed an undeclared field")
        order = {stage.stage_id: i for i, stage in enumerate(self.chain.stages)}
        calls = sorted(deepcopy(self._results.calls),
            key=lambda call: (order[call["algorithm_stage"]], call["source_position"], call["call_index"]))
        parents = deepcopy(run.parents)
        for call in calls:
            parents[(call["row_id"], call["source_position"])] = (call["call_id"],)
        previous = deepcopy(parents)
        for call in calls:
            if call["algorithm_stage"] == self.chain.stages[-1].stage_id:
                previous[(call["row_id"], call["source_position"])] = tuple(call["parent_call_ids"])
        executed = {call["algorithm_stage"] for call in calls}
        stats = {method.stage.stage_id: dict(lm_report(method.lm), scope="worker_method_lifetime")
                 for method in self.chain.methods if method.stage.stage_id in executed}
        self._results.finalize_columns()
        return records, parents, previous, list(self._results.columns), run.calls + calls, {**run.stats, **stats}


class ChainRun:
    """Paused finite batch, shared by fused execution and row continuations.

    prepare leaves the stage unchanged. Only a complete associated result set is
    interpreted and advances it. The execution owner binds acceptance/TaskKey.
    """

    def __init__(self, chain, records, *, initial_parents=None, columns=None, allow_nulls=False,
                 _relation_execution=False):
        if not _relation_execution and any(stage.kind == "extract" for stage in chain.stages[:-1]):
            raise ValueError("complete Extract's relation barrier before running the next segment")
        if type(allow_nulls) is not bool:
            raise TypeError("allow_nulls must be a boolean")
        if type(records) is not list or any(type(r) is not RecordOccurrence for r in records):
            raise TypeError("chain input must be a finite list of RecordOccurrence")
        if _relation_execution:
            if any(a.source_position >= b.source_position for a, b in zip(records, records[1:])):
                raise ValueError("relation execution requires increasing source_position")
            if len({r.row_id for r in records}) != len(records):
                raise ValueError("relation execution requires globally unique row_id")
        self.keys = [(r.row_id, r.source_position) for r in records]
        if len(set(self.keys)) != len(self.keys):
            raise ValueError("chain input contains duplicate relation occurrences")
        if any(r.index_updates for r in records):
            raise ValueError("assemble final index updates before reusing a relation")
        self.chain, self.allow_nulls = chain, allow_nulls
        self._relation_execution = _relation_execution
        self._relation_stage = None
        self._relation_failed = False
        self._local_section = None
        self.current = [RecordOccurrence.from_dict(r.describe()) for r in records]
        self.parents = _parent_seeds(self.keys, initial_parents)
        self.columns = _ordered_columns(self.current, columns)
        if not _relation_execution:
            for stage in chain.stages:
                if stage.kind == "map" and stage.method.output_column not in self.columns:
                    self.columns.append(stage.method.output_column)
        self.position = 0
        self.calls, self.stats, self.raw_results, self.prepared = [], {}, [], []
        self.previous_parents = dict(self.parents)

    @property
    def method(self):
        return self.chain.methods[self.position]

    def prepare(self):
        if self._relation_failed:
            raise ValueError("relation method has failed")
        if self._local_section is not None:
            raise ValueError("a record-local section must be adopted before preparing")
        if self.relation_pending:
            return []
        if self.prepared:
            return self.prepared
        while self.position < len(self.chain.methods):
            self.raw_results = []
            self.previous_parents = dict(self.parents)
            stage = self.method.stage
            if self._relation_execution and stage.kind == "map" and stage.method.output_column not in self.columns:
                self.columns.append(stage.method.output_column)
            self.prepared = self.method.prepare_rows(
                self.current, query_id=self.chain.query_id, allow_nulls=self.allow_nulls)
            if self.prepared:
                return self.prepared
            self.position += 1
            if self._relation_execution and stage.kind == "extract":
                self._relation_stage = self.position - 1
                return []
        return []

    def complete(self, results):
        if self._relation_failed:
            raise ValueError("relation method has failed")
        if self._local_section is not None:
            raise ValueError("a record-local section must be adopted before completing")
        if not self.prepared or self.position >= len(self.chain.methods):
            raise ValueError("no prepared stage is awaiting results")
        if type(results) is not list:
            raise TypeError("model results must be a finite list")
        self.raw_results = deepcopy(results)
        expected = {r["call_id"]: r for r in self.prepared}
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
        ordered = [deepcopy(received[p["call_id"]]) for p in self.prepared]
        for record, result in zip(self.current, ordered, strict=True):
            key = (record.row_id, record.source_position)
            result["parent_call_ids"] = list(self.parents.get(key, ()))
            self.parents[key] = (result["call_id"],)
            self.calls.append(result)
        method = self.method
        stage = method.stage
        parsed = method.interpret_results(ordered)
        self.stats[stage.stage_id] = dict(lm_report(method.lm), scope="worker_method_lifetime")
        if stage.kind == "filter":
            for call, decision in zip(self.calls[-len(ordered):], parsed.outputs, strict=True):
                call["decision"] = decision
            self.current = [record for record, keep in zip(self.current, parsed.outputs, strict=True) if keep]
        else:
            for record, output in zip(self.current, parsed.outputs, strict=True):
                if type(stage.method) is MapMethod:
                    record.values[stage.method.output_column] = output
                else:
                    record.values.update(output)
                    record.index_updates.update(output)
                    for name in output:
                        if name not in self.columns:
                            self.columns.append(name)
        self.position += 1
        self.prepared, self.raw_results = [], []
        if self._relation_execution and stage.kind == "extract":
            self._relation_stage = self.position - 1

    @property
    def relation_pending(self):
        return self._relation_stage is not None

    def delegate_record_local(self):
        """Delegate a contiguous F/M interval to existing row continuations.

        Owner validation of actual source EOF/coverage is still required at
        adoption. This is a local method view, not a new model owner or route.
        """
        if not self._relation_execution or self._relation_failed:
            raise ValueError("record-local delegation requires a live relation state")
        if self._local_section is not None or self.prepared or self.relation_pending:
            raise ValueError("the method already owns prepared or delegated work")
        if self.position == len(self.chain.stages) or self.method.stage.kind not in ("filter", "map"):
            raise ValueError("the next method is not a record-local Filter/Map")
        if len({stage.method.model_name for stage in self.chain.stages}) != 1:
            raise ValueError("record-local sections require one model deployment")
        stop, role = self.position, self.method.stage.model_role
        while (stop < len(self.chain.stages) and self.chain.stages[stop].kind in ("filter", "map")
               and self.chain.stages[stop].model_role == role):
            stop += 1
        view = LotusRecordChain.__new__(LotusRecordChain)
        view.query_id, view.relation_barriers = self.chain.query_id, False
        view.methods = self.chain.methods[self.position:stop]
        view.stages = tuple(method.stage for method in view.methods)
        section = RecordLocalSection(view, self.current, self.parents, self.columns,
                                     self.allow_nulls, self.position, stop)
        self._local_section = section
        return section

    def adopt_record_local(self, section, *, source_exhausted):
        """Adopt complete parsed results once; never replay formatter or parser."""
        if self._relation_failed:
            raise ValueError("relation method has failed")
        if section is not self._local_section or section is None:
            raise ValueError("record-local section is foreign or already adopted")
        values = section._adoption(self, source_exhausted)
        self.current, self.parents, self.previous_parents, self.columns, self.calls, self.stats = values
        self.position = section._stop
        self.prepared, self.raw_results = [], []
        self._local_section, section._closed = None, True

    def complete_relation(self, *, source_exhausted):
        """Apply the existing whole-relation assembler exactly once at Extract.

        source_exhausted is the owner's verified successful EOF, not evidence
        inferred from an empty queue or the finite batch given to this object.
        """
        from .record_relation import complete_extract_relation

        if self._relation_failed:
            raise ValueError("relation method has failed")
        if not self.relation_pending:
            raise ValueError("no Extract relation is awaiting completion")
        completed = complete_extract_relation(self.current, columns=self.columns,
            extract_stage=self.chain.stages[self._relation_stage], query_id=self.chain.query_id,
            calls=self.calls, source_exhausted=source_exhausted)
        self.current, self.columns = list(completed.records), list(completed.columns)
        self.parents, self.allow_nulls = completed.parent_call_ids, True
        self._relation_stage = None
        return completed

    def result(self):
        if self._relation_failed:
            raise ValueError("relation method has failed")
        if self._local_section is not None:
            raise ValueError("chain has an unfinished record-local section")
        if self.relation_pending:
            raise ValueError("chain has an unfinished Extract relation")
        if self.position != len(self.chain.methods):
            raise ValueError("chain has unfinished stages")
        return dict(records=self.current, calls=self.calls, stage_stats=self.stats, columns=self.columns)

    def failure(self, error, *, submitted_call_ids=None):
        if submitted_call_ids is not None and (type(submitted_call_ids) not in (set, frozenset)
                or not submitted_call_ids.issubset({item["call_id"] for item in self.prepared})):
            raise ValueError("submitted calls must belong to this prepared stage")
        if self._relation_execution:
            self._relation_failed = True
        # Retain associated usage even when the full stage cannot be interpreted.
        retained = {c["call_id"] for c in self.calls}
        if self._local_section is not None:
            section = self._local_section
            for call in section._results.calls:
                if call["call_id"] not in retained:
                    self.calls.append(deepcopy(call))
                    retained.add(call["call_id"])
            section._closed, self._local_section = True, None
        for item in self.prepared:
            if (item["call_id"] in retained or submitted_call_ids is not None
                    and item["call_id"] not in submitted_call_ids):
                continue
            matches = [r for r in self.raw_results if type(r) is dict and all(
                r.get(k) == item[k] for k in IDENTITY_FIELDS)]
            call = (deepcopy(matches[0]) if len(matches) == 1
                    else _unconfirmed_call(item, "NativeTaskUnconfirmed" if submitted_call_ids is not None
                                           else type(error).__name__))
            call["parent_call_ids"] = list(self.previous_parents.get(
                (item["row_id"], item["source_position"]), ()))
            self.calls.append(call)
        error.chain_calls = deepcopy(self.calls)
        error.chain_stage_stats = {
            m.stage.stage_id: dict(lm_report(m.lm), scope="worker_method_lifetime") for m in self.chain.methods
        }
        error.chain_input_ids = self.keys
        error.chain_raw_results = self.raw_results
        error.chain_columns = list(self.columns)

    def snapshot(self):
        """Method-owned JSON value; no engine, Future, parser or callback is serialized."""
        if self._local_section is not None:
            raise ValueError("cannot snapshot a delegated record-local section")
        state = dict(position=self.position, records=[r.describe() for r in self.current],
            keys=self.keys, parents=[[list(k), list(v)] for k, v in self.parents.items()],
            previous_parents=[[list(k), list(v)] for k, v in self.previous_parents.items()],
            columns=self.columns, allow_nulls=self.allow_nulls, calls=self.calls,
            stats=self.stats, prepared=self.prepared)
        if self._relation_execution:
            state["relation_execution"] = dict(pending_stage=self._relation_stage, failed=self._relation_failed)
        return state

    @classmethod
    def restore(cls, chain, state):
        # Only the local MethodRun's bounded immutable state reaches this method.
        run = cls.__new__(cls)
        run.chain, run.position, run.allow_nulls = chain, state["position"], state["allow_nulls"]
        run.current = [RecordOccurrence.from_dict(r) for r in state["records"]]
        run.keys = [tuple(k) for k in state["keys"]]
        run.parents = {tuple(k): tuple(v) for k, v in state["parents"]}
        run.previous_parents = {tuple(k): tuple(v) for k, v in state["previous_parents"]}
        run.columns, run.calls, run.stats = state["columns"], state["calls"], state["stats"]
        run.prepared, run.raw_results = state["prepared"], []
        run._relation_execution = "relation_execution" in state
        run._relation_stage = state["relation_execution"]["pending_stage"] if run._relation_execution else None
        run._relation_failed = state["relation_execution"]["failed"] if run._relation_execution else False
        run._local_section = None
        return run


class LotusFilterMapExtract(LotusRecordChain):
    """Compatibility constructor for the first three-method acceptance example."""

    def __init__(self, stages, *, query_id, _require_bound=False):
        if (type(stages) is not tuple or len(stages) != 3
                or any(type(s) is not StageSpec for s in stages)
                or [s.kind for s in stages] != ["filter", "map", "extract"]):
            raise ValueError("this example requires Filter -> Map -> Extract")
        if stages[1].method.output_column not in stages[2].method.input_columns:
            raise ValueError("Extract must consume the new Map output column")
        super().__init__(stages, query_id=query_id, _require_bound=_require_bound)


def records_to_frame(records, *, columns=None, source_index=None, empty_prototype=None):
    """One final relation assembly, with original Extract label assignment.

The union of Extract's actual keys defines added columns, as in its accessor;
no declared-schema filtering or missing-key completion is applied to parsing.
Pass the original pandas Index to retain its dtype/name when filtering removes
all occurrences. Surviving records cannot recover that metadata by themselves.
An optional zero-row DataFrame supplies the final empty relation's column dtypes
and metadata. Its columns must exactly match explicit columns; source_index, if
given, still owns the index. It never casts or filters nonempty record values.
"""
    import pandas as pd

    records = sorted(records, key=lambda record: record.source_position)
    if empty_prototype is not None:
        if not isinstance(empty_prototype, pd.DataFrame):
            raise TypeError("empty_prototype must be a pandas DataFrame")
        if len(empty_prototype):
            raise ValueError("empty_prototype must have zero rows")
        if isinstance(empty_prototype.index, pd.MultiIndex):
            raise TypeError("empty_prototype must have a single pandas Index")
        _ordered_columns([], list(empty_prototype.columns))
    if columns is None:
        columns = (list(empty_prototype.columns) if not records and empty_prototype is not None
                   else list(dict.fromkeys(k for record in records for k in record.values)))
    else:
        columns = list(columns)
    if not records and empty_prototype is not None and list(empty_prototype.columns) != columns:
        raise ValueError("columns must match empty_prototype columns in order")
    labels = [record.source_index for record in records]
    if source_index is None:
        index = (empty_prototype.index.copy(deep=True)
                 if not records and empty_prototype is not None else labels)
    else:
        if not isinstance(source_index, pd.Index) or isinstance(source_index, pd.MultiIndex):
            raise TypeError("source_index must be a single pandas Index")
        index = (pd.Index(labels, dtype=source_index.dtype, name=source_index.name)
                 if records else source_index[:0])
        for label, value in zip(labels, index.tolist(), strict=True):
            if ((type(label) is int and (isinstance(value, bool) or not isinstance(value, Integral)))
                    or (type(label) is str and type(value) is not str) or value != label):
                raise ValueError("source_index dtype changed occurrence labels")
    if not records and empty_prototype is not None:
        frame = empty_prototype.copy(deep=True)
        frame.index = index
    else:
        frame = pd.DataFrame([{k: r.values.get(k) for k in columns} for r in records],
                             index=index, columns=columns)
    for record in records:
        for key, value in record.index_updates.items():
            if key not in frame.columns:
                frame[key] = None
            frame.loc[record.source_index, key] = value
    return frame
