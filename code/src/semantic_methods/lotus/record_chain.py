"""Finite LOTUS chain state, stage progression and record-local handoff.

Native formatter/parser details live in record_method; serializable contracts
live in record_types. The execution backend owns batching and model lifetime.
Existing record_chain imports resolve to the same types and functions.
"""

from copy import deepcopy
from dataclasses import replace

from .engine import lm_report
from .records import FilterMethod as FilterMethod
from .record_method import LotusRecordMethod as LotusRecordMethod
from .record_relation import records_to_frame as records_to_frame
from .record_types import (
    ExtractMethod as ExtractMethod, MapMethod as MapMethod, RecordOccurrence as RecordOccurrence,
    StageSpec as StageSpec, TextFilterMethod as TextFilterMethod,
    _name as _name, _ordered_columns as _ordered_columns, _parent_seeds as _parent_seeds,
    stage_identity as stage_identity,
)
from .source import CHAIN_FILES, source_report
from src.serving.backends.vllm_protocol import GenerationSpec as GenerationSpec, IDENTITY_FIELDS, _prepared


def _unconfirmed_call(item, error_type):
    return dict(**{k: item[k] for k in IDENTITY_FIELDS}, status="unknown",
                submission_confirmed=False, prompt_tokens=None, completion_tokens=None,
                total_tokens=None, error_type=error_type)


def validate_chain_source():
    """Compatibility entry for observed sources, independent of method execution."""
    return source_report("lotus-ordinary-record-methods-v1", CHAIN_FILES)


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

    def run_batch(self, records, infer, *, initial_parents=None, columns=None, allow_nulls=False,
                  prepared_rows=None):
        """Use the same method state, optionally reusing its single F/M preparation.

        The execution owner binds prepared_rows to the method and generation;
        this method checks ordinary inputs, message shape and ordered identities.
        """
        run = self.start_batch(records, initial_parents=initial_parents,
                               columns=columns, allow_nulls=allow_nulls)
        try:
            if prepared_rows is not None:
                # The producer uses this same method's formatter. Reuse only a
                # complete single F/M stage; successors still prepare normally.
                if len(self.stages) != 1 or self.stages[0].kind not in ('filter', 'map'):
                    raise ValueError('prepared rows require a single Filter or Map')
                run.method.validate_rows(run.current, allow_nulls=allow_nulls)
                _prepared(prepared_rows)
                stage = run.method.stage
                if len(prepared_rows) != len(run.current) or any(
                    any(row[key] != identity[key] for key in IDENTITY_FIELDS)
                    for row, identity in zip(prepared_rows, (
                        stage_identity(self.query_id, record, stage.stage_id, model_role=stage.model_role)
                        for record in run.current))
                ):
                    raise ValueError('prepared rows changed stage identity or member order')
                run.prepared = deepcopy(prepared_rows)
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
        self.records = tuple(replace(r) for r in records)
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
        records = sorted([replace(r) for r in self._results.pending],
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
        self.current = [replace(r) for r in records]
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
