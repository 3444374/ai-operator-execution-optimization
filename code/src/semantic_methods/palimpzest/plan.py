"""Select once and execute the same original PZ PhysicalPlan without reoptimizing."""

from dataclasses import dataclass, field
import dis
from enum import Enum
import hashlib
import json
import math
from types import CodeType, FunctionType

from .source import PALIMPZEST_COMMIT, validate_source


def _immutable(value):
    if type(value) is tuple:
        return [_immutable(item) for item in value]
    if type(value) not in (str, bool, int, float, type(None)):
        raise TypeError("PZ fixed UDF closure/default/global must contain immutable scalar values")
    return _value(value)


def _function(function):
    if type(function) is not FunctionType:
        raise TypeError("PZ fixed-plan slice requires an ordinary Python UDF")

    def code_identity(code):
        return dict(bytecode=code.co_code.hex(), constants=[
            code_identity(item) if type(item) is CodeType else _immutable(item)
            for item in code.co_consts], names=code.co_names, variables=code.co_varnames,
            freevars=code.co_freevars, cellvars=code.co_cellvars,
            argcount=code.co_argcount, posonlyargcount=code.co_posonlyargcount,
            kwonlyargcount=code.co_kwonlyargcount, flags=code.co_flags,
            exceptiontable=code.co_exceptiontable.hex())

    globals_used = {instruction.argval for instruction in dis.get_instructions(function)
                    if instruction.opname == "LOAD_GLOBAL"}
    # Builtin/function dependencies need a separately versioned UDF adapter.
    if any(name not in function.__globals__ for name in globals_used):
        raise TypeError("PZ fixed UDF builtin dependency needs a separately versioned adapter")
    globals_values = {name: _immutable(function.__globals__[name]) for name in globals_used}
    return dict(code=code_identity(function.__code__), defaults=_immutable(function.__defaults__),
                kwdefaults={key: _immutable(item) for key, item in
                            (function.__kwdefaults__ or {}).items()},
                closure=[_immutable(cell.cell_contents) for cell in function.__closure__ or ()],
                globals=globals_values)


def _value(value):
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, Enum):
        return {"enum": type(value).__qualname__, "value": _value(value.value)}
    if type(value) in (list, tuple):
        return [_value(item) for item in value]
    if type(value) is dict and all(type(key) is str for key in value):
        return {key: _value(item) for key, item in value.items()}
    if isinstance(value, type) and hasattr(value, "model_json_schema"):
        return {"schema": value.model_json_schema()}
    if type(value).__module__ == "palimpzest.core.elements.filters":
        return {"filter_condition": value.filter_condition,
                "filter_fn": None if value.filter_fn is None else _function(value.filter_fn)}
    raise TypeError("PZ fixed-plan slice cannot freeze parameter type " + type(value).__qualname__)


def _operator(operator):
    params = dict(operator.get_op_params())
    source = params.pop("datasource", None)
    # Runtime verbosity cannot change the model request or relational result.
    params.pop("verbose", None)
    descriptor = {
        "class": type(operator).__module__ + "." + type(operator).__qualname__,
        "id_params": _value(operator.get_id_params()),
        "params": _value(params),
    }
    if source is not None:
        descriptor["source"] = {"id": source.id, "rows": len(source),
                                "class": type(source).__module__ + "." + type(source).__qualname__}
    generator = getattr(operator, "generator", None)
    if generator is not None:
        descriptor["generator"] = _value({name: getattr(generator, name) for name in (
            "model", "model_name", "cardinality", "prompt_strategy", "reasoning_effort",
            "api_base", "desc",
        )})
        descriptor["prompt_factory"] = _value(vars(generator.prompt_factory))
    return descriptor


def _snapshot(plan):
    def tree(node):
        return {"operator": _operator(node.operator), "subplans": [tree(p) for p in node.subplans]}
    return json.dumps(tree(plan), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


@dataclass(frozen=True)
class FrozenPlan:
    """An in-process plan reference, not a serializable replacement execution graph.

    Source content, external caches and the dependency environment need separate runner
    manifests. The fingerprint includes source identity/length, never its full contents.
    """

    plan: object = field(repr=False, compare=False)
    plan_id: str
    fingerprint: str
    _snapshot_bytes: bytes = field(repr=False)
    _sources: tuple = field(repr=False, compare=False)

    def describe(self):
        return dict(upstream_commit=PALIMPZEST_COMMIT, plan_id=self.plan_id,
                    fingerprint=self.fingerprint,
                    operators=[type(op).__name__ for op in self.plan],
                    scheduler_owner="Palimpzest original execution strategy",
                    source_content_identity="caller workload manifest required")


def freeze_best_plan(query_processor, dataset=None):
    """Call the existing optimizer once; sampling/validation belongs to the caller.

    This is the fixed-plan execution boundary. It does not run QueryProcessor.execute's
    optional sentinel optimization and must not be presented as that full-system run.
    """
    validate_source()
    if getattr(query_processor, "validator", None) is not None:
        raise ValueError("PZ fixed-plan slice cannot skip validator sampling; use original execute()")
    plans = query_processor.optimizer.optimize(
        query_processor.dataset if dataset is None else dataset)
    plan = plans[0]
    snapshot = _snapshot(plan)
    sources = tuple(getattr(op, "datasource", None) for op in plan)
    return FrozenPlan(plan, plan.plan_id, hashlib.sha256(snapshot).hexdigest(), snapshot, sources)


def execute_frozen_plan(query_processor, frozen):
    """Return the original execute_plan result; no selection or scheduler is substituted."""
    validate_source()
    if type(frozen) is not FrozenPlan:
        raise TypeError("execute_frozen_plan requires FrozenPlan")
    sources = tuple(getattr(op, "datasource", None) for op in frozen.plan)
    if (frozen.plan.plan_id != frozen.plan_id or _snapshot(frozen.plan) != frozen._snapshot_bytes
            or len(sources) != len(frozen._sources)
            or any(actual is not expected for actual, expected in zip(sources, frozen._sources))):
        raise ValueError("PZ physical plan parameters changed after freezing")
    return query_processor.execution_strategy.execute_plan(plan=frozen.plan)
