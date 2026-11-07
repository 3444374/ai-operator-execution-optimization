"""Serializable LOTUS method specifications and relation occurrence contracts.

No formatter instance, model owner or chain runtime is stored in these values.
"""

from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
import json

from .engine import _input_instruction, bind_filter_method, validate_generation
from .records import FilterMethod, _resolved_columns
from src.serving.backends.vllm_protocol import GenerationSpec

# Keep the legacy serialized method tag readable as the same canonical type.
TextFilterMethod = FilterMethod


def _name(value, label):
    if type(value) is not str or not value.strip():
        raise ValueError(label + " must be a nonempty string")


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
        return cls(**value)


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
