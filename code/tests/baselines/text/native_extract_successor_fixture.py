"""Local synthetic responses for independent Extract-successor contracts."""

from copy import deepcopy
from dataclasses import asdict
import hashlib
import json

from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, MapMethod, RecordOccurrence, StageSpec,
)
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_engine import GenerationSpec


SOURCE_TEXTS = (
    "fixture duplicated source document",
    "fixture separate source document",
    "fixture duplicated source document",
    "fixture empty source document",
)


def source_records():
    return [RecordOccurrence(
        "occ-" + str(position), position, label,
        dict(text=SOURCE_TEXTS[position], row_id="business-row-" + str(position),
             call_id="business-call-" + str(position), optional=None),
        source_refs={"input": {"row_id": "input-" + str(position), "source_position": position}},
    ) for position, label in enumerate((17, 29, 17, 53))]


def relation_plan(query_id="independent-native-extract-successors", pre_filter=False):
    model = "openai/local-fixture"
    stages = (
        StageSpec("extract", ExtractMethod(model, ("text",), {"field": None}),
                  GenerationSpec(max_tokens=64, stop=(), seed=11,
                                 response_format={"type": "json_object"})),
        StageSpec("map", MapMethod(model, "Copy the value of {field}.", "mapped"),
                  GenerationSpec(max_tokens=32, stop=(), seed=19)),
        StageSpec("filter", FilterMethod(model, "The value in {mapped} expresses keep broadcast."),
                  GenerationSpec(max_tokens=8, seed=23)),
    )
    if pre_filter:
        stages = (StageSpec("pre-filter", FilterMethod(model,
            "Keep {row_id} unless its numeric suffix equals two."),
            GenerationSpec(max_tokens=8, seed=7)), *stages)
    return LotusRecordChain(stages, query_id=query_id, relation_barriers=True)


def response(prepared_row, *, empty_key=False):
    """Return a raw answer; Map/Filter depend on the actual prepared document."""
    stage = prepared_row["algorithm_stage"]
    messages = json.loads(prepared_row["messages_json"])
    content = json.dumps(messages, ensure_ascii=False)
    if stage == "extract":
        position = prepared_row["source_position"]
        value = ({"field": "drop earlier", "retained": "first only"} if position == 0 else
                 {"field": "keep broadcast", "late": "later"} if position == 2 else {})
        if empty_key:
            value[""] = "extra"
        return json.dumps(value)
    if stage == "pre-filter":
        return "False" if "business-row-2" in content else "True"
    if any(text in content for text in SOURCE_TEXTS):
        raise AssertionError("successor consumed a source document instead of its derived column")
    # Filter's instruction names its criterion, so inspect the document message
    # rather than allowing the instruction itself to make every row positive.
    document = json.dumps(messages[-1], ensure_ascii=False)
    if stage == "map":
        return ("mapped keep broadcast" if "keep broadcast" in document else
                "mapped drop earlier" if "drop earlier" in document else "mapped missing")
    if stage == "filter":
        return "True" if "mapped keep broadcast" in document else "False"
    raise AssertionError("undeclared synthetic fixture stage: " + stage)


class FixtureInfer:
    def __init__(self, *, empty_key=False):
        self.empty_key = empty_key
        self.prepared, self.generations, self.batches = [], [], []

    def response(self, prepared_row):
        return response(prepared_row, empty_key=self.empty_key)

    def __call__(self, prepared, *, generation):
        rows, options = deepcopy(prepared), asdict(generation)
        self.prepared.extend(deepcopy(rows))
        self.generations.append(deepcopy(options))
        self.batches.append((deepcopy(rows), deepcopy(options)))
        return [dict(row, status="succeeded", text=self.response(row),
                     prompt_tokens=11, completion_tokens=3, total_tokens=14,
                     finish_reason="stop", submission_confirmed=True, logprobs_json="null",
                     engine_request_id="synthetic-" + row["call_id"],
                     engine_uid="independent-extract-contract-fixture",
                     messages_sha256=hashlib.sha256(json.dumps(json.loads(row["messages_json"]),
                         ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                         allow_nan=False).encode()).hexdigest(),
                     actual_model_call=False, usage_source="synthetic CPU fixture") for row in rows]
