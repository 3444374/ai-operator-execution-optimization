"""Caller-owned LOTUS column order survives different worker hash seeds; CPU only."""

from copy import deepcopy
from dataclasses import replace
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus.records import FilterMethod, identity


LIBRARIES = all(importlib.util.find_spec(name) for name in ("lotus", "litellm", "pandas"))
CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
INSTRUCTION = "Read {heading}: decide whether {paragraph} is positive about {topic}"
VALUES = dict(paragraph="A clearly written paragraph，内容很清楚。", topic="writing", heading="Review")


PROBE = r'''
from dataclasses import asdict
import hashlib, importlib.metadata, json, sys
import lotus
import pandas as pd
from lotus.templates import task_instructions
from src.semantic_methods.lotus.engine import LotusFilterMethod, bind_filter_method
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, MapMethod, RecordOccurrence, StageSpec,
)
from src.semantic_methods.lotus.records import FilterMethod, identity
from src.serving.backends.vllm_engine import GenerationSpec

lotus.settings.configure(enable_cache=False)
instruction = "Read {heading}: decide whether {paragraph} is positive about {topic}"
values = dict(paragraph="A clearly written paragraph，内容很清楚。", topic="writing", heading="Review")
record = RecordOccurrence("row", 0, 17, values)

def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()

if sys.argv[1] == "caller":
    stages = (
        StageSpec("filter", FilterMethod("openai/local-fixture", instruction), GenerationSpec()),
        StageSpec("map", MapMethod("openai/local-fixture", "Summarize {paragraph} and {topic} under {heading}"),
                  GenerationSpec(max_tokens=32, stop=())),
        StageSpec("extract", ExtractMethod("openai/local-fixture", ("topic", "heading", "paragraph"),
                  {"summary": None}), GenerationSpec(stop=(), response_format={"type":"json_object"})),
    )
    chain = LotusRecordChain(stages, query_id="q")
    expected = []
    frame = pd.DataFrame([values], index=[17])
    for method in chain.methods:
        if method.stage.kind == "extract":
            columns = list(method.method.input_columns)
        else:
            columns = lotus.nl_expression.parse_cols(method.method.instruction)
            assert tuple(columns) == method.method.resolved_input_columns
        docs = task_instructions.df2multimodal_info(frame, columns)
        if method.stage.kind == "filter":
            rendered = lotus.nl_expression.nle2str(method.method.instruction, columns)
            messages = [task_instructions.filter_formatter(method.lm, doc, rendered,
                        system_prompt=method.method.system_prompt, output_tokens=method.method.output_tokens)
                        for doc in docs]
        elif method.stage.kind == "map":
            rendered = lotus.nl_expression.nle2str(method.method.instruction, columns)
            messages = [task_instructions.map_formatter(method.lm, doc, rendered,
                        system_prompt=method.method.system_prompt) for doc in docs]
        else:
            messages = [task_instructions.extract_formatter(method.lm, doc,
                        method.method.output_columns, method.method.extract_quotes) for doc in docs]
        expected.append(messages)
    print(json.dumps(dict(wire=chain.describe(), expected=expected,
                          caller_order=lotus.nl_expression.parse_cols(instruction)), ensure_ascii=False))
else:
    payload = json.load(sys.stdin)
    wire = payload["wire"]
    bound = FilterMethod.from_dict(wire["stages"][0]["method"])
    assert bind_filter_method(bound).describe() == bound.describe()
    direct = LotusFilterMethod(bound).prepare_rows([dict(identity("q", "row", 0), **values)])
    assert [json.loads(row["messages_json"]) for row in direct] == payload["expected"][0]
    chain = LotusRecordChain.from_dict(wire)
    messages = [[json.loads(row["messages_json"]) for row in method.prepare_rows([record], query_id="q")]
                for method in chain.methods]
    assert messages == payload["expected"]
    assert chain.methods[1].stage.bind().method.resolved_input_columns == chain.methods[1].method.resolved_input_columns
    assert chain.methods[2].method.input_columns == ("topic", "heading", "paragraph")
    assert chain.describe() == LotusRecordChain.from_dict(chain.describe()).describe()
    assert direct[0]["call_id"] == identity("q", "row", 0)["call_id"]
    assert "vllm" not in sys.modules
    print(json.dumps(dict(local_parse_order=lotus.nl_expression.parse_cols(instruction),
                          bound_order=list(bound.resolved_input_columns),
                          message_sha256=[digest(value) for value in messages],
                          model_calls=0, lotus_version=importlib.metadata.version("lotus-ai"))))
'''


@unittest.skipUnless(LIBRARIES, "requires the isolated pinned LOTUS CPU environment")
class ColumnBindingTests(unittest.TestCase):
    def setUp(self):
        import lotus
        from src.serving.backends.vllm_engine import GenerationSpec

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.generation = GenerationSpec()

    def test_binding_preserves_original_parse_order_is_owned_and_idempotent(self):
        import lotus
        from src.semantic_methods.lotus.engine import bind_filter_method
        from src.semantic_methods.lotus.record_chain import ExtractMethod, MapMethod, StageSpec
        from src.serving.backends.vllm_engine import GenerationSpec

        method = FilterMethod("fixture", INSTRUCTION)
        order = tuple(lotus.nl_expression.parse_cols(INSTRUCTION))
        bound = bind_filter_method(method)
        self.assertIsNone(method.resolved_input_columns)
        self.assertIsNot(bound, method)
        self.assertEqual(bound.resolved_input_columns, order)
        self.assertEqual(bind_filter_method(bound).describe(), bound.describe())
        mapped = StageSpec("map", MapMethod("fixture", INSTRUCTION), self.generation).bind()
        self.assertEqual(mapped.method.resolved_input_columns, order)
        self.assertEqual(mapped.bind().describe(), mapped.describe())
        extract = StageSpec("extract", ExtractMethod("fixture", ("topic", "heading"), {"summary": None}),
                            GenerationSpec(response_format={"type": "json_object"}))
        self.assertEqual(extract.bind().describe(), extract.describe())

    def test_unbound_or_changed_reference_sets_fail_before_lm_configuration(self):
        from src.semantic_methods.lotus.engine import LotusFilterMethod, bind_filter_method
        from src.semantic_methods.lotus.record_chain import LotusRecordMethod, MapMethod, StageSpec

        method = FilterMethod("fixture", INSTRUCTION)
        bound = bind_filter_method(method)
        changed = replace(bound, resolved_input_columns=("paragraph", "topic", "missing"))
        for candidate in (method, changed):
            with self.subTest(candidate=candidate), patch("lotus.models.LM") as constructor:
                with self.assertRaisesRegex(ValueError, "unbound|instruction references"):
                    LotusFilterMethod(candidate)
                constructor.assert_not_called()
        for columns in (None, ("paragraph", "topic", "missing")):
            stage = StageSpec("map", MapMethod("fixture", INSTRUCTION,
                                               resolved_input_columns=columns), self.generation)
            with self.subTest(columns=columns), patch("lotus.models.LM") as constructor:
                with self.assertRaisesRegex(ValueError, "unbound|instruction references"):
                    LotusRecordMethod(stage)
                constructor.assert_not_called()

    def test_chain_caller_binds_and_wire_rejects_missing_or_changed_binding(self):
        from src.semantic_methods.lotus.record_chain import LotusRecordChain, MapMethod, StageSpec

        stages = (StageSpec("filter", FilterMethod("fixture", INSTRUCTION), self.generation),
                  StageSpec("map", MapMethod("fixture", INSTRUCTION), self.generation))
        chain = LotusRecordChain(stages, query_id="q")
        self.assertTrue(all(stage.method.resolved_input_columns for stage in chain.stages))
        self.assertTrue(all(stage.method.resolved_input_columns is None for stage in stages))
        self.assertEqual(LotusRecordChain.from_dict(chain.describe()).describe(), chain.describe())
        for position in range(2):
            for change in ("missing", "changed"):
                wire = deepcopy(chain.describe())
                method = wire["stages"][position]["method"]
                if change == "missing":
                    method.pop("resolved_input_columns")
                else:
                    method["resolved_input_columns"] = ("paragraph", "topic", "missing")
                with self.subTest(position=position, change=change), self.assertRaisesRegex(
                    ValueError, "unbound|instruction references"
                ):
                    LotusRecordChain.from_dict(wire)

    def test_bound_filter_and_map_reject_missing_or_nontext_input_columns(self):
        from src.semantic_methods.lotus.engine import LotusFilterMethod, bind_filter_method
        from src.semantic_methods.lotus.record_chain import LotusRecordMethod, MapMethod, RecordOccurrence, StageSpec

        filtered = LotusFilterMethod(bind_filter_method(FilterMethod("fixture", INSTRUCTION)))
        mapped = LotusRecordMethod(StageSpec("map", MapMethod("fixture", INSTRUCTION), self.generation).bind())
        for value in (None, 12):
            values = dict(VALUES)
            if value is None:
                values.pop("topic")
            else:
                values["topic"] = value
            with self.subTest(value=value):
                with self.assertRaisesRegex(TypeError, "present text input columns"):
                    filtered.prepare_rows([dict(identity("q", "row", 0), **values)])
                with self.assertRaisesRegex(ValueError, "present text input columns"):
                    mapped.prepare_rows([RecordOccurrence("row", 0, 17, values)], query_id="q")

    def test_old_q3_config_binds_one_column_and_missing_payload_still_fails(self):
        from src.semantic_methods.lotus.engine import LotusFilterMethod, bind_filter_method

        old = FilterMethod.from_dict(dict(model_name="fixture"))
        bound = bind_filter_method(old)
        self.assertEqual(bound.resolved_input_columns, ("reviewText",))
        expected = identity("q", "row", 0)
        self.assertEqual(expected["call_id"], 'lotus-filter-v1:["q","row",0,0]')
        formatter = LotusFilterMethod(bound)
        self.assertEqual(formatter.prepare_rows([dict(expected, reviewText="good")])[0]["call_id"], expected["call_id"])
        with self.assertRaisesRegex(TypeError, "text input"):
            formatter.prepare_rows([expected])

    def test_actual_lotus_messages_match_caller_across_different_python_hash_seeds(self):
        def run(mode, seed, payload=None):
            result = subprocess.run([sys.executable, "-c", PROBE, mode], cwd=CODE_ROOT,
                                    input=None if payload is None else json.dumps(payload, ensure_ascii=False),
                                    capture_output=True, text=True, timeout=35,
                                    env=dict(os.environ, PYTHONHASHSEED=str(seed), HF_HUB_OFFLINE="1",
                                             TRANSFORMERS_OFFLINE="1", LITELLM_LOCAL_MODEL_COST_MAP="True"))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return json.loads(result.stdout)

        caller = run("caller", 7)
        workers = [dict(seed=seed, **run("worker", seed, caller)) for seed in (0, 1, 2)]
        self.assertGreater(len({tuple(caller["caller_order"]),
                                *(tuple(worker["local_parse_order"]) for worker in workers)}), 1)
        self.assertTrue(all(worker["bound_order"] == caller["caller_order"] for worker in workers))
        self.assertEqual(len({tuple(worker["message_sha256"]) for worker in workers}), 1)
        print(json.dumps(dict(test="real_lotus_bound_columns_across_hash_seeds", caller_seed=7,
                              caller_order=caller["caller_order"], workers=workers), ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
