"""Real formatter/parser checks for Q2b's explicit LOTUS strategy profile; no model."""

from collections import Counter
import hashlib
import importlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest, file_identity
from src.baselines.image import sembench_mmqa_compat as compat
from tests.experiments.test_sembench_mmqa import fixture_dataset, fixture_server
from tests.experiments.test_mmqa_service_observer import full_fixture_dataset


INSTRUCTION = "{image} shows the logo of horse racetrack {Track}"


class Q2bStrategyVersionTests(unittest.TestCase):
    def test_unknown_or_original_sdk_version_cannot_select_the_new_profile(self):
        for value in ("1.1.3", "1.2.5", "0.0.0"):
            with self.subTest(version=value), patch.object(compat, "version", return_value=value):
                with self.assertRaisesRegex(ValueError, "requires LOTUS 1.2.4"):
                    compat.resolve_join_strategy("zs-cot")

    def test_other_strategy_spellings_and_types_are_rejected(self):
        for value in (None, "cot", "zs_cot", "ZS_COT", "default", 1):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "only supports"):
                compat.resolve_join_strategy(value)


@unittest.skipUnless(os.environ.get("SEMLOOM_TEST_SEMBENCH_ROOT"),
                     "requires pinned SemBench and real LOTUS1.2.4 formatter/parser; no model")
class Q2bStrategyNativeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.checkout = Path(os.environ["SEMLOOM_TEST_SEMBENCH_ROOT"])

    def prepared(self, dataset, model, strategy):
        import pandas as pd
        from lotus.dtype_extensions.image import ImageArray
        from lotus.templates import task_instructions as formatter
        value = dataset.read()
        paths = [str(Path(value["data_path"]) / "images" / i["name"]) for i in value["images"]]
        left = formatter.df2multimodal_info(pd.DataFrame(dict(image=ImageArray(paths))), ["image"])
        right = formatter.df2multimodal_info(dataset.table_frame()[["Track"]], ["Track"])
        docs = [d for image in left for d in formatter.merge_multimodal_info([image], right)]
        return [formatter.filter_formatter(model, doc, INSTRUCTION, strategy=strategy) for doc in docs]

    def model(self, endpoint):
        from tokenizers import Tokenizer, models
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.sembench_animals import configure_lm
        tokenizer = self.root / "tokenizer.json"
        Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]")).save(str(tokenizer))
        config = FixedModelConfig(endpoint, "fixture-mmqa", 5000)
        return config, tokenizer, configure_lm(config, 2, 32, tokenizer)

    def test_325_reconstructed_prompts_change_only_system_reasoning_and_keep_media_and_track(self):
        dataset = full_fixture_dataset(self.root / "full-input", self.checkout)
        _config, _tokenizer, lm = self.model("http://127.0.0.1:1/v1/chat/completions")
        old = self.prepared(dataset, lm, "zs-cot")
        new = self.prepared(dataset, lm, compat.resolve_join_strategy("zs-cot"))
        self.assertEqual(len(old), 325)
        self.assertEqual(len(new), 325)
        for legacy, current in zip(old, new):
            self.assertEqual(legacy[1:], current[1:])
            self.assertNotEqual(legacy, current)
            self.assertNotIn("Let's think step by step.", legacy[0]["content"])
            self.assertIn("Let's think step by step.", current[0]["content"])
            self.assertIn("Reasoning:", current[0]["content"])
            self.assertIn("Answer:", current[0]["content"])
        self.assertEqual(hashlib.sha256(old[0][0]["content"].encode()).hexdigest(),
                         "4a36c7971be0da4bcb8ae751118dc2367a40093de07b3802c3206e1722663988")
        self.assertEqual(hashlib.sha256(new[0][0]["content"].encode()).hexdigest(),
                         "577dd2765ccbcbbd82d0ae8d91de961fe3b93fc091e8e003166b2732b72b8921")

    def test_original_query_outgoing_prompts_match_real_formatter_and_native_parser_handles_reasoning(self):
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.experiments.sembench_mmqa import run_query
        dataset = fixture_dataset(self.root / "input", self.checkout)
        ledger = CellBudgetLedger.create(self.root / "budget.sqlite", AttemptBudget("fixture-strategy", 12), deadline_utc=None)
        ledger.reserve_unit("q2b", 12)
        unit = ledger.claim_shared_unit("q2b")
        try:
            with fixture_server(cot=True) as (endpoint, calls):
                model, tokenizer, lm = self.model(endpoint)
                expected = self.prepared(dataset, lm, compat.resolve_join_strategy("zs-cot"))
                profile = dict(model=dict(id=model.model_id, revision="software-fixture", dtype="software-fixture"),
                    generation=dict(temperature=0, top_p=1, max_tokens=32, retries=0),
                    service_preprocess=dict(processor_revision="software-fixture", parameters=dict(no_model=True)),
                    tokenizer_sha256=file_identity(tokenizer)["sha256"])
                result = run_query(dataset, model, output=self.root / "result", unit=unit, tokenizer=tokenizer,
                    profile=profile, concurrency=2, max_tokens=32, max_request_bytes=1024 * 1024,
                    max_prepared_bytes=8 * 1024 * 1024, max_result_bytes=1024 * 1024)
                joins = [c["messages"] for c in calls if c.get("response_format") != {"type": "json_object"}]
                self.assertEqual(Counter(map(content_digest, joins)), Counter(map(content_digest, expected)))
                self.assertEqual(len(calls), 9)
        finally:
            ledger.close_shared_unit("q2b")
        self.assertEqual(result["status"], "completed")
        self.assertEqual([s["output_rows"] for s in result["stages"]], [3, 3])
        original = json.loads((self.root / "result/original-result.json").read_text())
        self.assertTrue(all(r[0] == 10 for r in original["data"]))
        self.assertEqual(result["strategy_compatibility"], compat.strategy_profile())
        self.assertEqual(result["stages"][0]["execution_profile"], compat.PROFILE_ID)

    def test_native_parser_keeps_final_answer_separate_from_opposite_reasoning_and_preserves_default(self):
        from lotus.sem_ops.postprocessors import filter_postprocess
        _config, _tokenizer, lm = self.model("http://127.0.0.1:1/v1/chat/completions")
        raw = ["Reasoning:\nTrue is the rejected candidate.\n\nAnswer: False",
               "Reasoning:\nFalse is the rejected candidate.\n\nAnswer: True", "Answer: False"]
        parsed = filter_postprocess(raw, lm)
        self.assertEqual(parsed.outputs, [False, True, False])
        self.assertEqual(parsed.raw_outputs, raw)
        self.assertIn("True", parsed.explanations[0])
        self.assertEqual(filter_postprocess(["no boolean token"], lm).outputs, [True])
        self.assertEqual(filter_postprocess(["no boolean token"], lm, default=False).outputs, [False])

    def test_mapping_reaches_native_accessor_and_restores_after_success_error_and_nested_use(self):
        Join = importlib.import_module("lotus.sem_ops.sem_join").SemJoinDataframe
        from lotus.types import ReasoningStrategy
        calls, sentinel = [], object()
        def native(*args, **kwargs):
            calls.append((args, kwargs))
            return sentinel
        with patch.object(Join, "__call__", native):
            with compat.q2b_join_compatibility():
                self.assertIs(Join.__call__(object(), object(), INSTRUCTION, strategy="zs-cot"), sentinel)
                with self.assertRaisesRegex(RuntimeError, "nested"):
                    with compat.q2b_join_compatibility():
                        pass
            self.assertIs(Join.__call__, native)
            with self.assertRaises(ZeroDivisionError):
                with compat.q2b_join_compatibility():
                    raise ZeroDivisionError("fixture")
            self.assertIs(Join.__call__, native)
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][1]["strategy"], ReasoningStrategy.ZS_COT)

    def test_extra_options_and_changed_enum_source_stop_before_native_call(self):
        Join = importlib.import_module("lotus.sem_ops.sem_join").SemJoinDataframe
        with patch.object(Join, "__call__") as native:
            with compat.q2b_join_compatibility():
                for kwargs in ({}, dict(strategy=None), dict(strategy="cot"),
                               dict(strategy="zs-cot", default=False)):
                    with self.subTest(kwargs=kwargs), self.assertRaisesRegex(ValueError, "unsupported"):
                        Join.__call__(object(), object(), INSTRUCTION, **kwargs)
            native.assert_not_called()
            with patch.dict(compat.STRATEGY_SOURCES, {"types.py": "0" * 64}):
                with self.assertRaisesRegex(ValueError, "source differs at lotus/types.py"):
                    with compat.q2b_join_compatibility():
                        pass
            self.assertIs(Join.__call__, native)
