"""Generative Map identities and completion policy at the public pure-value seam."""
import hashlib
from dataclasses import FrozenInstanceError
import unittest


class SemanticMapContractTests(unittest.TestCase):
    def test_map_plan_identity_matches_independent_ascii_vector(self) -> None:
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        self.assertEqual(hashlib.sha256(plan.canonical_bytes()).hexdigest(),
                         "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba")
        self.assertEqual(plan.digest, "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba")

    def test_map_plan_validates_all_variable_values_before_encoding(self) -> None:
        from src.execution_provider.semantic_map import SemanticMapPlan

        for instruction, model, maximum in (("", "model", 128), ("x" * 4097, "model", 128),
            ("x\0secret", "model", 128), ("\ud800", "model", 128),
            ("echo", "", 128), ("echo", "é" * 65, 128), ("echo", "x\0secret", 128),
            ("echo", "\udfff", 128), ("echo", "model", 0), ("echo", "model", 4097),
            ("echo", "model", -1), ("echo", "model", True), ("echo", "model", 128.0)):
            with self.subTest(instruction_length=len(instruction), model_length=len(model), maximum=maximum):
                with self.assertRaises(ValueError):
                    SemanticMapPlan(instruction, model, maximum)
        for instruction, model in ((None, "model"), ("echo", None), (1, "model"), ("echo", [])):
            with self.subTest(instruction_type=type(instruction).__name__, model_type=type(model).__name__):
                with self.assertRaises(TypeError):
                    SemanticMapPlan(instruction, model, 128)

    def test_map_plan_preserves_identity_and_exposes_fresh_generation_values(self) -> None:
        from src.execution_provider.semantic_map import SemanticMapPlan, PROMPT_PROGRAM_DIGEST, RESULT_PARSER_DIGEST

        self.assertEqual(PROMPT_PROGRAM_DIGEST, "72bbbd2abec0c7167158200281b7a88c44b94cd949f8b63f398a9101f8826afb")
        self.assertEqual(RESULT_PARSER_DIGEST, "540ea50c27d6f2d6800146b3b26404b4a5a64c6debef02e5501e67a829caec07")
        plan = SemanticMapPlan("原样返回输入。", "golden-map-v1", 128)
        self.assertEqual(plan.digest, "85c6173c584925bc2c400eebb78bd752e898c1971f7c8d9ecd8c4b83a43e58fd")
        with self.assertRaises(FrozenInstanceError):
            plan.instruction = "changed"
        expected = {"temperature": 0, "top_p": 1, "max_tokens": 128, "n": 1, "stream": False, "stop": None}
        self.assertEqual(plan.generation_constraints(), expected)
        exported = plan.generation_constraints()
        exported["max_tokens"] = 8
        exported["stop"] = ["\n"]
        self.assertEqual(plan.generation_constraints(), expected)
        for alternative in (SemanticMapPlan("原样返回输入。 ", "golden-map-v1", 128),
                            SemanticMapPlan("原样返回输入。", "another-model", 128),
                            SemanticMapPlan("原样返回输入。", "golden-map-v1", 127)):
            self.assertNotEqual(alternative.digest, plan.digest)
        for maximum in (1, 4096):
            self.assertEqual(SemanticMapPlan("é" * 2048, "é" * 64, maximum).max_tokens, maximum)

    def test_shared_completion_preserves_raw_values(self) -> None:
        from src.execution_provider.completion import Completion

        value = Completion(" \nTRUE\t", "golden-map-v1", 17, 1, "stop")
        self.assertEqual(value.raw_output, " \nTRUE\t")
        with self.assertRaises(FrozenInstanceError):
            value.raw_output = "trimmed"

    def test_map_completion_preserves_text_and_distinguishes_failure_classes(self) -> None:
        from src.execution_provider.completion import Completion
        from src.execution_provider.semantic_map import SemanticMapPlan, MapCompletionStatus, completion_status

        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        for output in ("", " \nTRUE\tFALSE\nUNKNOWN ", "甲\n\"乙\"\\丙\t{}", "é" * 32768):
            with self.subTest(output_bytes=len(output.encode())):
                value = Completion(output, "golden-map-v1", 2**64 - 1, 0, "stop")
                self.assertEqual(completion_status(plan, value), MapCompletionStatus.VALID)
                self.assertEqual(value.raw_output, output)
        for reason in ("length", "content_filter", "tool_calls", "x" * 32):
            with self.subTest(reason=reason):
                self.assertEqual(completion_status(plan, Completion("text", "golden-map-v1", 0, 128, reason)),
                                 MapCompletionStatus.INCOMPLETE)
        for reason in ("stop", "length"):
            with self.subTest(reason=reason):
                self.assertEqual(completion_status(plan, Completion("x" * 65537, "golden-map-v1", 0, 1, reason)),
                                 MapCompletionStatus.TOO_LARGE)

    def test_map_completion_checks_representation_model_and_usage_first(self) -> None:
        from src.execution_provider.completion import Completion
        from src.execution_provider.semantic_map import SemanticMapPlan, MapCompletionStatus, completion_status

        plan = SemanticMapPlan("echo", "golden-map-v1", 128)
        valid = dict(raw_output="x" * 65537, response_model_id="golden-map-v1", prompt_tokens=17,
                     output_tokens=1, finish_reason="length")
        invalid_fields = {
            "raw_output": [("null", None), ("bytes", b"text"), ("nul", "x\0y"), ("surrogate", "x\ud800y")],
            "response_model_id": [("null", None), ("empty", ""), ("mismatch", "another-model"),
                                  ("too-long", "x" * 129), ("nul", "x\0y"), ("surrogate", "\udfff")],
            "prompt_tokens": [("null", None), ("boolean", True), ("negative", -1),
                              ("overflow", 2**64), ("float", 1.0)],
            "output_tokens": [("null", None), ("boolean", True), ("negative", -1),
                              ("plan-limit", 129), ("overflow", 2**64), ("float", 1.0)],
            "finish_reason": [("null", None), ("empty", ""), ("too-long", "x" * 33),
                              ("nul", "x\0y"), ("surrogate", "\ud800")],
        }
        for field, values in invalid_fields.items():
            for label, value in values:
                with self.subTest(field=field, invalid=label):
                    completion = Completion(**{**valid, field: value})
                    self.assertEqual(completion_status(plan, completion), MapCompletionStatus.INVALID)


if __name__ == "__main__":
    unittest.main()
