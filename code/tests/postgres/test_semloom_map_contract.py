"""Generative Map identities and completion policy at the public pure-value seam."""
import hashlib
import ctypes
from dataclasses import FrozenInstanceError
from pathlib import Path
import subprocess
import tempfile
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


class BoundValue(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("length", ctypes.c_uint32), ("is_null", ctypes.c_bool)]


class MapPlanValues(ctypes.Structure):
    _fields_ = [("instruction", BoundValue), ("model_id", BoundValue), ("max_tokens", ctypes.c_uint32)]


class MachineCompletion(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("length", ctypes.c_uint32), ("is_null", ctypes.c_bool),
                ("response_model_id", BoundValue), ("finish_reason", BoundValue),
                ("prompt_tokens", ctypes.c_uint64), ("output_tokens", ctypes.c_uint64)]


class MapCContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        source = Path(__file__).resolve().parents[2] / "postgres/semloom_pg/src"
        directory = tempfile.TemporaryDirectory(prefix="semloom-map-values-")
        cls.addClassCleanup(directory.cleanup)
        library = Path(directory.name) / "map-values.so"
        subprocess.run(["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pedantic",
                        "-shared", "-fPIC", str(source / "semantic_map_contract.c"),
                        str(source / "sem_text.c"), "-o", str(library)], check=True, capture_output=True)
        cls.library = ctypes.CDLL(str(library))
        cls.library.semloom_map_plan_encode.argtypes = [ctypes.POINTER(MapPlanValues), ctypes.c_void_p,
                                                       ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
        cls.library.semloom_map_plan_encode.restype = ctypes.c_bool

    def test_c_plan_identity_matches_independent_vectors_and_python(self) -> None:
        from src.execution_provider.semantic_map import SemanticMapPlan

        for instruction, digest in (("Echo the input.", "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba"),
                                    ("原样返回输入。", "85c6173c584925bc2c400eebb78bd752e898c1971f7c8d9ecd8c4b83a43e58fd")):
            with self.subTest(instruction=instruction):
                instruction_data = ctypes.create_string_buffer(instruction.encode())
                model_data = ctypes.create_string_buffer(b"golden-map-v1")
                plan = MapPlanValues(BoundValue(ctypes.addressof(instruction_data), len(instruction_data) - 1, False),
                                     BoundValue(ctypes.addressof(model_data), 13, False), 128)
                length = ctypes.c_size_t()
                self.assertTrue(self.library.semloom_map_plan_encode(ctypes.byref(plan), None, 0, ctypes.byref(length)))
                output = ctypes.create_string_buffer(length.value)
                self.assertTrue(self.library.semloom_map_plan_encode(ctypes.byref(plan), output, len(output), ctypes.byref(length)))
                self.assertEqual(hashlib.sha256(output.raw).hexdigest(), digest)
                self.assertEqual(output.raw, SemanticMapPlan(instruction, "golden-map-v1", 128).canonical_bytes())

    def test_c_completion_policy_matches_python_and_never_changes_text(self) -> None:
        from src.execution_provider.completion import Completion
        from src.execution_provider.semantic_map import SemanticMapPlan, completion_status

        function = self.library.semloom_map_completion_status
        function.argtypes = [ctypes.POINTER(MapPlanValues), ctypes.POINTER(MachineCompletion)]
        function.restype = ctypes.c_uint32
        instruction = ctypes.create_string_buffer(b"echo")
        model = ctypes.create_string_buffer(b"golden-map-v1")
        c_plan = MapPlanValues(BoundValue(ctypes.addressof(instruction), 4, False),
                              BoundValue(ctypes.addressof(model), 13, False), 128)
        plan = SemanticMapPlan("echo", "golden-map-v1", 128)
        cases = (("", "stop", "golden-map-v1", 0, 0), (" \nTRUE\t", "stop", "golden-map-v1", 2**64 - 1, 128),
                 ("é" * 32768, "stop", "golden-map-v1", 1, 1), ("x" * 65537, "stop", "golden-map-v1", 1, 1),
                 ("x" * 65537, "length", "golden-map-v1", 1, 1), ("text", "length", "golden-map-v1", 1, 1),
                 ("x" * 65537, "length", "other-model", 1, 1), ("text", "stop", "golden-map-v1", 1, 129),
                 ("x\0y", "length", "golden-map-v1", 1, 1), ("x", "", "golden-map-v1", 1, 1),
                 ("x", "x" * 33, "golden-map-v1", 1, 1))
        for raw, reason, response_model, prompt_tokens, output_tokens in cases:
            with self.subTest(raw_length=len(raw), reason=reason, response_model=response_model, output_tokens=output_tokens):
                output = ctypes.create_string_buffer(raw.encode())
                finish = ctypes.create_string_buffer(reason.encode())
                response = ctypes.create_string_buffer(response_model.encode())
                completion = MachineCompletion(ctypes.addressof(output), len(output) - 1, False,
                    BoundValue(ctypes.addressof(response), len(response) - 1, False),
                    BoundValue(ctypes.addressof(finish), len(finish) - 1, False), prompt_tokens, output_tokens)
                expected = completion_status(plan, Completion(raw, response_model, prompt_tokens, output_tokens, reason))
                self.assertEqual(function(ctypes.byref(c_plan), ctypes.byref(completion)), expected)
                self.assertEqual(output.raw, raw.encode() + b"\0")

    def test_c_plan_rejects_invalid_values_without_partial_output(self) -> None:
        cases = ((b"", b"model", 128), (b"x" * 4097, b"model", 128), (b"\xff", b"model", 128),
                 (b"x\0y", b"model", 128), (b"echo", b"", 128), (b"echo", b"x" * 129, 128),
                 (b"echo", b"\xff", 128), (b"echo", b"x\0y", 128),
                 (b"echo", b"model", 0), (b"echo", b"model", 4097), (b"echo", b"model", 2**32 - 1))
        for instruction, model, maximum in cases:
            with self.subTest(instruction_length=len(instruction), model_length=len(model), maximum=maximum):
                instruction_data = ctypes.create_string_buffer(instruction)
                model_data = ctypes.create_string_buffer(model)
                plan = MapPlanValues(BoundValue(ctypes.addressof(instruction_data), len(instruction), False),
                                     BoundValue(ctypes.addressof(model_data), len(model), False), maximum)
                written = ctypes.c_size_t(999)
                output = ctypes.create_string_buffer(b"!" * 8192, 8192)
                self.assertFalse(self.library.semloom_map_plan_encode(ctypes.byref(plan), output, len(output), ctypes.byref(written)))
                self.assertEqual(written.value, 0)
                self.assertEqual(output.raw, b"!" * 8192)

    def test_c_plan_capacity_and_borrowed_null_flags_are_checked(self) -> None:
        instruction = ctypes.create_string_buffer(b"echo", 4)
        model = ctypes.create_string_buffer(b"model", 5)
        plan = MapPlanValues(BoundValue(ctypes.addressof(instruction), 4, False),
                             BoundValue(ctypes.addressof(model), 5, False), 128)
        needed = ctypes.c_size_t()
        function = self.library.semloom_map_plan_encode
        self.assertTrue(function(ctypes.byref(plan), None, 0, ctypes.byref(needed)))
        for capacity in (0, needed.value - 1):
            output = ctypes.create_string_buffer(b"!" * (needed.value + 2), needed.value + 2)
            written = ctypes.c_size_t(999)
            self.assertFalse(function(ctypes.byref(plan), ctypes.byref(output, 1), capacity, ctypes.byref(written)))
            self.assertEqual(written.value, 0)
            self.assertEqual(output.raw, b"!" * len(output))
        output = ctypes.create_string_buffer(b"!" * (needed.value + 3), needed.value + 3)
        written = ctypes.c_size_t()
        self.assertTrue(function(ctypes.byref(plan), ctypes.byref(output, 1), needed.value + 1, ctypes.byref(written)))
        self.assertEqual(written.value, needed.value)
        self.assertEqual(output.raw[:1], b"!")
        self.assertEqual(output.raw[-2:], b"!!")
        for field in (plan.instruction, plan.model_id):
            field.is_null = True
            self.assertFalse(function(ctypes.byref(plan), None, 0, ctypes.byref(written)))
            field.is_null = False
        self.assertFalse(function(None, None, 0, ctypes.byref(written)))
        self.assertFalse(function(ctypes.byref(plan), None, 0, None))
        self.assertFalse(function(ctypes.byref(plan), None, 1, ctypes.byref(written)))

    def test_c_completion_rejects_invalid_storage_and_encoding(self) -> None:
        function = self.library.semloom_map_completion_status
        function.argtypes = [ctypes.POINTER(MapPlanValues), ctypes.POINTER(MachineCompletion)]
        function.restype = ctypes.c_uint32
        instruction = ctypes.create_string_buffer(b"echo")
        model = ctypes.create_string_buffer(b"model")
        finish = ctypes.create_string_buffer(b"stop")
        bad = ctypes.create_string_buffer(b"\xed\xa0\x80")
        plan = MapPlanValues(BoundValue(ctypes.addressof(instruction), 4, False),
                             BoundValue(ctypes.addressof(model), 5, False), 128)
        for field in ("output", "model", "finish"):
            with self.subTest(field=field):
                completion = MachineCompletion(None, 0, False,
                    BoundValue(ctypes.addressof(model), 5, False),
                    BoundValue(ctypes.addressof(finish), 4, False), 0, 0)
                self.assertEqual(function(ctypes.byref(plan), ctypes.byref(completion)), 0)
                if field == "output":
                    completion.data, completion.length = ctypes.addressof(bad), 3
                elif field == "model":
                    completion.response_model_id = BoundValue(ctypes.addressof(bad), 3, False)
                else:
                    completion.finish_reason = BoundValue(ctypes.addressof(bad), 3, False)
                self.assertEqual(function(ctypes.byref(plan), ctypes.byref(completion)), 1)
        self.assertEqual(function(ctypes.byref(plan), None), 1)
        self.assertEqual(function(None, None), 1)


if __name__ == "__main__":
    unittest.main()
