"""Message bytes through the public C machine and Python semantic interfaces."""
from __future__ import annotations

import ctypes
from pathlib import Path
import subprocess
import tempfile
import unittest

from src.execution_provider.wire.semantic import canonical_messages


FILTER_MESSAGES = (
    b'[{"role":"system","content":"Evaluate whether the input satisfies the instruction. '
    b'Reply with exactly TRUE, FALSE, or UNKNOWN. Use UNKNOWN only when the input lacks '
    b'enough information.\\nInstruction:\\nEcho the input."},{"role":"user","content":"hello"}]'
)

MAP_VECTORS = (
    ("Echo the input.", "hello", 81,
     b'[{"role":"system","content":"Echo the input."},{"role":"user","content":"hello"}]'),
    ("Echo the input.", "", 76,
     b'[{"role":"system","content":"Echo the input."},{"role":"user","content":""}]'),
    ("原样返回输入。", '甲\n"乙"\\丙\t{}', 103,
     '[{"role":"system","content":"原样返回输入。"},{"role":"user","content":"甲\\n\\"乙\\"\\\\丙\\t{}"}]'.encode()),
)


class BoundValue(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("length", ctypes.c_uint32), ("is_null", ctypes.c_bool)]


class OperatorMachine(ctypes.Structure):
    _fields_ = [("methods", ctypes.c_void_p), ("plan_schema_version", ctypes.c_uint32),
                ("instruction", ctypes.c_void_p), ("instruction_length", ctypes.c_uint32),
                ("invalid_completion_message", ctypes.c_void_p)]


class MessageCompilationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        source = Path(__file__).resolve().parents[2] / "postgres/semloom_pg/src"
        directory = tempfile.TemporaryDirectory(prefix="semloom-message-test-")
        cls.addClassCleanup(directory.cleanup)
        library = Path(directory.name) / "messages.so"
        subprocess.run([
            "cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pedantic", "-shared", "-fPIC",
            str(source / "sem_operator_machine.c"), str(source / "sem_filter_machine.c"),
            str(source / "sem_map_machine.c"), str(source / "sem_message_writer.c"),
            str(source / "sem_text.c"), "-o", str(library),
        ], check=True, capture_output=True, text=True)
        cls.library = ctypes.CDLL(str(library))
        cls.library.semloom_operator_machine_init.argtypes = [ctypes.POINTER(OperatorMachine),
            ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32]
        cls.library.semloom_operator_machine_init.restype = ctypes.c_bool
        cls.library.semloom_operator_machine_task_size.argtypes = [
            ctypes.POINTER(OperatorMachine), ctypes.POINTER(BoundValue)]
        cls.library.semloom_operator_machine_task_size.restype = ctypes.c_size_t
        cls.library.semloom_operator_machine_write_task.argtypes = [
            ctypes.POINTER(OperatorMachine), ctypes.POINTER(BoundValue), ctypes.c_void_p, ctypes.c_size_t]
        cls.library.semloom_operator_machine_write_task.restype = ctypes.c_bool
        cls.library.semloom_operator_machine_handle_null.argtypes = [ctypes.POINTER(OperatorMachine)]
        cls.library.semloom_operator_machine_handle_null.restype = ctypes.c_int
        cls.library.semloom_map_task_size.argtypes = [ctypes.POINTER(BoundValue), ctypes.POINTER(BoundValue)]
        cls.library.semloom_map_task_size.restype = ctypes.c_size_t
        cls.library.semloom_map_write_task.argtypes = [ctypes.POINTER(BoundValue), ctypes.POINTER(BoundValue),
                                                     ctypes.c_void_p, ctypes.c_size_t]
        cls.library.semloom_map_write_task.restype = ctypes.c_bool

    def compile_map(self, instruction: bytes, value: bytes) -> bytes:
        instruction_data = ctypes.create_string_buffer(instruction, len(instruction))
        input_data = ctypes.create_string_buffer(value, len(value))
        bound_instruction = BoundValue(ctypes.addressof(instruction_data), len(instruction), False)
        bound_input = BoundValue(ctypes.addressof(input_data), len(value), False)
        length = self.library.semloom_map_task_size(ctypes.byref(bound_instruction), ctypes.byref(bound_input))
        self.assertGreater(length, 0)
        output = ctypes.create_string_buffer(length)
        self.assertTrue(self.library.semloom_map_write_task(
            ctypes.byref(bound_instruction), ctypes.byref(bound_input), output, length))
        return output.raw

    def test_existing_filter_profiles_keep_their_literal_messages(self) -> None:
        instruction = ctypes.create_string_buffer(b"Echo the input.")
        data = ctypes.create_string_buffer(b"hello")
        value = BoundValue(ctypes.addressof(data), 5, False)
        for schema in (2, 3):
            with self.subTest(schema=schema):
                machine = OperatorMachine()
                self.assertTrue(self.library.semloom_operator_machine_init(
                    ctypes.byref(machine), 2, schema, instruction, 15))
                length = self.library.semloom_operator_machine_task_size(ctypes.byref(machine), ctypes.byref(value))
                output = ctypes.create_string_buffer(length)
                self.assertTrue(self.library.semloom_operator_machine_write_task(
                    ctypes.byref(machine), ctypes.byref(value), output, length))
                self.assertEqual(output.raw, FILTER_MESSAGES)
        self.assertEqual(canonical_messages("Echo the input.", "hello"), FILTER_MESSAGES)

    def test_existing_filter_profiles_keep_unicode_and_escaping(self) -> None:
        instruction_text = " 判断\n\"内容\"\\\t\x01 "
        input_text = "甲\n\"乙\"\\丙\t{}"
        expected = (
            '[{"role":"system","content":"Evaluate whether the input satisfies the instruction. '
            'Reply with exactly TRUE, FALSE, or UNKNOWN. Use UNKNOWN only when the input lacks '
            'enough information.\\nInstruction:\\n 判断\\n\\"内容\\"\\\\\\t\\u0001 "},'
            '{"role":"user","content":"甲\\n\\"乙\\"\\\\丙\\t{}"}]'
        ).encode()
        instruction = ctypes.create_string_buffer(instruction_text.encode())
        data = ctypes.create_string_buffer(input_text.encode())
        value = BoundValue(ctypes.addressof(data), len(data) - 1, False)
        for schema in (2, 3):
            with self.subTest(schema=schema):
                machine = OperatorMachine()
                self.assertTrue(self.library.semloom_operator_machine_init(
                    ctypes.byref(machine), 2, schema, instruction, len(instruction) - 1))
                length = self.library.semloom_operator_machine_task_size(ctypes.byref(machine), ctypes.byref(value))
                output = ctypes.create_string_buffer(length)
                self.assertTrue(self.library.semloom_operator_machine_write_task(
                    ctypes.byref(machine), ctypes.byref(value), output, length))
                self.assertEqual(output.raw, expected)
        self.assertEqual(canonical_messages(instruction_text, input_text), expected)

    def test_recording_machines_still_do_not_compile_messages(self) -> None:
        data = ctypes.create_string_buffer(b"hello")
        value = BoundValue(ctypes.addressof(data), 5, False)
        for kind, null_disposition in ((1, 1), (2, 2)):
            with self.subTest(kind=kind):
                machine = OperatorMachine()
                self.assertTrue(self.library.semloom_operator_machine_init(ctypes.byref(machine), kind, 1, None, 0))
                self.assertEqual(self.library.semloom_operator_machine_task_size(ctypes.byref(machine), ctypes.byref(value)), 0)
                self.assertTrue(self.library.semloom_operator_machine_write_task(
                    ctypes.byref(machine), ctypes.byref(value), None, 0))
                self.assertEqual(self.library.semloom_operator_machine_handle_null(ctypes.byref(machine)), null_disposition)
        machine = OperatorMachine()
        self.assertFalse(self.library.semloom_operator_machine_init(ctypes.byref(machine), 1, 99, data, 5))

    def test_generated_machine_dispatch_uses_map_message_vectors(self) -> None:
        for instruction, value, length, expected in MAP_VECTORS:
            instruction_data = ctypes.create_string_buffer(instruction.encode())
            input_data = ctypes.create_string_buffer(value.encode())
            bound = BoundValue(ctypes.addressof(input_data), len(value.encode()), False)
            machine = OperatorMachine()
            self.assertTrue(self.library.semloom_operator_machine_init(
                ctypes.byref(machine), 1, 4, instruction_data, len(instruction.encode())))
            self.assertEqual(self.library.semloom_operator_machine_task_size(ctypes.byref(machine), ctypes.byref(bound)), length)
            output = ctypes.create_string_buffer(length)
            self.assertTrue(self.library.semloom_operator_machine_write_task(
                ctypes.byref(machine), ctypes.byref(bound), output, length))
            self.assertEqual(output.raw, expected)
            self.assertEqual(self.library.semloom_operator_machine_handle_null(ctypes.byref(machine)), 1)


    def test_map_compiles_two_verbatim_messages_without_filter_directive(self) -> None:
        for instruction, value, length, expected in MAP_VECTORS:
            with self.subTest(instruction=instruction, value=value):
                output = self.compile_map(instruction.encode(), value.encode())
                self.assertEqual(output, expected)
                self.assertEqual(len(output), length)

    def test_map_rejects_nul_and_malformed_utf8_before_publishing(self) -> None:
        invalid = (b"\0", b"x\0y", b"\x80", b"\xc0\xaf", b"\xe0\x80\x80",
                   b"\xed\xa0\x80", b"\xf0\x80\x80\x80", b"\xf4\x90\x80\x80",
                   b"\xf5\x80\x80\x80", b"\xe4\xb8", b"\xc2A", b"\xff")
        for field in ("instruction", "input"):
            for data in invalid:
                with self.subTest(field=field, data=data.hex()):
                    instruction_data = ctypes.create_string_buffer(data if field == "instruction" else b"echo")
                    input_data = ctypes.create_string_buffer(data if field == "input" else b"text")
                    instruction = BoundValue(ctypes.addressof(instruction_data), len(instruction_data)-1, False)
                    value = BoundValue(ctypes.addressof(input_data), len(input_data)-1, False)
                    self.assertEqual(self.library.semloom_map_task_size(ctypes.byref(instruction), ctypes.byref(value)), 0)
                    output = ctypes.create_string_buffer(b"sentinel", 8)
                    self.assertFalse(self.library.semloom_map_write_task(
                        ctypes.byref(instruction), ctypes.byref(value), output, len(output)))
                    self.assertEqual(output.raw, b"sentinel")

    def test_map_rejects_oversized_text_before_publishing(self) -> None:
        for instruction, value in ((b"x" * 4097, b""), (b"echo", b"x" * 163841),
                                   ("é".encode() * 2048 + b"x", b""),
                                   (b"echo", "é".encode() * 81920 + b"x")):
            with self.subTest(instruction_length=len(instruction), input_length=len(value)):
                instruction_data = ctypes.create_string_buffer(instruction)
                input_data = ctypes.create_string_buffer(value)
                bound_instruction = BoundValue(ctypes.addressof(instruction_data), len(instruction), False)
                bound_input = BoundValue(ctypes.addressof(input_data), len(value), False)
                self.assertEqual(self.library.semloom_map_task_size(
                    ctypes.byref(bound_instruction), ctypes.byref(bound_input)), 0)
                output = ctypes.create_string_buffer(b"sentinel", 8)
                self.assertFalse(self.library.semloom_map_write_task(
                    ctypes.byref(bound_instruction), ctypes.byref(bound_input), output, len(output)))
                self.assertEqual(output.raw, b"sentinel")

    def test_map_distinguishes_empty_from_null_and_invalid_storage(self) -> None:
        data = ctypes.create_string_buffer(b"echo")
        valid = BoundValue(ctypes.addressof(data), 4, False)
        empty = BoundValue(None, 0, False)
        self.assertEqual(self.library.semloom_map_task_size(ctypes.byref(valid), ctypes.byref(empty)), 65)
        for bad in (None, BoundValue(None, 0, True), BoundValue(ctypes.addressof(data), 4, True),
                    BoundValue(None, 1, False)):
            pointer = None if bad is None else ctypes.byref(bad)
            for instruction, value in ((pointer, ctypes.byref(valid)), (ctypes.byref(valid), pointer)):
                with self.subTest(bad=bad, instruction=instruction):
                    self.assertEqual(self.library.semloom_map_task_size(instruction, value), 0)
                    output = ctypes.create_string_buffer(b"sentinel", 8)
                    self.assertFalse(self.library.semloom_map_write_task(instruction, value, output, len(output)))
                    self.assertEqual(output.raw, b"sentinel")
        self.assertEqual(self.library.semloom_map_task_size(ctypes.byref(empty), ctypes.byref(valid)), 0)

    def test_map_requires_exact_capacity_without_writing_on_failure(self) -> None:
        instruction_data = ctypes.create_string_buffer(b"Echo the input.", 15)
        input_data = ctypes.create_string_buffer(b"hello", 5)
        instruction = BoundValue(ctypes.addressof(instruction_data), 15, False)
        value = BoundValue(ctypes.addressof(input_data), 5, False)
        for capacity in (0, 1, 80, 82):
            with self.subTest(capacity=capacity):
                output = ctypes.create_string_buffer(b"!" * 84, 84)
                self.assertFalse(self.library.semloom_map_write_task(
                    ctypes.byref(instruction), ctypes.byref(value), ctypes.byref(output, 1), capacity))
                self.assertEqual(output.raw, b"!" * 84)
        self.assertFalse(self.library.semloom_map_write_task(
            ctypes.byref(instruction), ctypes.byref(value), None, 81))
        guarded = ctypes.create_string_buffer(b"!" * 83, 83)
        self.assertTrue(self.library.semloom_map_write_task(
            ctypes.byref(instruction), ctypes.byref(value), ctypes.byref(guarded, 1), 81))
        self.assertEqual(guarded.raw, b"!" + MAP_VECTORS[0][3] + b"!")

    def test_map_preserves_scalar_boundaries_whitespace_and_normalization(self) -> None:
        scalars = "\u007f\u0080\u07ff\u0800\ud7ff\ue000\uffff\U00010000\U0010ffffé e\u0301"
        instruction = ' \t"\\\b\f\n\r\x01\x1f/ '
        expected = (b'[{"role":"system","content":" \\t\\"\\\\\\b\\f\\n\\r\\u0001\\u001f/ "},'
                    b'{"role":"user","content":"' + scalars.encode() + b'"}]')
        self.assertEqual(self.compile_map(instruction.encode(), scalars.encode()), expected)
        self.assertNotEqual(self.compile_map(b"echo", "é".encode()), self.compile_map(b"echo", "e\u0301".encode()))

    def test_map_accepts_maximum_utf8_byte_lengths_and_escape_expansion(self) -> None:
        output = self.compile_map(b"\x01" * 4096, b"\x01" * 163840)
        self.assertEqual(len(output), 1007677)
        self.assertEqual(output, b'[{"role":"system","content":"' + b"\\u0001" * 4096
                         + b'"},{"role":"user","content":"' + b"\\u0001" * 163840 + b'"}]')
        output = self.compile_map("é".encode() * 2048, "é".encode() * 81920)
        self.assertEqual(len(output), 167997)

    def test_python_map_compiles_the_same_literal_messages(self) -> None:
        from src.execution_provider.semantic_map import canonical_messages as map_messages

        for instruction, value, length, expected in MAP_VECTORS:
            with self.subTest(instruction=instruction, value=value):
                self.assertEqual(map_messages(instruction, value), expected)
                self.assertEqual(len(map_messages(instruction, value)), length)

    def test_python_map_rejects_invalid_text_with_static_errors(self) -> None:
        from src.execution_provider.semantic_map import canonical_messages as map_messages

        for field in ("instruction", "input"):
            limit = 4096 if field == "instruction" else 163840
            for invalid in ("x\0secret", "x\ud800secret", "x\udfffsecret", "x" * (limit + 1),
                            "é" * (limit // 2) + "x"):
                with self.subTest(field=field, length=len(invalid)):
                    with self.assertRaises(ValueError) as caught:
                        map_messages(invalid if field == "instruction" else "echo",
                                     invalid if field == "input" else "hello")
                    self.assertEqual(str(caught.exception), "invalid Map message text")
            for invalid in (None, 1, b"hello", []):
                with self.subTest(field=field, type=type(invalid).__name__):
                    with self.assertRaises(TypeError) as caught:
                        map_messages(invalid if field == "instruction" else "echo",
                                     invalid if field == "input" else "hello")
                    self.assertEqual(str(caught.exception), "Map message contents must be text")
        with self.assertRaisesRegex(ValueError, "^invalid Map message text$"):
            map_messages("", "hello")

    def test_python_and_c_map_match_text_and_byte_boundaries(self) -> None:
        from src.execution_provider.semantic_map import canonical_messages as map_messages

        for instruction, value in (("\x01" * 4096, "\x01" * 163840), ("é" * 2048, "é" * 81920),
                                   (' \t"\\\b\f\n\r\x01\x1f/ ', "é e\u0301\u007f\u0080\u07ff\u0800\ud7ff\ue000\uffff\U00010000\U0010ffff")):
            with self.subTest(instruction_bytes=len(instruction.encode()), input_bytes=len(value.encode())):
                self.assertEqual(map_messages(instruction, value), self.compile_map(instruction.encode(), value.encode()))


if __name__ == "__main__":
    unittest.main()
