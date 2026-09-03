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
            "cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic", "-shared", "-fPIC",
            str(source / "sem_operator_machine.c"), str(source / "sem_filter_machine.c"),
            str(source / "sem_map_machine.c"), "-o", str(library),
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

    def test_map_compiles_two_verbatim_messages_without_filter_directive(self) -> None:
        size = self.library.semloom_map_task_size
        size.argtypes = [ctypes.POINTER(BoundValue), ctypes.POINTER(BoundValue)]
        size.restype = ctypes.c_size_t
        write = self.library.semloom_map_write_task
        write.argtypes = [ctypes.POINTER(BoundValue), ctypes.POINTER(BoundValue),
                          ctypes.c_void_p, ctypes.c_size_t]
        write.restype = ctypes.c_bool
        instruction_data = ctypes.create_string_buffer(b"Echo the input.", 15)
        input_data = ctypes.create_string_buffer(b"hello", 5)
        instruction = BoundValue(ctypes.addressof(instruction_data), 15, False)
        value = BoundValue(ctypes.addressof(input_data), 5, False)
        output = ctypes.create_string_buffer(size(ctypes.byref(instruction), ctypes.byref(value)))
        self.assertTrue(write(ctypes.byref(instruction), ctypes.byref(value), output, len(output)))
        self.assertEqual(output.raw,
            b'[{"role":"system","content":"Echo the input."},{"role":"user","content":"hello"}]')
        self.assertEqual(len(output), 81)


if __name__ == "__main__":
    unittest.main()
