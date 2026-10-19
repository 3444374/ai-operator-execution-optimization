"""Replay exact 82e parent/actor methods in memory; expected result is four REDs.

Run from the repository root with PYTHONPATH=code using the existing CPU venv.
No file, original evidence, server, model or native-library source is modified.
The child serve implementation is unchanged by this fix and remains a CPU fake.
"""

import ast
import importlib
import subprocess
import textwrap

import pytest


BASE = "82e685b3233e6524732934bbf220fb5563e515f8"
METHODS = (
    ("src.serving.backends.async_engine_process", "AsyncEngineProcess", ("_io", "abort")),
    ("src.execution_provider.adapters.lotus_records_async_actor", "AsyncCoreModelActor", ("close",)),
)


def main():
    for module_name, class_name, names in METHODS:
        module = importlib.import_module(module_name)
        path = "code/" + module_name.replace(".", "/") + ".py"
        source = subprocess.check_output(["git", "show", BASE + ":" + path], text=True)
        tree = ast.parse(source)
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
        for name in names:
            method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == name)
            namespace = {}
            exec(compile(textwrap.dedent(ast.get_source_segment(source, method)),
                         "<git:" + BASE + ":" + path + ">", "exec"), module.__dict__, namespace)
            setattr(getattr(module, class_name), name, namespace[name])
    prefix = "code/tests/serving/test_async_cancel_close_order.py::CancelCloseOrderTests::"
    return pytest.main(["-q", "-p", "no:cacheprovider",
        prefix + "test_queued_abort_reaches_child_before_immediate_shutdown",
        prefix + "test_close_freeze_rejects_new_cancel_instead_of_claiming_late_acceptance",
        prefix + "test_close_hands_off_active_abort_when_cancel_rpc_enters_after_closing",
        "code/tests/execution_provider/test_async_cancel_core_close.py"])


if __name__ == "__main__":
    raise SystemExit(main())
