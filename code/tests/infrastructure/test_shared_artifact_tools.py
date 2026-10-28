"""Shared tool migration: legacy identity, private bytes and dependency boundaries."""

import importlib
import os
from pathlib import Path
import pickle
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


class SharedArtifactToolTests(unittest.TestCase):
    def test_redaction_legacy_module_and_pickle_globals_share_one_implementation(self):
        canonical = importlib.import_module("src.infrastructure.redact")
        legacy = importlib.import_module("src.baselines.common.redact")
        self.assertIs(legacy, canonical)
        for name in ("redact_text", "redact_argument_list", "redact_database_url", "redact_json_values"):
            with self.subTest(name=name):
                function = getattr(canonical, name)
                old_global = ("csrc.baselines.common.redact\n" + name + "\n.").encode()
                self.assertIs(pickle.loads(old_global), function)
                self.assertIs(pickle.loads(pickle.dumps(function)), function)

    def test_private_artifact_legacy_functions_keep_identity_and_pickle_compatibility(self):
        canonical = importlib.import_module("src.infrastructure.private_artifacts")
        legacy = importlib.import_module("src.baselines.common.private_artifacts")
        names = ("content_digest", "file_identity", "json_metric", "require_outside_git",
                 "new_private_directory", "open_private_text", "write_private_json", "publish_private_json")
        for name in names:
            with self.subTest(name=name):
                function = getattr(canonical, name)
                self.assertIs(getattr(legacy, name), function)
                old_global = ("csrc.baselines.common.private_artifacts\n" + name + "\n.").encode()
                self.assertIs(pickle.loads(old_global), function)
                self.assertIs(pickle.loads(pickle.dumps(function)), function)
        self.assertFalse(hasattr(canonical, "workload_summary"))
        self.assertEqual(legacy.workload_summary.__module__, legacy.__name__)

    def test_private_file_bytes_permissions_exclusive_creation_and_legacy_os_patch(self):
        from src.infrastructure.private_artifacts import open_private_text, write_private_json
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "verbatim.txt"
            with open_private_text(path, newline="") as stream:
                stream.write("unchanged\nprivate fixture\n")
            self.assertEqual(path.read_bytes(), b"unchanged\nprivate fixture\n")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                with open_private_text(path):
                    self.fail("an existing private artifact must not be opened")
            target = Path(directory) / "value.json"
            with patch("src.baselines.common.private_artifacts.os.fsync", wraps=os.fsync) as sync:
                write_private_json(target, {"value": "unchanged"})
            sync.assert_called_once()
            self.assertEqual(target.read_bytes(), b'{\n  "value": "unchanged"\n}\n')

    def test_neutral_imports_do_not_load_baselines_experiments_tests_or_model_libraries(self):
        code = Path(__file__).resolve().parents[2]
        program = '''
import importlib, importlib.abc, sys
sys.path.insert(0, sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        forbidden = ('src.baselines', 'src.experiments', 'tests', 'ray', 'daft',
                     'lotus', 'torch', 'transformers', 'vllm')
        if any(fullname == name or fullname.startswith(name + '.') for name in forbidden):
            raise AssertionError('unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
importlib.import_module('src.infrastructure.redact')
importlib.import_module('src.infrastructure.private_artifacts')
'''
        result = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(code)],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
