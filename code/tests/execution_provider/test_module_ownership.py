"""Incremental execution imports independently of synchronous execution."""

import os
from pathlib import Path
import subprocess
import sys
import unittest


class ModuleOwnershipTests(unittest.TestCase):
    def test_incremental_execution_imports_without_synchronous_facades(self):
        script = """
import sys
from src.execution_provider.adapters.incremental_execution import IncrementalExecution
from src.execution_provider.adapters.native_tasks import NativeTaskSession
from src.execution_provider.image_gateway import ImageGateway
from src.scheduling.core.session import SessionEngine
for name in (
    "src.execution_provider.adapters.semantic_session",
    "src.scheduling.core.scheduler",
):
    assert name not in sys.modules, name
"""
        root = Path(__file__).resolve().parents[2]
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            env=dict(os.environ, PYTHONPATH=str(root)),
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
