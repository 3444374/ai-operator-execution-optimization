"""Shared execution imports and current protocol objects retain their owners."""

import os
from pathlib import Path
import subprocess
import sys
import unittest


class ModuleOwnershipTests(unittest.TestCase):
    def test_shared_execution_imports_do_not_load_http_transports(self):
        script = """
import sys
from src.execution_provider.adapters.incremental_execution import IncrementalExecution
from src.execution_provider.adapters.native_tasks import NativeTaskSession
from src.execution_provider.image_gateway import ImageGateway
from src.scheduling.core.session import SessionEngine
for name in (
    "src.execution_provider.adapters.semantic_session",
    "src.execution_provider.adapters.openai_compatible_fixed",
    "src.execution_provider.adapters.async_fixed_model",
    "src.execution_provider.adapters.ray_map_transport",
    "src.execution_provider.wire.v5",
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

    def test_legacy_exports_are_aliases_not_duplicate_types(self):
        from src.execution_provider import completion
        from src.execution_provider.adapters import semantic_session
        from src.execution_provider.wire import map_codec, v5
        from src.scheduling.core import policy_contracts, scheduler

        for name in (
            "Completion",
            "CompletionRequest",
            "CompletionAdapterError",
            "CompletionAdapter",
        ):
            self.assertIs(getattr(semantic_session, name), getattr(completion, name))
        for name in ("AdmissionPolicy", "PoolRouter", "EndpointRouter"):
            self.assertIs(getattr(scheduler, name), getattr(policy_contracts, name))
        self.assertIs(v5.validate_open, map_codec.validate_open)
        self.assertIs(v5.OpenContext, map_codec.OpenContext)
