"""New entry points must not load legacy loops; compatibility names stay identical."""

import os
from pathlib import Path
import subprocess
import sys
import unittest


class ModuleOwnershipTests(unittest.TestCase):
    def test_incremental_imports_do_not_load_legacy_implementations(self):
        script = """
import sys
from src.execution_provider.adapters.incremental_session import IncrementalMapSessionAdapter
from src.scheduling.core.session import SessionEngine
for name in (
    "src.execution_provider.adapters.semantic_session",
    "src.execution_provider.adapters.openai_compatible_fixed",
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
        from src.execution_provider.adapters import (
            model_config,
            openai_compatible_fixed,
            semantic_session,
        )
        from src.execution_provider.wire import map_codec, v5
        from src.scheduling.core import policy_contracts, scheduler

        for name in (
            "Completion",
            "CompletionRequest",
            "CompletionAdapterError",
            "CompletionAdapter",
        ):
            self.assertIs(getattr(semantic_session, name), getattr(completion, name))
        for name in ("FixedModelConfig", "load_fixed_model_config"):
            self.assertIs(getattr(openai_compatible_fixed, name), getattr(model_config, name))
        for name in ("AdmissionPolicy", "PoolRouter", "EndpointRouter", "SharedCreditPolicy"):
            self.assertIs(getattr(scheduler, name), getattr(policy_contracts, name))
        self.assertIs(v5.validate_open, map_codec.validate_open)
        self.assertIs(v5.OpenContext, map_codec.OpenContext)
