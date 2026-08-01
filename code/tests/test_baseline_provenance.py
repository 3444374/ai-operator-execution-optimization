from __future__ import annotations

import sys
import unittest
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.provenance import (
    adapter_provenance,
    registered_adapters,
)


class BaselineProvenanceTests(unittest.TestCase):
    def test_native_arms_never_contain_project_scheduling(self) -> None:
        for adapter in registered_adapters():
            provenance = adapter_provenance(adapter)
            if provenance.formal_baseline_eligible:
                self.assertFalse(
                    provenance.custom_scheduling_code,
                    adapter,
                )

    def test_controls_and_service_ceiling_are_not_native_baselines(
        self,
    ) -> None:
        for adapter in (
            "vllm_bench",
            "bounded_http",
            "bounded_completions",
        ):
            provenance = adapter_provenance(adapter)
            self.assertFalse(provenance.formal_baseline_eligible)
            self.assertTrue(provenance.formal_control_eligible)

    def test_unclassified_adapter_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "no provenance"):
            adapter_provenance("custom_actor_pool")
