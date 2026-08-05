from __future__ import annotations

import sys
import unittest
from pathlib import Path


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.provenance import (
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

    def test_direct_client_is_honestly_classified_as_project_scheduled_control(
        self,
    ) -> None:
        # direct_client runs the project's own asyncio.Semaphore concurrency cap,
        # so it MUST be marked custom_scheduling_code=True (project scheduling),
        # reuse the existing direct_client_control role (NOT an ad-hoc role
        # outside the ComparisonRole Literal), and stay a control (not a formal
        # baseline). Regression: an earlier entry used the invalid role
        # "direct_service_control" and lied custom_scheduling_code=False.
        provenance = adapter_provenance("direct_client")
        self.assertEqual(provenance.comparison_role, "direct_client_control")
        self.assertTrue(provenance.custom_scheduling_code)
        self.assertFalse(provenance.formal_baseline_eligible)
        self.assertTrue(provenance.formal_control_eligible)
        self.assertEqual(provenance.scheduler_owner,
                         "project_asyncio_semaphore_control")

    def test_project_static_is_method_under_test_not_baseline_or_control(
        self,
    ) -> None:
        # Regression (codex): project_static is the paper's frozen-best static
        # method -- the METHOD UNDER TEST. Three honesty constraints hold.
        provenance = adapter_provenance("project_static")
        # (1) the project method owns scheduling (Ray actor + static K/active-work
        #     + token-budget organizer) -> custom_scheduling_code MUST be True.
        self.assertTrue(provenance.custom_scheduling_code)
        # (2) the project method is NOT a vendor/product baseline.
        self.assertFalse(provenance.formal_baseline_eligible)
        # (3) its role is the dedicated project_scheduled_method, NOT any baseline
        #     or control role. Conflating it with direct_client_control would mix
        #     the method under test with a bare-client control in later grouping,
        #     figures, and audit.
        self.assertEqual(provenance.comparison_role, "project_scheduled_method")
        self.assertNotIn(
            provenance.comparison_role,
            {"direct_client_control", "framework_native_baseline",
             "database_product_native_baseline", "service_ceiling"},
        )
