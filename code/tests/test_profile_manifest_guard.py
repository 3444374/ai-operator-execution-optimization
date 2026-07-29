from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.contracts import ChatRequest  # noqa: E402
from src.profiling.manifest_guard import (  # noqa: E402
    ProfileManifestGuard,
    validate_profile_manifest_contract,
)


def request(
    doc_id: int,
    *,
    prompt: str,
    prompt_tokens: int,
    output_tokens: int,
    endpoint_index: int,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=prompt,
        arrival_time_s=0.0,
        prompt_tokens=prompt_tokens,
        max_output_tokens=256,
        estimated_output_tokens=output_tokens,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=endpoint_index,
    )


def table(
    *,
    doc_ids: list[int] | None = None,
    prompts: list[str] | None = None,
    prompt_tokens: list[int] | None = None,
    output_tokens: list[int] | None = None,
) -> pa.Table:
    return pa.table(
        {
            "doc_id": doc_ids or [1, 2],
            "text": prompts or ["one", "two"],
            "prompt_tokens": prompt_tokens or [3, 4],
            "target_output_tokens": output_tokens or [7, 8],
        }
    )


class ProfileManifestGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.requests = (
            request(
                1,
                prompt="one",
                prompt_tokens=3,
                output_tokens=7,
                endpoint_index=1,
            ),
            request(
                2,
                prompt="two",
                prompt_tokens=4,
                output_tokens=8,
                endpoint_index=0,
            ),
        )

    def guard(self) -> ProfileManifestGuard:
        return ProfileManifestGuard(
            requests=self.requests,
            manifest_sha256="a" * 64,
            endpoint_ids=("endpoint-0", "endpoint-1"),
        )

    def test_guard_validates_rows_and_adds_pinned_endpoint_id(self) -> None:
        guard = self.guard()

        annotated = guard.validate_and_annotate(table())
        evidence = guard.finish()

        self.assertEqual(
            annotated["preferred_endpoint_id"].to_pylist(),
            ["endpoint-1", "endpoint-0"],
        )
        self.assertEqual(evidence.manifest_sha256, "a" * 64)
        self.assertEqual(evidence.manifest_rows, 2)
        self.assertEqual(evidence.validated_rows, 2)

    def test_guard_rejects_row_semantic_mismatches(self) -> None:
        invalid = [
            ({"prompts": ["changed", "two"]}, "prompt mismatch"),
            ({"prompt_tokens": [30, 4]}, "prompt_tokens mismatch"),
            ({"output_tokens": [70, 8]}, "target_output_tokens mismatch"),
        ]

        for columns, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self.guard().validate_and_annotate(table(**columns))

    def test_guard_rejects_duplicate_and_missing_rows(self) -> None:
        guard = self.guard()
        guard.validate_and_annotate(
            table(
                doc_ids=[1],
                prompts=["one"],
                prompt_tokens=[3],
                output_tokens=[7],
            )
        )

        with self.assertRaisesRegex(ValueError, "duplicate manifest row"):
            guard.validate_and_annotate(
                table(
                    doc_ids=[1],
                    prompts=["one"],
                    prompt_tokens=[3],
                    output_tokens=[7],
                )
            )
        with self.assertRaisesRegex(ValueError, "missing manifest rows"):
            guard.finish()

    def test_guard_rejects_endpoint_index_outside_topology(self) -> None:
        invalid_request = request(
            1,
            prompt="one",
            prompt_tokens=3,
            output_tokens=7,
            endpoint_index=2,
        )

        with self.assertRaisesRegex(ValueError, "endpoint_index"):
            ProfileManifestGuard(
                requests=(invalid_request,),
                manifest_sha256="a" * 64,
                endpoint_ids=("endpoint-0", "endpoint-1"),
            )

    def test_profile_manifest_contract_accepts_same_condition_request_run(
        self,
    ) -> None:
        validate_profile_manifest_contract(
            self.requests,
            total_rows=2,
            operator="ai_complete",
            model_backend="compatible_http",
            endpoint_count=2,
            completion_protocol="chat_completions",
            completion_max_tokens=256,
            output_cost_mode="trace_target_output",
            source_order="doc_id",
            executor="ray_actor",
            submission_granularity="request",
            endpoint_routing="manifest_pinned",
            arrival_replay=False,
        )

    def test_profile_manifest_contract_rejects_non_equivalent_runs(
        self,
    ) -> None:
        valid = {
            "total_rows": 2,
            "operator": "ai_complete",
            "model_backend": "compatible_http",
            "endpoint_count": 2,
            "completion_protocol": "chat_completions",
            "completion_max_tokens": 256,
            "output_cost_mode": "trace_target_output",
            "source_order": "doc_id",
            "executor": "ray_actor",
            "submission_granularity": "request",
            "endpoint_routing": "manifest_pinned",
            "arrival_replay": False,
        }
        invalid = [
            ("total_rows", 1, "row count"),
            ("operator", "ai_embed", "ai_complete"),
            ("model_backend", "fake", "compatible_http"),
            ("endpoint_count", 1, "two endpoints"),
            ("completion_protocol", "completions", "chat_completions"),
            ("completion_max_tokens", 128, "max output"),
            ("output_cost_mode", "fixed_output_cap", "trace_target_output"),
            ("source_order", "arrival_time", "doc_id"),
            ("executor", "ray_task", "ray_actor"),
            ("submission_granularity", "batch", "request"),
            ("endpoint_routing", "least_work", "manifest_pinned"),
            ("arrival_replay", True, "arrival replay"),
        ]

        for field, value, message in invalid:
            with self.subTest(field=field):
                options = {**valid, field: value}
                with self.assertRaisesRegex(ValueError, message):
                    validate_profile_manifest_contract(
                        self.requests,
                        **options,
                    )


if __name__ == "__main__":
    unittest.main()
