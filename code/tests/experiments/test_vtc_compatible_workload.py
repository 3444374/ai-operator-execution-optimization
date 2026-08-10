from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.experiments.vtc_compatible import (
    VtcSourceRow,
    build_suite,
    runner_environment,
    suite_spec,
)


def _source_rows(count: int) -> tuple[VtcSourceRow, ...]:
    return tuple(
        VtcSourceRow(
            doc_id=index,
            tenant_id=index % 8,
            category="chat",
            text=f"prompt {index}",
            prompt_tokens=256 + (index % 9) - 4,
            session_id=f"source-{index}",
            prefix_key=f"prefix-{index}",
        )
        for index in range(count)
    )


class VtcCompatibleWorkloadTests(unittest.TestCase):
    def test_runner_environment_restores_global_first_arrival_offsets(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for index in range(2):
                (root / f"client_{index}.jsonl").write_text("{}\n", encoding="utf-8")
            environment = runner_environment(
                {
                    "status": "prepared",
                    "target_workload": "vtc_on_off",
                    "suite": {
                        "suite_id": "on_off_overload",
                        "rates_per_s": [2.0, 3.0],
                    },
                    "job_row_counts": [7, 11],
                    "job_first_arrival_s": [0.25, 0.5],
                },
                root,
            )

        self.assertEqual(environment["VTC_ON_OFF_CLIENT0_ROWS"], "7")
        self.assertEqual(environment["VTC_ON_OFF_CLIENT1_OFFSET_S"], "0.5")

    def test_on_off_requires_two_complete_cycles(self) -> None:
        with self.assertRaisesRegex(ValueError, "two complete"):
            suite_spec("on_off_overload", duration_s=180.0)

    def test_overload_multi_build_is_deterministic_and_disjoint(self) -> None:
        spec = suite_spec("overload_multi", duration_s=180.0)
        first = build_suite(
            _source_rows(2000),
            spec=spec,
            workload_name="vtc-overload",
            doc_id_base=10_000,
            seed=7,
            endpoint_count=2,
        )
        second = build_suite(
            _source_rows(2000),
            spec=spec,
            workload_name="vtc-overload",
            doc_id_base=10_000,
            seed=7,
            endpoint_count=2,
        )

        self.assertEqual(first, second)
        jobs, manifests = first
        self.assertEqual(len(jobs), 8)
        doc_ids = [row.doc_id for job in jobs for row in job]
        self.assertEqual(len(doc_ids), len(set(doc_ids)))
        for rows, manifest in zip(jobs, manifests):
            self.assertEqual(len(rows), len(manifest))
            self.assertEqual(
                [row.arrival_time_s for row in rows],
                sorted(row.arrival_time_s for row in rows),
            )
            endpoint_counts = [
                sum(request.endpoint_index == endpoint for request in manifest)
                for endpoint in range(2)
            ]
            self.assertLessEqual(abs(endpoint_counts[0] - endpoint_counts[1]), 1)

    def test_overload_runner_environment_uses_config_prefix(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for index in range(8):
                (root / f"client_{index}.jsonl").write_text(
                    "{}\n", encoding="utf-8"
                )
            environment = runner_environment(
                {
                    "status": "prepared",
                    "target_workload": "vtc_overload_multi",
                    "suite": {
                        "suite_id": "overload_multi",
                        "rates_per_s": [0.4] * 8,
                    },
                    "job_row_counts": [1] * 8,
                    "job_first_arrival_s": [0.25] * 8,
                },
                root,
            )

        self.assertEqual(
            environment["VTC_OVERLOAD_WORKLOAD"],
            "vtc_overload_multi",
        )
        self.assertEqual(environment["VTC_OVERLOAD_CLIENT7_ROWS"], "1")

    def test_on_off_client_has_no_arrivals_in_off_windows(self) -> None:
        spec = suite_spec("on_off_overload", duration_s=240.0)
        jobs, _manifests = build_suite(
            _source_rows(2000),
            spec=spec,
            workload_name="vtc-on-off",
            doc_id_base=20_000,
            seed=11,
            endpoint_count=2,
        )

        self.assertTrue(jobs[0])
        self.assertTrue(
            all(int(row.arrival_time_s // 60.0) % 2 == 0 for row in jobs[0])
        )
        self.assertTrue(any(60.0 <= row.arrival_time_s < 120.0 for row in jobs[1]))


if __name__ == "__main__":
    unittest.main()
