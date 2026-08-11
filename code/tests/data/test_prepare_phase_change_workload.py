"""Contract tests for the project-derived phase-change workload builder."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DATA = Path(__file__).resolve().parents[2] / "scripts" / "data"
if str(SCRIPTS_DATA) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DATA))

import prepare_phase_change_workload as pc  # noqa: E402

CODE_ROOT = Path(__file__).resolve().parents[2]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.manifests import read_manifest, write_manifest  # noqa: E402


def _source_rows(start: int, count: int, target: int) -> tuple[pc.SourceRow, ...]:
    return tuple(
        pc.SourceRow(
            doc_id=start + index,
            tenant_id=index % 3,
            category="short" if target < 512 else "long",
            text=f"real prompt {start + index}",
            prompt_tokens=target + (index % 5) - 2,
            session_id=f"source-{start + index}",
            prefix_key=f"prefix-{index % 7}",
        )
        for index in range(count)
    )


class TestPhaseChangeWorkload(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = pc.PhaseChangeSpec(rate_a=0.1, rate_b=0.1)

    def test_derived_note_disclaims_official_vtc(self) -> None:
        self.assertIn("not official VTC reproduction", pc.DERIVED_NOTE)

    def test_poisson_is_deterministic_and_inside_window(self) -> None:
        first = pc.poisson_arrivals(5.0, 60.0, 120.0, 42)
        second = pc.poisson_arrivals(5.0, 60.0, 120.0, 42)
        self.assertEqual(first, second)
        self.assertTrue(all(60.0 < value < 120.0 for value in first))
        self.assertGreater(len(first), 100)

    def test_off_first_phase_order(self) -> None:
        self.assertEqual(
            pc.phase_segments(240.0, 60.0),
            (
                (0.0, 60.0, False),
                (60.0, 120.0, True),
                (120.0, 180.0, False),
                (180.0, 240.0, True),
            ),
        )

    def test_build_is_disjoint_and_job_b_only_arrives_when_active(self) -> None:
        jobs, manifests = pc.build_phase_change(
            _source_rows(1, 200, 256),
            _source_rows(1001, 200, 1024),
            spec=self.spec,
            workload_name="phase_change_test",
            doc_id_base=900000,
            seed=17,
            endpoint_count=2,
        )
        self.assertEqual(len(jobs), 2)
        self.assertEqual([len(job) for job in jobs], [len(item) for item in manifests])
        self.assertFalse(
            {row.source_doc_id for row in jobs[0]}
            & {row.source_doc_id for row in jobs[1]}
        )
        self.assertTrue(
            all(
                60.0 < row.arrival_time_s < 120.0
                or 180.0 < row.arrival_time_s < 240.0
                for row in jobs[1]
            )
        )
        self.assertTrue(
            all(request.max_output_tokens == 512 for job in manifests for request in job)
        )

    def test_manifest_round_trip_uses_canonical_schema(self) -> None:
        _, manifests = pc.build_phase_change(
            _source_rows(1, 200, 256),
            _source_rows(1001, 200, 1024),
            spec=self.spec,
            workload_name="phase_change_test",
            doc_id_base=910000,
            seed=23,
            endpoint_count=2,
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "client_0.jsonl"
            write_manifest(path, manifests[0])
            self.assertEqual(read_manifest(path), manifests[0])

    def test_exact_distance_shortage_fails_without_relaxing(self) -> None:
        with self.assertRaisesRegex(ValueError, "stop without repeating or loosening"):
            pc._select_source_rows(
                _source_rows(1, 4, 260),
                target_tokens=256,
                max_distance=1,
                count=1,
                seed=1,
                label="short",
            )

    def test_source_overlap_is_rejected(self) -> None:
        rows = _source_rows(1, 200, 256)
        overlapping_long = tuple(
            pc.SourceRow(
                doc_id=row.doc_id,
                tenant_id=row.tenant_id,
                category="long",
                text=row.text,
                prompt_tokens=1024,
                session_id=row.session_id,
                prefix_key=row.prefix_key,
            )
            for row in rows
        )
        with self.assertRaisesRegex(ValueError, "selections overlap"):
            pc.build_phase_change(
                rows,
                overlapping_long,
                spec=self.spec,
                workload_name="phase_change_test",
                doc_id_base=920000,
                seed=31,
                endpoint_count=2,
            )

    def test_nonempty_destination_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            (destination / "existing.txt").write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "absent or empty"):
                pc._prepare_destination(destination)

    def test_spec_rejects_less_than_two_cycles(self) -> None:
        with self.assertRaisesRegex(ValueError, "two complete OFF/ON cycles"):
            pc.PhaseChangeSpec(rate_a=1.0, rate_b=1.0, duration_s=180.0)


if __name__ == "__main__":
    unittest.main()
