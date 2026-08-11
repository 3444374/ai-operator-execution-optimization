"""Tests for fail-closed phase-change runner contracts."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DATA = CODE_ROOT / "scripts" / "data"
for item in (CODE_ROOT, SCRIPTS_DATA):
    if str(item) not in sys.path:
        sys.path.insert(0, str(item))

import prepare_phase_change_workload as builder  # noqa: E402
from src.experiments.phase_change import load_contract, runner_environment  # noqa: E402


def _source(start: int, target: int) -> tuple[builder.SourceRow, ...]:
    return tuple(
        builder.SourceRow(
            doc_id=start + index,
            tenant_id=0,
            category="test",
            text=f"prompt {start + index}",
            prompt_tokens=target,
            session_id="source",
            prefix_key="prefix",
        )
        for index in range(200)
    )


class TestPhaseChangeContract(unittest.TestCase):
    def _contract(self, root: Path) -> dict[str, object]:
        spec = builder.PhaseChangeSpec(rate_a=0.1, rate_b=0.1)
        jobs, manifests = builder.build_phase_change(
            _source(1, 256),
            _source(1001, 1024),
            spec=spec,
            workload_name="phase_change_contract_test",
            doc_id_base=800000,
            seed=41,
            endpoint_count=2,
        )
        audit = builder._write_contract(
            root,
            jobs=jobs,
            manifests=manifests,
            spec=spec,
            short_source="short_source",
            long_source="long_source",
            target_workload="phase_change_contract_test",
            doc_id_base=800000,
            endpoint_count=2,
            seed=41,
        )
        receipt = {
            "schema_version": 1,
            "status": "imported",
            "target_workload": audit["target_workload"],
            "inserted_rows": audit["total_rows"],
            "distinct_doc_ids": audit["total_rows"],
            "doc_id_range": audit["doc_id_range"],
            "output_cap": audit["spec"]["output_cap"],
            "server_version": "18.3",
            "pgvector_version": "0.8.1",
        }
        (root / "database_import_receipt.json").write_text(
            json.dumps(receipt), encoding="utf-8"
        )
        return audit

    def test_valid_contract_resolves_runner_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            expected = self._contract(root)
            audit = load_contract(root)
            environment = runner_environment(audit, root)
            self.assertEqual(audit["target_workload"], expected["target_workload"])
            self.assertEqual(audit["job_row_counts"], expected["job_row_counts"])
            self.assertEqual(environment["PHASE_CHANGE_OUTPUT_CAP"], "512")
            self.assertEqual(
                int(environment["PHASE_CHANGE_CLIENT0_ROWS"]),
                expected["job_row_counts"][0],
            )
            self.assertEqual(
                float(environment["PHASE_CHANGE_CLIENT0_OFFSET_S"]),
                expected["job_first_arrival_s"][0],
            )
            self.assertEqual(
                float(environment["PHASE_CHANGE_CLIENT1_OFFSET_S"]),
                expected["job_first_arrival_s"][1],
            )

    def test_manifest_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._contract(root)
            with (root / "client_0.jsonl").open("a", encoding="utf-8") as stream:
                stream.write("{}\n")
            with self.assertRaises((TypeError, ValueError)):
                load_contract(root)

    def test_missing_import_receipt_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._contract(root)
            (root / "database_import_receipt.json").unlink()
            with self.assertRaisesRegex(ValueError, "audit and import receipt"):
                load_contract(root)

    def test_double_length_last_phase_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._contract(root)
            audit_path = root / "audit.json"
            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            audit["phase_segments"][-1]["end_s"] = 300.0
            audit_path.write_text(json.dumps(audit), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "cover the duration"):
                load_contract(root)

    def test_tampered_first_arrival_offset_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._contract(root)
            audit_path = root / "audit.json"
            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            audit["job_first_arrival_s"][1] = 0.0
            audit_path.write_text(json.dumps(audit), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "first-arrival offset"):
                load_contract(root)


if __name__ == "__main__":
    unittest.main()
