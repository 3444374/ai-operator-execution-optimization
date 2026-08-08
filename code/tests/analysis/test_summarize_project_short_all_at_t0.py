import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "analysis"
    / "summarize_project_short_all_at_t0.py"
)
SPEC = importlib.util.spec_from_file_location("short_all_at_t0_summary", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ShortAllAtT0SummaryTest(unittest.TestCase):
    def test_timing_contract_forbids_misaligned_full_pipeline_rank(self) -> None:
        rows = {row["timer_id"]: row for row in MODULE._timing_contract_rows()}

        self.assertEqual(
            rows["T0_full_pipeline_wall"]["current_cross_system_status"],
            "unavailable",
        )
        self.assertEqual(
            rows["T3_model_request_window"]["current_cross_system_status"],
            "comparable_short_job_diagnostic",
        )

    def test_load_daft_service_windows_requires_three_clean_formals(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            values = [10.0, 11.0, 12.0]
            for repeat, value in enumerate(values, start=1):
                path = (
                    root
                    / "runs"
                    / f"00{repeat}_formal_0{repeat}_daft_native"
                    / "daft_native"
                    / "gate.json"
                )
                path.parent.mkdir(parents=True)
                path.write_text(
                    json.dumps(
                        {
                            "status": "passed",
                            "incidents": [],
                            "metrics": {
                                "manifest_rows": 512,
                                "group_service_wall_s": value,
                            },
                        }
                    ),
                    encoding="utf-8",
                )

            self.assertEqual(MODULE._load_daft_service_windows(root), values)


if __name__ == "__main__":
    unittest.main()
