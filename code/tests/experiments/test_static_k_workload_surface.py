from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.analysis.summarize_static_k_workload_surface import summarize  # noqa: E402


class StaticKWorkloadSurfaceTests(unittest.TestCase):
    def test_passes_when_optima_move_and_wrong_k_has_cost(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runs.csv"
            self._write(
                path,
                {
                    "low_arrival": {
                        64: (100.0, 10.0, 10.0),
                        128: (99.0, 9.0, 11.0),
                        256: (96.0, 8.0, 12.0),
                    },
                    "burst": {
                        64: (80.0, 7.0, 14.0),
                        128: (94.0, 9.0, 11.0),
                        256: (100.0, 10.0, 10.0),
                    },
                },
            )

            result = summarize(path)

            self.assertEqual(result["status"], "passed")
            self.assertEqual(
                result["selected_k_by_workload"],
                {"burst": 256, "low_arrival": 64},
            )

    def test_stops_when_one_static_region_is_robust(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "runs.csv"
            self._write(
                path,
                {
                    "low_arrival": {
                        64: (98.0, 9.8, 10.2),
                        128: (100.0, 10.0, 10.0),
                        256: (99.0, 9.9, 10.1),
                    },
                    "burst": {
                        64: (97.5, 9.7, 10.3),
                        128: (100.0, 10.0, 10.0),
                        256: (99.0, 9.9, 10.1),
                    },
                },
            )

            result = summarize(path)

            self.assertEqual(result["status"], "not_justified")
            self.assertEqual(result["decision"], "stop_adaptive_formal_ranking")

    @staticmethod
    def _write(
        path: Path,
        values: dict[str, dict[int, tuple[float, float, float]]],
    ) -> None:
        fields = [
            "status",
            "phase",
            "repeat_index",
            "scenario_id",
            "actor_worker_failures",
            "model_request_tokens_per_s",
            "request_slo_goodput_per_s",
            "e2e_s",
        ]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for workload, cells in values.items():
                for k, metrics in cells.items():
                    for repeat in range(1, 4):
                        writer.writerow(
                            {
                                "status": "ok",
                                "phase": "formal",
                                "repeat_index": repeat,
                                "scenario_id": f"{workload}_k{k}",
                                "actor_worker_failures": "0;0",
                                "model_request_tokens_per_s": metrics[0],
                                "request_slo_goodput_per_s": metrics[1],
                                "e2e_s": metrics[2],
                            }
                        )


if __name__ == "__main__":
    unittest.main()
