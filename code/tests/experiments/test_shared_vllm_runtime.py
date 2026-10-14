from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.shared_vllm.runtime import (  # noqa: E402
    EndpointServiceRateTracker,
    build_observe_only_text_state_rows,
)
from src.experiments.shared_vllm.runner import _apply_state_control  # noqa: E402
from src.experiments.shared_vllm.evidence import (  # noqa: E402
    _write_json_atomic,
    _write_trace_rows_atomic,
)
from src.scheduling.submission_control.capacity import (  # noqa: E402
    BoundedCapacityController,
    CapacityArm,
)


class SharedVllmRuntimeStateTests(unittest.TestCase):
    def test_builds_observe_only_staged_state(self) -> None:
        rows = build_observe_only_text_state_rows(
            [
                {
                    "endpoint_id": "endpoint-0",
                    "observed_epoch_s": 10.0,
                    "elapsed_s": 2.0,
                    "request_limit": 128,
                    "work_limit": 131072,
                    "active_requests": 64,
                    "active_work": 32768,
                    "waiting_work": 8192,
                    "oldest_waiting_age_s": 0.5,
                }
            ],
            [
                {
                    "endpoint_index": 0,
                    "running": 60.0,
                    "waiting": 2.0,
                    "kv_usage": 0.25,
                }
            ],
            endpoint_ids=("endpoint-0",),
            calibration_signature="sig",
            service_rates={"endpoint-0": 7000.0},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["runtime_state_mode"], "observe_only")
        self.assertEqual(rows[0]["organizer_queued_work"], 8192)
        self.assertEqual(rows[0]["model_queued_work_estimated"], 1024)
        self.assertEqual(rows[0]["model_capacity_work"], 131072)
        self.assertEqual(rows[0]["service_rate_tokens_s"], 7000.0)

    def test_service_rate_tracker_uses_cumulative_token_delta(self) -> None:
        tracker = EndpointServiceRateTracker(alpha=1.0)
        first = tracker.update(
            [
                {
                    "endpoint_index": 0,
                    "observed_epoch_s": 10.0,
                    "completed_tokens_total": 100.0,
                }
            ],
            endpoint_ids=("endpoint-0",),
        )
        second = tracker.update(
            [
                {
                    "endpoint_index": 0,
                    "observed_epoch_s": 12.0,
                    "completed_tokens_total": 300.0,
                }
            ],
            endpoint_ids=("endpoint-0",),
        )

        self.assertEqual(first, {})
        self.assertEqual(second, {"endpoint-0": 100.0})

    def test_zero_completion_rate_remains_unavailable_in_typed_state(self) -> None:
        rows = build_observe_only_text_state_rows(
            [
                {
                    "endpoint_id": "endpoint-0",
                    "observed_epoch_s": 10.0,
                    "elapsed_s": 2.0,
                    "request_limit": 96,
                    "work_limit": 98304,
                    "active_requests": 0,
                    "active_work": 0,
                    "waiting_work": 0,
                    "oldest_waiting_age_s": 0.0,
                }
            ],
            [
                {
                    "endpoint_index": 0,
                    "running": 0.0,
                    "waiting": 0.0,
                    "kv_usage": 0.0,
                }
            ],
            endpoint_ids=("endpoint-0",),
            calibration_signature="sig",
            service_rates={"endpoint-0": 0.0},
        )

        self.assertEqual(rows[0]["service_rate_tokens_s"], "")

    def test_actuator_applies_bounded_capacity_decision(self) -> None:
        class Observer:
            def __init__(self) -> None:
                self.updates = []

            def update_capacity(self, endpoint_id, **kwargs):
                self.updates.append((endpoint_id, kwargs))
                return {"request_limit": kwargs["request_limit"]}

        controller = BoundedCapacityController(
            (CapacityArm(128, 131072), CapacityArm(160, 131072)),
            fallback=CapacityArm(128, 131072),
            target_service_rate_tokens_s=7600.0,
            consecutive_samples=1,
            cooldown_samples=0,
        )
        rows = build_observe_only_text_state_rows(
            [
                {
                    "endpoint_id": "endpoint-0",
                    "observed_epoch_s": 10.0,
                    "elapsed_s": 2.0,
                    "request_limit": 128,
                    "work_limit": 131072,
                    "active_requests": 128,
                    "active_work": 65536,
                    "waiting_work": 8192,
                    "oldest_waiting_age_s": 0.5,
                }
            ],
            [
                {
                    "endpoint_index": 0,
                    "running": 120.0,
                    "waiting": 0.0,
                    "kv_usage": 0.25,
                }
            ],
            endpoint_ids=("endpoint-0",),
            calibration_signature="sig",
            service_rates={"endpoint-0": 6000.0},
        )
        observer = Observer()

        controlled = _apply_state_control(
            rows,
            controllers={"endpoint-0": controller},
            observer=observer,
            calibration_signature="sig",
            max_state_age_s=10_000_000_000.0,
        )

        self.assertEqual(controlled[0]["control_action"], "increase")
        self.assertEqual(controlled[0]["control_applied_request_limit"], 160)
        self.assertEqual(observer.updates[0][1]["work_limit"], 131072)

    def test_real_helper_distinguishes_unknown_clear_and_queued_waiting(self) -> None:
        class Observer:
            def __init__(self):
                self.updates = []

            def update_capacity(self, endpoint_id, **limits):
                self.updates.append((endpoint_id, limits))
                return limits

        for waiting, action, arm in (
            ("", "fallback", (2, 20)),
            (None, "fallback", (2, 20)),
            (0, "increase", (4, 40)),
            (1, "decrease", (1, 10)),
        ):
            with self.subTest(waiting=waiting):
                controller = BoundedCapacityController(
                    (CapacityArm(1, 10), CapacityArm(2, 20), CapacityArm(4, 40)),
                    fallback=CapacityArm(2, 20),
                    target_service_rate_tokens_s=100,
                )
                observer = Observer()
                with patch.object(controller, "select", wraps=controller.select) as select:
                    for t in range(4):
                        row = {
                            "endpoint_id": "e", "vllm_waiting": waiting,
                            "service_rate_tokens_s": 1, "vllm_kv_usage": .1,
                            "model_active_work": 20, "organizer_queued_work": 100,
                            "active_requests": 2, "organizer_oldest_queue_age_s": 2,
                            "model_capacity_work": 20, "request_limit": 2,
                            "observed_epoch_s": t,
                        }
                        with patch("src.experiments.shared_vllm.runner.time.time", return_value=t):
                            _apply_state_control(
                                [row], controllers={"e": controller}, observer=observer,
                                calibration_signature="fixture", max_state_age_s=1,
                            )
                        self.assertEqual(row["vllm_waiting"], waiting)
                        self.assertEqual(row["endpoint_id"], "e")
                        self.assertEqual(row["observed_epoch_s"], t)
                        snapshot = select.call_args.args[0]
                        self.assertEqual(snapshot.observed_at_s, t)
                        self.assertEqual(snapshot.calibration_signature, "fixture")
                        model_queue = snapshot.for_stage("model").queued_work
                        self.assertEqual(
                            model_queue, None if waiting in (None, "") else waiting * 10,
                        )
                        self.assertEqual(
                            select.call_args.kwargs["service_waiting_requests"],
                            None if waiting == "" else waiting,
                        )
                self.assertEqual(row["control_action"], action)
                self.assertEqual(
                    (row["control_applied_request_limit"], row["control_applied_work_limit"]),
                    arm,
                )
                self.assertEqual(len(observer.updates), 0 if action == "fallback" else 1)

    def test_observe_only_missing_queue_is_not_an_estimated_zero(self) -> None:
        for resource in ({}, {"waiting": None}, {"waiting": ""}, {"waiting": 0}):
            with self.subTest(resource=resource):
                row = build_observe_only_text_state_rows(
                    [{
                        "endpoint_id": "e", "observed_epoch_s": 10.0, "elapsed_s": 2,
                        "request_limit": 2, "work_limit": 20, "active_requests": 2,
                        "active_work": 20, "waiting_work": 100, "oldest_waiting_age_s": 2,
                    }],
                    [{"endpoint_index": 0, "kv_usage": .1, **resource}],
                    endpoint_ids=("e",), calibration_signature="fixture",
                )[0]
                self.assertEqual(
                    row["model_queued_work_estimated"],
                    0 if resource.get("waiting") == 0 else None,
                )
                self.assertEqual(row["vllm_waiting"], resource.get("waiting", ""))
                self.assertEqual((row["endpoint_id"], row["observed_epoch_s"]), ("e", 10.0))
                self.assertEqual(row["organizer_queued_work"], 100)

    def test_saved_state_distinguishes_unknown_waiting_from_known_zero(self) -> None:
        for waiting in (None, "", 0):
            with self.subTest(waiting=waiting), tempfile.TemporaryDirectory() as temporary:
                row = build_observe_only_text_state_rows(
                    [{
                        "endpoint_id": "e", "observed_epoch_s": 10.0, "elapsed_s": 2,
                        "request_limit": 2, "work_limit": 20, "active_requests": 2,
                        "active_work": 20, "waiting_work": 100, "oldest_waiting_age_s": 2,
                    }],
                    [{"endpoint_index": 0, "waiting": waiting, "kv_usage": .1}],
                    endpoint_ids=("e",), calibration_signature="fixture",
                )[0]
                directory = Path(temporary)
                _write_json_atomic(directory / "state.json", row)
                _write_trace_rows_atomic(directory / "state.csv", [row])
                saved_json = json.loads((directory / "state.json").read_text())
                with (directory / "state.csv").open(newline="") as handle:
                    saved_csv = next(csv.DictReader(handle))
                self.assertEqual(saved_json["model_queued_work_estimated"],
                                 0 if waiting == 0 else None)
                self.assertEqual(saved_csv["model_queued_work_estimated"],
                                 "0" if waiting == 0 else "")
                self.assertEqual(saved_json["vllm_waiting"], waiting)
                self.assertEqual(saved_csv["vllm_waiting"], "0" if waiting == 0 else "")
                controller = BoundedCapacityController(
                    (CapacityArm(1, 10), CapacityArm(2, 20), CapacityArm(4, 40)),
                    fallback=CapacityArm(2, 20), target_service_rate_tokens_s=100,
                )
                with patch.object(controller, "select", wraps=controller.select) as select, \
                        patch("src.experiments.shared_vllm.runner.time.time", return_value=10):
                    _apply_state_control(
                        [saved_csv], controllers={"e": controller}, observer=unittest.mock.Mock(),
                        calibration_signature="fixture", max_state_age_s=1,
                    )
                self.assertEqual(select.call_args.kwargs["service_waiting_requests"],
                                 0 if waiting == 0 else None)
                self.assertEqual(select.call_args.args[0].for_stage("model").queued_work,
                                 0 if waiting == 0 else None)


if __name__ == "__main__":
    unittest.main()
