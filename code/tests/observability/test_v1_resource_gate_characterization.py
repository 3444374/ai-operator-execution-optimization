"""Characterization tests: the v1 resource gate measured process TOTAL FDs.

These tests freeze the actual behavior of the 2026-09-04 v1 metric so the
v2 policy cannot silently reinterpret history. They replay the archived
attempt artifact (raw/semmap_res2/stress/measurements-attempt-93.json)
rather than re-running anything.
"""
import json
import unittest
from pathlib import Path

ATTEMPT = Path(__file__).resolve().parents[3] / (
    "experiments/results/postgresql/semmap_real_model_resource_20260904/"
    "raw/semmap_res2/stress/measurements-attempt-93.json")


def v1_check_stress_limits(baseline, ending, samples):
    """Verbatim re-implementation of the v1 uds gate over archived samples."""
    peaks = {
        name: {
            field: max(point["processes"][name][field] for point in samples)
            for field in ("rss_bytes", "fd", "threads")
        }
        for name in baseline
    }
    violations = []
    baseline_uds = baseline["gateway"]["fd"] + baseline["backend"]["fd"]
    peak_uds = peaks["gateway"]["fd"] + peaks["backend"]["fd"]
    ending_uds = ending["gateway"]["fd"] + ending["backend"]["fd"]
    if peak_uds - baseline_uds > 2:
        violations.append({
            "scope": "gateway+backend",
            "metric": "uds_peak_delta",
            "observed": peak_uds - baseline_uds,
            "limit": 2,
            "base_uds": baseline_uds,
            "peak_uds": peak_uds,
            "extra_metric": "fd",
        })
    if ending_uds - baseline_uds != 0:
        violations.append({
            "scope": "gateway+backend",
            "metric": "uds_end_delta",
            "observed": ending_uds - baseline_uds,
            "limit": 0,
        })
    return peaks, violations


@unittest.skipUnless(ATTEMPT.exists(), "archived v1 attempt artifact not present")
class V1GateCharacterization(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = json.loads(ATTEMPT.read_text(encoding="utf-8"))

    def test_v1_metric_measured_total_fd_not_udS_identity(self):
        samples = self.data["samples"]
        # v1 sampled only rss/fd/threads counts; no FD identity fields exist.
        first = samples[0]["processes"]["backend"]
        self.assertEqual(sorted(first), ["fd", "rss_bytes", "threads"])

    def test_v1_replay_reproduces_archived_violation(self):
        peaks, violations = v1_check_stress_limits(
            self.data["baseline"], self.data["ending"], self.data["samples"])
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0]["metric"], "uds_peak_delta")
        self.assertEqual(violations[0]["observed"], 3)
        self.assertEqual(violations[0]["limit"], 2)
        self.assertEqual(
            violations, self.data["violations"],
            "replay must reproduce the archived violation byte-for-byte")

    def test_v1_end_state_returns_to_baseline(self):
        for role in ("backend", "gateway"):
            self.assertEqual(self.data["ending"][role]["fd"],
                             self.data["baseline"][role]["fd"], role)
        # So the end gate (limit 0) never fired; only the peak gate is red.
        self.assertFalse(
            any(v["metric"] == "uds_end_delta" for v in self.data["violations"]))

    def test_peak_composition_backend_two_gateway_one(self):
        samples = self.data["samples"]
        backend_peak = max(s["processes"]["backend"]["fd"] for s in samples)
        gateway_peak = max(s["processes"]["gateway"]["fd"] for s in samples)
        self.assertEqual(backend_peak - self.data["baseline"]["backend"]["fd"], 2)
        self.assertEqual(gateway_peak - self.data["baseline"]["gateway"]["fd"], 1)
        # The archived data cannot attribute these +3 FDs to any category:
        # v1 recorded counts only. v2 must classify before judging.

    def test_ninety_three_attempts_are_one_irreversible_peak(self):
        aggregate = ATTEMPT.parent / "measurements-aggregate.json"
        if not aggregate.exists():
            self.skipTest("aggregate not present")
        data = json.loads(aggregate.read_text(encoding="utf-8"))
        self.assertEqual(len(data["per_attempt"]), 93)
        distinct = {json.dumps(a["violations"], sort_keys=True)
                    for a in data["per_attempt"]}
        self.assertEqual(len(distinct), 1,
                         "all 93 settle polls re-judged the same frozen peak")


if __name__ == "__main__":
    unittest.main()
