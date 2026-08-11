"""Unit tests for prepare_phase_change_workload: Poisson + OFF-first phase logic (no PG)."""
import sys
import unittest
from pathlib import Path

SCRIPTS_DATA = Path(__file__).resolve().parents[2] / "scripts" / "data"
if str(SCRIPTS_DATA) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DATA))
import prepare_phase_change_workload as pc  # noqa: E402


class TestPhaseChangeWorkload(unittest.TestCase):
    def test_derived_note_disclaims_official_vtc(self):
        self.assertIn("not official VTC reproduction", pc.DERIVED_NOTE)

    def test_poisson_within_window(self):
        arrivals = pc.poisson(rate=5.0, t_start=60.0, t_end=120.0, seed=42)
        self.assertTrue(all(60.0 < t < 120.0 for t in arrivals))
        self.assertGreater(len(arrivals), 100)  # ~5 req/s * 60s

    def test_poisson_seed_deterministic(self):
        a = pc.poisson(3.0, 0.0, 30.0, 7)
        b = pc.poisson(3.0, 0.0, 30.0, 7)
        self.assertEqual(a, b)

    def test_off_first_phase_order(self):
        """Job B ON windows = [60,120] and [180,240] (OFF-first)."""
        duration, period = 240.0, 60.0
        windows, k = [], 0
        while True:
            a, b = k * 2 * period + period, (k + 1) * 2 * period
            if a >= duration:
                break
            windows.append((a, min(b, duration)))
            k += 1
        self.assertEqual(windows, [(60.0, 120.0), (180.0, 240.0)])

    def test_no_job_b_arrivals_in_off_phase(self):
        """Job B arrivals must NOT appear in [0,60] or [120,180]."""
        b = []
        for a_s, b_s in [(60.0, 120.0), (180.0, 240.0)]:
            b.extend(pc.poisson(2.0, a_s, b_s, 11))
        self.assertTrue(all(60.0 <= t <= 120.0 or 180.0 <= t <= 240.0 for t in b))


if __name__ == "__main__":
    unittest.main()
