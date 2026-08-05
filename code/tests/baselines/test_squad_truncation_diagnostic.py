from __future__ import annotations

import importlib.util
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

_DIAG = CODE_ROOT / "scripts" / "baselines" / "squad_truncation_diagnostic.py"
_spec = importlib.util.spec_from_file_location("squad_truncation_diagnostic", _DIAG)
diag = importlib.util.module_from_spec(_spec)
sys.modules["squad_truncation_diagnostic"] = diag
_spec.loader.exec_module(diag)


def _run(finish_reason, http_status=200):
    return {"http_status": http_status, "finish_reason": finish_reason}


class DirectAllMatchTests(unittest.TestCase):
    """Stability requires UNANIMITY: every repeat HTTP 200 + same finish_reason.

    A partial run (1 success + 2 failures) or an all-failed run must NOT be
    judged stable. An earlier version that filtered to HTTP-200 rows and
    all()-ed the survivors would wrongly accept a single survivor.
    """

    def test_all_200_same_finish_matches(self) -> None:
        self.assertTrue(diag._direct_all_match([_run("length"), _run("length"), _run("length")], "length"))
        self.assertTrue(diag._direct_all_match([_run("stop"), _run("stop")], "stop"))

    def test_partial_failure_not_stable(self) -> None:
        # 1 success + 2 HTTP failures -> NOT stable (the blocker case).
        runs = [_run("length"), _run(None, http_status=500), _run(None, http_status=500)]
        self.assertFalse(diag._direct_all_match(runs, "length"))

    def test_all_http_failed_not_stable(self) -> None:
        runs = [_run(None, 500), _run(None, 502), _run(None, 500)]
        self.assertFalse(diag._direct_all_match(runs, "length"))
        self.assertFalse(diag._direct_all_match(runs, "stop"))

    def test_mixed_finish_reasons_not_stable(self) -> None:
        # All HTTP 200 but finish_reasons disagree -> not stable.
        self.assertFalse(diag._direct_all_match([_run("length"), _run("stop")], "length"))
        self.assertFalse(diag._direct_all_match([_run("length"), _run("length"), _run("stop")], "length"))

    def test_wrong_want_not_stable(self) -> None:
        self.assertFalse(diag._direct_all_match([_run("stop"), _run("stop")], "length"))

    def test_empty_not_stable(self) -> None:
        self.assertFalse(diag._direct_all_match([], "length"))


if __name__ == "__main__":
    unittest.main()
