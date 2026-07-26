from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts import run_kmax_interference_experiment as experiment  # noqa: E402


class KmaxInterferenceScriptTests(unittest.TestCase):
    def test_default_outputs_use_new_schema_version_without_overwriting_history(
        self,
    ) -> None:
        with patch.object(sys, "argv", ["run_kmax_interference_experiment"]):
            args = experiment.parse_args()

        self.assertTrue(args.small_output.endswith("_20260726.csv"))
        self.assertTrue(args.bulk_output.endswith("_20260726.csv"))


if __name__ == "__main__":
    unittest.main()
